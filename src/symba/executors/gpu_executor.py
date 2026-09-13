"""gpu-profile executor: one warm subprocess per worker (spec 11.1, 11.3).

Model weights load ONCE, at boot, inside a long-lived child (``@worker.on_gpu_init``
hooks run there) and stay warm across jobs — AD-9's warm-weights rationale. Jobs are
serialized over a duplex pipe (slots are small, 1-2). A crash respawns + re-inits,
guarded by a circuit breaker: 3 crashes in 60s marks gpu tasks unclaimable (the worker
announces reduced tags) and logs CRITICAL, so a wedged GPU can't become an infinite
claim-crash-requeue loop against the whole queue.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from symba.errors import RetryableError, SymbaError
from symba.logging import get_logger

from ._pump import build_snapshot, replay_log, service_ctx_call
from .ctx_proxy import CtxCall, CtxProxy, JobDone, LogRecord, RunJob, Shutdown
from .process_executor import _rehydrate_child_error, _serialize_child_failure

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from symba.context import Ctx
    from symba.task_registry import RegisteredTask

_log = get_logger(component="executor.gpu")

_CIRCUIT_WINDOW_S = 60.0
_CIRCUIT_THRESHOLD = 3


def _gpu_child_main(
    conn: Connection,
    handlers: dict[str, Callable[..., Any]],
    on_init: list[Callable[[], None]],
) -> None:
    """Warm subprocess main loop: init once, then serve RunJob frames (spec 11.3)."""
    from symba.retry_classify import classify

    for hook in on_init:
        hook()
    conn.send(JobDone(ok=True, result="__gpu_init_ok__"))

    while True:
        try:
            frame = conn.recv()
        except EOFError:
            return
        if isinstance(frame, Shutdown):
            return
        if not isinstance(frame, RunJob):
            continue
        handler = handlers.get(frame.task_name)
        proxy = CtxProxy(frame.snapshot, conn)
        try:
            if handler is None:
                raise SymbaError(f"gpu task {frame.task_name!r} not registered in subprocess")
            result = handler(proxy, frame.payload)
            conn.send(JobDone(ok=True, result=result))
        except SymbaError as exc:
            conn.send(_serialize_child_failure(exc, retryable=exc.retryable))
        except Exception as exc:  # classified in-child + marshalled
            conn.send(_serialize_child_failure(exc, retryable=classify(exc)))


class GpuExecutor:
    """Warm-subprocess gpu executor with respawn + circuit breaker (spec 11.3)."""

    def __init__(
        self,
        handlers: dict[str, Callable[..., Any]],
        on_init: list[Callable[[], None]],
        *,
        on_circuit_open: Callable[[], None] | None = None,
    ) -> None:
        self._handlers = handlers
        self._on_init = on_init
        self._on_circuit_open = on_circuit_open
        self._ctx = mp.get_context("spawn")
        self._proc: Any | None = None
        self._conn: Connection | None = None
        self._lock = asyncio.Lock()
        self._crash_times: list[float] = []
        self._circuit_open = False
        #: Dedicated pool for the blocking pipe reads (init + `_pump`), so gpu pipe
        #: reads never contend with the host's default ThreadPoolExecutor. Small:
        #: gpu jobs are serialized (one in-flight), so 2 threads is ample.
        self._pump_pool: ThreadPoolExecutor | None = None

    async def start(self) -> None:
        self._pump_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="symba-gpu-pump")
        await self._spawn()

    async def _spawn(self) -> None:
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        proc = self._ctx.Process(
            target=_gpu_child_main,
            args=(child_conn, self._handlers, self._on_init),
            daemon=True,
        )
        proc.start()
        child_conn.close()
        self._proc = proc
        self._conn = parent_conn
        loop = asyncio.get_running_loop()
        init: JobDone = await loop.run_in_executor(self._pump_pool, parent_conn.recv)
        if not init.ok:
            raise SymbaError(f"gpu subprocess init failed: {init.error_message}")
        _log.info("gpu_subprocess_warm", pid=proc.pid)

    async def _respawn(self) -> None:
        now = time.monotonic()
        self._crash_times = [t for t in self._crash_times if now - t < _CIRCUIT_WINDOW_S]
        self._crash_times.append(now)
        if len(self._crash_times) >= _CIRCUIT_THRESHOLD:
            self._circuit_open = True
            _log.critical(
                "gpu_circuit_open",
                crashes=len(self._crash_times),
                window_s=_CIRCUIT_WINDOW_S,
                hint="gpu tasks now unclaimable; a wedged GPU won't crash-loop the queue",
            )
            if self._on_circuit_open is not None:
                self._on_circuit_open()
            return
        _log.warning("gpu_subprocess_respawning", crash_count=len(self._crash_times))
        await self._spawn()

    async def run(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        if self._circuit_open:
            raise RetryableError(
                "gpu circuit breaker open (too many subprocess crashes); this worker is "
                "not claiming gpu work until restarted"
            )
        async with self._lock:  # serialized: one in-flight job per subprocess
            return await self._run_locked(task, ctx, payload)

    async def _run_locked(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        if self._conn is None or self._proc is None or not self._proc.is_alive():
            await self._respawn()
            if self._circuit_open:
                raise RetryableError("gpu circuit breaker open; not claiming gpu work")
        assert self._conn is not None
        loop = asyncio.get_running_loop()
        snapshot = build_snapshot(ctx, payload)
        self._conn.send(RunJob(task_name=task.name, snapshot=snapshot, payload=payload))
        try:
            done = await self._pump(self._conn, ctx, loop)
        except (EOFError, BrokenPipeError, ConnectionError) as exc:
            await self._respawn()
            raise RetryableError(
                f"gpu subprocess died running task {task.name!r}; respawned, job will retry"
            ) from exc
        if not done.ok:
            raise _rehydrate_child_error(done)
        return done.result

    async def _pump(self, conn: Connection, ctx: Ctx, loop: asyncio.AbstractEventLoop) -> JobDone:
        while True:
            frame = await loop.run_in_executor(self._pump_pool, conn.recv)
            if isinstance(frame, JobDone):
                return frame
            if isinstance(frame, LogRecord):
                replay_log(frame, ctx.logger)
                continue
            if isinstance(frame, CtxCall):
                reply = await service_ctx_call(frame, ctx)
                conn.send(reply)
                continue

    async def stop(self, drain_s: float) -> None:
        if self._conn is not None and self._proc is not None and self._proc.is_alive():
            try:
                self._conn.send(Shutdown())
            except (BrokenPipeError, OSError):
                pass
            self._proc.join(timeout=drain_s)
            if self._proc.is_alive():
                self._proc.terminate()
        if self._pump_pool is not None:
            self._pump_pool.shutdown(wait=False, cancel_futures=True)
            self._pump_pool = None
        self._proc = None
        self._conn = None


__all__ = ["GpuExecutor"]
