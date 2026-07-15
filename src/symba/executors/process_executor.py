"""cpu-profile executor: a pool of warm forkserver workers (spec 11.1, 11.4).

Sync ``def`` handlers run one-per-process. Rather than ``ProcessPoolExecutor`` (which
pickles task args through a feeder thread and so can't hand a live duplex pipe to the
child), this manages its own small pool of persistent forkserver processes, each with
a dedicated duplex pipe. Verbs marshal back to the parent over that pipe (the child
owns no gRPC channel). A dead process (segfault, OOM-kill) surfaces as a
:class:`~symba.errors.RetryableError`; the slot's process is respawned so the pool
self-heals (spec 11.1). Processes are spawned lazily on first use (spec 8.3 step 3).
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from typing import TYPE_CHECKING, Any

from symba.errors import RetryableError, SymbaError
from symba.logging import get_logger

from ._pump import build_snapshot, replay_log, service_ctx_call
from .ctx_proxy import CtxCall, CtxProxy, JobDone, LogRecord, RunJob, Shutdown

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from symba.context import Ctx
    from symba.task_registry import RegisteredTask

_log = get_logger(component="executor.cpu")


def _cpu_worker_main(conn: Connection, handlers: dict[str, Any]) -> None:
    """Persistent worker loop: serve RunJob frames until Shutdown/EOF (spec 11.1)."""
    from symba.retry_classify import classify

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
                raise SymbaError(f"cpu task {frame.task_name!r} not registered in subprocess")
            result = handler(proxy, frame.payload)
            conn.send(JobDone(ok=True, result=result))
        except SymbaError as exc:
            conn.send(
                JobDone(
                    ok=False,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    retryable=exc.retryable,
                )
            )
        except Exception as exc:  # classified in-child + marshalled, never swallowed
            conn.send(
                JobDone(
                    ok=False,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    retryable=classify(exc),
                )
            )


class _Slot:
    """One persistent forkserver process + its duplex pipe."""

    __slots__ = ("conn", "proc")

    def __init__(self, proc: Any, conn: Connection) -> None:
        self.proc = proc
        self.conn = conn

    def alive(self) -> bool:
        return self.proc is not None and self.proc.is_alive()


class ProcessExecutor:
    """cpu executor: a lazily-built, self-healing pool of forkserver workers (spec 11.1)."""

    def __init__(self, max_workers: int, handlers: dict[str, Any] | None = None) -> None:
        self._max_workers = max(1, max_workers)
        self._handlers = handlers or {}
        self._ctx = mp.get_context("forkserver")
        self._idle: asyncio.LifoQueue[_Slot] | None = None
        self._started = False
        self._lock = asyncio.Lock()

    def register_handlers(self, handlers: dict[str, Any]) -> None:
        self._handlers = handlers

    async def start(self) -> None:
        return None  # lazy: first cpu job spins the pool up (spec 8.3 step 3)

    async def _ensure_started(self) -> asyncio.LifoQueue[_Slot]:
        async with self._lock:
            if not self._started:
                self._idle = asyncio.LifoQueue()
                for _ in range(self._max_workers):
                    self._idle.put_nowait(self._spawn_slot())
                self._started = True
                _log.info("cpu_pool_started", max_workers=self._max_workers)
            assert self._idle is not None
            return self._idle

    def _spawn_slot(self) -> _Slot:
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        proc = self._ctx.Process(
            target=_cpu_worker_main, args=(child_conn, self._handlers), daemon=True
        )
        proc.start()
        child_conn.close()  # the child holds its own end; the parent keeps parent_conn
        return _Slot(proc, parent_conn)

    async def run(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        idle = await self._ensure_started()
        slot = await idle.get()
        if not slot.alive():
            slot = self._spawn_slot()
        loop = asyncio.get_running_loop()
        snapshot = build_snapshot(ctx, payload)
        try:
            slot.conn.send(RunJob(task_name=task.name, snapshot=snapshot, payload=payload))
            done = await self._pump(slot.conn, ctx, loop)
        except (EOFError, BrokenPipeError, ConnectionError, OSError) as exc:
            # process died mid-job: replace the slot, fail retryable (pool self-heals).
            _log.warning("cpu_process_died_rebuilding", task=task.name)
            idle.put_nowait(self._spawn_slot())
            raise RetryableError(
                f"cpu process for task {task.name!r} died ({type(exc).__name__}); "
                f"slot respawned, job will retry"
            ) from exc
        else:
            idle.put_nowait(slot)
        if not done.ok:
            raise _rehydrate_child_error(done)
        return done.result

    async def _pump(self, conn: Connection, ctx: Ctx, loop: asyncio.AbstractEventLoop) -> JobDone:
        """Service CtxCall/LogRecord frames until JobDone (runs on the parent loop)."""
        while True:
            frame = await loop.run_in_executor(None, conn.recv)
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
        if not self._started or self._idle is None:
            return
        slots: list[_Slot] = []
        while not self._idle.empty():
            slots.append(self._idle.get_nowait())
        for slot in slots:
            if slot.alive():
                try:
                    slot.conn.send(Shutdown())
                except (BrokenPipeError, OSError):
                    pass
                slot.proc.join(timeout=drain_s)
                if slot.proc.is_alive():
                    slot.proc.terminate()
        self._started = False
        self._idle = None


def _rehydrate_child_error(done: JobDone) -> BaseException:
    """Rebuild a marshalled child error, preserving its name + child-computed verdict.

    The live exception cannot cross the pipe, so the child already ran classification
    (spec 15.2) and shipped the ``retryable`` verdict. The parent re-raises a
    :class:`SubprocessError` whose ``original_type`` echoes the original type (for logs
    + ``FailRequest.error_type``) and whose ``retryable`` mirrors the child's decision —
    the dispatch pipeline trusts ``exc.retryable`` for any :class:`SymbaError`.
    """
    err = SubprocessError(done.error_message or "subprocess handler failed")
    err.original_type = done.error_type or "SubprocessError"
    err.retryable = bool(done.retryable)
    return err


class SubprocessError(SymbaError):
    """A handler error that crossed a cpu/gpu process boundary (spec 11.4).

    Not part of the public taxonomy — it carries the original type name and the
    child's retryable verdict so the pipeline reports and retries faithfully.
    """

    original_type: str = "SubprocessError"

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(message, **context)


__all__ = ["ProcessExecutor", "SubprocessError"]
