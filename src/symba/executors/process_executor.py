"""cpu-profile executor: a pool of warm forkserver workers (spec 11.1, 11.4).

Sync ``def`` handlers run one-per-process. Rather than ``ProcessPoolExecutor`` (which
pickles task args through a feeder thread and so can't hand a live duplex pipe to the
child), this manages its own small pool of persistent forkserver processes, each with
a dedicated duplex pipe. Verbs marshal back to the parent over that pipe (the child
owns no gRPC channel). A dead process (segfault, OOM-kill) surfaces as a
:class:`~symba.errors.RetryableError`; the slot's process is respawned so the pool
self-heals (spec 11.1).

Processes are spawned lazily ON DEMAND, one at a time, up to ``max_workers`` (spec
8.3 step 3) -- NOT all eagerly on first use. Eager spawn is a thundering herd: with
a large pool every process boots (and each cpu handler may load model weights) at
once, spiking memory hard enough to OOM-kill the host. Lazy growth means only as
many processes as there are concurrent cpu jobs are ever created, and the warm ones
are reused thereafter.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from symba._error_serialization import SerializedFailure, serialize_exception
from symba.errors import RetryableError, SymbaError
from symba.logging import get_logger

from ._pump import build_snapshot, replay_log, service_ctx_call
from .ctx_proxy import CtxCall, CtxProxy, JobDone, LogRecord, RunJob, Shutdown

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from symba.context import Ctx
    from symba.task_registry import RegisteredTask

_log = get_logger(component="executor.cpu")

#: Grace period for a recycled subprocess to exit on ``Shutdown`` before terminate.
_RECYCLE_JOIN_S = 5.0


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
            conn.send(_serialize_child_failure(exc, retryable=exc.retryable))
        except Exception as exc:  # classified in-child + marshalled, never swallowed
            conn.send(_serialize_child_failure(exc, retryable=classify(exc)))


class _Slot:
    """One persistent forkserver process + its duplex pipe.

    ``jobs`` counts COMPLETED jobs on this process (crashes respawn a fresh slot,
    so its counter resets). Used for pool-hygiene recycling (spec 11.1).
    """

    __slots__ = ("conn", "jobs", "proc")

    def __init__(self, proc: Any, conn: Connection) -> None:
        self.proc = proc
        self.conn = conn
        self.jobs = 0

    def alive(self) -> bool:
        return self.proc is not None and self.proc.is_alive()


class ProcessExecutor:
    """cpu executor: a lazily-built, self-healing pool of forkserver workers (spec 11.1)."""

    def __init__(
        self,
        max_workers: int,
        handlers: dict[str, Any] | None = None,
        *,
        max_jobs_per_process: int | None = None,
    ) -> None:
        self._max_workers = max(1, max_workers)
        self._handlers = handlers or {}
        #: Recycle a worn process after this many completed jobs (``None`` = never).
        self._max_jobs_per_process = max_jobs_per_process
        self._ctx = mp.get_context("forkserver")
        self._idle: asyncio.LifoQueue[_Slot] | None = None
        #: Live process count (grown lazily up to ``max_workers``). A dead slot
        #: replaced in place is net-zero, so this only ever counts real growth.
        self._spawned = 0
        self._started = False
        self._lock = asyncio.Lock()
        #: Dedicated pool for the blocking pipe reads in ``_pump``. Kept OFF the
        #: default ThreadPoolExecutor, which the host hammers via
        #: ``asyncio.to_thread`` for io-handler DB work -- so subprocess reads can
        #: never be starved by io threads. Sized to ``max_workers``: in-flight cpu
        #: jobs <= slots, so one thread per slot is exactly enough, never more.
        self._pump_pool: ThreadPoolExecutor | None = None

    def register_handlers(self, handlers: dict[str, Any]) -> None:
        self._handlers = handlers

    async def start(self) -> None:
        return None  # lazy: first cpu job spins the pool up (spec 8.3 step 3)

    async def _ensure_started(self) -> asyncio.LifoQueue[_Slot]:
        async with self._lock:
            if not self._started:
                # Lazy: the queue starts EMPTY. Processes are forked on demand in
                # `_acquire` up to `max_workers`, never all at once (see module
                # docstring: eager spawn is a memory thundering herd).
                self._idle = asyncio.LifoQueue()
                self._spawned = 0
                self._pump_pool = ThreadPoolExecutor(
                    max_workers=self._max_workers, thread_name_prefix="symba-cpu-pump"
                )
                self._started = True
                _log.info("cpu_pool_started", max_workers=self._max_workers)
            assert self._idle is not None
            return self._idle

    async def _acquire(self, idle: asyncio.LifoQueue[_Slot]) -> _Slot:
        """Get a warm slot, or grow the pool by one, or wait for one to free.

        Precedence: reuse an idle warm process -> if under `max_workers`, fork a
        new one -> otherwise block until a busy slot is returned. This caps the
        live process count at `max_workers` while only ever spawning as many as
        concurrent demand requires.
        """
        try:
            return idle.get_nowait()
        except asyncio.QueueEmpty:
            pass
        async with self._lock:
            if self._spawned < self._max_workers:
                self._spawned += 1
                return self._spawn_slot()
        # At capacity and none idle: wait for an in-flight job to return its slot.
        return await idle.get()

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
        slot = await self._acquire(idle)
        if not slot.alive():
            # A warm slot died while idle: replace it in place (net-zero count).
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
            self._release_or_recycle(slot, idle, loop)
        if not done.ok:
            raise _rehydrate_child_error(done)
        return done.result

    def _release_or_recycle(
        self, slot: _Slot, idle: asyncio.LifoQueue[_Slot], loop: asyncio.AbstractEventLoop
    ) -> None:
        """Return a healthy slot to the pool, or recycle it if it hit the job cap.

        Recycling replaces the slot with a fresh one (``_spawned`` stays net-zero,
        one down one up) and RETIRES the worn process OFF the event loop: the
        teardown join blocks, so it must never run inline (would stall the claim
        loop) and must use the DEFAULT executor, not the pump pool (a rare teardown
        must not occupy a pipe-read thread).
        """
        slot.jobs += 1
        cap = self._max_jobs_per_process
        if cap is not None and slot.jobs >= cap:
            _log.info("cpu_slot_recycled", jobs=slot.jobs, max_jobs=cap)
            idle.put_nowait(self._spawn_slot())
            loop.run_in_executor(None, self._retire_slot, slot)
            return
        idle.put_nowait(slot)

    def _retire_slot(self, slot: _Slot) -> None:
        """Gracefully stop a recycled subprocess (runs in a worker thread; blocks).

        Mirrors ``stop()``'s teardown for a single slot. Joining here (not inline)
        reaps the daemon child so recycling leaves no zombies.
        """
        if not slot.alive():
            return
        try:
            slot.conn.send(Shutdown())
        except (BrokenPipeError, OSError):
            pass
        slot.proc.join(timeout=_RECYCLE_JOIN_S)
        if slot.proc.is_alive():
            slot.proc.terminate()

    async def _pump(self, conn: Connection, ctx: Ctx, loop: asyncio.AbstractEventLoop) -> JobDone:
        """Service CtxCall/LogRecord frames until JobDone (runs on the parent loop)."""
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
        if self._pump_pool is not None:
            # wait=False: in-flight blocking recv() threads can't be cancelled and
            # will exit when their child dies; don't block shutdown on them.
            self._pump_pool.shutdown(wait=False, cancel_futures=True)
            self._pump_pool = None
        self._started = False
        self._idle = None
        self._spawned = 0


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
    err._symba_serialized_failure = SerializedFailure(
        error_type=err.original_type,
        message=done.error_message or "Task handler failed",
        metadata=dict(done.error_metadata or {}),
        rate_limited=done.rate_limited,
        retry_after_s=done.retry_after_s,
    )
    return err


def _serialize_child_failure(exc: BaseException, *, retryable: bool) -> JobDone:
    failure = serialize_exception(exc)
    return JobDone(
        ok=False,
        error_type=failure.error_type,
        error_message=failure.message,
        error_metadata=failure.metadata,
        error_message_safe=True,
        rate_limited=failure.rate_limited,
        retry_after_s=failure.retry_after_s,
        retryable=retryable,
    )


class SubprocessError(SymbaError):
    """A handler error that crossed a cpu/gpu process boundary (spec 11.4).

    Not part of the public taxonomy — it carries the original type name and the
    child's retryable verdict so the pipeline reports and retries faithfully.
    """

    original_type: str = "SubprocessError"
    _symba_serialized_failure: SerializedFailure | None = None

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(message, **context)


__all__ = ["ProcessExecutor", "SubprocessError"]
