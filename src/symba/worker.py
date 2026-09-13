"""Worker runtime (spec 8): lifecycle, claim stream, slot accounting, drain.

Slot accounting follows the law in spec 8.4:

1. every spawned job task is strong-referenced in ``self._running``;
2. there is exactly ONE release point — the task's done-callback — never inline
   in success/failure branches;
3. abandoned claims release through the same path (a pre-completed task);
4. drain bookkeeping is independent of the stop-claiming flag;
5. one assignment arriving during the bounded Complete-response slot handoff
   may wait briefly; sustained over-assignment is failed back with
   ``retryable=True`` + a WARNING (defence against engine accounting bugs).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from typing import Any

import grpc

from ._proto import control_plane_pb2_grpc, data_plane_pb2, data_plane_pb2_grpc
from ._version import SDK_VERSION_STRING
from .checkpoint import open_redis
from .config import SdkSettings, load_settings
from .dispatch import DispatchDeps, Dispatcher
from .executors.asyncio_executor import AsyncioExecutor
from .executors.base import Executor
from .executors.gpu_executor import GpuExecutor
from .executors.process_executor import ProcessExecutor
from .logging import configure as configure_logging
from .logging import get_logger, resolve_worker_name
from .middleware import LoggingMiddleware, MiddlewareChain, WorkerMiddleware
from .profiles import Profile
from .retry_classify import ClassificationRule
from .schemas import register_output_schema
from .task_registry import TaskRegistry
from .transport import TlsConfig, Transport
from .types import RetryPolicy
from .watchdog import EventLoopWatchdog

_log = get_logger(component="worker")

_SLOT_HANDOFF_GRACE_S = 1.0
_MAX_CONNECTION_AGE_DETAILS = "max connection age"

HandlerDecorator = Callable[[Callable[..., Any]], Callable[..., Any]]


def _is_scheduled_claim_stream_recycle(exc: grpc.aio.AioRpcError) -> bool:
    """Return whether the engine intentionally recycled its gRPC connection."""
    details = (exc.details() or "").strip().lower()
    return exc.code() == grpc.StatusCode.UNAVAILABLE and details == _MAX_CONNECTION_AGE_DETAILS


class Worker:
    """The worker runtime (spec 8.1)."""

    def __init__(
        self,
        engine: str | None = None,
        *,
        token: str | None = None,
        tags: list[str] | None = None,
        slots: int | None = None,
        cpu_slots: int | None = None,
        cpu_max_jobs_per_process: int | None = None,
        worker_id: str | None = None,
        strict_schemas: bool = False,
        heartbeat_interval_s: float | None = None,
        labels: dict[str, str] | None = None,
        middleware: list[WorkerMiddleware] | None = None,
        shutdown_drain_s: float | None = None,
        classify_overrides: list[ClassificationRule] | None = None,
        admission_control: Callable[[], bool] | None = None,
        liveness_file: str | None = None,
        tls: TlsConfig | None = None,
        settings: SdkSettings | None = None,
        checkpoint_redis_url: str | None = None,
        load_dotenv: bool = False,
    ) -> None:
        overrides: dict[str, Any] = {}
        engine_over: dict[str, Any] = {}
        if engine is not None:
            engine_over["target"] = engine
        if token is not None:
            engine_over["token"] = token
        if engine_over:
            overrides["engine"] = engine_over
        # Explicit checkpoint Redis URL wins over the env aliases so hosts that
        # configure via their own system (Dynaconf, etc.) need not export a var.
        if checkpoint_redis_url is not None:
            overrides["redis"] = {"url": checkpoint_redis_url}
        worker_over: dict[str, Any] = {}
        if tags is not None:
            worker_over["tags"] = tags
        if slots is not None:
            worker_over["slots"] = slots
        if cpu_slots is not None:
            worker_over["cpu_slots"] = cpu_slots
        if cpu_max_jobs_per_process is not None:
            worker_over["cpu_max_jobs_per_process"] = cpu_max_jobs_per_process
        if liveness_file is not None:
            worker_over["liveness_file"] = liveness_file
        if worker_id is not None:
            worker_over["name"] = worker_id
        if strict_schemas:
            worker_over["strict_schemas"] = True
        if heartbeat_interval_s is not None:
            worker_over["heartbeat_interval_s"] = heartbeat_interval_s
        if shutdown_drain_s is not None:
            worker_over["drain_timeout_s"] = shutdown_drain_s
        if worker_over:
            overrides["worker"] = worker_over

        self._settings = settings or load_settings(load_dotenv=load_dotenv, **overrides)
        self.tenant = "default"
        self.worker_id = resolve_worker_name(self._settings.worker.name or worker_id)
        self.labels = labels or {}
        self._extra_tags = list(self._settings.worker.tags)
        self._explicit_slots = self._settings.worker.slots
        self._explicit_cpu_slots = self._settings.worker.cpu_slots
        self._explicit_cpu_max_jobs = self._settings.worker.cpu_max_jobs_per_process
        self._strict_schemas = self._settings.worker.strict_schemas
        self._heartbeat_interval_s = self._settings.worker.heartbeat_interval_s
        self._drain_timeout_s = self._settings.worker.drain_timeout_s
        self._classify_overrides = classify_overrides or []
        # Optional, opt-in robustness hooks. The SDK ships NO logic behind them:
        # `admission_control` is a host-supplied probe (True == accept work), and
        # `liveness_file` is just a path the loop touches. Both inert when unset.
        self._admission_control = admission_control
        self._admission_poll_s = self._settings.worker.admission_poll_s
        self._liveness_file = self._settings.worker.liveness_file

        self.registry = TaskRegistry()
        self._transport = Transport(self._settings.engine, self._settings.grpc, tls=tls)
        self._stub: data_plane_pb2_grpc.WorkerServiceStub | None = None
        self._client_stub: control_plane_pb2_grpc.ClientServiceStub | None = None
        self._redis: Any | None = None
        self._executors: dict[Profile, Executor] = {Profile.IO: AsyncioExecutor()}
        self._middleware = [LoggingMiddleware(), *(middleware or [])]
        self._on_gpu_init: list[Callable[[], None]] = []
        self._gpu_unclaimable = False
        self._watchdog: EventLoopWatchdog | None = None

        self._running: set[asyncio.Task[Any]] = set()
        self._running_task_names: dict[asyncio.Task[Any], str] = {}
        # Only assignment-dispatch tasks own capacity. Defensive rejection tasks
        # are tracked for drain/error handling but must not release a slot they
        # never acquired.
        self._slot_owners: set[asyncio.Task[Any]] = set()
        self._accepting = True
        #: Local backpressure gate driven by ``admission_control`` (spec 8.4). When
        #: False the worker announces ``free_slots=0`` (honest local capacity) but
        #: keeps its claim stream open, so it resumes the moment the host recovers.
        self._admission_ok = True
        self._admission_task: asyncio.Task[None] | None = None
        self._liveness_task: asyncio.Task[None] | None = None
        self._slots = 0
        self._free_slots = 0
        self._slot_changed = asyncio.Event()
        self._slot_available = asyncio.Event()
        self._pending_slot_waiter = False
        self._stopped = asyncio.Event()
        self._dispatcher: Dispatcher | None = None

    # ----------------------------------------------------------- registration
    def task(
        self,
        name: str,
        *,
        profile: str | Profile = Profile.IO,
        runs_on: list[str] | None = None,
        rate_class: str | None = None,
        timeout_s: int | None = None,
        lease_ttl_s: int | None = None,
        max_attempts: int | None = None,
        backoff: RetryPolicy | None = None,
        max_concurrent_per_group: int | None = None,
        input_schema: Any = None,
        output_schema: Any = None,
    ) -> HandlerDecorator:
        """Register a handler (spec 8.2). Returns the function unchanged.

        A task's ``lease_ttl_s`` MUST be >= its own ``timeout_s`` (with heartbeat
        margin); otherwise the lease can lapse mid-run and the engine dispatches a
        duplicate. io tasks inherit the engine default lease (~60s), so set
        ``lease_ttl_s`` explicitly on any io task expected to run longer than a few
        heartbeats — boot validation raises ``ConfigError`` for a long ``timeout_s``
        left under the short engine-default lease.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.registry.register(
                name,
                fn,
                profile=profile,
                runs_on=runs_on,
                rate_class=rate_class,
                timeout_s=timeout_s,
                lease_ttl_s=lease_ttl_s,
                max_attempts=max_attempts,
                backoff=backoff,
                max_concurrent_per_group=max_concurrent_per_group,
                input_schema=input_schema,
                output_schema=output_schema,
            )
            register_output_schema(name, output_schema)
            return fn

        return decorator

    def on_gpu_init(self, fn: Callable[[], None]) -> Callable[[], None]:
        """Register a hook that runs ONCE in the warm gpu subprocess (spec 11.3).

        Load model weights here — they stay warm across every gpu job on this worker.
        """
        self._on_gpu_init.append(fn)
        return fn

    # ---------------------------------------------------------------- boot
    def _client(self) -> data_plane_pb2_grpc.WorkerServiceStub:
        if self._stub is None:
            self._stub = data_plane_pb2_grpc.WorkerServiceStub(self._transport.channel())
        return self._stub

    def _control_client(self) -> control_plane_pb2_grpc.ClientServiceStub:
        """Control-plane stub over the SAME channel — for ctx.submit (spec 10.1)."""
        if self._client_stub is None:
            self._client_stub = control_plane_pb2_grpc.ClientServiceStub(self._transport.channel())
        return self._client_stub

    def _resolve_slots(self) -> int:
        if self._explicit_slots is not None:
            return self._explicit_slots
        # profile-derived default: gpu tasks -> small, else generous io default.
        profiles = self.registry.profiles()
        if Profile.GPU in profiles:
            return 2
        if Profile.CPU in profiles and profiles == {Profile.CPU}:
            return 4
        return 100

    def _resolve_cpu_slots(self) -> int:
        """Size the cpu forkserver pool (spec 11.1), bounded by CPU cores.

        The cpu pool must NOT inherit ``slots``: ``slots`` is the io concurrency
        budget (await-bound, routinely set to the hundreds), but every cpu slot
        is a real OS subprocess that may load model weights. Forking ``slots``
        (e.g. 200) such processes OOM-kills the host. An explicit
        ``worker.cpu_slots`` always wins; otherwise default to the core count,
        never above the overall slot budget.
        """
        if self._explicit_cpu_slots is not None:
            return self._explicit_cpu_slots
        return max(1, min(self._slots, os.cpu_count() or 1))

    def _build_profile_executors(self) -> None:
        """Add cpu/gpu executors only for the profiles this worker actually registers."""
        profiles = self.registry.profiles()
        if Profile.CPU in profiles:
            cpu_handlers = {
                name: self.registry.get(name).handler  # type: ignore[union-attr]
                for name in self.registry.names()
                if self.registry.get(name).profile == Profile.CPU  # type: ignore[union-attr]
            }
            self._executors[Profile.CPU] = ProcessExecutor(
                max_workers=self._resolve_cpu_slots(),
                handlers=cpu_handlers,
                max_jobs_per_process=self._explicit_cpu_max_jobs,
            )
        if Profile.GPU in profiles:
            gpu_handlers = {
                name: self.registry.get(name).handler  # type: ignore[union-attr]
                for name in self.registry.names()
                if self.registry.get(name).profile == Profile.GPU  # type: ignore[union-attr]
            }
            self._executors[Profile.GPU] = GpuExecutor(
                gpu_handlers, self._on_gpu_init, on_circuit_open=self._open_gpu_circuit
            )

    def _open_gpu_circuit(self) -> None:
        """Circuit breaker tripped: stop advertising gpu tags (spec 11.3)."""
        self._gpu_unclaimable = True
        self._slot_changed.set()

    def _gpu_task_names(self) -> set[str]:
        return {
            name
            for name in self.registry.names()
            if self.registry.get(name).profile == Profile.GPU  # type: ignore[union-attr]
        }

    def _all_tags(self) -> list[str]:
        tags = set(self._extra_tags) | self.registry.all_runs_on()
        if self._gpu_unclaimable:
            # Drop tags that only exist to serve gpu tasks (circuit breaker open).
            gpu_only = self._gpu_only_tags()
            tags -= gpu_only
        return sorted(tags)

    def _gpu_only_tags(self) -> set[str]:
        gpu_tags: set[str] = set()
        other_tags: set[str] = set()
        for name in self.registry.names():
            task = self.registry.get(name)
            assert task is not None
            target = gpu_tags if task.profile == Profile.GPU else other_tags
            target.update(task.runs_on)
        return gpu_tags - other_tags

    def _boot(self) -> None:
        configure_logging(
            level=self._settings.log.level,
            fmt=self._settings.log.format,
            worker_name=self.worker_id,
        )
        self.registry.validate(
            strict_schemas=self._strict_schemas,
            heartbeat_interval_s=self._heartbeat_interval_s,
        )
        self._slots = self._resolve_slots()
        self._free_slots = self._slots
        if self._free_slots > 0:
            self._slot_available.set()
        self._build_profile_executors()
        self._dispatcher = Dispatcher(
            DispatchDeps(
                stub=self._client(),
                registry=self.registry,
                middleware=MiddlewareChain(self._middleware),
                executors=self._executors,
                tenant=self.tenant,
                heartbeat_interval_s=self._heartbeat_interval_s,
                classify_overrides=self._classify_overrides,
                client_stub=self._control_client(),
                redis_client=self._redis,
            )
        )
        _log.info(
            "[worker] booted",
            worker_id=self.worker_id,
            slots=self._slots,
            tags=self._all_tags(),
            tasks=self.registry.names(),
            sdk_version=SDK_VERSION_STRING,
        )

    # ----------------------------------------------------------- entrypoints
    def run(self) -> None:
        """Blocking entry: creates the loop, installs signal handlers (spec 8.3)."""
        asyncio.run(self.arun())

    async def arun(self) -> None:
        """Async entry for embedding into an existing asyncio app (spec 8.3)."""
        await self._open_checkpoint_redis()
        self._boot()
        await self._start_executors()
        self._start_watchdog()
        self._start_admission_loop()
        self._start_liveness_loop()
        self._install_signal_handlers()
        try:
            await self._claim_loop()
        finally:
            await self._stop_background_loops()
            await self._drain()
            await self._stop_watchdog()
            await self._stop_executors()
            await self._close_checkpoint_redis()
            await self._transport.aclose()

    async def _start_executors(self) -> None:
        for profile, executor in self._executors.items():
            if profile == Profile.IO:
                continue  # io needs no warm-up
            await executor.start()

    async def _stop_executors(self) -> None:
        for profile, executor in self._executors.items():
            if profile == Profile.IO:
                continue
            with contextlib.suppress(Exception):
                await executor.stop(self._drain_timeout_s)

    def _start_watchdog(self) -> None:
        self._watchdog = EventLoopWatchdog(
            lambda: sorted(set(self._running_task_names.values())),
            on_lag=self._feed_loop_lag,
        )
        self._watchdog.start()

    def _feed_loop_lag(self, lag_ms: float) -> None:
        """Forward watchdog lag samples to any MetricsMiddleware (spec 11.5, 17)."""
        for mw in self._middleware:
            observer = getattr(mw, "observe_loop_lag", None)
            if observer is not None:
                observer(lag_ms)

    async def _stop_watchdog(self) -> None:
        if self._watchdog is not None:
            await self._watchdog.stop()
            self._watchdog = None

    # ------------------------------------------------- opt-in background loops
    def _start_admission_loop(self) -> None:
        """Start the admission poller only when a host hook is supplied (spec 8.4)."""
        if self._admission_control is None:
            return
        self._admission_task = asyncio.ensure_future(self._admission_loop())

    async def _admission_loop(self) -> None:
        """Poll the host admission hook; gate claims on local resource pressure.

        The hook runs in a thread (a slow probe must never block the claim loop)
        and is FAIL-OPEN: any exception is logged and treated as 'accept', so a
        broken probe can never wedge the worker into announcing zero slots forever.
        Only a True<->False transition re-announces (one ``_slot_changed`` set), so
        this never thrashes the claim stream.
        """
        assert self._admission_control is not None
        loop = asyncio.get_running_loop()
        while not self._stopped.is_set():
            try:
                ok = await loop.run_in_executor(None, self._admission_control)
            except Exception as exc:  # fail-open: a bad probe must never wedge the worker
                _log.warning("[worker] admission_probe_failed", error=str(exc))
                ok = True
            if ok != self._admission_ok:
                self._admission_ok = ok
                self._slot_changed.set()  # force a re-announce with the new free_slots
                if ok:
                    _log.info("[worker] admission_resumed", worker_id=self.worker_id)
                else:
                    _log.warning(
                        "[worker] admission_blocked",
                        worker_id=self.worker_id,
                        hint="local resource watermark exceeded; announcing free_slots=0",
                    )
            await asyncio.sleep(self._admission_poll_s)

    def _start_liveness_loop(self) -> None:
        """Start the liveness-file writer only when a path is configured (spec 11.5)."""
        if not self._liveness_file:
            return
        self._liveness_task = asyncio.ensure_future(self._liveness_loop())

    async def _liveness_loop(self) -> None:
        """Touch the liveness file FROM the event loop every heartbeat.

        A wedged loop (e.g. a sync call blocking the claim loop) cannot run this, so
        the file's mtime goes stale -- which an external healthcheck reads as
        unhealthy. The SDK ships no probe; the host owns the freshness threshold.
        """
        assert self._liveness_file is not None
        path = Path(self._liveness_file)
        while not self._stopped.is_set():
            try:
                # ASYNC240: the touch MUST run ON the loop -- that is exactly what
                # proves the loop is alive. It is a microsecond tmpfs stat+utime,
                # not a real blocking wait, so keeping it inline is intentional.
                path.touch()  # noqa: ASYNC240
            except OSError as exc:
                _log.warning("[worker] liveness_touch_failed", path=str(path), error=str(exc))
            await asyncio.sleep(self._heartbeat_interval_s)

    async def _stop_background_loops(self) -> None:
        """Cancel the admission + liveness loops (idempotent; safe if never started)."""
        for task in (self._admission_task, self._liveness_task):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._admission_task = None
        self._liveness_task = None

    async def _open_checkpoint_redis(self) -> None:
        """Open the shared checkpoint Redis client if a URL is configured (spec 13.1)."""
        url = self._settings.redis.url
        if not url:
            return
        self._redis = await open_redis(url)
        _log.info("[worker] checkpoint_redis_enabled")

    async def _close_checkpoint_redis(self) -> None:
        if self._redis is None:
            return
        with contextlib.suppress(Exception):
            await self._redis.aclose()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop)

    def stop(self) -> None:
        """Begin graceful shutdown: stop claiming, announce zero slots, drain (spec 8.4)."""
        if self._accepting:
            _log.info("[worker] draining", worker_id=self.worker_id)
        self._accepting = False
        self._free_slots = 0
        self._slot_changed.set()
        self._slot_available.set()
        self._stopped.set()

    # ------------------------------------------------------------ claim loop
    async def _claim_loop(self) -> None:
        """Open the bidi Claim stream; reconnect forever with backoff (spec 5.2)."""
        backoff = self._settings.grpc.initial_reconnect_backoff_s
        while not self._stopped.is_set():
            try:
                stream_task = asyncio.create_task(self._run_claim_stream())
                stop_task = asyncio.create_task(self._stopped.wait())
                done, _pending = await asyncio.wait(
                    {stream_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    stream_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await stream_task
                    return
                stop_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stop_task
                # Propagate the stream result or transport exception into the
                # existing reconnect policy below.
                await stream_task
                backoff = self._settings.grpc.initial_reconnect_backoff_s
            except grpc.aio.AioRpcError as exc:
                if self._stopped.is_set():
                    return
                reconnect_delay_s = min(
                    backoff,
                    self._settings.grpc.max_reconnect_backoff_s,
                )
                log_fields = {
                    "code": exc.code().name,
                    "details": exc.details() or "",
                    "reconnect_delay_s": reconnect_delay_s,
                }
                if exc.code() in {
                    grpc.StatusCode.UNAUTHENTICATED,
                    grpc.StatusCode.PERMISSION_DENIED,
                }:
                    # Credentials cannot heal through transport backoff. Exit
                    # so the process supervisor can restart after deployment
                    # configuration is corrected, rather than leaving a live
                    # but permanently unregistered worker.
                    _log.error("[worker] claim_stream_auth_failed", **log_fields)
                    raise
                if _is_scheduled_claim_stream_recycle(exc):
                    _log.info("[worker] claim_stream_recycled", **log_fields)
                else:
                    _log.warning("[worker] claim_stream_dropped", **log_fields)
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=reconnect_delay_s,
                    )
                    return
                except TimeoutError:
                    pass
                backoff *= 2

    async def _run_claim_stream(self) -> None:
        stub = self._client()
        tags = self._all_tags()
        call = stub.Claim(self._claim_requests(tags))
        async for assignment in call:
            if self._stopped.is_set():
                break
            self._handle_assignment(assignment)

    async def _claim_requests(
        self, tags: list[str]
    ) -> AsyncGenerator[data_plane_pb2.ClaimRequest, None]:
        """Announce capacity on change and periodically while idle.

        The engine uses ClaimRequest arrivals as the worker-registration
        heartbeat. Waiting only for ``_slot_changed`` meant an idle worker sent
        no traffic, became stale, and stopped receiving the next job despite its
        claim stream and event loop still being healthy.
        """
        # Task registration is immutable after boot. Advertise the exact local
        # handler inventory separately from routing tags so operator consoles can
        # display capabilities without changing scheduling semantics.
        registered_tasks = sorted(self.registry.names())
        while not self._stopped.is_set():
            yield data_plane_pb2.ClaimRequest(
                worker_id=self.worker_id,
                tags=tags,
                registered_tasks=registered_tasks,
                free_slots=self._announce_free_slots(),
                sdk_version=SDK_VERSION_STRING,
                labels={**self.labels, "symba.slots_total": str(self._slots)},
            )
            self._slot_changed.clear()
            try:
                await asyncio.wait_for(
                    self._slot_changed.wait(),
                    timeout=self._heartbeat_interval_s,
                )
            except TimeoutError:
                # Idle registration heartbeat: re-yield unchanged capacity.
                pass

    def _announce_free_slots(self) -> int:
        """Slots to advertise to the engine: honest LOCAL capacity.

        Zeroed while draining (``_accepting`` false) or when the optional admission
        hook reports this box is over its resource watermark (``_admission_ok``
        false). The claim stream stays open either way, so the worker resumes the
        moment capacity returns.
        """
        if not self._accepting or not self._admission_ok:
            return 0
        return self._free_slots

    # -------------------------------------------------------- slot accounting
    def _handle_assignment(self, assignment: data_plane_pb2.JobAssignment) -> None:
        if not self._accepting:
            _log.warning("[worker] assignment_without_slot", job_id=assignment.job.id)
            self._spawn(self._reject_assignment(assignment), owns_slot=False)
            return
        if self._free_slots <= 0:
            # Complete is committed by the engine just before its RPC response
            # reaches this worker. Hold one assignment through that sub-second
            # handoff instead of retrying a healthy job. Sustained or multiple
            # over-assignment is still rejected by the existing defence.
            if self._pending_slot_waiter:
                _log.warning("[worker] assignment_without_slot", job_id=assignment.job.id)
                self._spawn(self._reject_assignment(assignment), owns_slot=False)
                return
            self._pending_slot_waiter = True
            self._spawn(self._wait_for_slot_or_reject(assignment), owns_slot=False)
            return
        self._start_assignment(assignment)

    def _start_assignment(self, assignment: data_plane_pb2.JobAssignment) -> bool:
        """Acquire one local slot and dispatch, or return False if none is free."""
        if not self._accepting or self._free_slots <= 0:
            return False
        self._free_slots -= 1
        if self._free_slots <= 0:
            self._slot_available.clear()
        self._slot_changed.set()
        self._spawn(self._dispatch_one(assignment), task_name=assignment.job.spec.task_name)
        return True

    async def _wait_for_slot_or_reject(self, assignment: data_plane_pb2.JobAssignment) -> None:
        """Absorb the bounded Complete-response/local-release handoff window."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _SLOT_HANDOFF_GRACE_S
        try:
            while self._accepting:
                if self._start_assignment(assignment):
                    return
                self._slot_available.clear()
                # Re-check after clear so a concurrent release cannot be lost.
                if self._free_slots > 0:
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self._slot_available.wait(), timeout=remaining)
                except TimeoutError:
                    break
            _log.warning("[worker] assignment_without_slot", job_id=assignment.job.id)
            await self._reject_assignment(assignment)
        finally:
            self._pending_slot_waiter = False

    def _spawn(
        self,
        coro: Awaitable[None],
        *,
        task_name: str | None = None,
        owns_slot: bool = True,
    ) -> None:
        task = asyncio.ensure_future(coro)
        self._running.add(task)
        if owns_slot:
            self._slot_owners.add(task)
        if task_name is not None:
            self._running_task_names[task] = task_name
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        """THE single release point (spec 8.4 rule 2)."""
        self._running.discard(task)
        self._running_task_names.pop(task, None)
        owns_slot = task in self._slot_owners
        self._slot_owners.discard(task)
        if owns_slot and self._accepting:
            self._free_slots = min(self._free_slots + 1, self._slots)
            if self._free_slots > 0:
                self._slot_available.set()
        self._slot_changed.set()
        exc = task.exception() if not task.cancelled() else None
        if exc is not None:
            _log.error("[worker] dispatch_task_crashed", error=str(exc), exc_info=exc)

    async def _dispatch_one(self, assignment: data_plane_pb2.JobAssignment) -> None:
        assert self._dispatcher is not None
        await self._dispatcher.dispatch(assignment)

    async def _reject_assignment(self, assignment: data_plane_pb2.JobAssignment) -> None:
        """Fail an over-assigned job back with retryable=True (spec 8.4 rule 5)."""
        try:
            await self._client().Fail(
                data_plane_pb2.FailRequest(
                    job_id=assignment.job.id,
                    lease_token=assignment.lease_token,
                    error_type="NoFreeSlot",
                    error_message="worker had no free slot for this assignment",
                    error_message_safe=True,
                    retryable=True,
                )
            )
        except grpc.aio.AioRpcError:
            pass

    # ---------------------------------------------------------------- drain
    async def _drain(self) -> None:
        """Wait for running tasks to finish, bounded by drain_timeout_s (spec 8.4)."""
        if not self._running:
            return
        _log.info("[worker] draining_jobs", count=len(self._running))
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._running, return_exceptions=True),
                timeout=self._drain_timeout_s,
            )
        except TimeoutError:
            abandoned = len(self._running)
            _log.warning("[worker] drain_timeout_abandoning", count=abandoned)


__all__ = ["Worker"]
