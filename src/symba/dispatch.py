"""The per-assignment dispatch pipeline (spec 9).

Turns one ``JobAssignment`` into exactly one terminal outcome (Complete / Fail),
following the sequence in spec 9. Slot release is the caller's concern (the
worker's done-callback, spec 8.4); this module only computes and delivers the
outcome.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import grpc
from tenacity import retry, stop_after_delay, wait_exponential

from . import _json
from ._control import _Parked
from ._error_serialization import serialize_exception
from ._proto import control_plane_pb2_grpc, data_plane_pb2, data_plane_pb2_grpc
from .checkpoint import CheckpointStore
from .context import Ctx, Skip, StopChain, UpstreamOutputs
from .ctx_backend import IoCtxBackend
from .errors import SymbaError
from .heartbeat import CancelReason, HeartbeatShell
from .idempotency import attempt_key, derive_key
from .logging import get_logger
from .profiles import Profile
from .retry_classify import ClassificationRule, classify
from .schemas import output_schema_for, serialize_result, validate_payload

if TYPE_CHECKING:
    from .executors.base import Executor
    from .middleware import MiddlewareChain
    from .task_registry import RegisteredTask, TaskRegistry

_log = get_logger(component="dispatch")

_ERROR_MESSAGE_CAP = 2048


@dataclass(slots=True)
class DispatchDeps:
    """Everything the pipeline needs, injected by the Worker (keeps it testable)."""

    stub: data_plane_pb2_grpc.WorkerServiceStub
    registry: TaskRegistry
    middleware: MiddlewareChain
    executors: dict[Profile, Executor]
    tenant: str
    heartbeat_interval_s: float
    classify_overrides: list[ClassificationRule]
    #: Control-plane stub for ctx.submit / ctx.submit_children (spec 10.1). When
    #: absent (unit tests), those verbs raise a clear UnsupportedInProfile.
    client_stub: control_plane_pb2_grpc.ClientServiceStub | None = None
    default_pipeline: str | None = None
    #: Optional shared async Redis client for the checkpoint fast path (spec 13.1).
    #: ``None`` => durable PutCheckpoint only.
    redis_client: Any | None = None


def _stack_hash(exc: BaseException) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return hashlib.sha256(tb.encode()).hexdigest()[:16]


class Dispatcher:
    """Runs the pipeline for one assignment at a time (spec 9)."""

    def __init__(self, deps: DispatchDeps) -> None:
        self._deps = deps

    async def dispatch(self, assignment: data_plane_pb2.JobAssignment) -> None:
        """Process one assignment end to end. Never raises for job-level errors."""
        job = assignment.job
        task = self._deps.registry.get(job.spec.task_name)
        if task is None:
            await self._fail(
                job.id,
                assignment.lease_token,
                error_type="TaskNotRegistered",
                message=f"task {job.spec.task_name!r} not registered on this worker",
                retryable=False,
                stack_hash="",
                message_safe=True,
            )
            return

        ctx, _logger = self._build_ctx(assignment, task)
        await self._deps.middleware.on_claim(ctx)

        try:
            payload = validate_payload(ctx.payload, task.input_schema)
            ctx.payload = payload
        except SymbaError as exc:
            await self._fail_from_exc(
                job.id, assignment.lease_token, exc, retryable=False, max_attempts=task.max_attempts
            )
            await self._deps.middleware.on_fail(ctx, exc, False)
            return

        shell = self._make_shell(assignment, task)
        handler_task: asyncio.Task[Any] | None = None

        async def _cancel(_: CancelReason) -> None:
            if handler_task is not None and not handler_task.done():
                handler_task.cancel()

        shell._on_cancel = _cancel  # type: ignore[attr-defined]
        ctx._heartbeat_shell = shell  # type: ignore[attr-defined]
        shell.start()
        started = time.monotonic()

        executor = self._deps.executors[task.profile]
        try:
            handler_task = asyncio.ensure_future(executor.run(task, ctx, payload))
            outcome = await handler_task
        except _Parked as parked:
            # wait_for_event parked the job engine-side: no Complete, no Fail — the
            # engine owns WAITING, the slot releases via the caller's done-callback (spec 14.1).
            await shell.stop()
            ctx.logger.info("[dispatch] job_parked", wait_key=parked.wait_key)
            await self._deps.middleware.on_park(ctx, parked.wait_key)
            return
        except asyncio.CancelledError:
            await shell.stop()
            await self._finalize_cancel(job.id, assignment.lease_token, ctx, shell)
            return
        except BaseException as exc:
            await shell.stop()
            await self._finalize_failure(
                job.id, assignment.lease_token, ctx, exc, max_attempts=task.max_attempts
            )
            return

        await shell.stop()
        await self._finalize_success(job.id, assignment.lease_token, ctx, task, outcome, started)

    # ------------------------------------------------------------- build ctx
    def _build_ctx(
        self, assignment: data_plane_pb2.JobAssignment, task: RegisteredTask
    ) -> tuple[Ctx, Any]:
        job = assignment.job
        spec = job.spec
        ctx_id = spec.ctx_id or job.id
        idem = derive_key(
            tenant=job.tenant or self._deps.tenant, dedup_key=spec.dedup_key, job_id=job.id
        )
        logger = get_logger(
            job_id=job.id,
            ctx_id=ctx_id,
            task_name=spec.task_name,
            attempt=job.attempt,
            tenant=job.tenant or self._deps.tenant,
        )
        payload = _json.loads(spec.payload_json) if spec.payload_json else {}
        event_payload = (
            _json.loads(assignment.event_payload_json) if assignment.event_payload_json else None
        )
        checkpoint_data = (
            _json.loads(assignment.checkpoint_json) if assignment.checkpoint_json else None
        )
        tenant = job.tenant or self._deps.tenant
        pipeline = spec.pipeline or None
        backend = self._make_backend(
            assignment,
            ctx_id=ctx_id,
            tenant=tenant,
            pipeline=pipeline,
            idempotency_key=idem,
            attempt=job.attempt,
            event_payload=event_payload,
            has_checkpoint=checkpoint_data is not None,
            logger=logger,
        )
        output = UpstreamOutputs(
            list(job.upstream),
            lazy_fetch=backend.resolve_upstream if backend is not None else None,
            schema_resolver=output_schema_for,
        )
        ctx = Ctx(
            job_id=job.id,
            ctx_id=ctx_id,
            task_name=spec.task_name,
            attempt=job.attempt,
            tenant=tenant,
            payload=payload,
            output=output,
            logger=logger,
            pipeline=pipeline,
            stage=spec.stage or None,
            group_key=spec.group_key or None,
            event_payload=event_payload,
            checkpoint_data=checkpoint_data,
            idempotency_key=idem,
            idempotency_key_attempt=attempt_key(idem, job.attempt),
            backend=backend,
            profile=task.profile.value,
        )
        return ctx, logger

    def _make_backend(
        self,
        assignment: data_plane_pb2.JobAssignment,
        *,
        ctx_id: str,
        tenant: str,
        pipeline: str | None,
        idempotency_key: str,
        attempt: int,
        event_payload: dict[str, Any] | None,
        has_checkpoint: bool,
        logger: Any,
    ) -> IoCtxBackend | None:
        if self._deps.client_stub is None:
            return None
        checkpoint_store = CheckpointStore(
            worker_stub=self._deps.stub,
            job_id=assignment.job.id,
            lease_token=assignment.lease_token,
            idempotency_key=idempotency_key,
            redis=self._deps.redis_client,
        )
        return IoCtxBackend(
            client_stub=self._deps.client_stub,
            worker_stub=self._deps.stub,
            tenant=tenant,
            ctx_id=ctx_id,
            pipeline=pipeline,
            job_id=assignment.job.id,
            lease_token=assignment.lease_token,
            default_pipeline=self._deps.default_pipeline,
            checkpoint_store=checkpoint_store,
            attempt=attempt,
            event_payload=event_payload,
            has_checkpoint=has_checkpoint,
            logger=logger,
        )

    def _make_shell(
        self, assignment: data_plane_pb2.JobAssignment, task: RegisteredTask
    ) -> HeartbeatShell:
        lease_ttl = task.effective_lease_ttl_s or 60
        timeout = task.effective_timeout_s

        async def _noop(_: CancelReason) -> None:
            return None

        return HeartbeatShell(
            self._deps.stub,
            job_id=assignment.job.id,
            lease_token=assignment.lease_token,
            interval_s=self._deps.heartbeat_interval_s,
            lease_ttl_s=lease_ttl,
            timeout_s=float(timeout) if timeout is not None else None,
            on_cancel=_noop,
        )

    # ---------------------------------------------------------- finalization
    async def _finalize_success(
        self,
        job_id: str,
        lease_token: str,
        ctx: Ctx,
        task: RegisteredTask,
        outcome: Any,
        started: float,
    ) -> None:
        duration_ms = (time.monotonic() - started) * 1000
        try:
            if isinstance(outcome, Skip):
                await self._complete(job_id, lease_token, b"", skipped=True)
            elif isinstance(outcome, StopChain):
                result = serialize_result(outcome.result or {}, task.output_schema)
                await self._complete(job_id, lease_token, _json.dumps(result), drop_chain_tail=True)
            else:
                result = serialize_result(outcome, task.output_schema)
                await self._complete(job_id, lease_token, _json.dumps(result))
        except SymbaError as exc:
            await self._finalize_failure(
                job_id, lease_token, ctx, exc, max_attempts=task.max_attempts
            )
            return
        await self._cleanup_checkpoints(ctx)
        await self._deps.middleware.on_complete(ctx, outcome, duration_ms)

    async def _cleanup_checkpoints(self, ctx: Ctx) -> None:
        """Drain in-flight PutCheckpoints and reap the Redis fast-path key (spec 13.3)."""
        backend = ctx._backend  # type: ignore[attr-defined]
        store = getattr(backend, "_checkpoint_store", None)
        if store is None:
            return
        await store.drain()
        await store.delete_fast()

    async def _finalize_failure(
        self,
        job_id: str,
        lease_token: str,
        ctx: Ctx,
        exc: BaseException,
        *,
        max_attempts: int | None = None,
    ) -> None:
        if isinstance(exc, SymbaError):
            retryable = exc.retryable
        else:
            retryable = classify(exc, overrides=self._deps.classify_overrides)
        failure = serialize_exception(exc)
        stack_hash = _stack_hash(exc)
        ctx.logger.error(
            "[dispatch] handler_raised",
            error_type=failure.error_type,
            retryable=retryable,
            stack_hash=stack_hash,
            error_metadata=failure.metadata,
        )
        await self._fail(
            job_id,
            lease_token,
            error_type=failure.error_type,
            message=failure.message,
            retryable=retryable,
            stack_hash=stack_hash,
            max_attempts=max_attempts,
            metadata=failure.metadata,
            message_safe=True,
            rate_limited=failure.rate_limited,
            retry_after_s=failure.retry_after_s,
        )
        await self._deps.middleware.on_fail(ctx, exc, retryable)

    async def _finalize_cancel(
        self, job_id: str, lease_token: str, ctx: Ctx, shell: HeartbeatShell
    ) -> None:
        if shell.cancel_reason == CancelReason.TIMEOUT:
            await self._fail(
                job_id,
                lease_token,
                error_type="JobTimeout",
                message="job exceeded timeout_s",
                retryable=True,
                stack_hash="",
                message_safe=True,
            )
            err: BaseException = TimeoutError("job timeout")
        else:
            await self._fail(
                job_id,
                lease_token,
                error_type="Cancelled",
                message="cancelled by engine",
                retryable=False,
                stack_hash="",
                message_safe=True,
            )
            err = asyncio.CancelledError()
        await self._deps.middleware.on_fail(ctx, err, shell.cancel_reason == CancelReason.TIMEOUT)

    # ------------------------------------------------------------- RPC layer
    async def _complete(
        self,
        job_id: str,
        lease_token: str,
        result_json: bytes,
        *,
        drop_chain_tail: bool = False,
        skipped: bool = False,
    ) -> None:
        req = data_plane_pb2.CompleteRequest(
            job_id=job_id,
            lease_token=lease_token,
            result_json=result_json,
            drop_chain_tail=drop_chain_tail,
            skipped=skipped,
        )
        await self._deliver(self._deps.stub.Complete, req, job_id, "Complete")

    async def _fail_from_exc(
        self,
        job_id: str,
        lease_token: str,
        exc: BaseException,
        *,
        retryable: bool,
        max_attempts: int | None = None,
    ) -> None:
        failure = serialize_exception(exc)
        await self._fail(
            job_id,
            lease_token,
            error_type=failure.error_type,
            message=failure.message,
            retryable=retryable,
            stack_hash=_stack_hash(exc),
            max_attempts=max_attempts,
            metadata=failure.metadata,
            message_safe=True,
            rate_limited=failure.rate_limited,
            retry_after_s=failure.retry_after_s,
        )

    async def _fail(
        self,
        job_id: str,
        lease_token: str,
        *,
        error_type: str,
        message: str,
        retryable: bool,
        stack_hash: str,
        max_attempts: int | None = None,
        metadata: dict[str, object] | None = None,
        message_safe: bool = False,
        rate_limited: bool = False,
        retry_after_s: float | None = None,
    ) -> None:
        req = data_plane_pb2.FailRequest(
            job_id=job_id,
            lease_token=lease_token,
            error_type=error_type,
            error_message=message[:_ERROR_MESSAGE_CAP],
            stack_hash=stack_hash,
            retryable=retryable,
            # Report the worker-declared retry cap so the engine dies at the right
            # attempt; 0/unset leaves the engine on its stored default.
            max_attempts=max_attempts or 0,
            error_metadata_json=_json.dumps(metadata or {}),
            error_message_safe=message_safe,
            rate_limited=rate_limited,
            retry_after_s=retry_after_s or 0.0,
        )
        await self._deliver(self._deps.stub.Fail, req, job_id, "Fail")

    async def _deliver(self, method: Any, request: Any, job_id: str, verb: str) -> None:
        """Deliver Complete/Fail with tenacity (5 attempts, max 10s); swallow StaleLease (spec 9.4)."""

        @retry(stop=stop_after_delay(10), wait=wait_exponential(multiplier=0.2, max=2))
        async def _attempt() -> None:
            await method(request)

        try:
            await _attempt()
        except grpc.aio.AioRpcError as exc:
            if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                _log.warning("stale_lease_swallowed", job_id=job_id, verb=verb)
                return
            _log.error("finalization_dropped", job_id=job_id, verb=verb, code=exc.code().name)
        except Exception as exc:
            _log.error("finalization_dropped", job_id=job_id, verb=verb, error=str(exc))


__all__ = ["Dispatcher", "DispatchDeps"]
