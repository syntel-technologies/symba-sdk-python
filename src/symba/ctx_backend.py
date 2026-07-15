"""Ctx verb backends (spec 10.1, 10.3, 11.4).

``ctx.submit`` / ``ctx.submit_children`` and the lazy ``ctx.output`` tier need to
reach the engine, but ``Ctx`` must not know *how* — the io profile talks gRPC
directly; cpu/gpu profiles marshal the same verbs over a pipe (M5). Both satisfy
:class:`CtxBackend`.

Identity inheritance (AD-17) lives here, not in handler code: every job a handler
spawns inherits the parent's ``ctx_id``, ``tenant`` and ``pipeline`` unless the
handler overrides them explicitly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import grpc

from . import _json
from ._control import _Parked
from ._grpc_errors import translate
from ._proto import (
    control_plane_pb2,
    control_plane_pb2_grpc,
    data_plane_pb2,
    data_plane_pb2_grpc,
)
from .errors import SymbaError, WaitKeyAlreadyConsumed
from .job import Gate, JobHandle
from .specs import MAX_FANOUT_CHILDREN, spec_from_dict

if TYPE_CHECKING:
    from .checkpoint import CheckpointStore


class CtxBackend(Protocol):
    """What ``Ctx`` needs from its environment to run verbs (spec 10.1)."""

    async def submit(self, spec: dict[str, Any]) -> JobHandle: ...

    async def submit_children(
        self,
        children: list[dict[str, Any]],
        on_complete: dict[str, Any] | None,
        gate_policy: str,
    ) -> Gate: ...

    async def resolve_upstream(self, key: str) -> dict[str, Any] | None:
        """Lazy ``GetResult`` tier for ``ctx.output`` misses (spec 10.3 step 4)."""
        ...

    async def checkpoint(self, data: dict[str, Any]) -> None:
        """Persist a checkpoint (spec 13.1)."""
        ...

    async def wait_for_event(self, key: str, timeout_s: int) -> dict[str, Any] | None:
        """Park on an external event; may unwind the handler via ``_Parked`` (spec 14)."""
        ...


class IoCtxBackend:
    """Direct-gRPC backend for io-profile handlers (spec 10.1).

    Holds the control-plane stub (Submit/FanOut), the data-plane stub + this job's
    lease (GetResult), and the identity to inherit. One backend per execution.
    """

    def __init__(
        self,
        *,
        client_stub: control_plane_pb2_grpc.ClientServiceStub,
        worker_stub: data_plane_pb2_grpc.WorkerServiceStub,
        tenant: str,
        ctx_id: str,
        pipeline: str | None,
        job_id: str,
        lease_token: str,
        default_pipeline: str | None = None,
        checkpoint_store: CheckpointStore | None = None,
        attempt: int = 1,
        event_payload: dict[str, Any] | None = None,
        has_checkpoint: bool = False,
        logger: Any | None = None,
    ) -> None:
        self._client = client_stub
        self._worker = worker_stub
        self._tenant = tenant
        self._ctx_id = ctx_id
        self._pipeline = pipeline
        self._job_id = job_id
        self._lease_token = lease_token
        self._default_pipeline = default_pipeline
        self._checkpoint_store = checkpoint_store
        self._attempt = attempt
        self._event_payload = event_payload
        self._has_checkpoint = has_checkpoint
        self._logger = logger
        self._consumed_wait_keys: set[str] = set()

    def _inherit(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Apply ctx_id/pipeline inheritance without clobbering explicit values (AD-17)."""
        out = dict(spec)
        out.setdefault("ctx_id", self._ctx_id)
        if self._pipeline is not None:
            out.setdefault("pipeline", self._pipeline)
        return out

    async def submit(self, spec: dict[str, Any]) -> JobHandle:
        built = spec_from_dict(self._inherit(spec), default_pipeline=self._default_pipeline)
        req = control_plane_pb2.SubmitRequest(tenant=self._tenant, specs=[built])
        try:
            resp = await self._client.Submit(req)
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc
        return JobHandle(
            _DetachedEngineOps(),
            resp.job_ids[0],
            task_name=built.task_name,
            ctx_id=built.ctx_id or None,
            deduplicated=bool(resp.deduplicated and resp.deduplicated[0]),
        )

    async def submit_children(
        self,
        children: list[dict[str, Any]],
        on_complete: dict[str, Any] | None,
        gate_policy: str,
    ) -> Gate:
        if len(children) > MAX_FANOUT_CHILDREN:
            raise SymbaError(
                f"submit_children has {len(children)} children, exceeds "
                f"the {MAX_FANOUT_CHILDREN} limit"
            )
        child_specs = [
            spec_from_dict(self._inherit(c), default_pipeline=self._default_pipeline)
            for c in children
        ]
        req = control_plane_pb2.FanOutRequest(
            tenant=self._tenant,
            children=child_specs,
            gate_policy=gate_policy,
            ctx_id=self._ctx_id,
        )
        continuation_task: str | None = None
        if on_complete is not None:
            oc_spec = spec_from_dict(
                self._inherit(on_complete), default_pipeline=self._default_pipeline
            )
            req.on_complete.CopyFrom(oc_spec)
            continuation_task = oc_spec.task_name
        try:
            resp = await self._client.FanOut(req)
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc
        handles = [
            JobHandle(
                _DetachedEngineOps(), jid, task_name=child_specs[i].task_name, ctx_id=self._ctx_id
            )
            for i, jid in enumerate(resp.child_job_ids)
        ]
        return Gate(
            _DetachedEngineOps(),
            resp.gate_id,
            handles,
            ctx_id=self._ctx_id,
            continuation_task=continuation_task,
        )

    async def resolve_upstream(self, key: str) -> dict[str, Any] | None:
        req = data_plane_pb2.GetResultRequest(
            job_id=self._job_id, lease_token=self._lease_token, task_name=key
        )
        try:
            resp = await self._worker.GetResult(req)
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc
        if not resp.found:
            return None
        return _json.loads(resp.result_json)

    async def checkpoint(self, data: dict[str, Any]) -> None:
        if self._checkpoint_store is None:
            # No store => durable-only via a bare PutCheckpoint on the worker stub.
            req = data_plane_pb2.PutCheckpointRequest(
                job_id=self._job_id,
                lease_token=self._lease_token,
                checkpoint_json=_json.dumps(data),
            )
            try:
                await self._worker.PutCheckpoint(req)
            except grpc.aio.AioRpcError as exc:
                raise translate(exc) from exc
            self._has_checkpoint = True
            return
        await self._checkpoint_store.write(data)
        self._has_checkpoint = True

    async def wait_for_event(self, key: str, timeout_s: int) -> dict[str, Any] | None:
        """Issue ``Wait``; return an inline payload or unwind via ``_Parked`` (spec 14)."""
        if key in self._consumed_wait_keys:
            raise WaitKeyAlreadyConsumed(
                f"wait key {key!r} was already consumed in this execution; repeat waits "
                f"in one handler need distinct keys (suffix a step name) — spec 14.2 rule 3"
            )

        # Re-entry (rule 2): a resumed execution carries the consumed signal in the
        # assignment; the same key flows past the wait without re-parking.
        if self._event_payload is not None:
            self._consumed_wait_keys.add(key)
            return self._event_payload

        # Re-entry (rule 1): warn when a resumed execution waits with nothing checkpointed.
        if (self._attempt > 1 or self._event_payload is not None) and not self._has_checkpoint:
            if self._logger is not None:
                self._logger.warning(
                    "wait_for_event_on_resume_without_checkpoint",
                    wait_key=key,
                    attempt=self._attempt,
                    hint="checkpoint expensive pre-wait work so resume is free (spec 14.2 rule 1)",
                )

        req = data_plane_pb2.WaitRequest(
            job_id=self._job_id,
            lease_token=self._lease_token,
            wait_key=key,
            timeout_s=timeout_s,
        )
        try:
            resp = await self._worker.Wait(req)
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc

        if not resp.parked:
            # Signal-first rendezvous: consumed inline, never parked (spec 14.1).
            self._consumed_wait_keys.add(key)
            if resp.event_payload_json:
                return _json.loads(resp.event_payload_json)
            return None

        # Parked engine-side: unwind the handler cleanly; the engine owns WAITING.
        raise _Parked(key)


class _DetachedEngineOps:
    """A JobHandle/Gate returned from inside a handler is a bare identity view.

    Awaiting its ``.result()`` from within the same handler would deadlock the
    slot, so the worker-side handles are detached — callers use the returned id
    with an :class:`~symba.engine.Engine` if they need to await it. This keeps
    ``ctx.submit`` non-blocking (spec 10.1: submit returns immediately)."""

    async def _await_job(self, job_id: str, timeout: float | None) -> Any:
        raise SymbaError(
            "await a ctx.submit()'d job from an Engine client, not from inside the "
            "producing handler (that would block the worker slot)"
        )

    async def _get_status(self, job_id: str) -> Any:
        raise SymbaError("use an Engine client to inspect jobs spawned via ctx.submit")

    async def _cancel(self, job_id: str, cascade: bool) -> Any:
        raise SymbaError("use an Engine client to cancel jobs spawned via ctx.submit")

    async def _job_events(self, job_id: str) -> Any:
        raise SymbaError("use an Engine client to read events for ctx.submit jobs")

    async def _gate_status(self, gate_id: str) -> Any:
        raise SymbaError("use an Engine client to inspect gates spawned via ctx.submit_children")

    async def _gate_result(
        self,
        gate_id: str,
        ctx_id: str | None,
        continuation_task: str | None,
        timeout: float | None,
    ) -> Any:
        raise SymbaError("await a ctx.submit_children gate from an Engine client, not the handler")


__all__ = ["CtxBackend", "IoCtxBackend"]
