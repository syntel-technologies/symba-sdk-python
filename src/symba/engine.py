"""Client SDK: Engine (spec 6).

The async control-plane client. Cheap to construct, lazy to connect, safe to
share across tasks within one event loop, usable as an async context manager.
Long-lived apps create one per process and keep it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from . import _json
from ._grpc_errors import translate
from ._proto import common_pb2, control_plane_pb2, control_plane_pb2_grpc
from .config import SdkSettings, load_settings
from .errors import JobCancelled, JobFailed
from .job import Gate, JobHandle
from .logging import get_logger
from .specs import MAX_FANOUT_CHILDREN, build_job_spec, spec_from_dict
from .transport import TlsConfig, Transport
from .types import CancelOutcome, GateStatus, JobEvent, JobState, JobStatus

_log = get_logger(component="engine")

#: One AwaitJob request never blocks the server longer than this; result()
#: re-issues across slices until the caller's own deadline (spec 7.1).
_AWAIT_SLICE_S = 60


def _ts_to_dt(ts: Timestamp) -> datetime | None:
    if ts.seconds == 0 and ts.nanos == 0:
        return None
    return ts.ToDatetime()


def _job_to_status(job: common_pb2.Job) -> JobStatus:
    return JobStatus(
        id=job.id,
        task_name=job.spec.task_name,
        state=JobState(job.state),
        attempt=job.attempt,
        ctx_id=job.spec.ctx_id or None,
        tenant=job.tenant or None,
        last_error=job.last_error or None,
        created_at=_ts_to_dt(job.created_at),
        started_at=_ts_to_dt(job.started_at),
        finished_at=_ts_to_dt(job.finished_at),
    )


class QueryResult:
    """Lazy async-iterable over the keyset-paginated ``Query`` cursor (spec 6.5)."""

    def __init__(self, engine: Engine, request: control_plane_pb2.QueryRequest, limit: int | None):
        self._engine = engine
        self._request = request
        self._limit = limit

    async def __aiter__(self) -> AsyncIterator[JobStatus]:
        seen = 0
        token = ""
        while True:
            req = control_plane_pb2.QueryRequest()
            req.CopyFrom(self._request)
            req.page_token = token
            resp = await self._engine._query_page(req)
            for job in resp.jobs:
                yield _job_to_status(job)
                seen += 1
                if self._limit is not None and seen >= self._limit:
                    return
            token = resp.next_page_token
            if not token:
                return

    def __await__(self):
        return self._materialize().__await__()

    async def _materialize(self) -> list[JobStatus]:
        out: list[JobStatus] = []
        async for status in self:
            out.append(status)
        return out


class Engine:
    """Async client for the Symba control plane (spec 6.2)."""

    def __init__(
        self,
        target: str | None = None,
        *,
        tenant: str = "default",
        token: str | None = None,
        tls: TlsConfig | None = None,
        default_pipeline: str | None = None,
        settings: SdkSettings | None = None,
        load_dotenv: bool = False,
    ) -> None:
        overrides: dict[str, Any] = {}
        if target is not None or token is not None:
            engine_over: dict[str, Any] = {}
            if target is not None:
                engine_over["target"] = target
            if token is not None:
                engine_over["token"] = token
            overrides["engine"] = engine_over
        self._settings = settings or load_settings(load_dotenv=load_dotenv, **overrides)
        self.tenant = tenant
        self.default_pipeline = default_pipeline
        self._transport = Transport(self._settings.engine, self._settings.grpc, tls=tls)
        self._stub: control_plane_pb2_grpc.ClientServiceStub | None = None
        self._admin: Any | None = None

    def _client(self) -> control_plane_pb2_grpc.ClientServiceStub:
        if self._stub is None:
            self._stub = control_plane_pb2_grpc.ClientServiceStub(self._transport.channel())
        return self._stub

    @property
    def admin(self) -> Any:
        """Namespaced ``AdminService`` wrapper (spec 6.7). Built lazily in M7."""
        if self._admin is None:
            from .admin import AdminClient

            self._admin = AdminClient(self._transport, self.tenant)
        return self._admin

    async def __aenter__(self) -> Engine:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._transport.aclose()

    def job(self, job_id: str) -> JobHandle:
        """Reconstruct a handle from a bare id (spec 7.1)."""
        return JobHandle(self, job_id)

    # ----------------------------------------------------------------- submit
    async def submit(self, task: str, payload: Any = None, **kwargs: Any) -> JobHandle:
        """Submit one job; returns immediately with a ``JobHandle`` (spec 6.3)."""
        spec = build_job_spec(
            task=task, payload=payload, default_pipeline=self.default_pipeline, **kwargs
        )
        req = control_plane_pb2.SubmitRequest(tenant=self.tenant, specs=[spec])
        resp = await self._call(self._client().Submit, req)
        return JobHandle(
            self,
            resp.job_ids[0],
            task_name=task,
            ctx_id=spec.ctx_id or None,
            deduplicated=bool(resp.deduplicated and resp.deduplicated[0]),
        )

    async def submit_many(self, specs: list[dict[str, Any]]) -> list[JobHandle]:
        """One ``SubmitRequest`` with n specs — all-or-nothing (spec 6.3 step 3)."""
        built = [spec_from_dict(s, default_pipeline=self.default_pipeline) for s in specs]
        req = control_plane_pb2.SubmitRequest(tenant=self.tenant, specs=built)
        resp = await self._call(self._client().Submit, req)
        handles: list[JobHandle] = []
        for i, job_id in enumerate(resp.job_ids):
            handles.append(
                JobHandle(
                    self,
                    job_id,
                    task_name=built[i].task_name,
                    ctx_id=built[i].ctx_id or None,
                    deduplicated=bool(i < len(resp.deduplicated) and resp.deduplicated[i]),
                )
            )
        return handles

    async def fan_out(
        self,
        children: list[dict[str, Any]],
        *,
        on_complete: dict[str, Any],
        gate_policy: str = "all_success",
        ctx_id: str | None = None,
    ) -> tuple[list[JobHandle], Gate]:
        """Fan out children + a gate continuation in one transaction (spec 6.4)."""
        if len(children) > MAX_FANOUT_CHILDREN:
            from .errors import SymbaError

            raise SymbaError(
                f"fan_out has {len(children)} children, exceeds {MAX_FANOUT_CHILDREN} limit"
            )
        child_specs = [spec_from_dict(c, default_pipeline=self.default_pipeline) for c in children]
        req = control_plane_pb2.FanOutRequest(
            tenant=self.tenant,
            children=child_specs,
            on_complete=spec_from_dict(on_complete, default_pipeline=self.default_pipeline),
            gate_policy=gate_policy,
            ctx_id=ctx_id or "",
        )
        continuation_task = on_complete.get("task")
        resp = await self._call(self._client().FanOut, req)
        handles = [
            JobHandle(self, jid, task_name=child_specs[i].task_name, ctx_id=ctx_id)
            for i, jid in enumerate(resp.child_job_ids)
        ]
        return handles, Gate(
            self, resp.gate_id, handles, ctx_id=ctx_id, continuation_task=continuation_task
        )

    # ------------------------------------------------------------- query/read
    def query(
        self,
        *,
        ctx_id: str | None = None,
        state: JobState | str | None = None,
        task_name: str | None = None,
        pipeline: str | None = None,
        stage: str | None = None,
        group_key: str | None = None,
        created_after: datetime | None = None,
        page_size: int = 100,
        limit: int | None = 1000,
    ) -> QueryResult:
        """Filtered, keyset-paginated query (spec 6.5). Awaitable OR async-iterable."""
        req = control_plane_pb2.QueryRequest(
            tenant=self.tenant,
            ctx_id=ctx_id or "",
            task_name=task_name or "",
            pipeline=pipeline or "",
            stage=stage or "",
            group_key=group_key or "",
            page_size=page_size,
        )
        if state is not None:
            req.state = _coerce_state(state).value
        if created_after is not None:
            ts = Timestamp()
            ts.FromDatetime(created_after)
            req.created_after.CopyFrom(ts)
        return QueryResult(self, req, limit)

    async def get_job(self, job_id: str) -> JobStatus:
        """One atomic ``GetJob`` read (spec 6.5)."""
        return await self._get_status(job_id)

    async def stream_events(
        self, *, ctx_id: str, reconnect: bool = True
    ) -> AsyncIterator[JobEvent]:
        """Live event tail by ctx_id, reconnecting on drops (spec 6.5).

        The stream is resumed after a transient drop and already-delivered events are
        suppressed via a client-side ``since`` watermark (the last event's timestamp
        plus a per-timestamp seen-set), so a reconnect never double-emits.
        """
        req = control_plane_pb2.StreamEventsRequest(tenant=self.tenant, ctx_id=ctx_id)
        stub = self._client()
        since: datetime | None = None
        seen_at_watermark: set[tuple[str, str]] = set()
        backoff = self._settings.grpc.initial_reconnect_backoff_s
        while True:
            try:
                async for ev in stub.StreamEvents(req):
                    at = _ts_to_dt(ev.at)
                    key = (ev.job_id, ev.event)
                    if since is not None and at is not None:
                        if at < since or (at == since and key in seen_at_watermark):
                            continue  # already delivered before the drop
                    if at is not None and at != since:
                        since = at
                        seen_at_watermark = set()
                    if at is not None:
                        seen_at_watermark.add(key)
                    yield JobEvent(
                        job_id=ev.job_id,
                        event=ev.event,
                        at=at,
                        detail=_json.loads(ev.detail_json) or {},
                    )
                return  # server closed the stream cleanly
            except grpc.aio.AioRpcError as exc:
                if not reconnect or exc.code() not in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.CANCELLED,
                ):
                    raise translate(exc) from exc
                await asyncio.sleep(min(backoff, self._settings.grpc.max_reconnect_backoff_s))
                backoff *= 2

    # --------------------------------------------------------------- ops verbs
    async def cancel(self, job_id: str, cascade: bool = True) -> CancelOutcome:
        return await self._cancel(job_id, cascade)

    async def resubmit(self, job_id: str) -> JobHandle:
        handles = await self.resubmit_many([job_id])
        return handles[0]

    async def resubmit_many(self, job_ids: list[str]) -> list[JobHandle]:
        req = control_plane_pb2.ResubmitRequest(tenant=self.tenant, job_ids=job_ids)
        resp = await self._call(self._client().Resubmit, req)
        return [JobHandle(self, jid) for jid in resp.job_ids]

    async def signal(self, wait_key: str, payload: Any = None, *, signaled_by: str = "") -> int:
        """Deliver a payload to a waiting job; returns delivered count (spec 6.6)."""
        req = control_plane_pb2.SignalRequest(
            tenant=self.tenant,
            wait_key=wait_key,
            payload_json=_json.dumps(payload) if payload is not None else b"",
            signaled_by=signaled_by,
        )
        resp = await self._call(self._client().Signal, req)
        return resp.delivered

    # ------------------------------------------------- private handle backends
    async def _await_job(self, job_id: str, timeout: float | None) -> Any:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            slice_s = _AWAIT_SLICE_S
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"job {job_id} did not finish within {timeout}s")
                slice_s = min(_AWAIT_SLICE_S, max(1, int(remaining)))
            req = control_plane_pb2.AwaitJobRequest(
                tenant=self.tenant, job_id=job_id, timeout_s=slice_s
            )
            job = await self._call(self._client().AwaitJob, req)
            state = JobState(job.state)
            if state == JobState.SUCCEEDED:
                return _json.loads(job.result_json)
            if state == JobState.DEAD:
                raise JobFailed(
                    job.last_error or "job failed",
                    job_id=job_id,
                    error_history=_json.loads(job.result_json) if job.result_json else [],
                )
            if state == JobState.CANCELLED:
                raise JobCancelled(job_id=job_id)
            # non-terminal: server slice expired, re-issue

    async def _get_status(self, job_id: str) -> JobStatus:
        req = control_plane_pb2.GetJobRequest(tenant=self.tenant, job_id=job_id)
        job = await self._call(self._client().GetJob, req)
        return _job_to_status(job)

    async def _cancel(self, job_id: str, cascade: bool) -> CancelOutcome:
        req = control_plane_pb2.CancelRequest(tenant=self.tenant, job_id=job_id, cascade=cascade)
        resp = await self._call(self._client().Cancel, req)
        return CancelOutcome(
            previous_state=JobState(resp.previous_state),
            cancelled=resp.cancelled,
            note=resp.note,
        )

    async def _job_events(self, job_id: str) -> list[JobEvent]:
        status = await self._get_status(job_id)
        ctx_id = status.ctx_id
        if ctx_id is None:
            return []
        events: list[JobEvent] = []
        req = control_plane_pb2.StreamEventsRequest(tenant=self.tenant, ctx_id=ctx_id)
        # A bounded read of the ledger for one job; the full live tail is stream_events.
        try:
            async for ev in self._client().StreamEvents(req):
                if ev.job_id == job_id:
                    events.append(
                        JobEvent(
                            job_id=ev.job_id,
                            event=ev.event,
                            at=_ts_to_dt(ev.at),
                            detail=_json.loads(ev.detail_json) or {},
                        )
                    )
        except grpc.aio.AioRpcError as exc:
            raise translate(exc) from exc
        return events

    async def _gate_status(self, gate_id: str) -> GateStatus:
        """Aggregate child terminal/succeeded counts for the gate (spec 7.2).

        The engine exposes no GateStatus RPC in v1; we compute it from the child
        rows the caller already holds via a group_key=gate_id Query. Callers who
        only want the continuation result use :meth:`_gate_result`.
        """
        expected = 0
        terminal = 0
        succeeded = 0
        req = control_plane_pb2.QueryRequest(tenant=self.tenant, group_key=gate_id, page_size=1000)
        async for status in QueryResult(self, req, None):
            expected += 1
            if status.state.is_terminal:
                terminal += 1
            if status.state == JobState.SUCCEEDED:
                succeeded += 1
        return GateStatus(
            gate_id=gate_id, expected=expected, terminal=terminal, succeeded=succeeded
        )

    async def _gate_result(
        self,
        gate_id: str,
        ctx_id: str | None,
        continuation_task: str | None,
        timeout: float | None,
    ) -> Any:
        """Resolve + await the continuation job the gate fires once (spec 7.2).

        The continuation job does not exist until the gate fires, so we Query by
        (ctx_id, continuation_task) with a light poll until it appears, then hand
        off to the server-side long-poll AwaitJob. Eventually-consistent by one
        dispatcher tick (documented).
        """
        if ctx_id is None or continuation_task is None:
            raise JobFailed(
                "gate.result() needs the ctx_id and continuation task; reconstruct the "
                "Gate from fan_out(...) rather than a bare id",
                job_id=gate_id,
            )
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        backoff = 0.2
        while True:
            req = control_plane_pb2.QueryRequest(
                tenant=self.tenant, ctx_id=ctx_id, task_name=continuation_task, page_size=1
            )
            resp = await self._query_page(req)
            if resp.jobs:
                remaining = None if deadline is None else max(0.0, deadline - loop.time())
                return await self._await_job(resp.jobs[0].id, remaining)
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(
                    f"gate {gate_id} continuation {continuation_task!r} did not fire "
                    f"within {timeout}s"
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 2.0)

    async def _query_page(
        self, req: control_plane_pb2.QueryRequest
    ) -> control_plane_pb2.QueryResponse:
        return await self._call(self._client().Query, req)

    async def _call(self, method: Any, request: Any) -> Any:
        """Invoke a unary RPC, translating gRPC errors and retrying UNAVAILABLE.

        Transport-level retry is a light tenacity-free loop here (spec 5.2: the
        channel handles reconnection; we only bound the visible attempts).
        """
        attempts = 0
        backoff = self._settings.grpc.initial_reconnect_backoff_s
        while True:
            try:
                return await method(request)
            except grpc.aio.AioRpcError as exc:
                if exc.code() == grpc.StatusCode.UNAVAILABLE and attempts < 4:
                    attempts += 1
                    await asyncio.sleep(min(backoff, self._settings.grpc.max_reconnect_backoff_s))
                    backoff *= 2
                    continue
                raise translate(exc) from exc


def _coerce_state(state: JobState | str) -> JobState:
    if isinstance(state, JobState):
        return state
    return JobState[state.upper()]
