"""In-memory gRPC stub stand-ins backing :class:`SymbaTest` (spec 20.1).

The real :class:`~symba.dispatch.Dispatcher` and the ctx backends talk to these
exactly as they would to gRPC stubs — same method names, same request/response
protos — so the production pipeline runs unmodified. Only the wire is faked.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from symba._proto import common_pb2, control_plane_pb2, data_plane_pb2
from symba.types import JobState

if TYPE_CHECKING:
    from symba.testing.fake_engine import SymbaTest


class MemWorkerStub:
    """Data-plane stub over the store: Complete/Fail/Heartbeat/GetResult/PutCheckpoint/Wait."""

    def __init__(self, sim: SymbaTest) -> None:
        self._sim = sim

    async def Heartbeat(self, req: data_plane_pb2.HeartbeatRequest):
        cancelled = req.job_id in self._sim._cancel_requested
        return data_plane_pb2.HeartbeatResponse(cancelled=cancelled)

    async def Complete(self, req: data_plane_pb2.CompleteRequest):
        self._sim._on_complete(
            req.job_id,
            req.result_json,
            drop_chain_tail=req.drop_chain_tail,
            skipped=req.skipped,
        )
        return data_plane_pb2.CompleteResponse(accepted=True)

    async def Fail(self, req: data_plane_pb2.FailRequest):
        will_retry = self._sim._on_fail(
            req.job_id,
            error_type=req.error_type,
            error_message=req.error_message,
            retryable=req.retryable,
        )
        return data_plane_pb2.FailResponse(accepted=True, will_retry=will_retry)

    async def GetResult(self, req: data_plane_pb2.GetResultRequest):
        raw = self._sim._resolve_result(req.task_name)
        if raw is None:
            return data_plane_pb2.GetResultResponse(found=False)
        return data_plane_pb2.GetResultResponse(result_json=raw, found=True)

    async def PutCheckpoint(self, req: data_plane_pb2.PutCheckpointRequest):
        self._sim._store.checkpoints[req.job_id] = req.checkpoint_json
        return data_plane_pb2.PutCheckpointResponse(accepted=True)

    async def Wait(self, req: data_plane_pb2.WaitRequest):
        payload = self._sim._try_consume_signal(req.wait_key)
        if payload is not None:
            return data_plane_pb2.WaitResponse(parked=False, event_payload_json=payload)
        self._sim._park(req.job_id, req.wait_key)
        return data_plane_pb2.WaitResponse(parked=True)


class MemClientStub:
    """Control-plane stub over the store: Submit/FanOut/Query/GetJob/AwaitJob/Cancel/Signal."""

    def __init__(self, sim: SymbaTest) -> None:
        self._sim = sim

    async def Submit(self, req: control_plane_pb2.SubmitRequest):
        job_ids: list[str] = []
        deduped: list[bool] = []
        for spec in req.specs:
            jid, was_dedup = self._sim._enqueue(spec, tenant=req.tenant)
            job_ids.append(jid)
            deduped.append(was_dedup)
        return control_plane_pb2.SubmitResponse(job_ids=job_ids, deduplicated=deduped)

    async def FanOut(self, req: control_plane_pb2.FanOutRequest):
        gate_id, child_ids = self._sim._fan_out(
            list(req.children),
            on_complete=req.on_complete if req.HasField("on_complete") else None,
            gate_policy=req.gate_policy or "all_success",
            ctx_id=req.ctx_id,
            tenant=req.tenant,
        )
        return control_plane_pb2.FanOutResponse(child_job_ids=child_ids, gate_id=gate_id)

    async def Query(self, req: control_plane_pb2.QueryRequest):
        jobs = self._sim._query(
            ctx_id=req.ctx_id or None,
            task_name=req.task_name or None,
            state=JobState(req.state) if req.state else None,
            group_key=req.group_key or None,
        )
        return control_plane_pb2.QueryResponse(jobs=jobs, next_page_token="")

    async def GetJob(self, req: control_plane_pb2.GetJobRequest):
        return self._sim._job_proto(req.job_id)

    async def AwaitJob(self, req: control_plane_pb2.AwaitJobRequest):
        await self._sim._run_until_job_terminal(req.job_id, req.timeout_s)
        return self._sim._job_proto(req.job_id)

    async def Cancel(self, req: control_plane_pb2.CancelRequest):
        prev, cancelled = self._sim._do_cancel(req.job_id, cascade=req.cascade)
        return control_plane_pb2.CancelResponse(previous_state=prev.value, cancelled=cancelled)

    async def Signal(self, req: control_plane_pb2.SignalRequest):
        delivered = self._sim._signal(req.wait_key, req.payload_json)
        return control_plane_pb2.SignalResponse(delivered=delivered)


def blank_job(job_id: str) -> common_pb2.Job:
    """A JOB_STATE_UNSPECIFIED placeholder for unknown ids (mirrors engine GetJob)."""
    return common_pb2.Job(id=job_id, state=common_pb2.JOB_STATE_UNSPECIFIED)


__all__ = ["MemWorkerStub", "MemClientStub", "blank_job"]
