"""In-process fakes for the ClientService stub (unit tests, no real gRPC)."""

from __future__ import annotations

from typing import cast

from symba._proto import common_pb2, control_plane_pb2, data_plane_pb2
from symba.types import JobState


def make_job(
    job_id: str,
    state: JobState,
    *,
    task_name: str = "t",
    attempt: int = 1,
    result: bytes = b"",
    last_error: str = "",
    error_history_json: bytes = b"",
) -> common_pb2.Job:
    job = common_pb2.Job(
        id=job_id,
        state=cast("common_pb2.JobState", state.value),
        attempt=attempt,
        result_json=result,
        last_error=last_error,
        error_history_json=error_history_json,
    )
    job.spec.task_name = task_name
    return job


class _AsyncStreamOf:
    def __init__(self, items: list[common_pb2.JobEvent]):
        self._items = items

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for item in self._items:
            yield item


class FakeClientStub:
    """Minimal stand-in for ``ClientServiceStub``. Each method is an async callable."""

    def __init__(self) -> None:
        self.submit_job_ids: list[str] = []
        self.submit_dedup: list[bool] = []
        self.await_job: common_pb2.Job | None = None
        self.get_job: common_pb2.Job | None = None
        self.cancel_previous_state: JobState = JobState.RUNNING
        self.cancel_cancelled: bool = True
        self.signal_delivered: int = 0
        self.query_pages: list[tuple[list[str], str]] = []
        self.events: list[common_pb2.JobEvent] = []
        # captured requests
        self.last_submit: control_plane_pb2.SubmitRequest | None = None
        self.last_cancel: control_plane_pb2.CancelRequest | None = None
        self.last_signal: control_plane_pb2.SignalRequest | None = None

    async def Submit(self, req: control_plane_pb2.SubmitRequest):
        self.last_submit = req
        return control_plane_pb2.SubmitResponse(
            job_ids=self.submit_job_ids, deduplicated=self.submit_dedup
        )

    async def AwaitJob(self, req: control_plane_pb2.AwaitJobRequest):
        assert self.await_job is not None
        return self.await_job

    async def GetJob(self, req: control_plane_pb2.GetJobRequest):
        assert self.get_job is not None
        return self.get_job

    async def Cancel(self, req: control_plane_pb2.CancelRequest):
        self.last_cancel = req
        return control_plane_pb2.CancelResponse(
            previous_state=self.cancel_previous_state.value,
            cancelled=self.cancel_cancelled,
        )

    async def Signal(self, req: control_plane_pb2.SignalRequest):
        self.last_signal = req
        return control_plane_pb2.SignalResponse(delivered=self.signal_delivered)

    async def Query(self, req: control_plane_pb2.QueryRequest):
        # pop the page matching the incoming token position
        idx = 0
        if req.page_token:
            idx = int(req.page_token.removeprefix("tok"))
        job_ids, next_token = self.query_pages[idx]
        jobs = [make_job(jid, JobState.QUEUED) for jid in job_ids]
        return control_plane_pb2.QueryResponse(jobs=jobs, next_page_token=next_token)

    def StreamEvents(self, req: control_plane_pb2.StreamEventsRequest):
        return _AsyncStreamOf(self.events)


def make_assignment(
    job_id: str,
    *,
    task_name: str,
    payload: bytes = b"",
    lease_token: str = "lease-1",
    attempt: int = 1,
    tenant: str = "default",
) -> data_plane_pb2.JobAssignment:
    job = common_pb2.Job(
        id=job_id,
        tenant=tenant,
        state=cast("common_pb2.JobState", JobState.RUNNING.value),
        attempt=attempt,
    )
    job.spec.task_name = task_name
    if payload:
        job.spec.payload_json = payload
    return data_plane_pb2.JobAssignment(job=job, lease_token=lease_token)


class FakeWorkerStub:
    """Stand-in for ``WorkerServiceStub``: records Complete/Fail, never cancels."""

    def __init__(self) -> None:
        self.completes: list[data_plane_pb2.CompleteRequest] = []
        self.fails: list[data_plane_pb2.FailRequest] = []
        self.heartbeats: int = 0
        #: task_name -> result bytes for GetResult (lazy upstream tier).
        self.results: dict[str, bytes] = {}
        self.last_get_result: data_plane_pb2.GetResultRequest | None = None
        #: recorded checkpoints and wait requests (M4).
        self.checkpoints: list[data_plane_pb2.PutCheckpointRequest] = []
        self.waits: list[data_plane_pb2.WaitRequest] = []
        #: Wait behaviour: parked=True => job parks; else the payload is returned inline.
        self.wait_parked: bool = True
        self.wait_payload: bytes = b""

    async def Heartbeat(self, req: data_plane_pb2.HeartbeatRequest):
        self.heartbeats += 1
        return data_plane_pb2.HeartbeatResponse(cancelled=False)

    async def Complete(self, req: data_plane_pb2.CompleteRequest):
        self.completes.append(req)
        return data_plane_pb2.CompleteResponse(accepted=True)

    async def Fail(self, req: data_plane_pb2.FailRequest):
        self.fails.append(req)
        return data_plane_pb2.FailResponse(accepted=True, will_retry=req.retryable)

    async def GetResult(self, req: data_plane_pb2.GetResultRequest):
        self.last_get_result = req
        raw = self.results.get(req.task_name)
        if raw is None:
            return data_plane_pb2.GetResultResponse(found=False)
        return data_plane_pb2.GetResultResponse(result_json=raw, found=True)

    async def PutCheckpoint(self, req: data_plane_pb2.PutCheckpointRequest):
        self.checkpoints.append(req)
        return data_plane_pb2.PutCheckpointResponse(accepted=True)

    async def Wait(self, req: data_plane_pb2.WaitRequest):
        self.waits.append(req)
        return data_plane_pb2.WaitResponse(
            parked=self.wait_parked, event_payload_json=self.wait_payload
        )


class FakeControlStub:
    """Stand-in for ``ClientServiceStub`` covering Submit + FanOut (ctx verbs)."""

    def __init__(self) -> None:
        self.submits: list[control_plane_pb2.SubmitRequest] = []
        self.fanouts: list[control_plane_pb2.FanOutRequest] = []
        self.submit_job_ids: list[str] = ["child-0"]
        self.gate_id: str = "gate-0"
        self.child_job_ids: list[str] = ["c1", "c2"]

    async def Submit(self, req: control_plane_pb2.SubmitRequest):
        self.submits.append(req)
        return control_plane_pb2.SubmitResponse(
            job_ids=self.submit_job_ids, deduplicated=[False] * len(self.submit_job_ids)
        )

    async def FanOut(self, req: control_plane_pb2.FanOutRequest):
        self.fanouts.append(req)
        return control_plane_pb2.FanOutResponse(
            child_job_ids=self.child_job_ids, gate_id=self.gate_id
        )


class FakeRedis:
    """Minimal async Redis stand-in for the checkpoint fast path (spec 13.1)."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.deleted: list[str] = []

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        self.store[key] = value

    async def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.store.pop(key, None)
