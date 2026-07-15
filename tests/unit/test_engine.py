"""Engine client tests against a fake in-process stub (spec 6, 7)."""

from __future__ import annotations

from datetime import UTC, datetime

import grpc
import pytest

from symba import _json
from symba._proto import common_pb2
from symba.engine import Engine
from symba.errors import JobCancelled, JobFailed
from symba.types import JobState

from ._fakes import FakeClientStub, make_job

pytestmark = pytest.mark.asyncio


@pytest.fixture
def engine_with_fake():
    engine = Engine("grpc://localhost:7233", tenant="acme")
    fake = FakeClientStub()
    engine._stub = fake  # type: ignore[assignment]
    return engine, fake


async def test_submit_returns_handle(engine_with_fake):
    engine, fake = engine_with_fake
    fake.submit_job_ids = ["job-1"]
    handle = await engine.submit("download", payload={"doc": 1}, ctx_id="ctx-1")
    assert handle.id == "job-1"
    assert handle.task_name == "download"
    assert handle.ctx_id == "ctx-1"
    assert handle.deduplicated is False
    # the request the engine saw
    assert fake.last_submit.tenant == "acme"
    assert fake.last_submit.specs[0].task_name == "download"


async def test_submit_dedup_flag(engine_with_fake):
    engine, fake = engine_with_fake
    fake.submit_job_ids = ["job-1"]
    fake.submit_dedup = [True]
    handle = await engine.submit("download")
    assert handle.deduplicated is True


async def test_submit_many_all_or_nothing(engine_with_fake):
    engine, fake = engine_with_fake
    fake.submit_job_ids = ["a", "b"]
    handles = await engine.submit_many([{"task": "x"}, {"task": "y"}])
    assert [h.id for h in handles] == ["a", "b"]
    assert len(fake.last_submit.specs) == 2


async def test_result_success_deserializes(engine_with_fake):
    engine, fake = engine_with_fake
    fake.await_job = make_job("job-1", JobState.SUCCEEDED, result=_json.dumps({"ok": True}))
    handle = engine.job("job-1")
    assert await handle.result() == {"ok": True}


async def test_result_dead_raises_jobfailed(engine_with_fake):
    engine, fake = engine_with_fake
    history = [{"error_type": "ValueError", "attempt": 1}]
    fake.await_job = make_job(
        "job-1", JobState.DEAD, result=_json.dumps(history), last_error="boom"
    )
    handle = engine.job("job-1")
    with pytest.raises(JobFailed) as exc:
        await handle.result()
    assert exc.value.job_id == "job-1"
    assert exc.value.error_history == history


async def test_result_cancelled_raises(engine_with_fake):
    engine, fake = engine_with_fake
    fake.await_job = make_job("job-1", JobState.CANCELLED)
    with pytest.raises(JobCancelled):
        await engine.job("job-1").result()


async def test_status_maps_fields(engine_with_fake):
    engine, fake = engine_with_fake
    fake.get_job = make_job("job-1", JobState.RUNNING, task_name="parse", attempt=2)
    status = await engine.get_job("job-1")
    assert status.state == JobState.RUNNING
    assert status.task_name == "parse"
    assert status.attempt == 2


async def test_cancel_outcome(engine_with_fake):
    engine, fake = engine_with_fake
    fake.cancel_previous_state = JobState.RUNNING
    fake.cancel_cancelled = True
    outcome = await engine.cancel("job-1", cascade=True)
    assert outcome.cancelled is True
    assert outcome.previous_state == JobState.RUNNING
    assert fake.last_cancel.cascade is True


async def test_signal_returns_delivered(engine_with_fake):
    engine, fake = engine_with_fake
    fake.signal_delivered = 2
    n = await engine.signal("approve:1", {"ok": True}, signaled_by="me")
    assert n == 2
    assert _json.loads(fake.last_signal.payload_json) == {"ok": True}


async def test_query_paginates(engine_with_fake):
    engine, fake = engine_with_fake
    fake.query_pages = [
        (["a", "b"], "tok1"),
        (["c"], ""),
    ]
    results = await engine.query(ctx_id="ctx-1")
    assert [r.id for r in results] == ["a", "b", "c"]


async def test_query_respects_limit(engine_with_fake):
    engine, fake = engine_with_fake
    fake.query_pages = [(["a", "b", "c"], "tok1")]
    results = await engine.query(ctx_id="ctx-1", limit=2)
    assert [r.id for r in results] == ["a", "b"]


def _event(job_id: str, name: str, at: datetime, detail: dict | None = None) -> common_pb2.JobEvent:
    return common_pb2.JobEvent(
        job_id=job_id,
        event=name,
        at=at,
        detail_json=_json.dumps(detail or {}),
    )


class _DroppingEventStub:
    """StreamEvents that drops mid-stream once, then resumes from the full log.

    Mirrors the engine replaying its persisted event log from the start on a
    reconnect: the client must suppress events it already delivered before the drop.
    """

    def __init__(self, events: list[common_pb2.JobEvent]) -> None:
        self._events = events
        self.calls = 0

    def StreamEvents(self, req):
        self.calls += 1
        first_call = self.calls == 1
        events = self._events

        async def _gen():
            for i, ev in enumerate(events):
                if first_call and i == 1:
                    from grpc.aio import Metadata

                    raise grpc.aio.AioRpcError(
                        grpc.StatusCode.UNAVAILABLE, Metadata(), Metadata(), details="drop"
                    )
                yield ev

        return _gen()


async def test_stream_events_reconnect_dedups(monkeypatch):
    engine = Engine("grpc://localhost:7233", tenant="acme")
    # make reconnect backoff instant so the test doesn't sleep
    engine._settings.grpc.initial_reconnect_backoff_s = 0.0
    engine._settings.grpc.max_reconnect_backoff_s = 0.0

    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    t2 = datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)
    events = [
        _event("job-1", "queued", t0),
        _event("job-1", "running", t1),
        _event("job-1", "succeeded", t2),
    ]
    stub = _DroppingEventStub(events)
    engine._stub = stub  # type: ignore[assignment]

    seen = [ev.event async for ev in engine.stream_events(ctx_id="ctx-1")]

    # the drop happens after the first event; the resume replays from the start,
    # but the client watermark suppresses the already-delivered "queued".
    assert seen == ["queued", "running", "succeeded"]
    assert stub.calls == 2
