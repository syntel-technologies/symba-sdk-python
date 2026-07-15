"""wait_for_event mechanics + re-entry contract (spec 14)."""

from __future__ import annotations

import pytest

from symba import _json
from symba._control import _Parked
from symba.checkpoint import CheckpointStore
from symba.context import Ctx, UpstreamOutputs
from symba.ctx_backend import IoCtxBackend
from symba.errors import WaitKeyAlreadyConsumed
from symba.logging import get_logger

from ._fakes import FakeControlStub, FakeWorkerStub

pytestmark = pytest.mark.asyncio


def _backend(
    control: FakeControlStub,
    worker: FakeWorkerStub,
    *,
    attempt: int = 1,
    event_payload: dict | None = None,
    has_checkpoint: bool = False,
) -> IoCtxBackend:
    store = CheckpointStore(
        worker_stub=worker,  # type: ignore[arg-type]
        job_id="job-parent",
        lease_token="lease-parent",
        idempotency_key="idem-1",
    )
    return IoCtxBackend(
        client_stub=control,  # type: ignore[arg-type]
        worker_stub=worker,  # type: ignore[arg-type]
        tenant="acme",
        ctx_id="ctx-parent",
        pipeline="enrich",
        job_id="job-parent",
        lease_token="lease-parent",
        checkpoint_store=store,
        attempt=attempt,
        event_payload=event_payload,
        has_checkpoint=has_checkpoint,
        logger=get_logger(),
    )


def _ctx(backend: IoCtxBackend, **over) -> Ctx:
    defaults = dict(
        job_id="job-parent",
        ctx_id="ctx-parent",
        task_name="parent",
        attempt=1,
        tenant="acme",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
        backend=backend,
    )
    defaults.update(over)
    return Ctx(**defaults)  # type: ignore[arg-type]


async def test_signal_first_returns_inline_no_park():
    control, worker = FakeControlStub(), FakeWorkerStub()
    worker.wait_parked = False
    worker.wait_payload = _json.dumps({"approved": True})
    ctx = _ctx(_backend(control, worker))

    result = await ctx.wait_for_event("approve:T-1", timeout_s=60)

    assert result == {"approved": True}
    assert worker.waits[0].wait_key == "approve:T-1"


async def test_parked_raises_internal_signal():
    control, worker = FakeControlStub(), FakeWorkerStub()
    worker.wait_parked = True
    ctx = _ctx(_backend(control, worker))

    with pytest.raises(_Parked) as ei:
        await ctx.wait_for_event("approve:T-2", timeout_s=60)
    assert ei.value.wait_key == "approve:T-2"


async def test_reentry_returns_consumed_payload_without_parking():
    control, worker = FakeControlStub(), FakeWorkerStub()
    backend = _backend(
        control, worker, attempt=2, event_payload={"approved": True}, has_checkpoint=True
    )
    ctx = _ctx(backend, attempt=2, event_payload={"approved": True})

    result = await ctx.wait_for_event("approve:T-3", timeout_s=60)

    assert result == {"approved": True}
    # No Wait RPC issued — the consumed signal flowed past the wait (spec 14.2 rule 2).
    assert worker.waits == []


async def test_repeat_key_raises_already_consumed():
    control, worker = FakeControlStub(), FakeWorkerStub()
    worker.wait_parked = False
    worker.wait_payload = _json.dumps({"ok": 1})
    ctx = _ctx(_backend(control, worker))

    await ctx.wait_for_event("step-a", timeout_s=60)
    with pytest.raises(WaitKeyAlreadyConsumed):
        await ctx.wait_for_event("step-a", timeout_s=60)


async def test_timeout_resume_returns_none():
    control, worker = FakeControlStub(), FakeWorkerStub()
    worker.wait_parked = False
    worker.wait_payload = b""  # wait-timeout: no payload
    ctx = _ctx(_backend(control, worker))

    assert await ctx.wait_for_event("step-b", timeout_s=1) is None


async def test_checkpoint_verb_delegates_to_store():
    control, worker = FakeControlStub(), FakeWorkerStub()
    ctx = _ctx(_backend(control, worker))

    await ctx.checkpoint({"llm": "response"})

    assert len(worker.checkpoints) == 1
    assert _json.loads(worker.checkpoints[0].checkpoint_json) == {"llm": "response"}
