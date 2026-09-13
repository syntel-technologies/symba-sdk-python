"""ctx.submit / ctx.submit_children inheritance + lazy GetResult (spec 10.1, 10.3)."""

from __future__ import annotations

import pytest

from symba import _json
from symba.context import Ctx, UpstreamOutputs
from symba.ctx_backend import IoCtxBackend
from symba.logging import get_logger

from ._fakes import FakeControlStub, FakeWorkerStub

pytestmark = pytest.mark.asyncio


def _backend(control: FakeControlStub, worker: FakeWorkerStub) -> IoCtxBackend:
    return IoCtxBackend(
        client_stub=control,  # type: ignore[arg-type]
        worker_stub=worker,  # type: ignore[arg-type]
        tenant="acme",
        ctx_id="ctx-parent",
        pipeline="enrich",
        job_id="job-parent",
        lease_token="lease-parent",
    )


def _ctx(backend: IoCtxBackend) -> Ctx:
    return Ctx(
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


async def test_submit_inherits_ctx_and_pipeline():
    control, worker = FakeControlStub(), FakeWorkerStub()
    ctx = _ctx(_backend(control, worker))

    handle = await ctx.submit(task="child")

    assert handle.id == "child-0"
    spec = control.submits[0].specs[0]
    assert spec.ctx_id == "ctx-parent"
    assert spec.pipeline == "enrich"


async def test_submit_explicit_ctx_id_wins():
    control, worker = FakeControlStub(), FakeWorkerStub()
    ctx = _ctx(_backend(control, worker))

    await ctx.submit(task="child", ctx_id="override")

    assert control.submits[0].specs[0].ctx_id == "override"


async def test_submit_children_builds_gate():
    control, worker = FakeControlStub(), FakeWorkerStub()
    ctx = _ctx(_backend(control, worker))

    gate = await ctx.submit_children(
        [{"task": "leaf", "payload": {"i": i}} for i in range(3)],
        on_complete={"task": "reduce"},
    )

    assert gate.id == "gate-0"
    assert gate.continuation_task == "reduce"
    fan = control.fanouts[0]
    assert len(fan.children) == 3
    assert fan.ctx_id == "ctx-parent"
    assert fan.on_complete.task_name == "reduce"
    # children inherit pipeline
    assert all(c.pipeline == "enrich" for c in fan.children)


async def test_lazy_upstream_via_getresult():
    control, worker = FakeControlStub(), FakeWorkerStub()
    worker.results["deep_ancestor"] = _json.dumps({"value": 99})
    backend = _backend(control, worker)

    out = UpstreamOutputs([], lazy_fetch=backend.resolve_upstream)
    assert await out.fetch("deep_ancestor") == {"value": 99}
    assert worker.last_get_result is not None
    assert worker.last_get_result.task_name == "deep_ancestor"
    assert worker.last_get_result.job_id == "job-parent"


async def test_lazy_upstream_not_found_is_keyerror():
    control, worker = FakeControlStub(), FakeWorkerStub()
    backend = _backend(control, worker)

    out = UpstreamOutputs([], lazy_fetch=backend.resolve_upstream)
    with pytest.raises(KeyError):
        await out.fetch("absent")
