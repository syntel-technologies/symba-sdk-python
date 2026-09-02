"""Unit tests for SymbaTest — the in-memory engine (spec 20).

These verify the harness itself: the real dispatch pipeline runs, and the faked
engine bookkeeping (chains, gates, retries, wait/signal, checkpoints, dedup,
cancellation) behaves like the production engine's public contract.
"""

from __future__ import annotations

from typing import Any

import pytest

from symba import Worker
from symba.errors import JobFailed, RetryableError
from symba.testing import SymbaTest

pytestmark = pytest.mark.asyncio


def _worker() -> Worker:
    return Worker(engine="inmemory://test")


async def test_submit_and_result_runs_real_pipeline():
    w = _worker()

    @w.task("double")
    async def double(ctx, payload):
        return {"out": payload["n"] * 2}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("double", {"n": 21})
    result = await handle.result()

    assert result == {"out": 42}


async def test_chain_threads_upstream_result():
    w = _worker()

    @w.task("first")
    async def first(ctx, payload):
        return {"value": payload["seed"] + 1}

    @w.task("second")
    async def second(ctx, payload):
        upstream = await ctx.output.fetch("first")
        return {"value": upstream["value"] * 10}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("first", {"seed": 4}, chain=["first", "second"])
    # the head runs `first`; the tail runs `second`. result() awaits the head job.
    await handle.result()
    sim_jobs = {j.task_name: j for j in sim.jobs()}
    second_job = sim_jobs["second"]
    tail_result = await (await sim.get(second_job.id)).result()
    assert tail_result == {"value": 50}


async def test_stop_chain_drops_tail():
    w = _worker()

    @w.task("gate")
    async def gate(ctx, payload):
        return ctx.stop_chain({"halted": True})

    @w.task("never")
    async def never(ctx, payload):
        return {"ran": True}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("gate", {}, chain=["gate", "never"])
    await handle.result()
    await sim.run_until_idle()
    tasks = {j.task_name for j in sim.jobs()}
    assert "never" not in tasks


async def test_retry_then_success_with_compressed_backoff():
    w = _worker()
    attempts = {"n": 0}

    @w.task("flaky", max_attempts=3)
    async def flaky(ctx, payload):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RetryableError("transient")
        return {"ok": attempts["n"]}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("flaky", {})
    result = await handle.result()
    assert result == {"ok": 2}


async def test_retry_exhaustion_raises_job_failed():
    w = _worker()

    @w.task("always_bad", max_attempts=2)
    async def always_bad(ctx, payload):
        raise RetryableError("still broken")

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("always_bad", {})
    with pytest.raises(JobFailed) as ei:
        await handle.result()
    assert len(ei.value.error_history) == 2


async def test_fail_next_forces_one_retryable_failure():
    w = _worker()
    runs = {"n": 0}

    @w.task("counted", max_attempts=3)
    async def counted(ctx, payload):
        runs["n"] += 1
        return {"run": runs["n"]}

    sim = SymbaTest()
    sim.register(w)
    sim.fail_next("counted")

    handle = await sim.submit("counted", {})
    result = await handle.result()
    # first attempt was forced to fail; the retry succeeded on the second run.
    assert result == {"run": 2}


async def test_dedup_returns_same_job():
    w = _worker()

    @w.task("idem")
    async def idem(ctx, payload):
        return {"ok": True}

    sim = SymbaTest()
    sim.register(w)

    h1 = await sim.submit("idem", {}, dedup_key="k1")
    h2 = await sim.submit("idem", {}, dedup_key="k1")
    assert h1.id == h2.id
    assert h2.deduplicated is True


async def test_fan_out_gate_fires_continuation():
    w = _worker()

    @w.task("child")
    async def child(ctx, payload):
        return {"squared": payload["x"] ** 2}

    @w.task("reduce")
    async def reduce(ctx, payload):
        manifest = payload["__gate__"]
        total = sum(r["result"]["squared"] for r in manifest["results"])
        return {"total": total, "succeeded": manifest["succeeded"]}

    sim = SymbaTest()
    sim.register(w)

    gate = await sim.fan_out(
        [{"task": "child", "payload": {"x": x}} for x in (1, 2, 3)],
        on_complete={"task": "reduce"},
        ctx_id="ctx-fan",
    )
    result = await gate.result()
    assert result == {"total": 14, "succeeded": 3}


async def test_wait_for_event_resumes_on_signal():
    w = _worker()

    @w.task("approver")
    async def approver(ctx, payload):
        decision = await ctx.wait_for_event("approve:doc-1", timeout_s=300)
        return {"approved": decision["ok"]}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("approver", {})
    await sim.run_until_idle()  # runs until the job parks on the wait
    status = await handle.status()
    assert status.state.name == "WAITING"

    delivered = await sim.signal("approve:doc-1", {"ok": True})
    assert delivered == 1
    result = await handle.result()
    assert result == {"approved": True}


async def test_checkpoint_is_captured():
    w = _worker()

    @w.task("ckpt")
    async def ckpt(ctx, payload):
        await ctx.checkpoint({"stage": "half", "n": payload["n"]})
        return {"done": True}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("ckpt", {"n": 7})
    await handle.result()
    assert sim.checkpoints[handle.id] == {"stage": "half", "n": 7}


async def test_gate_continuation_preserves_payload_and_adds_manifest():
    """SDK-2: the caller on_complete payload survives; the manifest is under __gate__."""
    w = _worker()

    @w.task("child")
    async def child(ctx, payload):
        return {"x": payload["x"]}

    seen: dict[str, Any] = {}

    @w.task("assemble")
    async def assemble(ctx, payload):
        seen.update(payload)
        return {"ok": True}

    sim = SymbaTest()
    sim.register(w)

    gate = await sim.fan_out(
        [{"task": "child", "payload": {"x": x}} for x in (1, 2)],
        on_complete={"task": "assemble", "payload": {"document_id": "d1"}},
        ctx_id="ctx-payload",
    )
    await gate.result()

    assert seen["document_id"] == "d1"  # caller payload preserved
    assert seen["__gate__"]["expected"] == 2
    assert seen["__gate__"]["succeeded"] == 2


async def test_all_success_gate_blocks_continuation_on_dead_child():
    """FE-1 + FE-2: a DEAD child under all_success blocks the continuation."""
    w = _worker()

    @w.task("ocr_page", max_attempts=3)
    async def ocr_page(ctx, payload):
        return {"page": payload["page"]}

    @w.task("assemble")
    async def assemble(ctx, payload):
        return {"assembled": True}

    sim = SymbaTest()
    sim.register(w)
    sim.fail_always("ocr_page")  # drive every ocr_page to DEAD

    gate = await sim.fan_out(
        [{"task": "ocr_page", "payload": {"page": p}} for p in range(3)],
        on_complete={"task": "assemble", "payload": {"document_id": "d1"}},
        gate_policy="all_success",
        ctx_id="ctx-dead",
    )
    await sim.run_until_idle()

    tasks = {j.task_name for j in sim.jobs()}
    assert "assemble" not in tasks  # continuation blocked
    with pytest.raises(JobFailed):
        await gate.result()


async def test_all_terminal_gate_fires_despite_dead_child():
    """FE-1: all_terminal fires the continuation once every child is terminal."""
    w = _worker()

    @w.task("maybe", max_attempts=1)
    async def maybe(ctx, payload):
        if payload["die"]:
            raise RetryableError("boom")
        return {"ok": True}

    @w.task("after")
    async def after(ctx, payload):
        return {"terminal": payload["__gate__"]["expected"]}

    sim = SymbaTest()
    sim.register(w)

    gate = await sim.fan_out(
        [{"task": "maybe", "payload": {"die": d}} for d in (False, True)],
        on_complete={"task": "after"},
        gate_policy="all_terminal",
        ctx_id="ctx-terminal",
    )
    result = await gate.result()
    assert result == {"terminal": 2}


async def test_fail_next_non_retryable_drives_dead():
    """FE-2: fail_next(retryable=False) drives a task to DEAD in one attempt."""
    w = _worker()

    @w.task("once", max_attempts=3)
    async def once(ctx, payload):
        return {"ok": True}

    sim = SymbaTest()
    sim.register(w)
    sim.fail_next("once", retryable=False)

    handle = await sim.submit("once", {})
    with pytest.raises(JobFailed):
        await handle.result()
    status = await handle.status()
    assert status.state.name == "DEAD"


async def test_fail_always_exhausts_attempts_to_dead():
    """FE-2: fail_always exhausts max_attempts and reaches DEAD."""
    w = _worker()

    @w.task("triple", max_attempts=3)
    async def triple(ctx, payload):
        return {"ok": True}

    sim = SymbaTest()
    sim.register(w)
    sim.fail_always("triple")

    handle = await sim.submit("triple", {})
    with pytest.raises(JobFailed) as ei:
        await handle.result()
    assert len(ei.value.error_history) == 3


async def test_cancel_queued_job():
    w = _worker()

    @w.task("slow")
    async def slow(ctx, payload):
        return {"ok": True}

    sim = SymbaTest()
    sim.register(w)

    handle = await sim.submit("slow", {})
    outcome = await handle.cancel()
    assert outcome.cancelled is True
    status = await handle.status()
    assert status.state.name == "CANCELLED"
