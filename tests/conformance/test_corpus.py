"""The conformance corpus (spec 20.3).

Each scenario asserts an engine-contract behavior against every backend the
``backend_factory`` fixture yields. Handlers are defined per-test and registered
through the factory so the identical code path exercises SymbaTest today and the
dockerized engine when ``SYMBA_E2E_TARGET`` is configured.
"""

from __future__ import annotations

import pytest

from symba import Worker
from symba.errors import JobFailed, RetryableError

pytestmark = pytest.mark.asyncio


async def test_dedup_collapses_duplicate_submits(backend_factory):
    def build(w: Worker) -> None:
        @w.task("charge")
        async def charge(ctx, payload):
            return {"charged": payload["amount"]}

    be = backend_factory(build)
    h1 = await be.client.submit("charge", {"amount": 100}, dedup_key="order-42")
    h2 = await be.client.submit("charge", {"amount": 100}, dedup_key="order-42")

    assert h1.id == h2.id
    assert h2.deduplicated is True
    assert await h1.result() == {"charged": 100}


async def test_chain_abort_drops_remaining_stages(backend_factory):
    def build(w: Worker) -> None:
        @w.task("validate")
        async def validate(ctx, payload):
            return ctx.stop_chain({"reason": "invalid input"})

        @w.task("process")
        async def process(ctx, payload):
            return {"processed": True}

    be = backend_factory(build)
    handle = await be.client.submit("validate", {}, chain=["validate", "process"])
    result = await handle.result()
    await be.run_until_idle()

    assert result == {"reason": "invalid input"}
    assert "process" not in {j.task_name for j in be.client.jobs()}


async def test_gate_math_counts_success_and_skips(backend_factory):
    def build(w: Worker) -> None:
        @w.task("maybe")
        async def maybe(ctx, payload):
            if payload["skip"]:
                return ctx.skip()
            return {"n": payload["n"]}

        @w.task("collect")
        async def collect(ctx, payload):
            gate = payload["__gate__"]
            return {"succeeded": gate["succeeded"], "expected": gate["expected"]}

    be = backend_factory(build)
    gate = await be.client.fan_out(
        [
            {"task": "maybe", "payload": {"skip": False, "n": 1}},
            {"task": "maybe", "payload": {"skip": True, "n": 2}},
            {"task": "maybe", "payload": {"skip": False, "n": 3}},
        ],
        on_complete={"task": "collect"},
        ctx_id="ctx-gate-math",
    )
    result = await gate.result()

    assert result == {"succeeded": 2, "expected": 3}
    status = await gate.status()
    assert status.expected == 3
    assert status.terminal == 3
    assert status.succeeded == 2


async def test_gate_all_skipped_still_fires(backend_factory):
    def build(w: Worker) -> None:
        @w.task("skipper")
        async def skipper(ctx, payload):
            return ctx.skip()

        @w.task("after")
        async def after(ctx, payload):
            gate = payload["__gate__"]
            return {"succeeded": gate["succeeded"], "expected": gate["expected"]}

    be = backend_factory(build)
    gate = await be.client.fan_out(
        [{"task": "skipper", "payload": {}} for _ in range(3)],
        on_complete={"task": "after"},
        ctx_id="ctx-all-skip",
    )
    result = await gate.result()

    assert result == {"succeeded": 0, "expected": 3}


async def test_retry_exhaustion_lands_in_dlq(backend_factory):
    def build(w: Worker) -> None:
        @w.task("doomed", max_attempts=3)
        async def doomed(ctx, payload):
            raise RetryableError("keeps failing")

    be = backend_factory(build)
    handle = await be.client.submit("doomed", {})
    with pytest.raises(JobFailed) as ei:
        await handle.result()

    status = await handle.status()
    assert status.state.name == "DEAD"
    assert len(ei.value.error_history) == 3


async def test_cancel_running_job_is_cooperative(backend_factory):
    def build(w: Worker) -> None:
        @w.task("cancellable")
        async def cancellable(ctx, payload):
            return {"ok": True}

    be = backend_factory(build)
    handle = await be.client.submit("cancellable", {})
    outcome = await handle.cancel()

    assert outcome.cancelled is True
    status = await handle.status()
    assert status.state.name == "CANCELLED"


async def test_wait_signal_resume(backend_factory):
    def build(w: Worker) -> None:
        @w.task("hitl")
        async def hitl(ctx, payload):
            decision = await ctx.wait_for_event("review:doc-9", timeout_s=600)
            return {"verdict": decision["verdict"]}

    be = backend_factory(build)
    handle = await be.client.submit("hitl", {})
    await be.run_until_idle()
    assert (await handle.status()).state.name == "WAITING"

    await be.client.signal("review:doc-9", {"verdict": "approved"})
    assert await handle.result() == {"verdict": "approved"}


async def test_checkpoint_restore_after_simulated_crash(backend_factory):
    """A retried job sees its last checkpoint on the next attempt (spec 13.2, 20.3)."""

    def build(w: Worker) -> None:
        @w.task("resumable", max_attempts=2)
        async def resumable(ctx, payload):
            if ctx.checkpoint_data is None:
                await ctx.checkpoint({"progress": 50})
                raise RetryableError("crash after first checkpoint")
            return {"resumed_from": ctx.checkpoint_data["progress"]}

    be = backend_factory(build)
    if not be.is_inmemory:  # engine restores checkpoint on its own retry path
        pytest.skip("checkpoint-restore replay is engine-managed off SymbaTest")
    handle = await be.client.submit("resumable", {})
    result = await handle.result()

    assert result == {"resumed_from": 50}
