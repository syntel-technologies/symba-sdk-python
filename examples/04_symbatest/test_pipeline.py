"""Testing with SymbaTest — the real dispatch pipeline, in-memory.

    pytest examples/04_symbatest/test_pipeline.py

No gRPC, no Postgres, no Redis: SymbaTest runs your handlers through the actual
dispatch code against an in-memory job table, so chains, retries, gates, waits,
checkpoints, and dedup all behave as in production.
"""

from __future__ import annotations

import pytest

from symba import Ctx, Worker
from symba.errors import RetryableError
from symba.testing import SymbaTest


def _build_worker() -> Worker:
    worker = Worker(engine="inmemory://test")

    @worker.task("parse_content")
    async def parse_content(ctx: Ctx, payload: dict):
        if payload.get("duplicate"):
            return ctx.stop_chain({"reason": "duplicate_content"})
        return {"chunk_refs": ["a", "b"], "content_hash": "h1"}

    @worker.task("fan_out_summaries")
    async def fan_out_summaries(ctx: Ctx, payload: dict):
        return {"ok": True}

    @worker.task("summarize_chunk", max_attempts=3)
    async def summarize_chunk(ctx: Ctx, payload: dict):
        if ctx.checkpoint_data:
            return {"summary_ref": ctx.checkpoint_data["summary_ref"]}
        await ctx.checkpoint({"summary_ref": f"sum::{payload['chunk_ref']}"})
        return {"summary_ref": f"sum::{payload['chunk_ref']}"}

    return worker


@pytest.mark.asyncio
async def test_duplicate_content_stops_chain():
    async with SymbaTest() as sim:
        sim.register(_build_worker())
        # chain includes the head task, then its continuation(s).
        h = await sim.submit(
            "parse_content",
            {"document_id": "d1", "staging_ref": "s3://x", "duplicate": True},
            chain=["parse_content", "fan_out_summaries"],
        )
        result = await h.result(timeout=5)
        assert result == {"reason": "duplicate_content"}
        # the chain tail never ran
        assert "fan_out_summaries" not in {j.task_name for j in sim.jobs()}


@pytest.mark.asyncio
async def test_retry_uses_checkpoint():
    async with SymbaTest() as sim:
        sim.register(_build_worker())
        sim.fail_next("summarize_chunk")  # attempt 1 fails after the checkpoint write
        h = await sim.submit("summarize_chunk", {"chunk_ref": "c1"})
        result = await h.result(timeout=5)
        assert result == {"summary_ref": "sum::c1"}
        # the checkpoint from attempt 1 was visible to attempt 2
        assert sim.checkpoints[h.id] == {"summary_ref": "sum::c1"}


@pytest.mark.asyncio
async def test_retryable_error_is_retried():
    worker = Worker(engine="inmemory://test")
    attempts = {"n": 0}

    @worker.task("flaky", max_attempts=3)
    async def flaky(ctx: Ctx, payload: dict):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RetryableError("blip")
        return {"ok": attempts["n"]}

    async with SymbaTest() as sim:
        sim.register(worker)
        h = await sim.submit("flaky", {})
        assert await h.result(timeout=5) == {"ok": 2}
