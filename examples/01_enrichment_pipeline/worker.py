"""Enrichment pipeline — chain + fan-out/gate + schemas + checkpoint.

Run the worker:

    symba run examples.01_enrichment_pipeline.worker:worker
    # or: python -m examples.01_enrichment_pipeline.worker

Then submit the flow with ``submit.py`` in this directory. Needs an engine on
``grpc://localhost:7233`` (``docker compose up`` in the engine repo). The external
helpers (``parse``, ``stage``, ``llm``) are stubbed so the file runs as-is; swap in
your real implementations.
"""

from __future__ import annotations

import hashlib

from pydantic import BaseModel

from symba import Ctx, Worker

# strict_schemas would require EVERY task to declare both input_schema and
# output_schema; this pipeline only types parse_content (to show the pattern), so
# it stays off. Flip it on once all your tasks carry schemas.
worker = Worker(engine="grpc://localhost:7233", tags=["llm"], slots=100)


class ParseInput(BaseModel):
    document_id: str
    staging_ref: str


class ParseOutput(BaseModel):
    document_id: str  # threaded downstream: a chained tail starts with an EMPTY payload
    chunk_refs: list[str]
    content_hash: str


# --- stubbed externals (replace with real I/O) -------------------------------
async def parse(staging_ref: str) -> list[str]:
    return [f"{staging_ref}#chunk-{i}" for i in range(3)]


async def stage(chunks: list[str]) -> list[str]:
    return [f"staged://{c}" for c in chunks]


def hash_of(chunks: list[str]) -> str:
    return hashlib.sha256("".join(chunks).encode()).hexdigest()[:16]


async def already_processed(document_id: str, content_hash: str) -> bool:
    return False


async def summarize(chunk_ref: str) -> str:
    return f"summary-of::{chunk_ref}"


# --- handlers ----------------------------------------------------------------
@worker.task("parse_content", input_schema=ParseInput, output_schema=ParseOutput)
async def parse_content(ctx: Ctx, payload: ParseInput) -> ParseOutput:
    chunks = await parse(payload.staging_ref)
    content_hash = hash_of(chunks)
    if await already_processed(payload.document_id, content_hash):
        return ctx.stop_chain({"reason": "duplicate_content"})  # drop the chain tail
    return ParseOutput(
        document_id=payload.document_id,
        chunk_refs=await stage(chunks),
        content_hash=content_hash,
    )


@worker.task("summarize_chunk")
async def summarize_chunk(ctx: Ctx, payload: dict):
    if ctx.checkpoint_data:  # retry resumes without re-paying for the model call
        return {"summary_ref": ctx.checkpoint_data["summary_ref"]}
    summary_ref = await summarize(payload["chunk_ref"])
    await ctx.checkpoint({"summary_ref": summary_ref})
    return {"summary_ref": summary_ref}


@worker.task("fan_out_summaries")
async def fan_out_summaries(ctx: Ctx, payload: dict):
    # A chained tail starts with an EMPTY payload; the predecessor's result is NOT
    # delivered inline. ctx.output[key] only works for upstreams named in depends_on;
    # for a plain chain hop, pull the result from the lazy GetResult tier with
    # `await ctx.output.fetch(...)` (returns a plain dict).
    parsed = await ctx.output.fetch("parse_content")
    # The lazy tier decodes through the producer's output_schema when one is set, so
    # `parsed` is a ParseOutput here; fall back to dict access if it isn't typed.
    if isinstance(parsed, ParseOutput):
        chunk_refs, document_id = parsed.chunk_refs, parsed.document_id
    else:
        chunk_refs, document_id = parsed["chunk_refs"], parsed["document_id"]
    gate = await ctx.submit_children(
        children=[{"task": "summarize_chunk", "payload": {"chunk_ref": r}} for r in chunk_refs],
        # The caller payload here is PRESERVED into the continuation; the gate manifest
        # arrives alongside it under "__gate__". Thread document_id explicitly.
        on_complete={"task": "executive_summary", "payload": {"document_id": document_id}},
        gate_policy="all_success",
    )
    return {"children": len(gate.children)}


@worker.task("executive_summary")
async def executive_summary(ctx: Ctx, payload: dict):
    # A gate continuation receives the caller's on_complete payload UNCHANGED, plus
    # the aggregate under the reserved "__gate__" key: {gate_id, results, expected,
    # succeeded} (succeeded EXCLUDES ctx.skip() children). Read the count from there,
    # not the top level.
    gate = payload.get("__gate__", {})
    return {"document_id": payload["document_id"], "summarized": gate.get("succeeded", 0)}


if __name__ == "__main__":
    worker.run()
