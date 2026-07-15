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

worker = Worker(engine="grpc://localhost:7233", tags=["llm"], slots=100, strict_schemas=True)


class ParseInput(BaseModel):
    document_id: str
    staging_ref: str


class ParseOutput(BaseModel):
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
    return ParseOutput(chunk_refs=await stage(chunks), content_hash=content_hash)


@worker.task("summarize_chunk")
async def summarize_chunk(ctx: Ctx, payload: dict):
    if ctx.checkpoint_data:  # retry resumes without re-paying for the model call
        return {"summary_ref": ctx.checkpoint_data["summary_ref"]}
    summary_ref = await summarize(payload["chunk_ref"])
    await ctx.checkpoint({"summary_ref": summary_ref})
    return {"summary_ref": summary_ref}


@worker.task("fan_out_summaries")
async def fan_out_summaries(ctx: Ctx, payload: dict):
    parsed = ctx.output["parse_content"]  # typed via the schema index when strict_schemas
    chunk_refs = parsed.chunk_refs if isinstance(parsed, ParseOutput) else parsed["chunk_refs"]
    gate = await ctx.submit_children(
        children=[{"task": "summarize_chunk", "payload": {"chunk_ref": r}} for r in chunk_refs],
        on_complete={"task": "executive_summary", "payload": payload},
        gate_policy="all_success",
    )
    return {"children": len(gate.children)}


@worker.task("executive_summary")
async def executive_summary(ctx: Ctx, payload: dict):
    return {"document_id": payload["document_id"], "summarized": payload.get("succeeded", 0)}


if __name__ == "__main__":
    worker.run()
