"""Submit the enrichment flow as a chain and await the head result.

    python -m examples.01_enrichment_pipeline.submit

Run ``worker.py`` first (in another shell) so there is something to claim the jobs.
"""

from __future__ import annotations

import asyncio
import uuid

from symba import Engine


async def main() -> None:
    doc_id = uuid.uuid4().hex[:8]
    async with Engine("grpc://localhost:7233", tenant="acme") as engine:
        job = await engine.submit(
            task="parse_content",
            payload={"document_id": doc_id, "staging_ref": f"s3://bucket/{doc_id}"},
            chain=["parse_content", "fan_out_summaries"],
            ctx_id=doc_id,
            pipeline="ingestion",
            stage="parsing",
            dedup_key=f"parse:{doc_id}",
        )
        print(f"submitted head job {job.id} (ctx_id={doc_id})")
        result = await job.result(timeout=60)
        print("parse_content result:", result)


if __name__ == "__main__":
    asyncio.run(main())
