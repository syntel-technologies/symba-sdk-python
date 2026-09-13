"""Deliver an approval decision to a parked review_gate job.

    python -m examples.02_human_in_the_loop.approve <ticket_id>

Mirrors what your approval webhook handler would do.
"""

from __future__ import annotations

import asyncio
import sys

from symba import Engine


async def main(ticket_id: str) -> None:
    async with Engine("grpc://localhost:7233", tenant="default") as engine:
        delivered = await engine.signal(
            f"approve:{ticket_id}",
            {"approved": True, "reviewer": "reviewer@acme.co"},
            signaled_by="reviewer@acme.co",
        )
        print(f"delivered approval to {delivered} waiting job(s)")


if __name__ == "__main__":
    ticket = sys.argv[1] if len(sys.argv) > 1 else "ticket-1"
    asyncio.run(main(ticket))
