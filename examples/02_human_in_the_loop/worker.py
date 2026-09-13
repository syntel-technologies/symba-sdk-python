"""Human-in-the-loop — park on wait_for_event, resume on signal.

    symba run examples.02_human_in_the_loop.worker:worker

The job checkpoints its expensive draft BEFORE waiting, then parks into WAITING
(releasing its slot) until an external ``engine.signal`` arrives or the timeout
elapses. See ``approve.py`` to deliver the decision.
"""

from __future__ import annotations

from symba import Ctx, Worker

worker = Worker(engine="grpc://localhost:7233", tags=["review"], slots=50)


async def prepare_draft(payload: dict) -> str:
    return f"draft://{payload['ticket_id']}"


@worker.task("review_gate")
async def review_gate(ctx: Ctx, payload: dict):
    # checkpoint the expensive work BEFORE waiting — wait_for_event re-runs the
    # handler from the top on resume, so anything not checkpointed is redone.
    if not ctx.checkpoint_data:
        draft_ref = await prepare_draft(payload)
        await ctx.checkpoint({"draft_ref": draft_ref})
    else:
        draft_ref = ctx.checkpoint_data["draft_ref"]

    decision = await ctx.wait_for_event(f"approve:{payload['ticket_id']}", timeout_s=86_400)
    if decision is None:
        return ctx.stop_chain({"outcome": "approval_timed_out"})
    if not decision["approved"]:
        return ctx.stop_chain({"outcome": "rejected", "by": decision["reviewer"]})
    return {"draft_ref": draft_ref, "approved_by": decision["reviewer"]}


if __name__ == "__main__":
    worker.run()
