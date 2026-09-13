"""Parent-side helpers shared by the cpu/gpu executors (spec 11.4).

The child runs the handler; the parent owns the gRPC channel. These helpers build
the picklable :class:`CtxSnapshot` from a live :class:`~symba.context.Ctx`, and
service the ``CtxCall`` frames a child emits by running the corresponding async verb
on the real :class:`~symba.ctx_backend.CtxBackend`.
"""

from __future__ import annotations

import structlog

from symba.context import Ctx

from .ctx_proxy import CtxCall, CtxReply, CtxSnapshot, LogRecord


def build_snapshot(ctx: Ctx, payload: object) -> CtxSnapshot:
    """Freeze the picklable slice of ``ctx`` for the child (spec 11.4)."""
    inline: dict[str, object] = {}
    for key in ctx.output:  # inline tier only; lazy fetches marshal back
        try:
            inline[key] = ctx.output[key]
        except Exception:  # ambiguous/unmaterialized keys stay lazy
            continue
    return CtxSnapshot(
        job_id=ctx.job_id,
        ctx_id=ctx.ctx_id,
        task_name=ctx.task_name,
        attempt=ctx.attempt,
        tenant=ctx.tenant,
        pipeline=ctx.pipeline,
        stage=ctx.stage,
        group_key=ctx.group_key,
        payload=payload,
        inline_output=inline,
        checkpoint_data=ctx.checkpoint_data,
        event_payload=ctx.event_payload,
        idempotency_key=ctx.idempotency_key,
        idempotency_key_attempt=ctx.idempotency_key_attempt,
        profile=ctx.profile,
    )


async def service_ctx_call(call: CtxCall, ctx: Ctx) -> CtxReply:
    """Run one child-requested verb against the live ``ctx`` and its backend."""
    try:
        verb = call.verb
        args = call.args
        if verb == "checkpoint":
            await ctx.checkpoint(args["data"])
            return CtxReply(ok=True, value=None)
        if verb == "heartbeat":
            await ctx.heartbeat()
            return CtxReply(ok=True, value=None)
        if verb == "submit":
            handle = await ctx.submit(**args["spec"])
            return CtxReply(ok=True, value={"job_id": handle.id})
        if verb == "submit_children":
            gate = await ctx.submit_children(
                args["children"], args.get("on_complete"), args.get("gate_policy", "all_success")
            )
            return CtxReply(
                ok=True,
                value={
                    "gate_id": gate.id,
                    "child_job_ids": [h.id for h in gate.children],
                },
            )
        if verb == "resolve_upstream":
            value = await ctx.output.fetch(args["key"])  # type: ignore[union-attr]
            return CtxReply(ok=True, value=value)
        return CtxReply(ok=False, error=f"unknown ctx verb {verb!r}", error_type="SymbaError")
    except Exception as exc:  # translated to a SymbaError in the child
        return CtxReply(ok=False, error=str(exc), error_type=type(exc).__name__)


def replay_log(record: LogRecord, logger: structlog.stdlib.BoundLogger) -> None:
    """Re-emit a child log record through the parent's logging pipeline (spec 18)."""
    method = getattr(logger, record.level, logger.info)
    method(record.event, **record.fields)


__all__ = ["build_snapshot", "service_ctx_call", "replay_log"]
