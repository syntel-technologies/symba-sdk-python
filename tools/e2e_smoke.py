"""Full live-engine smoke test for the SDK <-> engine contract.

Exercises every control-plane RPC and the core worker execution paths against a
REAL engine over gRPC (not SymbaTest), the way a consumer like iKnowledge would.
This is the "does the whole thing actually work end to end" check, complementing
tools/e2e_gate_manifest.py (which focuses narrowly on the gate skip/manifest
contract).

Coverage:

    control plane   Submit, GetJob, AwaitJob, Query, Cancel, Signal, Resubmit,
                    StreamEvents, FanOut
    worker paths    single task, chain (tail reads ctx.output), on_failure fires
                    on DEAD, retries then DEAD, checkpoint round-trip,
                    wait_for_event resumed by Signal

Usage (engine must be running, e.g. `docker compose up -d` in the engine repo):

    uv run python tools/e2e_smoke.py [grpc://localhost:7233]

Exits non-zero on the first failed assertion.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid

from symba import Engine, JobFailed, JobState, RetryableError, RetryPolicy, Worker


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _ok(msg: str) -> None:
    sys.stdout.write(f"  ok  {msg}\n")


def _build_worker(target: str) -> Worker:
    w = Worker(engine=target, tags=["general"], slots=8)

    @w.task("echo")
    async def echo(ctx, payload):
        return {"echo": payload}

    @w.task("boom")
    async def boom(ctx, payload):
        raise RuntimeError("intentional failure")

    @w.task("record_failure")
    async def record_failure(ctx, payload):
        return {"handled": True}

    @w.task("head")
    async def head(ctx, payload):
        return {"from_head": payload.get("seed", 0) + 1}

    @w.task("tail")
    async def tail(ctx, payload):
        # SDK-3: the tail starts with an EMPTY payload; upstream data flows via the
        # lazy GetResult tier (ctx.output.fetch), not inline, for a plain chain.
        upstream = await ctx.output.fetch("head")
        return {"from_tail": upstream["from_head"] + 1}

    @w.task("flaky")
    async def flaky(ctx, payload):
        # Always fails with a RETRYABLE error; with a bounded retry policy the engine
        # reschedules until max_attempts, then it ends DEAD. A plain RuntimeError is
        # classified non-retryable and would die on attempt 1.
        raise RetryableError("flaky failure")

    @w.task("checkpointer")
    async def checkpointer(ctx, payload):
        await ctx.checkpoint({"stage": "one", "value": 42})
        return {"checkpointed": True}

    @w.task("waiter")
    async def waiter(ctx, payload):
        signal = await ctx.wait_for_event(payload["wait_key"], timeout_s=30)
        return {"resumed_with": signal}

    @w.task("gate_collect")
    async def gate_collect(ctx, payload):
        # The gate continuation reads the authoritative aggregate manifest the engine
        # merges under __gate__ (this is the contract-guaranteed path, unlike the
        # heuristic gate.status()).
        gate = payload["__gate__"]
        return {"succeeded": gate["succeeded"], "expected": gate["expected"]}

    return w


async def _await_state(engine: Engine, job_id: str, want, *, timeout: float = 20.0):
    """Poll GetJob until the job reaches `want` (a JobState) or times out."""
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        status = await engine.get_job(job_id)
        last = status.state
        if status.state == want:
            return status
        await asyncio.sleep(0.4)
    raise AssertionError(f"job {job_id} never reached {want!r}; last state {last!r}")


async def _submit_success(engine: Engine) -> None:
    """Submit + AwaitJob happy path."""
    handle = await engine.submit("echo", {"hello": "world"})
    result = await handle.result(timeout=20)
    assert result == {"echo": {"hello": "world"}}, result
    _ok(f"Submit + AwaitJob: {result}")


async def _get_job(engine: Engine) -> None:
    """GetJob returns a coherent terminal snapshot."""
    handle = await engine.submit("echo", {"n": 1})
    await handle.result(timeout=20)
    status = await engine.get_job(handle.id)
    assert status.id == handle.id, status
    assert status.task_name == "echo", status
    assert status.state == JobState.SUCCEEDED, status
    _ok(f"GetJob: state={status.state.name} attempt={status.attempt}")


async def _query(engine: Engine) -> None:
    """Query by ctx_id returns the jobs we submitted under it."""
    ctx_id = _uid("q")
    for i in range(3):
        await engine.submit("echo", {"i": i}, ctx_id=ctx_id)
    # Give the engine a beat to persist, then read them back.
    await asyncio.sleep(0.5)
    rows = await engine.query(ctx_id=ctx_id)
    assert len(rows) == 3, f"expected 3 rows for ctx {ctx_id}, got {len(rows)}"
    _ok(f"Query by ctx_id: {len(rows)} rows")


async def _cancel(engine: Engine) -> None:
    """Cancel a scheduled-in-the-future job before it runs."""
    from datetime import UTC, datetime, timedelta

    handle = await engine.submit(
        "echo", {"late": True}, run_at=datetime.now(UTC) + timedelta(hours=1)
    )
    outcome = await handle.cancel()
    assert outcome.cancelled, outcome
    status = await engine.get_job(handle.id)
    assert status.state == JobState.CANCELLED, status
    _ok(f"Cancel: previous={outcome.previous_state.name} -> CANCELLED")


async def _resubmit(engine: Engine) -> None:
    """Resubmit a DEAD job produces a fresh runnable job."""
    handle = await engine.submit("boom", {})
    with contextlib.suppress(JobFailed):
        await handle.result(timeout=20)
    dead = await engine.get_job(handle.id)
    assert dead.state == JobState.DEAD, dead
    fresh = await engine.resubmit(handle.id)
    assert fresh.id != handle.id, "resubmit must mint a new job id"
    _ok(f"Resubmit: {handle.id[:8]} (DEAD) -> {fresh.id[:8]}")


async def _stream_events(engine: Engine) -> None:
    """StreamEvents yields ledger rows for a ctx as jobs progress."""
    ctx_id = _uid("stream")
    handle = await engine.submit("echo", {"stream": True}, ctx_id=ctx_id)
    await handle.result(timeout=20)
    events = await handle.events()
    assert events, f"expected at least one event for job {handle.id}"
    _ok(f"StreamEvents/events(): {len(events)} events for ctx {ctx_id}")


async def _signal(engine: Engine) -> None:
    """wait_for_event suspends a job; Signal resumes it (spec 6.6)."""
    wait_key = _uid("wait")
    handle = await engine.submit("waiter", {"wait_key": wait_key})
    # Let the job start and register its wait before signalling.
    await asyncio.sleep(1.5)
    delivered = await engine.signal(wait_key, {"payload": "go"})
    assert delivered >= 1, f"signal delivered to {delivered} waiters, expected >=1"
    result = await handle.result(timeout=25)
    assert result["resumed_with"] == {"payload": "go"}, result
    _ok(f"Signal + wait_for_event: resumed with {result['resumed_with']}")


async def _chain(engine: Engine) -> None:
    """A chained head->tail runs both; tail reads head's output (SDK-3)."""
    ctx_id = _uid("chain")
    handle = await engine.submit("head", {"seed": 10}, chain=["tail"], ctx_id=ctx_id)
    await handle.result(timeout=20)
    # The tail is a separate job in the same ctx; wait for it to appear + succeed.
    tail_id = None
    for _ in range(30):
        rows = await engine.query(ctx_id=ctx_id)
        tail_rows = [r for r in rows if r.task_name == "tail"]
        if tail_rows:
            tail_id = tail_rows[0].id
            break
        await asyncio.sleep(0.4)
    assert tail_id is not None, f"tail job never enqueued for ctx {ctx_id}"
    tail_status = await _await_state(engine, tail_id, JobState.SUCCEEDED)
    assert tail_status.state == JobState.SUCCEEDED, tail_status
    _ok(f"Chain head->tail: head SUCCEEDED, tail SUCCEEDED ({tail_id[:8]})")


async def _on_failure(engine: Engine) -> None:
    """A DEAD job fires its on_failure spec (spec 6.3)."""
    ctx_id = _uid("onfail")
    handle = await engine.submit(
        "boom",
        {},
        ctx_id=ctx_id,
        on_failure={"task": "record_failure", "payload": {}},
    )
    with contextlib.suppress(JobFailed):
        await handle.result(timeout=20)
    await asyncio.sleep(1.0)
    rows = await engine.query(ctx_id=ctx_id)
    handler_rows = [r for r in rows if r.task_name == "record_failure"]
    assert handler_rows, f"on_failure handler never enqueued for ctx {ctx_id}"
    _ok(f"on_failure: DEAD job spawned {len(handler_rows)} handler job(s)")


async def _retries_then_dead(engine: Engine) -> None:
    """A flaky task with bounded retries lands DEAD with attempt>1 (spec 6.3)."""
    handle = await engine.submit(
        "flaky",
        {},
        retry=RetryPolicy(max_attempts=3, backoff_base_s=0.2, backoff_factor=1.0),
    )
    with contextlib.suppress(JobFailed):
        await handle.result(timeout=30)
    dead = await _await_state(engine, handle.id, JobState.DEAD, timeout=30)
    assert dead.attempt >= 2, f"expected multiple attempts, got attempt={dead.attempt}"
    _ok(f"Retries -> DEAD: final attempt={dead.attempt}")


async def _checkpoint(engine: Engine) -> None:
    """A task that checkpoints still completes cleanly (checkpoint round-trip)."""
    handle = await engine.submit("checkpointer", {})
    result = await handle.result(timeout=20)
    assert result == {"checkpointed": True}, result
    _ok("Checkpoint round-trip: task completed after ctx.checkpoint()")


async def _fan_out(engine: Engine) -> None:
    """FanOut fires the gate continuation once all children terminate."""
    ctx_id = _uid("fanout")
    _handles, gate = await engine.fan_out(
        [{"task": "echo", "payload": {"i": i}} for i in range(3)],
        on_complete={"task": "gate_collect"},
        gate_policy="all_success",
        ctx_id=ctx_id,
    )
    result = await gate.result(timeout=30)
    assert result["succeeded"] == 3, result
    assert result["expected"] == 3, result
    _ok(f"FanOut: gate fired, succeeded={result['succeeded']}/{result['expected']}")


async def _main(target: str) -> int:
    engine = Engine(target, tenant="default")
    worker = _build_worker(target)
    worker_task = asyncio.create_task(worker.arun())
    checks = [
        ("Submit + AwaitJob", _submit_success),
        ("GetJob", _get_job),
        ("Query", _query),
        ("Cancel", _cancel),
        ("Resubmit", _resubmit),
        ("StreamEvents", _stream_events),
        ("Chain head->tail", _chain),
        ("on_failure", _on_failure),
        ("Retries -> DEAD", _retries_then_dead),
        ("Checkpoint", _checkpoint),
        ("FanOut gate", _fan_out),
    ]
    try:
        await asyncio.sleep(1.5)  # let the worker boot + register tags
        for name, fn in checks:
            sys.stdout.write(f"{name}\n")
            await fn(engine)
        sys.stdout.write("Signal + wait_for_event\n")
        await _signal(engine)
    finally:
        worker.stop()
        try:
            await asyncio.wait_for(worker_task, timeout=15)
        except TimeoutError:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await worker_task
        await engine.aclose()
    sys.stdout.write(
        f"\nPASS: full control-plane + worker smoke verified against {target}\n"
    )
    return 0


if __name__ == "__main__":
    tgt = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:7233"
    raise SystemExit(asyncio.run(_main(tgt)))
