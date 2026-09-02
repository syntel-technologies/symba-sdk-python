"""Live-engine end-to-end check for the SDK-2 gate contract (E1 + E2 + E3).

Runs the two conformance-corpus gate scenarios against a REAL engine reached over
gRPC (not SymbaTest), proving the cross-repo contract now holds end to end:

    E1  the SDK's ClientService gRPC calls reach the engine (Submit/FanOut/Query/
        AwaitJob), i.e. the servicer is registered and the proto package matches.
    E3  a ctx.skip() child settles the gate but is EXCLUDED from `succeeded`.
    E2  the fired continuation payload PRESERVES the caller payload AND carries the
        aggregate manifest under the reserved `__gate__` key.

Usage (engine must be running, e.g. `docker compose up -d` in the engine repo):

    uv run python tools/e2e_gate_manifest.py [grpc://localhost:7233]

Exits non-zero on the first failed assertion so it can gate CI. This is a script,
not a pytest case, because the conformance `engine` backend fixture is not wired to
spin up infra in-process; see tests/conformance/conftest.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid

from symba import Engine, Worker


def _build_worker(target: str) -> Worker:
    w = Worker(engine=target, tags=["general"], slots=8)

    @w.task("maybe")
    async def maybe(ctx, payload):
        if payload["skip"]:
            return ctx.skip()
        return {"n": payload["n"]}

    @w.task("skipper")
    async def skipper(ctx, payload):
        return ctx.skip()

    @w.task("collect")
    async def collect(ctx, payload):
        gate = payload["__gate__"]
        return {
            "succeeded": gate["succeeded"],
            "expected": gate["expected"],
            "document_id": payload.get("document_id"),
        }

    return w


async def _run_case(engine: Engine, *, children, on_complete, ctx_id, expect) -> None:
    _handles, gate = await engine.fan_out(
        children, on_complete=on_complete, gate_policy="all_success", ctx_id=ctx_id
    )
    result = await gate.result(timeout=30)
    status = await gate.status()
    for key, want in expect.items():
        got = result.get(key) if key in result else getattr(status, key, None)
        assert got == want, f"{ctx_id}: {key} expected {want!r}, got {got!r} (result={result})"
    sys.stdout.write(f"  ok  {ctx_id}: {result}\n")


async def _main(target: str) -> int:
    engine = Engine(target, tenant="default")
    worker = _build_worker(target)
    worker_task = asyncio.create_task(worker.arun())
    try:
        # Give the worker a moment to boot + register its tags with the engine.
        await asyncio.sleep(1.5)

        # Unique ctx per run: a reused ctx_id collides with continuation jobs left
        # RUNNING by an interrupted prior run, and _gate_result would await the stale
        # one forever. Each run gets a fresh lineage.
        suffix = uuid.uuid4().hex[:8]

        sys.stdout.write(
            "E3 + E2: all_success gate with one skipped child "
            "(succeeded excludes skip)\n"
        )
        await _run_case(
            engine,
            children=[
                {"task": "maybe", "payload": {"skip": False, "n": 1}},
                {"task": "maybe", "payload": {"skip": True, "n": 2}},
                {"task": "maybe", "payload": {"skip": False, "n": 3}},
            ],
            on_complete={"task": "collect", "payload": {"document_id": "d1"}},
            ctx_id=f"e2e-gate-math-{suffix}",
            expect={"succeeded": 2, "expected": 3, "document_id": "d1"},
        )

        sys.stdout.write("E3: all-skipped gate still fires with succeeded == 0\n")
        await _run_case(
            engine,
            children=[{"task": "skipper", "payload": {}} for _ in range(3)],
            on_complete={"task": "collect"},
            ctx_id=f"e2e-all-skip-{suffix}",
            expect={"succeeded": 0, "expected": 3},
        )
    finally:
        worker.stop()
        # stop() flips the claim loop to draining; if the graceful path doesn't
        # settle quickly, cancel the task so the script always exits cleanly.
        try:
            await asyncio.wait_for(worker_task, timeout=15)
        except TimeoutError:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await worker_task
        await engine.aclose()
    sys.stdout.write(f"PASS: E1 + E2 + E3 verified end-to-end against {target}\n")
    return 0


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:7233"
    raise SystemExit(asyncio.run(_main(target)))
