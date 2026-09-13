"""Dispatch pipeline tests against a fake WorkerService stub (spec 9)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from symba import _json
from symba.context import Ctx
from symba.dispatch import DispatchDeps, Dispatcher
from symba.errors import RetryableError
from symba.executors.asyncio_executor import AsyncioExecutor
from symba.middleware import MiddlewareChain
from symba.profiles import Profile
from symba.schemas import register_output_schema
from symba.task_registry import TaskRegistry

from ._fakes import FakeControlStub, FakeWorkerStub, make_assignment

pytestmark = pytest.mark.asyncio


def _dispatcher(
    registry: TaskRegistry,
    stub: FakeWorkerStub,
    control: FakeControlStub | None = None,
) -> Dispatcher:
    return Dispatcher(
        DispatchDeps(
            stub=stub,  # type: ignore[arg-type]
            registry=registry,
            middleware=MiddlewareChain([]),
            executors={Profile.IO: AsyncioExecutor()},
            tenant="default",
            heartbeat_interval_s=15.0,
            classify_overrides=[],
            client_stub=control,  # type: ignore[arg-type]
        )
    )


async def test_success_completes_with_result():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        return {"echo": payload["x"]}

    registry.register("echo", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j1", task_name="echo", payload=_json.dumps({"x": 42})))

    assert len(stub.completes) == 1
    assert not stub.fails
    assert _json.loads(stub.completes[0].result_json) == {"echo": 42}


async def test_unknown_task_fails_non_retryable():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        return {}

    registry.register("known", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j2", task_name="ghost"))

    assert len(stub.fails) == 1
    assert stub.fails[0].error_type == "TaskNotRegistered"
    assert stub.fails[0].retryable is False


async def test_retryable_error_marks_retryable():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        raise RetryableError("try again")

    registry.register("flaky", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j3", task_name="flaky"))

    assert len(stub.fails) == 1
    assert stub.fails[0].retryable is True
    assert stub.fails[0].error_type == "RetryableError"


async def test_unhandled_exception_classified_fatal_by_default():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        raise ValueError("bad input")

    registry.register("boom", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j4", task_name="boom"))

    assert len(stub.fails) == 1
    assert stub.fails[0].retryable is False


async def test_stop_chain_drops_tail():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict):
        return ctx.stop_chain({"final": True})

    registry.register("stopper", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j5", task_name="stopper"))

    assert len(stub.completes) == 1
    assert stub.completes[0].drop_chain_tail is True


async def test_skip_completes_with_skipped_flag():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict):
        return ctx.skip()

    registry.register("skipper", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j6", task_name="skipper"))

    assert len(stub.completes) == 1
    assert stub.completes[0].skipped is True


async def test_payload_validation_failure_is_fatal():
    class In(BaseModel):
        n: int

    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: In) -> dict:
        return {"n": payload.n}

    registry.register("typed", handler, profile=Profile.IO, input_schema=In)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(
        make_assignment("j7", task_name="typed", payload=_json.dumps({"n": "not-an-int"}))
    )

    assert len(stub.fails) == 1
    assert stub.fails[0].retryable is False
    assert stub.fails[0].error_type == "PayloadValidationError"


async def test_ctx_submit_from_handler_inherits_ctx():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        child = await ctx.submit(task="child_task")
        return {"child_id": child.id}

    registry.register("spawner", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    control = FakeControlStub()
    control.submit_job_ids = ["spawned-1"]
    disp = _dispatcher(registry, stub, control)

    assignment = make_assignment("parent-1", task_name="spawner")
    assignment.job.spec.ctx_id = "ctx-abc"
    await disp.dispatch(assignment)

    assert len(stub.completes) == 1
    assert _json.loads(stub.completes[0].result_json) == {"child_id": "spawned-1"}
    assert control.submits[0].specs[0].ctx_id == "ctx-abc"


async def test_wait_park_sends_no_terminal_outcome():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        await ctx.wait_for_event("approve:X", timeout_s=60)
        return {"unreachable": True}

    registry.register("waiter", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    stub.wait_parked = True
    control = FakeControlStub()
    disp = _dispatcher(registry, stub, control)

    await disp.dispatch(make_assignment("parked-1", task_name="waiter"))

    # Parked engine-side: neither Complete nor Fail; the engine owns WAITING (spec 14.1).
    assert stub.completes == []
    assert stub.fails == []
    assert stub.waits[0].wait_key == "approve:X"


async def test_wait_signal_first_flows_through_to_complete():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        evt = await ctx.wait_for_event("approve:Y", timeout_s=60)
        return {"got": evt}

    registry.register("resumer", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    stub.wait_parked = False
    stub.wait_payload = _json.dumps({"approved": True})
    control = FakeControlStub()
    disp = _dispatcher(registry, stub, control)

    await disp.dispatch(make_assignment("resume-1", task_name="resumer"))

    assert len(stub.completes) == 1
    assert _json.loads(stub.completes[0].result_json) == {"got": {"approved": True}}


async def test_checkpoint_is_reaped_on_complete():
    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> dict:
        await ctx.checkpoint({"partial": 1})
        return {"done": True}

    registry.register("ckpt", handler, profile=Profile.IO)
    stub = FakeWorkerStub()
    control = FakeControlStub()
    disp = _dispatcher(registry, stub, control)

    await disp.dispatch(make_assignment("ckpt-1", task_name="ckpt"))

    assert len(stub.completes) == 1
    # durable-only path (no Redis) still records the checkpoint
    assert len(stub.checkpoints) == 1


async def test_output_schema_serializes_model():
    class Out(BaseModel):
        doubled: int

    registry = TaskRegistry()

    async def handler(ctx: Ctx, payload: dict) -> Out:
        return Out(doubled=payload["x"] * 2)

    registry.register("double", handler, profile=Profile.IO, output_schema=Out)
    register_output_schema("double", Out)
    stub = FakeWorkerStub()
    disp = _dispatcher(registry, stub)

    await disp.dispatch(make_assignment("j8", task_name="double", payload=_json.dumps({"x": 5})))

    assert len(stub.completes) == 1
    assert _json.loads(stub.completes[0].result_json) == {"doubled": 10}
