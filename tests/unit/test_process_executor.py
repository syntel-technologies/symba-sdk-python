"""cpu ProcessExecutor: real forkserver subprocess, crash containment (spec 11.1)."""

from __future__ import annotations

import sys

import pytest

from symba.checkpoint import CheckpointStore
from symba.context import Ctx, UpstreamOutputs
from symba.ctx_backend import IoCtxBackend
from symba.errors import RetryableError, SymbaError
from symba.executors.process_executor import ProcessExecutor
from symba.logging import get_logger
from symba.profiles import Profile
from symba.task_registry import RegisteredTask

from . import _subproc_handlers as H
from ._fakes import FakeControlStub, FakeWorkerStub

pytestmark = pytest.mark.asyncio

# forkserver is unavailable on Windows; these tests target POSIX workers.
_skip_win = pytest.mark.skipif(sys.platform == "win32", reason="forkserver is POSIX-only")

_HANDLERS = {
    "double": H.double,
    "ckpt": H.checkpoint_then_return,
    "boom": H.raise_value_error,
    "retry": H.raise_retryable,
    "crash": H.hard_crash,
}


def _executor(max_workers: int = 1) -> ProcessExecutor:
    return ProcessExecutor(max_workers=max_workers, handlers=_HANDLERS)


def _task(name: str, handler) -> RegisteredTask:
    return RegisteredTask(name=name, handler=handler, profile=Profile.CPU)


def _ctx(worker: FakeWorkerStub, control: FakeControlStub) -> Ctx:
    store = CheckpointStore(
        worker_stub=worker,  # type: ignore[arg-type]
        job_id="j1",
        lease_token="lease",
        idempotency_key="idem",
    )
    backend = IoCtxBackend(
        client_stub=control,  # type: ignore[arg-type]
        worker_stub=worker,  # type: ignore[arg-type]
        tenant="acme",
        ctx_id="c1",
        pipeline="p",
        job_id="j1",
        lease_token="lease",
        checkpoint_store=store,
    )
    return Ctx(
        job_id="j1",
        ctx_id="c1",
        task_name="crunch",
        attempt=1,
        tenant="acme",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
        backend=backend,
        profile="cpu",
    )


@_skip_win
async def test_cpu_handler_runs_in_child_process():
    ex = _executor(2)
    await ex.start()
    try:
        ctx = _ctx(FakeWorkerStub(), FakeControlStub())
        result = await ex.run(_task("double", H.double), ctx, {"n": 21})
        assert result["doubled"] == 42
    finally:
        await ex.stop(1.0)


@_skip_win
async def test_cpu_checkpoint_marshals_to_parent_backend():
    ex = _executor(1)
    await ex.start()
    worker, control = FakeWorkerStub(), FakeControlStub()
    try:
        ctx = _ctx(worker, control)
        await ex.run(_task("ckpt", H.checkpoint_then_return), ctx, {"n": 7})
        # the child's ctx.checkpoint() round-tripped to the parent's real backend
        assert len(worker.checkpoints) == 1
    finally:
        await ex.stop(1.0)


@_skip_win
async def test_cpu_value_error_classified_fatal():
    ex = _executor(1)
    await ex.start()
    try:
        ctx = _ctx(FakeWorkerStub(), FakeControlStub())
        with pytest.raises(SymbaError) as ei:
            await ex.run(_task("boom", H.raise_value_error), ctx, {})
        assert ei.value.retryable is False
        assert getattr(ei.value, "original_type", None) == "ValueError"
    finally:
        await ex.stop(1.0)


@_skip_win
async def test_cpu_retryable_error_preserved():
    ex = _executor(1)
    await ex.start()
    try:
        ctx = _ctx(FakeWorkerStub(), FakeControlStub())
        with pytest.raises(SymbaError) as ei:
            await ex.run(_task("retry", H.raise_retryable), ctx, {})
        assert ei.value.retryable is True
    finally:
        await ex.stop(1.0)


@_skip_win
async def test_cpu_hard_crash_is_retryable_and_pool_self_heals():
    ex = _executor(1)
    await ex.start()
    try:
        ctx = _ctx(FakeWorkerStub(), FakeControlStub())
        with pytest.raises(RetryableError):
            await ex.run(_task("crash", H.hard_crash), ctx, {})
        # pool self-heals: the next job runs fine
        result = await ex.run(_task("double", H.double), ctx, {"n": 5})
        assert result["doubled"] == 10
    finally:
        await ex.stop(1.0)
