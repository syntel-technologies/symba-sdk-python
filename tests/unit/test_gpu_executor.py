"""gpu GpuExecutor: warm subprocess, init hook, respawn (spec 11.3)."""

from __future__ import annotations

import pytest

from symba.context import Ctx, UpstreamOutputs
from symba.errors import RetryableError
from symba.executors.gpu_executor import GpuExecutor
from symba.logging import get_logger
from symba.profiles import Profile
from symba.task_registry import RegisteredTask

from . import _subproc_handlers as H

pytestmark = pytest.mark.asyncio


def _task(name: str, handler) -> RegisteredTask:
    return RegisteredTask(name=name, handler=handler, profile=Profile.GPU)


def _ctx() -> Ctx:
    return Ctx(
        job_id="j1",
        ctx_id="c1",
        task_name="infer",
        attempt=1,
        tenant="acme",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
        profile="gpu",
    )


async def test_gpu_init_runs_once_and_weights_stay_warm():
    ex = GpuExecutor({"infer": H.gpu_infer}, [H.gpu_init])
    await ex.start()
    try:
        r1 = await ex.run(_task("infer", H.gpu_infer), _ctx(), {"x": 1})
        r2 = await ex.run(_task("infer", H.gpu_infer), _ctx(), {"x": 2})
        # init ran in the subprocess, and both jobs saw the same warm process
        assert r1["warm"] is True
        assert r1["same_process"] is True
        assert r2["echo"] == 2
    finally:
        await ex.stop(1.0)


async def test_gpu_circuit_breaker_opens_after_repeated_crashes():
    opened: list[bool] = []
    ex = GpuExecutor(
        {"crash": H.hard_crash}, [H.gpu_init], on_circuit_open=lambda: opened.append(True)
    )
    await ex.start()
    try:
        for _ in range(3):
            with pytest.raises(RetryableError):
                await ex.run(_task("crash", H.hard_crash), _ctx(), {})
        # after 3 crashes in the window the breaker is open
        assert opened == [True]
        with pytest.raises(RetryableError):
            await ex.run(_task("crash", H.hard_crash), _ctx(), {})
    finally:
        await ex.stop(1.0)
