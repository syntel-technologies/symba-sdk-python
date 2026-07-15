"""Ctx + UpstreamOutputs two-tier resolution + schema index (spec 10)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from symba import _json
from symba._proto import common_pb2
from symba.context import Ctx, Skip, StopChain, UpstreamOutputs
from symba.errors import AmbiguousResultKey
from symba.logging import get_logger

pytestmark = pytest.mark.asyncio


def _upstream(*pairs: tuple[str, str, dict]) -> list[common_pb2.UpstreamResult]:
    return [
        common_pb2.UpstreamResult(key=key, job_id=job_id, result_json=_json.dumps(value))
        for key, job_id, value in pairs
    ]


async def test_inline_tier_hits_synchronously():
    out = UpstreamOutputs(_upstream(("parse", "j1", {"tokens": 5})))
    assert out["parse"] == {"tokens": 5}
    assert "parse" in out
    assert list(out) == ["parse"]


async def test_ambiguous_key_raises():
    out = UpstreamOutputs(_upstream(("dup", "j1", {"a": 1}), ("dup", "j2", {"a": 2})))
    with pytest.raises(AmbiguousResultKey):
        _ = out["dup"]


async def test_inline_miss_is_keyerror_not_none():
    out = UpstreamOutputs(_upstream(("parse", "j1", {"x": 1})))
    with pytest.raises(KeyError):
        _ = out["missing"]


async def test_lazy_fetch_tier_memoized():
    calls: list[str] = []

    async def lazy(key: str):
        calls.append(key)
        return {"deep": True} if key == "ancestor" else None

    out = UpstreamOutputs(_upstream(), lazy_fetch=lazy)
    assert await out.fetch("ancestor") == {"deep": True}
    # second fetch is memoized, no new RPC
    assert await out.fetch("ancestor") == {"deep": True}
    assert calls == ["ancestor"]


async def test_lazy_fetch_not_found_raises_keyerror():
    async def lazy(key: str):
        return None

    out = UpstreamOutputs(_upstream(), lazy_fetch=lazy)
    with pytest.raises(KeyError):
        await out.fetch("nope")


async def test_iteration_does_not_trigger_lazy():
    async def lazy(key: str):
        raise AssertionError("iteration must not fetch")

    out = UpstreamOutputs(_upstream(("a", "j1", {"n": 1})), lazy_fetch=lazy)
    assert list(out) == ["a"]


async def test_schema_typing_on_inline_hit():
    class ParseOut(BaseModel):
        tokens: int

    out = UpstreamOutputs(
        _upstream(("parse", "j1", {"tokens": 7})),
        schema_resolver=lambda k: ParseOut if k == "parse" else None,
    )
    value = out["parse"]
    assert isinstance(value, ParseOut)
    assert value.tokens == 7


async def test_stop_chain_and_skip_are_sentinels():
    ctx = _bare_ctx()
    sc = ctx.stop_chain({"done": True})
    assert isinstance(sc, StopChain)
    assert sc.result == {"done": True}
    assert isinstance(ctx.skip(), Skip)


def _bare_ctx() -> Ctx:
    return Ctx(
        job_id="j",
        ctx_id="c",
        task_name="t",
        attempt=1,
        tenant="default",
        payload={},
        output=UpstreamOutputs([]),
        logger=get_logger(),
    )
