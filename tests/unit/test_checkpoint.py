"""Checkpoint store: fast path, durable path, drain, and reap (spec 13)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import grpc
import pytest

from symba import _json
from symba import checkpoint as checkpoint_mod
from symba._proto import data_plane_pb2
from symba.checkpoint import CheckpointStore

from ._fakes import FakeRedis, FakeWorkerStub

pytestmark = pytest.mark.asyncio


def _store(worker: FakeWorkerStub, redis: FakeRedis | None = None) -> CheckpointStore:
    return CheckpointStore(
        worker_stub=worker,  # type: ignore[arg-type]
        job_id="job-1",
        lease_token="lease-1",
        idempotency_key="idem-abc",
        redis=redis,
    )


async def test_durable_only_write_is_awaited():
    worker = FakeWorkerStub()
    store = _store(worker)

    await store.write({"llm_response": "hi"})

    assert len(worker.checkpoints) == 1
    req = worker.checkpoints[0]
    assert req.job_id == "job-1"
    assert _json.loads(req.checkpoint_json) == {"llm_response": "hi"}


async def test_fast_path_writes_redis_and_fires_background_rpc():
    worker, redis = FakeWorkerStub(), FakeRedis()
    store = _store(worker, redis)

    await store.write({"n": 1})

    # Redis got the synchronous write, keyed by dedup identity.
    assert redis.store["symba:ckpt:idem-abc"] == _json.dumps({"n": 1})
    # PutCheckpoint fires in the background; drain to observe it.
    await store.drain()
    assert len(worker.checkpoints) == 1


async def test_transient_checkpoint_rpc_failure_is_retried(monkeypatch):
    class FlakyWorkerStub(FakeWorkerStub):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def PutCheckpoint(self, req: data_plane_pb2.PutCheckpointRequest):
            self.attempts += 1
            if self.attempts < 3:
                raise grpc.aio.AioRpcError(
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.aio.Metadata(),
                    grpc.aio.Metadata(),
                    details="connection timed out",
                )
            return await super().PutCheckpoint(req)

    worker, redis = FlakyWorkerStub(), FakeRedis()
    sleep = AsyncMock()
    monkeypatch.setattr(checkpoint_mod.asyncio, "sleep", sleep)
    store = _store(worker, redis)

    await store.write({"n": 1})
    await store.drain()

    assert worker.attempts == 3
    assert len(worker.checkpoints) == 1
    assert sleep.await_count == 2


async def test_read_fast_prefers_redis():
    worker, redis = FakeWorkerStub(), FakeRedis()
    store = _store(worker, redis)
    await store.write({"fresh": True})

    assert await store.read_fast() == {"fresh": True}


async def test_read_fast_none_without_redis():
    store = _store(FakeWorkerStub())
    assert await store.read_fast() is None


async def test_delete_fast_reaps_key():
    worker, redis = FakeWorkerStub(), FakeRedis()
    store = _store(worker, redis)
    await store.write({"x": 1})
    await store.drain()

    await store.delete_fast()

    assert "symba:ckpt:idem-abc" in redis.deleted
    assert "symba:ckpt:idem-abc" not in redis.store


async def test_delete_fast_noop_without_redis():
    store = _store(FakeWorkerStub())
    await store.delete_fast()  # must not raise


async def test_durable_fallback_warns_once(monkeypatch):
    """SDK-5: the durable-only path WARNs once so a missing Redis is visible."""
    monkeypatch.setattr(checkpoint_mod, "_warned_no_redis", False)
    warnings: list[dict] = []
    monkeypatch.setattr(
        checkpoint_mod._log,
        "warning",
        lambda event, **kw: warnings.append({"event": event, **kw}),
    )

    store = _store(FakeWorkerStub())
    await store.write({"a": 1})
    await store.write({"a": 2})  # second write must NOT re-warn

    assert len(warnings) == 1
    assert warnings[0]["event"] == "checkpoint_durable_path_only"


async def test_fast_path_does_not_warn(monkeypatch):
    """SDK-5: with Redis configured the fallback WARN must not fire."""
    monkeypatch.setattr(checkpoint_mod, "_warned_no_redis", False)
    warnings: list[str] = []
    monkeypatch.setattr(checkpoint_mod._log, "warning", lambda event, **kw: warnings.append(event))

    store = _store(FakeWorkerStub(), FakeRedis())
    await store.write({"a": 1})
    await store.drain()

    assert "checkpoint_durable_path_only" not in warnings
