"""Checkpoints (spec 13, AD-14).

*An LLM response, once received, is never lost to a downstream failure, and is
never paid for twice.*

Two paths, one verb:

* **Fast path (optional):** when the ``redis`` extra is installed AND a checkpoint
  Redis URL is configured, write synchronously to Redis keyed by the job's dedup
  identity (``ctx.idempotency_key``), and fire ``PutCheckpoint`` in the background
  (error-logged, never blocking the handler).
* **Durable path (always):** ``PutCheckpoint(job_id, lease_token, checkpoint_json)``
  to the engine (Postgres write-behind). Without Redis this is awaited — it is
  then the only copy and must land before the handler proceeds.

The read path is pre-loaded into ``ctx.checkpoint_data`` (Redis-first, else the
assignment's durable copy). Redis import is guarded so the base install never
needs it (spec 3, rule 3).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import grpc

from . import _json
from ._proto import data_plane_pb2
from .logging import get_logger

if TYPE_CHECKING:
    from ._proto import data_plane_pb2_grpc

_log = get_logger(component="checkpoint")

#: Redis key TTL for fast-path checkpoints; the engine owns durable retention.
_REDIS_TTL_S = 7 * 24 * 3600

# PutCheckpoint is an idempotent upsert. A bounded retry closes the short
# transport-gap window where Redis has the fast copy but Postgres misses the
# durable copy because the shared gRPC channel is reconnecting.
_CHECKPOINT_RPC_MAX_ATTEMPTS = 4
_CHECKPOINT_RPC_INITIAL_BACKOFF_S = 0.2
_CHECKPOINT_RPC_MAX_BACKOFF_S = 2.0
_RETRYABLE_CHECKPOINT_CODES = frozenset(
    {
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNAVAILABLE,
    }
)

#: Process-level latch so the "no Redis, using durable path" WARN fires once, not
#: once per checkpoint (spec 13.1; SDK-5 — make the silent fallback visible).
_warned_no_redis = False


def redis_available() -> bool:
    """True when the optional ``redis`` extra can be imported (spec 3, rule 3)."""
    try:
        import redis.asyncio  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        return False
    return True


async def open_redis(url: str) -> Any:
    """Open an async Redis client, or raise a clear error if the extra is missing."""
    try:
        import redis.asyncio as aioredis  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on host env
        raise RuntimeError(
            "SYMBA_CHECKPOINT_REDIS_URL is set but the 'redis' extra is not installed; "
            "install syntel-symba[redis] or unset the URL to use the durable PutCheckpoint path"
        ) from exc
    return aioredis.from_url(url, decode_responses=False)


class CheckpointStore:
    """Per-execution checkpoint writer/reader (spec 13.1-13.3).

    One instance per running job. ``redis`` is an already-open async client shared
    across executions on the worker, or ``None`` for the durable-only path.
    """

    def __init__(
        self,
        *,
        worker_stub: data_plane_pb2_grpc.WorkerServiceStub,
        job_id: str,
        lease_token: str,
        idempotency_key: str,
        redis: Any | None = None,
    ) -> None:
        self._worker = worker_stub
        self._job_id = job_id
        self._lease_token = lease_token
        self._redis = redis
        self._key = f"symba:ckpt:{idempotency_key}"
        self._bg: set[asyncio.Task[None]] = set()

    async def write(self, data: dict[str, Any]) -> None:
        """Write a checkpoint (spec 13.1). Redis-fast + background RPC, or awaited RPC."""
        raw = _json.dumps(data)
        if self._redis is not None:
            await self._redis.set(self._key, raw, ex=_REDIS_TTL_S)
            self._fire_put_checkpoint(raw)
            return
        self._warn_durable_fallback_once()
        await self._put_checkpoint(raw)

    def _warn_durable_fallback_once(self) -> None:
        """WARN (once per process) that checkpointing is taking the durable RPC path.

        Without a checkpoint Redis URL the fast path is silently skipped: correct
        but slower, and any pre-checkpoint work is re-done on a lease reclaim. Make
        that visible so a missing ``SYMBA_CHECKPOINT_REDIS_URL`` is not a silent
        perf/correctness cliff (SDK-5).
        """
        global _warned_no_redis
        if _warned_no_redis:
            return
        _warned_no_redis = True
        _log.warning(
            "checkpoint_durable_path_only",
            hint="no checkpoint Redis configured (SYMBA_CHECKPOINT_REDIS_URL or "
            "Worker(checkpoint_redis_url=...)); ctx.checkpoint uses the durable "
            "PutCheckpoint RPC — correct but slower, and pre-wait work is re-done on reclaim",
        )

    async def read_fast(self) -> dict[str, Any] | None:
        """Redis-first read; the fresher unflushed write may live only here (spec 13.2)."""
        if self._redis is None:
            return None
        raw = await self._redis.get(self._key)
        if not raw:
            return None
        return _json.loads(raw)

    async def delete_fast(self) -> None:
        """Best-effort Redis key delete on Complete; engine owns durable reaping (spec 13.3)."""
        if self._redis is None:
            return
        try:
            await self._redis.delete(self._key)
        except Exception as exc:  # best-effort cleanup; engine owns durable reaping
            _log.warning("checkpoint_redis_delete_failed", job_id=self._job_id, error=str(exc))

    async def drain(self) -> None:
        """Await any in-flight background PutCheckpoint tasks (called before Complete)."""
        if self._bg:
            await asyncio.gather(*self._bg, return_exceptions=True)

    def _fire_put_checkpoint(self, raw: bytes) -> None:
        task = asyncio.ensure_future(self._put_checkpoint(raw))
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _put_checkpoint(self, raw: bytes) -> None:
        req = data_plane_pb2.PutCheckpointRequest(
            job_id=self._job_id, lease_token=self._lease_token, checkpoint_json=raw
        )
        try:
            await self._put_checkpoint_with_retry(req)
        except Exception as exc:  # durable write-behind; error-logged, re-raised only if sole copy
            _log.error("put_checkpoint_failed", job_id=self._job_id, error=str(exc))
            if self._redis is None:
                raise

    async def _put_checkpoint_with_retry(
        self,
        req: data_plane_pb2.PutCheckpointRequest,
    ) -> None:
        attempt = 1
        backoff_s = _CHECKPOINT_RPC_INITIAL_BACKOFF_S
        while True:
            try:
                await self._worker.PutCheckpoint(req)
                return
            except grpc.aio.AioRpcError as exc:
                code = exc.code()
                if (
                    code not in _RETRYABLE_CHECKPOINT_CODES
                    or attempt >= _CHECKPOINT_RPC_MAX_ATTEMPTS
                ):
                    raise
                _log.warning(
                    "put_checkpoint_retrying",
                    job_id=self._job_id,
                    attempt=attempt,
                    max_attempts=_CHECKPOINT_RPC_MAX_ATTEMPTS,
                    code=code.name,
                    retry_in_s=backoff_s,
                )
                await asyncio.sleep(backoff_s)
                attempt += 1
                backoff_s = min(backoff_s * 2, _CHECKPOINT_RPC_MAX_BACKOFF_S)


__all__ = ["CheckpointStore", "redis_available", "open_redis"]
