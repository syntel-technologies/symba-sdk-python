"""Ctx: the execution context (spec 10).

Everything a handler can need, nothing global. M2 delivers the identity/data
surface, the ``stop_chain``/``skip`` sentinels, ``heartbeat``, and an inline-tier
``UpstreamOutputs``. The lazy tier (``GetResult``), ``checkpoint``,
``wait_for_event``, and ``submit``/``submit_children`` are wired in M3/M4.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from . import _json
from .errors import AmbiguousResultKey, UnsupportedInProfile

if TYPE_CHECKING:
    import structlog
    from pydantic import BaseModel

    from ._proto import common_pb2
    from .ctx_backend import CtxBackend
    from .heartbeat import HeartbeatShell
    from .job import Gate, JobHandle


@dataclass(slots=True, frozen=True)
class StopChain:
    """Sentinel return value: complete with a result AND drop the chain tail (spec 10.2)."""

    result: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class Skip:
    """Sentinel return value: success without work; the chain continues (spec 10.2)."""


class _InlineResult:
    __slots__ = ("job_id", "raw")

    def __init__(self, job_id: str, raw: bytes) -> None:
        self.job_id = job_id
        self.raw = raw


class UpstreamOutputs(Mapping[str, Any]):
    """Two-tier upstream results (spec 10.3).

    The inline tier (immediate chain predecessor + declared ``depends_on``
    results, shipped in ``Job.upstream``) is available synchronously via ``[]``.
    The lazy ``GetResult`` tier is an RPC and therefore lives behind the async
    :meth:`fetch`. When the local worker registry knows the producer's
    ``output_schema`` the dict is model-validated into that type (AD-15).
    """

    def __init__(
        self,
        upstream: list[common_pb2.UpstreamResult],
        *,
        lazy_fetch: Callable[[str], Awaitable[dict[str, Any] | None]] | None = None,
        schema_resolver: Callable[[str], type[BaseModel] | None] | None = None,
    ) -> None:
        self._by_key: dict[str, list[_InlineResult]] = {}
        for u in upstream:
            self._by_key.setdefault(u.key, []).append(_InlineResult(u.job_id, u.result_json))
        self._lazy_fetch = lazy_fetch
        self._schema_resolver = schema_resolver
        self._memo: dict[str, Any] = {}

    def _typed(self, key: str, value: dict[str, Any]) -> Any:
        if self._schema_resolver is not None:
            model = self._schema_resolver(key)
            if model is not None:
                return model.model_validate(value)
        return value

    def _inline(self, key: str) -> Any:
        """Resolve the inline tier only; raise ``KeyError`` on a miss."""
        if key in self._memo:
            return self._memo[key]
        matches = self._by_key.get(key)
        if not matches:
            raise KeyError(key)
        if len(matches) > 1:
            raise AmbiguousResultKey(
                f"upstream key {key!r} matches {len(matches)} producers; "
                f"declare an alias in depends_on to disambiguate"
            )
        value = self._typed(key, _json.loads(matches[0].raw))
        self._memo[key] = value
        return value

    def __getitem__(self, key: str) -> Any:
        try:
            return self._inline(key)
        except KeyError:
            raise KeyError(
                f"no inline upstream result for {key!r}; declare it in depends_on for inline "
                f"delivery, or use `await ctx.output.fetch({key!r})` for the lazy GetResult tier"
            ) from None

    async def fetch(self, key: str) -> Any:
        """Inline-first, then the lazy ``GetResult`` tier; ``KeyError`` if neither (spec 10.3)."""
        try:
            return self._inline(key)
        except KeyError:
            pass
        if self._lazy_fetch is not None:
            value = await self._lazy_fetch(key)
            if value is not None:
                typed = self._typed(key, value)
                self._memo[key] = typed
                return typed
        raise KeyError(
            f"no upstream result for {key!r} in the inline or lazy tier; it is not an "
            f"ancestor of this job (or produced no result)"
        )

    def __iter__(self) -> Iterator[str]:
        # iteration deliberately does NOT trigger lazy fetches (spec 10.3)
        return iter(self._by_key)

    def __len__(self) -> int:
        return len(self._by_key)


class Ctx:
    """Per-execution handler context (spec 10.1)."""

    def __init__(
        self,
        *,
        job_id: str,
        ctx_id: str,
        task_name: str,
        attempt: int,
        tenant: str,
        payload: Any,
        output: UpstreamOutputs,
        logger: structlog.stdlib.BoundLogger,
        pipeline: str | None = None,
        stage: str | None = None,
        group_key: str | None = None,
        event_payload: dict[str, Any] | None = None,
        checkpoint_data: dict[str, Any] | None = None,
        idempotency_key: str = "",
        idempotency_key_attempt: str = "",
        heartbeat_shell: HeartbeatShell | None = None,
        backend: CtxBackend | None = None,
        profile: str = "io",
    ) -> None:
        self.job_id = job_id
        self.ctx_id = ctx_id
        self.task_name = task_name
        self.attempt = attempt
        self.tenant = tenant
        self.pipeline = pipeline
        self.stage = stage
        self.group_key = group_key
        self.payload = payload
        self.output = output
        self.event_payload = event_payload
        self.checkpoint_data = checkpoint_data
        self.idempotency_key = idempotency_key
        self.idempotency_key_attempt = idempotency_key_attempt
        self.logger = logger
        self.profile = profile
        self._heartbeat_shell = heartbeat_shell
        self._backend = backend

    # ------------------------------------------------------------ sentinels
    def stop_chain(self, result: dict[str, Any] | None = None) -> StopChain:
        """Return this to complete AND drop the chain tail (spec 10.2)."""
        return StopChain(result)

    def skip(self) -> Skip:
        """Return this to succeed without work; the chain continues (spec 10.2)."""
        return Skip()

    # -------------------------------------------------------------- verbs
    async def heartbeat(self) -> None:
        """Manually poke the heartbeat shell (tight CPU loops in io handlers, spec 12)."""
        if self._heartbeat_shell is not None:
            await self._heartbeat_shell.beat_now()

    async def checkpoint(self, data: dict[str, Any]) -> None:
        """Persist intermediate results so a resume never repays for them (spec 13.1).

        Fast path (Redis, when configured) + durable ``PutCheckpoint``; without Redis
        the durable write is awaited. The pre-loaded read side is :attr:`checkpoint_data`.
        """
        await self._require_backend().checkpoint(data)

    async def wait_for_event(self, key: str, timeout_s: int) -> dict[str, Any] | None:
        """Park until an external ``signal(key, ...)`` arrives, or ``timeout_s`` elapses.

        This is NOT in-place suspension: on resume the handler **re-runs from the top**
        (spec 14). Checkpoint expensive pre-wait work. If a signal was already pending
        the payload returns inline without parking; on timeout the call returns ``None``.
        Raises :class:`~symba.errors.UnsupportedInProfile` in cpu/gpu handlers.
        """
        return await self._require_backend().wait_for_event(key, timeout_s)

    async def submit(self, **spec: Any) -> JobHandle:
        """Spawn a job that inherits this execution's ctx_id/tenant/pipeline (spec 10.1)."""
        return await self._require_backend().submit(spec)

    async def submit_children(
        self,
        children: list[dict[str, Any]],
        on_complete: dict[str, Any] | None = None,
        gate_policy: str = "all_success",
    ) -> Gate:
        """Fan out children + a gate continuation from inside a handler (spec 10.1)."""
        return await self._require_backend().submit_children(children, on_complete, gate_policy)

    def _require_backend(self) -> CtxBackend:
        if self._backend is None:
            raise UnsupportedInProfile(
                "ctx.submit/submit_children need an engine backend; this Ctx was built "
                "without one (test contexts should inject a fake backend)"
            )
        return self._backend


__all__ = ["Ctx", "UpstreamOutputs", "StopChain", "Skip"]
