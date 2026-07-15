"""Ctx proxy + parent<->child frame protocol (spec 11.4).

cpu/gpu handlers run in a child process that owns no gRPC channel. Identity/data
(payload, inline ``ctx.output``, ``checkpoint_data``, idempotency keys) are pickled
across at job start as a plain snapshot; **verbs marshal back over the pipe** to the
parent, which owns the channel. ``wait_for_event`` is the one verb that cannot cross
the boundary — it raises :class:`~symba.errors.UnsupportedInProfile` (coordination
belongs in io tasks, spec 10.3 / engine spec 11.4).

The frames (length-prefixed pickle over a duplex ``multiprocessing.Connection``):

* downward: ``RunJob`` (start), ``CtxReply`` (verb result), ``Shutdown`` (gpu only)
* upward:   ``CtxCall`` (a verb to run in the parent), ``LogRecord`` (structured log),
  ``JobDone`` (result or exc_info)

One in-flight job per child keeps the protocol trivially ordered: a child alternates
between running handler code and blocking on a single ``CtxReply``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from symba.errors import SymbaError, UnsupportedInProfile

if TYPE_CHECKING:
    from multiprocessing.connection import Connection


# --------------------------------------------------------------------------- frames
@dataclass(slots=True)
class CtxSnapshot:
    """The plain, picklable slice of ``Ctx`` a child needs (spec 11.4)."""

    job_id: str
    ctx_id: str
    task_name: str
    attempt: int
    tenant: str
    pipeline: str | None
    stage: str | None
    group_key: str | None
    payload: Any
    inline_output: dict[str, Any]
    checkpoint_data: dict[str, Any] | None
    event_payload: dict[str, Any] | None
    idempotency_key: str
    idempotency_key_attempt: str
    profile: str


@dataclass(slots=True)
class RunJob:
    task_name: str
    snapshot: CtxSnapshot
    payload: Any


@dataclass(slots=True)
class CtxCall:
    """A verb the child asks the parent to execute on its behalf."""

    verb: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CtxReply:
    ok: bool
    value: Any = None
    error: str | None = None
    error_type: str | None = None


@dataclass(slots=True)
class LogRecord:
    level: str
    event: str
    fields: dict[str, Any]


@dataclass(slots=True)
class JobDone:
    ok: bool
    result: Any = None
    #: (error_type, message) — the child cannot pickle live tracebacks safely.
    error_type: str | None = None
    error_message: str | None = None
    retryable: bool | None = None


@dataclass(slots=True)
class Shutdown:
    pass


# --------------------------------------------------------------------- child-side ctx
class _ChildLogger:
    """A stdlib-ish logger that ships structured records up the pipe (spec 11.4, 18)."""

    def __init__(self, conn: Connection, base: dict[str, Any]) -> None:
        self._conn = conn
        self._base = base

    def _emit(self, level: str, event: str, **fields: Any) -> None:
        merged = {**self._base, **fields}
        merged.pop("exc_info", None)  # tracebacks don't pickle cleanly across the pipe
        self._conn.send(LogRecord(level=level, event=event, fields=merged))

    def debug(self, event: str, **f: Any) -> None:
        self._emit("debug", event, **f)

    def info(self, event: str, **f: Any) -> None:
        self._emit("info", event, **f)

    def warning(self, event: str, **f: Any) -> None:
        self._emit("warning", event, **f)

    def error(self, event: str, **f: Any) -> None:
        self._emit("error", event, **f)

    def bind(self, **_f: Any) -> _ChildLogger:
        return self


class CtxProxy:
    """The ``ctx`` a cpu/gpu handler sees (spec 11.4).

    Data fields are plain locals from the snapshot; verbs synchronously round-trip a
    :class:`CtxCall` up the pipe and block for the :class:`CtxReply`. It mirrors the
    public :class:`~symba.context.Ctx` surface a compute handler is allowed to touch.
    """

    def __init__(self, snapshot: CtxSnapshot, conn: Connection) -> None:
        self._conn = conn
        self.job_id = snapshot.job_id
        self.ctx_id = snapshot.ctx_id
        self.task_name = snapshot.task_name
        self.attempt = snapshot.attempt
        self.tenant = snapshot.tenant
        self.pipeline = snapshot.pipeline
        self.stage = snapshot.stage
        self.group_key = snapshot.group_key
        self.payload = snapshot.payload
        self.output = _ProxyOutputs(snapshot.inline_output, self._call)
        self.event_payload = snapshot.event_payload
        self.checkpoint_data = snapshot.checkpoint_data
        self.idempotency_key = snapshot.idempotency_key
        self.idempotency_key_attempt = snapshot.idempotency_key_attempt
        self.profile = snapshot.profile
        self.logger = _ChildLogger(
            conn, {"job_id": snapshot.job_id, "task_name": snapshot.task_name}
        )

    def _call(self, verb: str, **args: Any) -> Any:
        """Marshal one verb to the parent and block for its reply (spec 11.4)."""
        self._conn.send(CtxCall(verb=verb, args=args))
        reply: CtxReply = self._conn.recv()
        if not reply.ok:
            raise SymbaError(reply.error or f"ctx.{verb} failed in the parent process")
        return reply.value

    # cpu/gpu handlers are plain `def`; verbs are synchronous from their view.
    def checkpoint(self, data: dict[str, Any]) -> None:
        self._call("checkpoint", data=data)

    def heartbeat(self) -> None:
        self._call("heartbeat")

    def submit(self, **spec: Any) -> Any:
        return self._call("submit", spec=spec)

    def submit_children(
        self,
        children: list[dict[str, Any]],
        on_complete: dict[str, Any] | None = None,
        gate_policy: str = "all_success",
    ) -> Any:
        return self._call(
            "submit_children",
            children=children,
            on_complete=on_complete,
            gate_policy=gate_policy,
        )

    def wait_for_event(self, key: str, timeout_s: int) -> dict[str, Any] | None:
        raise UnsupportedInProfile(
            f"ctx.wait_for_event is not available in {self.profile!r} profile: compute "
            f"tasks compute, coordination belongs in an io task (spec 11.4). Move the wait "
            f"to an io handler that submits this compute job."
        )

    def stop_chain(self, result: dict[str, Any] | None = None) -> Any:
        from symba.context import StopChain

        return StopChain(result)

    def skip(self) -> Any:
        from symba.context import Skip

        return Skip()


class _ProxyOutputs:
    """``ctx.output`` for a child: inline tier is local, lazy tier marshals up (spec 10.3)."""

    def __init__(self, inline: dict[str, Any], call: Any) -> None:
        self._inline = inline
        self._call = call

    def __getitem__(self, key: str) -> Any:
        if key in self._inline:
            return self._inline[key]
        raise KeyError(
            f"no inline upstream result for {key!r}; use ctx.output.fetch({key!r}) "
            f"for the lazy tier (it marshals through the parent)"
        )

    def __contains__(self, key: object) -> bool:
        return key in self._inline

    def __iter__(self) -> Any:
        return iter(self._inline)

    def __len__(self) -> int:
        return len(self._inline)

    def get(self, key: str, default: Any = None) -> Any:
        return self._inline.get(key, default)

    def fetch(self, key: str) -> Any:
        if key in self._inline:
            return self._inline[key]
        value = self._call("resolve_upstream", key=key)
        if value is None:
            raise KeyError(f"no upstream result for {key!r} in inline or lazy tier")
        return value


__all__ = [
    "CtxSnapshot",
    "RunJob",
    "CtxCall",
    "CtxReply",
    "LogRecord",
    "JobDone",
    "Shutdown",
    "CtxProxy",
]
