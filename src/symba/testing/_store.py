"""In-memory job table + fake clock for :class:`SymbaTest` (spec 20.1).

A dict-backed store keyed by ``job_id``, with the bookkeeping the engine owns:
chains, fan-out gates, dedup identities, signals-in-waiting, checkpoints, and a
per-job event ledger. Backoff is measured against a :class:`FakeClock` so retry
delays of minutes elapse instantly while preserving ordering (spec 20.2).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

from symba._proto import common_pb2
from symba.types import JobState


class FakeClock:
    """Monotonic virtual clock; ``advance`` fast-forwards backoff windows (spec 20.2)."""

    def __init__(self) -> None:
        self._now = 0.0

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += max(0.0, seconds)


@dataclass(slots=True)
class JobRecord:
    """One row of the in-memory job table (mirrors the engine's job row, spec 20.1)."""

    id: str
    spec: common_pb2.JobSpec
    tenant: str
    state: JobState = JobState.QUEUED
    attempt: int = 1
    result: bytes = b""
    skipped: bool = False
    last_error: str = ""
    error_history: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    #: chain tail task names still to run after this job (spec 20.1).
    chain_tail: list[str] = field(default_factory=list)
    #: resolved upstream (alias/task_name -> (job_id, result_bytes)) for ctx.output.
    upstream: list[common_pb2.UpstreamResult] = field(default_factory=list)
    #: fan-out gate this job belongs to, if any.
    gate_id: str | None = None
    #: wait key this job is parked on (WAITING), if any.
    wait_key: str | None = None
    #: consumed signal payload delivered on a WAITING resume.
    event_payload: bytes = b""
    #: next monotonic time this job is eligible to run (backoff / run_at).
    run_after: float = 0.0

    def log(self, event: str, **detail: Any) -> None:
        self.events.append({"event": event, "detail": detail})


@dataclass(slots=True)
class GateRecord:
    """Fan-out gate state (spec 7.2, 20.1)."""

    id: str
    ctx_id: str
    child_ids: list[str]
    gate_policy: str
    on_complete: common_pb2.JobSpec | None
    continuation_task: str | None
    fired: bool = False
    continuation_job_id: str | None = None


class MemStore:
    """The whole in-memory engine state (spec 20.1)."""

    def __init__(self) -> None:
        self.jobs: dict[str, JobRecord] = {}
        self.gates: dict[str, GateRecord] = {}
        #: dedup identity -> job_id (first submit wins; duplicates return it).
        self.dedup: dict[str, str] = {}
        #: wait_key -> pending signal payload (signal-first rendezvous, spec 14.1).
        self.pending_signals: dict[str, bytes] = {}
        #: job_id -> latest checkpoint bytes (dict-backed, no Redis — spec 20.2).
        self.checkpoints: dict[str, bytes] = {}
        self.clock = FakeClock()
        self._job_seq = itertools.count(1)
        self._gate_seq = itertools.count(1)

    def new_job_id(self) -> str:
        return f"job-{next(self._job_seq)}"

    def new_gate_id(self) -> str:
        return f"gate-{next(self._gate_seq)}"

    def dedup_identity(self, tenant: str, spec: common_pb2.JobSpec) -> str | None:
        if not spec.dedup_key:
            return None
        return f"{tenant}:{spec.dedup_key}"


__all__ = ["MemStore", "JobRecord", "GateRecord", "FakeClock"]
