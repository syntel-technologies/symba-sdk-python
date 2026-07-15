"""Public value types (spec 6, 7).

Plain dataclasses / enums that mirror the proto wire messages but keep the proto
types out of the public API — users never import from ``symba._proto``.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class JobState(enum.Enum):
    """Mirror of ``common.proto`` ``JobState`` (spec 4.2)."""

    UNSPECIFIED = 0
    SUBMITTED = 1
    QUEUED = 2
    RUNNING = 3
    WAITING = 4
    SUCCEEDED = 5
    DEAD = 6
    CANCELLED = 7

    @property
    def is_terminal(self) -> bool:
        return self in (JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED)


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    """Dataclass mirror of the proto ``RetryPolicy`` message (spec 6.1).

    A field left ``None`` is sent as the proto zero value so the engine's
    ``[defaults]`` apply — the SDK never bakes its own copy of engine defaults
    (spec 4.2).
    """

    max_attempts: int | None = None
    backoff_base_s: float | None = None
    backoff_factor: float | None = None
    backoff_max_s: float | None = None
    jitter: bool | None = None


@dataclass(slots=True)
class JobStatus:
    """Snapshot returned by ``JobHandle.status()`` — one atomic ``GetJob`` (spec 7.1)."""

    id: str
    task_name: str
    state: JobState
    attempt: int
    ctx_id: str | None = None
    tenant: str | None = None
    last_error: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True)
class CancelOutcome:
    """Result of a cancel (spec 6.6/7.1)."""

    previous_state: JobState
    cancelled: bool
    note: str = ""


@dataclass(slots=True)
class GateStatus:
    """Fan-out gate view (spec 7.2)."""

    gate_id: str
    expected: int
    terminal: int
    succeeded: int
    fired_at: datetime | None = None


@dataclass(slots=True)
class JobEvent:
    """One row of a job's event ledger (spec 6.5)."""

    job_id: str
    event: str
    at: datetime | None = None
    detail: dict[str, Any] = field(default_factory=dict)


__all__ = [
    "JobState",
    "RetryPolicy",
    "JobStatus",
    "CancelOutcome",
    "GateStatus",
    "JobEvent",
]
