"""Idempotency key derivation (spec 10.1, AD-21).

MUST mirror the engine's ``core/idempotency.py`` byte-for-byte — a conformance
test pins identical output for identical inputs against a shared vector table
(spec 3, rule 4). The derivation is::

    sha256(f"{tenant}:{dedup_key or job_id}").hexdigest()[:32]

The key is STABLE across retries and duplicate submits (same ``dedup_key`` =>
same key), which is exactly what makes checkpoints and external-API dedup work.
"""

from __future__ import annotations

import hashlib


def derive_key(*, tenant: str, dedup_key: str | None, job_id: str) -> str:
    """Stable idempotency key for a logical job (spec 10.1)."""
    identity = dedup_key or job_id
    digest = hashlib.sha256(f"{tenant}:{identity}".encode()).hexdigest()
    return digest[:32]


def attempt_key(idempotency_key: str, attempt: int) -> str:
    """Per-attempt key for APIs where a retry SHOULD be a new operation (spec 10.1)."""
    return f"{idempotency_key}-a{attempt}"


__all__ = ["derive_key", "attempt_key"]
