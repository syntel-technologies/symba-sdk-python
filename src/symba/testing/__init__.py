"""In-process test engine (spec 20).

``SymbaTest`` runs the SDK's real dispatch pipeline against an in-memory job table —
no gRPC, no Postgres, no Redis. Only transport + persistence are faked; schema
validation, middleware, error classification, chains, fan-out gates, retries,
wait/signal, checkpoints, dedup and cancellation behave as in production.
"""

from __future__ import annotations

from symba.testing.fake_engine import SymbaTest

__all__ = ["SymbaTest"]
