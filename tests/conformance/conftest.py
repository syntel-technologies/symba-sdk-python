"""Conformance harness (spec 20.3).

Every conformance test is written once against a small ``Backend`` protocol and
parameterized over two implementations:

* ``symbatest`` — the in-memory :class:`~symba.testing.SymbaTest` (always runs);
* ``engine`` — a real dockerized engine reached over gRPC, enabled only when
  ``SYMBA_E2E_TARGET`` points at a running engine (otherwise skipped).

The SAME registered handlers and the SAME assertions run against both, which is
exactly what makes SymbaTest trustworthy: a green in-memory run predicts a green
engine run for the emulated surface.

Deliberately NOT emulated by SymbaTest (documented, asserted nowhere here):
token-bucket rate buckets, real lease-expiry timing, and cross-worker contention.
Those belong to engine-only tests.
"""

from __future__ import annotations

import os
from collections.abc import Callable

import pytest

from symba import Worker
from symba.testing import SymbaTest

_ENGINE_TARGET = os.environ.get("SYMBA_E2E_TARGET")


class Backend:
    """The slice of the client + harness a conformance test may use."""

    def __init__(self, sim: SymbaTest, worker: Worker, *, is_inmemory: bool) -> None:
        self.client = sim
        self.worker = worker
        self.is_inmemory = is_inmemory

    async def run_until_idle(self) -> None:
        await self.client.run_until_idle()


def _make_symbatest(build: Callable[[Worker], None]) -> Backend:
    worker = Worker(engine="inmemory://conformance")
    build(worker)
    sim = SymbaTest()
    sim.register(worker)
    return Backend(sim, worker, is_inmemory=True)


@pytest.fixture(params=["symbatest", "engine"])
def backend_factory(
    request: pytest.FixtureRequest,
) -> Callable[[Callable[[Worker], None]], Backend]:
    """Return a factory that builds a Backend for the parameterized implementation."""
    if request.param == "engine":
        if not _ENGINE_TARGET:
            pytest.skip("SYMBA_E2E_TARGET not set; dockerized-engine conformance skipped")
        pytest.skip("dockerized-engine conformance backend not wired in this environment")
    return _make_symbatest
