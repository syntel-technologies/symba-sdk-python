"""Unit tests for the CLI (spec 22).

Commands that don't touch the network (``version``, ``tasks``) run end-to-end via
Typer's ``CliRunner``; connectivity commands are covered by integration tests.
"""

from __future__ import annotations

import sys
import types

import pytest
from typer.testing import CliRunner

from symba.cli import app

runner = CliRunner()


def test_version_command_prints_versions():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "sdk" in result.stdout
    assert "engine_protocol" in result.stdout


def test_tasks_command_lists_registry(monkeypatch: pytest.MonkeyPatch):
    module = types.ModuleType("cli_worker_fixture")
    exec(  # building a throwaway worker module for the CLI import path
        "from symba import Worker\n"
        "worker = Worker(engine='grpc://localhost:1')\n"
        "@worker.task('parse')\n"
        "async def parse(ctx, payload):\n"
        "    return {}\n",
        module.__dict__,
    )
    monkeypatch.setitem(sys.modules, "cli_worker_fixture", module)

    result = runner.invoke(app, ["tasks", "cli_worker_fixture:worker"])
    assert result.exit_code == 0
    assert "parse" in result.stdout
    assert "io" in result.stdout


def test_submit_rejects_bad_json():
    result = runner.invoke(app, ["submit", "task", "--payload", "{not json}"])
    assert result.exit_code != 0


def test_tasks_bad_reference_errors():
    result = runner.invoke(app, ["tasks", "no_colon_here"])
    assert result.exit_code != 0


def test_doctor_reports_staged_failure_without_hanging(monkeypatch: pytest.MonkeyPatch):
    """ENG-1: an unanswered probe yields a staged FAIL and a nonzero exit, not a hang."""
    import symba.sync as sync_mod
    from symba.engine import ProbeResult

    def fake_probe(self, *, timeout_s: float = 8.0):
        return ProbeResult(ok=False, stage="unanswered", detail="no response within 8.0s")

    monkeypatch.setattr(sync_mod.SyncEngine, "probe", fake_probe, raising=True)
    monkeypatch.setattr(sync_mod.SyncEngine, "close", lambda self: None, raising=True)

    result = runner.invoke(app, ["doctor", "--engine", "grpc://localhost:1"])
    assert result.exit_code == 1
    assert "FAIL" in result.stdout


def test_doctor_passes_when_probe_ok(monkeypatch: pytest.MonkeyPatch):
    """ENG-1: an answered probe passes the grpc_reachable check."""
    import symba.sync as sync_mod
    from symba.engine import ProbeResult

    monkeypatch.setattr(
        sync_mod.SyncEngine,
        "probe",
        lambda self, *, timeout_s=8.0: ProbeResult(ok=True, stage="answered", detail="ok"),
        raising=True,
    )
    monkeypatch.setattr(sync_mod.SyncEngine, "close", lambda self: None, raising=True)

    result = runner.invoke(app, ["doctor", "--engine", "grpc://localhost:1"])
    assert result.exit_code == 0
    assert "PASS" in result.stdout


def test_classify_probe_error_maps_stages():
    """ENG-1: gRPC status codes map to distinct diagnostic stages."""
    import grpc

    from symba.engine import _classify_probe_error

    class _Err(grpc.aio.AioRpcError):
        def __init__(self, code):
            self._c = code

        def code(self):
            return self._c

        def details(self):
            return ""

    assert _classify_probe_error(_Err(grpc.StatusCode.NOT_FOUND), 8.0).ok is True
    assert _classify_probe_error(_Err(grpc.StatusCode.DEADLINE_EXCEEDED), 8.0).stage == "unanswered"
    assert (
        _classify_probe_error(_Err(grpc.StatusCode.UNIMPLEMENTED), 8.0).stage
        == "method-unimplemented"
    )
    assert _classify_probe_error(_Err(grpc.StatusCode.UNAVAILABLE), 8.0).stage == "dial"
    assert _classify_probe_error(_Err(grpc.StatusCode.UNAUTHENTICATED), 8.0).stage == "auth"
