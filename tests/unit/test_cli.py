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
