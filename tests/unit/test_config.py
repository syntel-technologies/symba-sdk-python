"""Configuration tests (spec 19)."""

from __future__ import annotations

import pytest

from symba.config import load_settings
from symba.errors import ConfigError


def test_defaults():
    s = load_settings()
    assert s.engine.target == "grpc://localhost:7233"
    assert s.worker.slots is None
    assert s.grpc.max_receive_mb == 16


def test_kwargs_win_over_defaults():
    s = load_settings(engine={"target": "grpcs://prod:7233", "tls": True})
    assert s.engine.target == "grpcs://prod:7233"
    assert s.engine.tls is True


def test_env_is_read(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SYMBA_ENGINE__TARGET", "grpc://from-env:7233")
    monkeypatch.setenv("SYMBA_WORKER__SLOTS", "42")
    s = load_settings()
    assert s.engine.target == "grpc://from-env:7233"
    assert s.worker.slots == 42


def test_kwargs_beat_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SYMBA_WORKER__SLOTS", "10")
    s = load_settings(worker={"slots": 99})
    assert s.worker.slots == 99


def test_top_level_token_flows_to_engine(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SYMBA_TOKEN", "secret")
    s = load_settings()
    assert s.engine.token == "secret"


def test_invalid_value_raises_config_error():
    with pytest.raises(ConfigError) as exc:
        load_settings(worker={"slots": 0})  # ge=1
    assert "worker.slots" in str(exc.value)
    assert exc.value.error_code == "config_error"
