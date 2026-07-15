"""Transport target parsing + channel discipline (spec 5)."""

from __future__ import annotations

from symba.transport import parse_target


def test_parse_grpc_insecure():
    p = parse_target("grpc://localhost:7233")
    assert p.tls is False
    assert p.authority == "localhost:7233"
    assert p.host == "localhost"


def test_parse_grpcs_tls():
    p = parse_target("grpcs://prod.internal:7233")
    assert p.tls is True
    assert p.host == "prod.internal"


def test_parse_bare_hostport():
    p = parse_target("host:7233")
    assert p.tls is False
    assert p.authority == "host:7233"
