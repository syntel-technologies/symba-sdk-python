"""Transport target parsing + channel discipline (spec 5)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import mock_open

import grpc
import pytest

from symba._version import SDK_VERSION_STRING
from symba.config import EngineSettings, GrpcSettings
from symba.errors import ConfigError
from symba.transport import Transport, parse_target


def test_tls_config_is_part_of_public_sdk_surface() -> None:
    from symba import TlsConfig
    from symba.transport import TlsConfig as TransportTlsConfig

    assert TlsConfig is TransportTlsConfig


def test_parse_grpc_insecure():
    p = parse_target("grpc://localhost:7233")
    assert p.tls is False
    assert p.authority == "localhost:7233"
    assert p.host == "localhost"


def test_parse_grpcs_tls():
    p = parse_target("grpcs://prod.internal:7233")
    assert p.tls is True
    assert p.host == "prod.internal"


def test_parse_dns_preserves_resolver_target():
    p = parse_target("dns:///symba.internal:7233")
    assert p.authority == "dns:///symba.internal:7233"
    assert p.host == "symba.internal"
    assert p.tls is False


def test_parse_inmemory_preserves_symbatest_backend():
    p = parse_target("inmemory://conformance")
    assert p.authority == "inmemory://conformance"
    assert p.host == "conformance"
    assert p.tls is False


@pytest.mark.parametrize("target", ("", "https://symba.internal:7233", "grpc:///missing"))
def test_invalid_targets_fail_at_construction(target: str):
    with pytest.raises(ConfigError):
        Transport(EngineSettings(target=target), GrpcSettings())


@pytest.mark.asyncio
async def test_engine_settings_supply_tls_material(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, bytes | None] = {}

    def _credentials(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("symba.transport.grpc.ssl_channel_credentials", _credentials)
    monkeypatch.setattr("symba.transport.grpc.aio.secure_channel", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("builtins.open", mock_open(read_data=b"pem-data"))
    transport = Transport(
        EngineSettings(
            target="grpcs://symba.internal:7233",
            tls_ca_file="/runtime/ca.crt",
            tls_cert_file="/runtime/client.crt",
            tls_key_file="/runtime/client.key",
        ),
        GrpcSettings(),
    )

    transport.channel()

    assert captured == {
        "root_certificates": b"pem-data",
        "private_key": b"pem-data",
        "certificate_chain": b"pem-data",
    }


@pytest.mark.asyncio
async def test_partial_client_identity_fails_closed(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("builtins.open", mock_open(read_data=b"pem-data"))
    transport = Transport(
        EngineSettings(
            target="grpcs://symba.internal:7233",
            tls_cert_file="/runtime/client.crt",
        ),
        GrpcSettings(),
    )

    with pytest.raises(ConfigError, match="both certificate and private key"):
        transport.channel()


def test_parse_bare_hostport():
    p = parse_target("host:7233")
    assert p.tls is False
    assert p.authority == "host:7233"


@pytest.mark.asyncio
async def test_bidirectional_stream_sends_bearer_and_sdk_metadata() -> None:
    """The worker Claim stream must authenticate, not only unary RPCs.

    grpc.aio classifies an interceptor through an ``if``/``elif`` chain. This
    real channel/server test prevents a multi-cardinality interceptor from
    silently being installed for unary calls only.
    """

    observed: dict[str, str] = {}

    async def claim(
        request_iterator: AsyncIterator[bytes],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[bytes]:
        for key, value in context.invocation_metadata() or ():
            observed[key] = value.decode() if isinstance(value, bytes) else value
        async for request in request_iterator:
            yield request
            return

    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "symba.test.Auth",
                {
                    "Claim": grpc.stream_stream_rpc_method_handler(
                        claim,
                        request_deserializer=lambda value: value,
                        response_serializer=lambda value: value,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    transport = Transport(
        EngineSettings(
            target=f"grpc://127.0.0.1:{port}",
            token="stream-secret",
        ),
        GrpcSettings(),
    )

    async def requests() -> AsyncIterator[bytes]:
        yield b"claim"

    try:
        claim_rpc: grpc.aio.StreamStreamMultiCallable[bytes, bytes] = (
            transport.channel().stream_stream("/symba.test.Auth/Claim")
        )
        call = claim_rpc(requests())
        assert await call.read() == b"claim"
        assert observed["authorization"] == "Bearer stream-secret"
        assert observed["sdk_version"] == SDK_VERSION_STRING
    finally:
        await transport.aclose()
        await server.stop(grace=None)
