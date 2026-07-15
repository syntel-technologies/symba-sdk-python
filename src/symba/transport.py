"""Transport layer (spec 5).

One module owns every gRPC channel in the SDK — nothing else creates channels.
Responsibilities:

* channel construction with the engine-mirrored keepalive / reconnect options
  (spec 5.1);
* scheme handling (``grpc://`` insecure, ``grpcs://`` TLS) + a one-time WARNING
  for non-loopback insecure targets;
* bearer-token metadata + the ``sdk_version`` handshake header on every call
  (spec 4.3, 5.1) via a client interceptor;
* event-loop discipline — a channel is bound to the loop it was created on, and
  cross-loop use raises :class:`WrongEventLoop` instead of the classic silent
  grpc.aio hang (spec 5.3).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urlparse

import grpc

from ._version import SDK_VERSION_STRING
from .config import EngineSettings, GrpcSettings
from .errors import WrongEventLoop
from .logging import get_logger

_log = get_logger(component="transport")

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", ""})
_warned_insecure: set[str] = set()


@dataclass(slots=True)
class TlsConfig:
    """TLS material for ``grpcs://`` targets (system trust unless a CA is given)."""

    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None


@dataclass(slots=True)
class ParsedTarget:
    authority: str
    tls: bool
    host: str


def parse_target(target: str) -> ParsedTarget:
    """Split ``grpc(s)://host:port`` / bare ``host:port`` into its parts."""
    if "://" in target:
        parsed = urlparse(target)
        scheme = parsed.scheme
        authority = parsed.netloc
        host = parsed.hostname or ""
        tls = scheme == "grpcs"
    else:
        authority = target
        host = target.rsplit(":", 1)[0]
        tls = False
    return ParsedTarget(authority=authority, tls=tls, host=host)


def _channel_options(grpc_cfg: GrpcSettings) -> list[tuple[str, int]]:
    return [
        ("grpc.keepalive_time_ms", int(grpc_cfg.keepalive_time_s * 1000)),
        ("grpc.keepalive_timeout_ms", int(grpc_cfg.keepalive_timeout_s * 1000)),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.max_receive_message_length", grpc_cfg.max_receive_mb * 1024 * 1024),
        ("grpc.max_send_message_length", grpc_cfg.max_send_mb * 1024 * 1024),
        ("grpc.initial_reconnect_backoff_ms", int(grpc_cfg.initial_reconnect_backoff_s * 1000)),
        ("grpc.max_reconnect_backoff_ms", int(grpc_cfg.max_reconnect_backoff_s * 1000)),
    ]


class _AuthInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
):
    """Injects the bearer token + ``sdk_version`` header on every call (spec 4.3)."""

    def __init__(self, token: str | None) -> None:
        self._token = token

    def _augment(
        self, client_call_details: grpc.aio.ClientCallDetails
    ) -> grpc.aio.ClientCallDetails:
        metadata = list(client_call_details.metadata or [])
        metadata.append(("sdk_version", SDK_VERSION_STRING))
        if self._token:
            metadata.append(("authorization", f"Bearer {self._token}"))
        return client_call_details._replace(metadata=metadata)  # type: ignore[attr-defined]

    async def intercept_unary_unary(self, continuation, client_call_details, request):  # type: ignore[override]
        return await continuation(self._augment(client_call_details), request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):  # type: ignore[override]
        return await continuation(self._augment(client_call_details), request)

    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):  # type: ignore[override]
        return await continuation(self._augment(client_call_details), request_iterator)


def _credentials(parsed: ParsedTarget, tls: TlsConfig | None) -> grpc.ChannelCredentials:
    root = tls.ca_file if tls else None
    private_key = None
    cert_chain = None
    if tls and tls.cert_file and tls.key_file:
        with open(tls.key_file, "rb") as f:
            private_key = f.read()
        with open(tls.cert_file, "rb") as f:
            cert_chain = f.read()
    root_bytes = None
    if root:
        with open(root, "rb") as f:
            root_bytes = f.read()
    return grpc.ssl_channel_credentials(
        root_certificates=root_bytes,
        private_key=private_key,
        certificate_chain=cert_chain,
    )


class Transport:
    """Owns a single lazy ``grpc.aio`` channel bound to one event loop (spec 5.3)."""

    def __init__(
        self,
        engine: EngineSettings,
        grpc_cfg: GrpcSettings,
        *,
        tls: TlsConfig | None = None,
    ) -> None:
        self._engine = engine
        self._grpc_cfg = grpc_cfg
        self._tls = tls
        self._parsed = parse_target(engine.target)
        self._channel: grpc.aio.Channel | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def target(self) -> str:
        return self._parsed.authority

    def _ensure_loop(self) -> None:
        current = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = current
        elif self._loop is not current:
            raise WrongEventLoop()

    def channel(self) -> grpc.aio.Channel:
        """Return the channel, creating it lazily on first use (spec 5.1)."""
        self._ensure_loop()
        if self._channel is not None:
            return self._channel

        options = _channel_options(self._grpc_cfg)
        interceptors = [_AuthInterceptor(self._engine.token)]
        use_tls = self._parsed.tls or self._engine.tls

        if use_tls:
            creds = _credentials(self._parsed, self._tls)
            self._channel = grpc.aio.secure_channel(
                self._parsed.authority, creds, options=options, interceptors=interceptors
            )
        else:
            if (
                self._parsed.host not in _LOOPBACK_HOSTS
                and self._parsed.authority not in _warned_insecure
            ):
                _warned_insecure.add(self._parsed.authority)
                _log.warning(
                    "insecure_channel_non_loopback",
                    target=self._parsed.authority,
                    hint="use grpcs:// for non-local engines",
                )
            self._channel = grpc.aio.insecure_channel(
                self._parsed.authority, options=options, interceptors=interceptors
            )
        return self._channel

    async def aclose(self) -> None:
        if self._channel is not None:
            await self._channel.close(grace=None)
            self._channel = None


__all__ = ["Transport", "TlsConfig", "ParsedTarget", "parse_target"]
