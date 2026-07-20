"""Configuration (spec 19).

Same philosophy as the engine (``pydantic-settings``, nested via ``__``), but the
SDK is a *library*: **constructor arguments always win** over environment. The
precedence is kwargs > ``SYMBA_*`` env vars > ``.env`` (opt-in) > defaults.

Validation happens at construction, not first use. ``Engine(...)`` / ``Worker(...)``
funnel invalid settings through :func:`load_settings`, which re-wraps pydantic's
``ValidationError`` into a :class:`symba.errors.ConfigError` that lists every bad
field at once — users never see a raw pydantic traceback.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import ConfigError


class EngineSettings(BaseModel):
    """Connection to the engine (data + control plane)."""

    #: gRPC target. Accepts ``host:port``, ``grpc://host:port`` (insecure),
    #: ``grpcs://host:port`` (TLS), and ``dns:///`` forms (spec 5.1).
    target: str = "grpc://localhost:7233"
    #: HTTP control-plane / admin base URL (spec 19).
    http_url: str = "http://localhost:7300"
    #: Bearer token; falls back to ``SYMBA_TOKEN`` (see :class:`SdkSettings`).
    token: str | None = None
    #: TLS toggle; when true the channel uses system trust unless a CA is given.
    tls: bool = False
    tls_ca_file: str | None = None
    tls_cert_file: str | None = None
    tls_key_file: str | None = None


class WorkerSettings(BaseModel):
    """Worker-process runtime knobs (spec 8.1)."""

    name: str | None = None
    #: ``None`` lets the profile decide (spec 11); an explicit value always wins.
    slots: int | None = Field(default=None, ge=1)
    #: Size of the cpu-profile forkserver pool (spec 11.1). SEPARATE from
    #: ``slots`` on purpose: ``slots`` is the io concurrency budget (await-bound,
    #: safe to set in the hundreds), whereas each cpu subprocess is a real OS
    #: process that may load model weights, so it must be bounded by CPU cores —
    #: NOT by the io slot budget. ``None`` => ``min(slots, os.cpu_count())``.
    #: Sizing the cpu pool to ``slots`` (e.g. 200) forks hundreds of
    #: model-loading subprocesses and OOM-kills the host.
    cpu_slots: int | None = Field(default=None, ge=1)
    #: Recycle a cpu-profile forkserver subprocess after this many jobs (spec
    #: 11.1 pool hygiene). A process that repeatedly loads/runs model weights can
    #: fragment or leak memory over thousands of jobs; recycling bounds that, the
    #: same way a DB pool recycles connections. ``None`` disables recycling.
    cpu_max_jobs_per_process: int | None = Field(default=None, ge=1)
    #: Poll interval for the optional ``admission_control`` hook (see
    #: ``Worker(admission_control=...)``). Only relevant when a hook is supplied.
    admission_poll_s: float = Field(default=1.0, gt=0)
    #: Optional path the worker touches from the event loop every heartbeat so an
    #: external healthcheck can detect an alive-but-WEDGED loop (a stale mtime).
    #: ``None`` disables the writer. The SDK ships NO probe -- the host decides
    #: the freshness threshold in its own healthcheck.
    liveness_file: str | None = None
    tags: list[str] = Field(default_factory=list)
    drain_timeout_s: float = Field(default=30.0, ge=0)
    heartbeat_interval_s: float = Field(default=15.0, gt=0)
    strict_schemas: bool = False


class RedisSettings(BaseModel):
    """Optional checkpoint fast path (spec 13.1). Absent => durable RPC path only.

    The spec names the env var ``SYMBA_CHECKPOINT_REDIS_URL``; the nested form
    ``SYMBA_REDIS__URL`` is also accepted for consistency with the other groups.
    """

    url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("url", "SYMBA_CHECKPOINT_REDIS_URL"),
    )


class LogSettings(BaseModel):
    level: str = "INFO"
    format: Literal["json", "console"] = "json"


class GrpcSettings(BaseModel):
    """Channel-level tuning (spec 5.1)."""

    keepalive_time_s: float = Field(default=10.0, gt=0)
    keepalive_timeout_s: float = Field(default=5.0, gt=0)
    max_receive_mb: int = Field(default=16, ge=1)
    max_send_mb: int = Field(default=16, ge=1)
    initial_reconnect_backoff_s: float = Field(default=0.2, gt=0)
    max_reconnect_backoff_s: float = Field(default=30.0, gt=0)


class SdkSettings(BaseSettings):
    """Root settings object. Reads ``SYMBA_*`` env with ``__`` nesting."""

    model_config = SettingsConfigDict(
        env_prefix="SYMBA_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    engine: EngineSettings = Field(default_factory=EngineSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    log: LogSettings = Field(default_factory=LogSettings)
    grpc: GrpcSettings = Field(default_factory=GrpcSettings)

    #: Convenience top-level token: ``SYMBA_TOKEN`` (spec 5.1 precedence).
    token: str | None = None


def load_settings(*, load_dotenv: bool = False, **overrides: Any) -> SdkSettings:
    """Build :class:`SdkSettings`, re-wrapping validation failures as ``ConfigError``.

    ``overrides`` are explicit kwargs and win over the environment. When
    ``load_dotenv`` is true a ``.env`` file is consulted (spec 19).
    """
    try:
        if load_dotenv:
            settings = SdkSettings(
                _env_file=".env",  # type: ignore[call-arg]
                **overrides,
            )
        else:
            settings = SdkSettings(**overrides)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        raise ConfigError(f"Invalid Symba configuration: {problems}") from exc

    # Token precedence: engine.token (explicit) wins, else the top-level SYMBA_TOKEN.
    if settings.engine.token is None and settings.token is not None:
        settings.engine.token = settings.token
    return settings


__all__ = [
    "SdkSettings",
    "EngineSettings",
    "WorkerSettings",
    "RedisSettings",
    "LogSettings",
    "GrpcSettings",
    "load_settings",
]
