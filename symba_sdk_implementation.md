# Symba SDK — Technical Implementation Specification (`symba-sdk-python`)

**Status:** Draft v1 (bootstrapped from the engine's `job_engine_design.md` rev 8 and `symba_implementation.md` rev 4 — the SDK sections of those documents are the source of the requirements restated here; where this doc goes deeper, it is the owner of SDK-internal decisions)
**Date:** 2026-07-15
**Companions:** `symba/docs/job_engine_design.md` (architecture decisions AD-1..AD-23, referenced not re-argued), `symba/docs/symba_implementation.md` (engine internals, wire contract source of truth, DDL owner)
**Scope:** Everything needed to build `symba-sdk-python` from an empty repo: pinned dependency ranges, repository layout, generated-stub policy, the full public API surface (`Engine`, `Worker`, `Ctx`, `JobHandle`, `Gate`), worker runtime internals (claim stream, dispatch pipeline, executors, heartbeat shell, slot accounting), context implementation (two-tier `ctx.output`, checkpoints, wait/signal re-entry), error taxonomy and retry classification, logging, the `SymbaTest` in-memory engine and conformance obligations, the sync facade, the CLI, testing strategy, CI/CD, and the build order.

The engine (`symba` repo) is the server; this repo ships the **two SDKs in one package**: the **client SDK** (submit/query/await/signal — what application code calls) and the **worker SDK** (register handlers, claim, execute — what runs on the fleet). One PyPI package `symba`, one import root `symba` (pip install syntel-symba) (design doc, open question 6: decided).

---

## Table of Contents

1. [What the SDK is and is not](#1-what-the-sdk-is-and-is-not)
2. [Dependencies and supported environments](#2-dependencies-and-supported-environments)
3. [Repository layout](#3-repository-layout)
4. [Wire contract consumption (generated stubs)](#4-wire-contract-consumption-generated-stubs)
5. [Transport layer](#5-transport-layer)
6. [Client SDK: Engine](#6-client-sdk-engine)
7. [JobHandle and Gate](#7-jobhandle-and-gate)
8. [Worker SDK: Worker and TaskRegistry](#8-worker-sdk-worker-and-taskregistry)
9. [The dispatch pipeline](#9-the-dispatch-pipeline)
10. [Ctx: the execution context](#10-ctx-the-execution-context)
11. [Executors and profiles](#11-executors-and-profiles)
12. [Heartbeat, timeout, and cancellation shell](#12-heartbeat-timeout-and-cancellation-shell)
13. [Checkpoints](#13-checkpoints)
14. [wait_for_event and the re-entry contract](#14-wait_for_event-and-the-re-entry-contract)
15. [Error taxonomy and retry classification](#15-error-taxonomy-and-retry-classification)
16. [Schemas and validation](#16-schemas-and-validation)
17. [Middleware](#17-middleware)
18. [Logging](#18-logging)
19. [Configuration](#19-configuration)
20. [SymbaTest: the in-memory engine](#20-symbatest-the-in-memory-engine)
21. [Sync facade](#21-sync-facade)
22. [CLI](#22-cli)
23. [Testing strategy](#23-testing-strategy)
24. [CI/CD, versioning, release engineering](#24-cicd-versioning-release-engineering)
25. [Build order (milestones)](#25-build-order-milestones)

---

## 1. What the SDK is and is not

### 1.1 Mission (F14, AD-16)

**A new engineer wires a working task in minutes.** The bar, verbatim from the design doc: *"The SDK must be easy and intuitive — a working task is a decorator and a function, with sane defaults for everything."* Every API decision in this document is subordinate to that sentence. Concretely:

- A worker is `Worker(engine=..., tags=[...], slots=n)` + `@worker.task("name")` + `worker.run()`. Nothing else is mandatory.
- A submit is `await engine.submit(task="name", payload={...})`. Nothing else is mandatory.
- Every knob (profiles, schemas, retry policies, rate classes, checkpoints, waits) is **opt-in, discoverable, and defaulted** — the io profile, 5 attempts with full-jitter exponential backoff, 600s timeout, 60s lease all come from the engine's `[defaults]` config when the SDK sends zero values.

### 1.2 Hard boundaries (AD-18, N5)

- **The SDK never imports engine server code.** The only shared artifact is the protobuf contract, consumed as generated stubs regenerated per engine release tag. No reaching into engine internals, ever — this is what keeps the wire contract honest.
- **The SDK has no server-side dependencies.** No FastAPI, no asyncpg, no Postgres client. A worker footprint is grpcio + protobuf + pydantic + structlog (+ optional redis for the checkpoint fast path, + optional typer/rich for the CLI). This matters operationally: a GPU parse box must not accumulate an accidental postgres client.
- **The engine is schema-blind; the SDK is where validation lives** (AD-15). Payload/result validation, output typing, retry classification of app exceptions — all SDK-side, costing the engine nothing.
- **Protocol compatibility is negotiated, not assumed.** Every `ClaimRequest`/handshake carries `sdk_version`; the engine rejects SDKs outside its supported protocol range with a clear error naming both versions (gRPC `FAILED_PRECONDITION`). SDK N.x supports engine protocol N-1 and N (Section 24).

### 1.3 Non-goals

- No workflow DSL, no stored pipeline definitions — flows are Python code at submit time (AD-8, AD-19).
- No LLM awareness. `ctx_id` is the only join key to app-side LLM observability (design doc boundary check).
- No exactly-once. The SDK ships idempotency *helpers* (`ctx.idempotency_key`, dedup keys, checkpoints); the contract is at-least-once + idempotent handlers (AD-7).
- No PII middleware in v1 (hook exists, Section 17).
- No code distribution — workers pre-own their code (AD-9); the SDK routes data to code.

---

## 2. Dependencies and supported environments

### 2.1 The library rule

The engine pins exact versions (it is a deployable). The SDK declares **ranges** — exact pins in a library installed into user environments cause dependency hell. This is non-negotiable and enforced by review: any PR pinning an exact runtime dependency version in `pyproject.toml` is rejected.

### 2.2 pyproject.toml (authoritative)

```toml
[project]
name = "symba"
requires-python = ">=3.11"        # user-env floor; engine itself targets 3.14
dependencies = [
    "grpcio>=1.66,<2.0",          # wide floor: user envs vary
    "protobuf>=5.29,<8.0",        # must span the engine's generated-stub line (7.x)
    "pydantic>=2.7,<3.0",         # schemas are pydantic v2
    "structlog>=24.1,<27.0",      # upper bound covers the engine's 26.x (CalVer)
    "tenacity>=8.3,<10.0",        # Complete/Fail RPC retries, channel reconnect backoff
]

[project.optional-dependencies]
redis = ["redis>=5.0,<9.0"]       # checkpoint fast path ONLY; 8.x RESP3-default in CI matrix
cli = ["typer>=0.15,<1.0", "rich>=13.0,<16.0"]

[dependency-groups]               # dev-only, never shipped
dev = [
    "grpcio-tools>=1.66,<2.0",    # stub regeneration (Makefile target, not user-facing)
    "ruff==0.15.21",              # pinned exact in dev — tooling, not a runtime dep
    "pyright==1.1.411",
    "pytest>=9.0,<10.0",
    "pytest-asyncio>=1.4,<2.0",
    "hypothesis>=6.156,<7.0",     # conformance property suites (Section 23)
]
```

Notes:
- `orjson` is deliberately **not** a dependency: payload/result JSON in the SDK is stdlib `json` with an internal seam (`symba/_json.py`) that uses orjson **if the host app already has it** — zero-dependency principle wins over the marginal speedup (SDK payloads are small by contract; the engine is where orjson matters).
- `symba.testing` (`SymbaTest`) ships **in the main package, not an extra** — test ergonomics are a core feature, and an extra would mean every user's CI needs an extra install line before their first test runs.
- Python floor 3.11: `asyncio.TaskGroup`, `ExceptionGroup`, `tomllib` are all available; nothing in the SDK needs 3.12+. CI tests 3.11 → 3.14 (Section 24).

### 2.3 CI environment matrix

| Axis | Values | Why |
|---|---|---|
| Python | 3.11, 3.12, 3.13, 3.14 | floor-to-engine-target coverage |
| grpcio | oldest supported (1.66), latest | range floor must actually work, not just resolve |
| redis extra | absent, redis 5.x, redis 8.x | degraded mode + RESP3-default behavior both exercised |
| Engine | engine@main, engine@last-release | compatibility NFR (Section 24) |

---

## 3. Repository layout

```
symba-sdk-python/
├── pyproject.toml               # ranges (2.2); package symba, import symba
├── README.md                    # quickstart: worker in 15 lines, submit in 5
├── LICENSE                      # Apache-2.0 (matches engine)
├── Makefile                     # proto-gen SYMBA_TAG=vX.Y.Z, test, lint, typecheck
├── uv.lock                      # committed (dev reproducibility; users install by range)
├── src/symba/
│   ├── __init__.py              # THE public API (6.1/8.1); everything else is private
│   ├── _proto/                  # generated stubs, COMMITTED, regenerated per engine tag
│   │   ├── __init__.py
│   │   ├── common_pb2.py / .pyi
│   │   ├── data_plane_pb2.py / .pyi / _grpc.py
│   │   ├── control_plane_pb2.py / .pyi / _grpc.py
│   │   ├── admin_pb2.py / .pyi / _grpc.py
│   │   └── VERSION              # engine tag the stubs were generated from
│   ├── _json.py                 # json seam: orjson if importable, stdlib otherwise
│   ├── engine.py                # Engine (async client) + .sync facade property
│   ├── worker.py                # Worker: lifecycle, claim stream, drain
│   ├── task_registry.py         # @worker.task bookkeeping + boot validation (8.3)
│   ├── dispatch.py              # per-assignment pipeline (Section 9)
│   ├── context.py               # Ctx + UpstreamOutputs (Section 10)
│   ├── job.py                   # JobHandle / Gate (Section 7)
│   ├── specs.py                 # JobSpec builder: kwargs -> proto, validation, defaults
│   ├── schemas.py               # pydantic validate/serialize glue (Section 16)
│   ├── profiles.py              # io/cpu/gpu profile definitions (Section 11)
│   ├── executors/
│   │   ├── __init__.py
│   │   ├── base.py              # Executor protocol
│   │   ├── asyncio_executor.py  # io profile
│   │   ├── process_executor.py  # cpu profile (ProcessPoolExecutor, forkserver)
│   │   ├── gpu_executor.py      # gpu profile (warm long-lived subprocess)
│   │   └── ctx_proxy.py         # subprocess Ctx proxy + duplex pipe protocol (11.4)
│   ├── heartbeat.py             # per-job heartbeat/timeout/cancel shell (Section 12)
│   ├── checkpoint.py            # redis fast path + engine RPC write-behind (Section 13)
│   ├── middleware.py            # WorkerMiddleware protocol + builtins (Section 17)
│   ├── retry_classify.py        # retryable-vs-fatal classification (Section 15)
│   ├── errors.py                # exception taxonomy (Section 15)
│   ├── idempotency.py           # key derivation — MUST mirror engine core/idempotency.py
│   ├── logging.py               # structlog defaults; inherits app config if present (18)
│   ├── transport.py             # channel lifecycle, keepalive opts, reconnect (Section 5)
│   ├── _sync.py                 # thread-owned-loop wrapper behind Engine.sync (Section 21)
│   ├── cli.py                   # `symba` CLI entry (optional extra; Section 22)
│   └── testing/
│       ├── __init__.py          # SymbaTest export
│       ├── fake_engine.py       # in-memory lifecycle engine (Section 20)
│       └── assertions.py        # assert_chain_executed etc.
└── tests/
    ├── unit/                    # no I/O: registry, classify, specs, ctx, executors
    ├── conformance/             # property suites run vs BOTH SymbaTest and real engine
    └── e2e/                     # against dockerized engine (compose from symba repo)
```

Structural rules (enforced, not aspirational):

1. **`__init__.py` is the whole public API.** Anything not exported there is private (`_`-prefixed modules are hard-private). Semver applies to `__init__.py` exports only; internals may change in minors. A test asserts the export list matches the documented surface (6.1/8.1) exactly — accidental export is a test failure.
2. **`_proto/` is committed.** Users never run protoc. Regeneration is a Makefile target against a named engine tag; CI fails if regeneration against the pinned tag produces a diff (Section 4).
3. **No server imports.** A ruff banned-import rule forbids `asyncpg`, `fastapi`, `uvicorn` anywhere in `src/`. `redis` may only be imported inside `checkpoint.py` behind the extras guard.
4. **`idempotency.py` mirrors the engine byte-for-byte.** The derivation (`sha256(f"{tenant}:{dedup_key or job_id}")[:32]`) is specified in both docs; a conformance test pins identical output for identical inputs against a table of vectors shared with the engine repo.
5. **One event loop assumption per process.** The Worker owns its loop via `worker.run()`; the Engine binds to the loop it is first awaited on. Cross-loop use raises a clear error naming the fix (`Engine.sync` for sync/threaded contexts) instead of the classic grpc.aio cross-loop hang.

---

## 4. Wire contract consumption (generated stubs)

### 4.1 Source of truth and regeneration

The proto files live in the **engine repo** (`symba/proto/symba/v1/*.proto`) — the SDK repo has no proto sources, only generated output. Regeneration:

```make
# Makefile
proto-gen:   ## make proto-gen SYMBA_TAG=v0.3.0
	rm -rf /tmp/symba-proto && \
	git clone --depth 1 --branch $(SYMBA_TAG) $(SYMBA_REPO) /tmp/symba-proto && \
	python -m grpc_tools.protoc -I/tmp/symba-proto/proto \
	    --python_out=src/symba/_proto --grpc_python_out=src/symba/_proto \
	    --pyi_out=src/symba/_proto \
	    /tmp/symba-proto/proto/symba/v1/*.proto && \
	python tools/fix_proto_imports.py && \
	echo "$(SYMBA_TAG)" > src/symba/_proto/VERSION
```

- `fix_proto_imports.py` rewrites the generated absolute imports (`from symba.v1 import ...`) to the package-relative `_proto` location — a standard grpcio-tools wart, fixed once in a tool, never by hand.
- `_proto/VERSION` records the engine tag; the SDK exposes it as `symba.__engine_protocol__` and sends it in every handshake alongside `symba.__version__`.
- CI job `stub-check`: run `make proto-gen SYMBA_TAG=$(cat src/symba/_proto/VERSION)` and fail on a dirty diff — committed stubs can never drift from their declared tag.

### 4.2 What the SDK consumes (from the engine's proto, v1)

| Service | RPCs used by | Notes |
|---|---|---|
| `WorkerService` (data plane) | Worker SDK | `Claim` (bidi stream, one per worker process), `Heartbeat`, `Complete`, `Fail`, `Wait`, `PutCheckpoint`, `GetCheckpoint`, `GetResult` |
| `ClientService` (control plane) | Client SDK | `Submit`, `FanOut`, `Query`, `GetJob`, `AwaitJob`, `Cancel`, `Signal`, `Resubmit`, `StreamEvents` |
| `AdminService` | CLI + power users | `ListRateClasses`, `UpsertRateClass`, cron CRUD, `ListWorkers` — exposed as `engine.admin.*`, deliberately unpolished (ops surface, not app surface) |

Key wire semantics the SDK must honor (from `symba_implementation.md` Section 4):

- `JobSpec.payload_json` / `Job.result_json` are **UTF-8 JSON bytes**, engine schema-blind; payload cap 256KB, result cap 64KB — the SDK pre-validates sizes client-side to fail fast with the same error text the engine would return (`PAYLOAD_TOO_LARGE` / `RESULT_TOO_LARGE` naming the store-a-reference fix).
- `Job.upstream` carries **only the inline tier**: immediate chain predecessor + declared `depends_on` results, ≤ 256KB total (submit-enforced by the engine). Deeper ancestors resolve via `GetResult` (Section 10.3).
- `JobAssignment` carries `lease_token` (opaque, accompanies every mutation), `lease_expires_at`, pre-loaded `checkpoint_json`, and `event_payload_json` (consumed signal on WAITING resume).
- `HeartbeatResponse.cancelled` is the cooperative-cancel channel (Section 12).
- `WaitResponse.parked=false` means a pending signal was consumed inline (wait-first race resolved server-side) — the SDK returns the payload without ever parking (Section 14).
- Retry defaults (5 attempts, base 1.0s, factor 2.0, cap 300s, full jitter) live in the engine's `[defaults]`; the SDK sends **zero values for anything unset** so engine defaults apply — the SDK never bakes its own copies of engine defaults.

### 4.3 Version handshake

`ClaimRequest.sdk_version` (worker) and a `sdk_version` metadata header on every control-plane call (client) carry `symba/<pkg-version> proto/<stub-tag>`. On `FAILED_PRECONDITION` with the version-mismatch detail, the SDK raises `ProtocolMismatch` naming both versions and the upgrade direction. This surface is tested in the cross-version e2e job (Section 24).

---

## 5. Transport layer (`transport.py`)

One module owns every gRPC channel in the SDK. Nothing else creates channels.

### 5.1 Channel construction

```python
def build_channel(target: str, *, token: str | None, tls: TlsConfig | None) -> grpc.aio.Channel:
    options = [
        ("grpc.keepalive_time_ms", 10_000),           # mirrors engine 9.1 (client side)
        ("grpc.keepalive_timeout_ms", 5_000),
        ("grpc.http2.max_pings_without_data", 0),     # keepalive even on idle streams
        ("grpc.max_receive_message_length", 4 * 1024 * 1024),
        ("grpc.max_send_message_length", 4 * 1024 * 1024),
        # reconnect backoff: 200ms initial, capped 30s (engine spec 10.5)
        ("grpc.initial_reconnect_backoff_ms", 200),
        ("grpc.max_reconnect_backoff_ms", 30_000),
    ]
```

- Scheme handling: `grpc://host:port` = insecure (dev; the SDK logs a WARNING once for non-loopback insecure targets), `grpcs://host:port` = TLS (system trust store; `TlsConfig` allows CA/client-cert override for mTLS per engine auth mode).
- Auth: `token` rides as `authorization: Bearer <token>` metadata on every call via a client interceptor; mTLS via channel credentials. Precedence: constructor arg → `SYMBA_TOKEN` env.
- **One channel per Engine instance, one per Worker instance.** Channels are lazy (created on first call) and closed on `aclose()` / context-manager exit.

### 5.2 Reconnect philosophy: the lease is truth, the stream is transport

The engine's invariant (engine spec 9.1) shapes the entire client design: long-lived bidi Claim streams die constantly in real deployments (LB idle timeouts, NAT evictions, rolling deploys, the engine's own deliberate `max_connection_age` recycling every ~30min). Therefore:

- **A dropped Claim stream is a non-event.** Running jobs keep running; their heartbeats are unary RPCs on the channel (which reconnects independently); their `Complete`/`Fail` are retried per Section 9.4. The worker re-opens the stream with exponential backoff (200ms → 30s cap, **infinite** — a worker that lost its engine keeps trying forever) and re-announces `ClaimRequest{worker_id, tags, free_slots, sdk_version, labels}` with its *current* free slots.
- **No state is kept stream-side.** The engine rebuilds its in-memory worker registry from re-announcements; the SDK rebuilds nothing — its truth is the local running-task set.
- Unary RPCs (`Heartbeat`, `Complete`, `Fail`, `Wait`, checkpoint RPCs, all control-plane calls) rely on gRPC's built-in reconnection plus tenacity retry policies defined per call-site (Section 9.4) — transport code adds no retry of its own (retrying at two layers multiplies attempts unpredictably).

### 5.3 Event-loop discipline

`grpc.aio` channels are bound to the loop they are created on. The transport module records the creating loop and raises `WrongEventLoop` (with the `Engine.sync` pointer in the message) if a call arrives from another loop — turning the classic silent-hang failure mode into an immediate, named error.

---

## 6. Client SDK: `Engine`

### 6.1 Public surface (everything importable from `symba`)

```python
from symba import (
    Engine,            # async client (this section)
    Worker,            # worker runtime (Section 8)
    Ctx,               # for type hints in handlers (Section 10)
    JobHandle, Gate,   # returned by submit / fan_out (Section 7)
    RetryPolicy,       # dataclass mirror of the proto message
    # errors (Section 15)
    SymbaError, RetryableError, FatalError, RateLimitedError,
    JobFailed, JobCancelled, StaleLease, ResultTooLarge,
    PayloadValidationError, OutputValidationError,
    AmbiguousResultKey, UnsupportedInProfile, WaitKeyAlreadyConsumed,
    EngineUnavailable, ProtocolMismatch,
)
from symba.testing import SymbaTest
```

### 6.2 Constructor

```python
engine = Engine(
    "grpcs://symba.internal:7233",
    tenant="acme-docs",            # stamped on every request; required for multi-tenant engines
    token=None,                    # -> SYMBA_TOKEN env fallback
    tls=None,                      # TlsConfig for mTLS deployments
    default_pipeline=None,         # optional: label inherited by every submit from this Engine
)
```

`Engine` is cheap to construct, lazy to connect, safe to share across tasks within one loop, and usable as an async context manager. Long-lived apps create one per process and keep it.

### 6.3 `submit` — the workhorse

```python
job: JobHandle = await engine.submit(
    task="download_source",                 # required; everything else optional
    payload={"document_id": doc_id},        # dict | pydantic BaseModel (serialized via schema glue)
    ctx_id=track_id,                        # correlation (AD-17); propagated to all descendants
    # flow structure (AD-19 / AD-12 / AD-20c)
    chain=["parse_content", "persist_parsed"],
    depends_on=None,                        # list[str job_ids] | dict[alias, job_id]
    on_failure={"task": "mark_stage_failed", "payload": {...}},
    # grouping labels (AD-15) — zero engine behavior, pure filter/rollup
    pipeline="ingestion", stage="parsing",
    # scheduling / identity
    group_key=doc_id,                       # fairness unit + per-group ceilings
    dedup_key=f"parse:{doc_id}",            # idempotent submit (F9) + checkpoint identity
    priority=0,                             # higher claims first
    run_at=None,                            # datetime -> delayed job (F10)
    # routing / budgets — usually set at task REGISTRATION, submit overrides
    runs_on=None,                           # e.g. ["parse", "gpu"]
    rate_class=None,                        # e.g. "azure-gpt5"
    max_concurrent_per_group=None,          # AD-22
    # execution/retry — engine defaults when None
    timeout_s=None, lease_ttl_s=None,
    retry=None,                             # RetryPolicy(...)
)
```

Client-side behavior, precisely:

1. **Spec building (`specs.py`):** kwargs → `JobSpec` proto. `depends_on` accepts a list (aliases default to producer task_name) or an alias dict (`{"first_pass": job1.id}` — aliases win over task names in `ctx.output`, AD-13). `on_failure` is itself a spec dict, recursively built.
2. **Client-side validation, fail-fast:** payload > 256KB, chain > 50 entries, unknown kwargs, `depends_on` referencing a `JobHandle` instead of an id (auto-unwrapped, actually — ergonomics), non-JSON-serializable payload — all raise locally with the same wording the engine would use. The SDK validates *shape*, never *semantics* (dep-job existence is the engine's transactional check).
3. **Batch form:** `await engine.submit_many([spec, ...])` maps to one `SubmitRequest` with n specs — one transaction engine-side, all-or-nothing.
4. **Dedup is not an error:** a dedup hit returns the existing job's handle with `job.deduplicated == True` (gRPC `OK` + `deduplicated=true` per engine 8.3).
5. Returns a `JobHandle` immediately — submit never waits for execution.

### 6.4 `fan_out` — children + gate (F4, AD-8)

```python
children, gate = await engine.fan_out(
    children=[{"task": "summarize_chunk", "payload": {...}, "rate_class": "azure-gpt5",
               "group_key": doc_id, "dedup_key": f"summ:{doc_id}:{c.id}"} for c in chunks],
    on_complete={"task": "executive_summary", "payload": {"document_id": doc_id},
                 "chain": ["apply_summaries"]},     # continuation may carry its own chain tail
    gate_policy="all_success",                       # "all_success" | "all_terminal" | "quorum(0.9)"
    ctx_id=track_id,
)
```

Maps to one `FanOutRequest` (one engine transaction: gate row + n children). Child specs are the same dict shape as `submit` kwargs. Returns `(list[JobHandle], Gate)`. Cap: 100k children (engine limit, pre-validated client-side).

### 6.5 Query / observe surface

```python
jobs   = await engine.query(ctx_id=track_id)                       # everything about one document
dead   = await engine.query(state="dead", task_name="summarize_chunk",
                            created_after=now - timedelta(hours=1))
job    = await engine.get_job(job_id)                              # one atomic read (never composed)
async for ev in engine.stream_events(ctx_id=track_id): ...         # live tail (StreamEvents)
```

- `query` auto-paginates: it returns an async-iterable `QueryResult` that lazily walks the keyset cursor (`page_token`); `await engine.query(...)` in a plain list context materializes up to `limit=` (default 1000, explicit `limit=None` walks everything). Filters mirror `QueryRequest` 1:1 — `ctx_id`, `state`, `task_name`, `pipeline`, `stage`, `group_key`, `created_after`.
- `stream_events` reconnects on stream drop with a `since` watermark (last seen event `at`) so a blip never loses events — the ledger is replayable by design.

### 6.6 Ops verbs

```python
await engine.cancel(job_id, cascade=True)       # per-state matrix is engine-side (5.8); response says what happened
await engine.resubmit(job_id)                   # DLQ replay: fresh row, lineage preserved
await engine.resubmit_many([j.id for j in dead])
await engine.signal("approve:T-123", {"approved": True}, signaled_by=user.email)   # AD-23
```

`signal` returns the delivered count (0 = signal parked durably for a future wait — rendezvous semantics, not an error).

### 6.7 `engine.admin` (namespaced, CLI-grade)

`engine.admin.list_rate_classes() / upsert_rate_class(name, capacity, refill_per_s) / list_cron() / upsert_cron(...) / set_cron_enabled(id, bool) / list_workers()` — thin 1:1 wrappers over `AdminService`, no sugar. Exists so ops scripts and the CLI need no second client.

---

## 7. `JobHandle` and `Gate` (`job.py`)

### 7.1 JobHandle

```python
class JobHandle:
    id: str
    task_name: str
    ctx_id: str | None
    deduplicated: bool                     # this submit collapsed into an existing job

    async def result(self, timeout: float | None = None) -> dict | BaseModel:
        # Server-side long-poll via AwaitJob (NOT client polling): the engine holds
        # the request until terminal or timeout_s. On SUCCEEDED -> deserialized result
        # (typed if this process knows the producer's output_schema). On DEAD ->
        # raises JobFailed carrying error_history. On CANCELLED -> raises JobCancelled.
        # timeout=None -> wait forever (re-issuing AwaitJob on server timeout slices).

    async def status(self) -> JobStatus:   # ONE GetJob call — one atomic read, never
        # a composition of multiple reads (pins a known bogus-not-found-mid-transition
        # bug class from prior art, design doc 5.1).

    async def cancel(self, cascade: bool = True) -> CancelOutcome
    async def events(self) -> list[JobEvent]        # the ledger for this job
```

Design rules:
- `result()` maps `AwaitJob` timeout slices transparently: the client asks in ≤60s server slices and re-issues until its own deadline — resilient to LB idle limits without holding hour-long requests.
- `JobFailed.error_history` is the full engine-side attempt history (a failed job is *a row you can read, diff, and resubmit — not a log line*).
- `JobHandle` is returned by `engine.submit`, `ctx.submit`, and reconstructable from a bare id: `engine.job(job_id)` — no state lives in the handle beyond identity.

### 7.2 Gate

```python
class Gate:
    id: str
    children: list[JobHandle]

    async def result(self, timeout: float | None = None) -> dict:
        # awaits the CONTINUATION job's result (the gate fires it exactly once)
    async def status(self) -> GateStatus   # expected/terminal/succeeded counts, fired_at
```

The gate itself is engine state; the SDK object is a view. `gate.result()` resolves the continuation job id lazily (it does not exist until the gate fires) via a `Query(ctx_id, task_name=continuation)` fallback — documented as eventually-consistent by one dispatcher tick.

---

## 8. Worker SDK: `Worker` and `TaskRegistry`

### 8.1 Constructor (everything defaulted, everything env-overridable)

```python
worker = Worker(
    engine="grpcs://symba.internal:7233",   # SYMBA_ENGINE
    token=None,                             # SYMBA_TOKEN
    tags=["llm"],                           # SYMBA_TAGS="llm" — what kind of worker I am (AD-5)
    slots=100,                              # SYMBA_SLOTS — max concurrent jobs
    worker_id=None,                         # SYMBA_WORKER_NAME -> WORKER_NAME -> HOSTNAME -> <hostname>-<uuid8>
    strict_schemas=False,                   # True: every task MUST declare both schemas (AD-15)
    heartbeat_interval_s=15,                # must be << lease_ttl; boot-warns if > ttl/3 for any task
    labels={"host": "spark-01", "gpu": "gh200"},   # fleet-view metadata, free-form
    profile_defaults=None,                  # e.g. {"gpu": {"subprocess_memory_mb": 8192}}
    middleware=[],                          # Section 17; LoggingMiddleware always prepended
    shutdown_drain_s=30,                    # SIGTERM drain budget
)
```

Env fallback for every parameter (same names, `SYMBA_` prefix) so **the same worker script deploys across heterogeneous boxes with env-only differences** — the four Spark parse nodes run one script with different env; this is the whole heterogeneous-hardware deployment story (engine spec 17.2).

### 8.2 Task registration

```python
@worker.task(
    "summarize_chunk",                # task_name: THE identity, globally unique (AD-15)
    profile="io",                     # io (default) | cpu | gpu (Section 11)
    runs_on=["llm"],                  # registration is the RIGHT place for runs_on —
                                      # the task knows its hardware; submits then never repeat it
    rate_class="azure-gpt5",
    timeout_s=None, lease_ttl_s=None, # None -> profile default -> engine default
    max_attempts=None, backoff=None,
    max_concurrent_per_group=None,    # AD-22
    input_schema=None,                # pydantic models; validated when present,
    output_schema=None,               # REQUIRED when worker.strict_schemas=True
)
async def summarize_chunk(ctx, payload): ...
```

Registration-time defaults are per-task submit defaults: a submit that omits `runs_on`/`rate_class`/`timeout_s` inherits them. Submit-time values always override (AD-16). The decorator returns the function unchanged (handlers stay plain-callable in unit tests).

### 8.3 Boot sequence and registry validation (hard errors, never warnings)

1. Import flow modules (side effect: registrations land in `TaskRegistry`). The convention from the design doc 11.0 holds: one module per flow, handlers + entry function together; the worker process is an assembly file that imports flow modules and runs.
2. Validate the registry — each violation is a named exception at boot, not a runtime surprise:
   - duplicate `task_name` (AD-15: one identity, registered once per fleet);
   - `strict_schemas=True` with a task missing either schema;
   - handler signature not `(ctx, payload)` (inspected, with a did-you-mean for `(payload, ctx)`);
   - **async handler registered with `profile="cpu"|"gpu"`** — compute profiles take sync callables; a coroutine cannot cross a process boundary;
   - sync handler registered with `profile="io"` → hard error naming the fix (make it async, or declare cpu) — a sync handler on the event loop is the #1 silent-latency bug;
   - `timeout_s >= lease_ttl_s * 10` (suspicious config, almost always a unit mistake);
   - `wait_for_event` reachable in a cpu/gpu task is not statically detectable — that one stays a runtime `UnsupportedInProfile` (Section 14).
3. Start executors (spawn the warm gpu subprocess + run `@worker.on_gpu_init` hooks; create the cpu pool lazily on first cpu job).
4. Open the Claim stream; announce `{worker_id, tags, free_slots=slots, sdk_version, labels}`.
5. Enter the dispatch loop (Section 9).

`worker.run()` is the blocking entry (creates the loop, installs SIGTERM/SIGINT handlers); `await worker.arun()` for embedding into an existing asyncio app.

### 8.4 Slot accounting — the single most bug-prone area, so the rules are law

Mined from prior-art failure modes (design doc 5.1/5.3; engine spec 10.2) and enforced by dedicated unit tests:

1. **Strong-reference task set.** Every spawned job task goes into `self._running: set[asyncio.Task]` — Python GC silently cancels unreferenced tasks (documented asyncio gotcha).
2. **One unconditional release point.** `free_slots += 1`, set-removal, and the stream slot-update all happen in the task's **done-callback only** — never inline in success/failure/abort branches. A bare semaphore with releases scattered across branches deadlocks on abort paths; the done-callback pattern makes leak-free release structural.
3. **Count claims you abandon.** An assignment the SDK rejects before spawning (validation failure, unknown task, shutdown race) releases its slot through the same done-callback path (a pre-completed task is created and immediately resolved) — abandoned claims must not leak slots.
4. **Drain bookkeeping is independent of the stop-claiming flag.** SIGTERM sets `accepting=False` and announces `free_slots=0`; the done-callbacks keep running regardless — a known stuck-terminating-worker bug class is exactly a drain loop whose completion bookkeeping was gated behind "am I picking jobs". Drain waits for `self._running` to empty, bounded by `shutdown_drain_s`; jobs still running at the deadline are abandoned (lease expiry re-runs them — crash-only, at-least-once absorbs it).
5. **The engine never assigns beyond announced slots** (flow control is worker-driven per the proto contract), but the SDK still guards: an assignment arriving with zero local free slots is failed back immediately with `retryable=true` and a WARNING (defense against engine accounting bugs, never a crash).

---

## 9. The dispatch pipeline (`dispatch.py`)

The exact per-assignment sequence (engine spec 10.2, expanded to implementation level):

```
JobAssignment (from Claim stream)
  1  registry lookup: unknown task_name -> Fail(fatal, "task not registered on this worker")
     (a routing/deploy mismatch is fatal-per-attempt, visible in the DLQ, never retried
      into a loop on the same broken fleet)
  2  build Ctx:
       - payload bytes -> dict (json seam)
       - ctx.output   <- JobAssignment.job.upstream (inline tier, Section 10.3)
       - checkpoint_data <- JobAssignment.checkpoint_json (pre-loaded by the engine)
       - event_payload   <- JobAssignment.event_payload_json (WAITING resume)
       - idempotency keys derived (idempotency.py)
       - logger bound: job_id, ctx_id, task_name, attempt, tenant
  3  middleware.on_claim(ctx)               # exceptions logged + suppressed (17)
  4  input_schema.model_validate(payload)   # if declared;
     failure -> PayloadValidationError -> Fail(retryable=False)  # bad submits fail fast,
                                                                 # not deep inside business logic
  5  start heartbeat/timeout shell (Section 12)
  6  executor by profile (Section 11) runs the handler
  7  handler outcome:
       return dict | BaseModel  -> output_schema validate (if declared; failure ->
                                   OutputValidationError -> Fail(retryable=False) —
                                   a typo'd field fails HERE, not as a KeyError three
                                   jobs downstream)
                                -> result size check (64KB) -> Complete(result)
       return ctx.stop_chain(r) -> Complete(result=r, drop_chain_tail=True)   # AD-20a
       return ctx.skip()        -> Complete(skipped=True)                     # AD-20b
       raise SymbaError         -> Fail(retryable=exc.retryable)
       raise anything else      -> retry_classify (Section 15) -> Fail(retryable=...)
  8  middleware.on_complete / on_fail
  9  done-callback: slot release + stream slot update (Section 8.4)
```

### 9.1 Concurrency shape

Each assignment is one supervised `asyncio.Task` (AD-10). io-profile handlers run inline on the loop; cpu/gpu handlers run in their executor while the *shell* (heartbeat, timeout, cancel watch) remains an asyncio concern on the loop — heartbeats are never blocked by a busy GIL because the busy code is in another process.

### 9.2 Return-value contract (AD-20)

Plain dict/BaseModel = result + chain continues. `ctx.stop_chain(result)` = result + chain tail dropped (conditional termination — the dedup case). `ctx.skip()` = success without doing work, chain continues (idempotency skip — the summarize case). Raising = failure with retry policy. These are the only four outcomes; a handler returning anything else (a string, a list) is an `OutputValidationError` naming the contract.

### 9.3 Handlers deciding branches (AD-20d/e) — what the SDK does NOT provide

No `goto`, no `ctx.continue_with(other_task)` — redirecting the flow to an arbitrary next task from inside a handler is rejected by design (it makes chains unreadable; every possible path must be visible in the flow module). Branching lives caller-side (`await job.result()` + `if` + `submit`); durable loops are `ctx.submit(...)` + `ctx.stop_chain()` with an explicit round counter in the payload. The SDK docs carry both patterns verbatim from the design doc (11.6-worked examples).

### 9.4 Finalization delivery (Complete/Fail) resilience

`Complete`/`Fail` RPC failures (engine briefly unreachable) are retried with tenacity: **5 attempts, exponential backoff, max 10s total**. If still failing: log ERROR and drop — the lease will expire and the engine re-runs the job; at-least-once absorbs it. `StaleLease` responses (`FAILED_PRECONDITION`) are logged WARNING and swallowed — the retry attempt won; this attempt's result is discarded by design (the failure-model table row "worker network partition": the engine rejects the loser's Complete; dedup keys + handler idempotency prevent double side effects).

---

## 10. `Ctx`: the execution context (`context.py`)

Everything a handler can need, nothing global (design doc 9.2).

### 10.1 Surface

```python
class Ctx:
    # identity (read-only)
    job_id: str; ctx_id: str; task_name: str; attempt: int; tenant: str
    pipeline: str | None; stage: str | None; group_key: str | None

    # data
    payload: dict | BaseModel          # validated instance when input_schema declared
    output: UpstreamOutputs            # upstream results by task_name/alias (10.3)
    event_payload: dict | None         # consumed signal payload after a WAITING resume

    # generated idempotency (AD-21)
    idempotency_key: str               # sha256(f"{tenant}:{dedup_key or job_id}")[:32]
                                       # STABLE across retries and duplicate submits
    idempotency_key_attempt: str       # f"{idempotency_key}-a{attempt}" — for APIs where
                                       # a retry SHOULD be a new operation

    # verbs
    async def checkpoint(self, data: dict) -> None          # Section 13
    checkpoint_data: dict | None                            # pre-loaded from the assignment
    async def heartbeat(self) -> None                       # manual lease extension (tight loops)
    async def wait_for_event(self, key: str, timeout_s: int) -> dict | None   # Section 14
    async def submit(self, **spec) -> JobHandle             # ctx_id/tenant/pipeline inherited
    async def submit_children(self, children: list[dict],
                              on_complete: dict | None = None,
                              gate_policy: str = "all_success") -> Gate
    def stop_chain(self, result: dict | None = None) -> StopChain   # sentinel return values,
    def skip(self) -> Skip                                          # not exceptions

    logger: BoundLogger                # pre-bound job_id/ctx_id/task_name/attempt (Section 18)
```

`ctx.submit` / `ctx.submit_children` inherit `ctx_id`, `tenant`, and `pipeline` automatically (AD-17: propagation is the SDK's job, not the handler author's). In cpu/gpu profiles these verbs marshal through the proxy pipe (Section 11.4).

### 10.2 `stop_chain` / `skip` are sentinels, not exceptions

They are returned, not raised — control flow via exception for a *successful* outcome makes handler code and middleware reasoning worse. The dispatch pipeline pattern-matches the return value. Raising `StopChain` accidentally (someone `raise ctx.stop_chain()`) is caught and re-explained in the error message.

### 10.3 `UpstreamOutputs` — the two-tier contract (AD-13)

```python
class UpstreamOutputs(Mapping):
    def __getitem__(self, key: str):
        # 1. exact ALIAS match wins (aliases declared in depends_on at submit)
        # 2. else unique task_name match in the inline tier
        # 3. multiple task_name matches without alias -> AmbiguousResultKey (never guess)
        # 4. inline miss -> LAZY tier: GetResult(job_id, lease_token, task_name) RPC,
        #    memoized for the life of the execution
        # 5. GetResult found=false -> KeyError (not an ancestor / no result) — NEVER a
        #    silent None
        # Deserialization: if THIS worker's registry knows the producer's output_schema,
        # the dict is model_validate'd into that type — consumers get autocomplete and
        # type-checked access without reading the producer's function body (AD-15).
```

- **Inline tier** (zero fetches, the common case): immediate chain predecessor + declared `depends_on` results — shipped in `Job.upstream` with the assignment, ≤ 256KB total (engine enforces at submit).
- **Lazy tier** (the escape hatch, not the norm): any deeper chain ancestor. One synchronous `GetResult` RPC, scoped engine-side to the caller's ctx_id + tenant, memoized. Handlers needing a deep ancestor on the hot path should declare it in `depends_on` for inline delivery — the SDK docs and the `KeyError` message both say so.
- In cpu/gpu profiles, the lazy path marshals via the Ctx proxy pipe (the subprocess never owns a gRPC channel).
- `.get(key, default)`, `in`, iteration over inline keys — standard Mapping semantics; iteration deliberately does NOT trigger lazy fetches.

---

## 11. Executors and profiles (`profiles.py`, `executors/`)

### 11.1 The three profiles (AD-16)

Rule of thumb, printed in the docstring and the docs: **"Is the task mostly waiting or mostly working? Waiting → `io`. Working → `cpu`. Working on a GPU → `gpu`."** If unsure, start with `io`; the event-loop lag watchdog (11.5) tells you when to move.

| Profile | Handler kind | Mechanism | Concurrency | Default timeout/lease | Crash containment |
|---|---|---|---|---|---|
| `io` (default) | `async def` | coroutine on the worker loop | up to `slots` interleaved | engine defaults (600s/60s) | exception = job failure only |
| `cpu` | `def` (sync) | `ProcessPoolExecutor(max_workers=slots)`, **forkserver** start method | one job per process | 900s/120s | pool detects broken process → `Fail(retryable=True)` + pool self-heals |
| `gpu` | `def` (sync) | ONE long-lived warm subprocess per worker; jobs over a duplex pipe | serialized (slots small: 1-2) | 3600s/300s | subprocess crash → `Fail(retryable=True)`, respawn + re-run `on_gpu_init` |

Profile defaults are a table in `profiles.py`; task registration overrides any field; submit overrides registration. `Worker(profile_defaults={...})` overrides the table per-worker (e.g. subprocess memory limits).

### 11.2 Executor protocol (`executors/base.py`)

```python
class Executor(Protocol):
    async def start(self) -> None
    async def run(self, task: RegisteredTask, ctx: Ctx, payload: Any) -> Any:
        """Run to completion or raise. Must be cancellable (Section 12)."""
    async def stop(self, drain_s: float) -> None
```

### 11.3 GPU executor specifics

- The subprocess is spawned at worker boot (not per job): `@worker.on_gpu_init` hooks run inside it once — model weights load once and stay warm (AD-9's warm-weights rationale).
- Parent ↔ child protocol over a `multiprocessing.Pipe` duplex connection: length-prefixed pickled frames — `RunJob{task_name, ctx_snapshot, payload}`, `CtxCall{verb, args}` (upward), `CtxReply`, `JobDone{result | exc_info}`, `Shutdown`. One in-flight job per subprocess keeps the protocol trivially ordered.
- Respawn policy: crash → respawn + re-init, with a circuit breaker (3 crashes in 60s → worker marks its gpu tasks unclaimable by announcing reduced tags, logs CRITICAL) — a wedged GPU must not turn into an infinite claim-crash-requeue loop against the whole queue.

### 11.4 The Ctx proxy (`executors/ctx_proxy.py`)

In cpu/gpu profiles the handler receives a `CtxProxy`: identity/data fields (payload, output inline tier, checkpoint_data, idempotency keys) are plain values pickled across at job start; **verbs marshal over the pipe to the parent**, which owns the gRPC channel — `checkpoint`, `heartbeat`, `submit`, `submit_children`, and lazy `ctx.output` fetches all round-trip through `CtxCall` frames. `wait_for_event` raises `UnsupportedInProfile` (compute tasks compute; coordination belongs in io tasks — engine spec 10.3). `ctx.logger` in the subprocess writes structured records to the pipe; the parent emits them through the process pipeline (one logging pipeline per process, Section 18).

### 11.5 Event-loop lag watchdog (io profile)

A 100ms ticker measures loop drift; sustained lag > 250ms logs a WARNING naming the currently running task_names — the actionable *"this handler should be `profile="cpu"`"* signal. Exposed as `symba_worker_event_loop_lag_ms` when metrics middleware is on.

---

## 12. Heartbeat, timeout, and cancellation shell (`heartbeat.py`)

One shell per running job, always an asyncio task in the parent process:

```
every heartbeat_interval_s (default 15):
    resp = Heartbeat(job_id, lease_token)          # unary, cheap, hot-pool engine-side
    lease_expires_at = resp.lease_expires_at       # engine extended the lease
    if resp.cancelled:                             # cooperative cancel (engine 5.8)
        io:      inject asyncio.CancelledError into the handler task
        cpu/gpu: SIGTERM the subprocess; SIGKILL after 5s   # soft-to-hard escalation
        -> Fail(retryable=False, error_type="Cancelled")    # confirms the cancel
on timeout_s expiry (worker-side timer, engine lease expiry is the backstop):
    same cancellation path -> Fail(retryable=True, error_type="JobTimeout")
```

- **Timeouts vs. leases (design doc 7.2):** `timeout_s` is the total execution ceiling per attempt; `lease_ttl_s` is the liveness contract. A slow-but-alive job heartbeats past many lease windows until `timeout_s` kills it. The SDK enforces `timeout_s` primarily; the engine's lease reclaim covers a fully dead worker.
- Heartbeat RPC failures are tolerated up to `lease_ttl / heartbeat_interval - 1` consecutive misses (logged WARNING each); the job keeps running — if the engine truly lost us, the lease expires and our eventual `Complete` is a `StaleLease`, swallowed per 9.4.
- `ctx.heartbeat()` (manual) exists for tight CPU loops inside io handlers between awaits; it just pokes the same shell.
- io-handler cleanup contract: handlers may `try/finally` around `CancelledError` for cleanup, but must re-raise — the shell logs and force-fails a handler that swallows cancellation and keeps running past a 5s grace.

---

## 13. Checkpoints (`checkpoint.py`, AD-14)

**The user requirement in one sentence: an LLM response, once received, is never lost to a downstream failure, and is never paid for twice.**

### 13.1 Write path — `await ctx.checkpoint(data)`

1. **Fast path (optional):** if the `redis` extra is installed AND `SYMBA_CHECKPOINT_REDIS_URL` is configured, write synchronously to Redis, keyed by **dedup identity** (`{tenant}:{dedup_key or job_id}` — the same identity as `ctx.idempotency_key`; duplicate submits of the same logical job see the same checkpoint by construction).
2. **Durable path (always):** `PutCheckpoint(job_id, lease_token, checkpoint_json)` RPC to the engine, which owns the Postgres write-behind copy. When Redis is configured, the RPC is fired without blocking the handler (background task, error-logged); without Redis, it is awaited — the durable copy is then the only copy and must land before the handler proceeds.

### 13.2 Read path

`ctx.checkpoint_data` is **pre-loaded**: the engine ships the durable checkpoint in `JobAssignment.checkpoint_json`; if the Redis fast path is configured, the SDK checks Redis first (it may hold a fresher unflushed write) and falls back to the assignment copy. Redis lost between checkpoint and retry = at worst the final unflushed seconds are re-done (the design's honest window).

### 13.3 Lifecycle

Cleanup on terminal success and TTL reaping are **engine-side** (sweeper + 7-day safety net); the SDK deletes only its Redis fast-path key on `Complete` (best-effort). The SDK never invents checkpoint retention policy.

### 13.4 The two patterns (both supported, docs teach both)

- **(a) Job splitting** (default for LLM stages): `summarize_chunk` (compute) → `apply_chunk_summary` (store) linked by `depends_on` — retrying the store never re-calls the LLM, visibility is per-step (compute/apply decoupling, design doc 7.4).
- **(b) `ctx.checkpoint`** (single-job convenience): checkpoint after the LLM call, store after; a retry loads `checkpoint_data` and skips the expensive part.

```python
@worker.task("summarize_chunk")
async def summarize_chunk(ctx, payload):
    if ctx.checkpoint_data:                               # resume after retry
        llm_out_ref = ctx.checkpoint_data["llm_out_ref"]
    else:
        result = await llm_gateway.ainvoke(...)           # expensive
        llm_out_ref = await stage_result(payload["staging_ref"], result)
        await ctx.checkpoint({"llm_out_ref": llm_out_ref})
    return {"staging_ref": llm_out_ref}
```

---

## 14. `wait_for_event` and the re-entry contract (AD-23)

### 14.1 Mechanics

`await ctx.wait_for_event(key, timeout_s)` issues the `Wait(job_id, lease_token, wait_key, timeout_s)` RPC:

- `WaitResponse.parked=false` → a signal was already pending (signal-first rendezvous); the payload is returned immediately, the job never parks.
- `parked=true` → the job is now WAITING engine-side, **its slot is released locally through the normal done-callback path**, and the handler's execution ends here — the SDK raises an internal `_Parked` control signal that unwinds the handler task cleanly (not a Fail, not a Complete; the engine owns the state).
- Resume = re-queue + re-claim, possibly on a different worker: the new assignment carries `event_payload_json` (the consumed signal, or null on wait-timeout) and the handler **re-runs from the top**.

### 14.2 The re-entry contract — three rules, SDK-enforced

This is NOT in-place suspension, and every piece of SDK surface says so loudly (docstring, docs, warnings):

1. **Everything expensive before a wait must be checkpointed or idempotent.** On resume the pre-wait code executes again; the checkpoint makes that re-execution free. The SDK logs a WARNING when `wait_for_event` is called on a resumed execution (`attempt > 1` or `event_payload` present) with no checkpoint present.
2. **Re-entry does not re-park:** `wait_for_event` with the same key on a resumed execution returns `ctx.event_payload` immediately — the SDK sees the consumed signal delivered in the assignment and the handler flows past the wait naturally, no "am I resuming?" special-casing.
3. **Repeat waits in one handler need distinct keys** (suffix a step name). Reusing a consumed key raises `WaitKeyAlreadyConsumed` rather than silently returning the stale payload.

Timeout: the handler resumes with `ctx.event_payload = None` / the call returns `None` — the handler decides (proceed, fail, escalate); no job waits forever.

`UnsupportedInProfile` is raised in cpu/gpu handlers (11.4). The canonical example (human approval + serialized webhook processing + checkpoint, design doc 11.9) ships in the README and the test suite verbatim.

---

## 15. Error taxonomy and retry classification (`errors.py`, `retry_classify.py`)

### 15.1 Exception hierarchy

Class-level `error_code` + message, structured context that lands in the engine's `error_history` JSONB:

```python
class SymbaError(Exception):
    """Base. Carries error_code, retryable default, and structured context."""
    error_code: str = "symba_error"
    retryable: bool = False
    def __init__(self, message: str | None = None, **context):
        self.message = message or self.__class__.message
        self.context = context              # -> error_history JSONB
        super().__init__(self.message)

# --- raised BY handlers (app -> engine direction) ---
class RetryableError(SymbaError):     # explicitly request a retry (overrides classification)
    error_code, retryable = "retryable", True
class FatalError(SymbaError):         # explicitly refuse retry: job goes DEAD immediately
    error_code, retryable = "fatal", False
class RateLimitedError(RetryableError):
    """Retryable AND drains the job's rate_class bucket engine-wide (engine 6.3):
    one worker discovering a 429 backs off the whole fleet, not just itself."""
    error_code = "rate_limited"
    def __init__(self, message=None, retry_after_s: float | None = None, **ctx): ...

# --- raised BY the SDK (engine -> app direction) ---
class JobFailed(SymbaError): ...            # JobHandle.result() on DEAD; carries error_history
class JobCancelled(SymbaError): ...         # JobHandle.result() on CANCELLED
class StaleLease(SymbaError): ...           # Complete/Fail rejected; SDK swallows + WARNs (9.4)
class ResultTooLarge(SymbaError): ...       # 64KB cap; message names the fix (store a reference)
class PayloadValidationError(SymbaError)    # input_schema rejected the payload
class OutputValidationError(SymbaError)     # output_schema rejected the return value
class AmbiguousResultKey(SymbaError)        # ctx.output name collision without alias
class UnsupportedInProfile(SymbaError)      # wait_for_event outside io profile
class WaitKeyAlreadyConsumed(SymbaError)    # repeat wait on a consumed key (14.2)
class EngineUnavailable(RetryableError)     # transport-level, after channel retries exhausted
class ProtocolMismatch(SymbaError)          # version handshake rejection (4.3)
class WrongEventLoop(SymbaError)            # cross-loop use (5.3); message points at .sync
```

### 15.2 Classification of unhandled handler exceptions

When a handler raises something that is not a `SymbaError`, `retry_classify.py` decides `FailRequest.retryable`. The engine trusts it — the SDK is closest to the exception:

| Exception source | Retryable | Rationale |
|---|---|---|
| `TimeoutError`, `asyncio.TimeoutError`, `ConnectionError` + subclasses, `OSError` with transient errnos (ECONNRESET, ETIMEDOUT, EPIPE, ...) | yes | infrastructure blips |
| httpx / httpcore / aiohttp transport + timeout errors (matched **by qualified class name**, no imports of those libs) | yes | same |
| any exception exposing `status_code`/`status` in {408, 429, 500, 502, 503, 504} | yes | transient HTTP; 429 also triggers engine-wide bucket drain when the job carries a `rate_class` |
| `status_code` in {400, 401, 403, 404, 422} | no | permanent — retrying cannot fix auth/validation |
| `KeyError`, `TypeError`, `ValueError`, `AttributeError`, pydantic `ValidationError` | no | programming/data errors — retry is noise |
| `MemoryError`, `SystemExit`, `KeyboardInterrupt` | no — **re-raise** | process-level, not job-level; the worker itself handles them |
| anything else | **no** | conservative default: unknown = don't retry; handlers opt in via `raise RetryableError(...) from exc` |

Duck-typed HTTP matching (attribute probe + module-name prefix match) keeps the SDK free of HTTP-client dependencies while classifying the three big clients correctly. The table is data (`CLASSIFICATION_RULES` list), unit-tested rule by rule, and extensible per-worker: `Worker(classify_overrides=[...])` prepends rules (used sparingly; documented as a last resort before fixing the handler).

### 15.3 What travels in `FailRequest`

`error_type` (exception class name), `error_message` (truncated to 2KB), `stack_hash` (`sha256(traceback)[:16]` — the engine UI groups DEAD jobs by it so one bug appearing 4000 times reads as one row), `retryable`. The full traceback goes to the worker's own log at ERROR — the engine stores the compact form, the log stores the forensics, `job_id` joins them.

### 15.4 gRPC status mapping (SDK-side handling)

| Engine response | SDK behavior |
|---|---|
| `FAILED_PRECONDITION` stale lease | `StaleLease` — swallowed + WARN in the dispatch pipeline; raised to callers of explicit verbs (checkpoint on a lost lease) |
| `OK` + `deduplicated=true` | `JobHandle.deduplicated = True`, never an exception |
| `NOT_FOUND` | `KeyError`-derived `JobNotFound` on query verbs |
| `INVALID_ARGUMENT` (too large / chain too long) | `ResultTooLarge` / `PayloadValidationError` with the engine's message verbatim |
| `RESOURCE_EXHAUSTED` (tenant queue cap) | `EngineUnavailable(retryable=True)` with `retry_after` honored by `submit` retries only when the caller opts in (`submit(..., retry_on_backpressure=True)`) — silent client-side queueing is worse than a visible error |
| `UNAUTHENTICATED` / `PERMISSION_DENIED` | `AuthError` immediately, never retried |
| transport `UNAVAILABLE` | tenacity per call-site policy; then `EngineUnavailable` |

---

## 16. Schemas and validation (`schemas.py`, AD-15)

```python
@worker.task("parse_content", input_schema=ParseContentInput, output_schema=ParseContentOutput)
def parse_content(ctx, payload: ParseContentInput) -> ParseContentOutput: ...
```

When declared, the SDK:

1. validates `payload` before invoking the handler (bad submits fail fast as `PayloadValidationError`, fatal — not deep inside business logic);
2. validates the return value before `Complete` (`OutputValidationError`, fatal — a typo'd field fails HERE, not as a KeyError three jobs downstream);
3. registers the output type in a process-local schema index so `ctx.output["parse_content"]` deserializes into `ParseContentOutput` for any *consumer task in the same process* — consumers learn a result's shape by reading the producer's schema, with autocomplete. Consumers on workers that don't import the producer's module get plain dicts (documented; schema knowledge is process-local by design — the engine stays schema-blind).

`strict_schemas` posture: `Worker(strict_schemas=True)` makes both schemas mandatory per task (boot-time hard error otherwise) — the recommended production setting; `False` (default) validates when present. Serialization: BaseModel payloads/results go through `model_dump(mode="json")`; `payload` arrives as the validated model instance when a schema is declared, a plain dict otherwise — handler signatures stay honest either way.

---

## 17. Middleware (`middleware.py`)

```python
class WorkerMiddleware(Protocol):
    async def on_claim(self, ctx: Ctx) -> None: ...
    async def on_complete(self, ctx: Ctx, result: dict, duration_ms: float) -> None: ...
    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None: ...
```

- **Middleware must not raise.** Every hook call is wrapped; exceptions are logged and suppressed — a broken metrics middleware must never fail a job (prior art let middleware exceptions kill message processing by design; Symba deliberately does the opposite, design doc 5.3).
- Builtins: `LoggingMiddleware` (always on, prepended; one INFO line per lifecycle transition matching the engine's `job_events` vocabulary) and `MetricsMiddleware` (opt-in; prometheus-client if importable — `symba_worker_jobs_total{task,outcome}`, `symba_worker_job_duration_seconds{task}`, `symba_worker_slots_busy`, `symba_worker_event_loop_lag_ms`, exposed on an optional local port).
- Ordering: `on_claim` in registration order, `on_complete`/`on_fail` in reverse (nesting semantics).
- PII filtering: explicitly out of scope v1; the hook surface is where it will attach later.

---

## 18. Logging (`logging.py`)

The prime directive: **the SDK does NOT configure structlog if the host app already has** (detected via `structlog.is_configured()`). It only binds its context keys — `ctx.logger` is a child of whatever pipeline the process owns, so app flow modules keep logging through their existing setup while the SDK adds job context.

Standalone workers (no prior config) get the engine-identical pipeline: `add_log_level` → ISO `TimeStamper` → app context (`app_name="symba"`, version, `worker_name`) → trace_id (otel contextvar if present, empty string otherwise) → error info (`{'exception': {name, message, stack}}`) → `StackInfoRenderer` → `format_exc_info` → `JSONRenderer` (console renderer when `SYMBA_LOG__FORMAT=console`).

Mandatory bound keys on job-scoped lines: `job_id`, `ctx_id` (empty string, never absent), `task_name`, `attempt`, `tenant`, plus `worker_id` on claim/execution lines, `duration_ms` on completions, first-8-chars-only of `lease_token` in lease disputes (full token never logged). Event-name convention `[fn_name] Action` with static message strings — all variance in keys.

Worker-name resolution precedence (7.3 of the engine spec, reused verbatim): `SYMBA_WORKER_NAME` → `WORKER_NAME` → `HOSTNAME` → `<hostname>-<uuid8>` — the same stable name flows to `ClaimRequest.worker_id`, `jobs.claimed_by`, the fleet UI, and every log line.

Hard rules (ruff-enforced in this repo too): no emojis, no `print()`, no unstructured interpolated strings, `import logging` forbidden outside `logging.py`.

---

## 19. Configuration (`config.py`)

Same philosophy as the engine (`pydantic-settings`, nested via `__`), different sources: the SDK is a library, so **constructor arguments always win** over environment. Precedence: explicit kwargs > `SYMBA_*` env vars > `.env` (only when `Worker(load_dotenv=True)`) > defaults.

```python
class SdkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SYMBA_", env_nested_delimiter="__")

    engine: EngineSettings          # SYMBA_ENGINE__GRPC_TARGET, __HTTP_URL, __API_KEY, __TLS
    worker: WorkerSettings          # SYMBA_WORKER__NAME, __SLOTS, __QUEUES, __DRAIN_TIMEOUT_S
    redis: RedisSettings | None     # SYMBA_REDIS__URL — optional checkpoint fast path
    log: LogSettings                # SYMBA_LOG__LEVEL, __FORMAT
    grpc: GrpcSettings              # keepalive, max message sizes, retry budget
```

Key entries and defaults:

| Setting | Default | Notes |
|---|---|---|
| `engine.grpc_target` | `localhost:50051` | host:port; `dns:///` supported |
| `engine.http_url` | `http://localhost:8080` | control-plane REST + admin |
| `engine.api_key` | — | required against non-local engines; sent as `authorization: Bearer` metadata |
| `engine.tls` | `False` | `True` -> `grpc.ssl_channel_credentials()`; custom CA via `engine.tls_ca_file` |
| `worker.slots` | profile-derived | see 11; explicit value wins |
| `worker.queues` | `["default"]` | claim filter |
| `worker.drain_timeout_s` | `30` | graceful-shutdown budget before Fail-with-requeue |
| `grpc.max_receive_mb` | `16` | must be >= engine's cap (engine enforces result size anyway) |
| `grpc.keepalive_time_s` | `30` | detects half-open connections behind LBs |

Validation happens at construction, not first use: `Engine(...)` / `Worker(...)` raise `ConfigError` (a `FatalError`) listing every invalid field at once — the pydantic error is re-wrapped so users see `symba.ConfigError`, not a pydantic traceback. No config file format of our own: apps already have their own config story; env + kwargs compose with all of them.

---

## 20. `SymbaTest`: the in-memory engine (`testing.py`)

The single highest-leverage DX feature (design doc 7.6): tests exercise real handler code, real chain/fan-out semantics, real retry classification — with zero infrastructure.

```python
async def test_enrichment_flow():
    async with SymbaTest() as sim:
        sim.register(worker)                    # reuse the production TaskRegistry
        h = await sim.submit("parse_content", {"url": "https://x.test"})
        result = await h.result(timeout=5)
        assert result["status"] == "parsed"
        assert sim.jobs("parse_content")[0].attempts == 1
```

### 20.1 What it is

An in-process implementation of the *engine's semantics* behind the same `Engine`/`JobHandle` API:

- dict-backed job table keyed by `job_id`; `asyncio`-scheduled instead of claim-loop-driven;
- chains, fan-out/`Gate`, `stop_chain`/`skip`, retries with (compressed) backoff, `wait_for_event`/`signal`, checkpoints (dict-backed), dedup keys, cancellation — all honored;
- the dispatch pipeline is the REAL one (section 9): schema validation, middleware, error classification run exactly as in production. Only transport + persistence are faked.

### 20.2 Controls

| Control | Purpose |
|---|---|
| `sim.register(worker)` | mount a production `Worker`'s registry (validated same as boot) |
| `sim.submit / fan_out / signal / cancel` | mirror `Engine` verbs |
| `sim.tick()` / `sim.run_until_idle(timeout=10)` | deterministic stepping vs run-to-quiescence |
| `sim.jobs(task_name=None, state=None)` | inspect job records (attempts, error, result, events) |
| `sim.clock` | fake clock; backoff of minutes elapses instantly but ordering is preserved |
| `sim.fail_next("task", exc)` | fault injection for retry-path tests |
| `sim.checkpoints[job_id]` | assert checkpoint writes without Redis |

### 20.3 The conformance suite

The risk with fakes is drift. Countermeasure: a shared test corpus (`tests/conformance/`) parameterized over `SymbaTest` and a real dockerized engine (same scenarios, both backends; the real-engine leg runs in the SDK repo's nightly CI and in the engine repo's CI against SDK `main`). Scenarios: dedup, chain abort, gate math (incl. all-skipped), retry exhaustion -> DLQ state, cancellation of running jobs, wait/signal resume, checkpoint restore after simulated crash. Divergence = release blocker for whichever repo introduced it.

`SymbaTest` intentionally does NOT emulate: rate-class token buckets (jobs run immediately; assert on submissions instead), lease expiry/sweeper (no real time), multi-worker contention, Postgres-specific failure modes. Tests for those belong against the real engine.

---

## 21. Sync facade (`sync.py`)

For scripts, notebooks, Django views — call sites that aren't async. Thin wrappers, zero logic duplication:

```python
from symba.sync import SyncEngine

eng = SyncEngine("localhost:50051")
handle = eng.submit("parse_content", {"url": "..."})   # blocking
result = handle.result(timeout=120)                    # blocking await
```

Implementation: a dedicated background event loop thread per `SyncEngine` (started lazily, `atexit`-closed); every method is `asyncio.run_coroutine_threadsafe(...)` + `.result(timeout)`. This sidesteps the classic `asyncio.run()`-per-call trap (breaks inside Jupyter's running loop) and keeps one persistent gRPC channel. Calling `SyncEngine` *from* async code raises `RuntimeError` with a pointed message ("you are in an event loop; use Engine"). There is deliberately **no sync Worker**: workers are long-running processes and own their loop; a blocking facade there would be a footgun.


---

## 22. CLI (`cli.py`, `python -m symba`)

Typer-based, mirrors the engine's ops surface from the developer's side. Installed as `symba` console script.

| Command | What it does |
|---|---|
| `symba run app.worker:worker` | import the `Worker` object and run it (the production entrypoint; flags: `--slots`, `--queues`, `--profile-check`) |
| `symba tasks app.worker:worker` | print the registry: task names, queues, schemas, timeouts — boot validation without connecting |
| `symba submit <task> --payload '{...}' [--wait]` | one-off submit from the shell; `--wait` polls to terminal state and prints the result |
| `symba job <job_id>` | job details + event history (control-plane query) |
| `symba cancel <job_id>` / `symba resubmit <job_id>` / `symba signal <token> --payload '{}'` | ops verbs |
| `symba doctor` | connectivity check: gRPC target, HTTP url, auth, version handshake, Redis (if configured) — prints a table of PASS/FAIL with remediation hints |
| `symba gen-stubs --proto-dir ../symba/proto` | regenerate committed stubs (maintainers only; see 4.2) |

`symba doctor` is the support-load killer: every "worker won't start" report begins with its output. All commands honor `SYMBA_*` env vars; `--engine` / `--http-url` flags override.

---

## 23. Testing strategy (SDK repo)

Layout mirrors the engine repo (`tests/unit/`, `tests/integration/`, `tests/conformance/`).

**Unit (no I/O, no engine):** registry validation matrix (dupe names, bad signatures, strict-schema violations); retry classification table-driven tests (every row of 15.2); backoff math; `Ctx` sentinel semantics (`stop_chain`/`skip` returned vs raised); `UpstreamOutputs` two-tier resolution; schema index round-trips; middleware suppression (a raising middleware must not fail the job); slot-accounting invariants under fabricated executor crashes (property-based, hypothesis: slots never leak, never go negative); heartbeat shell with fake clock (timeout fires, cancellation propagates, GPU non-cancellable path).

**Integration (dockerized engine via testcontainers, marked `-m integration`):** full lifecycle submit->claim->complete over real gRPC; lease expiry and reclaim (short TTL); dedup across two `Engine` instances; wait/signal across processes; checkpoint restore with real Redis; drain under load (SIGTERM mid-burst — every job either completed or requeued, none lost); version-handshake rejection (engine pinned to an older image).

**Conformance:** section 20.3 — the same corpus against `SymbaTest` and the real engine.

**Executor-specific:** cpu/gpu subprocess tests marked and skipped where hardware is absent; a crash-only test asserts `WorkerCrash` fail path with requeue.

Coverage gate ≥ 90% on `src/symba/` excluding generated stubs. `pytest-asyncio` in strict mode; `pytest-timeout` at 60s to catch hangs, which in this codebase are always bugs.

---

## 24. CI/CD, versioning, release engineering

**CI matrix (GitHub Actions):** lint (ruff, format+check) -> mypy (strict; generated stubs excluded via overrides) -> unit across the support matrix of section 2.3 (3.11/3.12/3.13 × min-deps/latest-deps) -> integration against the engine's `:latest` and last released image -> conformance -> build (`uv build`, twine check).

**Versioning:** SemVer, independent of the engine's version. Compatibility is expressed against the **proto contract version** (section 4.3), not engine releases: `symba 1.x` speaks `symba.v1`. Breaking public-API changes -> major; new verbs/params -> minor; fixes -> patch. A `COMPATIBILITY.md` table maps SDK ranges to engine ranges — the answer to "can I upgrade the engine without touching workers?" must be a table lookup, not a shrug.

**Releases:** tag-driven (`v1.4.0`), trusted-publisher OIDC to PyPI, no long-lived tokens. Changelog via towncrier fragments enforced per PR. Deprecations: warn (with `DeprecationWarning` + changelog) for one minor version minimum before removal; removed only in majors.

**Stub refresh discipline:** a scheduled workflow diffs `proto/` against engine `main`; drift opens an automated PR running `gen-stubs` + full test suite. Engine proto changes are additive within `v1` (buf-enforced there), so these PRs are routine, not fire drills.

---

## 25. Build order (milestones)

Dependency-ordered; each milestone lands green on CI before the next starts. Mirrors the engine spec's section 14 style.

**M0 — Skeleton + contract.** Repo scaffolding (pyproject, ruff/mypy/pytest config, CI); vendor `proto/`, generate + commit stubs; `errors.py` taxonomy; `config.py`. *Exit: `import symba` works, stubs typecheck.*

**M1 — Client core.** `transport.py`; `Engine.submit/get_job/query/cancel`; `JobHandle.result/status` (poll-based); serialization + client-side validation. *Exit: submit/await against a dev engine.*

**M2 — Worker core.** `TaskRegistry` + `@worker.task`; boot validation; claim loop (io profile only); dispatch pipeline; heartbeat/timeout shell; `Complete`/`Fail` with retry classification; graceful drain. *Exit: end-to-end job on a real engine, kill -TERM requeues.*

**M3 — Ctx + chains.** Full `Ctx` surface; `stop_chain`/`skip`; `UpstreamOutputs`; chain submission sugar; `fan_out` + `Gate`; schema index. *Exit: enrichment-pipeline example from the design doc runs.*

**M4 — Durability verbs.** Checkpoints (Redis + PG paths); `wait_for_event` + re-entry contract; `signal`; idempotency-key helpers. *Exit: kill a worker mid-LLM-flow, resume without repaying.*

**M5 — Executors.** cpu (`ProcessPoolExecutor`) and gpu (dedicated subprocess + `CtxProxy`); crash containment; slot accounting hardening; event-loop-lag watchdog. *Exit: crash-only tests green; property tests on slots green.*

**M6 — SymbaTest + conformance.** In-memory engine; controls; conformance corpus running against both backends. *Exit: conformance green on both; drift gate wired into CI.*

**M7 — DX shell.** Sync facade; CLI incl. `doctor`; `stream_events`/`watch`; `engine.admin`; MetricsMiddleware. *Exit: `symba doctor` + `symba run` demo path.*

**M8 — Release readiness.** Docs (README quickstart, API reference via mkdocs, COMPATIBILITY.md); examples directory (enrichment pipeline, human-in-the-loop, GPU batch); version handshake tests against pinned engine images; `1.0.0` to PyPI.

Estimated effort concentrates in M2/M5 (slot accounting + executor crash containment) and M6 (conformance) — everything else is disciplined assembly around the wire contract.
