# Symba — Technical Implementation Specification

**Status:** Draft v4 (anti-over-engineering pass: known distributed job-queue failure modes reviewed at the design level — claim-time revalidation + one-clock rule (Section 5.3), sweeper provenance + scope pins (Section 5.6), cancel-per-state matrix (Section 5.8), loop containment (Section 6.2), cron as dedup-key upsert with no load-bearing election (Section 6.5), SDK slot-accounting rules (Section 10.2), regression pins for cancel-state, loop-containment, and sweeper-scope behavior (Section 15.6). Prior v3: pins re-verified live 2026-07-13; Python 3.14 + Postgres 18; LISTEN/NOTIFY removed — polling dispatcher; hot-table evacuation to `jobs_archive`; `uuidv7()` PKs; `group_running` counter table; `GetResult` lazy upstream fetch; this doc is the **single owner of the DDL** — the companion design doc's data model section is conceptual only)
**Date:** 2026-07-13
**Scope:** Everything needed to start building: pinned technology stack, repository structures, wire contract, database DDL + hot-path SQL, engine subsystem internals, the full logging specification (based on a conventional structlog setup), error taxonomy, retry classification, configuration reference, SDK public API with worked examples, testing strategy with example code, CI/CD, deployment, and the milestone plan. Architecture decisions are defined in `job_engine_design.md` and are referenced, not re-argued.

---

## Table of Contents

1. [Technology stack and versions](#1-technology-stack-and-versions)
2. [Repository: symba (engine server)](#2-repository-symba-engine-server)
3. [Repository: symba-sdk-python](#3-repository-symba-sdk-python)
4. [Wire contract (protobuf)](#4-wire-contract-protobuf)
5. [Database: DDL and hot-path SQL](#5-database-ddl-and-hot-path-sql)
6. [Engine internals](#6-engine-internals)
7. [Logging specification](#7-logging-specification)
8. [Error taxonomy and retry classification](#8-error-taxonomy-and-retry-classification)
9. [Configuration reference](#9-configuration-reference)
10. [Worker SDK internals](#10-worker-sdk-internals)
11. [Client SDK: public API and usage](#11-client-sdk-public-api-and-usage)
12. [Non-functional requirements: how each is achieved](#12-non-functional-requirements-how-each-is-achieved)
13. [Web UI implementation](#13-web-ui-implementation)
14. [Observability](#14-observability)
15. [Testing strategy](#15-testing-strategy)
16. [CI/CD, versioning, release engineering](#16-cicd-versioning-release-engineering)
17. [Deployment](#17-deployment)
18. [Build order (milestones)](#18-build-order-milestones)

---

## 1. Technology stack and versions

Pins verified against PyPI as of 2026-07. Rule: engine pins exact versions (it is a deployable); the SDK declares ranges (it is a library installed into user environments — exact pins in a library cause dependency hell).

### 1.1 Engine (`symba`) — exact pins

All pins verified against live PyPI/registry data 2026-07-13 (rev 3 re-verification).

| Library | Version | Role | Why this one / migration notes |
|---|---|---|---|
| python | **3.14 (target), 3.13 supported+tested** | runtime | 3.14 is the current stable feature line (6 patch releases in); best asyncio performance. The engine is a separate deployable and takes the newer runtime; the SDK floor stays 3.11 for user envs |
| grpcio | ==1.82.1 | data-plane server (`grpc.aio`) | asyncio-native; 1.82.1 requires protobuf >=7.35.1 |
| grpcio-health-checking | ==1.82.1 | standard gRPC health service | LB/K8s probes speak it natively |
| grpcio-reflection | ==1.82.1 | dev/debug only (flag-gated) | grpcurl/Postman against a live engine |
| protobuf | >=7.35.1,<8.0 | wire serialization | 7.x major line (since Feb 2026); lower bound required by grpcio-tools 1.82.x; regenerate stubs with matching grpcio-tools |
| asyncpg | ==0.31.0 | Postgres driver | Fastest PG driver; binary protocol; supports PG 18. Note: if a pooler (PgBouncer tx-mode) ever fronts the engine DB, set `statement_cache_size=0` — documented trap |
| redis | ==8.0.1 | buckets + checkpoint cache (`redis.asyncio`) | **redis-py 8: RESP3 is the default protocol** with legacy-shaped responses preserved; verify bucket Lua scripts under RESP3 in integration tests |
| fastapi | ==0.139.0 | HTTP control plane + UI API | **>=0.137 removed startup/shutdown events — lifespan is mandatory** (we use lifespan anyway per house rules); Starlette 1.0 underneath |
| uvicorn | ==0.51.0 | ASGI server | current stable |
| pydantic | ==2.13.4 | DTOs, config validation | latest 2.x (pydantic-core now merged into the pydantic repo); v3 not yet released |
| pydantic-settings | ==2.14.2 | config loading (TOML + `SYMBA_*` env) | >=2.14.2 mandatory (symlink CVE GHSA-4xgf-cpjx-pc3j in 2.12–2.14.1) |
| structlog | ==26.1.0 | logging | CalVer; conventional processor-chain design — pipelines stay compatible across projects, version is just newer |
| tenacity | ==9.1.4 | internal retries (Redis ops, DB reconnect) | current stable 9.x |
| croniter | ==6.2.4 | cron expression parsing | adopted by pallets-eco in 2025 — actively maintained again; supports seconds field. (`cronsim` remains the fallback if maintenance regresses) |
| prometheus-client | ==0.25.0 | /metrics | standard registry; multiprocess mode not needed (single proc) |
| opentelemetry-sdk + otlp exporter | ==1.43.0 | traces | SDK/exporter/instrumentation versions must stay in lockstep |
| pyjwt | ==2.13.0 | token auth verification | current stable 2.x |
| orjson | ==3.11.9 | JSON encode/decode of payload/result | 5-10x stdlib; deterministic bytes for hashing |

Deliberately NOT used: SQLAlchemy (the engine has ~20 queries, all hand-written SQL in files — an ORM adds a layer exactly where we need `EXPLAIN`-able control); alembic (migrations are plain ordered SQL, runner built in ~50 lines — deliberately not a JVM-based migration tool: the engine must be a single self-migrating container with no JVM sidecar, to keep the deployment footprint to one container plus database); generic task-queue libraries (the engine itself provides this functionality — we are the queue); litellm/langchain (engine runs *something*, never knows about LLMs).

**Infrastructure floors:** PostgreSQL **18** (required — `uuidv7()` PKs, see Section 5.1; also async I/O read perf; use the latest 18.x minor for CVE fixes). Redis **8.x** or Valkey 9.x (wire-compatible; note Redis's tri-license RSALv2/SSPLv1/AGPLv3 since 8.0 — Valkey is the BSD-3 alternative if legal requires it; see TODOS.md).

### 1.2 SDK (`symba-sdk`) — ranges

```toml
[project]
name = "symba-sdk"
requires-python = ">=3.11"
dependencies = [
    "grpcio>=1.66,<2.0",          # wide floor: user envs vary
    "protobuf>=5.29,<8.0",
    "pydantic>=2.7,<3.0",         # schemas are pydantic v2
    "structlog>=24.1,<27.0",      # upper bound covers the engine's 26.x (CalVer)
    "tenacity>=8.3,<10.0",
]

[project.optional-dependencies]
redis = ["redis>=5.0,<9.0"]        # only needed for the checkpoint fast path; 8.x RESP3-default covered by CI matrix
cli = ["typer>=0.15,<1.0", "rich>=13.0,<16.0"]
```

The SDK has NO dependency on FastAPI, asyncpg, or anything server-side. A worker footprint is: grpcio + pydantic + structlog. This matters for the GPU boxes — minimal surface, no accidental postgres client on a parse node.

### 1.3 UI

| Tool | Version | Notes |
|---|---|---|
| React | 19.x | SPA |
| Vite | **8.x** | build; the SPA ships as a SEPARATE container (nginx serving the Vite build from `frontend/`), NOT embedded in the engine image. Vite 8 = Rolldown bundler (Rust): use `rolldownOptions`, avoid esbuild-format plugins |
| TypeScript | **7.x** | TS 7 = native Go compiler (~10x faster), `strict` default. Caveat: the Compiler API is absent in 7.0 (returns 7.1) — `openapi-typescript` codegen must be verified against TS 7, else pin TS 6.x for the codegen step only |
| TanStack Query + Router | 5.x / 1.x | server-state caching, URL-driven filters |
| @xyflow/react (React Flow) | 12.x | dependency-graph / pipeline DAG view |
| Tailwind CSS | 4.x | styling (CSS-first config) |

### 1.4 Toolchain (both repos)

| Tool | Version | Role |
|---|---|---|
| uv | ==0.11.x (pin exact in CI) | package/env manager (lockfiles committed); house style: uv binary copied into images, cache mounts |
| ruff | ==0.15.21 (pin exact) | lint + format (line-length 120) |
| pyright | ==1.1.411 (pin exact) | strict type checking on `src/` |
| pytest / pytest-asyncio | **9.x** / 1.4.x | tests (`asyncio_mode = "auto"`). pytest 9: deprecation warnings are errors — fix at first occurrence, never suppress |
| testcontainers | 4.14.x | PG + Redis integration tests |
| hypothesis | 6.156.x | property-based DAG/lifecycle tests |
| buf | 1.71.x | proto lint + breaking-change gate in CI |
| k6 | **2.x** | load benchmarks (claim path); k6 crossed to 2.x in 2026 — write scripts against the v2 API from the start |

---

## 2. Repository: symba (engine server)

### 2.1 Repo layout

```
symba/
├── pyproject.toml               # exact pins (1.1); uv.lock committed
├── README.md
├── LICENSE                      # Apache-2.0
├── Dockerfile                   # multi-stage: ui build -> python build -> distroless-ish runtime
├── docker-compose.yml           # engine + postgres:18 + redis:8 for local dev
├── Makefile                     # proto, test, lint, typecheck, run, ui
├── proto/                       # THE wire contract source of truth
│   ├── symba/v1/
│   │   ├── common.proto         # JobSpec, Job, JobState, RetryPolicy
│   │   ├── data_plane.proto     # WorkerService (claim stream, complete, wait...)
│   │   ├── control_plane.proto  # ClientService (submit, query, signal...)
│   │   └── admin.proto          # rate classes, cron, DLQ ops, fleet
│   └── buf.yaml                 # lint + breaking-change config
├── database/symba/              # Flyway-managed schema (versioned migrations, one dir per schema)
│   ├── V001__jobs.sql           # + jobs_archive + group_running + autovacuum tuning
│   ├── V002__job_events.sql
│   ├── V003__dependencies_gates.sql
│   ├── V004__checkpoints.sql
│   ├── V005__workers_cron.sql
│   ├── V006__signals.sql
│   └── V007__rate_classes.sql   # bucket configs (6.3) — was missing pre-rev-3
├── flyway.toml                  # Flyway config (schema=symba); flyway sidecar runs it
├── src/symba/
│   ├── __init__.py
│   ├── main.py                  # entrypoint: config -> pools -> subsystems -> serve (Flyway migrates out-of-band)
│   ├── config.py                # pydantic-settings model (Section 9)
│   ├── db/
│   │   ├── pool.py              # two asyncpg pools (hot / general), search_path=symba
│   │   ├── migrate.py           # TEST-ONLY schema loader; prod migrations run by Flyway
│   │   └── queries/             # ALL SQL as named .sql files, loaded at import
│   │       ├── claim.sql        # THE hot query (5.3)
│   │       ├── complete.sql     # multi-statement transaction (5.4), incl. archive move
│   │       ├── submit.sql
│   │       ├── fail.sql         # incl. archive move on DEAD
│   │       ├── get_result.sql   # lazy upstream fetch (rev 7)
│   │       ├── cancel.sql       # per-state cancel matrix (5.8)
│   │       ├── sweep_leases.sql
│   │       ├── sweep_waits.sql
│   │       └── signal.sql
│   ├── core/                    # PURE logic: no I/O imports allowed (enforced by lint rule)
│   │   ├── states.py            # JobState enum + TRANSITIONS legality table
│   │   ├── retry.py             # backoff math: base * factor^attempt, cap, full jitter
│   │   ├── readiness.py         # remaining_deps bookkeeping rules
│   │   ├── gates.py             # gate policies: all_success | all_terminal | quorum(n)
│   │   ├── chain.py             # linked-list advance: (on_success, chain_tail) -> next JobSpec
│   │   └── idempotency.py       # idempotency key derivation (shared w/ SDK spec)
│   ├── services/
│   │   ├── submit_service.py
│   │   ├── claim_service.py     # matcher (6.2)
│   │   ├── complete_service.py
│   │   ├── lease_service.py
│   │   ├── sweeper.py
│   │   ├── cron_service.py
│   │   ├── signal_service.py
│   │   └── rate_limiter.py      # token buckets (6.3)
│   ├── transport/
│   │   ├── grpc_server.py       # grpc.aio server; interceptors: auth, logging, metrics
│   │   ├── http_server.py       # FastAPI: REST mirror + SSE stream (lifespan-managed); API-only, no embedded UI
│   │   └── auth.py              # token/mTLS verification, tenant resolution
│   └── observability/
│       ├── logging.py           # structlog pipeline (Section 7)
│       ├── metrics.py           # prometheus registry (Section 14)
│       └── tracing.py           # otel setup + trace_id contextvar
├── frontend/                    # React SPA source; built + served as a SEPARATE container (not embedded in the engine)
└── tests/
    ├── unit/
    ├── integration/
    ├── property/
    ├── chaos/
    └── load/
```

### 2.2 Structural rules (enforced, not aspirational)

1. **`core/` imports nothing but stdlib + pydantic.** A ruff `tid` (banned-imports) rule forbids `asyncpg`, `redis`, `grpc`, `fastapi` inside `core/`. Everything in `core/` is testable with plain pytest in microseconds. This is where the correctness of the state machine lives.
2. **All SQL in `db/queries/*.sql`.** Loaded once at import into a `Queries` namespace (`Q.CLAIM`, `Q.COMPLETE`, ...). Never inline SQL in services. Each query file has a header comment stating its transaction context and lock behavior. CI runs `EXPLAIN (FORMAT JSON)` against every query on the migrated schema and fails if the claim query plan contains a seq scan on `jobs`.
3. **Transport is dumb.** A gRPC servicer method: decode -> call service -> encode -> return. No business decisions, no SQL — a conventional router/service/repo discipline.
4. **One process, subsystems as asyncio tasks.** `main.py` builds a `TaskGroup` with: grpc server, http server, dispatcher loop, sweeper loop, cron loop, bucket refiller. `--role=api|sweeper|all` (default `all`) lets big deployments split later without a refactor.
5. **Crash-only.** No shutdown step is load-bearing. SIGTERM = stop accepting, drain politely, exit; anything unfinished is recovered by lease expiry. Every subsystem must tolerate being killed mid-write (transactions guarantee it).

---

## 3. Repository: symba-sdk-python

```
symba-sdk-python/
├── pyproject.toml               # ranges (1.2); package symba-sdk, import symba
├── README.md                    # quickstart: worker in 15 lines, submit in 5
├── Makefile                     # proto-gen SYMBA_TAG=vX.Y.Z, test, lint
├── src/symba/
│   ├── __init__.py              # public API: Engine, Worker, Ctx, JobHandle, errors, testing
│   ├── _proto/                  # generated stubs, committed, regenerated per engine tag
│   ├── engine.py                # Engine (async client) + engine.sync facade
│   ├── worker.py                # Worker: registration, claim loop, dispatch
│   ├── context.py               # Ctx implementation (10.3)
│   ├── job.py                   # JobHandle: result()/status()/cancel()
│   ├── task_registry.py         # @worker.task bookkeeping + strict_schemas enforcement
│   ├── schemas.py               # pydantic validate/serialize glue
│   ├── profiles.py              # io/cpu/gpu execution profiles
│   ├── executors/
│   │   ├── base.py              # Executor protocol
│   │   ├── asyncio_executor.py
│   │   ├── process_executor.py
│   │   └── gpu_executor.py
│   ├── checkpoint.py            # redis fast path + engine write-behind
│   ├── heartbeat.py             # per-job heartbeat shell
│   ├── middleware.py            # WorkerMiddleware protocol + builtin logging/metrics mw
│   ├── retry_classify.py        # retryable-vs-fatal classification (Section 8)
│   ├── errors.py                # exception taxonomy (Section 8)
│   ├── logging.py               # structlog defaults; inherits app config if present
│   ├── transport.py             # channel lifecycle, keepalive opts, reconnect
│   ├── cli.py                   # `symba` CLI: run worker, submit, query, signal (optional extra)
│   └── testing.py               # SymbaTest in-memory engine
└── tests/
    ├── unit/
    └── e2e/                     # against dockerized engine (compose from symba repo)
```

Rules: stubs committed (no protoc needed by users); `symba.testing` ships in the main package (not an extra) because test ergonomics are a core feature; the SDK never imports server code — protocol version negotiation happens in the Claim/Submit handshake (`sdk_version` field), and the engine rejects SDKs outside its supported range with a clear error naming both versions.

---

## 4. Wire contract (protobuf)

`buf` enforces backward compatibility in CI: no field renumbering, no type changes, additions only. Full message set below; field-by-field semantics follow the design doc ADs.

### 4.1 common.proto

```protobuf
syntax = "proto3";
package symba.v1;
import "google/protobuf/timestamp.proto";

enum JobState {
  JOB_STATE_UNSPECIFIED = 0;
  SUBMITTED = 1;   // deps/run_at not yet satisfied
  QUEUED    = 2;   // claimable
  RUNNING   = 3;
  WAITING   = 4;   // suspended on wait_for_event
  SUCCEEDED = 5;
  DEAD      = 6;   // attempts exhausted / fatal
  CANCELLED = 7;
}

message RetryPolicy {
  uint32 max_attempts    = 1;   // default 5
  double backoff_base_s  = 2;   // default 1.0
  double backoff_factor  = 3;   // default 2.0
  double backoff_max_s   = 4;   // default 300
  bool   jitter          = 5;   // default true (full jitter)
}

message Dependency {
  string job_id = 1;
  string alias  = 2;            // key in ctx.output; defaults to producer task_name
}

message JobSpec {
  string task_name        = 1;
  bytes  payload_json     = 2;  // UTF-8 JSON, engine schema-blind, cap 256KB
  string pipeline         = 3;  // optional grouping
  string stage            = 4;
  string ctx_id           = 5;  // correlation
  string group_key        = 6;
  string dedup_key        = 7;
  repeated string runs_on = 8;  // tag routing
  string rate_class       = 9;
  int32  priority         = 10;
  uint32 timeout_s        = 11;
  uint32 lease_ttl_s      = 12;
  RetryPolicy retry       = 13;
  google.protobuf.Timestamp run_at = 14;
  repeated string chain   = 15; // tail; engine stores linked list
  repeated Dependency depends_on = 16;
  JobSpec on_failure      = 17; // failure hook
  uint32 max_concurrent_per_group = 18; // per-group concurrency ceiling; 0 = uncapped
}

message UpstreamResult {
  string key = 1;               // task_name or alias
  string job_id = 2;
  bytes result_json = 3;
}

message Job {
  string id = 1;
  string tenant = 2;
  JobSpec spec = 3;
  JobState state = 4;
  uint32 attempt = 5;
  bytes result_json = 6;
  string claimed_by = 7;
  string last_error = 8;
  google.protobuf.Timestamp created_at = 9;
  google.protobuf.Timestamp started_at = 10;
  google.protobuf.Timestamp finished_at = 11;
  repeated UpstreamResult upstream = 12;  // ctx.output source
}
```

### 4.2 data_plane.proto (worker <-> engine)

```protobuf
service WorkerService {
  // Bidirectional stream, one per worker process. Worker announces itself and
  // keeps free_slots current; engine pushes assignments. Flow control is
  // worker-driven: the engine never assigns beyond the last announced slots.
  rpc Claim(stream ClaimRequest) returns (stream JobAssignment);

  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);   // unary, cheap
  rpc Complete(CompleteRequest) returns (CompleteResponse);
  rpc Fail(FailRequest) returns (FailResponse);
  rpc Wait(WaitRequest) returns (WaitResponse);

  rpc PutCheckpoint(PutCheckpointRequest) returns (PutCheckpointResponse);
  rpc GetCheckpoint(GetCheckpointRequest) returns (GetCheckpointResponse);

  // Lazy tier (rev 3): fetch a deep ancestor's result on demand.
  // ctx.output[name] for a task NOT in {immediate predecessor, depends_on}
  // resolves through this; SDK memoizes per execution. Reads jobs_all
  // (hot first, archive fallback), scoped to the caller's ctx_id + tenant.
  rpc GetResult(GetResultRequest) returns (GetResultResponse);
}

message GetResultRequest {
  string job_id = 1;            // the RUNNING job asking (authz scope)
  string lease_token = 2;
  string task_name = 3;         // ancestor task_name/alias within this ctx_id
}
message GetResultResponse {
  bytes result_json = 1;        // <= 64KB by construction (result cap)
  bool found = 2;               // false -> not an ancestor / no result yet
}

message ClaimRequest {
  string worker_id = 1;         // stable name (see logging spec 7.3)
  repeated string tags = 2;
  uint32 free_slots = 3;
  string sdk_version = 4;
  map<string, string> labels = 5;  // host, gpu model, region -- fleet view metadata
}

message JobAssignment {
  Job job = 1;
  string lease_token = 2;       // opaque; must accompany all mutations
  google.protobuf.Timestamp lease_expires_at = 3;
  bytes checkpoint_json = 4;    // pre-loaded checkpoint if one exists
  bytes event_payload_json = 5; // consumed signal payload on WAITING resume
}
// Job.upstream carries ONLY the inline tier (rev 3): immediate chain
// predecessor + declared depends_on results, <= 256KB total (submit-enforced).
// Deeper ancestors: GetResult.

message HeartbeatRequest { string job_id = 1; string lease_token = 2; }
message HeartbeatResponse {
  google.protobuf.Timestamp lease_expires_at = 1;
  bool cancelled = 2;           // cooperative-cancel flag: worker should abort
}

message CompleteRequest {
  string job_id = 1;
  string lease_token = 2;
  bytes result_json = 3;        // cap 64KB
  bool drop_chain_tail = 4;     // ctx.stop_chain()
  bool skipped = 5;             // ctx.skip()
}

message FailRequest {
  string job_id = 1;
  string lease_token = 2;
  string error_type = 3;        // exception class name
  string error_message = 4;     // truncated to 2KB
  string stack_hash = 5;        // sha256[:16] of stack -- groups identical failures in UI
  bool retryable = 6;
}

message WaitRequest {
  string job_id = 1;
  string lease_token = 2;
  string wait_key = 3;
  uint32 timeout_s = 4;         // mandatory
}
```

### 4.3 control_plane.proto (client <-> engine)

```protobuf
service ClientService {
  rpc Submit(SubmitRequest) returns (SubmitResponse);        // 1..n specs, one transaction
  rpc FanOut(FanOutRequest) returns (FanOutResponse);        // children + gate
  rpc Query(QueryRequest) returns (QueryResponse);           // paged
  rpc GetJob(GetJobRequest) returns (Job);
  rpc AwaitJob(AwaitJobRequest) returns (Job);               // server-side long-poll
  rpc Cancel(CancelRequest) returns (CancelResponse);        // job or tree; see state matrix below
  rpc Signal(SignalRequest) returns (SignalResponse);        // wait/signal coordination
  rpc Resubmit(ResubmitRequest) returns (SubmitResponse);    // DLQ replay
  rpc StreamEvents(StreamEventsRequest) returns (stream JobEvent);  // live tail by ctx_id
}

message SubmitRequest { string tenant = 1; repeated JobSpec specs = 2; }
message SubmitResponse { repeated string job_ids = 1; repeated bool deduplicated = 2; }

message QueryRequest {
  string tenant = 1;
  string ctx_id = 2;            // any subset of filters
  JobState state = 3;
  string task_name = 4;
  string pipeline = 5;
  string stage = 6;
  string group_key = 7;
  google.protobuf.Timestamp created_after = 8;
  uint32 page_size = 9;         // default 100, max 1000
  string page_token = 10;       // keyset cursor (created_at, id)
}

message SignalRequest {
  string tenant = 1;
  string wait_key = 2;
  bytes payload_json = 3;
  string signaled_by = 4;       // audited into job_events
}
```

The FastAPI control plane mirrors this 1:1 as REST (`POST /v1/jobs`, `GET /v1/jobs`, `POST /v1/signals`, `POST /v1/jobs/{id}/resubmit`, `GET /v1/events/stream` as SSE). Same service classes serve both transports; the REST layer exists for the UI and for curl-ability, gRPC for programmatic clients.

---

## 5. Database: DDL and hot-path SQL

**This section is the single owner of the schema** (rev 3; the companion design doc's data model section is conceptual only). Here: the authoritative DDL, the connection topology, and the exact hot-path SQL with its concurrency argument.

### 5.0 The hot/archive split — the load-bearing decision

Every production Postgres queue that survived scale evacuates terminal rows from the claimable table (Graphile deletes them, Oban Pro partitions finished states, pgmq archives, Hatchet split queue tables from monitoring tables). The reason is MVCC, not locking: every state transition creates dead tuples, a single long transaction anywhere pins the vacuum horizon, and the claim scan degrades into a hot loop over invisible tuples — the classic SKIP-LOCKED queue collapse (brandur, PlanetScale 2025).

```
            submit                    terminal (same transaction)
  client ───────────►  jobs  ─────────────────────────►  jobs_archive
                      (hot:                              (partitioned monthly;
                       live states only:                  retention = partition drop;
                       submitted/queued/                  DEAD partitions exempt
                       running/waiting;                   while unresolved — DLQ)
                       physically tiny)
                         │ every transition                    ▲
                         ▼ same tx                             │ UI/API read
                     job_events (ledger, partitioned)      UNION view jobs_all
```

Rules:
- `jobs` holds **live states only**. `Complete`/`Fail(fatal)`/`Cancel` move the row to `jobs_archive` **in the terminal transaction** (INSERT INTO archive ... DELETE FROM jobs — atomic, crash-safe).
- Queries/UI read `jobs_all`, a UNION ALL view; the claim path never touches the archive.
- Resubmit (DLQ replay) inserts a **fresh row** into `jobs` referencing the archived original (`resubmitted_from`).
- Retention: archive partitions dropped after `succeeded_jobs_days`; partitions still containing unresolved DEAD jobs are skipped (the DLQ contract — a DEAD job is never silently pruned).

### 5.1 Authoritative DDL (database/symba/V001__jobs.sql, abridged to the decisions that matter)

```sql
-- Postgres 18 required: uuidv7() gives time-ordered PKs -> right-leaning B-tree
-- inserts, no random-UUID index bloat (Hatchet measured this; rev 3 decision).
CREATE TABLE jobs (
    id                UUID PRIMARY KEY DEFAULT uuidv7(),
    task_name         TEXT NOT NULL,
    pipeline          TEXT,
    stage             TEXT,
    tenant            TEXT NOT NULL DEFAULT 'default',
    ctx_id            TEXT,
    state             TEXT NOT NULL DEFAULT 'submitted',
                      -- LIVE states only: submitted|queued|running|waiting
    priority          SMALLINT NOT NULL DEFAULT 0,
    group_key         TEXT,
    max_concurrent_per_group SMALLINT,
    wait_key          TEXT,
    wait_expires_at   TIMESTAMPTZ,
    event_payload     JSONB,                        -- consumed signal payload
    dedup_key         TEXT,
    runs_on           TEXT[] NOT NULL DEFAULT '{}',
    rate_class        TEXT,
    payload           JSONB NOT NULL,
    result            JSONB,                        -- populated transiently pre-archive
    parent_gate_id    UUID,
    on_success        TEXT,
    chain_tail        TEXT[] NOT NULL DEFAULT '{}',
    on_failure        JSONB,
    remaining_deps    INT NOT NULL DEFAULT 0,
    attempt           SMALLINT NOT NULL DEFAULT 0,
    max_attempts      SMALLINT NOT NULL DEFAULT 5,
    backoff           JSONB,
    timeout_s         INT NOT NULL DEFAULT 600,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_ttl_s       INT NOT NULL DEFAULT 60,
    lease_token       TEXT,                         -- guards every mutation (5.3/5.4)
    claimed_by        TEXT,
    lease_expires_at  TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    resubmitted_from  UUID,                         -- DLQ replay lineage
    error_history     JSONB NOT NULL DEFAULT '[]',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ
)
WITH (
    -- MVCC hygiene: the hot table must be vacuumed aggressively and cheaply.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold   = 200,
    autovacuum_vacuum_cost_limit  = 2000,
    autovacuum_vacuum_cost_delay  = 2,
    fillfactor = 85                                  -- room for HOT updates on transitions
);

-- Terminal rows: same shape + terminal fields, partitioned for drop-based retention.
CREATE TABLE jobs_archive (
    LIKE jobs INCLUDING DEFAULTS,
    finished_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    final_state  TEXT NOT NULL                       -- succeeded|dead|cancelled
) PARTITION BY RANGE (finished_at);                  -- monthly; nightly pre-create

CREATE VIEW jobs_all AS
    SELECT j.*, NULL::timestamptz AS finished_at, j.state AS final_state FROM jobs j
    UNION ALL
    SELECT a.*, a.finished_at, a.final_state FROM jobs_archive a;  -- (column-aligned in the real migration)

-- Per-group running counts (the concurrency-ceiling design), maintained transactionally on claim/terminal.
-- Replaces the per-candidate count(*) subquery on the claim path (rev 3, perf review).
CREATE TABLE group_running (
    tenant     TEXT NOT NULL,
    group_key  TEXT NOT NULL,
    task_name  TEXT NOT NULL,
    running    INT  NOT NULL DEFAULT 0 CHECK (running >= 0),
    PRIMARY KEY (tenant, group_key, task_name)
);
```

Indexes:

```sql
CREATE INDEX ix_jobs_claimable ON jobs (priority DESC, run_at ASC)
    WHERE state = 'queued';                          -- THE claim scan; partial = tiny
CREATE INDEX ix_jobs_runs_on   ON jobs USING GIN (runs_on)
    WHERE state = 'queued';                          -- tag cover check
CREATE UNIQUE INDEX ux_jobs_dedup ON jobs (tenant, dedup_key)
    WHERE dedup_key IS NOT NULL;                     -- F9: hot table = live jobs only,
                                                     -- so this is dedup-among-live by construction
CREATE INDEX ix_jobs_ctx       ON jobs (tenant, ctx_id, created_at DESC);   -- correlation lookup
CREATE INDEX ix_jobs_lease     ON jobs (lease_expires_at)
    WHERE state = 'running';                         -- sweeper reclaim scan
CREATE INDEX ix_jobs_waiting   ON jobs (tenant, wait_key)
    WHERE state = 'waiting';                         -- signal fan-in
CREATE INDEX ix_jobs_state_task ON jobs (tenant, state, task_name, created_at DESC);  -- UI
-- archive: ctx_id, (tenant, final_state, task_name), dedup lineage — per partition
```

`job_events` (database/symba/V002__job_events.sql): declarative-partitioned by month on `at`; **BRIN index on `at`** (append-only, time-correlated — B-tree wasted here), B-tree on `(job_id, at)`; **no FK to jobs** (the job row moves to the archive; the ledger outlives it); `fillfactor = 100` (never updated). Retention is `DROP TABLE` of old partitions; a nightly job pre-creates the next partition. Events for DEAD jobs are exempt from pruning while the archived job exists.

**Operational invariant (alert, not just docs): `max(age(backend_xmin))` on the instance is the leading indicator of claim-path collapse** — a stuck analytics transaction pins the vacuum horizon and the partial indexes stop protecting you. Alert at 5 minutes (Section 14).

### 5.2 Connection topology

| Pool | Size (default) | Used by | Isolation |
|---|---|---|---|
| `hot` | 20 | claim, complete, fail, heartbeat, get_result | `command_timeout=5s` — a slow statement here is a bug, fail fast |
| `general` | 20 | submit, query, UI, sweeper, cron, signals | `command_timeout=30s` |

Sizing math (rev 3): at the throughput target of 500 claims/s + 500 completes/s per instance with ~2ms per hot statement, the hot pool carries ~2 concurrent statements steady-state — 20 connections is ~10x headroom for p99 spikes and heartbeat bursts (a 100-worker fleet at 15s intervals ≈ 7 heartbeats/s). `symba_pool_acquire_wait_seconds` (Section 14) is the saturation early-warning; resize by config, never by guesswork. Total 40 connections per engine instance — well within a default PG 18 `max_connections=100`, and 2 instances still fit. No LISTEN connection exists (rev 3: dispatch is polling-only).

### 5.3 Claim (THE hot query — `db/queries/claim.sql`)

```sql
-- Context: one transaction (claim + counter bumps). Safe for N concurrent engine
-- instances and N workers: FOR UPDATE SKIP LOCKED serializes row claims without blocking.
WITH ranked AS (
    SELECT j.id, j.group_key, j.task_name, j.tenant, j.max_concurrent_per_group,
           row_number() OVER (PARTITION BY j.group_key
                              ORDER BY j.priority DESC, j.run_at ASC) AS rn_in_group
    FROM jobs j
    LEFT JOIN group_running g                        -- O(1) counter lookup,
           ON (g.tenant, g.group_key, g.task_name)   -- not a per-candidate count(*)
            = (j.tenant, j.group_key, j.task_name)
    WHERE j.state = 'queued'
      AND j.run_at <= now()
      AND j.runs_on <@ $1::text[]                    -- worker tags cover runs_on
      AND ($2::text[] = '{}' OR j.rate_class IS NULL
           OR NOT (j.rate_class = ANY($2)))          -- exhausted classes skipped
      AND (j.max_concurrent_per_group IS NULL
           OR COALESCE(g.running, 0) < j.max_concurrent_per_group)
),
candidate AS (
    SELECT r.id
    FROM ranked r
    JOIN jobs j2 ON j2.id = r.id
    WHERE r.rn_in_group <= $5                        -- fairness: per-group cap INSIDE
    ORDER BY j2.priority DESC, r.rn_in_group ASC,    -- the batch, so one giant group
             j2.run_at ASC                           -- cannot monopolize the LIMIT
    LIMIT $3
    FOR UPDATE OF j2 SKIP LOCKED
)
UPDATE jobs j
SET state = 'running',
    claimed_by = $4,
    lease_token = gen_random_uuid()::text,
    attempt = attempt + 1,
    started_at = COALESCE(j.started_at, now()),
    lease_expires_at = now() + make_interval(secs => j.lease_ttl_s),
    last_heartbeat_at = now()
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;

-- Same transaction: bump the counters for capped groups among the claimed rows.
INSERT INTO group_running (tenant, group_key, task_name, running)
SELECT tenant, group_key, task_name, count(*) FROM claimed_rows
WHERE max_concurrent_per_group IS NOT NULL GROUP BY 1,2,3
ON CONFLICT (tenant, group_key, task_name) DO UPDATE
SET running = group_running.running + EXCLUDED.running;
```

`$5` = per-group batch cap (config `matcher.max_per_group_per_batch`, default `GREATEST(2, LIMIT/8)`).

Correctness arguments:
- **No double-claim:** the `FOR UPDATE SKIP LOCKED` in the CTE locks candidate rows; a concurrent claim skips locked rows instead of blocking. Two engines claiming for two workers can never return the same row.
- **Readiness revalidated at lock time:** `run_at <= now()` sits inside the locking statement itself, never delegated to an earlier dispatcher scan. This pins a well-known claim-revalidation bug class in Redis-backed task queues (worker B claimed from a stale poll batch a job worker A had just deferred); one SQL statement makes the read and the lock structurally ordered — a property that has to be restored by hand in designs where a later refactor (e.g. introducing concurrent gathering) accidentally splits the read from the lock. **Regression risk: any future refactor that splits "find candidates" from "lock candidates" into two statements reintroduces both bugs.**
- **One clock source:** every time comparison in every query uses DB `now()` — no client timestamps in SQL, ever (a well-known failure mode in Postgres-backed queues is mixing client and server clocks, which fires cron early; the fix is reverting to a single clock).
- **Per-group concurrency ceiling under concurrency (rev 3 — counter table, not count(*)):** `group_running` is bumped in the claim transaction and decremented in the terminal/requeue transaction, so it is exact under any interleaving *within* one transaction's view. Two concurrent multi-engine claims can still each read `running=0` for the same group (their transactions haven't committed); the residual race is bounded to (number of concurrent claim batches) and only for *distinct* jobs of one group. The matcher additionally serializes assignments per `(group_key, task_name)` within its pass, making `max_concurrent_per_group=1` exact on a single engine; `strict_group_caps=true` escalates to `pg_advisory_xact_lock(hashtext(tenant||group_key))` for multi-engine exactness. A sweeper statement reconciles counters against reality every pass (drift is self-healing, never cumulative).
- **Fairness (rev 3 — inside the query, not after):** the `row_number() ... PARTITION BY group_key` cap means a 5,000-chunk document can take at most `$5` slots of any one batch — other documents claim in the same tick instead of starving behind it. The matcher's round-robin interleave (6.2) remains as assignment-order shaping on top.
- **Rate classes:** the matcher reserves tokens BEFORE running this query and passes exhausted class names as `$2`. Unused reservations (query returned fewer rows) are returned to the bucket. Keeps the SQL sargable.
- **Upstream results (rev 3 — bounded):** a second query per assignment batch fetches only each job's *immediate chain predecessor* result + declared `depends_on` results (`WHERE id = ANY($ids)`), attached to `JobAssignment.upstream` under the 256KB-per-assignment cap enforced at submit time. Deeper ancestors are served on demand by the `GetResult` RPC (4.2) from `jobs_all` (hot table first, archive fallback) — the SDK memoizes per execution.

### 5.4 Complete (`db/queries/complete.sql` — one transaction)

```sql
-- Statement 1: terminal move, lease-guarded: DELETE from hot + INSERT into archive
-- in one atomic statement. 0 rows -> STALE_LEASE: another attempt owns the job.
WITH moved AS (
    DELETE FROM jobs
    WHERE id=$1 AND lease_token=$2 AND state='running'
    RETURNING *
)
INSERT INTO jobs_archive
SELECT m.*, now() AS finished_at, 'succeeded' AS final_state FROM moved m
RETURNING on_success, chain_tail, parent_gate_id, group_key, task_name,
          max_concurrent_per_group, ctx_id, pipeline, priority, tenant;
-- ($3 = result is set on the moved row via the CTE in the real query)

-- Statement 2: release the group slot (rev 3: counter table)
UPDATE group_running SET running = running - 1
WHERE (tenant, group_key, task_name) = ($tenant, $group_key, $task_name)
  AND $max_concurrent_per_group IS NOT NULL;

-- Statement 3 (if on_success != NULL and NOT drop_chain_tail): insert continuation
INSERT INTO jobs (task_name, payload, ctx_id, pipeline, group_key, priority, tenant,
                  on_success, chain_tail, state, remaining_deps, ...)
VALUES ($next_task, $inherited..., 'queued', 0, ...);

-- Statement 4: decrement dependents; newly-ready flip to queued
WITH dec AS (
    UPDATE jobs SET remaining_deps = remaining_deps - 1
    WHERE id IN (SELECT job_id FROM job_dependencies WHERE depends_on_job_id = $1)
    RETURNING id, remaining_deps, run_at
)
UPDATE jobs j SET state='queued' FROM dec
WHERE j.id = dec.id AND dec.remaining_deps = 0 AND j.state='submitted' AND dec.run_at <= now();

-- Statement 5: gate bump; fire exactly once
UPDATE gates SET completed_children = completed_children + 1
WHERE id = $gate_id
RETURNING (completed_children >= expected_children) AS ready, fired_at;
-- if ready and fired_at IS NULL: UPDATE gates SET fired_at=now() WHERE id=$gate_id AND fired_at IS NULL
-- (rowcount=1 -> this transaction owns the continuation insert; rowcount=0 -> raced, skip)

-- Statement 6: audit
INSERT INTO job_events (job_id, event, at, detail) VALUES ($1, 'completed', now(), $detail);
```

After commit: nothing to signal — the dispatcher's next tick (≤50ms away) sees any newly-queued continuations/dependents. (rev 3: no NOTIFY anywhere; polling dispatcher by design.)

**`GetResult` reads (lazy tier)** hit `jobs_all`: hot table first (job still live or just-completed sibling in a fan-out), archive fallback via `ctx_id`-scoped indexes. Results live as long as the archived row does.

### 5.5 Fail path (`fail.sql`)

Retryable + attempts remaining: `state='queued'`, `run_at = now() + backoff(attempt)` (computed in `core/retry.py`, passed as parameter: full-jitter `random(0, min(cap, base*factor^attempt))`), append error to `error_history` JSONB array (capped at last 20, older entries evicted), decrement `group_running` (the slot is released while the job waits out its backoff). Fatal or exhausted: **move the row to `jobs_archive` with `final_state='dead'`** (same CTE pattern as 5.4), decrement `group_running`; insert `on_failure` hook job if the spec carries one; cascade-cancel dependents (recursive CTE over `job_dependencies`, all transitively dependent non-terminal live jobs -> archive with `final_state='cancelled'`, each with a `job_events` row naming the root cause job id).

### 5.6 Sweeper pass (advisory-locked, every 5s)

```sql
SELECT pg_try_advisory_lock(hashtext('symba:sweeper'));  -- non-blocking; loser skips the pass
```
One pass = six statements: reclaim expired leases (-> fail path with `lease_reclaimed` event, `group_running` decrement included); expire waits (`state='waiting' AND wait_expires_at < now()` -> `queued`, `wait_timed_out` event, null event payload); reconcile `group_running` against actual running counts (self-healing counter drift, 5.3); GC checkpoints/signals past retention; pre-create next month's `jobs_archive`/`job_events` partitions + drop expired ones (DEAD-containing partitions skipped); mark workers stale (`last_seen < now() - 3 * heartbeat_interval`).

Provenance — two well-known sweeper mistakes in this design space, pinned as tests: a naive first-pass sweeper implementation can reclaim *every* active job without checking stuckness — Symba's reclaim predicate is exactly `lease_expires_at < now()`, tested as a dedicated regression case; sweepers can also miss a scope predicate and sweep other queues' jobs — every Symba sweeper statement carries its full tenant/state scope, and L2 tests seed foreign-scope rows that must survive a pass. The session-scoped advisory lock (auto-released if the holder dies) is the same kind of election other Postgres-backed queues keep after learning that per-job advisory locks cause operational instability (stale locks, unlock-on-disconnect bugs) — election is the only justified use of advisory locks in a queue.

### 5.7 Signal rendezvous (`signal.sql`)

Both orders race-free in one transaction each:
- **Signal first:** insert `signals` row; `UPDATE jobs SET state='queued', wait_key=NULL, event_payload=$payload WHERE tenant=$1 AND wait_key=$2 AND state='waiting'`; mark consumed for matched jobs. Next dispatcher tick picks the job up.
- **Wait first:** the `Wait` RPC transaction first does `SELECT ... FROM signals WHERE tenant=$1 AND wait_key=$2 AND consumed_at IS NULL FOR UPDATE SKIP LOCKED LIMIT 1`; if found -> consume it, job never parks (returns payload immediately); else -> `state='waiting'`, `wait_expires_at = now() + timeout`.

### 5.8 Cancel — the per-state matrix (`cancel.sql`)

A recurring failure mode in reference task queues: cancel paths that only work for *running* jobs, silently no-oping for jobs still deferred or queued (aborting a not-yet-running job does nothing because no worker would ever see the flag), or setting a flag that nothing ever reads. Cancel takes `SELECT ... FOR UPDATE` on the row and acts per state — every state has an explicit outcome, none falls through:

| State at cancel | Action (one transaction) |
|---|---|
| `submitted` / `queued` / `waiting` | Move directly to archive `final_state='cancelled'`; decrement nothing (never claimed); cascade to dependents/chain (the cascade-cancel design) |
| `running` | Set a `cancel_requested` flag on the row; the next `Heartbeat` response carries `cancelled=true` (cooperative, 10.5); worker confirms via `Fail`; lease expiry is the backstop if the worker is dead |
| terminal (already archived) | No-op, idempotent; response says so |

The `run_at`-in-the-future case needs no special path (unlike the score-rewrite hacks some reference queues need for deferred jobs): a scheduled job is just `queued`/`submitted` state and cancels directly.

---

## 6. Engine internals

### 6.1 main.py wiring

```python
async def main() -> None:
    cfg = SymbaConfig()                       # pydantic-settings, SYMBA_* env + .env + symba.toml
    # Logging is configured at import of symba.observability.logging (module-level
    # `logger`, conventional structlog style). No configure_logging() call needed.
    pools = await create_pools(cfg.postgres)  # hot + general, search_path=symba
    # NOTE: schema migrations are applied by the Flyway sidecar BEFORE the engine
    # starts (docker-compose depends_on: flyway service_completed_successfully).
    # The engine does NOT self-migrate. Tests use symba.db.migrate.apply_schema.

    buckets = RateLimiter(cfg.redis, pools.general)
    services = build_services(pools, buckets, cfg)

    async with asyncio.TaskGroup() as tg:
        if "api" in cfg.server.roles or "all" in cfg.server.roles:
            tg.create_task(serve_grpc(services, cfg))
            tg.create_task(serve_http(services, cfg))
            tg.create_task(Dispatcher(services, cfg).run())   # 6.2: THE latency tier
        if "sweeper" in cfg.server.roles or "all" in cfg.server.roles:
            tg.create_task(Sweeper(services, cfg).run())
            tg.create_task(CronService(services, cfg).run())
```

`TaskGroup` semantics give us the crash-only property for free: any subsystem dying with an unhandled exception tears down the process; the orchestrator (compose/K8s) restarts it; Postgres state makes restart safe.

### 6.2 The dispatcher + matcher (claim_service)

In-memory registry: `worker_id -> WorkerConn(tags: frozenset, free_slots: int, stream, labels)`.

**The dispatcher loop (rev 3 — the only latency tier in the system):**

```python
async def run(self) -> None:
    tick = self.cfg.dispatcher.min_tick_ms          # 10ms
    while True:
        try:
            assigned = await self.matcher.pass_()   # 0 rows when nothing ready
        except Exception:
            # A pass may fail (PG blip, one bad row); the LOOP may not die.
            # A well-known failure mode in reference implementations: one
            # unclamped TTL in the poll loop froze the whole worker silently.
            # Log ERROR, count it, back off to max tick.
            logger.error("[dispatcher] Pass failed", exc_info=True)
            assigned = 0
        # adaptive: busy -> floor; idle -> decay toward max_tick_ms (250ms)
        tick = (self.cfg.dispatcher.min_tick_ms if assigned
                else min(tick * 2, self.cfg.dispatcher.max_tick_ms))
        await asyncio.sleep(tick / 1000)
```

The same containment rule applies to the sweeper and cron loops: **an exception in one pass is logged and counted (`symba_loop_errors_total{loop}`), never propagated** — a permanently-failing loop is surfaced by the metric alert, not by silent death. (This deliberately overrides the Section 6.1 crash-only default for the three periodic loops: a transient PG blip must not tear down the process every few seconds; a *persistent* failure still pages via the alert.)

Two extra wake sources short-circuit the sleep (they `set()` an `asyncio.Event`; they are optimizations, never required for correctness): a local submit landing on this instance, and a worker's `free_slots` going 0 → positive. An idle system costs one indexed `LIMIT 0`-ish query per 250ms per tag group — constant, trivially monitorable (`symba_dispatch_pass_seconds`).

Matching pass (single async task — no matcher concurrency to reason about within one engine):
1. Snapshot workers with `free_slots > 0`, group by tag-set (workers with identical tags share one claim query).
2. Peek distinct `rate_class` values present in the ready set (cheap indexed `SELECT DISTINCT`); reserve tokens per class; classes with empty buckets go into the exhausted array `$2`.
3. Run claim (5.3) with `LIMIT = sum(free_slots)` for the tag group.
4. **Fairness shaping:** interleave the returned rows round-robin by `group_key` (equal priority band only — priority always wins first). Then assign to workers round-robin.
5. Push `JobAssignment`s; decrement local `free_slots`; return unused rate tokens.

Per-group exactness for `max_concurrent_per_group=1`: the matcher tracks in-flight assignments per `(group_key, task_name)` within the pass, never assigning two in one batch; combined with the SQL guard this makes the cap exact on a single engine and near-exact multi-engine (documented; strict multi-engine mode via advisory lock is a config flag `strict_group_caps=true`).

### 6.3 Rate limiter

Token bucket per `rate_class`, config stored in PG (`rate_classes` table: `name, capacity, refill_per_s`), cached in memory, editable at runtime via admin API (change takes effect next refill tick).

- **Redis mode:** one Lua script does refill-and-take atomically (`EVALSHA`, keys `rl:{class}`, fields `tokens, ts`). Shared across engine instances — engine-wide correctness.
- **PG fallback:** `UPDATE rate_classes SET tokens = LEAST(capacity, tokens + (extract(epoch from now()-refilled_at) * refill_per_s)) - $take, refilled_at = now() WHERE name=$1 AND ... RETURNING tokens` under the general pool. Higher latency, same semantics (the degraded-mode design: Redis loss degrades latency, never correctness).
- 429-feedback: a worker failing a job with `error_type` classified as rate-limit ALSO reports the class; the engine empties that bucket immediately (fast back-off engine-wide rather than N workers discovering individually).

### 6.4 Backpressure and hard limits

| Limit | Default | On breach |
|---|---|---|
| `result_json` size | 64KB | Complete rejected: `RESULT_TOO_LARGE` naming the inline-vs-lazy-result design ("store a reference, not the data") |
| `payload_json` size | 256KB | Submit rejected: `PAYLOAD_TOO_LARGE` |
| chain length | 50 | Submit rejected (a longer chain is a design smell -> use fan-out/deps) |
| fan-out children per gate | 100k | Submit rejected |
| per-tenant queued jobs | off (configurable) | Submit returns `RESOURCE_EXHAUSTED` (gRPC) / 429 (REST) — retryable by contract |
| query page size | 1000 | clamped |

### 6.5 Cron service — dedup-key upsert, no leader election

Deliberately the simplest mechanism that is correct, because multiple prior-art designs converged on it independently:

- Every engine instance's cron loop ticks (1s); for each enabled schedule it computes the next fire time and submits a job with a **deterministic dedup key**: `dedup_key = "cron:{schedule_id}:{next_fire_iso}"`. Dedup across N instances falls out of the ordinary `ux_jobs_dedup` unique index — **cron correctness does not depend on the advisory-lock election** (the election only avoids redundant work; a dedup-key-upsert design has no election at all and is still correct).
- The fire time is computed by advancing from the *intended previous tick*, not from `now()` (a well-known failure mode: computing from `now()` under skewed loop timing produces both duplicates and skipped ticks).
- **No missed-window backfill**: if all instances are down for an hour, the schedule fires once on recovery, not 60 times (a common design choice in this space; backfill is an app-level decision).
- A schedule whose previous occurrence is still live simply dedups away — no overlap without needing an extra "still running" check.
- Guard against a well-known cron failure mode: a computed fire time in the past is clamped to `now()`; the submit for one schedule failing is logged and skipped, never allowed to kill the loop (Section 6.2 containment rule).

## 7. Logging specification

Symba follows a conventional structlog pipeline design — a fixed processor chain, a stable JSON shape, deterministic worker-name resolution — so any existing ELK/Fluentbit setup that already ingests structured JSON logs needs zero mapping work to pick up Symba's. Env prefix is `SYMBA_`, and the bound context keys are engine-domain (`job_id`, not any app-specific entity id).

### 7.1 Processor chain (both engine and SDK)

```python
structlog.configure(processors=[
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    add_app_context,        # app_name="symba", app_version, environment, worker_name
    add_trace_id,           # otel trace_id from contextvar (empty string if no span)
    add_error_info,         # 'error' key -> {'exception': {name, message, stack}} (same shape as the client's existing setup)
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.JSONRenderer(),    # console renderer in dev (SYMBA_LOG__FORMAT=console)
])
```

Handler policy identical to the client's existing setup: stdout by default; `SYMBA_LOG__FILE_PATH` adds a `RotatingFileHandler` (50MB x 5) for Fluentbit tailing; third-party loggers (`grpc`, `asyncpg`, `httpx`, `uvicorn.access`) capped at WARNING and routed through the structlog handler.

### 7.2 Mandatory bound context

Every logger in the codebase is bound at module import, matching the convention used elsewhere in the client's stack:

```python
from symba.observability.logging import logger
logger = logger.bind(service="claim_service", context="engine/services")
```

Per-operation keys — the contract every log line in the job lifecycle must satisfy:

| Key | Present on | Notes |
|---|---|---|
| `job_id` | every job-scoped line | |
| `ctx_id` | every job-scoped line | the app join key; empty string, never absent |
| `task_name`, `attempt`, `state` | every lifecycle transition | |
| `tenant` | every line | |
| `worker_id` | claim/execution lines | |
| `lease_token` (first 8 chars) | lease disputes | full token never logged |
| `duration_ms` | every completed operation | float, 1 decimal |
| `group_key`, `pipeline`, `stage` | when set on the job | |

Event-name convention (`[fn_name] Action` style, matching the client's existing logging convention): `logger.info("[claim] Assigned jobs", worker_id=w, count=n, ...)`. Message strings are static — all variance goes in keys (log aggregation groups by message).

### 7.3 Worker name resolution (SDK)

Adapted from the client's existing `_resolve_worker_name()` helper: precedence `SYMBA_WORKER_NAME` -> `WORKER_NAME` -> `HOSTNAME` -> `<hostname>-<uuid8>`. This is the `worker_id` sent in `ClaimRequest` and stamped on claims — logs, UI fleet view, and `jobs.claimed_by` all show the same stable name.

### 7.4 What is logged at which level

| Level | Engine events |
|---|---|
| DEBUG | claim query timings, dispatcher tick adjustments, bucket reservations |
| INFO | job lifecycle transitions (one line per transition, matching the `job_events` row), worker connect/disconnect, cron fires, signal delivery |
| WARNING | lease reclaimed, wait timed out, retry with backoff > 30s, bucket exhausted > 60s, stale-lease Complete rejected, SDK version near end of support |
| ERROR | job -> DEAD, gate stuck alert, migration failure, hot-pool acquire timeout |
| CRITICAL | Postgres unreachable (engine is down in all but process) |

Rules carried over from the client's existing logging conventions, non-negotiable: **no emojis anywhere; no print(); no unstructured interpolated strings; `import logging` forbidden outside `observability/logging.py`** (ruff banned-import rule enforces it).

### 7.5 SDK logging behavior

The SDK does NOT configure structlog if the host app already has (detected via `structlog.is_configured()`). It only binds its context keys. Standalone workers (no app config) get the same pipeline as the engine. This is exactly what lets a client application's flow modules log through their existing `shared.config.logging` setup while the SDK adds `job_id`/`ctx_id` — `ctx.logger` is a child of whatever pipeline the process owns:

```python
@worker.task("summarize_chunk")
async def summarize_chunk(ctx, payload):
    ctx.logger.info("[summarize_chunk] Starting", chunk_id=payload["chunk_id"])
    # ctx.logger == process logger.bind(job_id=..., ctx_id=..., task_name=..., attempt=...)
```

---

## 8. Error taxonomy and retry classification

### 8.1 SDK exception hierarchy (`symba/errors.py`)

Modeled on a conventional `CustomException` pattern already used by client applications (class-level `error_code` + message, structured `to_dict()`), adapted to job semantics:

```python
class SymbaError(Exception):
    """Base. Carries error_code, retryable default, and structured context."""
    error_code: str = "symba_error"
    retryable: bool = False

    def __init__(self, message: str | None = None, **context):
        self.message = message or self.__class__.message
        self.context = context          # goes into error_history JSONB
        super().__init__(self.message)

# --- raised BY handlers (app -> engine direction) ---
class RetryableError(SymbaError):
    """Explicitly request a retry (overrides classification)."""
    error_code, retryable = "retryable", True

class FatalError(SymbaError):
    """Explicitly refuse retry: job goes DEAD immediately."""
    error_code, retryable = "fatal", False

class RateLimitedError(RetryableError):
    """Retryable AND drains the job's rate_class bucket engine-wide (6.3)."""
    error_code = "rate_limited"
    def __init__(self, message=None, retry_after_s: float | None = None, **ctx): ...

# --- raised BY the SDK (engine -> app direction) ---
class JobFailed(SymbaError):        # JobHandle.result() on DEAD; carries error_history
class JobCancelled(SymbaError):     # JobHandle.result() on CANCELLED
class StaleLease(SymbaError):       # Complete/Fail rejected; SDK swallows + logs WARNING
class ResultTooLarge(SymbaError):   # 64KB cap; names the fix in the message
class PayloadValidationError(SymbaError):   # input_schema rejected the payload
class OutputValidationError(SymbaError):    # output_schema rejected the return value
class AmbiguousResultKey(SymbaError)        # ctx.output name collision without alias
class UnsupportedInProfile(SymbaError)      # wait_for_event outside io profile
class EngineUnavailable(RetryableError)     # transport-level, after channel retries exhausted
```

### 8.2 Default classification of unhandled handler exceptions

When a handler raises something that is not a `SymbaError`, the SDK classifies (`retry_classify.py`) — the same approach as a conventional `is_retryable_error` helper already used in client applications' LLM-calling code, generalized:

| Exception source | Retryable | Rationale |
|---|---|---|
| `TimeoutError`, `asyncio.TimeoutError`, `ConnectionError` and subclasses, `OSError` (transient errnos) | yes | Infrastructure blips |
| httpx/httpcore/aiohttp transport + timeout errors | yes | Same |
| Any exception with `status_code` in {408, 429, 500, 502, 503, 504} | yes | Transient HTTP (429 also triggers bucket drain if the job has a rate_class) |
| Any exception with `status_code` in {400, 401, 403, 404, 422} | no | Permanent — retrying cannot fix auth/validation |
| `KeyError`, `TypeError`, `ValueError`, `AttributeError`, pydantic `ValidationError` | no | Programming/data errors — retry is noise |
| `MemoryError`, `SystemExit`, `KeyboardInterrupt` | no (re-raise) | Process-level, not job-level |
| Anything else | **no** | Same conservative default used elsewhere in client applications: unknown = don't retry. Handlers opt in via `raise RetryableError(...) from exc` |

The classification result travels in `FailRequest.retryable`; the engine trusts it (the SDK is closest to the exception). `error_type`, truncated `error_message` (2KB) and `stack_hash` land in `error_history` and `job_events` — the UI groups DEAD jobs by `stack_hash` so one bug appearing 4000 times reads as one row.

### 8.3 gRPC status mapping (engine responses)

| Condition | gRPC code | REST |
|---|---|---|
| stale lease | `FAILED_PRECONDITION` | 412 |
| dedup hit (submit) | `OK` + `deduplicated=true` | 200 (idempotent by design, not an error) |
| unknown task/tenant | `NOT_FOUND` | 404 |
| payload/result too large, chain too long | `INVALID_ARGUMENT` | 400 |
| tenant queue cap | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| auth failure | `UNAUTHENTICATED` / `PERMISSION_DENIED` | 401 / 403 |
| SDK protocol out of range | `FAILED_PRECONDITION` with both versions in detail | 412 |

---

## 9. Configuration reference

### 9.1 Engine (`config.py`, pydantic-settings; file `symba.toml` + env `SYMBA_<SECTION>__<KEY>`)

Provenance note (rev 3): the *prefixed-env + file* convention mirrors a similar env-prefix scheme used in client applications, but the mechanism deliberately differs — that scheme uses Dynaconf; Symba uses pydantic-settings so config validation errors are pydantic errors like everything else, and the config model doubles as documentation.

```toml
[server]
grpc_port = 7233
http_port = 7300
roles = ["all"]                    # "api" | "sweeper" | "all"
grpc_max_message_mb = 4
# Stream survival (rev 3): long-lived bidi Claim streams die constantly in
# real deployments (LB idle timeouts, NAT table evictions, rolling deploys).
# Invariant: THE LEASE IS TRUTH, THE STREAM IS TRANSPORT — a dropped stream is a
# non-event (leases keep ticking via unary Heartbeat; worker reconnects and
# re-announces; the dispatcher simply stops pushing to a dead stream).
grpc_keepalive_time_ms = 20000        # server pings idle streams (matches client 10.5)
grpc_keepalive_timeout_ms = 5000
grpc_max_connection_age_s = 1800      # + jitter: forcibly recycle streams so LB drains
grpc_max_connection_age_grace_s = 60  # and rolling deploys are exercised constantly,
                                      # not just during incidents
shutdown_drain_s = 30

[postgres]
dsn = "postgresql://symba:...@pg:5432/symba"
hot_pool_size = 20                 # sizing math in 5.2; watch pool_acquire_wait
hot_command_timeout_s = 5
general_pool_size = 20
general_command_timeout_s = 30

[redis]                            # OPTIONAL section; absent -> degraded mode
url = "redis://redis:6379/0"
socket_timeout_s = 2               # Redis slowness must never stall the claim path

[defaults]                         # job-level defaults; task registration and submit override
lease_ttl_s = 60
timeout_s = 600
max_attempts = 5
backoff_base_s = 1.0
backoff_factor = 2.0
backoff_max_s = 300
jitter = true

[limits]
max_result_kb = 64
max_payload_kb = 256
max_chain_len = 50
max_fanout_children = 100000
tenant_queued_cap = 0              # 0 = unlimited
query_max_page = 1000

[sweeper]
interval_s = 5
worker_stale_after_heartbeats = 3

[retention]
job_events_days = 90               # partition drops
succeeded_jobs_days = 30           # DEAD jobs are NEVER auto-pruned (DLQ contract)
checkpoints_hours = 72
consumed_signals_days = 7

[dispatcher]                       # rev 3: THE latency tier; no other knob affects the claim-latency target
min_tick_ms = 10                   # tick while work is flowing
max_tick_ms = 250                  # idle decay ceiling
max_per_group_per_batch = 0        # 0 -> GREATEST(2, limit/8); fairness cap in 5.3

[matcher]
strict_group_caps = false          # true -> advisory-lock exactness for the per-group concurrency ceiling, multi-engine
max_upstream_inline_kb = 256       # inline tier cap, enforced at submit

[auth]
mode = "token"                     # "token" | "mtls" | "none" (dev only, refuses non-loopback)
token_jwks_url = ""                # empty -> static shared-secret tokens from [auth.tokens]

[log]
level = "INFO"
format = "json"                    # "json" | "console" (dev)
file_path = ""                     # set for Fluentbit tailing
enable_stdout = true

[observability]
metrics_enabled = true
otlp_endpoint = ""                 # empty -> tracing off
```

Validation is fail-fast at boot: bad DSN, unknown role, `hot_pool_size < 2` etc. abort with a message naming the exact key. `symba config check` (CLI) validates a file without starting.

### 9.2 Worker (SDK) configuration

```python
worker = Worker(
    engine="grpc://symba.internal:7233",
    token=os.environ["SYMBA_TOKEN"],
    tags=["parse", "gpu"],
    slots=2,
    strict_schemas=True,           # schema validation on/off (10.2)
    profile_defaults={"gpu": {"subprocess_memory_mb": 8192}},
    heartbeat_interval_s=15,       # must be << lease_ttl_s; SDK warns if > ttl/3
    labels={"host": "spark-01", "gpu": "gh200"},   # fleet view metadata
    middleware=[MetricsMiddleware(), ...],
)
```

Every constructor parameter is also readable from env (`SYMBA_ENGINE`, `SYMBA_TOKEN`, `SYMBA_TAGS="parse,gpu"`, `SYMBA_SLOTS`) so the same worker script deploys across heterogeneous boxes with env-only differences — the same pattern used for scaling workers under the legacy workflow orchestrator, kept.

---

## 10. Worker SDK internals

### 10.1 Boot sequence

1. Import flow modules (side effect: `@worker.task` registrations land in `TaskRegistry`).
2. Registry validation — hard errors, not warnings: duplicate `task_name`; `strict_schemas=True` with a task missing either schema; handler signature not `(ctx, payload)`; async handler registered with `profile="cpu"|"gpu"` (compute profiles take sync callables — a coroutine cannot cross a process boundary); `timeout_s >= lease_ttl_s * 10` (suspicious config).
3. Open the `Claim` stream: send `ClaimRequest{worker_id, tags, free_slots, sdk_version, labels}`.
4. Dispatch loop: for each `JobAssignment` -> spawn a supervised job task.

### 10.2 Per-job dispatch pipeline

```
JobAssignment
  -> build Ctx (payload, ctx.output from upstream[], idempotency keys,
     checkpoint_json preloaded, bound logger)
  -> middleware.on_claim(ctx)
  -> input_schema.model_validate(payload)        # if declared -> PayloadValidationError = Fail(fatal)
  -> executor by profile (10.4) + heartbeat shell (10.5)
  -> handler returns:
       dict/BaseModel  -> output_schema validate -> Complete(result)
       ctx.stop_chain(r) -> Complete(result=r, drop_chain_tail=true)
       ctx.skip()        -> Complete(skipped=true)
  -> handler raises:
       SymbaError        -> Fail(retryable=exc.retryable)
       anything else     -> retry_classify (8.2) -> Fail(retryable=...)
  -> middleware.on_complete / on_fail
  -> free_slots += 1 -> stream update
```

`Complete`/`Fail` RPC failures (engine briefly unreachable) are retried with tenacity (5 attempts, exp backoff, max 10s). If still failing, the SDK logs ERROR and drops — the lease will expire and the engine will re-run the job; at-least-once absorbs it. `StaleLease` responses are logged WARNING and swallowed (the retry attempt won; this attempt's result is discarded by design).

Slot-accounting rules (the single most bug-prone area in every reference worker — a bare semaphore can deadlock slot accounting on abort paths, and a misplaced one can end up serializing everything through it):
- The SDK keeps job tasks in a **strong-reference set** (Python GC can silently cancel unreferenced asyncio tasks — a well-documented Python asyncio gotcha that reference implementations guard against explicitly).
- `free_slots += 1` and set-removal happen in the task's **done-callback only** — one unconditional release point, never inline in success/failure/abort branches (a well-known failure mode is leaking slots on the expired-job path when release isn't unconditional).
- A claimed assignment the SDK *abandons* before spawning (validation failure, shutdown race) must release its slot through the same path — count claims you abandon.
- Graceful drain: SIGTERM sets `accepting = False` (stream announces `free_slots=0`) and waits for the task set to empty (bounded by `shutdown_drain_s`); the done-callback bookkeeping runs **independently of the accepting flag** — a well-known stuck-terminating-worker bug class is exactly a drain loop whose completion bookkeeping was gated behind "am I picking jobs". Jobs still running at the deadline are abandoned; lease expiry re-runs them (crash-only, Section 2.2).

### 10.3 Ctx implementation

```python
class Ctx:
    # identity (read-only)
    job_id: str; ctx_id: str; task_name: str; attempt: int; tenant: str
    pipeline: str | None; stage: str | None; group_key: str | None

    # data
    payload: dict | BaseModel          # validated instance when input_schema declared
    output: UpstreamOutputs            # mapping-like; see below
    event_payload: dict | None         # signal payload after wait_for_event resume

    # generated idempotency key
    idempotency_key: str               # sha256(f"{tenant}:{dedup_key or job_id}")[:32]
    idempotency_key_attempt: str       # f"{idempotency_key}-a{attempt}"

    # verbs
    async def checkpoint(self, data: dict) -> None
    checkpoint_data: dict | None       # preloaded from JobAssignment
    async def heartbeat(self) -> None  # manual lease extension (usually automatic)
    async def wait_for_event(self, key: str, timeout_s: int) -> dict | None   # io profile only
    async def submit(self, **spec) -> JobHandle    # ctx_id/pipeline/tenant inherited
    async def submit_children(self, children: list[dict], on_complete: dict) -> Gate
    def stop_chain(self, result: dict | None = None) -> StopChain
    def skip(self) -> Skip

    logger: BoundLogger                # pre-bound job_id/ctx_id/task_name/attempt

class UpstreamOutputs(Mapping):
    def __getitem__(self, key: str):
        # exact alias match wins; then unique task_name match;
        # multiple task_name matches without alias -> AmbiguousResultKey
        # deserialized into producer's output_schema type when known to this worker
        #
        # Two-tier (rev 3): inline tier (immediate predecessor + depends_on)
        # is a local dict hit. A miss on a DEEPER ancestor issues GetResult(job_id,
        # lease_token, task_name) synchronously and memoizes for this execution.
        # In cpu/gpu profiles the lazy path marshals via the Ctx proxy pipe.
        # Not an ancestor at all -> KeyError (found=false), never a silent None.
```

In `cpu`/`gpu` profiles the handler runs in a subprocess; `Ctx` there is a **proxy**: identity/data fields are plain values pickled across, verbs marshal over the duplex pipe to the parent (which owns the gRPC channel). `wait_for_event` raises `UnsupportedInProfile` in compute profiles — compute tasks compute; coordination belongs in `io` tasks.

### 10.4 Executors

| Profile | Mechanism | Concurrency | Crash containment |
|---|---|---|---|
| `io` | coroutine on the worker event loop | up to `slots` jobs interleaved | exception = job failure only |
| `cpu` | `ProcessPoolExecutor(max_workers=slots)`, fork-server start method | one job per process | pool detects broken process -> Fail(retryable) + pool self-heals |
| `gpu` | one long-lived warm subprocess (model loaded once at worker boot via registered `@worker.on_gpu_init` hook); jobs over a duplex pipe | serialized per subprocess (slots small: 1-2) | subprocess crash -> Fail(retryable), respawn + re-init |

Event-loop lag watchdog (io): a 100ms ticker measures drift; sustained lag > 250ms logs WARNING naming the currently running tasks — the "this handler should be profile=cpu" signal.

### 10.5 Heartbeat and timeout shell

Per running job: a parent-side asyncio task fires `Heartbeat(job_id, lease_token)` every `heartbeat_interval_s` (default 15s, engine extends the lease each time). The response carries `cancelled` — cooperative cancellation: io handlers get `asyncio.CancelledError` injected; cpu/gpu subprocesses get SIGTERM then SIGKILL after 5s. The same shell enforces `timeout_s`: on expiry -> cancel the handler the same way -> `Fail(retryable=true, error_type="JobTimeout")`. Worker-side enforcement is primary; the engine's lease expiry is the backstop for a fully dead worker.

Transport keepalive (channel options, mirrors engine 9.1): `keepalive_time_ms=10000`, `keepalive_timeout_ms=5000`, `max_pings_without_data=0`, reconnect with exponential backoff from 200ms capped at 30s, infinite — a worker that lost its engine keeps trying forever and re-announces on reconnect (running jobs continue and their Complete is retried per 10.2).

### 10.6 Middleware

```python
class WorkerMiddleware(Protocol):
    async def on_claim(self, ctx: Ctx) -> None: ...
    async def on_complete(self, ctx: Ctx, result: dict, duration_ms: float) -> None: ...
    async def on_fail(self, ctx: Ctx, exc: BaseException, retryable: bool) -> None: ...
```

Same philosophy as the LLM-call middleware used elsewhere in client applications: builtin `LoggingMiddleware` (always on) and `MetricsMiddleware` (Prometheus per-task counters/histograms, opt-in); middleware must not raise (exceptions logged + suppressed — a metrics bug must never fail a job); no PII middleware in v1 (hook exists).

---

## 11. Client SDK: public API and usage

### 11.1 Public surface (everything importable from `symba`)

```python
from symba import (
    Engine,            # async client
    Worker,            # worker runtime
    Ctx,               # for type hints in handlers
    JobHandle, Gate,   # returned by submit / fan_out
    RetryPolicy,
    RetryableError, FatalError, RateLimitedError,
    JobFailed, JobCancelled,
)
from symba.testing import SymbaTest
```

### 11.2 Example 1 — minimal: one task, submit, await

```python
# tasks.py
from symba import Worker

worker = Worker(engine="grpc://symba.internal:7233", tags=["general"], slots=16)

@worker.task("send_report")
async def send_report(ctx, payload):
    await mailer.send(payload["email"], report=payload["report_id"],
                      idempotency_key=ctx.idempotency_key)     # retry-safe
    return {"sent": True}

if __name__ == "__main__":
    worker.run()      # blocking; SIGTERM-graceful

# caller (anywhere in the app)
from symba import Engine
engine = Engine("grpc://symba.internal:7233", tenant="acme-docs")

job = await engine.submit(task="send_report",
                          payload={"email": e, "report_id": r},
                          ctx_id=track_id,
                          dedup_key=f"report:{r}:{e}")   # duplicate submits collapse
result = await job.result(timeout=120)                   # raises JobFailed on DEAD
```

### 11.3 Example 2 — typed chain with schemas (a document-parsing stage)

```python
# flows/parse_flow.py
from pydantic import BaseModel
from symba import Worker, Ctx, RetryPolicy

worker = Worker(engine=..., tags=["parse", "gpu"], slots=2, strict_schemas=True)

class DownloadIn(BaseModel):  document_id: str
class DownloadOut(BaseModel): source_path: str; size_bytes: int
class ParseOut(BaseModel):    output_ref: str; total_pages: int; language: str

@worker.task("download_source", profile="io", timeout_s=300,
             input_schema=DownloadIn, output_schema=DownloadOut)
async def download_source(ctx: Ctx, payload: DownloadIn) -> DownloadOut:
    path = await storage.fetch(payload.document_id)
    return DownloadOut(source_path=path, size_bytes=path.stat().st_size)

@worker.task("parse_content", profile="gpu", timeout_s=1500, max_attempts=2,
             input_schema=DownloadIn, output_schema=ParseOut)
def parse_content(ctx: Ctx, payload: DownloadIn) -> ParseOut:      # sync -> subprocess
    src: DownloadOut = ctx.output["download_source"]               # typed via schema binding
    parsed = parser.parse(src.source_path)
    ref = stage_output(payload.document_id, parsed)                # big text -> MinIO ref
    return ParseOut(output_ref=ref, total_pages=parsed.pages, language=parsed.lang)

@worker.task("persist_parsed", profile="io", timeout_s=300)
async def persist_parsed(ctx: Ctx, payload):
    parsed: ParseOut = ctx.output["parse_content"]
    await service.persist_parse_metadata(payload["document_id"], parsed)
    await service.set_document_status(payload["document_id"], "PARSED")
    return {"output_ref": parsed.output_ref}

@worker.task("mark_stage_failed")                                  # on_failure hook
async def mark_stage_failed(ctx: Ctx, payload):
    await service.mark_stage_failed(payload["document_id"],
                                    stage=payload["stage"],
                                    reason=payload["failed"]["error"])

# --- flow entry (what the app calls; the only place the chain is written) ---
async def start_parse(engine, doc_id: str, ctx_id: str) -> JobHandle:
    return await engine.submit(
        task="download_source",
        chain=["parse_content", "persist_parsed"],
        payload={"document_id": doc_id},
        pipeline="ingestion", stage="parse",
        group_key=doc_id,
        dedup_key=f"parse:{doc_id}",
        ctx_id=ctx_id,
        on_failure={"task": "mark_stage_failed",
                    "payload": {"document_id": doc_id, "stage": "parse"}},
        retry=RetryPolicy(max_attempts=3, backoff_base_s=5),
    )
```

### 11.4 Example 3 — fan-out with gate (per-chunk summarization)

```python
async def start_summarize(engine, doc_id: str, chunk_ids: list[str], ctx_id: str):
    children, gate = await engine.fan_out(
        children=[{"task": "summarize_chunk",
                   "payload": {"chunk_id": c},
                   "rate_class": "llm.azure.gpt5",     # engine-enforced budget
                   "group_key": doc_id}
                  for c in chunk_ids],
        on_complete={"task": "generate_executive_summary",   # fires when ALL succeed
                     "payload": {"document_id": doc_id},
                     "chain": ["apply_summaries"]},
        gate_policy="all_success",     # or all_terminal / quorum(0.9)
        ctx_id=ctx_id,
    )
    return gate
```

500 chunks = 500 independent, retryable, rate-limited jobs claimed by every free llm-tagged worker; the gate submits the executive summary exactly once.

### 11.5 Example 4 — static join with depends_on and aliases

```python
dense  = await engine.submit(task="embed_dense",  payload=p, runs_on=["gpu"],  ctx_id=cid)
sparse = await engine.submit(task="embed_sparse", payload=p, runs_on=["cpu"],  ctx_id=cid)
store  = await engine.submit(task="store_vectors", payload=p, runs_on=["store"],
                             depends_on={"dense": dense.id, "sparse": sparse.id},
                             ctx_id=cid)
# In store_vectors: ctx.output["dense"], ctx.output["sparse"] (aliases win over task names)
```

### 11.6 Example 5 — caller-side branching + durable agentic loop

```python
async def start_extraction(engine, doc_id: str, ctx_id: str):
    classify = await engine.submit(task="classify_doc", payload={"document_id": doc_id}, ctx_id=ctx_id)
    result = await classify.result()
    next_task = ("extract_contract_fields" if result["category"] == "contract"
                 else "extract_generic_fields")
    await engine.submit(task=next_task, chain=["grade_extraction"],
                        payload={"document_id": doc_id, "round": 1}, ctx_id=ctx_id)

@worker.task("grade_extraction", profile="io")
async def grade_extraction(ctx: Ctx, payload):
    grade = await llm_judge(ctx.output["extract_generic_fields"])
    if grade.confidence >= 0.9 or payload["round"] >= 3:
        return {"final": True, "confidence": grade.confidence}
    await ctx.submit(task="extract_generic_fields", chain=["grade_extraction"],
                     payload={**payload, "round": payload["round"] + 1})
    return ctx.stop_chain(result={"final": False, "retry_round": payload["round"] + 1})
```

### 11.7 Example 6 — human approval + serialized processing + checkpoint

```python
# Re-entry contract: wait_for_event is NOT in-place suspension. The slot is
# released; resume re-queues the job and the handler RE-RUNS FROM THE TOP (possibly
# on another worker). Hence: checkpoint everything expensive BEFORE the wait; on
# re-entry the same wait key returns the consumed signal payload immediately (no
# re-park); the SDK warns if a resumed execution reaches a wait with no checkpoint.
@worker.task("apply_crm_update", profile="io", max_concurrent_per_group=1)   # per-group concurrency ceiling
async def apply_crm_update(ctx: Ctx, payload):
    if ctx.checkpoint_data is None:                        # don't re-buy the LLM call
        draft = await llm_draft_update(payload)            # expensive
        await ctx.checkpoint({"draft": draft})
    draft = (ctx.checkpoint_data or {}).get("draft")

    if payload["amount"] > 10_000:                         # wait/signal coordination for human approval
        approval = await ctx.wait_for_event(f"approve:{payload['ticket_id']}",
                                            timeout_s=3 * 86400)
        if not (approval and approval.get("approved")):
            return ctx.stop_chain(result={"applied": False, "reason": "not approved"})

    await crm.apply(draft, idempotency_key=ctx.idempotency_key)   # generated idempotency key
    return {"applied": True}

# app-side approval endpoint:
await engine.signal(f"approve:{ticket_id}", {"approved": True}, signaled_by=user.email)
```

### 11.8 Example 7 — query, DLQ triage, replay (ops surface as code)

```python
# everything about one document, one lookup:
jobs = await engine.query(ctx_id=track_id)

# failed summarizations in the last hour, grouped triage:
dead = await engine.query(state="dead", task_name="summarize_chunk",
                          created_after=now - timedelta(hours=1))
for j in dead:
    print(j.id, j.last_error, j.spec.payload)              # a row, not a log line
await engine.resubmit_many([j.id for j in dead])           # DLQ replay, fresh attempts

# live tail of a pipeline:
async for event in engine.stream_events(ctx_id=track_id):
    print(event.job_id, event.event, event.at)
```

### 11.9 Sync facade and CLI

```python
from symba import Engine
engine = Engine("grpc://...", tenant="acme-docs").sync    # thread-owned loop wrapper
job = engine.submit(task="reindex_document", payload={...})  # for scripts/legacy sync code
```

```bash
symba submit --task send_report --payload '{"report_id": "..."}' --ctx-id abc
symba query --ctx-id abc --state dead
symba signal approve:T-123 '{"approved": true}' --by ops@syntel
symba worker run flows.parse_flow                            # imports + runs the module's worker
```

---

## 12. Non-functional requirements: how each is achieved

| NFR | Target | Mechanism | Verified by |
|---|---|---|---|
| Throughput | 10k-job burst drains without engine saturation; sustained 500 claims/s per engine instance | partial-index claim scan (5.1) over a hot table kept tiny by archive-on-terminal (5.0); batch claiming (`LIMIT free_slots`); orjson; two-pool isolation | k6 load suite, CI-nightly, regression gate at -20% |
| Latency | p95 queue->claim < 150ms with idle workers | 10–50ms adaptive dispatcher tick (6.2) + single-transaction claim; heartbeats on the hot pool never queue behind UI queries | histogram `symba_ready_to_claim_ms`, load suite asserts |
| Durability | zero acknowledged-then-lost jobs, crash anywhere | every transition is one PG transaction; no state lives only in Redis; WAL is the truth | chaos suite: kill -9 engine/worker at every lifecycle point, assert convergence |
| At-least-once | no job silently vanishes; duplicates bounded by lease TTL | leases + sweeper reclaim; stale-lease rejection; `ux_jobs_dedup` | property tests: random kill schedules, invariant "every job terminal, exactly one SUCCEEDED effect per dedup_key" |
| Scale-out | engines and workers scale horizontally, no coordination config | SKIP LOCKED claims; advisory-locked sweeper/cron election; stateless matcher rebuilt from streams | integration test with 3 engines + 50 workers |
| Footprint | one container + PG (+optional Redis) minimum | single-process subsystems; built-in migrations; embedded UI | compose file IS the test env |
| Degraded mode | Redis loss = latency, never correctness | PG token-bucket fallback; checkpoint PG write-behind; dispatch never depended on Redis | chaos: kill Redis mid-run, assert completion + WARNING logs |
| Observability | every job reconstructable after the fact | `job_events` immutable ledger, `ctx_id` on everything, stack_hash grouping | audit test: run flow, assert event sequence complete |
| Security | no anonymous submit/claim in prod | token (JWKS or shared-secret) / mTLS on both planes; tenant scoping on every query; `mode="none"` refuses non-loopback binds | authZ test matrix |
| Compatibility | SDK N supports engine protocol N-1..N | buf breaking-change CI gate; version handshake with explicit reject | cross-version e2e job in CI (SDK@prev vs engine@main) |

---

## 13. Web UI implementation

SPA served by the engine at `/`; API at `/v1/*`; live updates over one SSE channel (`/v1/events/stream?filters...`) fed by a fan-out of `job_events` inserts (in-process pubsub; multi-engine UIs are eventually-consistent within a second via PG). TypeScript API client generated from the engine's OpenAPI schema in CI — the UI cannot drift from the API silently.

| View (F20) | Backing endpoint(s) | Implementation notes |
|---|---|---|
| Live board | `GET /v1/stats/board` | single aggregate over `ix_jobs_state_task`; SSE-refreshed |
| Pipeline per ctx_id | `GET /v1/jobs?ctx_id=` + `GET /v1/jobs/{id}/tree` | React Flow DAG: chain edges (on_success lineage), dep edges, gate nodes; node color = state; WAITING nodes show wait_key + age |
| Failed / DLQ | `GET /v1/jobs?state=dead` grouped by `stack_hash` | one row per distinct failure, count badge; per-row expand -> error_history diff; bulk Resubmit |
| Waiting | `GET /v1/jobs?state=waiting` | Signal button opens a JSON payload form -> `POST /v1/signals`; `signaled_by` = authenticated user |
| Job detail | `GET /v1/jobs/{id}` + `/events` + `/checkpoints` | full timeline, upstream/downstream links, payload/result viewers (JSON, collapsed by default) |
| Fleet | `GET /v1/workers` | tags, labels, slots in use, last_seen, current jobs; stale workers flagged |
| Queues | `GET /v1/stats/queues` | depth + oldest-age per (task, runs_on, rate_class); bucket fill gauges |
| Cron | `GET/PUT /v1/cron` | enable/disable, last/next fire |

Rule: the UI has zero private endpoints — everything it renders and every button it offers is the public REST API. Anything an operator can click, they can script.

---

## 14. Observability

### 14.1 Metrics (prometheus-client, `/metrics` on the HTTP port)

```
symba_jobs_total{tenant,task,state}                    counter
symba_job_duration_seconds{task}                       histogram (exec time)
symba_job_queue_wait_seconds{task}                     histogram (queued -> claimed)
symba_ready_to_claim_ms                                histogram (latency-NFR health signal)
symba_dispatch_pass_seconds                            histogram (dispatcher tick cost)
symba_dispatch_tick_ms                                 gauge (current adaptive tick)
symba_queue_depth{task,state}                          gauge (sweeper-refreshed)
symba_queue_oldest_age_seconds{task}                   gauge
symba_rate_bucket_tokens{rate_class}                   gauge
symba_gate_age_seconds{policy}                         gauge (unfired gates)
symba_waiting_jobs{tenant}                             gauge
symba_lease_reclaims_total / symba_wait_timeouts_total counter
symba_claim_query_duration_seconds                     histogram
symba_pool_acquire_wait_seconds{pool}                  histogram (saturation early-warning)
symba_get_result_total{source}                         counter (lazy-tier hit rate; hot|archive)
symba_pg_oldest_xact_age_seconds                       gauge (MVCC horizon — see alert below)
symba_archive_moved_total{final_state}                 counter (hot-table evacuation health)
symba_loop_errors_total{loop}                          counter (dispatcher|sweeper|cron pass failures — Section 6.2 containment; alert on rate)
```

Worker-side (`MetricsMiddleware`, exposed on an optional local port): `symba_worker_jobs_total{task,outcome}`, `symba_worker_job_duration_seconds{task}`, `symba_worker_slots_busy`, `symba_worker_event_loop_lag_ms`.

Alert starters (shipped as example Prometheus rules in the repo): queue oldest-age > 10m; DEAD rate > 1% over 15m; ready-to-claim p95 > 500ms; bucket empty > 5m; unfired gate > 1h; **pool acquire wait p95 > 100ms** (resize pools before it becomes latency); **`symba_pg_oldest_xact_age_seconds` > 300** — the leading indicator of claim-path MVCC collapse (a stuck transaction anywhere on the instance pins the vacuum horizon; page on this, don't wait for latency to degrade).

### 14.2 Tracing

OpenTelemetry, OTLP exporter, off unless `otlp_endpoint` set. Span model: `submit` (client) -> link -> `claim`/`execute` (worker, one span per attempt) -> `complete` (engine). `traceparent` rides in job metadata; `ctx_id` is a span attribute on every span — the join key to app-side traces (including the app's own LLM traces; the engine records nothing LLM-specific, per the boundary decision).

### 14.3 The audit ledger is the primary debugging tool

`job_events` is not telemetry, it is the product (F21): who submitted, when queued, which worker claimed, every heartbeat gap, every retry with error + stack_hash, who signaled, what payload resumed a wait. The UI job-detail timeline renders it verbatim. Retention 90 days by partition drop; DEAD jobs' events are exempt from pruning while the job row exists.

---

## 15. Testing strategy

### 15.1 Layers

| Layer | Scope | Infra | Runs |
|---|---|---|---|
| L1 unit | `core/` pure logic: states, retry math, chain advance, gates, readiness, idempotency keys | none | every commit, seconds |
| L2 SQL integration | every query in `db/queries/` against a real migrated PG | testcontainers `postgres:18` | every commit |
| L3 property | lifecycle + DAG invariants under randomized histories | testcontainers | every commit (bounded examples), nightly (deep) |
| L4 e2e | real engine + real SDK workers over gRPC, full flows | docker compose | every commit |
| L5 chaos | kill -9 engine/worker/Redis at randomized points | compose + fault script | nightly |
| L6 load | claim throughput, ready-to-claim latency | k6 + compose | nightly, regression-gated |
| SDK unit | Ctx, executors, classification, schema glue, `SymbaTest` | none | every SDK commit |
| SDK e2e | published SDK against engine@main AND engine@previous-tag | compose | every SDK commit (compatibility NFR) |
| Conformance (rev 3) | L3 property suites + GAP tests (15.6) run against BOTH the real engine and `SymbaTest` — the in-memory fake must exhibit identical semantics (incl. lazy `GetResult` and wait re-entry) or the suite fails | testcontainers / none | every commit in both repos |

### 15.2 L2 example — the claim query's contract

```python
async def test_claim_respects_group_ceiling(pg_pool):
    await seed_jobs(pg_pool, [
        job(task="webhook", group_key="cust-1", max_concurrent_per_group=1),
        job(task="webhook", group_key="cust-1", max_concurrent_per_group=1),
        job(task="webhook", group_key="cust-2", max_concurrent_per_group=1),
    ])
    claimed = await run_claim(pg_pool, tags=["general"], limit=10, worker="w1")
    assert {j["group_key"] for j in claimed} == {"cust-1", "cust-2"}
    assert len(claimed) == 2                     # second cust-1 job held back

async def test_concurrent_claims_never_double_assign(pg_pool):
    await seed_jobs(pg_pool, [job() for _ in range(200)])
    results = await asyncio.gather(*[
        run_claim(pg_pool, tags=["general"], limit=50, worker=f"w{i}") for i in range(8)
    ])
    ids = [j["id"] for batch in results for j in batch]
    assert len(ids) == len(set(ids))             # SKIP LOCKED does its job
```

### 15.3 L3 property test — the core invariants (hypothesis)

```python
@given(dag=random_dags(max_jobs=30), schedule=random_event_schedules())
async def test_lifecycle_invariants(pg_pool, dag, schedule):
    """Random DAG + random interleaving of claims/completes/fails/kills/sweeps."""
    await execute_schedule(pg_pool, dag, schedule)
    await run_sweeper_until_fixpoint(pg_pool)
    jobs = await fetch_all(pg_pool)
    for j in jobs:
        assert j.state in TERMINAL or j.reachable_from_live_worker
        if j.state == "succeeded" and j.chain_tail:
            assert continuation_exists(jobs, j)                    # chain-tail continuation
        if j.depends_on:
            assert all(dep.state == "succeeded" for dep in deps(j)) or j.state == "cancelled"
    for g in await fetch_gates(pg_pool):
        assert g.fired_count <= 1                                  # gate fires at most once
    assert event_ledger_is_gapless(await fetch_events(pg_pool))    # observability NFR
```

### 15.4 L5 chaos scenarios (each asserts convergence + no lost/duplicated effects)

1. kill -9 engine between claim and assignment push -> lease expires -> re-queued.
2. kill -9 worker mid-handler -> reclaim -> retry -> effect-once via handler idempotency_key (test double records keys).
3. Redis down mid-run -> PG fallback engages, throughput drops, zero failures (degraded-mode NFR).
4. Signal fired 1ms before/after Wait RPC -> both orders resume exactly once (5.7 race).
5. Duplicate Complete from a stale lease -> rejected, ledger shows both attempts.
6. PG failover (container restart) -> engine reconnects, in-flight transactions rolled back cleanly, resumes.

### 15.5 SDK testing utility (ships in the package)

```python
from symba.testing import SymbaTest

async def test_parse_flow():
    async with SymbaTest() as sim:                       # in-memory engine, no infra
        sim.register(worker)                             # the real Worker object
        job = await sim.submit(task="download_source",
                               chain=["parse_content", "persist_parsed"],
                               payload={"document_id": "d1"})
        result = await job.result()
        assert sim.job_for("persist_parsed").state == "succeeded"
        sim.assert_chain_executed(["download_source", "parse_content", "persist_parsed"])
```

`SymbaTest` implements the same lifecycle semantics as the engine — **enforced, not aspirational** (rev 3): the conformance suite (L3 property tests + all of 15.6) runs against BOTH the real engine and `SymbaTest` in both repos' CI. That includes the sharp edges: lazy `GetResult` resolution in `ctx.output`, the re-entry contract (handler re-runs from top; same-key wait returns the consumed payload), group ceilings, and stale-lease rejection. A green `SymbaTest` run that would fail against the real engine is a conformance bug, release-blocking. This is what a client application's own unit tests will use — no compose needed to test a flow.

### 15.6 Gap-closing tests (rev 3 — each pins a reviewed failure mode)

| # | Test | Failure mode it pins | Layer |
|---|---|---|---|
| 1 | Archive-move atomicity: kill -9 the engine between the `DELETE FROM jobs` CTE and commit in 5.4 — assert the job is either fully live or fully archived, never absent from both, and `group_running` matches reality after sweeper reconcile | terminal-move crash leaving a ghost/lost job | L5 |
| 2 | `group_running` drift: randomized claim/complete/fail/reclaim interleavings, then force-kill mid-transaction; assert sweeper reconciliation restores exact counts and no group ever exceeds its cap *persistently* | counter drift silently throttling or over-admitting a group | L3 |
| 3 | Fairness under monopoly: seed 5,000 jobs in one `group_key` + 10 in another at equal priority; assert the small group's jobs all claim within N batches (window-function cap in 5.3 working) | starvation behind a giant fan-out | L2 |
| 4 | Lazy `GetResult` correctness: chain of 10 tasks; task 10 reads task 1's result via `ctx.output` — assert one RPC, memoized on second access, `KeyError` for a non-ancestor, and identical behavior in `SymbaTest` | deep-ancestor fetch wrong/unscoped; fake drift | L4 + conformance |
| 5 | Re-entry contract: handler checkpoints, waits, is resumed on a *different* worker; assert pre-wait code re-ran, checkpoint prevented recompute (spy counter), same-key wait returned payload without re-parking, `WaitKeyAlreadyConsumed` on key reuse | un-checkpointed pre-wait work re-executing expensive effects | L4 + conformance |
| 6 | Dispatcher latency floor: idle system, submit one job, assert claim within `max_tick_ms` + margin; busy system, assert p95 ready-to-claim < 150ms at 500 claims/s (no LISTEN/NOTIFY) | polling tick silently degrading the latency requirement | L6 |
| 7 | Inline-tier cap: submit a chain whose predecessor+deps results would exceed 256KB — assert submit-time rejection naming the producers; assert a job whose deps fit is delivered fully inline (zero GetResult calls, spy on the RPC) | oversized assignments discovered at claim-time instead of submit-time | L2 + L4 |
| 8 | Cancel state matrix (5.8): cancel a job in each of the 7 states (incl. future `run_at` and already-archived); assert the exact per-state outcome, idempotent double-cancel, and dependents cascaded — pins a known class of fall-through regression seen in other job-queue implementations | cancel silently no-oping for a non-running state | L2 + L4 |
| 9 | Loop containment (Section 6.2): inject a poisoned pass (bad row / PG error) into dispatcher, sweeper, and cron; assert the loop survives, `symba_loop_errors_total` increments, and throughput recovers — pins a known periodic-loop-death failure mode | one bad pass silently killing a periodic loop forever | L4 + L5 |
| 10 | Sweeper scope: seed rows for a foreign tenant and non-expired leases; run sweeper passes; assert zero foreign/healthy rows touched — pins a known over-eager-sweep failure mode | over-eager sweep reclaiming healthy or out-of-scope jobs | L2 |

---

## 16. CI/CD, versioning, release engineering

### 16.1 Versioning

- **Engine:** SemVer `vMAJOR.MINOR.PATCH`. Wire protocol version = engine MAJOR.MINOR. DB migrations are append-only; a release may add migrations, never edit shipped ones.
- **SDK:** independent SemVer. Compatibility matrix in both READMEs, enforced by a version handshake: SDK N.x supports engine protocol N-1 and N.
- **Proto:** `buf breaking --against '.git#tag=<last-release>'` gates every PR touching `proto/`.

### 16.2 Engine pipeline (GitHub Actions)

`lint (ruff) -> typecheck (pyright strict on src/) -> L1 -> L2+L3 (testcontainers) -> L4 (compose) -> buf lint+breaking -> EXPLAIN gate (claim plan must not seq-scan jobs) -> docker build (multi-arch amd64/arm64) -> ui build + embed`. Nightly adds L5 chaos + L6 load with a stored baseline; > 20% claim-throughput regression fails the run. Release on tag: image `ghcr.io/.../symba:vX.Y.Z` + migration bundle artifact + changelog.

### 16.3 SDK pipeline

`lint -> typecheck -> unit -> e2e vs engine@main -> e2e vs engine@last-release -> build wheel/sdist -> publish to PyPI on tag`. Stub regeneration is a PR check: `make proto-gen SYMBA_TAG=<engine-tag>` must produce a clean diff.

---

## 17. Deployment

### 17.0 Container + compose spec (rev 3 — build these exactly at M0)

`Dockerfile` (multi-stage; a conventional uv-based build adapted to a leaner runtime):

The engine image is **API-only** — it does NOT embed the SPA. The operator
console is built from `frontend/` and shipped as a separate container (nginx
serving the Vite build; see the `frontend` compose service, added at M5).

```dockerfile
# Stage 1: Python deps (uv binary copied in, cache mount)
FROM python:3.14-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src/ src/
# Migrations are NOT baked into the engine image — the Flyway sidecar mounts
# database/symba/ and applies them before the engine starts.
RUN uv sync --frozen --no-dev

# Stage 2: runtime (no uv, no compilers, non-root)
FROM python:3.14-slim
RUN useradd -r -u 10001 symba
WORKDIR /app
COPY --from=build /app/.venv .venv
COPY --from=build /app/src src
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app/src PYTHONUNBUFFERED=1
USER symba
EXPOSE 7233 7300
HEALTHCHECK CMD ["python", "-m", "symba.healthcheck"]   # hits grpc health svc
ENTRYPOINT ["python", "-m", "symba.main"]
```

`docker-compose.yml` (local dev + the L4/L5/L6 test env — the compose file IS the test env, keeping the footprint to one container plus database):

```yaml
services:
  postgres:
    image: postgres:18.1                  # PG 18 floor: uuidv7() (5.1)
    environment: { POSTGRES_USER: symba, POSTGRES_PASSWORD: symba, POSTGRES_DB: symba }
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]
    healthcheck: { test: ["CMD-SHELL", "pg_isready -U symba"], interval: 2s, retries: 15 }

  redis:                                  # optional (degraded-mode fallback exists without it); compose includes it,
    image: redis:8.4                      # chaos suite kills it
    ports: ["6379:6379"]
    healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: 2s, retries: 15 }

  engine:
    build: .
    environment:
      SYMBA_POSTGRES__DSN: postgresql://symba:symba@postgres:5432/symba
      SYMBA_REDIS__URL: redis://redis:6379/0
      SYMBA_AUTH__MODE: none              # dev only; refuses non-loopback in prod builds
      SYMBA_LOG__FORMAT: console
    ports: ["7233:7233", "7300:7300"]
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      # no migration sidecar (unlike a typical Flyway-fronted service):
      # the engine self-migrates at boot under an advisory lock

volumes:
  pgdata:
```

Conventional patterns kept: `depends_on: service_healthy` discipline, uv-in-image, env-prefixed config. Deliberately dropped: a separate Flyway migration service (self-migrating engine instead, Section 1.1), single-stage image (engine ships multi-stage + non-root — it's an open-sourceable product, not an internal service).

### 17.1 Engine

Single container. Boot order inside `main.py`: config -> logging -> pools -> migrations (advisory-locked, so N replicas can start simultaneously) -> subsystems. HA = 2+ replicas behind a TCP LB (gRPC) + HTTP LB; all instances serve all roles by default; sweeper/cron self-elect via advisory locks. **LB requirements for the Claim streams (rev 3):** L4/TCP passthrough (or an L7 proxy with gRPC support and its stream idle timeout raised above `grpc_keepalive_time_ms`); the engine's `max_connection_age` recycling (9.1) means streams churn by design and reconnect storms are a tested path, not an incident surprise. PG sizing guidance: the hot `jobs` table stays in the thousands-of-rows range by construction (archive-on-terminal, 5.0) — the partial indexes stay hot in cache; `jobs_archive` + `job_events` dominate disk — hence partitioning + drop-based retention.

### 17.2 Workers (outbound-only dialers)

```python
# spark-01 (GPU parse box)          # llm-worker (any cloud VM)        # store-worker
Worker(tags=["parse","gpu"],        Worker(tags=["llm"],               Worker(tags=["store"],
       slots=2, ...)                       slots=64, ...)                     slots=16, ...)
```

No inbound ports, no service discovery, no engine config listing workers — a new box with a token and an engine URL joins the fleet on first Claim. This is the whole heterogeneous-hardware story: the four NVIDIA Spark parse nodes run the same script with different env.

### 17.3 Client application integration (strangler, per the migration plan in the design doc)

A client application adds `symba-sdk` alongside its existing workflow-orchestrator client library; flows move stage-by-stage (parse first — it exercises gpu profile, chains, on_failure; then summarize — fan-out + rate classes; then the rest). The app-side pipeline spine stays in place; the scheduler submits Symba flows instead of launching workflows on the old orchestrator, with `ctx_id = document_id`. The old orchestrator is removed only when the last of its workflow definitions is deleted.

---

## 18. Build order (milestones)

| # | Name | Delivers | Exit criteria |
|---|---|---|---|
| M0 | Skeleton | repos, proto, CI, migrations runner, config, logging | compose up; health green; L1/L2 harness runs |
| M1 | Single-job lifecycle | submit/claim/complete/fail, leases, retries, sweeper, dedup | chaos scenarios 1-2 pass; claim benchmark baseline stored |
| M2 | Flow primitives | chains, depends_on + ctx.output, gates/fan-out, stop_chain/skip/on_failure | L3 property suite green on random DAGs |
| M3 | Heterogeneity + budgets | runs_on routing, rate classes (Redis + PG fallback), priorities, group fairness + per-group concurrency caps | multi-worker e2e; Redis-kill chaos green |
| M4 | SDK polish | schemas/strict mode, profiles + executors, checkpoints, SymbaTest, sync facade, CLI | a client parse flow runs end-to-end on Symba in staging |
| M5 | Coordination | WAITING/signals, cron, cancel trees, resubmit/DLQ replay | scenario 4 race test green; ops runbook drafted |
| M6 | UI | all eight views, SSE live updates, signal/resubmit actions | triage-a-failure drill done by someone who didn't build it |
| M7 | Hardening | authn/z, tenant caps, load suite in CI, docs site, compat matrix | v1.0.0 tag; a client summarize stage migrated |

Sequencing rationale: correctness of the single-job machine (M1) before any flow sugar; flow semantics (M2) before performance work (M3) because fairness/caps change the claim query; the SDK gets deep polish (M4) only once the wire contract has survived M2/M3; UI (M6) late because it renders APIs that must exist first — but before 1.0 because the UI is a launch requirement, not an add-on.










