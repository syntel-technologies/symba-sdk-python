# HTTP API reference

The control plane is HTTP/JSON on `:7300`. This is the operator/integrator summary; the
**machine-readable source of truth is the live OpenAPI** at `GET /openapi.json` (and
Swagger UI at `/docs`), which CI drift-gates against the generated TS client
(`tools/check_openapi.py`). The worker **data plane** is gRPC on `:7233` — see the
protobuf in [`proto/symba/v1/data_plane.proto`](../proto/symba/v1/data_plane.proto) and
the [`examples/raw_grpc_worker/`](../examples/raw_grpc_worker/) sample.

Runnable `curl` samples for every endpoint below live in [`examples/`](../examples/).

## Auth

With `[auth] mode = "none"` (default) loopback callers need no credentials; a routable
deploy must run `token` or `mtls`. In `token` mode send `Authorization: Bearer <token>`.
**The tenant is authoritative from the credential (N9)** — a `tenant` field in a request
body is ignored; you cannot submit into or read another tenant's jobs.

`EventSource` can't set headers, so the SSE endpoint also accepts `?access_token=`.

## Health & metrics (no auth)

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness — 200 once booted |
| GET | `/readyz` | readiness — 200 when hot pool acquirable + migrations current, else 503 |
| GET | `/metrics` | Prometheus exposition (`symba_*`) |

## Jobs — submit & lifecycle

| Method | Path | Body / params | Returns |
|---|---|---|---|
| POST | `/v1/jobs` | `{tenant, specs:[JobSpec,...]}` | `{job_ids, deduplicated}` |
| POST | `/v1/fanout` | `{ctx_id?, children:[JobSpec], on_complete:JobSpec, gate_policy}` | `{child_job_ids, gate_id}` |
| GET | `/v1/jobs/{id}` | — | the job (state, attempt, timestamps, result) |
| POST | `/v1/jobs/{id}/cancel` | `?cascade=bool` | `{cancelled, was_running, cascaded[], note}` |
| POST | `/v1/jobs/{id}/resubmit` | — | `{new_job_id, resubmitted_from}` (DLQ replay) |
| POST | `/v1/signals` | `{wait_key, payload?, signaled_by?}` | `{delivered, job_id}` |

### `JobSpec` (submit / fanout children)

| Field | Default | Notes |
|---|---|---|
| `task_name` | — | required; routes to a worker registered for it |
| `payload` | `{}` | arbitrary JSON, ≤ `limits.max_payload_kb` |
| `priority` | `0` | higher claimed first |
| `group_key` / `max_concurrent_per_group` | `null` | concurrency ceiling |
| `dedup_key` | `null` | idempotent submit |
| `runs_on` | `[]` | tag routing; empty = any untagged worker |
| `rate_class` | `null` | token-bucket class |
| `max_attempts` / `timeout_s` / `lease_ttl_s` | `5` / `600` / `60` | override `[defaults]` |
| `on_success` / `chain_tail` | `null` / `[]` | linked-list chaining |

`gate_policy` ∈ `all_success` \| `all_terminal` \| `quorum(n)`.

## Read / stats surface (powers the UI)

| Method | Path | Params | Returns |
|---|---|---|---|
| GET | `/v1/jobs` | `state_filter?, task_name?, ctx_id?, limit=100, offset=0` | `{jobs:[...]}` |
| GET | `/v1/stats/board` | — | `{counts}` by state |
| GET | `/v1/stats/queues` | — | depth + oldest age per (task, runs_on, rate_class) |
| GET | `/v1/workers` | — | fleet: tags, slots, last_seen |
| GET | `/v1/cron` | — | schedules with last/next fire |
| PUT | `/v1/cron/{id}` | `{enabled}` | toggle a schedule |
| GET | `/v1/jobs/{id}/events` | — | the immutable `job_events` ledger (N8) |
| GET | `/v1/jobs/{id}/tree` | — | DAG edges (chain / dep / gate) |
| GET | `/v1/jobs/{id}/checkpoints` | — | `{job_id, checkpoint}` |
| GET | `/v1/events/stream` | `?access_token=` | SSE `text/event-stream` of live `job_events` |

> `state_filter=dead` is the dead-letter queue; DEAD jobs are never auto-pruned.

## Errors

Errors surface with the engine's taxonomy (`core/errors.py`); the transport maps each to
an HTTP status. Notable mappings:

| Condition | HTTP | gRPC |
|---|---|---|
| tenant queue cap exceeded | 429 (+ `Retry-After`) | `RESOURCE_EXHAUSTED` |
| not found / foreign-tenant id | 404 | `NOT_FOUND` |
| auth missing/invalid | 401 | `UNAUTHENTICATED` |
| protocol version out of range | 426 | `FAILED_PRECONDITION` |
| stale-lease / consumed wait-key | 409 | `FAILED_PRECONDITION` |

The response body is `{status_code, error_code, message, trace_id, context}`.
