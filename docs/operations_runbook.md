# Symba Operations Runbook

Operator-facing playbook for running the Symba job engine in production. Pairs with
`symba_implementation.md` Section 14 (observability) and Section 6.2 (loop containment). Every alert
below maps to a shipped Prometheus rule and a concrete first action.

> Golden rule: Symba is crash-only (Section 6.1). For most failures the right move is *let it
> restart* — the engine recovers state from Postgres on boot. The exceptions are the
> three periodic loops (dispatcher/sweeper/cron), which are contained (Section 6.2): a bad pass
> is logged + counted, never a process death. So a rising `symba_loop_errors_total` is a
> *silent* degradation — it will NOT page you via a crash loop. Watch the metric.

---

## 1. Health & readiness

| Probe | Endpoint | Meaning |
| --- | --- | --- |
| Liveness | `GET /healthz` (HTTP) | process is up; always 200 once booted |
| Readiness | `GET /readyz` (HTTP) | hot pool acquirable + migrations current; gate for traffic |
| gRPC | `grpc.health.v1.Health/Check` | data-plane serving |

`readyz` failing on boot ⇒ almost always migrations behind or Postgres unreachable.
Check the Flyway sidecar (`docker compose logs flyway`) and `schema_migrations`.

---

## 2. Alert → cause → action

Alert rules ship as example Prometheus rules in the repo. Ordered by "page vs. ticket".

### 2.1 `symba_pg_oldest_xact_age_seconds > 300` — PAGE

Leading indicator of **claim-path MVCC collapse**. A stuck transaction *anywhere* on the
instance pins the vacuum horizon; the partial indexes that keep `claim.sql` fast stop
protecting you and claim latency will cliff.

1. Find the culprit: `SELECT pid, age(backend_xmin), state, query FROM pg_stat_activity ORDER BY age(backend_xmin) DESC NULLS LAST LIMIT 10;`
2. Usually an idle-in-transaction analytics/BI session or a leaked connection.
3. `SELECT pg_terminate_backend(<pid>);` — do NOT wait for latency to degrade first.
4. Follow up: enforce `idle_in_transaction_session_timeout` on the offending role.

### 2.2 `symba_loop_errors_total{loop}` rate rising — PAGE if persistent

A periodic loop is failing every pass. It is contained (won't crash), so throughput for
that loop's job silently stops.

- `loop="dispatcher"` → no jobs being assigned to workers. Check hot pool + matcher logs.
- `loop="sweeper"` → leases not reclaimed, waits not expiring, counters drifting. Check
  the general pool and the advisory-lock election (only one engine sweeps at a time).
- `loop="cron"` → schedules not firing. Check `cron_schedules` for a poisoned `cron_expr`
  (one bad row is skipped, but a systemic PG error stalls all firing).

Action: read the ERROR logs for that `loop`, fix the root cause (bad row / PG blip /
pool exhaustion). A transient blip self-heals — the metric returns to flat.

### 2.3 `pool_acquire_wait p95 > 100ms` — TICKET (resize before it bites)

Pool saturation early-warning, before it shows up as user latency. Bump `pool_size` for
the saturated pool (`{pool}` label: `hot` | `general`) and confirm Postgres
`max_connections` has headroom.

### 2.4 queue oldest-age > 10m — TICKET

Jobs sitting unclaimed. Either no workers with the required `runs_on` tags are connected,
a `rate_class` bucket is exhausted, or a group hit its concurrency ceiling. Check
`symba_waiting_jobs`, `symba_rate_bucket_tokens{rate_class}`, connected worker tags.

### 2.5 DEAD rate > 1% over 15m — TICKET

Handlers are failing fatally. Triage the DLQ (Section 4) — the `stack_hash` grouping tells you
if it's one bug or many.

### 2.6 unfired gate > 1h — TICKET

A fan-out gate never fired (a child is stuck or the policy can't be met). Inspect the
parent job's children in the audit ledger.

### 2.7 bucket empty > 5m — TICKET

A `rate_class` is starved. Either upstream is over-submitting or the class capacity is
mis-sized. Edit the class at runtime (rate-class admin) or throttle the submitter.

### 2.8 `ready_to_claim` p95 > 500ms — TICKET (N2 latency floor slipping)

`symba_ready_to_claim_ms` is the queue→claim interval (DB-clock: `run_at`→`started_at`).
The N2 target is p95 < 150ms; the alert fires at 500ms — the dispatcher's adaptive tick
is not keeping up. Usual causes: hot-pool saturation (see 2.3), a claim-path plan
regression (the EXPLAIN gate should catch this in CI), or MVCC bloat (see 2.1). Confirm
the histogram is populated at all — a metric stuck at `+Inf` means a bucket-scale bug,
not real latency (`ready_to_claim_ms` carries explicit millisecond buckets for exactly
this reason).

> Note: `symba_ready_to_claim_ms` measures the interval only for jobs that *do* get
> claimed. An unattended queue (no workers for a task's `runs_on`) shows up in 2.4
> (queue oldest-age), not here.

---

## 3. Redis loss (degraded mode, N7)

Redis is a **latency optimization, never a correctness dependency**. If Redis
dies:

- Rate limiting transparently falls back to the Postgres token bucket (higher latency,
  same semantics).
- Checkpoints fall back to Postgres read/write (the PG table is the system of record;
  Redis is only a fast-path cache).
- You'll see a single WARNING log per outage ("Redis unavailable; using Postgres only"),
  not one per call.

**No operator action is required for correctness.** Restore Redis to recover latency.
Do NOT treat a Redis outage as a Symba outage.

---

## 4. DLQ triage & replay

A DEAD job is **never auto-pruned** (the DLQ contract — archive partitions containing
unresolved DEAD jobs are skipped by retention). Triage:

1. List: `GET /v1/jobs?state_filter=dead` (UI groups by `stack_hash` — one row per distinct
   failure with a count badge; expand a row for the `error_history` diff).
2. Decide which were *transient* failures worth replaying.
3. Replay: `POST /v1/jobs/{id}/resubmit` per job, or bulk via the admin surface.

A resubmit is **not** an in-place retry: it inserts a *fresh* `queued` row
(`resubmitted_from` = original id, `attempt=0`, `dedup_key` cleared). The original stays
archived as the audit record. Verify the fresh id came back and is `queued`.

---

## 5. Human-in-the-loop signals (WAITING)

A job parks in `WAITING` when its handler calls `wait(wait_key, timeout_s)`. Resume it:

- `POST /v1/signals` with `{tenant, wait_key, payload, signaled_by}`.
- Both orders are race-free: signal-first is remembered and consumed inline when the wait
  arrives; wait-first parks and is woken by the signal (single delivery, Section 15 chaos 4).
- If the timeout elapses first, the sweeper resumes the job with a `wait_timed_out`
  audit event and a null payload — the handler must distinguish signal vs. timeout.

Stuck WAITING jobs: check `symba_waiting_jobs{tenant}` and the audit ledger for the
`wait_key` that was never signaled.

---

## 6. The audit ledger is the primary debugging tool

`job_events` is the product, not telemetry (F21): who submitted, when queued, which
worker claimed, every heartbeat gap, every retry with error + `stack_hash`, who signaled,
what payload resumed a wait. The UI job-detail timeline renders it verbatim. When in
doubt about *what happened to this job*, read its event timeline before reaching for logs.

Retention 90 days by partition drop; a DEAD job's events are exempt from pruning while
the job row exists.

---

## 7. Log levels (Section 14)

| Level | Emitted for |
| --- | --- |
| WARNING | lease reclaimed, wait timed out, retry backoff > 30s, bucket exhausted > 60s, stale-lease Complete rejected |
| ERROR | job → DEAD, gate stuck, migration failure, hot-pool acquire timeout, loop pass failure |

Logs are structured (structlog); filter by `service`, `loop`, `job_id`, `tenant`.
