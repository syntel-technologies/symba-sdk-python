# Symba — Job Engine Requirements, Decisions, and Design

**Status:** Draft for review (rev 8 — anti-over-engineering pass: prior-art failure modes mined at the design-principle level (Section 5); cron decided as dedup-key upsert, no leader election (Section 13.1); cancel-per-state matrix, loop containment, slot-accounting and one-clock rules folded into the implementation doc. Prior: rev 7 — LISTEN/NOTIFY **removed** in favor of a polling dispatcher (AD-3, per a documented commit-lock outage mode at scale); hot-table evacuation to `jobs_archive` on terminal; `uuidv7()` PKs on Postgres 18; `ctx.output` two-tier delivery (inline predecessor+deps, lazy `GetResult` for deep ancestors, AD-13); explicit `wait_for_event` re-entry contract (AD-23); schema ownership moved to the implementation doc)
**Date:** 2026-07-13
**Companion:** `symba_implementation.md` — the technical implementation blueprint (repo structures, wire contract, hot-path SQL, SDK internals, build order)
**Scope:** Symba is a standalone, generic, potentially open-sourcable job execution engine that replaces coarse workflow-orchestrator task execution for document ingestion pipelines and future workloads (connector sync/re-sync, integrations, evals). Ships as two repositories: `symba` (engine server) and `symba-sdk-python` (AD-18).

---

## Table of Contents

1. [Why we need our own engine](#1-why-we-need-our-own-engine)
2. [The workloads this engine must serve](#2-the-workloads-this-engine-must-serve)
3. [Requirements](#3-requirements)
4. [What exists today and why it is not enough](#4-what-exists-today-and-why-it-is-not-enough)
5. [Design principles learned from prior art in the job-queue space](#5-substrate-evaluation)
6. [Architecture decisions](#6-architecture-decisions)
7. [System design](#7-system-design)
8. [Data model](#8-data-model)
9. [Worker model](#9-worker-model)
10. [Failure model](#10-failure-model)
11. [Usage examples](#11-usage-examples)
12. [Migration strategy (strangler)](#12-migration-strategy)
13. [Open questions](#13-open-questions)
14. [References](#14-references)

---

## 1. Why we need our own engine

### 1.1 The problem in one sentence

We need to run **tens of thousands of heterogeneous work items** (LLM calls, GPU parsing, embedding, connector syncs) with **per-item durability, visibility, and retry**, across **specialized hardware we do not control from the app server** — and nothing on the market gives us that without either massive operational weight (Temporal) or fundamental gaps (every Python task queue).

### 1.2 What actually hurts today

The single scenario that drives everything: **a client onboards with 10,000 documents.** Today that takes days, not hours. Concretely:

- **Sequential per-chunk LLM calls.** Chunk summarization loops chunk-by-chunk in one orchestrator task. A 200-chunk document is 200 serial LLM round-trips inside one task execution. Same story for graph extraction.
- **No per-element state.** If chunk 178 of 200 fails on an LLM rate limit, the whole task fails and retries all 200. We cannot see, retry, or prioritize individual chunks. Batching hides failures: "pieces here and pieces there as part of batches — no real parallelization."
- **A workflow orchestrator is an orchestrator, not an executor.** It tells our workers *what* to run but has no concept of *where*. It cannot express "parsing must run on the box with the GPU accelerators" or "embedding runs on the GPU node, LLM calls run on the cheap API workers." Task-to-worker affinity is a deployment hack (which worker process polls which task type), not an engine feature.
- **The orchestrator's operational surface is oversized for what we use.** We run linear pipelines (`dedup -> parse -> classify -> chunk -> embed -> summarize -> tag -> extract -> graph`) — a full workflow DSL, versioned workflow definitions, its own persistence, its own UI, and a heavyweight server buy us almost nothing over a stage spine we already own (an admission scheduler + a stage poller). We already built the scheduler and the poller *around* the orchestrator because it alone couldn't manage admission and stage progression the way we need.
- **Throughput knobs are coarse.** A fixed worker-thread-count setting and a fixed per-stage concurrency cap are process-level dials. There is no notion of "LLM provider X allows 500 req/min, spread that fairly across all documents currently in flight."

### 1.3 Why not just fix it inside the existing orchestrator

We evaluated the in-orchestrator options and rejected them:

| Option | Why rejected |
|---|---|
| Threadpool inside the task | No per-element durability. A crash loses all in-flight chunks. Rate-limit failures kill whole batches. No visibility. |
| Dynamic fork-join per chunk | 200-branch dynamic forks per document x hundreds of documents overwhelms a lightweight orchestrator deployment. Fork branches still share the same fixed worker-thread pool, so it's fake parallelism. |
| Sub-workflow per chunk | Same as above plus per-workflow bookkeeping overhead. Thousands of workflow executions per document is exactly the write pattern a lightweight orchestrator deployment is worst at. |
| Scale the orchestrator itself (heavier backend, cluster) | We would be investing in operating a heavyweight cluster to keep features we don't use, and it *still* wouldn't give us per-element work items or hardware-aware routing. |

### 1.4 Why not an existing library or platform

Detailed evaluation in [Section 5](#5-substrate-evaluation). Summary:

- **Temporal**: closest philosophically (durable executions, task queues as routing), but it's a heavyweight multi-service platform (server cluster + Cassandra/Postgres + Elastic), its determinism/replay programming model is a poor fit for "call an LLM 200 times," and adopting it means our engine's core value lives in someone else's operational model. Too much machine for a linear pipeline.
- **Existing Python task-queue libraries** (surveyed broadly): each solves a slice — none has capability-based routing, none has fan-out/fan-in ("gate") primitives, none keeps durable queryable per-job history, none is designed for heterogeneous remote compute. They are Python task queues, not job engines.
- **Older sync-first task queues**: same gaps, older designs, sync-first execution model, broker-opaque state.

The conclusion is not "nothing is reusable" — it's that **the engine is ours, and the best ideas from prior art are inputs** (see 5.5).

### 1.5 The latency stack: why the current pipeline is structurally slow

The slowness has **two independent causes**, and both must be attacked:

**Cause 1 — stage execution time (the dominant one).** Real measured pain per document: **graph extraction > 30 minutes, summarization ~ 20 minutes.** Both are sequences of LLM calls executed one after another inside a single task. The math is unforgiving: 200 chunks x ~6 s per LLM call = 20 minutes *serial*, but the same 200 calls at 50-wide parallelism = **24 seconds**. Nothing about the work is inherently slow — only its serialization. The fix is splitting *who does what*:

| Concern | Lives in | Concretely |
|---|---|---|
| WHAT to do per element (prompts, parsing, chunk logic, staging writes) | **the application** (worker handlers) | `summarize_chunk` handler = fetch chunk, call LLM, stage result. App code, app repo. |
| HOW MANY run at once, WHERE, in WHAT ORDER, what happens on failure | **the engine** | 200 chunk jobs claimable simultaneously by any number of llm-workers; rate buckets cap provider pressure; gates fire the next step the instant the last chunk lands. |
| Splitting a document into per-element jobs | **the application** (it knows its domain) | After chunking, the app submits the fan-out — the engine doesn't know what a "chunk" is. |
| Making that split *safe* (per-element retry, dedup, visibility, fan-in) | **the engine** | The reason the app can afford to split: 200 jobs are not 200 things to babysit. |

That division is the whole speed story: **the app stops doing loops; the engine makes wide fan-out safe.** A 30-minute graph extraction becomes: fan out `extract_graph_batch` jobs across all llm-workers (bounded only by the provider rate bucket), gate, then apply. Wall-clock ≈ (slowest single call) x (number of sequential waves the rate limit forces), not (number of calls) x (call latency).

**Cause 2 — stacked polling tiers.** Even when work is ready, the current system waits for timers. A document admitted right after a tick waits through every layer before any work happens:

| Tier | Interval / cap | Config |
|---|---|---|
| Document admission poll | every 60 s, max 10 docs/tick, 5 concurrent workflows | `document_processing.poll_interval_minutes=1`, `batch_size=10`, `max_concurrent_workflows=5` |
| Spine stage poll | every 10 s, max 10 rows/stage/tick | `pipeline_stages.poll_interval_seconds=10`, `batch_size=10` |
| Per-stage concurrency caps | 5-25 per stage | `parse_max_concurrent=5`, `summarize_max_concurrent=10`, ... |
| Orchestrator task polling | worker poll interval + 10 shared threads | `ORCHESTRATOR_WORKER_ALL_THREAD_COUNT=10` |
| Stuck detection | 15-75 min thresholds before rescue | `stuck_threshold_minutes`, `stage_absolute_ceiling_minutes=120` |

Worst case, a single stage transition costs 60 s (admission) + 10 s (spine) + poll latency before the task even starts — **multiplied by 9 stages**. And throughput is hard-capped at ~10 documents per stage per 10-second tick regardless of how much worker capacity exists. Zombie recovery waits *minutes* because state is split across two systems (spine in our DB, execution in the workflow orchestrator) that can only reconcile by polling each other.

**The engine's answer is one fast dispatcher, not stacked timers:** workers hold open claim streams; a single engine-side dispatcher loop polls the ready set on a 10–50 ms adaptive tick and pushes work the moment it exists. One 10–50 ms tick replaces today's 60 s + 10 s + poll-latency stack — three orders of magnitude, and crucially *one* tier instead of five. (Deliberately NOT Postgres LISTEN/NOTIFY: per-transition NOTIFY serializes **all** commits instance-wide via an AccessExclusiveLock at commit time — a documented production outage mode at exactly our target volume; a polling dispatcher avoids that failure mode entirely, which is the design we adopt here.) No admission valve, no per-stage tick caps — backpressure comes from worker slots and rate buckets, not from polling intervals. State lives in one place, so "stuck" detection is a lease expiry (seconds), not a cross-system reconciliation (minutes). This is requirement N2: **job-ready to worker-claimed in < 50 ms p99, sustained at millions of jobs/day.**

### 1.6 The second driver: this is a product capability, not an internal fix

The engine is deliberately **generic and decoupled** from any single client application:

- It must not share a client app's database or schemas. Every application is *a client* of the engine.
- Other workloads are already queued up behind ingestion: connector sync/re-sync (GDrive, SharePoint), integration jobs (Jira/CRM/ERP), scheduled evals, prompt-optimization runs.
- Potential open-source release. That forces clean boundaries now: protobuf wire contract, SDK-based access, no client-app concepts in the core.

---

## 2. The workloads this engine must serve

Design against real workloads, not abstractions. These are the four concrete shapes:

### 2.1 Document ingestion (the driver)

`dedup -> parse -> classify -> chunk -> embed -> summarize -> tag -> extract -> graph`

- **Parse**: CPU/GPU heavy (docling, vLLM-based parsers, OCR). MUST run on dedicated hardware — e.g. the 4 NVIDIA Spark boxes. Minutes per document. This is *compute placement*, not orchestration.
- **Embed**: GPU (local) **or** API (cloud provider) — *depends on the client deployment*. The same logical stage routes to different worker classes per deployment.
- **Summarize / tag / extract (graph)**: LLM API calls. I/O-bound, rate-limited, cheap workers. THE fan-out case: one document explodes into N per-chunk jobs, results gate back into one per-document completion.
- **Dedup / classify / chunk**: light CPU, run anywhere.

Shape: **linear pipeline of stages; inside a stage, massive fan-out over elements (chunks); fan-in gate before the next stage.**

### 2.2 Connector sync and re-sync (GDrive, SharePoint)

- Scheduled and on-demand scans: list a drive (paginated, rate-limited API), diff against known state, enqueue per-file download/ingest jobs.
- **Re-sync**: thousands of files where 99% are unchanged — cheap check jobs with a few expensive ingest jobs mixed in. Priority matters (interactive re-sync vs. background initial sync).
- Long-running crawls must survive worker restarts: the crawl is itself a job that emits child jobs and checkpoints its cursor.

### 2.3 Integration jobs (Jira, CRM, ERP)

- Bursty, webhook-triggered or scheduled. Strict per-provider rate limits. Need dedup ("this ticket is already being processed") and idempotency keys.

### 2.4 Batch/scheduled workloads (evals, maintenance)

- Eval runs: fan out over an eval set, fan in to compute aggregates. Cron scheduling. Same primitives as ingestion.

**What falls out of these four:** every workload reduces to the same small set of primitives — `submit(job)`, `chain`, `depends_on` (static parallel branches joining: dense ∥ sparse -> store), `fan_out` + `gate(children) -> continuation` (dynamic N), priorities, rate-limit classes, capability routing, cron, checkpoints. No DAG DSL needed. That is the entire orchestration surface.

---

## 3. Requirements

### 3.1 Functional

| # | Requirement | Notes |
|---|---|---|
| F1 | **Per-element durable jobs.** Every unit of work (a chunk summary, a file check) is an individually tracked, retryable, queryable record. | The core lesson from today's pain. |
| F2 | **Tag-based routing (`runs_on`).** In plain words: a job says what kind of worker it needs (`runs_on=["gpu"]`); each worker says what it is (`tags=["gpu","parse"]`); the engine only gives a job to a worker whose tags cover the job's `runs_on`. | Replaces "which process polls which task type." |
| F3 | **Pull-based scheduling.** Workers claim work when they have capacity. The engine never pushes to a busy/dead worker. | Self-balancing across heterogeneous hardware (Sparrow-style late binding). |
| F4 | **Fan-out / gate (fan-in).** A job can emit N child jobs; a gate fires a continuation when children reach a terminal state (all-success or quorum policy). | The only "orchestration" primitive we need beyond chains. |
| F5 | **Chained stages.** `on_success -> submit(next_stage)` — linear pipelines as data, not as workflow DSL. | Our pipelines are linear; keep it that way. |
| F6 | **Priorities + fairness.** Interactive re-sync beats bulk backfill; one giant document must not starve others. | Weighted fair claiming per group key (e.g. document_id, tenant). |
| F7 | **Rate-limit classes.** Named token buckets (e.g. `azure-gpt5: 500/min`) enforced at claim time, shared across all workers. | LLM providers are the scarce resource. |
| F8 | **Retries with backoff + dead-letter.** Per-job-type policy; exhausted jobs land in a DLQ state with full error history. | |
| F9 | **Idempotency / dedup keys.** Submitting the same logical job twice yields one execution. | Connector re-sync, webhook storms. |
| F10 | **Scheduling.** Delayed jobs (`run_at`) and cron jobs. | Pollers, eval schedules. |
| F11 | **Heartbeats + lease reclaim.** A claimed job carries a lease; missed heartbeats return it to the queue. | Crash recovery, at-least-once. |
| F12 | **Cancellation.** Cancel a job tree (document withdrawn mid-ingestion). | |
| F13 | **Full visibility.** Query any job by id/key/state/capability/ctx_id; per-stage throughput; queue depth; worker fleet status. Web UI + API. | "You seriously need to have a queue for each singular element and have visibility on it." |
| F14 | **SDKs.** Python client SDK (submit/await/query) and Python worker SDK (register handlers, advertise capabilities). Wire contract in protobuf so other languages can follow. **The SDK must be easy and intuitive** — a new engineer wires a working task in minutes, with sane defaults for everything (see AD-16). | |
| F15 | **Static dependencies (`depends_on`).** A job can declare it runs only after named jobs succeed, receiving their results as inputs. Covers small static DAGs (dense ∥ sparse -> store; extract -> apply). | Complements F4 (dynamic fan-out) and F5 (chains). |
| F16 | **Result passing between jobs.** Small results stored inline on the job; large results passed by reference to durable storage. Dependents receive them automatically. | See AD-13. |
| F17 | **Intra-job checkpoints.** `ctx.checkpoint()` persists expensive intermediate output (an LLM response) mid-job; retries resume from the checkpoint instead of re-doing the expensive call; cleaned up on success. | See AD-14. |
| F18 | **Correlation id (`ctx_id`).** Every job carries a client-supplied correlation id (the backend's track_id), propagated to children/continuations, queryable in API and UI, present in every log line. | Reconciliation between app and engine. |
| F19 | **Per-job timeouts.** `timeout_s` on every job — a hard execution ceiling independent of the lease; exceeded jobs are failed (retryable) and the worker slot reclaimed. | Production readiness. |
| F20 | **Mature operational UI.** Not a flat job list. Four non-negotiable views: running now, failed (with full history + retry), dependency graph, the queue (waiting-to-run, with the reason it waits). | See Section 7.5. |
| F21 | **Immutable audit ledger.** Every lifecycle event (submitted, claimed, started, checkpointed, failed, retried, completed, ...) is an append-only row — a job's full history is reconstructable like a payment transaction. | See `job_events`, Section 8. |
| F22 | **Compute/apply decoupling.** Slow store steps (Qdrant, graph writes) never block or occupy compute workers; independent applies fan out, transactional applies gate into one job. | See Section 7.4. |
| F23 | **Generated idempotency keys.** `ctx.idempotency_key` — deterministic, stable across retries — handed to every handler for external side-effecting calls. | See AD-21. |
| F24 | **Per-group concurrency ceilings.** `max_concurrent_per_group=N` serializes (or caps) execution per `group_key` value, engine-wide. | See AD-22. |
| F25 | **Human-in-the-loop / external events.** `ctx.wait_for_event(key)` + `engine.signal(key, payload)`: jobs suspend into a `WAITING` state (slot released) and resume on signal or timeout; signals are durable and audited. | See AD-23. |

### 3.2 Non-functional

| # | Requirement | Target |
|---|---|---|
| N1 | Throughput | **Millions of jobs/day, non-stop.** Steady-state: ~10k documents/day fully processed (≈ 2-5M element jobs); burst: 100k+ queued jobs without degradation of claim latency. |
| N2 | Claim latency | < 50 ms p99 from job-ready to worker-claimed under load (dispatcher tick + push over held claim streams). **A task that arrives when workers are idle starts within one 10–50 ms dispatcher tick — no stacked polling tiers** (contrast with the 60s+10s stacked polls today, Section 1.5). |
| N3 | Delivery | At-least-once. Handlers must be idempotent (they already are — staging tables + ON CONFLICT). |
| N4 | Durability | No acknowledged job is ever lost. Engine state survives full restart of every component. |
| N5 | Isolation | Engine has its own database. Clients talk protobuf/gRPC + HTTP. Zero shared schemas with any client app. |
| N6 | Operability | Single-binary/container engine server. One Postgres. Optional Redis. That is the entire footprint. |
| N7 | Security | mTLS or token auth between workers/clients and engine; tenant isolation on job visibility. |
| N8 | Language | Python 3.14 (engine target; SDK floor 3.11), asyncio core. CPU/GPU work isolated in worker processes, not engine threads. |

### 3.3 Explicit non-goals (v1)

- **No general DAG DSL.** Chains + fan-out/gate cover every current workload. A DSL is where workflow-orchestrator complexity tends to come from.
- **No deterministic replay / event-sourced histories** (Temporal's model). Our handlers are idempotent side-effecting calls; replay buys nothing and costs the programming model.
- **No code distribution.** Workers pre-own their code (deployed images per hardware class). The engine routes *data to code*, never code to hardware. Warm model weights on GPU boxes make cold code-push pointless.
- **No exactly-once.** At-least-once + idempotent handlers is the honest contract.
- **No PII filtering in v1.** Middleware hooks exist so it can be added later, but it is out of scope now.
- **No LLM observability.** Token counts, prompt traces, model costs live in the application (LLMGateway/Langfuse). Symba observes jobs, not what the jobs did. `ctx_id` is the join key between the two worlds.

---

## 4. What exists today and why it is not enough

### 4.1 Current pipeline anatomy

- An admission scheduler admits `PENDING` documents into a stage spine table.
- A stage poller launches one orchestrator workflow per stage slice (`parse_workflow`, `summarize_workflow`, ...), each a linear chain of coarse tasks.
- Workers are **sync** orchestrator pollers with a shared thread pool (default 10 threads for *all* task types).
- LLM outputs go through a staging table (good — this survives the migration as the client-side idempotency layer).
- The only fork in the entire system is a static 2-branch fork-join in one workflow definition.

### 4.2 The mismatch, precisely

| We need | The existing orchestrator gives |
|---|---|
| Per-chunk work items | Per-document coarse tasks |
| Route parse to GPU boxes | Task-type polling from anywhere |
| Shared rate-limit budget across workers | Per-process thread caps |
| Query "how many chunk-summaries pending for doc X" | Workflow execution JSON blobs |
| 200-way fan-out per document, thousands of documents | A deployment that struggles beyond dozens of concurrent workflow executions |
| Own the roadmap (open-source ambitions) | Someone else's OSS release cadence and fork politics |

We already route *around* the existing orchestrator (scheduler + spine + poller are ours). The engine completes that move: the spine becomes the engine, the orchestrator exits.

---

## 5. Design principles learned from prior art in the job-queue space

Several existing Python task-queue implementations and workflow platforms were surveyed to distill design principles and known failure modes, without adopting any of them wholesale.

### 5.1 Lightweight Redis-backed task queues

**Mechanics worth adapting:**
- Job scheduling as a sorted-set index scored by ready-timestamp — deferral and retry-with-backoff are just score updates. Elegant. (Symba's equivalent: one `run_at` column; no separate retry/schedule tables.)
- Claim via an atomic check-and-set on an "in-progress" marker with TTL — crash reclaim is "TTL expired, job visible again."
- Result TTLs, job-id-based dedup.
- **Cron dedup via deterministic job ids** (name + tick-timestamp): every worker runs the scheduler, duplicates collapse through the ordinary unique-job-id machinery — no leader election as a *correctness* requirement.

**Known failure modes in this design family that Symba's design must pin:**
- A worker can claim from a stale poll batch and run a job another worker had just deferred — **the claim query must revalidate `run_at <= now()` inside the locking statement**, never trust an earlier scan.
- A past-dated cron tick can produce a negative TTL → exception → the poll loop dies → the worker is silently frozen. **Scheduler/dispatcher loop exceptions must be contained; computed intervals clamped ≥ 0.**
- A bare semaphore alone can deadlock slot accounting on abort paths — release must be unconditional (done-callback/finally) and claims you abandon must be counted.
- Task-reaping gated behind "am I picking jobs" means graceful drain never observes completions. **Drain bookkeeping must be independent of the stop-claiming flag.**
- Job status composed from multiple sequential reads can produce a bogus `not_found` mid-transition. **Status = one atomic read.**
- Aborting a *deferred* (not yet running) job can silently do nothing if no worker would ever see the flag. **Cancellation needs an explicit path for every non-running state.**

**Why this design family is not the substrate:** no routing (one queue), no fan-in, Redis-only durability, results are opaque blobs, no visibility beyond key scans, thin ecosystem.

### 5.2 Postgres-backed task queues

**A well-built Postgres-backed queue is textbook and validates our data-plane design:**
- Claim: `SELECT ... FOR UPDATE SKIP LOCKED` inside a CTE, batch-claimed (one round trip claims for all waiting local consumers — Symba's `LIMIT = sum(free_slots)` is the same idea).
- Wakeup: `LISTEN/NOTIFY` so idle workers don't poll-spin — but treated as a *hint only*; a 1s poll runs regardless, and pure-polling modes exist for connection-pooler compatibility.
- Crash recovery: advisory-lock-guarded sweep marks stale-heartbeat jobs back to queued.
- Priorities, group keys, per-job heartbeats, an embeddable web UI, and even an HTTP proxy queue mode are common features in this space.
- **Cron with zero coordination:** every worker upserts a deterministic cron key every second; an `ON CONFLICT` guard scoped by status and scheduled-time makes N workers race-safe with no leader, no cron state table, no missed-window backfill.
- Retry = state flip on the same row (queued again, rescheduled) — job identity stable across attempts, which is exactly what Symba's attempt counter on one row gives.

**Known failure modes in this design family that Symba's design already avoids (verify with tests, don't re-learn):**
- Per-job advisory locks accumulate operational scars over time (pinned connections going stale, jobs never unlocked on retry, locks surviving disconnect) that eventually get fixed by deleting them entirely in favor of a status-flip + timestamp lease — which is the model Symba starts with.
- An early, naive sweeper implementation can sweep every active job *without checking stuckness*. The sweeper needs its own dedicated regression test suite.
- A sweeper missing a scope predicate (e.g. queue/tenant) can sweep other queues' or tenants' jobs — every sweeper statement needs a full tenant/state/lease scope predicate.
- Implicit transactions combined with NOTIFY-inside-transaction can dead-hang a connection pool — this validates Symba's short-explicit-transactions + no-NOTIFY stance.
- Mixing client-side wall-clock time with server `NOW()` can fire cron early. **One clock source everywhere — Symba uses the DB clock (`now()`) in all SQL, and nothing else.**
- Liveness stored inside a serialized blob forces the sweeper to deserialize *every* active job. Symba's indexed `lease_expires_at` column is the deliberate, justified extra schema.

**Why this design family is not the substrate:** job payloads are opaque blobs (no queryable columns), history is TTL-deleted (no durable audit), no capability routing, no fan-out/gate, a language-specific wire format (kills cross-language and open-source SDK story).

### 5.3 Broker/receiver-style async task-queue architectures

**Shape worth imitating:**
- Clean broker/receiver/result-backend separation, pluggable transports.
- **Configurable ack timing** (on-receive / on-execute / on-save / manual) — this is exactly how at-least-once semantics should be expressed. Ack-before-execute designs eventually have to move to ack-on-complete after learning that a crash between ack and execute means a lost message. Symba's ack-on-complete (AD-7) starts from that endpoint.
- A multi-hook middleware chain (client-side pre-send through worker-side post-save) — retries, smart backoff, metrics are all middlewares. One rough edge NOT to copy: some implementations let middleware exceptions go uncaught by design, which can kill message processing (a broken metrics middleware taking down execution) — Symba suppresses middleware exceptions instead.
- A process manager with auto-restart of dead worker processes, per-child task-count recycling, and a startup **readiness handshake** (needed after a parent process was seen hanging while waiting on children that never signaled). Soft-to-hard signal escalation (N repeated interrupts escalates to a hard kill).
- FastAPI-style dependency injection into handlers (including dependency overrides for tests).
- Keep a strong reference set for spawned asyncio tasks (the garbage collector can silently cancel unreferenced tasks — a well-documented Python asyncio gotcha); release slots in done-callbacks.

**A recurring failure mode in this design family — task identity across retries:** a retry that generates a new task id breaks any client waiting on the original result. This has recurred independently across the main execution path, the scheduler path, and interval-schedule paths in various implementations over time. Symba's DB-native retry (attempt counter on the same row, same job id) eliminates the class structurally. Some of these projects also later removed their own over-engineering (unnecessary per-schedule source references and pluggable merge functions) as it proved unneeded in practice.

**Why this design family is not the substrate:** no durable job state in core (delegated to broker plugins), no routing beyond one-broker-per-queue, no gate/fan-in (linear+map pipelines only), result waiting is polling-based.

### 5.4 The platforms

- **Coarse workflow orchestrators**: covered in Section 4. Orchestrator without an execution/placement story; heavy DSL; heavyweight operational surface.
- **Temporal**: durable execution + task-queue routing is genuinely close to F2/F3. Rejected on: multi-service operational weight (frontend/history/matching/worker services + Cassandra/Postgres + optional Elastic for visibility); the deterministic-workflow programming model (replay-safe code, no bare I/O in workflows) is hostile to "loop over 200 chunks calling LLMs"; and building our product on it means our differentiating layer is a thin veneer over someone else's engine — fatal for the open-source ambition.
- **In-process graph-execution libraries** (used elsewhere for conversational agent flows): a *state-machine library inside one process*, not a distributed engine — no queue, no workers, no placement, no fleet. Wrong tool for ingestion, but three of their ideas transfer directly and are adopted here: **(1) checkpointing as a first-class API** — persisting execution state per step to a checkpointer (Postgres/Redis backends) so execution resumes after a crash; our `ctx.checkpoint` (AD-14) is the job-engine analog. **(2) Explicit edges between named nodes** — an in-process "add edge between nodes" primitive is the in-process version of `depends_on` (AD-12); validation that referenced nodes exist happens at graph-build time, and our SDK should equally validate dependency references at submit time. **(3) Map-reduce-style dynamic fan-out** — mirrors our fan-out/gate. The boundary stays clean: an in-process graph library remains the right tool *inside* a single conversational request; the engine coordinates *across* processes, machines, and days.

### 5.5 Synthesis — what we take from prior art

| Design lineage | What we adopt |
|---|---|
| Postgres-backed queues | Postgres claim pattern (SKIP LOCKED CTE, batch claim for all waiting slots), advisory-lock sweeper election, heartbeat staleness, group_key, web UI ambition, **cron-as-idempotent-upsert (no leader election)**, retry-as-state-flip-on-same-row, one clock source (DB `now()`), short explicit transactions + autocommit elsewhere. (The LISTEN/NOTIFY wakeup pattern this lineage uses we deliberately do NOT adopt — see AD-3; it is itself typically treated as just a hint over a 1s poll, with pure-polling modes shipped alongside it) |
| Polling-dispatcher rewrites of prior hot-table designs | Polling dispatcher over LISTEN/NOTIFY; hot-table evacuation (terminal rows leave the claim table); gRPC streams as transport with DB leases as truth |
| Broker/receiver task-queue architectures | Broker/result abstraction boundaries, ack-timing semantics (ack-on-complete as the proven endpoint), middleware hooks (with exception suppression, unlike some of these designs), process manager with readiness handshake + soft-to-hard signal escalation, DI in worker SDK, strong-ref task sets + done-callback slot release |
| Redis-backed lightweight queues | Sorted-set-style ready-time scoring (as a `run_at` index), TTL-lease claim mental model, job_id dedup, claim-time revalidation of readiness, contained scheduler-loop exceptions, drain bookkeeping independent of stop-claiming, status as one atomic read |
| Temporal | Task queues as routing targets (renamed: capability queues), heartbeat-carrying long activities |
| Sparrow (SOSP'13) | Pull-based late binding: never assign work to a worker earlier than necessary; the claim moment is the placement decision |
| In-process graph-execution libraries | Checkpointing as a first-class resume API; explicit validated edges (-> `depends_on`); map-reduce-style dynamic fan-out |
| Prior internal orchestrator usage | The stage spine concept — but as engine-native chained jobs instead of external workflow JSON |

---

## 6. Architecture decisions

Each decision is recorded with its alternatives and rationale. These are the calls; changing one requires re-arguing it here.

### AD-1: Build our own engine (vs. adopt/extend)

**Decision:** Build. **Because:** Section 1.3/1.4 — no candidate provides capability routing + fan-out/gate + durable per-element visibility + heterogeneous placement in an operable package, and the engine is product surface for us.

### AD-2: Python, asyncio core

**Decision:** Python 3.14 (engine; SDK supports 3.11+), fully async engine server and SDKs. Not Go.
**Rationale:** The engine is I/O-bound (DB, sockets); asyncio handles tens of thousands of concurrent claims/heartbeats fine. Heavy compute never runs in the engine — it runs in workers, in separate processes. Team leverage, shared tooling with the main codebase, and open-source reach in the AI/Python ecosystem outweigh Go's raw performance, which we don't need at the engine tier. Revisit only if claim-path p99 becomes CPU-bound after profiling (mitigation path: rewrite the claim hot path, not the engine).

### AD-3: Postgres as the source of truth; polling dispatcher; Redis for buckets and checkpoint cache only

**Decision:** All job state lives in the engine's own Postgres. Job readiness is discovered by the engine's dispatcher loop polling the ready set on a 10–50 ms adaptive tick (backing off toward 250 ms when idle) — **not** by Postgres LISTEN/NOTIFY, and not by Redis pub/sub. Redis is an *optional* component for exactly two things: shared rate-limit token buckets (AD-11) and the checkpoint fast path (AD-14) — never for state, never for wakeups.
**Rationale:** Durability + queryability (F1, F13, N4) demand a real database; prior art with a Postgres-backed SKIP-LOCKED claim path proves this claim pattern is fast enough. LISTEN/NOTIFY is rejected deliberately (rev 7): transactions that issue NOTIFY take an AccessExclusiveLock at commit that serializes **every commit on the instance** — a documented production outage mode at our write volume, plus payload caps, fire-and-forget loss on disconnect, and connection-pooler incompatibility. A 10–50 ms poll from one dispatcher is a trivial, constant, easily-monitored load and is a pattern proven at scale by polling-dispatcher rewrites of prior hot-table designs. The tick is the *only* latency tier in the system, and polling is correct on its own — there is no notification channel whose loss could strand a job. Degraded mode without Redis: buckets fall back to Postgres counters; checkpoints fall back to the Postgres write-behind copy. Dispatch is unaffected (it never depended on Redis).

### AD-4: Engine owns its database; hard client boundary

**Decision:** The engine gets its own Postgres database (not a schema in the app DB). Clients interact only via SDK/API. Job payloads carry *references* (document_id, chunk ids, storage URIs), never the app's data.
**Rationale:** N5, open-source viability, and the multi-hardware deployment: GPU boxes must reach the engine, not the app's DB.

### AD-5: Pull-based claiming with tag matching (`runs_on`)

**Decision:** Workers long-poll/stream `Claim(tags=[...], slots=n)`; the engine matches ready jobs whose `runs_on` tags are all present in the worker's tags, honoring priority, fairness, and rate-limit budgets at claim time. Simple mental model: **`runs_on` = "what kind of machine/worker must run me"; worker tags = "what kind of machine/worker I am."**
**Rationale:** F2/F3. Push-based assignment requires the engine to model worker load; pull with late binding (Sparrow) makes load balancing emergent — a slow GPU box simply claims less. Placement = the claim, nothing else.

### AD-6: Protobuf wire contract; gRPC data plane + HTTP control plane

**Decision:** All wire messages defined in protobuf. Hot paths (claim, heartbeat, complete) over gRPC streaming; admin/query/UI over HTTP/JSON (grpc-gateway style).
**Rationale:** Cross-language SDKs for open source; streaming claims beat HTTP long-poll; humans and dashboards get plain HTTP. A language-specific serialization format (as used by some prior-art queues) is disqualified.

### AD-7: At-least-once + idempotent handlers; ack-on-complete

**Decision:** A job is acked only by an explicit `Complete`/`Fail` from the worker. Lease expiry requeues. Handlers must be idempotent; the SDK ships idempotency helpers (dedup keys, result-if-exists).
**Rationale:** N3. Our handlers already are idempotent (staging + ON CONFLICT). Exactly-once is a lie we refuse to tell. The ack-timing taxonomy surveyed in prior art (on-receive / on-execute / on-save / manual) confirms ack-on-complete as the safe default.

### AD-8: Orchestration = chains + fan-out/gate, evaluated in the engine, defined by clients in code

**Decision:** No workflow DSL, no workflow definitions stored in the engine. A job's `on_success` may name a continuation job type; a `fan_out` creates children under a `gate`; when the gate's policy is satisfied the engine emits the continuation job. Pipelines are expressed in the client SDK as code.
**Rationale:** Section 2 shows every workload reduces to these primitives. DSLs are where orchestrators grow an expression language and an editor and become a platform in their own right. Keeping definitions client-side keeps the engine generic.

### AD-9: Workers pre-own their code

**Decision:** Worker images are built and deployed per hardware class (parse-worker image on the Spark boxes, llm-worker image on API nodes). The engine never distributes code.
**Rationale:** GPU workers need warm model weights and heavyweight deps (docling, vLLM, torch) — cold code-push is worthless there. Versioning is handled by worker tags (`parse_v2`), letting old and new workers coexist during rollout.

### AD-10: One execution = one asyncio task; CPU/GPU isolation via worker processes

**Decision:** In the worker SDK, each claimed job runs as an asyncio task (I/O workloads: LLM calls, API syncs). CPU/GPU job types declare `execution=process`, and the SDK runs them in a `ProcessPoolExecutor` (or dedicated subprocess for GPU context ownership) with the async shell handling heartbeats.
**Rationale:** Answers the "thread vs async vs process" question per workload class instead of globally. Heartbeats must never be blocked by a busy GIL — the async shell owns them.

### AD-11: Shared rate limits are engine-enforced at claim time

**Decision:** Named rate-limit classes (token buckets) live in the engine (Redis when present, Postgres fallback). A job tagged `rate_class=azure-gpt5` is only claimable when the bucket has tokens.
**Rationale:** F7. Worker-side limiting cannot coordinate across a fleet; provider 429s at claim time are cheaper than after dispatch. This directly fixes the "LLM rate limits hit mid-batch" failure mode.

### AD-12: `depends_on` — static dependencies as a first-class primitive

**Decision:** A job may be submitted with `depends_on=[job_id, ...]`. It stays `SUBMITTED` (not claimable) until all dependencies reach `SUCCEEDED`, then flips to `QUEUED` via a transactional `remaining_deps` counter (no scanning). If any dependency ends `DEAD`/`CANCELLED`, the dependent is cancelled (policy hook for "run anyway" later).
**Rationale:** Chains cover linear, gates cover dynamic-N — but `dense ∥ sparse -> store` and `extract_graph -> apply_graph` are small *static* DAGs known at submit time. `depends_on` is the natural shape for them; internally, chains and gates are implemented on top of the same dependency counter, so the engine has ONE readiness mechanism, and the SDK exposes three shapes:

| Primitive | Shape | Example |
|---|---|---|
| `chain` | linear, static | pipeline stages |
| `depends_on` | small static DAG | dense ∥ sparse -> store; extract -> apply |
| `fan_out` + `gate` | dynamic N -> continuation | 200 chunk summaries -> executive summary |

This is deliberately still not a DSL — structure is declared in client code at submit time, never stored as workflow definitions.

### AD-13: Tiered result passing (inline / reference); Redis is never the only copy

**Decision:** A job's `Complete(result)` is stored on the job row (JSONB, hard size cap ~64KB, enforced at complete-time with a clear error). Dependents and chain successors access upstream results as **`ctx.output`, a dict keyed by `task_name`**, delivered in two tiers (rev 7 — bounds the claim path):

- **Shipped inline with the claim** (zero fetches for the common case): the result of the job's *immediate chain predecessor* plus the results of every job in its declared `depends_on` set — capped at **256KB total upstream bytes per assignment** (validated at submit time against the producers' declared caps; oversized combinations are rejected at submit with a clear error, not discovered at claim).
- **Lazily fetched on access** (the rare case): any *deeper* ancestor in the chain — `ctx.output["some_early_task"]` transparently issues a `GetResult` RPC to the engine, and the SDK memoizes it for the life of the execution. Handlers needing a deep ancestor's result on the hot path should declare it in `depends_on` (or re-emit it forward) to get inline delivery; lazy fetch keeps deep access *possible* without bloating every assignment with full ancestry.

Larger outputs go to durable storage the *worker* owns (client staging tables, MinIO/S3) and only a reference crosses the engine. Redis may cache hot results as a read-through optimization — never as the only copy.

Access rules for `ctx.output`:
- `ctx.output["parse_content"]` — result of the named upstream task. Valid because in practice each `task_name` appears once per flow tree; name-keyed access reads like the flow itself.
- If a flow legitimately runs the same task twice upstream of one job, disambiguate with **aliases at submit time**: `depends_on={"first_pass": job1.id, "second_pass": job2.id}` -> `ctx.output["first_pass"]`. Aliases win over task names on collision; the SDK raises on ambiguous unaliased access instead of guessing.
- The shape of each entry is whatever the producer returned — see the optional output schemas in AD-15 for how consumers know (and can type-check) that shape.

**Rationale:** F16. The engine moves coordination, not data — keeps the jobs table small and the claim path fast. The two-tier split exists because "ship the whole ancestry" is unbounded: a 50-step chain × 64KB results = 3.2MB per assignment, breaching gRPC message caps and turning the claim query into an ancestry walk. Immediate-predecessor + declared-deps covers every flow in Section 11 with zero fetches; the lazy path is the escape hatch, not the norm. Name-keyed results replace the `${task_ref.output.field}` string-plumbing pattern common in workflow orchestrators with plain dict/attribute access, and keep handler code readable without holding job-id objects in scope. Putting handoff state only in Redis would mean a Redis restart silently loses computed results and forces re-computation (exactly what F17 exists to prevent), violating AD-3.

### AD-14: Intra-job checkpoints — Redis-first with Postgres write-behind

**Decision:** `await ctx.checkpoint(data)` persists a checkpoint keyed by the job's **dedup identity** (not the attempt): written synchronously to Redis (fast path) and asynchronously flushed to the engine's Postgres (`job_checkpoints` table, the durable copy). On any retry/re-run/duplicate submit, the SDK loads the latest checkpoint before invoking the handler — `ctx.checkpoint_data` is populated and the handler skips the expensive part. On terminal success, checkpoints for that job are deleted (Redis immediately, Postgres by the sweeper). Checkpoints have a TTL safety net (default 7 days) so orphans never accumulate.
**Rationale:** F17 and the user requirement in one sentence: *an LLM response, once received, is never lost to a downstream failure, and is never paid for twice.* Redis gives checkpoint writes at LLM-call frequency without bloating the hot jobs table; the Postgres write-behind keeps AD-3's "Redis is never the only copy" honest — if Redis dies between checkpoint and retry, the retry reads the Postgres copy and at worst re-does work checkpointed in the final unflushed window (seconds). Idempotency is inherited from the dedup key: a duplicate submit of the same logical job sees the same checkpoint.

Two complementary patterns, both supported:
- **(a) Job splitting** (default for LLM stages): `summarize_chunk` (LLM call, result persisted per AD-13) -> `apply_chunk_summary` (store), linked by `depends_on`. Retrying the store never re-calls the LLM, and cost/failure visibility is per-step. See Section 7.4 for the full compute/apply decoupling rules.
- **(b) `ctx.checkpoint`** (single-job convenience): checkpoint after the LLM call, store after; a retry resumes from the checkpoint.

### AD-15: Task identity — unique `task_name` + optional `pipeline`/`stage` grouping

**Decision:** Three separate fields, not an encoded path:

| Field | Required | Meaning |
|---|---|---|
| `task_name` | **yes, unique** | The one identity of a task type. Registered once per worker fleet (`@worker.task("summarize_chunk")`); duplicate registration is an error. `depends_on`, chains, gates, dedup — everything references `task_name` (and job ids at runtime). |
| `pipeline` | optional | Pure grouping label: which flow this job belongs to (`ingestion`, `gdrive_sync`, `evals`). No engine behavior attached — it exists so the UI/API can show and filter whole flows. |
| `stage` | optional | Pure grouping label within a pipeline (`parsing`, `summarization`, `graph`). Same: filter/rollup only, zero impact on execution. |

A **pipeline as a concept** (the succession — parse then chunk then embed...) stays what it already is in this design: the chain/`depends_on` structure declared at submit time in client code. The `pipeline` label is how you *see* that succession in one filter; the dependency graph is how it *runs*. The engine never stores pipeline definitions.

**Rationale:** Encoding grouping into the name (`ingestion/classify/llm`) conflates identity with organization — renaming a stage would break task identity, and the same task couldn't be reused across two pipelines. With split fields, `summarize_chunk` is registered once, unique, stable; `pipeline`/`stage` are set per-submit and can differ per flow (the same `embed_dense` task can run under `pipeline=ingestion` and `pipeline=resync`). UI queries become `pipeline=ingestion&stage=parsing` instead of string prefix tricks.

**Input/output schemas — optional, worker-configurable.** A task may declare Pydantic schemas at registration:

```python
@worker.task("parse_content", input_schema=ParseContentInput, output_schema=ParseContentOutput)
def parse_content(ctx, payload: ParseContentInput) -> ParseContentOutput: ...
```

When declared, the SDK validates `payload` before invoking the handler (bad submits fail fast, not deep inside business logic), validates the return value before sending `Complete` (a typo'd field fails HERE, not as a KeyError three jobs downstream), and deserializes `ctx.output["parse_content"]` into the producer's declared output type — consumers learn a result's shape by reading the producer's schema, with autocomplete, instead of reading its function body. The engine itself stays schema-blind (payload/result are opaque JSONB) — validation is purely SDK-side, so it costs the engine nothing.

Whether schemas are *required* is a *worker-level* switch, because ceremony is not always worth it (one-off internal tasks, prototypes):

```python
worker = Worker(..., strict_schemas=True)   # every registered task MUST declare both schemas
worker = Worker(..., strict_schemas=False)  # default: schemas validated when present, optional otherwise
```

Recommended posture for production workers: `strict_schemas=True` — the same discipline the current orchestrator rules already mandate (task DTOs matching workflow references), now enforced by the SDK instead of by code review.

### AD-16: Execution-mode defaults per stage profile; override per task

**Decision:** The worker SDK ships named **profiles** with pre-tuned defaults (`execution`, `timeout_s`, `lease_ttl_s`, `max_attempts`, backoff), selected by the task's declared profile — and every value is overridable at registration or submit:

| Profile | Execution default | When to use it (simple rule) |
|---|---|---|
| `io` | `async` | The task *waits* on something external — LLM calls, HTTP APIs, DB reads/writes. Almost all our tasks. Default profile. |
| `cpu` | `process` | The task *computes* — parsing, chunking big texts, hashing. It would block the event loop, so it runs in a subprocess. |
| `gpu` | `process` (dedicated, warm) | The task needs the GPU — docling, vLLM, local embeddings. One long-lived subprocess owns the GPU context. |

Rule of thumb for choosing: **"Is the task mostly waiting or mostly working? Waiting -> `io`. Working -> `cpu`. Working on a GPU -> `gpu`."** If unsure, start with `io`; if the worker's event loop lags (SDK warns), move it to `cpu`.
**Rationale:** AD-10 answered *how* each mode runs; this answers *who chooses*. Engineers should not decide asyncio-vs-subprocess per task from first principles — the profile encodes it, and stage defaults (parse -> `gpu`, summarize -> `io`) make the common case zero-config.

### AD-17: Correlation id (`ctx_id`) end to end

**Decision:** Every submit accepts `ctx_id` (the backend's track_id). The engine propagates it automatically to every child, continuation, and dependent job; indexes it; exposes it in queries (`engine.query(ctx_id=...)`), the UI (pipeline view per ctx_id), and injects it into every structured log line and trace span on both engine and worker sides.
**Rationale:** F18. Reconciliation between the app's world ("document X, request Y") and the engine's world (thousands of jobs) must be one lookup, not a join across systems.

### AD-18: Engine and SDK are separate repositories

**Decision:** Two repos: `symba` (server: matcher, state, UI, protobuf definitions as the contract source) and `symba-sdk-python` (client SDK + worker SDK, generated protobuf stubs). SDK versions declare a supported engine protocol range.
**Rationale:** Independent release cadences (SDK iterates fast on ergonomics; engine on stability), clean open-source story (users install only the SDK), and it forces the wire contract to stay the real boundary — no reaching into engine internals from the SDK.

### AD-19: Chains are linked lists; each job carries only its own next step

**Decision:** `chain=[...]` at submit time is **SDK sugar, not engine state**. Internally each job row stores only `on_success` = its *immediate* next task plus the *remaining* chain tail. When a job completes, the engine submits the next job carrying `chain[1:]` as its own tail. There is no "chain object" anywhere — a chain of N steps is N jobs, each a linked-list node knowing only its successor.

So yes: **the chain is declared once, at the first submit** — that's the ergonomic surface. And yes, the "what if each task only knew its next one" idea is exactly the implementation — the two views are the same thing:

| You write (submit time) | Engine stores (per job) |
|---|---|
| `submit(task="download_source", chain=["parse_content", "persist_parsed", "complete_parse"])` | `download_source`: next=`parse_content`, tail=`[persist_parsed, complete_parse]` |
| — | `parse_content`: next=`persist_parsed`, tail=`[complete_parse]` |
| — | `persist_parsed`: next=`complete_parse`, tail=`[]` |

**Conjunction/`depends_on` composes cleanly because of this.** A chain step's continuation is just a normal submit — so when a flow needs a static join (dense ∥ sparse -> store), you don't force it into the chain: the handler (or the client) submits the parallel jobs with `depends_on`, and the dependent job can itself carry the rest of the chain. Chains for the linear 90%, `depends_on` for the static joins, `fan_out`+gate for dynamic N — all three end on the same readiness counter (AD-12), so they mix freely in one flow.

**Rationale:** One mental model ("every job knows at most its next step"), no chain-definition storage in the engine (stays true to AD-8: no stored workflows), and resubmitting/retrying any mid-chain job trivially resumes the rest of the flow because the tail travels with the job.

### AD-20: Handlers decide branches; `on_failure` replaces failure workflows

The prior workflow-orchestrator setup revealed three flow-control cases beyond a straight chain. All three are decided **in the handler (app code)**, never by an engine-side expression language:

**(a) Conditional continuation — the dedup case.** `check_duplicate_stage` either terminates the flow (duplicate found, link it, done) or proceeds to parse. Today that's a SWITCH-style node with a JS expression in the old orchestrator. In Symba the handler just returns a control value:

```python
@worker.task("dedup_doc")
async def dedup_doc(ctx, payload):
    dup = await service.check_duplicate(payload["document_id"])
    if dup:
        await service.link_duplicate(payload["document_id"], dup.id)
        return ctx.stop_chain(result={"is_duplicate": True})   # chain tail dropped
    return {"is_duplicate": False}                             # default: next in chain
```

**(b) Idempotency skip — the summarize case.** Today: a `check_document_status` task + a SWITCH + duplicated completion tasks on both branches. In Symba it's the first line of the handler: `if doc.summarized_at: return ctx.skip()` — job succeeds, chain continues, no LLM cost. No SWITCH machinery, no branch duplication.

**(c) Failure hooks — the `*_failure_workflow` case.** Every stage in the old orchestrator carried a `failureWorkflow` whose only task is `mark_stage_failed` (flip the spine row, mark the document FAILED so the FE never shows stale status). In Symba this is a submit-time parameter: `on_failure={"task": "mark_stage_failed", "payload": {...}}` — submitted by the engine when the job (or any job downstream in its chain tail) goes `DEAD` or is cancelled by dependency failure. The hook inherits `ctx_id` and receives the failed job's id, task_name, and final error in its payload.

**Rationale:** Branch conditions are always domain logic (is this a duplicate? is it already summarized?) — the app owns them, in Python, testable, no expression DSL to learn or debug (`evaluatorType: javascript`-style inline expression strings are exactly the complexity we're escaping). The engine's job is only to honor the outcome: continue, stop, or fire the failure hook. Return-value contract: a plain dict = result + continue; `ctx.stop_chain()` = result + drop tail; `ctx.skip()` = success without doing work; raising = failure with retry policy.

**(d) Branching to different task types (X vs Y) is caller-side, never handler-side.** A handler must NOT redirect the flow to an arbitrary next task (`goto`-style APIs are rejected: they make the chain unreadable — you'd have to open every handler's source to know the possible paths, which is exactly the workflow-JSON-archaeology problem the earlier system suffered from). When the next step depends on a job's output, the *code that submitted it* awaits the result and submits the branch — a plain `if` in the flow's entry function, visible in one place:

```python
async def start_extraction(doc_id, ctx_id):
    classify = await engine.submit(task="classify_doc", payload={"document_id": doc_id}, ctx_id=ctx_id)
    result = await classify.result()                       # await one job's outcome
    if result["category"] == "contract":
        next_task = "extract_contract_fields"
    else:
        next_task = "extract_generic_fields"
    await engine.submit(task=next_task, payload={"document_id": doc_id}, ctx_id=ctx_id)
```

Zero new engine primitives — `submit` + `await job.result()` + Python. The rule stated plainly: **the engine never inspects results to make routing decisions; every possible path is visible in the flow module.**

**(e) Agentic loops (evaluate -> maybe re-run) are the same pattern, made durable.** "Call the LLM, if confidence < 90% re-run from step 2" — ReAct-style loops — do NOT need a loop primitive in the engine. The controller is itself a job: it evaluates the result, and either finishes or *resubmits the sub-chain* with an attempt counter in the payload. Because the controller is a durable job, the loop survives crashes, every iteration is a visible job tree in the UI, and a runaway loop is bounded by an explicit counter (and, as a backstop, the controller's own `max_attempts`):

```python
@worker.task("grade_extraction")
async def grade_extraction(ctx, payload):
    grade = await llm_judge(ctx.output["extract_fields"])
    if grade.confidence >= 0.9 or payload["round"] >= 3:      # bounded, explicit
        return {"final": True, "confidence": grade.confidence}
    # re-run the sub-chain from step 2; the loop is jobs all the way down
    await ctx.submit(task="extract_fields",
                     chain=["grade_extraction"],
                     payload={**payload, "round": payload["round"] + 1})
    return ctx.stop_chain(result={"final": False, "retry_round": payload["round"] + 1})
```

The engine sees only ordinary submits; the app sees a durable, inspectable, bounded loop. This is deliberately how agentic workloads should compose on Symba: in-process, single-request loops stay in an in-process graph-execution library; loops that must survive restarts, span machines, or burn real money per iteration become job trees.

### AD-21: `ctx.idempotency_key` — generated for the handler, free to pass downstream

**Decision:** Every execution context exposes `ctx.idempotency_key`: a deterministic string derived from the job's *logical identity* (`tenant + dedup_key`, falling back to `job_id` when no dedup_key is set), stable across retries and duplicate submits of the same logical job. Handlers pass it straight into third-party SDKs that accept idempotency keys (Stripe, SendGrid, booking/payment/email APIs) — a retried job then cannot double-charge, double-email, or double-book, with zero hand-wired key plumbing. A per-attempt variant (`ctx.idempotency_key_attempt`, suffixed with the attempt number) exists for the rare API where a retry SHOULD be a new operation.
**Rationale:** The single most-cited production gap across every orchestrator reviewed: *"no orchestrator generates the idempotency key for you — you wire `(run_id, step_id)` into every tool client by hand, and when someone forgets, a retry double-charges."* Symba already owns the right identity (`dedup_key` is the logical-job key by design; AD-14 already keys checkpoints off it); handing it to the handler costs nothing and closes the gap by default.

### AD-22: Per-group concurrency ceilings (`max_concurrent_per_group`)

**Decision:** In addition to fairness (F6), `group_key` supports a hard cap: a task registered (or submitted) with `max_concurrent_per_group=N` never has more than N jobs of that task RUNNING per `group_key` value — excess jobs stay QUEUED, in submit order, claimed as slots free. Enforced in the claim query (per-group running-count guard), engine-wide across all workers.
**Rationale:** A named, recurring market ask for agent/webhook workloads: *"a duplicate alert storm hits — run ONE triage per alert key, queue the rest; no duplicate side effects, no race conditions, no token storms."* Fairness spreads load but does not serialize; `dedup_key` collapses *identical* submits, but distinct jobs for the same entity (three different updates to one ticket) must all run — just not concurrently. `max_concurrent_per_group=1` gives ordered, exclusive processing per entity with no app-side locking. Values >1 are useful too: cap per-document parallel applies to protect a downstream store.

### AD-23: Human-in-the-loop / external events as a first-class job state (`WAITING`)

**Decision:** A job may suspend itself on an external event: `await ctx.wait_for_event(key, timeout_s=...)` parks the job in a new `WAITING` state — its worker slot is **released** (waiting is free), the wait key is indexed, and the job resumes (re-queued, claimable, `ctx.event_payload` populated) when any authorized caller invokes `engine.signal(key, payload)` — an approval click in the app, a webhook handler, another job. Waits carry a mandatory timeout: on expiry the job resumes with `ctx.event_payload = None` and decides for itself (proceed, fail, escalate). Signals are durable rows: a signal that arrives *before* the wait is registered is not lost — the wait returns immediately (rendezvous semantics, no race window).

**The re-entry contract (rev 7 — this is NOT in-place suspension, and the docs say so loudly):** because the slot is released, resume means *re-queue and re-claim — possibly on a different worker — and the handler re-runs from the top*. Local variables computed before the wait are gone; only `ctx.checkpoint_data` and `ctx.event_payload` survive the park. Three rules, enforced by the SDK:
1. **Everything expensive before a wait must be checkpointed** (`ctx.checkpoint`) or idempotent. On resume, the handler's code before `wait_for_event` executes again — the checkpoint is what makes that re-execution free. The SDK logs a WARNING when `wait_for_event` is called on a resumed execution with no checkpoint present.
2. **On re-entry, `wait_for_event` with the same key does not re-park**: the SDK sees the consumed signal for this job (delivered in the assignment) and returns `ctx.event_payload` immediately — the handler flows past the wait naturally without special-casing "am I resuming?".
3. **Repeat waits in one handler need distinct keys** (e.g. suffix a step name); reusing a consumed key raises `WaitKeyAlreadyConsumed` rather than silently returning the stale payload.

Example 11.9 relies on this contract: the LLM draft is checkpointed *before* the approval wait, so re-entry skips the LLM call and lands on the wait, which returns the signal payload immediately.
**Rationale:** The second big market gap: approval/HITL pauses are "side channels bolted next to the engine" everywhere — state parked in app tables, resumed by bespoke pollers, invisible to the orchestrator's UI and audit trail. Making WAITING a real state means: the dependency-graph view shows *approval pending, 2 days* on the actual job; `job_events` records who signaled, when, with what payload (compliance-grade evidence for free, F21); and waiting is cheap — a job paused three days holds a row, not a worker slot or an open connection. Integration workloads (Jira/CRM approval steps) and agentic flows ("pause for human review when confidence < 90%," composing with AD-20e) need exactly this. Deliberately NOT Temporal-style signal handlers inside workflow code — one primitive, one state, no replay model.

### What Symba deliberately does NOT absorb (boundary check)

- **No LLM observability in the engine.** Token counts, prompt traces, model costs, Langfuse spans — all of it belongs to the application (the LLMGateway already owns tracing). Symba runs *something*; it does not know or care that the something called an LLM. The engine's observability surface is jobs, states, durations, retries, queues, workers — full stop. `ctx_id` is the join key if the app wants to line its LLM traces up with engine jobs; the engine does nothing more.
- **No policy/governance DSL.** AD-23 gives the approval *mechanism*; who may approve what is app logic behind `engine.signal`, not engine rules.
- **Dead-letter queues: already answered by design, worth naming.** The market complaint — "failed runs vanish into logs; you can't inspect or replay them" — is answered here structurally: DEAD is a queryable state, `job_events` holds every attempt's full history (F21), and the UI's Failed view is retry/replay-capable (F20). *A failed job is a row you can read, diff, and resubmit — not a log line.*

---

## 7. System design

### 7.1 Topology

```
+---------------------+          +----------------------------+
|  Client app         |          |  ENGINE SERVER (1..n)      |
|  (client SDK)       |--gRPC--> |  - submit/query API        |
|  submit, query,     |  HTTP    |  - dispatcher (10-50ms     |
|  await gates        |          |    tick) + claim matcher   |
+---------------------+          |  - gate evaluator          |
                                 |  - sweeper (lease reclaim) |
+---------------------+          |  - cron scheduler          |
|  Other clients      |--------> |  - rate-limit budgets      |
|  (evals, integr.)   |          |  - web UI / metrics        |
+---------------------+          +------+---------------+-----+
                                        |               |
                                 [Postgres 18: truth]  [Redis: buckets,
                                  jobs + archive        checkpoint cache
                                  + event ledger]       (optional)]
                                        ^
        claim/heartbeat/complete (gRPC stream)
                                        |
   +------------------+  +------------------+  +---------------------+
   | parse workers    |  | llm workers      |  | connector workers   |
   | (4x NVIDIA Spark)|  | (cheap API boxes)|  | (gdrive, sharepoint)|
   | caps: parse,gpu  |  | caps: llm        |  | caps: connector:*   |
   +------------------+  +------------------+  +---------------------+
```

- **Engine server** is stateless apart from Postgres; scale horizontally (SKIP LOCKED makes concurrent claim matchers safe; the sweeper takes an advisory lock).
- **Workers** are outbound-only (they dial the engine) — GPU boxes behind NAT/firewalls need no inbound ports.
- **Workers run anywhere, split however you want** — this is a feature, not an accident. A laptop, the Spark boxes, a cloud autoscaling group, all against the same engine simultaneously; the engine never knows or cares where a worker is, only what it can do. Worker tags double as dev/canary routing: a worker with tag `parse_dev` receives only jobs whose `runs_on` asks for it — route test traffic to your machine without touching production workers.

### 7.2 Job lifecycle

```
SUBMITTED --(ready: run_at reached AND remaining_deps = 0)--> QUEUED
QUEUED --(worker claim, lease granted)--> RUNNING
RUNNING --Complete--> SUCCEEDED --> decrement dependents' remaining_deps,
                                    bump gate counter, submit on_success continuation
RUNNING --Fail(retryable) / lease expired / timeout_s exceeded-->
                                    QUEUED (attempt+1, backoff via run_at)
RUNNING --Fail(fatal) or attempts exhausted--> DEAD (DLQ)
RUNNING --wait_for_event(key)--> WAITING (slot released; AD-23)
WAITING --signal(key) or wait timeout--> QUEUED (resume; event payload attached)
DEAD/CANCELLED --> cancel dependents (AD-12 failure propagation)
any --Cancel--> CANCELLED (propagates to children and dependents)
```

A job entering `QUEUED` is picked up by the next dispatcher tick (10–50 ms adaptive; Section 1.5) and pushed over a held claim stream — the tick is the only latency tier in the system, and dispatch correctness never depends on any notification channel.

Gate evaluation: when a child reaches a terminal state, the engine checks the parent gate's policy (`all_success` | `all_terminal` | `quorum(n)`); if satisfied, the continuation job is submitted with an aggregate manifest (child ids + states), not child payloads.

Timeouts vs. leases: `timeout_s` is the *total execution ceiling* for one attempt (enforced worker-side with engine-side backstop); `lease_ttl_s` is the *liveness contract* (how long between heartbeats before the job is presumed abandoned). A slow-but-alive job heartbeats past many lease windows until `timeout_s` kills it.

### 7.3 The claim path (hot path)

1. Worker holds a claim stream open: `Claim(worker_id, tags, slots)`.
2. The dispatcher ticks (10–50 ms adaptive, backing off toward 250 ms when idle), runs the match query for each connected tag group: ready jobs whose `runs_on` is covered by the worker's tags, rate-bucket check, per-group ceiling check (AD-22: skip jobs whose `group_key` already has `max_concurrent_per_group` RUNNING), ordered by `(priority, fair_share(group_key), run_at)`, `FOR UPDATE SKIP LOCKED LIMIT slots`.
3. Lease written (`lease_expires_at = now() + lease_ttl`), job streamed to worker. **The lease row in Postgres is the delivery truth; the stream is only transport** — a dropped stream is a non-event (the lease expires, the sweeper requeues; the worker reconnects and re-announces).
4. Worker heartbeats (`extend lease`) on an interval « lease_ttl; the async shell heartbeats even while a process-pool job crunches.
5. `Complete(result)` / `Fail(error, retryable)` finalizes.

### 7.4 The apply/store problem: slow writes must not sit behind fast compute

Pattern observed today: the expensive compute (LLM call) is followed by a *store* step (`apply_summary`, `apply_graph`) that can itself be very slow — Qdrant upserts, graph writes into Postgres, big batch transactions. If store runs inside the same job as compute, a slow write holds an llm-worker slot hostage and re-couples the two failure domains we just separated. Design rules:

**Rule 1 — compute and apply are separate jobs by default.** `summarize_chunk` (LLM, `runs_on=["llm"]`) and `apply_chunk_summary` (DB write, `runs_on=["cpu"]`) are different task types on different worker classes with different concurrency, timeouts, and retry policies. A Qdrant slowdown then throttles *apply* workers only — LLM workers keep claiming compute jobs at full speed, and finished results wait safely as staged references (AD-13/AD-14). Nothing expensive is ever queued behind a slow database.

**Rule 2 — the split has two shapes, chosen per stage by whether the write must be atomic:**

| Write shape | When | How |
|---|---|---|
| **Independent applies** | Each element's write stands alone — chunk summaries, chunk embeddings (idempotent upserts keyed per element) | Fan out N `apply_*` jobs exactly like compute jobs. Applies parallelize and retry per element too — a failed Qdrant write for chunk 178 re-runs alone. |
| **Single transactional apply** | The write must be all-or-nothing — `apply_graph` merging one document's nodes/edges consistently | All compute jobs gate into ONE `apply_graph` job carrying the staged refs. It runs with `profile="io"`, a long `timeout_s`, heartbeats, and per-document `dedup_key` (re-run safe). Batches *within* the transaction are the app's concern; keeping one slow apply from blocking anything else is the engine's (it's just one job on the `cpu` queue). |

Chunk summarization is the first shape (per your point — fully independent); graph apply is the second. The choice is per task type, made in the app, expressed with the same two primitives (`fan_out` vs. gate-into-one) — no new engine machinery.

**Rule 3 — applies are cheap to retry, so make them aggressively retryable.** An apply job's payload is only references to staged data; `max_attempts` can be high and backoff long without ever re-paying compute. This is why AD-14 splits checkpoint (compute output) from store: the expensive thing is bought once, the cheap thing retries forever.

### 7.5 Web UI (first-class deliverable, NOT a flat job list)

The UI is a core deliverable (F20) — operationally we live in it during a 10k-doc onboarding. A bare-bones job table with retry buttons is explicitly **not** the bar. The four non-negotiable views, plus supporting ones:

| # | View | Contents |
|---|---|---|
| 1 | **Running now** | Every job currently executing: task_name, pipeline/stage, worker, ctx_id, elapsed vs. `timeout_s` (progress bar), heartbeat age, attempt number. Live-updating. Filter by pipeline, stage, task_name, tag, tenant, ctx_id. |
| 2 | **Failed** | Failed and DEAD jobs with the full ledger from `job_events` (every attempt: when started, which worker, what error, what backoff was applied); one-click retry / retry-all-matching / cancel; DLQ management. |
| 3 | **Dependencies** | The dependency/chain/gate graph, per ctx_id or per group_key — every job of "document X" as a live graph: which nodes succeeded, which are running, which are blocked and on what. The reconciliation view for the backend's track_id. |
| 4 | **The queue (waiting to run)** | Everything not yet running and *why*, split by reason: ready-to-claim (queue depth per tag + oldest-wait age), **blocked on dependencies** (with which deps missing), **blocked on rate bucket** (which class, tokens remaining, projected wait), scheduled for later (`run_at`), waiting for a retry backoff to expire. |
| 5 | Fleet | Connected workers: tags, slots used/free, claim rate, last seen; dead-worker detection. |
| 6 | Timeline (per job) | The `job_events` ledger rendered as a timeline — submitted, deps met, claimed, started, checkpointed, failed, retried, completed — like a payment-transaction history. |
| 7 | Dashboards | Per pipeline/stage/task_name: jobs/min, p50/p99 duration, retry rate, rate-bucket utilization, queue-age heatmap. |

Implementation: the SPA is a separate container (built from `frontend/`) that talks to the engine's HTTP query API; real-time via SSE/WebSocket fed from an in-process fan-out of `job_events` inserts (multi-engine UIs converge within a second via the ledger). Views 1-4 are the v1 acceptance criteria for the UI — the engine is not "done" without them.

---

## 8. Data model

Engine's own Postgres (18+). Typed columns, not blobs — this is what makes F13 (visibility) real.

> **The authoritative DDL lives in `symba_implementation.md` (rev 7).** This section describes the *concepts*; the implementation doc owns every column, index, and migration file. The two used to duplicate the full schema and had already drifted — one owner now.

| Table | Role | Key design points |
|---|---|---|
| `jobs` | The **hot** table: only live jobs (submitted/queued/running/waiting) | `uuidv7()` PKs (time-ordered, no B-tree bloat); partial indexes per state; aggressive per-table autovacuum; `lease_token` guards every mutation |
| `jobs_archive` | Terminal jobs (succeeded/dead/cancelled), moved here **in the terminal transaction** | Same shape as `jobs`; partitioned monthly; retention = partition drop; DEAD rows exempt from pruning (DLQ contract). Keeps the hot table physically tiny — the #1 lesson from every production Postgres queue (MVCC bloat kills claim latency, not lock contention) |
| `job_events` | Immutable audit ledger (F21), one row per lifecycle event | Partitioned monthly; BRIN on `at`; written in the same transaction as the state change |
| `job_dependencies` | AD-12 static deps | Reverse index for the decrement-on-success pass |
| `job_checkpoints` | AD-14 durable checkpoint copy | Keyed by dedup identity; TTL safety net |
| `gates` | AD-8 fan-in counters | Transactional counter bump + `fired_at IS NULL` guard = exactly-once firing |
| `group_running` | AD-22 per-group running counts | Maintained transactionally on claim/finish; joined by the claim query instead of a per-candidate `count(*)` |
| `signals` | AD-23 durable rendezvous | Signal-before-wait persists; wait-before-signal parks |
| `workers`, `cron_schedules`, `rate_classes` | Fleet registry, cron, bucket configs | |

Cross-cutting rules (unchanged from rev 6):
- **`error_history` is append-only**; a failed job is a row you can read, diff, and resubmit — not a log line.
- **Large results never go in `result`** — workers write to shared storage (MinIO/S3) or the client's own tables and pass a reference. The engine moves *coordination*, not data.
- Gate firing and dependency readiness both use transactional counter bumps — no child scans, no double-fire.
- **`job_events` is written in the same transaction as the state change** — the ledger can never disagree with the job row. The `jobs`/`jobs_archive` row is the *current* state; `job_events` is the *history*.

<details>
<summary>Superseded rev-6 indicative DDL (kept for review archaeology; do not implement from this)</summary>

```sql
CREATE TABLE jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name         TEXT NOT NULL,              -- unique task identity (AD-15), e.g. 'summarize_chunk'
    pipeline          TEXT,                       -- optional grouping label, e.g. 'ingestion'
    stage             TEXT,                       -- optional grouping label, e.g. 'summarization'
    tenant            TEXT NOT NULL DEFAULT 'default',
    ctx_id            TEXT,                       -- client correlation/track id (AD-17)
    state             TEXT NOT NULL DEFAULT 'submitted',
                      -- submitted|queued|running|waiting|succeeded|dead|cancelled
    priority          SMALLINT NOT NULL DEFAULT 0, -- higher claims first (F6)
    group_key         TEXT,                       -- fairness unit, e.g. document_id
    max_concurrent_per_group SMALLINT,            -- AD-22: cap RUNNING per group_key (NULL = uncapped)
    wait_key          TEXT,                       -- AD-23: set while state='waiting'
    wait_expires_at   TIMESTAMPTZ,                -- AD-23: wait timeout
    dedup_key         TEXT,                       -- idempotent submit + checkpoint identity
    runs_on           TEXT[] NOT NULL DEFAULT '{}', -- worker tags this job needs, e.g. {gpu}, {llm}
    rate_class        TEXT,
    payload           JSONB NOT NULL,             -- references only, small
    result            JSONB,                      -- inline result, size-capped (AD-13)
    -- orchestration
    parent_gate_id    UUID REFERENCES gates(id),
    on_success        TEXT,                       -- immediate next task in the chain (AD-19)
    chain_tail        TEXT[] NOT NULL DEFAULT '{}', -- remainder of the chain after on_success (AD-19)
    on_failure        JSONB,                      -- failure-hook submit spec (AD-20c)
    remaining_deps    INT NOT NULL DEFAULT 0,     -- AD-12 readiness counter
    -- execution/retry
    attempt           SMALLINT NOT NULL DEFAULT 0,
    max_attempts      SMALLINT NOT NULL DEFAULT 5,
    backoff           JSONB,                      -- {base_s, factor, max_s, jitter}
    timeout_s         INT NOT NULL DEFAULT 600,   -- hard execution ceiling (F19)
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_ttl_s       INT NOT NULL DEFAULT 300,
    claimed_by        TEXT,
    lease_expires_at  TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    -- audit
    error_history     JSONB NOT NULL DEFAULT '[]',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    UNIQUE (tenant, dedup_key)
);
CREATE INDEX ix_jobs_claim ON jobs (state, run_at, priority DESC)
    WHERE state = 'queued';                        -- partial index = hot path
CREATE INDEX ix_jobs_gate  ON jobs (parent_gate_id) WHERE parent_gate_id IS NOT NULL;
CREATE INDEX ix_jobs_group ON jobs (group_key);
CREATE INDEX ix_jobs_ctx   ON jobs (ctx_id) WHERE ctx_id IS NOT NULL;
CREATE INDEX ix_jobs_name  ON jobs (task_name);
CREATE INDEX ix_jobs_pipe  ON jobs (pipeline, stage);   -- UI grouping filters

-- Immutable audit trail (F21): one row per lifecycle event, never updated,
-- never deleted inside retention. Reconstructs any job's history like a
-- payment transaction ledger.
CREATE TABLE job_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      UUID NOT NULL,
    ctx_id      TEXT,
    event       TEXT NOT NULL,
                -- submitted|deps_met|queued|claimed|heartbeat_missed|started|
                -- checkpointed|completed|failed|retried|timed_out|lease_reclaimed|
                -- cancelled|dead_lettered|gate_fired|waiting|signaled|wait_timed_out
    at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker_id   TEXT,                   -- who, when relevant
    attempt     SMALLINT,
    detail      JSONB                   -- error message, backoff applied, etc.
) PARTITION BY RANGE (at);              -- monthly partitions, retention by dropping
CREATE INDEX ix_events_job ON job_events (job_id, at);
CREATE INDEX ix_events_ctx ON job_events (ctx_id, at) WHERE ctx_id IS NOT NULL;

-- AD-12: static dependencies. On each dependency success, decrement
-- remaining_deps on dependents; at 0 the job flips submitted -> queued + NOTIFY.
CREATE TABLE job_dependencies (
    job_id            UUID NOT NULL REFERENCES jobs(id),
    depends_on_job_id UUID NOT NULL REFERENCES jobs(id),
    PRIMARY KEY (job_id, depends_on_job_id)
);
CREATE INDEX ix_deps_reverse ON job_dependencies (depends_on_job_id);

-- AD-14: durable copy of intra-job checkpoints (Redis is the fast path).
-- Keyed by dedup identity so retries and duplicate submits see the same checkpoint.
CREATE TABLE job_checkpoints (
    tenant       TEXT NOT NULL,
    dedup_key    TEXT NOT NULL,
    data         JSONB NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,             -- TTL safety net (default 7d)
    PRIMARY KEY (tenant, dedup_key)
);

CREATE TABLE gates (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant            TEXT NOT NULL,
    policy            TEXT NOT NULL DEFAULT 'all_success',
    continuation_type TEXT NOT NULL,
    continuation_payload JSONB NOT NULL DEFAULT '{}',
    expected_children INT NOT NULL,
    terminal_children INT NOT NULL DEFAULT 0,      -- counter, bumped transactionally
    fired_at          TIMESTAMPTZ
);

CREATE TABLE workers (
    id                TEXT PRIMARY KEY,             -- 'spark-01/parse'
    tags              TEXT[] NOT NULL,              -- what kind of worker I am
    slots             INT NOT NULL,
    last_seen_at      TIMESTAMPTZ NOT NULL,
    meta              JSONB                          -- host, version, gpu info
);

CREATE TABLE cron_schedules (
    id TEXT PRIMARY KEY, job_type TEXT NOT NULL, cron TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}', enabled BOOL NOT NULL DEFAULT true,
    last_fired_at TIMESTAMPTZ
);

-- AD-23: durable external events. A signal arriving before its wait is
-- registered persists here; the wait then returns immediately (rendezvous).
CREATE TABLE signals (
    tenant       TEXT NOT NULL,
    wait_key     TEXT NOT NULL,
    payload      JSONB,
    signaled_by  TEXT,                             -- audit: who fired it
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_by  UUID,                             -- job that consumed it
    consumed_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant, wait_key, created_at)
);
CREATE INDEX ix_signals_pending ON signals (tenant, wait_key) WHERE consumed_at IS NULL;
CREATE INDEX ix_jobs_waiting ON jobs (wait_key) WHERE state = 'waiting';
```

</details>

---

## 9. Worker model

### 9.1 Worker classes (deployment reality)

| Class | Hardware | Tags (what I am) | Execution mode |
|---|---|---|---|
| parse-worker | 4x NVIDIA Spark boxes | `parse`, `gpu`, `ocr` | subprocess (owns GPU context, warm docling/vLLM) |
| embed-worker | GPU node **or** none (client-dependent) | `embed_local` or `embed_api` | subprocess (GPU) / asyncio (API) |
| llm-worker | cheap CPU/API nodes | `llm` | asyncio, high slot count (50-200) |
| connector-worker | app-adjacent nodes | `gdrive`, `sharepoint` | asyncio |
| generic-worker | anywhere | `cpu` (dedup, classify, chunk) | asyncio + small process pool |

The *same logical stage* (embed) maps to different tags per client deployment — this is configuration in job submission, not engine logic.

### 9.2 Worker SDK shape

Design goal (F14): **intuitive first**. Sane defaults for everything via profiles (AD-16); a working task is a decorator and a function.

```python
from symba import Worker

worker = Worker(
    engine="grpcs://symba.internal:7233",
    tags=["llm"],                   # what kind of worker I am
    slots=100,                      # asyncio concurrency
)

# profile="io" is the default -- LLM/API/DB tasks need nothing extra
@worker.task("summarize_chunk")                           # task_name: unique (AD-15)
async def summarize_chunk(ctx, payload):
    if ctx.checkpoint_data:                               # AD-14: resume after retry
        llm_out_ref = ctx.checkpoint_data["llm_out_ref"]
    else:
        chunk = await fetch_chunk(payload["chunk_ref"])
        result = await llm_gateway.ainvoke(...)           # expensive
        llm_out_ref = await stage_result(payload["staging_ref"], result)
        await ctx.checkpoint({"llm_out_ref": llm_out_ref})
    return {"staging_ref": llm_out_ref}                   # store happens in apply job (Sec. 7.4)

# heavy compute: profile picks subprocess execution; long lease + timeout
@worker.task("parse_doc", profile="gpu",
             timeout_s=3600, lease_ttl_s=300, max_attempts=3)
def parse_document(ctx, payload):                         # sync, in subprocess
    ...                                                    # SDK shell heartbeats
```

The `ctx` object (everything a handler can need, nothing global):

| Member | Purpose |
|---|---|
| `ctx.ctx_id` | Correlation id (backend track_id), auto-propagated (AD-17) |
| `ctx.job_id`, `ctx.attempt`, `ctx.task_name` | Identity of this execution |
| `ctx.output` | Upstream results keyed by task_name/alias — immediate predecessor + `depends_on` shipped inline with the claim; deeper ancestors lazy-fetched via `GetResult` and memoized (AD-13); typed when the producer declared an `output_schema` (AD-15) |
| `ctx.idempotency_key` | Deterministic per-logical-job key for external side effects (AD-21) |
| `ctx.wait_for_event(key, timeout_s)` / `ctx.event_payload` | Suspend until `engine.signal(key, ...)` — human approval / webhook resume; payload of the consumed signal, `None` on timeout (AD-23) |
| `ctx.checkpoint(data)` / `ctx.checkpoint_data` | Persist / resume expensive intermediate output (AD-14) |
| `ctx.heartbeat()` | Manual lease extension for tight loops (usually automatic) |
| `ctx.submit(...)`, `ctx.submit_children(...)` | Enqueue follow-up / child jobs from inside a handler |
| `ctx.logger` | Structured logger pre-bound with job_id, ctx_id, task_name |

- Registration can set per-task defaults for `runs_on`, `rate_class`, `timeout_s`, `max_attempts`, backoff — submit-time values override them. In practice `runs_on` almost always belongs at registration (the task knows what hardware it needs); the submit side then never repeats it.
- Middleware hooks: metrics, tracing, structured logging. (PII filtering: explicitly out of scope for v1.)
- The process manager restarts dead subprocesses and drains asyncio tasks on SIGTERM (graceful shutdown = stop claiming, finish leases).
- Repo layout per AD-18: this SDK lives in `symba-sdk-python`, separate from the engine server repo.

---

## 10. Failure model

| Failure | Detection | Recovery |
|---|---|---|
| Worker crash mid-job | Lease expiry (no heartbeat) | Sweeper requeues with attempt+1; at-least-once means the handler may have partially run — idempotent writes absorb it |
| Worker network partition | Same as crash | If the worker finishes and reconnects after lease loss, its `Complete` is rejected (stale lease token) — the retry wins, dedup keys prevent double side effects |
| Engine server crash | Workers' streams drop; they reconnect to another engine instance | State is in Postgres; nothing lost |
| Redis loss | Bucket/checkpoint cache gone | Dispatch unaffected (never depended on Redis); buckets fall back to Postgres counters; checkpoints read the Postgres write-behind copy; degraded bucket latency, full correctness |
| Postgres loss | Everything halts | By design — Postgres is the truth; HA via standard PG replication |
| LLM provider rate storm | 429s / bucket exhaustion | Bucket empties, claims for that rate_class pause engine-wide; jobs wait QUEUED, no failure churn |
| Poison job | attempts exhausted | DEAD state with full error_history; DLQ view in UI; manual or rule-based resubmit |
| Gate never fires (child stuck) | Gate age metric + stuck-child alert | Cancel/resubmit child from UI; gate policies allow `all_terminal` for lenient continuations |
| Store fails after LLM call succeeded | Job fails retryable, checkpoint exists | Retry loads `ctx.checkpoint_data` (Redis, Postgres fallback) and skips the LLM call — the response is never re-bought (AD-14) |
| Redis lost between checkpoint and retry | Redis miss on checkpoint load | SDK falls back to the Postgres write-behind copy; at worst the final unflushed seconds of checkpoints are re-done |
| Dependency job dies | Dependency reaches DEAD/CANCELLED | All dependents are cancelled transitively (AD-12) — no job ever runs against missing inputs |
| Job exceeds `timeout_s` | Worker-side timer (engine backstop) | Attempt failed retryable, slot reclaimed immediately; distinguishes slow-but-alive (heartbeating, allowed) from truly overrunning |
| Signal never arrives for a WAITING job | `wait_expires_at` passes (sweeper) | Job resumes with `ctx.event_payload = None`; the handler decides (proceed / fail / escalate) — no job waits forever (AD-23) |
| Signal fired before the wait registers | Durable `signals` row exists | `wait_for_event` finds the pending row and returns immediately — rendezvous, no race, no lost approval (AD-23) |

---

## 11. Usage examples

### 11.0 Where this code lives: one module per flow

Answering "do I submit tasks randomly or make one `ingestion_use_case` file?": **one file (or package) per flow, containing both the handlers and the submit functions for that flow.** Not scattered, not one giant registry. The convention:

```
flows/
├── __init__.py            # worker assembly: imports flow modules, registers on Worker
├── ingestion.py           # ingestion flow: all @task handlers + start_ingestion() submit fn
├── gdrive_sync.py         # connector flow: scan/check/ingest handlers + trigger fn
└── evals.py               # eval flow
```

Three rules make this work:

1. **Handlers and their submit function live together.** The person reading `ingestion.py` sees the whole flow — what each task does *and* how the flow is wired (chain, depends_on, fan_out) — in one file. The wiring IS documentation; splitting it from the handlers is how flows become archaeology.
2. **Each flow module exposes one entry function** (`start_ingestion(doc_id, ctx_id)`) that the rest of the app calls. Routers/schedulers never call `engine.submit` with raw task names — they call the flow's entry function. One place to change the wiring.
3. **The worker process is just an assembly file** — it imports the flow modules (which registers their `@worker.task` handlers) and runs. Which flows a worker image imports determines what it can execute; combined with tags, that's the whole deployment story. A task used by two flows (e.g. `embed_dense` in ingestion and re-sync) lives in the flow that owns it and is imported by the other — `task_name` stays unique (AD-15).

**Not everything needs a ceremony function.** For a genuine one-shot task with no wiring, call `engine.submit(task="x", payload={...}, ctx_id=...)` inline wherever you are — that is the complete API. The entry-function convention exists for *flows* (anything with a chain, `depends_on`, or fan-out worth naming); do not wrap single submits in functions just for the ritual.

This mirrors what already exists: `flows/ingestion.py` replaces the trio of workflow JSON + task file + service wrapper per stage with one readable module.

### 11.1 The parse stage, fully worked (chain + data passing + failure hook)

The real `parse_workflow.json` is 4 tasks: `download_source -> parse_content -> persist_parsed_output -> complete_parse_stage`, with per-task retry counts, a 1500 s parse timeout, output plumbing (`${parse_ref.output.text}` into persist), and a `parse_failure_workflow` that marks the stage failed. The same flow in Symba, complete, in `flows/ingestion.py`:

```python
from symba import Engine, Worker

engine = Engine("grpcs://symba.internal:7233", tenant="acme")
worker = Worker(engine="grpcs://symba.internal:7233", tags=["parse", "gpu"], slots=4)

# --- handlers -------------------------------------------------------------

@worker.task("download_source", runs_on=["parse", "gpu"], timeout_s=300)
async def download_source(ctx, payload):              # runs on the Spark box so the
    path = await minio.download(payload["document_id"])   # file is local for parse
    return {"source_path": path}

@worker.task("parse_content", profile="gpu", runs_on=["parse", "gpu"],
             timeout_s=1500, max_attempts=2)
def parse_content(ctx, payload):                      # sync, subprocess, warm GPU
    downloaded = ctx.output["download_source"]       # upstream result by task name (AD-13)
    out = parser.parse(downloaded["source_path"], options=payload.get("parser_options"))
    ref = stage_parsed_output(payload["document_id"], out)   # big text -> MinIO/staging
    return {"output_ref": ref, "total_pages": out.pages, "language": out.language}

@worker.task("persist_parsed", timeout_s=300)
async def persist_parsed(ctx, payload):
    parsed = ctx.output["parse_content"]             # upstream result by task name
    await service.persist_parse_metadata(payload["document_id"], parsed)
    await service.set_document_status(payload["document_id"], "PARSED")
    return {"output_ref": parsed["output_ref"]}

@worker.task("mark_stage_failed")                     # the failure hook (AD-20c)
async def mark_stage_failed(ctx, payload):
    await service.mark_stage_failed(payload["document_id"],
                                    stage=payload["stage"],
                                    reason=payload["failed"]["error"])

# --- the flow entry function (what the app calls) --------------------------

async def start_parse(doc_id: str, ctx_id: str, parser_options: dict | None = None):
    await engine.submit(
        task="download_source",
        chain=["parse_content", "persist_parsed"],    # declared ONCE, here (AD-19)
        pipeline="ingestion", stage="parsing",        # grouping labels (AD-15)
        payload={"document_id": doc_id, "parser_options": parser_options},
        # runs_on comes from each task's registration defaults: download/parse
        # carry ["parse","gpu"] (Spark boxes only); persist_parsed has none (any worker)
        group_key=doc_id,
        dedup_key=f"parse:{doc_id}",
        ctx_id=ctx_id,
        on_failure={"task": "mark_stage_failed",      # replaces parse_failure_workflow
                    "payload": {"document_id": doc_id, "stage": "parse"}},
    )
```

What replaced what:

| Old orchestrator artifact | Symba equivalent |
|---|---|
| `parse_workflow.json` (136 lines) | the `chain=[...]` line in `start_parse` |
| `${download_ref.output.source_path}` plumbing | `ctx.output["download_source"]` — upstream results injected by task name automatically (AD-13); the original submit payload rides along unchanged |
| per-task `retryCount` / `timeoutSeconds` | per-task `max_attempts` / `timeout_s` at registration |
| `parse_failure_workflow.json` + `mark_stage_failed` task file | `on_failure=` parameter + one ordinary handler |
| `complete_parse_stage` (spine fan-out task) | gone — the chain itself is the progression; document status updates stay in the app (`persist_parsed` sets `PARSED`) |
| `start_pipeline_stage` / `publish_sse_event` bookkeeping tasks | gone / an event-bus call inside handlers where the product needs it |

### 11.2 Conditional flow: the dedup stage (AD-20a)

Today `dedup_workflow` either terminates (duplicate) or lets the poller fan out to parse. In Symba the whole decision is the handler shown in AD-20: `dedup_doc` returns `ctx.stop_chain()` on a duplicate, otherwise the chain proceeds into `download_source`. Submit-side:

```python
await engine.submit(
    task="dedup_doc",
    chain=["download_source", "parse_content", "persist_parsed"],
    pipeline="ingestion",
    payload={"document_id": doc_id},
    group_key=doc_id, dedup_key=f"ingest:{doc_id}:{content_hash}",
    ctx_id=ctx_id,
)
```

No SWITCH task, no `evaluatorType: javascript` expression, no duplicated completion branches — one `if` in Python.

### 11.3 Document summarization (the fan-out case)

```python
# after chunking: fan out per-chunk summaries, gate into executive summary
gate = await engine.fan_out(
    ctx_id=track_id,                                 # backend correlation id (AD-17)
    pipeline="ingestion", stage="summarization",     # grouping labels (AD-15)
    children=[
        {"task": "summarize_chunk", "runs_on": ["llm"],
         "rate_class": "azure-gpt5", "group_key": doc_id, "priority": 0,
         "payload": {"chunk_ref": c.id, "staging_ref": f"{doc_id}:{c.id}"},
         "dedup_key": f"summ:{doc_id}:{c.id}"}
        for c in chunks
    ],
    gate_policy="all_success",
    continuation={"task": "executive_summary",
                  "runs_on": ["llm"], "rate_class": "azure-gpt5",
                  "payload": {"document_id": doc_id}},
)
```

200 chunks become 200 individually visible, retryable jobs, claimed by however many llm-workers exist, throttled by the shared `azure-gpt5` bucket. Chunk 178 failing on a 429 retries *alone* with backoff; 199 others are untouched. When all succeed, `executive_summary` fires automatically. The whole tree is one query away: `engine.query(ctx_id=track_id)` or the UI's pipeline view.

Note the summarize idempotency: today `summarize_workflow` needs `check_document_status` + a SWITCH + duplicated branches. Here it's AD-20b — the first line of `summarize_chunk`'s handler is `if already_summarized: return ctx.skip()`.

### 11.4 The full pipeline as chained stage entries

The whole ingestion succession, declared once (`document_pipeline_stages` spine replaced by the chain):

```python
INGESTION = ["dedup_doc", "parse_doc", "classify_doc", "chunk_doc",
             "embed_doc", "summarize_doc", "tag_doc",
             "extract_graph", "apply_graph"]

await engine.submit(
    task=INGESTION[0],
    pipeline="ingestion",        # grouping label, inherited by the whole chain
    ctx_id=track_id,
    payload={"document_id": doc_id},
    chain=INGESTION[1:],         # linked list: each job carries only its tail (AD-19)
    group_key=doc_id, dedup_key=f"ingest:{doc_id}:{content_hash}",
    priority=1,                  # e.g. interactive upload outranks backfill (0)
)
```

Two composition patterns keep this readable as stages get internally complex:
- A chain entry can itself be a *stage entry task* whose handler fans out sub-work: `summarize_doc` is a tiny handler that calls `engine.fan_out(...)` (11.3) and lets the gate's continuation resume the chain tail. Chains stay linear at the document level; the explosion happens inside stages.
- Stages with static joins (embed: dense ∥ sparse -> store, next example) submit their `depends_on` cluster from the stage entry handler, with the store job carrying the chain tail forward.

The app queries progress with `engine.query(ctx_id=track_id)` or `engine.query(pipeline="ingestion", stage="parsing")`.

### 11.5 Embeddings: dense ∥ sparse -> store (`depends_on`, AD-12)

```python
dense  = await engine.submit(task="embed_dense", pipeline="ingestion",
                             ctx_id=track_id, runs_on=["embed_api"],
                             payload={"chunk_refs": refs})
sparse = await engine.submit(task="embed_sparse", pipeline="ingestion",
                             ctx_id=track_id, runs_on=["cpu"],
                             payload={"chunk_refs": refs})

await engine.submit(task="store_embeddings", pipeline="ingestion",
                    ctx_id=track_id, runs_on=["cpu"],
                    depends_on=[dense.id, sparse.id],   # runs only when both succeed
                    payload={"document_id": doc_id})
```

Dense and sparse run **in parallel on different worker classes**; `store` stays `SUBMITTED` until both succeed, then receives their results via `ctx.output["embed_dense"]` / `ctx.output["embed_sparse"]` (references to the staged vectors) and writes to Qdrant + Postgres. Same shape covers `extract_graph -> apply_graph`. If either embedding job dies, `store` is cancelled — never a half-written point. This replaces `embed_workflow.json`'s only-fork-in-the-system (`FORK_JOIN` dense/sparse + `JOIN` + store, ~240 lines of JSON) with three submits.

### 11.6 LLM output persistence: never pay for the same call twice (AD-14)

The `summarize_chunk` handler in Section 9.2 shows the full pattern. Timeline of the failure case:

1. Attempt 1: LLM call succeeds (cost incurred) -> `ctx.checkpoint({"llm_out_ref": ...})` (Redis now, Postgres write-behind) -> Qdrant write fails -> job fails retryable.
2. Attempt 2 (after backoff): SDK loads the checkpoint by dedup identity -> `ctx.checkpoint_data` is set -> handler **skips the LLM call** -> retries only the store.
3. Success: checkpoint deleted (Redis immediately, Postgres via sweeper). If cleanup were ever missed, `expires_at` reaps it.

Duplicate submits of the same logical job (same `dedup_key`) hit the same checkpoint — idempotent by construction. Teams that prefer structure over checkpoints use pattern (a): split into `summarize_chunk` -> `apply_chunk_summary` with `depends_on` (Section 7.4).

### 11.7 GDrive re-sync

```python
# cron: engine fires 'gdrive_scan' hourly per connection (pipeline="gdrive_sync")
@worker.task("gdrive_scan")
async def scan(ctx, payload):
    cursor = (ctx.checkpoint_data or payload)["cursor"]      # resume mid-crawl
    async for page in gdrive.list_changes(cursor):
        await ctx.submit_children([
            {"task": "gdrive_check_file",
             "runs_on": ["gdrive"],
             "priority": 5 if payload["interactive"] else 0,
             "dedup_key": f"gd:{f.id}:{f.md5}",       # unchanged files dedup away
             "payload": {"file_id": f.id}}
            for f in page.files
        ])
        await ctx.checkpoint({"cursor": page.next_cursor})   # crawl survives restarts
```

Unchanged files collapse via `dedup_key`; changed ones submit ingestion chains (11.4). Interactive re-syncs outrank background backfills via priority.

### 11.8 Parsing on the Spark boxes

Nothing special in the client — the job says `runs_on=["parse","gpu"]` (see the full `start_parse` in 11.1); only the four Spark workers carry those tags; they pull at their own pace. A dead Spark box just means the other three claim its share (Sparrow late binding). Zero placement config in the engine.

### 11.9 Human approval mid-flow (AD-23) + serialized webhook processing (AD-22)

```python
# An integration flow: a CRM update that needs human sign-off above a threshold
@worker.task("apply_crm_update", max_concurrent_per_group=1)   # one at a time per ticket
async def apply_crm_update(ctx, payload):
    # AD-23 re-entry contract: everything before the wait must be checkpointed or
    # idempotent -- on resume this handler RE-RUNS FROM THE TOP on some worker.
    if ctx.checkpoint_data is None:
        draft = await llm_draft_update(payload)                 # expensive
        await ctx.checkpoint({"draft": draft})
    draft = (ctx.checkpoint_data or {"draft": draft})["draft"]

    if payload["amount"] > 10_000:
        approval = await ctx.wait_for_event(                    # WAITING: slot released
            key=f"approve:{payload['ticket_id']}", timeout_s=3 * 86400)
        # on resume, the handler re-ran to here; the consumed signal is returned
        # immediately -- no re-park, no special "am I resuming?" branch needed
        if approval is None or not approval["approved"]:        # timeout or rejection
            return ctx.stop_chain(result={"applied": False, "reason": "not approved"})
    await crm.apply(draft, idempotency_key=ctx.idempotency_key)    # AD-21: retry-safe
    return {"applied": True}

# Elsewhere -- the app's approval endpoint (or another job) resumes it:
await engine.signal(f"approve:{ticket_id}", {"approved": True}, signaled_by=user.email)
```

Three market gaps in ten lines: the approval is a real job state (visible in the UI as *waiting, 2d*, audited in `job_events` with who signaled), a webhook storm for the same ticket processes strictly one-at-a-time in order (no app-side locking), and a retry after a network blip cannot double-apply the CRM update because the idempotency key is stable across attempts.

---

## 12. Migration strategy

Strangler, per the earlier decision — the old workflow orchestrator and the new engine coexist; stages move one at a time; each move is independently reversible.

| Phase | What moves | Why this order |
|---|---|---|
| 0 | Engine core: jobs table, claim path, worker SDK (async only), UI-minimum (job list/query) | Foundation; no production traffic |
| 1 | **Summarize stage** (chunk fan-out + gate) | Highest pain, purest I/O fan-out, `llm_results_staging` already makes handlers idempotent. `PipelineStagePoller` submits to the engine instead of launching `summarize_workflow` |
| 2 | **Graph extract** (same shape) + **tag** | Reuses everything Phase 1 built |
| 3 | Rate-limit classes + priorities/fairness | Now measurable against real LLM traffic |
| 4 | **Parse** on dedicated hardware (process execution, long leases, remote workers) | Proves the heterogeneous-placement story |
| 5 | Embed, classify, chunk, dedup; chains replace the stage spine | The poller shrinks to an admission valve |
| 6 | Connectors (scan/check/ingest jobs, cron, checkpoints) | Retires `connectors_worker.py` |
| 7 | Old orchestrator decommission | Delete workflow JSONs, the orchestrator client module, the JVM container |

Guardrails: every phase keeps the old orchestrator path deployable behind a flag (`pipeline_stages.<stage>_engine=true|false`); per-stage output parity checks (staging rows identical across paths) before flipping defaults.

---

## 13. Open questions

Deliberately deferred — none block the design, all block v1 code:

1. ~~**Engine HA semantics for cron**~~ **Decided (rev 8, informed by prior-art dedup-upsert cron patterns):** correctness comes from deterministic dedup keys (`cron:{schedule_id}:{tick}`) through the ordinary unique index — N instances can all fire and duplicates collapse. The advisory-lock election is kept only to avoid redundant work, and is not load-bearing. No leader election.
2. **Result reference contract** — standardize on URI scheme (`pg://`, `s3://`, `inline://`) or leave opaque to the engine. Leaning opaque-with-size-cap.
3. **Tenant model depth for open source** — row-level `tenant` column (current design) vs. database-per-tenant. Row-level for v1.
4. **Web UI stack** — embed (simple static app served by the engine) vs. separate frontend. Embed for v1.
5. **Backpressure on submit** — hard queue-depth cap per tenant vs. unbounded with alerting. Needs a load test to decide.
6. ~~**Name.**~~ **Decided: Symba.** Repos: `symba` (engine server) and `symba-sdk-python` (SDK). PyPI package `symba-sdk`, SDK import `from symba import Engine, Worker`.

---

## 14. References

- Lightweight Redis-backed task-queue implementations — claim/retry loop and ZSET-based enqueue patterns; commit-level lessons mined rev 8: claim revalidation under concurrent retries, worker-loop containment on handler exceptions, slot-accounting drift, drain bookkeeping on shutdown, atomic status transitions, cancel-in-any-state handling, cron dedup-on-upsert
- Postgres-backed task-queue implementations — `SKIP LOCKED` claim + sweep pattern, schema/migration approach; lessons mined rev 8: advisory-lock removal from the hot path, sweeper race-condition scars, autocommit/NOTIFY hang failure modes, single-clock-source requirement, upsert-based cron
- Broker/receiver-style async task-queue architectures — broker/receiver/result-backend split, ack-timing evolution toward save-based acknowledgment, middleware hooks, process manager + readiness handshake, task-identity-across-retries bug class, GC task-set race-condition guards
- Ousterhout et al., *Sparrow: Distributed, Low Latency Scheduling*, SOSP 2013 — pull-based late binding
- Industry write-ups on Postgres LISTEN/NOTIFY scaling limits — the AccessExclusiveLock commit serialization that motivated dropping NOTIFY (AD-3, rev 7)
- Production rewrite retrospectives from Postgres-backed queue vendors — polling dispatcher, hot-table evacuation, identity/time-ordered PKs, buffered writes; the closest production-proven cousin of this architecture
- Postgres job-queue engineering write-ups (*Job Queues & Failure By MVCC*, *Keeping a Postgres queue healthy*) — why terminal rows must leave the hot table (jobs_archive, rev 7)
- Partitioning/archive-table patterns from several Postgres-backed queue implementations — the survey behind the archive-on-terminal pattern
- Temporal docs — task queues as routing, activity heartbeats (concepts adopted; platform rejected)
- In-process graph-execution library docs — checkpointer API, explicit edge declarations, dynamic fan-out (concepts adopted; in-process library, not a distributed engine)
- Internal pipeline conventions and prior schema migrations for the document-pipeline spine and its idempotency/staging layer (source of the migration plan in Section 12)









