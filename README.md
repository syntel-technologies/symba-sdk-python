<p align="center">
  <img src="docs/assets/symba_readme_banner.svg" alt="Symba — a durable job execution engine" width="680">
</p>

<p align="center"><strong>The Python SDK for Symba — durable jobs with a decorator and a function.</strong></p>

<p align="center">
  <a href="https://github.com/syntel-technologies/symba-sdk-python/actions/workflows/ci.yml"><img src="https://github.com/syntel-technologies/symba-sdk-python/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/python-3.11%2B-blue.svg" alt="Python 3.11+"></a>
  <a href="https://github.com/syntel-technologies/symba"><img src="https://img.shields.io/badge/engine-Symba-336791.svg" alt="Symba engine"></a>
</p>

Write a worker as a decorated `async def`, submit jobs from anywhere, and get durable,
at-least-once execution — leases, retries, chains, fan-out/gates, human-in-the-loop signals,
checkpoints, and capability routing — all driven by the [Symba engine](https://github.com/syntel-technologies/symba).
The engine is **schema-blind and Postgres-backed**; this SDK is where your Python lives.

> **Status:** pre-1.0 (`0.1.0`). One package ships both halves: the client (`Engine`) you submit
> jobs from, and the worker (`Worker`) that runs them. The engine server + operator console live
> in the [separate engine repo](https://github.com/syntel-technologies/symba).

<p align="center">
  <a href="#-quickstart">Quickstart</a> •
  <a href="#-why-the-sdk">Why the SDK</a> •
  <a href="#-features">Features</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-install--extras">Install</a> •
  <a href="#-compatibility">Compatibility</a> •
  <a href="#-documentation">Docs</a>
</p>

---

## 🚀 Quickstart

A worker is a module with decorated handlers. It connects to the engine over gRPC and runs jobs:

```python
# app/worker.py
from symba import Worker

worker = Worker(engine="grpc://localhost:7233", tags=["general"], slots=50)

@worker.task("send_welcome_email")
async def send_welcome_email(ctx, payload):
    await email_client.send(to=payload["email"], template="welcome")
    return {"sent": True}

if __name__ == "__main__":
    worker.run()          # or: symba run app.worker:worker
```

Submit jobs from anywhere in your application:

```python
from symba import Engine

engine = Engine("grpc://localhost:7233", tenant="acme")
job = await engine.submit(task="send_welcome_email", payload={"email": "a@b.co"})
result = await job.result(timeout=30)      # {"sent": True}
```

Not in an async context? Use the [sync facade](#sync-facade--cli):

```python
from symba import SyncEngine

eng = SyncEngine("grpc://localhost:7233", tenant="acme")
print(eng.submit("send_welcome_email", {"email": "a@b.co"}).result(timeout=30))
```

You need a running engine on `:7233` — `docker compose up -d --wait` in the
[engine repo](https://github.com/syntel-technologies/symba) is the whole server. See
[`examples/`](examples/) for runnable end-to-end flows.

## 🤔 Why the SDK

The Symba engine is deliberately **schema-blind**: it routes, leases, retries, and durably
records jobs, but it never imports your code or knows what a job *does*. Everything that turns a
raw job row into a productive Python program — handler dispatch, `input_schema`/`output_schema`
validation, error classification, checkpoints, `wait_for_event`, chains and fan-out gates, the
`io`/`cpu`/`gpu` execution profiles — lives **here**, in the SDK.

That split is the point. You get a **thin, function-shaped surface** (`@worker.task` and
`engine.submit`) with none of the server's operational weight, and the SDK does the hard parts
you'd otherwise re-implement per project:

- **Slot accounting that never leaks** — one unconditional release point, drain-safe, property-tested.
- **Cooperative cancellation & timeouts** — heartbeat shell injects `CancelledError` on `io`,
  `SIGTERM→SIGKILL` on `cpu`/`gpu`.
- **Retry classification without HTTP imports** — transient OS/timeout/connection errors and
  `httpx`/`aiohttp` status codes are matched by qualified class name, so the SDK stays dependency-light.
  Any exception whose class is named `TimeoutError` (e.g. `sqlalchemy.exc.TimeoutError`, which does
  *not* subclass the builtin) is treated as retryable, regardless of its module.
- **Checkpoints that skip paid work on retry** — an optional Redis fast path in front of the
  engine's durable `PutCheckpoint`.
- **A real test engine** — `SymbaTest` runs your handlers through the *actual* dispatch pipeline
  in-memory: no gRPC, no Postgres, no Redis.

## ✨ Features

- 🎯 **Decorator-first workers** — `@worker.task("name")` registers an `async def (ctx, payload)`
  handler; boot validation catches signature mistakes, duplicate names, and profile/handler
  mismatches with did-you-mean hints before a single job runs.
- 🔀 **Runtime-decided graphs** — `chain=[...]`, `ctx.submit_children(..., on_complete=...)` +
  `Gate`, and `depends_on` are plain calls your handler makes at runtime, not a DAG authored up front.
- ✅ **Schemas, your way** — declare `input_schema`/`output_schema` as Pydantic models; the SDK
  validates payloads pre-handler and return values pre-Complete, and `ctx.output["task"]`
  deserializes upstream results back into their producer's typed model.
- 💾 **Checkpoints** — `await ctx.checkpoint({...})` persists expensive intermediate output so a
  retry resumes instead of re-paying for it. Optional Redis fast path via the `[redis]` extra.
- 👤 **Human-in-the-loop** — `await ctx.wait_for_event(key, timeout_s=...)` parks the job into
  `WAITING` (releasing its slot) and resumes it when someone calls `engine.signal(key, payload)`.
- 🧵 **Execution profiles** — `io` (asyncio, the default), `cpu` (a `forkserver` process pool),
  and `gpu` (one warm subprocess with `@worker.on_gpu_init` hooks and a crash circuit breaker) —
  each with its own timeout/lease defaults, all sharing one `Ctx` surface. A task's `lease_ttl_s`
  must be `>= timeout_s` (with heartbeat margin); io tasks inherit the engine default lease (~60s),
  so set `lease_ttl_s` explicitly on any io task expected to run longer than a few heartbeats —
  otherwise boot validation fails fast to stop a mid-run lease lapse from causing a duplicate dispatch.
- 🔁 **Retries & a real error taxonomy** — `RetryableError`/`FatalError`/`RateLimitedError` you
  raise, plus a full SDK exception hierarchy (`JobFailed`, `JobCancelled`, `StaleLease`, …) with
  `error_history` on dead jobs.
- 🧪 **`SymbaTest`** — the same dispatch pipeline in-memory, with `tick()`/`run_until_idle()`, a
  fake `clock`, `fail_next()`, and `checkpoints[job_id]` inspection. A shared conformance corpus
  runs identically against `SymbaTest` and a dockerized engine.
- 🖥️ **Sync facade + CLI** — `SyncEngine` for scripts/notebooks, and a `symba` command
  (`run`, `submit`, `job`, `cancel`, `signal`, `doctor`, …) for the shell.
- 📊 **Observability** — `structlog` logging identical to the engine's, plus an optional
  `MetricsMiddleware` exposing the four Prometheus metrics (jobs, duration, busy slots, loop lag).

## 🏗️ Architecture

The SDK talks to the engine over two gRPC surfaces — you use the **client** to submit and query,
your **worker** claims and runs:

```
   your app code                              your worker process
   ┌───────────────┐                          ┌─────────────────────────────┐
   │ Engine        │  control plane :7233     │ Worker (@worker.task)       │
   │  .submit()    │ ───────────────────────▶ │  Claim (bidi) ── dispatch    │
   │  .fan_out()   │  Submit / Query / Cancel  │  Heartbeat / Complete / Fail │
   │  .signal()    │  AwaitJob / Signal        │  Wait / Put|GetCheckpoint    │
   │  JobHandle    │                          │  io · cpu · gpu executors    │
   └───────────────┘                          └──────────────┬──────────────┘
            ▲                                                 │  ctx.checkpoint (optional)
            │                                                 ▼
            └──────────────── Symba engine ───────────▶ Postgres (state) · Redis (fast path)
                              (schema-blind: routes, leases, retries, records)
```

Everything the engine records is a queryable, retryable row keyed by `ctx_id`. The engine owns
durability; the SDK owns everything Python. See the engine README's
[architecture section](https://github.com/syntel-technologies/symba#-architecture) for the server side.

## 📦 Install & extras

The PyPI distribution is `syntel-symba`; Python imports remain `from symba import Worker`. The unrelated PyPI project named `symba` is not this SDK. Do not install both distributions into one environment because they share the import name.

The commands below apply after the first PyPI release. Until then, install from the reviewed repository checkout with `pip install .`.

```bash
pip install syntel-symba                 # client + worker, io profile, durable checkpoints
pip install "syntel-symba[redis]"        # + checkpoint fast path (Redis in front of PutCheckpoint)
pip install "syntel-symba[cli]"          # + the `symba` command (typer + rich)
pip install "syntel-symba[redis,cli]"    # everything
```

| Extra | Pulls in | Needed for |
|---|---|---|
| *(none)* | `grpcio`, `protobuf`, `pydantic`, `structlog`, `tenacity` | Submitting jobs, running `io`/`cpu`/`gpu` workers, durable checkpoints via the engine RPC. |
| `[redis]` | `redis` | **Only** the checkpoint fast path (spec §13.1). Without it, `ctx.checkpoint` falls back to the engine's durable `PutCheckpoint` — which always works. Enable per-worker with `SYMBA_CHECKPOINT_REDIS_URL`. |
| `[cli]` | `typer`, `rich` | **Only** the `symba` CLI. Library users importing `symba` never pull these. |

`cpu` and `gpu` profiles need no extra — they use the stdlib `multiprocessing` `forkserver`/`spawn`.
`MetricsMiddleware` activates automatically if `prometheus-client` is importable, and is a silent
no-op otherwise.

## 🧭 Deeper examples

### Enrichment pipeline — chain + fan-out/gate + schemas + checkpoint

```python
from pydantic import BaseModel
from symba import Worker, Ctx

worker = Worker(engine="grpc://localhost:7233", tags=["llm"], slots=100, strict_schemas=True)

class ParseInput(BaseModel):
    document_id: str
    staging_ref: str

class ParseOutput(BaseModel):
    chunk_refs: list[str]
    content_hash: str

@worker.task("parse_content", input_schema=ParseInput, output_schema=ParseOutput)
async def parse_content(ctx: Ctx, payload: ParseInput) -> ParseOutput:
    chunks = await parse(payload.staging_ref)
    if await already_processed(payload.document_id, hash_of(chunks)):
        return ctx.stop_chain({"reason": "duplicate_content"})   # drop the rest of the chain
    return ParseOutput(chunk_refs=await stage(chunks), content_hash=hash_of(chunks))

@worker.task("summarize_chunk", rate_class="azure-gpt5", runs_on=["llm"])
async def summarize_chunk(ctx: Ctx, payload: dict):
    if ctx.checkpoint_data:                                       # retry resumes without re-paying
        return {"summary_ref": ctx.checkpoint_data["summary_ref"]}
    ref = await stage_result(await llm.ainvoke(payload["chunk_ref"]))
    await ctx.checkpoint({"summary_ref": ref})
    return {"summary_ref": ref}

@worker.task("fan_out_summaries")
async def fan_out_summaries(ctx: Ctx, payload: dict):
    parsed: ParseOutput = ctx.output["parse_content"]            # typed via the schema index
    gate = await ctx.submit_children(
        children=[{"task": "summarize_chunk", "payload": {"chunk_ref": r}} for r in parsed.chunk_refs],
        on_complete={"task": "executive_summary", "payload": payload},
        gate_policy="all_success",
    )
    return {"children": len(gate.children)}
```

#### Threading data through chains and gates

A few contracts are easy to trip over:

- **A chained tail starts with an EMPTY payload (SDK-3).** The original `submit`
  payload does **not** flow down a `chain=[...]`. Thread data by RETURNING it from
  the predecessor and reading it back via `ctx.output[<predecessor_task>]` — not
  via `ctx.payload`.

  ```python
  # head returns everything the tail needs; the tail reads it via ctx.output.
  @worker.task("assemble")
  async def assemble(ctx: Ctx, payload: dict):
      return {"document_id": payload["document_id"], "output_ref": "s3://joined"}

  @worker.task("complete_stage")
  async def complete_stage(ctx: Ctx, payload: dict):        # payload == {} here
      up = ctx.output["assemble"]                            # thread via the return value
      await persist(up["document_id"], up["output_ref"])
      return {"ok": True}

  await sim.submit("assemble", {"document_id": "d1"}, chain=["assemble", "complete_stage"])
  ```

- **`task` + `chain` must agree (SDK-1).** When a continuation dict carries both a
  `task` and a `chain`, `chain[0]` must equal `task` (lead the chain with the task
  itself), otherwise the SDK raises at build time rather than silently running a
  different DAG:

  ```python
  on_complete={"task": "assemble", "chain": ["assemble", "complete_stage"]}   # ok
  on_complete={"task": "assemble", "chain": ["complete_stage"]}               # raises SymbaError
  ```

- **`ctx.output` access is inline-only unless you `fetch` (SDK-4).**
  `ctx.output[key]`, `key in ctx.output` and `ctx.output.get(key)` resolve the
  inline tier only and never issue a lazy `GetResult` RPC. `await
  ctx.output.fetch(key)` is the only path that consults the lazy tier after an
  inline miss.

- **The gate continuation preserves your payload AND adds a manifest (SDK-2).**
  The `on_complete` payload survives; the aggregate manifest is delivered under the
  reserved `__gate__` key: `{"gate_id", "results", "expected", "succeeded"}`.

### Human-in-the-loop

```python
@worker.task("review_gate")
async def review_gate(ctx: Ctx, payload: dict):
    decision = await ctx.wait_for_event(f"approve:{payload['ticket_id']}", timeout_s=86_400)
    if decision is None:
        return ctx.stop_chain({"outcome": "approval_timed_out"})
    return {"approved_by": decision["reviewer"]}

# from your approval webhook:
await engine.signal(f"approve:{ticket_id}", {"approved": True, "reviewer": user.email})
```

### Testing with `SymbaTest`

```python
import pytest
from symba.testing import SymbaTest
from app.worker import worker

@pytest.mark.asyncio
async def test_duplicate_content_stops_chain():
    async with SymbaTest() as sim:
        sim.register(worker)
        h = await sim.submit("parse_content",
                             {"document_id": "d1", "staging_ref": "s3://x"},
                             chain=["parse_content", "fan_out_summaries"])
        await h.result(timeout=5)
        assert "fan_out_summaries" not in {j.task_name for j in sim.jobs()}   # tail dropped
```

#### SymbaTest fidelity

`SymbaTest` runs the SDK's real dispatch pipeline in-process, so most behavior is
faithful. Know which guarantees it enforces vs. defers to the dockerized engine:

| Behavior | Enforced by SymbaTest? |
|---|---|
| Chains (head/tail split, `ctx.output` threading) | ✅ Enforced |
| Retries + compressed backoff | ✅ Enforced |
| `on_failure` hooks | ✅ Enforced |
| Checkpoints (`ctx.checkpoint` / `ctx.checkpoint_data`) | ✅ Enforced (dict-backed, no Redis) |
| `dedup_key` collapse | ✅ Enforced |
| Result shapes / schema validation | ✅ Enforced |
| `gate_policy` (`all_success` / `all_terminal` / `quorum(n)`) | ✅ Enforced (FE-1) |
| Forced DEAD via `fail_always()` / `fail_next(retryable=False)` | ✅ Enforced (FE-2) |
| Gate continuation payload (caller payload + `__gate__` manifest) | ✅ Enforced (SDK-2) |
| Lease / heartbeat timing, absolute-ceiling reclaim | ❌ Not enforced — integration suite |
| Real 64KB `result_json` byte cap | ❌ Not enforced — integration suite |

Driving a child to DEAD through a gate:

```python
async with SymbaTest() as sim:
    sim.register(worker)
    sim.fail_always("ocr_page")                 # exhaust max_attempts -> DEAD
    _, gate = await sim.fan_out(
        [{"task": "ocr_page", "payload": {"page": i}} for i in range(3)],
        on_complete={"task": "assemble", "payload": {"document_id": "d1"}},
        gate_policy="all_success",
    )
    # all_success + a DEAD child => the continuation is BLOCKED (not fired).
```

More runnable programs live in [`examples/`](examples/).

### Sync facade + CLI

```bash
symba run app.worker:worker --slots 50
symba submit parse_content --payload '{"document_id":"d1","staging_ref":"s3://x"}' --wait
symba job 019894c3-...
symba doctor            # connectivity + config + version handshake + Redis check
```

## 🔗 Compatibility

The SDK and engine share a protobuf wire contract; `symba.__engine_protocol__` records the proto
version the committed stubs were generated from. See [`COMPATIBILITY.md`](COMPATIBILITY.md) for the
SDK ↔ engine version matrix and the handshake behavior.

## 📚 Documentation

| Doc | What |
|---|---|
| [`symba_sdk_implementation.md`](symba_sdk_implementation.md) | The full implementation spec — the source of truth for every behavior. |
| [`examples/README.md`](examples/README.md) | Runnable end-to-end programs (enrichment pipeline, human-in-the-loop, GPU batch, SymbaTest). |
| [`COMPATIBILITY.md`](COMPATIBILITY.md) | SDK ↔ engine version matrix and the version handshake. |
| [Engine repo](https://github.com/syntel-technologies/symba) | The server, operator console, and the fuller architecture story. |

## Development

```bash
uv sync                          # install deps + dev group (exact pins for reproducibility)
uv run pytest -q                 # unit + conformance (no infra needed — SymbaTest)
uv run pytest -m integration     # against a dockerized engine (SYMBA_E2E_TARGET set)
uv run ruff check src tests      # lint
uv run pyright                   # types
```

See the [`Makefile`](Makefile) for proto regeneration (`make proto-gen`) and other targets.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

## Development and releases

See [Contributing](CONTRIBUTING.md), [the release workflow](docs/releasing.md), and [Security](SECURITY.md). Development targets `dev`; `main` requires a reviewed PR.
