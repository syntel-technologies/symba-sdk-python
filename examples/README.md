# Symba SDK examples

Runnable programs that exercise the SDK end-to-end. Each folder is self-contained; the external
I/O (LLM calls, model weights, storage) is stubbed so every file runs as-is — swap in your real
implementations.

Most examples need a running engine on `grpc://localhost:7233`. Bring one up with
`docker compose up -d --wait` in the [engine repo](https://github.com/syntel-technologies/symba). The
`04_symbatest` example needs **no engine** — it runs entirely in-memory.

Install the SDK (with the CLI extra to use `symba run`):

```bash
pip install "syntel-symba[cli]"
```

| Example | What it shows | How to run |
|---|---|---|
| [`01_enrichment_pipeline/`](01_enrichment_pipeline/) | Chain + fan-out/gate + Pydantic schemas + checkpoints | `symba run examples.01_enrichment_pipeline.worker:worker`, then `python -m examples.01_enrichment_pipeline.submit` |
| [`02_human_in_the_loop/`](02_human_in_the_loop/) | `wait_for_event` parking + `signal` resume, checkpoint-before-wait | `symba run examples.02_human_in_the_loop.worker:worker`, then `python -m examples.02_human_in_the_loop.approve <ticket>` |
| [`03_gpu_batch/`](03_gpu_batch/) | `gpu` profile: one warm subprocess, `on_gpu_init` weight loading, sync handler | `symba run examples.03_gpu_batch.worker:worker` |
| [`04_symbatest/`](04_symbatest/) | Testing handlers through the real pipeline in-memory (no infra) | `pytest examples/04_symbatest/test_pipeline.py` |

## Running a worker

A worker is any module exposing a `Worker` object. Point the CLI at it as `module:attribute`:

```bash
symba run examples.01_enrichment_pipeline.worker:worker --slots 50
```

or run the module directly (each `worker.py` has a `__main__` guard):

```bash
python -m examples.01_enrichment_pipeline.worker
```

## Submitting jobs

Any of the submit scripts, the `symba submit` CLI command, or the `Engine`/`SyncEngine` clients:

```bash
symba submit parse_content \
  --payload '{"document_id":"d1","staging_ref":"s3://x"}' --wait
```

## Checking connectivity

Before running anything against a real engine, confirm the pairing:

```bash
symba doctor      # prints SDK version, proto version, target, gRPC reachability, Redis
```
