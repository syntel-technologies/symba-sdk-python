"""GPU worker — one warm subprocess, weights loaded once via on_gpu_init.

    symba run examples.03_gpu_batch.worker:worker

The ``gpu`` profile runs the (sync) handler in a single warm subprocess. Model
weights load ONCE in ``@worker.on_gpu_init`` and stay warm across every job. The
handler is a plain ``def`` (enforced at boot for gpu/cpu); ``ctx`` verbs are
marshaled back to the parent over a pipe.
"""

from __future__ import annotations

from symba import Worker

worker = Worker(
    engine="grpc://localhost:7233",
    tags=["gpu"],
    slots=1,
    labels={"gpu": "gh200"},
)

# module-global filled in by the init hook, read by the handler (same subprocess).
model = None


@worker.on_gpu_init
def load_models() -> None:
    global model
    model = _load_layout_model("/models/layout-v3")


@worker.task("gpu_parse_pdf", profile="gpu", timeout_s=1800)
def gpu_parse_pdf(ctx, payload):  # sync def — required for gpu
    assert model is not None, "on_gpu_init must have run in this subprocess"
    pages = model.parse(payload["pdf_ref"])
    return {"parsed_ref": _stage(pages), "pages": len(pages)}


# --- stubbed externals (replace with real model + storage) -------------------
class _Model:
    def parse(self, pdf_ref: str) -> list[str]:
        return [f"{pdf_ref}#page-{i}" for i in range(4)]


def _load_layout_model(path: str) -> _Model:
    return _Model()


def _stage(pages: list[str]) -> str:
    return f"staged://{pages[0]}"


if __name__ == "__main__":
    worker.run()
