"""Top-level, picklable handlers for cpu/gpu executor subprocess tests.

Handlers must be importable by reference so forkserver/spawn children can find them.
"""

from __future__ import annotations

import os
from typing import Any


def double(ctx: Any, payload: dict) -> dict:
    return {"doubled": payload["n"] * 2, "pid": os.getpid()}


def checkpoint_then_return(ctx: Any, payload: dict) -> dict:
    ctx.checkpoint({"seen": payload["n"]})
    return {"ok": True}


def raise_value_error(ctx: Any, payload: dict) -> dict:
    raise ValueError("bad compute")


def raise_retryable(ctx: Any, payload: dict) -> dict:
    from symba.errors import RetryableError

    raise RetryableError("transient compute blip")


def hard_crash(ctx: Any, payload: dict) -> dict:
    os._exit(1)  # simulate a segfault / OOM-kill: process dies without a reply


# gpu warm-init marker set by the init hook, read by the handler.
_GPU_STATE: dict[str, Any] = {}


def gpu_init() -> None:
    _GPU_STATE["warm"] = True
    _GPU_STATE["init_pid"] = os.getpid()


def gpu_infer(ctx: Any, payload: dict) -> dict:
    return {
        "warm": _GPU_STATE.get("warm", False),
        "same_process": _GPU_STATE.get("init_pid") == os.getpid(),
        "echo": payload.get("x"),
    }
