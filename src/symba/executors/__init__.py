"""Executors: one per profile (spec 11)."""

from __future__ import annotations

from .asyncio_executor import AsyncioExecutor
from .base import Executor

__all__ = ["Executor", "AsyncioExecutor"]
