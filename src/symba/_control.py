"""Internal control-flow signals (spec 14.1).

``_Parked`` unwinds a handler task cleanly when ``wait_for_event`` parks the job
engine-side. It is neither a Complete nor a Fail — the engine owns the WAITING
state; the SDK just releases the slot and stops running. It never escapes the
dispatch pipeline, so it inherits ``BaseException`` to slip past a handler's
``except Exception``.
"""

from __future__ import annotations


class _Parked(BaseException):
    """Raised inside the handler when a wait parks the job (spec 14.1)."""

    def __init__(self, wait_key: str) -> None:
        self.wait_key = wait_key
        super().__init__(f"job parked on wait_key={wait_key!r}")


__all__ = ["_Parked"]
