"""Execution profiles (spec 11.1).

Three profiles pick the mechanism a handler runs under. Rule of thumb, printed
in the docs: *"Is the task mostly waiting or mostly working? Waiting -> io.
Working -> cpu. Working on a GPU -> gpu."*

Profile defaults are a table here; task registration overrides any field; submit
overrides registration; ``Worker(profile_defaults=...)`` overrides the table
per-worker.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

#: Engine default lease TTL (seconds) applied to io tasks that declare no
#: ``lease_ttl_s`` (the io profile defers its lease to the engine). Used by boot
#: validation to catch a long ``timeout_s`` running under the short engine lease
#: (SDK-7). Kept in lockstep with the engine ``[defaults]`` via COMPATIBILITY.md.
ENGINE_DEFAULT_IO_LEASE_TTL_S = 60


class Profile(enum.StrEnum):
    IO = "io"
    CPU = "cpu"
    GPU = "gpu"


@dataclass(slots=True, frozen=True)
class ProfileDefaults:
    """Per-profile default execution ceilings (spec 11.1).

    ``None`` means "send the proto zero value, let the engine ``[defaults]``
    decide"; the io profile intentionally defers everything to the engine.
    """

    timeout_s: int | None
    lease_ttl_s: int | None
    #: Whether handlers under this profile are ``async def`` (io) or ``def`` (cpu/gpu).
    wants_async: bool


PROFILE_DEFAULTS: dict[Profile, ProfileDefaults] = {
    # io defers timeout/lease to the engine defaults (600s/60s) — spec 11.1.
    Profile.IO: ProfileDefaults(timeout_s=None, lease_ttl_s=None, wants_async=True),
    Profile.CPU: ProfileDefaults(timeout_s=900, lease_ttl_s=120, wants_async=False),
    Profile.GPU: ProfileDefaults(timeout_s=3600, lease_ttl_s=300, wants_async=False),
}


def coerce_profile(value: str | Profile) -> Profile:
    if isinstance(value, Profile):
        return value
    try:
        return Profile(value)
    except ValueError as exc:
        raise ValueError(
            f"unknown profile {value!r}; expected one of {[p.value for p in Profile]}"
        ) from exc


__all__ = [
    "Profile",
    "ProfileDefaults",
    "PROFILE_DEFAULTS",
    "ENGINE_DEFAULT_IO_LEASE_TTL_S",
    "coerce_profile",
]
