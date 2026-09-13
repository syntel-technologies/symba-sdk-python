"""JSON seam (spec 2.2): use orjson if the host app already installed it,
otherwise stdlib ``json``. orjson is deliberately NOT a dependency — the
zero-dependency principle wins over a marginal speedup on small SDK payloads.
"""

from __future__ import annotations

import json as _stdlib_json
from typing import Any

try:
    import orjson  # type: ignore[import-not-found]

    _orjson: Any = orjson
except ImportError:  # pragma: no cover - depends on host env
    _orjson = None


def dumps(obj: Any) -> bytes:
    """Serialize ``obj`` to compact UTF-8 JSON bytes (the wire format)."""
    if _orjson is not None:
        return _orjson.dumps(obj)
    return _stdlib_json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def loads(data: bytes | str) -> Any:
    """Deserialize UTF-8 JSON bytes/str."""
    if not data:
        return None
    if _orjson is not None:
        return _orjson.loads(data)
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return _stdlib_json.loads(data)
