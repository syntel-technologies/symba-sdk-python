"""Package + engine-protocol version surface (spec 4.1, 4.3)."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path


def _pkg_version() -> str:
    try:
        return importlib.metadata.version("symba")
    except importlib.metadata.PackageNotFoundError:  # editable/source checkout
        return "0.0.0.dev0"


def _engine_protocol() -> str:
    version_file = Path(__file__).resolve().parent / "_proto" / "VERSION"
    try:
        return version_file.read_text().strip()
    except OSError:
        return "unknown"


__version__ = _pkg_version()
__engine_protocol__ = _engine_protocol()

#: Value sent in ``ClaimRequest.sdk_version`` and the control-plane metadata
#: header on every call (spec 4.3): ``symba/<pkg> proto/<stub-tag>``.
SDK_VERSION_STRING = f"symba/{__version__} proto/{__engine_protocol__}"
