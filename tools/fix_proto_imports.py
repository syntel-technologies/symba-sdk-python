"""Rewrite grpcio-tools' absolute imports to package-relative `_proto` imports.

grpcio-tools emits ``from symba.v1 import common_pb2 as ...`` and
``import symba.v1.common_pb2`` in the generated ``*_pb2.py`` / ``*_pb2_grpc.py``
files, assuming the proto package is importable as a top-level module. The SDK
vendors the stubs under ``symba._proto`` instead, so every generated file needs
those references rewritten to ``from . import common_pb2`` form. This is a
standard grpcio-tools wart, fixed once here and never by hand (spec 4.1).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PROTO_DIR = Path(__file__).resolve().parent.parent / "src" / "symba" / "_proto"

# The proto package as declared in the engine's .proto files (``package symba.v1;``).
# If the engine ever renames its package this constant is the only thing that changes.
PROTO_PKG = "symba.v1"


def _rewrite(text: str) -> str:
    pkg_path = PROTO_PKG.replace(".", r"\.")
    # from simba.v1 import common_pb2 as simba_dot_v1_dot_common__pb2
    text = re.sub(
        rf"^from {pkg_path} import (\w+) as (\w+)$",
        r"from . import \1 as \2",
        text,
        flags=re.MULTILINE,
    )
    # from simba.v1 import common_pb2
    text = re.sub(
        rf"^from {pkg_path} import (\w+)$",
        r"from . import \1",
        text,
        flags=re.MULTILINE,
    )
    # import simba.v1.common_pb2 as ...  (grpc stubs)
    text = re.sub(
        rf"^import {pkg_path}\.(\w+) as (\w+)$",
        r"from . import \1 as \2",
        text,
        flags=re.MULTILINE,
    )
    return text


def main() -> int:
    if not PROTO_DIR.is_dir():
        sys.stderr.write(f"proto dir not found: {PROTO_DIR}\n")
        return 1
    changed = 0
    for path in sorted(PROTO_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        original = path.read_text()
        rewritten = _rewrite(original)
        if rewritten != original:
            path.write_text(rewritten)
            changed += 1
    sys.stdout.write(f"fix_proto_imports: rewrote {changed} file(s) in {PROTO_DIR}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
