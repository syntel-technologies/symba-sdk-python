"""Generate/check SDK stubs from an immutable engine revision without modifying source on checks."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

from fix_proto_imports import _rewrite

ROOT = Path(__file__).resolve().parent.parent
DESTINATION = ROOT / "src/symba/_proto"


def generate(source: Path, output: Path) -> dict[str, str]:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{source}",
            f"--python_out={output}",
            f"--grpc_python_out={output}",
            f"--pyi_out={output}",
            *map(str, sorted((source / "symba/v1").glob("*.proto"))),
        ],
        check=True,
    )
    return {
        p.name: _rewrite(p.read_text())
        for p in (output / "symba/v1").glob("*")
        if p.suffix in {".py", ".pyi"}
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="Local engine proto directory")
    parser.add_argument("--ref", default=(DESTINATION / "ENGINE_REF").read_text().strip())
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="symba-proto-") as temporary:
        work = Path(temporary)
        source = args.source
        if source is None:
            checkout = work / "engine"
            checkout.mkdir()
            subprocess.run(["git", "init", "-q", str(checkout)], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(checkout),
                    "fetch",
                    "--depth=1",
                    "https://github.com/syntel-technologies/symba.git",
                    args.ref,
                ],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(checkout), "checkout", "--detach", "FETCH_HEAD"], check=True
            )
            source = checkout / "proto"
        output = work / "generated"
        output.mkdir()
        generated = generate(source.resolve(), output)
        if not generated:
            raise RuntimeError("No protobuf stubs generated")
        expected = {p.name for p in DESTINATION.glob("*_pb2*") if p.suffix in {".py", ".pyi"}}
        if args.check:
            changed = sorted(
                name
                for name in expected | generated.keys()
                if name not in generated
                or not (DESTINATION / name).exists()
                or (DESTINATION / name).read_text() != generated[name]
            )
            if changed:
                sys.stderr.write("Proto drift: " + ", ".join(changed) + "\n")
                return 1
            sys.stdout.write("All Python and typing stubs match the pinned engine contract.\n")
            return 0
        for name in expected - generated.keys():
            (DESTINATION / name).unlink()
        for name, text in generated.items():
            (DESTINATION / name).write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
