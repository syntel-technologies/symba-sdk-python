"""Release metadata contracts for reproducible SDK artifacts."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_build_backend_and_wheel_package_are_explicit() -> None:
    repository = Path(__file__).resolve().parents[2]
    manifest = tomllib.loads((repository / "pyproject.toml").read_text(encoding="utf-8"))

    assert manifest["build-system"] == {
        "requires": ["hatchling==1.32.0"],
        "build-backend": "hatchling.build",
    }
    assert manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["src/symba"]
    assert (repository / "src" / "symba" / "py.typed").is_file()
