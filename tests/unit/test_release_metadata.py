"""Release metadata contracts for reproducible SDK artifacts."""

from __future__ import annotations

import tomllib
from importlib.metadata import distribution
from pathlib import Path

import symba


def test_build_backend_and_wheel_package_are_explicit() -> None:
    repository = Path(__file__).resolve().parents[2]
    manifest = tomllib.loads((repository / "pyproject.toml").read_text(encoding="utf-8"))

    assert manifest["build-system"] == {
        "requires": ["hatchling==1.32.0"],
        "build-backend": "hatchling.build",
    }
    assert manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["src/symba"]
    assert (repository / "src" / "symba" / "py.typed").is_file()


def test_distribution_preserves_python_import_and_cli_contract() -> None:
    installed = distribution("syntel-symba")
    assert installed.metadata["Name"] == "syntel-symba"
    assert symba.__version__ == installed.version
    assert symba.Worker is not None
    assert any(
        entry.group == "console_scripts"
        and entry.name == "symba"
        and entry.value == "symba.cli:app"
        for entry in installed.entry_points
    )
