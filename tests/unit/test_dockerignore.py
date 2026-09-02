"""SDK Docker build-context hygiene contract."""

from pathlib import Path

_REQUIRED_EXCLUDES = {
    ".git",
    ".github",
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "**/__pycache__",
    "**/*.py[cod]",
    "tests",
    "docs",
    "examples",
    "tools",
    "dist",
    "build",
    "*.egg-info",
}
_REQUIRED_BUILD_INPUTS = {"pyproject.toml", "uv.lock", "src"}


def test_dockerignore_excludes_development_payload_but_keeps_build_inputs() -> None:
    repository = Path(__file__).resolve().parents[2]
    entries = {
        line.strip()
        for line in (repository / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert _REQUIRED_EXCLUDES <= entries
    assert _REQUIRED_BUILD_INPUTS.isdisjoint(entries)
