"""``python -m symba`` entrypoint — delegates to the Typer CLI (spec 22)."""

from __future__ import annotations

from .cli import app

if __name__ == "__main__":
    app()
