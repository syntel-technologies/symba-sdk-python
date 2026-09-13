"""The ``symba`` command-line interface (spec 22).

Typer-based, mirrors the engine's ops surface from the developer's side. Installed
as the ``symba`` console script (requires the ``[cli]`` extra: ``pip install
syntel-symba[cli]``). Every command honors ``SYMBA_*`` env vars; ``--engine`` overrides.

``symba doctor`` is the support-load killer: connectivity, auth, version handshake
and Redis reachability in one PASS/FAIL table with remediation hints.
"""

from __future__ import annotations

import importlib
import json
from typing import Any

from ._version import SDK_VERSION_STRING, __engine_protocol__, __version__
from .config import load_settings

try:
    import typer
    from rich.console import Console
    from rich.table import Table
except ImportError as exc:  # pragma: no cover - only hit without the [cli] extra
    raise SystemExit(
        "the symba CLI requires the 'cli' extra: install with `pip install syntel-symba[cli]`"
    ) from exc

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Symba SDK command line — run workers, submit jobs, and check connectivity.",
)
_console = Console()


def _load_worker(ref: str) -> Any:
    """Import a ``module:attr`` reference to a :class:`~symba.worker.Worker`."""
    if ":" not in ref:
        raise typer.BadParameter(
            f"expected a 'module:attribute' reference (e.g. 'app.worker:worker'), got {ref!r}"
        )
    module_name, attr = ref.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise typer.BadParameter(f"could not import module {module_name!r}: {exc}") from exc
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise typer.BadParameter(f"module {module_name!r} has no attribute {attr!r}") from exc


def _sync_engine(engine: str | None, token: str | None) -> Any:
    from .sync import SyncEngine

    return SyncEngine(engine, token=token)


@app.command()
def run(
    worker_ref: str = typer.Argument(..., help="Worker reference as 'module:attribute'."),
    slots: int | None = typer.Option(None, "--slots", help="Override concurrent slot count."),
    engine: str | None = typer.Option(None, "--engine", help="Override the gRPC target."),
) -> None:
    """Import the Worker object and run it (the production entrypoint)."""
    worker = _load_worker(worker_ref)
    if slots is not None:
        worker._explicit_slots = slots
    if engine is not None:
        worker._settings.engine.target = engine
    worker.run()


@app.command()
def tasks(
    worker_ref: str = typer.Argument(..., help="Worker reference as 'module:attribute'."),
) -> None:
    """Print the registry (task names, profiles, timeouts) — no engine connection."""
    worker = _load_worker(worker_ref)
    worker.registry.validate(
        strict_schemas=worker._strict_schemas,
        heartbeat_interval_s=worker._heartbeat_interval_s,
    )
    table = Table(title="Registered tasks")
    table.add_column("task", style="cyan")
    table.add_column("profile")
    table.add_column("timeout_s")
    table.add_column("lease_ttl_s")
    table.add_column("input_schema")
    table.add_column("output_schema")
    for name in sorted(worker.registry.names()):
        task = worker.registry.get(name)
        assert task is not None
        table.add_row(
            name,
            task.profile.value,
            str(task.effective_timeout_s),
            str(task.effective_lease_ttl_s),
            task.input_schema.__name__ if task.input_schema else "-",
            task.output_schema.__name__ if task.output_schema else "-",
        )
    _console.print(table)


@app.command()
def submit(
    task: str = typer.Argument(..., help="Task name to submit."),
    payload: str = typer.Option("{}", "--payload", help="JSON payload."),
    wait: bool = typer.Option(False, "--wait", help="Block until terminal and print the result."),
    timeout: float = typer.Option(120.0, "--timeout", help="Seconds to wait when --wait is set."),
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Submit one job from the shell; with --wait, poll to terminal and print the result."""
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--payload is not valid JSON: {exc}") from exc
    eng = _sync_engine(engine, token)
    try:
        handle = eng.submit(task, parsed)
        _console.print(f"submitted [cyan]{handle.id}[/cyan] task={task!r}")
        if wait:
            result = handle.result(timeout=timeout)
            _console.print_json(data=result)
    finally:
        eng.close()


@app.command()
def job(
    job_id: str = typer.Argument(..., help="Job id to inspect."),
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Show job details and the event history (control-plane query)."""
    eng = _sync_engine(engine, token)
    try:
        status = eng.get_job(job_id)
        table = Table(title=f"Job {job_id}")
        table.add_column("field", style="cyan")
        table.add_column("value")
        table.add_row("task", status.task_name)
        table.add_row("state", status.state.name)
        table.add_row("attempt", str(status.attempt))
        table.add_row("ctx_id", status.ctx_id or "-")
        table.add_row("last_error", status.last_error or "-")
        _console.print(table)
    finally:
        eng.close()


@app.command()
def cancel(
    job_id: str = typer.Argument(...),
    cascade: bool = typer.Option(True, "--cascade/--no-cascade"),
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Cancel a job (and, by default, its ctx descendants)."""
    eng = _sync_engine(engine, token)
    try:
        outcome = eng.cancel(job_id, cascade=cascade)
        _console.print(
            f"cancel {job_id}: previous={outcome.previous_state.name} cancelled={outcome.cancelled}"
        )
    finally:
        eng.close()


@app.command()
def resubmit(
    job_id: str = typer.Argument(...),
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Resubmit a terminal job as a fresh attempt."""
    eng = _sync_engine(engine, token)
    try:
        handle = eng.resubmit(job_id)
        _console.print(f"resubmitted as [cyan]{handle.id}[/cyan]")
    finally:
        eng.close()


@app.command()
def signal(
    wait_key: str = typer.Argument(..., help="The wait_key parked jobs are waiting on."),
    payload: str = typer.Option("{}", "--payload", help="JSON payload to deliver."),
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Deliver a signal payload to jobs parked on a wait_key."""
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--payload is not valid JSON: {exc}") from exc
    eng = _sync_engine(engine, token)
    try:
        delivered = eng.signal(wait_key, parsed)
        _console.print(f"delivered to {delivered} waiting job(s)")
    finally:
        eng.close()


@app.command()
def doctor(
    engine: str | None = typer.Option(None, "--engine"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    """Connectivity + config check: target, auth, version handshake, Redis."""
    from .sync import SyncEngine

    settings = load_settings()
    target = engine or settings.engine.target
    table = Table(title="symba doctor")
    table.add_column("check", style="cyan")
    table.add_column("status")
    table.add_column("detail / remediation")

    table.add_row("sdk_version", "INFO", SDK_VERSION_STRING)
    table.add_row("engine_protocol", "INFO", __engine_protocol__)
    table.add_row("target", "INFO", target)

    ok = True
    eng = SyncEngine(target, token=token or settings.engine.token)
    try:
        result = eng.probe(timeout_s=8.0)
        if result.ok:
            table.add_row("grpc_reachable", "PASS", f"connected to {target}: {result.detail}")
        else:
            ok = False
            table.add_row("grpc_reachable", "FAIL", f"[{result.stage}] {result.detail}")
    except Exception as exc:
        ok = False
        table.add_row(
            "grpc_reachable",
            "FAIL",
            f"{type(exc).__name__}: {exc}. Check the target and that the engine is running.",
        )
    finally:
        eng.close()

    redis_url = settings.redis.url
    if not redis_url:
        table.add_row("redis", "SKIP", "no SYMBA_CHECKPOINT_REDIS_URL set (durable path only)")
    else:
        table.add_row("redis", _redis_probe(redis_url), redis_url)

    _console.print(table)
    if not ok:
        raise typer.Exit(code=1)


def _redis_probe(url: str) -> str:
    try:
        import redis  # type: ignore[import-not-found]
    except ImportError:
        return "FAIL"  # extra not installed
    try:
        client = redis.from_url(url)
        client.ping()
        return "PASS"
    except Exception:
        return "FAIL"


@app.command(name="gen-stubs")
def gen_stubs(
    proto_dir: str = typer.Option(..., "--proto-dir", help="Path to the engine's proto/ dir."),
) -> None:
    """Regenerate committed stubs (maintainers only; see the Makefile proto-gen target)."""
    _console.print(
        f"run `make proto-gen PROTO_SRC={proto_dir}` — stub generation lives in the Makefile "
        f"so it stays reproducible in CI (spec 4.2)."
    )


@app.command()
def version() -> None:
    """Print SDK and engine-protocol versions."""
    _console.print_json(
        data={
            "sdk": __version__,
            "engine_protocol": __engine_protocol__,
            "version_string": SDK_VERSION_STRING,
        }
    )


if __name__ == "__main__":
    app()
