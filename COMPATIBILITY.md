# Compatibility

The Symba SDK (`symba`) and the [Symba engine](https://github.com/syntel-technologies/symba) communicate
over a **protobuf wire contract**. The SDK commits the generated stubs (`src/symba/_proto/`) and
records the proto version they were built from:

```python
import symba
symba.__version__            # e.g. "0.1.0"          — the SDK package version
symba.__engine_protocol__    # e.g. "v0.1.0"         — the proto contract the stubs target
```

## SDK ↔ engine version matrix

The wire contract is **additive within a major**: the SDK sends zero values for knobs it does not
set, so the engine applies its own defaults, and new proto fields are ignored by older peers. That
means an SDK works with any engine whose proto **major** matches, and vice-versa.

| SDK (`symba`) | Engine proto (`__engine_protocol__`) | Engine server | Status |
|---|---|---|---|
| `0.1.x` | `v0.2.0` | `0.1.x`+ | ✅ Supported — current pre-1.0 line (adds AdminService cron upsert/delete). |
| `0.1.x` | `v0.1.0` | `0.1.x` | ✅ Backward-compatible — the new admin cron RPCs are simply unavailable. |
| `0.1.x` | `v0.2.0` | future, same proto major | ✅ Forward-compatible — unknown fields ignored. |
| `0.1.x` | `v0.2.0` | `>= 1.0` if proto major bumps | ⚠️ Requires an SDK matching the new proto major. |

Until `1.0`, both projects are pre-release: patch/minor versions may move together. Pin an exact
engine and SDK pair in production and upgrade them in lockstep when the proto version changes.

## Engine defaults mirrored in the SDK

The io profile defers its `timeout_s`/`lease_ttl_s` to the engine `[defaults]`. Boot validation
needs to know the engine's io lease to catch a long `timeout_s` running under a short lease, so the
SDK mirrors it as `symba.profiles.ENGINE_DEFAULT_IO_LEASE_TTL_S` (currently `60`). Keep this value
in lockstep with the engine's `[defaults]` io lease whenever the engine changes it.

## Version handshake

On connect, the worker announces its `__engine_protocol__`. If the engine rejects it as
incompatible, the SDK raises `symba.errors.ProtocolMismatch` with both versions in the message —
fail fast rather than send malformed frames. `symba doctor` performs this handshake explicitly and
prints the SDK version, proto version, and target so you can confirm a pairing before deploying.

## Python & dependency support

- **Python:** 3.11 – 3.14 (see `requires-python` in [`pyproject.toml`](pyproject.toml)).
- **Runtime deps:** declared as ranges (`grpcio>=1.66,<2.0`, `protobuf>=5.29,<8.0`,
  `pydantic>=2.7,<3.0`, `structlog>=24.1,<27.0`, `tenacity>=8.3,<10.0`). CI tests the min and
  latest of each range.
- **Optional extras:** `redis>=5.0,<9.0` (checkpoint fast path), `typer`/`rich` (CLI). CI runs the
  matrix with Redis absent, 5.x, and 8.x.

## Regenerating stubs

Stubs are committed so the package installs without a proto toolchain. Maintainers regenerate them
from the engine's `proto/` with `make proto-gen` (see the [`Makefile`](Makefile)); CI's
`stub-check` job fails if the committed stubs drift from the pinned engine proto tag.
