# Configuration reference

Symba is configured by `symba.toml` + a `.env` file + `SYMBA_*` environment overrides.
The typed model in [`src/symba/config.py`](../src/symba/config.py) is the **source of
truth** — a bad key fails fast at boot with a pydantic error naming the exact field.
This page is the operator-facing summary; when in doubt, the model wins.

## Precedence

Highest wins:

```
explicit init kwargs  >  process env (SYMBA_*)  >  .env file  >  symba.toml  >  defaults
```

## Environment override syntax

Sections nest with a double underscore, prefixed `SYMBA_`:

```bash
SYMBA_POSTGRES__DSN=postgresql://symba:pw@db:5432/symba
SYMBA_SERVER__HTTP_PORT=7300
SYMBA_AUTH__MODE=token
SYMBA_LOG__LEVEL=DEBUG
```

Validate a config without booting the engine — `load_config()` raises a pydantic error
naming the exact bad field:

```bash
python -c "from symba.config import load_config; load_config(); print('config OK')"
# or point at an explicit TOML:
python -c "from symba.config import load_config; load_config('symba.toml'); print('OK')"
```

## Sections

Defaults shown are the model defaults; only override what you need.

### `[app]`
| Key | Default | Notes |
|---|---|---|
| `environment` | `development` | `development` \| `testing` \| `production` |
| `version_major/minor/patch` | `0`/`1`/`0` | stamped into logs (`app_version`) |

### `[server]`
| Key | Default | Notes |
|---|---|---|
| `grpc_port` | `7233` | data plane (workers) |
| `http_port` | `7300` | control plane (submit/query/UI proxy target) |
| `roles` | `["all"]` | any of `api` \| `sweeper` \| `all`; split roles to scale planes independently |
| `grpc_max_message_mb` | `4` | |
| `grpc_keepalive_time_ms` | `20000` | |
| `grpc_max_connection_age_s` | `1800` | forces periodic reconnection for LB rebalancing |
| `shutdown_drain_s` | `30` | SIGTERM grace before abandoning in-flight leases (recovered by TTL) |

### `[postgres]` — the only required dependency
| Key | Default | Notes |
|---|---|---|
| `dsn` | `postgresql://symba:symba@localhost:5432/symba` | **set this in prod** |
| `schema_name` | `symba` | Flyway-managed application schema |
| `hot_pool_size` | `20` (≥2) | claim-path pool; watch `symba_pool_acquire_wait_seconds{pool=hot}` |
| `hot_command_timeout_s` | `5` | keep tight — the hot path is fast by design |
| `general_pool_size` | `20` (≥2) | reads/admin/sweeps |
| `general_command_timeout_s` | `30` | |

### `[redis]` — optional (degraded mode)
| Key | Default | Notes |
|---|---|---|
| `url` | `""` | **empty = degraded mode**: rate limits + checkpoints fall back to Postgres |
| `socket_timeout_s` | `2.0` | on timeout the engine logs one WARNING and uses PG (N7) |

### `[defaults]` — per-job fallbacks (spec fields override these)
| Key | Default | Notes |
|---|---|---|
| `lease_ttl_s` | `60` | heartbeat within this or the sweeper reclaims |
| `timeout_s` | `600` | handler wall-clock budget |
| `max_attempts` | `5` | then DEAD |
| `backoff_base_s` / `backoff_factor` / `backoff_max_s` | `1.0` / `2.0` / `300.0` | full-jitter retry backoff |
| `jitter` | `true` | |

### `[limits]` — backpressure / safety caps
| Key | Default | Notes |
|---|---|---|
| `max_result_kb` | `64` | result-tier cap |
| `max_payload_kb` | `256` | submit-enforced |
| `max_chain_len` | `50` | |
| `max_fanout_children` | `100000` | |
| `tenant_queued_cap` | `0` | 0 = unlimited; >0 → over-cap submits get 429 + `Retry-After` |
| `query_max_page` | `1000` | UI/list page ceiling |

### `[sweeper]`
| Key | Default | Notes |
|---|---|---|
| `interval_s` | `5` | reclaim/expire/reconcile cadence; also the `Retry-After` hint |
| `batch_size` | `1000` | rows per statement per pass |
| `worker_stale_after_heartbeats` | `3` | missed heartbeats before a worker is flagged stale |

### `[retention]`
| Key | Default | Notes |
|---|---|---|
| `job_events_days` | `90` | partition-drop retention for the audit ledger |
| `succeeded_jobs_days` | `30` | **DEAD jobs are NEVER auto-pruned** (DLQ contract) |
| `checkpoints_hours` | `72` | |
| `consumed_signals_days` | `7` | |

### `[dispatcher]` — the N2 latency knob
| Key | Default | Notes |
|---|---|---|
| `min_tick_ms` | `10` | busy floor of the adaptive claim tick |
| `max_tick_ms` | `250` | idle ceiling; the idle-claim latency budget derives from this |
| `max_per_group_per_batch` | `0` | 0 → `GREATEST(2, limit/8)` fairness cap |

### `[matcher]`
| Key | Default | Notes |
|---|---|---|
| `strict_group_caps` | `false` | |
| `max_upstream_inline_kb` | `256` | inline-tier upstream cap (deeper via `GetResult`) |

### `[auth]` — see also the N9 test matrix
| Key | Default | Notes |
|---|---|---|
| `mode` | `none` | `none` fail-closes on any non-loopback peer; a routable deploy **must** set `token` or `mtls` |
| `token_jwks_url` | `""` | RS256 JWKS validation (token mode) |
| `tokens` | `{}` | shared-secret → tenant map (token mode) |

### `[log]`
| Key | Default | Notes |
|---|---|---|
| `level` | `INFO` | |
| `format` | `json` | `json` (prod) \| `console` (dev) |
| `file_path` | `""` | empty = stdout only |
| `enable_stdout` | `true` | |

### `[observability]`
| Key | Default | Notes |
|---|---|---|
| `metrics_enabled` | `true` | exposes `/metrics` |
| `otlp_endpoint` | `""` | **empty = tracing off**; set to enable the submit→claim→complete spans |

### `[cron]`
| Key | Default | Notes |
|---|---|---|
| `tick_s` | `1.0` | scheduler resolution; dedups on `cron:{id}:{next_fire}`, no backfill |

## Minimal production `.env`

```bash
SYMBA_APP__ENVIRONMENT=production
SYMBA_POSTGRES__DSN=postgresql://symba:${DB_PASSWORD}@db:5432/symba
SYMBA_REDIS__URL=redis://cache:6379/0        # optional; omit for degraded mode
SYMBA_AUTH__MODE=token                        # REQUIRED for any routable deploy
SYMBA_AUTH__TOKEN_JWKS_URL=https://idp/.well-known/jwks.json
SYMBA_LOG__FORMAT=json
SYMBA_OBSERVABILITY__OTLP_ENDPOINT=http://otel-collector:4317
```
