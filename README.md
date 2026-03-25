# Dagster MCP

An [MCP](https://modelcontextprotocol.io/) server that wraps the [Dagster](https://dagster.io/) GraphQL API, giving any MCP client (Claude Code, Cursor, etc.) full visibility and control over your Dagster instance.

## What it does

Exposes up to 19 tools that let you inspect, diagnose, and operate a Dagster instance (self-hosted or Dagster Cloud):

- **Runs** — list, inspect, get logs (with level filtering), get step stats, get consolidated failure summaries
- **Assets** — search, get details, get recent materializations, get consolidated health status
- **Jobs** — list all jobs across code locations
- **Schedules & Sensors** — list with status, cron, targets, inspect tick history
- **Instance** — global health check (daemon status, run queue, code location errors)
- **Code Locations** — list and reload
- **Backfills** — list recent backfills
- **Actions** — launch jobs, terminate runs, reload code locations _(opt-in, disabled by default)_

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A running [Dagster](https://dagster.io/) instance (self-hosted or Cloud)

### Install

The package is published on [PyPI](https://pypi.org/project/dagster-mcp/).

**Option A — run directly with uvx (no clone needed):**

```bash
uvx dagster-mcp
```

**Option B — install with pip:**

```bash
pip install dagster-mcp
```

**Option C — clone and run:**

```bash
git clone https://github.com/fabdendev/dagster-mcp.git
cd dagster-mcp
uv sync
```

### Configure

#### Single environment

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_URL` | Base URL of your Dagster instance | `http://localhost:3000` |
| `DAGSTER_API_TOKEN` | Dagster Cloud API token (leave empty for self-hosted) | _(empty)_ |
| `DAGSTER_EXTRA_HEADERS` | JSON object of additional request headers sent to Dagster GraphQL | _(empty)_ |
| `DAGSTER_READ_ONLY` | When `true`, only read tools are exposed (no launch/terminate/reload) | `true` |

**Self-hosted:**

```bash
export DAGSTER_URL=http://localhost:3000
```

**Dagster Cloud:**

```bash
export DAGSTER_URL=https://myorg.dagster.cloud/prod
export DAGSTER_API_TOKEN=your-dagster-cloud-user-token
```

**Custom auth / proxy headers:**

```bash
export DAGSTER_EXTRA_HEADERS='{"Authorization":"Bearer your-token","X-My-Header":"value"}'
```

#### Multiple environments

Use `DAGSTER_ENVS` to configure several Dagster instances in one server. Every tool then accepts an optional `env` parameter so the LLM can target the right instance.

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_ENVS` | JSON object mapping env names to `{url, token?, extra_headers?}` configs | _(empty)_ |
| `DAGSTER_DEFAULT_ENV` | Env name to use when `env` is not passed to a tool | _(empty)_ |

```bash
export DAGSTER_ENVS='{
  "prod": {"url": "https://myorg.dagster.cloud/prod", "token": "prod-token"},
  "staging": {"url": "https://myorg.dagster.cloud/staging", "token": "stg-token"},
  "dev": {"url": "http://localhost:3000"}
}'
export DAGSTER_DEFAULT_ENV=prod
```

When `DAGSTER_ENVS` is set, `DAGSTER_URL` / `DAGSTER_API_TOKEN` / `DAGSTER_EXTRA_HEADERS` are ignored. If only one env is defined, it is used automatically even without `DAGSTER_DEFAULT_ENV`.

### Add to Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json`):

**If using uvx (single env):**

```json
{
  "mcpServers": {
    "dagster": {
      "command": "uvx",
      "args": ["dagster-mcp"],
      "env": {
        "DAGSTER_URL": "http://localhost:3000"
      }
    }
  }
}
```

**If using uvx (multiple envs):**

```json
{
  "mcpServers": {
    "dagster": {
      "command": "uvx",
      "args": ["dagster-mcp"],
      "env": {
        "DAGSTER_ENVS": "{\"prod\":{\"url\":\"https://myorg.dagster.cloud/prod\",\"token\":\"prod-token\"},\"dev\":{\"url\":\"http://localhost:3000\"}}",
        "DAGSTER_DEFAULT_ENV": "prod"
      }
    }
  }
}
```

**If installed from clone:**

```json
{
  "mcpServers": {
    "dagster": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/dagster-mcp", "dagster-mcp"],
      "env": {
        "DAGSTER_URL": "http://localhost:3000"
      }
    }
  }
}
```

## Tools

### Runs

| Tool | Description |
|------|-------------|
| `get_runs` | List recent runs, filter by job name and/or status |
| `get_run_status` | Get status, config, and run lineage (rootRunId, parentRunId) |
| `get_run_logs` | Get logs/events for a run (with pagination and optional `level_filter`) |
| `get_run_stats` | Get step-level stats (timing, materializations) |
| `get_run_failure_summary` | **New** — consolidated failure diagnostics: failed steps, root cause error, per-step durations, and suggestions in a single call |

### Assets

| Tool | Description |
|------|-------------|
| `get_recent_materializations` | Get recent materializations for an asset |
| `get_asset_details` | Get details, dependencies, and partitions for assets |
| `search_assets` | Search/list assets by key prefix or group name |
| `get_asset_health` | **New** — consolidated health view: last materialization, run status, freshness policy, staleness (works with single asset or group) |

### Jobs, Schedules & Sensors

| Tool | Description |
|------|-------------|
| `list_jobs` | List all jobs across all code locations |
| `list_schedules` | List schedules with status, cron, and next tick |
| `list_sensors` | List sensors with status and target jobs |
| `get_tick_history` | **New** — tick history for a schedule or sensor (detect silent failures, see run associations) |

### Instance & Code Locations

| Tool | Description |
|------|-------------|
| `get_instance_status` | **New** — global health check: daemon health, queued run count, code location errors |
| `list_code_locations` | List all code locations and their load status |
| `list_backfills` | List recent backfills with status and progress |

### Write Operations

| Tool | Description |
|------|-------------|
| `reload_code_location` | Reload a code location (e.g. after deploy) |
| `terminate_run` | Terminate a running run |
| `launch_job` | Launch a job or materialize specific assets |

Write tools are only available when `DAGSTER_READ_ONLY=false`.

## How it differs from the official Dagster MCP

The [official Dagster MCP](https://dagster.io/blog/dagsters-mcp-server) (`dg[mcp]`) is a **development-time** tool — it helps AI write Dagster code, scaffold components, and work with the `dg` CLI.

This project is an **operations-time** tool — it lets AI (or any MCP client) **monitor and operate** a running Dagster instance: inspect runs, read logs, check schedules, launch jobs, and more.

They serve different purposes and work well together.

## Compatibility

Tested with Dagster 1.6+. All GraphQL queries target stable, non-deprecated API fields.

## Development

```bash
uv sync --extra dev
uv run ruff check dagster_mcp/    # lint
uv run pytest                     # tests (95 tests)
uv run python -m dagster_mcp      # start server locally
```

## License

MIT
