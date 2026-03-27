# Dagster MCP

[![PyPI version](https://img.shields.io/pypi/v/dagster-mcp)](https://pypi.org/project/dagster-mcp/)
[![PyPI downloads](https://img.shields.io/pypi/dm/dagster-mcp)](https://pypi.org/project/dagster-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://github.com/fabdendev/dagster-mcp/actions/workflows/tests.yml/badge.svg)](https://github.com/fabdendev/dagster-mcp/actions/workflows/tests.yml)

An [MCP](https://modelcontextprotocol.io/) server that gives AI agents full visibility and control over your [Dagster](https://dagster.io/) instance — like an SRE for your data pipelines.

Works with any MCP client: [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Claude Desktop](https://claude.ai), [Cursor](https://cursor.sh), and more.

## Why this exists

Data pipelines break at 3 AM. Schedules silently stop firing. Assets go stale. Instead of waking up to a dashboard full of red, give your AI agent the tools to **monitor, diagnose, and fix** your Dagster instance autonomously.

```
Agent: Checking instance health...
       get_instance_status() -> healthy: false, daemon "SCHEDULER" unhealthy

Agent: Scheduler daemon is down. Let me check recent failures...
       get_runs(statuses=["FAILURE"], limit=5) -> 3 failed runs in the last hour

Agent: Diagnosing the most recent failure...
       get_run_failure_summary("run_abc123") ->
         failed_steps: ["transform_orders"]
         root_cause: "NullPointerError: column 'price' is null"
         suggestions: ["Single step failed — consider re-running from failure"]

Agent: Re-launching the failed job...
       launch_job("etl_pipeline", "my_project") -> run_id: "run_def456", status: STARTED
```

## What it does

19 tools across 6 categories, designed for autonomous DataOps workflows:

| Category | Tools | What an agent can do |
|----------|-------|---------------------|
| **Runs** | `get_runs` `get_run_status` `get_run_logs` `get_run_stats` `get_run_failure_summary` | Find failures, diagnose root causes, inspect logs and step timing |
| **Assets** | `search_assets` `get_asset_details` `get_recent_materializations` `get_asset_health` | Discover assets, check freshness, detect stale data |
| **Jobs** | `list_jobs` | Inventory all jobs across code locations |
| **Schedules & Sensors** | `list_schedules` `list_sensors` `get_tick_history` | Detect silent failures, missed ticks, sensor errors |
| **Instance** | `get_instance_status` `list_code_locations` `list_backfills` | Global health check, daemon status, code location errors |
| **Actions** | `launch_job` `terminate_run` `reload_code_location` | Re-run failed jobs, stop stuck runs, reload after deploy |

> Actions are opt-in: set `DAGSTER_READ_ONLY=false` to enable write operations.

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A running [Dagster](https://dagster.io/) instance (self-hosted or Cloud)

### Install

The package is published on [PyPI](https://pypi.org/project/dagster-mcp/).

**Option A — run directly with uvx (no install needed):**

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

### Add to your MCP client

<details>
<summary><strong>Claude Code</strong></summary>

Add to `~/.claude/settings.json`:

**Single env:**

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

**Multiple envs:**

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

</details>

<details>
<summary><strong>Claude Desktop</strong></summary>

Add to `claude_desktop_config.json`:

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

</details>

<details>
<summary><strong>From a local clone</strong></summary>

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

</details>

## Tool reference

### Runs

| Tool | Description |
|------|-------------|
| `get_runs` | List recent runs, filter by job name and/or status |
| `get_run_status` | Get status, config, tags, and run lineage (re-execution chain via rootRunId/parentRunId) |
| `get_run_logs` | Get structured log events with pagination and optional level filtering (`ERROR`, `WARNING`, `INFO`) |
| `get_run_stats` | Get per-step execution stats: timing, materializations, expectation results |
| `get_run_failure_summary` | **Consolidated failure diagnosis** — failed steps, root cause error, step durations, and suggestions in one call |

### Assets

| Tool | Description |
|------|-------------|
| `search_assets` | Discover assets by key prefix or group name |
| `get_asset_details` | Get description, upstream/downstream dependencies, partitions, latest materialization |
| `get_recent_materializations` | Get materialization history with metadata for an asset |
| `get_asset_health` | **Consolidated health view** — staleness, freshness policy, last run status (works with single asset or entire group) |

### Jobs, Schedules & Sensors

| Tool | Description |
|------|-------------|
| `list_jobs` | List all jobs across all code locations (use to find names for `launch_job`) |
| `list_schedules` | List schedules with status (RUNNING/STOPPED), cron, target job, next tick |
| `list_sensors` | List sensors with status and target jobs |
| `get_tick_history` | Tick-by-tick history for a schedule or sensor — **essential for detecting silent failures** |

### Instance & Code Locations

| Tool | Description |
|------|-------------|
| `get_instance_status` | **Start here** — global health: daemon status, queued run count, code location errors |
| `list_code_locations` | List all code locations and their load status |
| `list_backfills` | List recent backfills with status and partition progress |

### Write Operations

| Tool | Description |
|------|-------------|
| `launch_job` | Launch a job or materialize specific assets (supports tags and asset selection) |
| `terminate_run` | Stop a stuck or runaway run |
| `reload_code_location` | Reload a code location after deploy |

> Write tools require `DAGSTER_READ_ONLY=false` (default is `true`).

## How it differs from the official Dagster MCP

| | **dagster-mcp** (this project) | **dg[mcp]** ([official](https://dagster.io/blog/dagsters-mcp-server)) |
|---|---|---|
| **Purpose** | Monitor and operate a **running** instance | Write Dagster **code** and scaffold components |
| **When** | Operations time | Development time |
| **What it does** | Inspect runs, read logs, check assets, launch jobs | Generate definitions, use `dg` CLI, build pipelines |

They serve different purposes and work well together.

## Compatibility

Tested with Dagster 1.6+. The `RunsFilter` field name (`jobName` vs `pipelineName`) is auto-detected via schema introspection, so the server works across all Dagster versions without configuration.

## Development

```bash
uv sync --extra dev
uv run ruff check dagster_mcp/    # lint
uv run pytest                     # tests (98 tests)
uv run python -m dagster_mcp      # start server locally
```

## License

MIT
