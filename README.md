# Dagster MCP

An [MCP](https://modelcontextprotocol.io/) server that wraps the [Dagster](https://dagster.io/) GraphQL API, giving any MCP client (Claude Code, Cursor, etc.) full visibility and control over your Dagster instance.

## What it does

Exposes up to 15 tools that let you inspect and operate a Dagster instance (self-hosted or Dagster Cloud):

- **Runs** — list, inspect, get logs, get step stats
- **Assets** — search, get details, get recent materializations
- **Jobs** — list all jobs across code locations
- **Schedules & Sensors** — list with status, cron, targets
- **Code Locations** — list and reload
- **Backfills** — list recent backfills
- **Actions** — launch jobs, terminate runs, reload code locations _(opt-in, disabled by default)_

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A running [Dagster](https://dagster.io/) instance (self-hosted or Cloud)

### Install

**Option A — run directly with uvx (no clone needed):**

```bash
uvx dagster-mcp
```

**Option B — clone and run:**

```bash
git clone https://github.com/fabdendev/dagster-mcp.git
cd dagster-mcp
uv sync
```

### Configure

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_URL` | Base URL of your Dagster instance | `http://localhost:3000` |
| `DAGSTER_API_TOKEN` | Dagster Cloud API token (leave empty for self-hosted) | _(empty)_ |
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

### Add to Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json`):

**If using uvx:**

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

| Tool | Description |
|------|-------------|
| `get_runs` | List recent runs, filter by job name and/or status |
| `get_run_status` | Get status and info for a specific run |
| `get_run_logs` | Get logs/events for a run (with pagination) |
| `get_run_stats` | Get step-level stats (timing, materializations) |
| `get_recent_materializations` | Get recent materializations for an asset |
| `get_asset_details` | Get details, dependencies, and partitions for assets |
| `search_assets` | Search/list assets by key prefix or group name |
| `list_jobs` | List all jobs across all code locations |
| `list_schedules` | List schedules with status, cron, and next tick |
| `list_sensors` | List sensors with status and target jobs |
| `list_code_locations` | List all code locations and their load status |
| `list_backfills` | List recent backfills with status and progress |
| `reload_code_location` | Reload a code location (e.g. after deploy) **(write)** |
| `terminate_run` | Terminate a running run **(write)** |
| `launch_job` | Launch a job or materialize specific assets **(write)** |

Tools marked **(write)** are only available when `DAGSTER_READ_ONLY=false`.

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
uv run python -m dagster_mcp      # start server locally
```

## License

MIT
