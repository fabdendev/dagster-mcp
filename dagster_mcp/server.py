"""Dagster MCP server — GraphQL wrapper for self-hosted and Dagster Cloud instances."""

import json
import os
import httpx
from fastmcp import FastMCP

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
DAGSTER_API_TOKEN = os.environ.get("DAGSTER_API_TOKEN", "")
DAGSTER_EXTRA_HEADERS = os.environ.get("DAGSTER_EXTRA_HEADERS", "")
READ_ONLY = os.environ.get("DAGSTER_READ_ONLY", "true").lower() in ("true", "1", "yes")

# Multi-env support
_DAGSTER_ENVS_RAW = os.environ.get("DAGSTER_ENVS", "")
_DAGSTER_DEFAULT_ENV = os.environ.get("DAGSTER_DEFAULT_ENV", "")


def _parse_dagster_envs(raw: str) -> dict[str, dict]:
    if not raw:
        return {}
    try:
        envs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "DAGSTER_ENVS must be a valid JSON object "
            '(example: \'{"prod": {"url": "https://prod.dagster.io", "token": "..."}, '
            '"dev": {"url": "http://localhost:3000"}}\').'
        ) from exc
    if not isinstance(envs, dict):
        raise RuntimeError("DAGSTER_ENVS must be a JSON object mapping env names to configs.")
    return envs


_ENVS: dict[str, dict] = _parse_dagster_envs(_DAGSTER_ENVS_RAW)

_mode = "read-only" if READ_ONLY else "read-write"
_env_info = (
    f"Available environments: {', '.join(_ENVS)}. Pass env=<name> to each tool. "
    if _ENVS
    else ""
)
mcp = FastMCP(
    "dagster",
    instructions=(
        f"Use these tools to monitor and operate a running Dagster instance ({_mode} mode). "
        f"{_env_info}"
        "Start with list_jobs or get_runs to explore what is available, then "
        "drill into specific runs, assets, schedules, or sensors as needed."
    ),
)


def _resolve_connection(env: str | None) -> tuple[str, str, str]:
    """Return (graphql_url, api_token, extra_headers_json) for the given env.

    In single-env mode (DAGSTER_ENVS not set), env is ignored and module-level
    DAGSTER_URL / DAGSTER_API_TOKEN / DAGSTER_EXTRA_HEADERS are used.
    """
    if not _ENVS:
        return (
            f"{DAGSTER_URL.rstrip('/')}/graphql",
            DAGSTER_API_TOKEN,
            DAGSTER_EXTRA_HEADERS,
        )

    name = env or _DAGSTER_DEFAULT_ENV
    if not name:
        if len(_ENVS) == 1:
            name = next(iter(_ENVS))
        else:
            raise RuntimeError(
                f"Multiple Dagster envs configured but no env specified. "
                f"Available: {', '.join(_ENVS)}. "
                "Pass env=<name> to the tool or set DAGSTER_DEFAULT_ENV."
            )

    if name not in _ENVS:
        raise RuntimeError(
            f"Unknown Dagster env '{name}'. Available: {', '.join(_ENVS)}."
        )

    cfg = _ENVS[name]
    url = cfg.get("url", "http://localhost:3000")
    token = cfg.get("token", "")
    extra = cfg.get("extra_headers", "")
    return f"{url.rstrip('/')}/graphql", token, extra


def _build_headers(
    api_token: str | None = None,
    extra_headers_json: str | None = None,
) -> dict[str, str]:
    if api_token is None:
        api_token = DAGSTER_API_TOKEN
    if extra_headers_json is None:
        extra_headers_json = DAGSTER_EXTRA_HEADERS

    headers: dict[str, str] = {}
    if api_token:
        headers["Dagster-Cloud-Api-Token"] = api_token
    if extra_headers_json:
        try:
            extra_headers = json.loads(extra_headers_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "DAGSTER_EXTRA_HEADERS must be a valid JSON object "
                "(example: '{\"Authorization\":\"Bearer token\"}')."
            ) from exc

        if not isinstance(extra_headers, dict):
            raise RuntimeError("DAGSTER_EXTRA_HEADERS must be a JSON object.")

        invalid_pairs = [
            (key, value)
            for key, value in extra_headers.items()
            if not isinstance(key, str) or not isinstance(value, str)
        ]
        if invalid_pairs:
            raise RuntimeError(
                "DAGSTER_EXTRA_HEADERS keys and values must be strings."
            )

        headers.update(extra_headers)
    return headers


# ---------------------------------------------------------------------------
# RunsFilter introspection — detect whether the instance uses "jobName" or
# "pipelineName" as the filter field (varies across Dagster versions).
# ---------------------------------------------------------------------------

_runs_filter_job_field: dict[str, str] = {}  # graphql_url -> field name

_INTROSPECTION_QUERY = '{ __type(name: "RunsFilter") { inputFields { name } } }'


def _get_runs_filter_job_field(env: str | None = None) -> str:
    """Return the correct RunsFilter field name for job filtering."""
    graphql_url, api_token, extra_headers_json = _resolve_connection(env)

    if graphql_url in _runs_filter_job_field:
        return _runs_filter_job_field[graphql_url]

    try:
        headers = _build_headers(api_token, extra_headers_json)
        response = httpx.post(
            graphql_url,
            json={"query": _INTROSPECTION_QUERY},
            headers=headers,
            timeout=30,
        )
        data = response.json()
        fields = {
            f["name"]
            for f in data.get("data", {}).get("__type", {}).get("inputFields", [])
        }
        if "jobName" in fields:
            field = "jobName"
        elif "pipelineName" in fields:
            field = "pipelineName"
        else:
            field = "jobName"
    except Exception:
        field = "jobName"

    _runs_filter_job_field[graphql_url] = field
    return field


def gql(query: str, variables: dict | None = None, env: str | None = None) -> dict:
    graphql_url, api_token, extra_headers_json = _resolve_connection(env)
    headers = _build_headers(api_token, extra_headers_json)
    try:
        response = httpx.post(
            graphql_url,
            json={"query": query, "variables": variables or {}},
            headers=headers,
            timeout=30,
        )
    except httpx.ConnectError:
        base_url = graphql_url.removesuffix("/graphql")
        raise RuntimeError(
            f"Cannot connect to Dagster at {base_url}. "
            "Check that DAGSTER_URL is correct and the instance is running."
        )
    except httpx.TimeoutException:
        base_url = graphql_url.removesuffix("/graphql")
        raise RuntimeError(
            f"Request to Dagster at {base_url} timed out after 30s."
        )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Dagster returned HTTP {response.status_code}: {response.text[:500]}"
        )
    data = response.json()
    if "errors" in data:
        messages = [e.get("message", str(e)) for e in data["errors"]]
        raise RuntimeError("Dagster GraphQL error: " + "; ".join(messages))
    return data["data"]


# ── Runs ──────────────────────────────────────────────────────────────────────


@mcp.tool()
def get_runs(
    job_name: str | None = None,
    statuses: list[str] | None = None,
    limit: int = 10,
    env: str | None = None,
) -> list[dict]:
    """List recent pipeline runs. Start here to discover what has been running.

    Returns runId, status, jobName, startTime, endTime, and tags for each run.
    Use the returned runId to drill into details with get_run_status,
    get_run_logs, get_run_stats, or get_run_failure_summary.

    Filtering:
    - job_name: filter by job (e.g. 'my_etl_job')
    - statuses: filter by one or more statuses.
      Valid values: 'SUCCESS', 'FAILURE', 'CANCELED', 'STARTED', 'QUEUED',
      'STARTING', 'CANCELING', 'NOT_STARTED'.
      Examples: ['FAILURE'], ['FAILURE', 'CANCELED'], ['STARTED', 'QUEUED']
    - limit: max runs to return (default 10)

    Typical workflows:
    - Find recent failures: get_runs(statuses=['FAILURE'])
    - Check if a job ran today: get_runs(job_name='my_job', limit=5)
    - Monitor active runs: get_runs(statuses=['STARTED', 'QUEUED'])
    """
    query = """
    query Runs($limit: Int!, $filter: RunsFilter) {
      runsOrError(limit: $limit, filter: $filter) {
        ... on Runs {
          results {
            runId
            status
            jobName
            startTime
            endTime
            tags { key value }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    filter_var: dict = {}
    if statuses:
        filter_var["statuses"] = statuses
    if job_name:
        field = _get_runs_filter_job_field(env)
        filter_var[field] = job_name
    data = gql(query, {"limit": limit, "filter": filter_var or None}, env=env)
    runs = data.get("runsOrError", {})
    return runs.get("results", [])


@mcp.tool()
def get_run_status(run_id: str, env: str | None = None) -> dict:
    """Get full details for a single run: status, config, tags, and run lineage.

    Returns: runId, status, startTime, endTime, jobName, tags, runConfigYaml,
    rootRunId, parentRunId, resolvedOpSelection.

    Use rootRunId and parentRunId to understand re-execution chains — if
    parentRunId is set, this run was re-executed from another run.
    resolvedOpSelection shows which steps were selected for re-execution.

    When to use: after get_runs to inspect a specific run, or to check
    whether a run is a re-execution of a previous one.
    """
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          startTime
          endTime
          jobName
          tags { key value }
          runConfigYaml
          rootRunId
          parentRunId
          resolvedOpSelection
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("runOrError", {})


@mcp.tool()
def get_run_logs(
    run_id: str,
    cursor: str | None = None,
    limit: int = 100,
    level_filter: str | None = None,
    env: str | None = None,
) -> dict:
    """Get structured log events for a run, with optional severity filtering and pagination.

    Returns events with __typename, timestamp, message, level, and (where applicable)
    stepKey and error details. Events include step starts/completions, failures,
    retries, materializations, and run-level events.

    Parameters:
    - run_id: the run to fetch logs for
    - level_filter: only return events at this level or above.
      Values: 'DEBUG', 'INFO', 'WARNING', 'ERROR'. When set to 'ERROR',
      also includes ExecutionStepFailureEvent and RunFailureEvent regardless
      of their level field. Default: None (return all events).
    - cursor: pagination cursor returned in previous response. Pass the
      cursor from the last call to get the next page.
    - limit: max events per page (default 100)

    When to use: to investigate what happened during a run. For a quick
    failure diagnosis, prefer get_run_failure_summary instead — it returns
    a consolidated view in a single call. Use get_run_logs when you need
    the full event stream or want to filter by level.
    """
    query = """
    query RunLogs($runId: ID!, $afterCursor: String, $limit: Int!) {
      logsForRun(runId: $runId, afterCursor: $afterCursor, limit: $limit) {
        ... on EventConnection {
          cursor
          hasMore
          events {
            __typename
            ... on MessageEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on LogsCapturedEvent {
              timestamp
              message
              level
              stepKey
              logKey
              fileKey
            }
            ... on ExecutionStepStartEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ExecutionStepSuccessEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ExecutionStepOutputEvent {
              timestamp
              message
              level
              stepKey
              outputName
            }
            ... on ExecutionStepInputEvent {
              timestamp
              message
              level
              stepKey
              inputName
            }
            ... on ExecutionStepFailureEvent {
              timestamp
              message
              level
              stepKey
              error { message causes { message } }
            }
            ... on RunFailureEvent {
              timestamp
              message
              level
              error { message causes { message } }
            }
            ... on ExecutionStepUpForRetryEvent {
              timestamp
              message
              level
              stepKey
              secondsToWait
              error { message causes { message } }
            }
            ... on MaterializationEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on ObjectStoreOperationEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on HandledOutputEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on LoadedInputEvent {
              timestamp
              message
              level
              stepKey
            }
            ... on EngineEvent {
              timestamp
              message
              level
              stepKey
              error { message causes { message } }
            }
            ... on RunStartEvent {
              timestamp
              message
              level
            }
            ... on RunSuccessEvent {
              timestamp
              message
              level
            }
            ... on RunStartingEvent {
              timestamp
              message
              level
            }
            ... on RunEnqueuedEvent {
              timestamp
              message
              level
            }
            ... on RunDequeuedEvent {
              timestamp
              message
              level
            }
            ... on RunCancelingEvent {
              timestamp
              message
              level
            }
            ... on RunCanceledEvent {
              timestamp
              message
              level
            }
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id, "afterCursor": cursor, "limit": limit}, env=env)
    result = data.get("logsForRun", {})

    if level_filter and "events" in result:
        upper = level_filter.upper()
        error_types = ("ExecutionStepFailureEvent", "RunFailureEvent")
        result["events"] = [
            e for e in result["events"]
            if e.get("level") == upper
            or (upper == "ERROR" and e.get("__typename") in error_types)
        ]

    return result


@mcp.tool()
def get_run_stats(run_id: str, env: str | None = None) -> dict:
    """Get per-step execution statistics for a run: timing, materializations, and expectations.

    Returns runId, status, and a stepStats array where each entry has:
    stepKey, status, startTime, endTime, materializations (with labels),
    and expectationResults (with success flag and labels).

    When to use: to find slow steps (compare startTime/endTime), check which
    steps materialized assets, or verify expectation results.
    For failed runs, prefer get_run_failure_summary which includes step stats
    alongside error details and suggestions.
    """
    query = """
    query RunStats($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          stepStats {
            stepKey
            status
            startTime
            endTime
            materializations { label }
            expectationResults { success label }
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("runOrError", {})


@mcp.tool()
def get_run_failure_summary(run_id: str, env: str | None = None) -> dict:
    """Get a consolidated failure diagnosis for a run in a single call.

    This is the BEST tool to use when investigating a failed or canceled run.
    It combines status, step stats, and error logs into one response, avoiding
    the need to call get_run_status + get_run_logs + get_run_stats separately.

    Returns:
    - status, job_name, duration_seconds
    - failed_steps: list of {step_key, duration, error} for each failed step
    - root_cause_error: the RunFailureEvent error (if any)
    - all_step_durations: timing for every step (not just failed ones)
    - suggestions: automated diagnostic hints (e.g. 'Multiple steps failed',
      'Step was retried before failing', 'Run was canceled')

    If the run did not fail, returns {message: 'Run did not fail.'}.

    When to use: always prefer this over get_run_logs for failed runs.
    Use get_run_logs only when you need the full event stream.
    """
    # 1. Fetch run status + step stats in one query
    status_query = """
    query FailureSummary($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run {
          runId
          status
          jobName
          startTime
          endTime
          stepStats {
            stepKey
            status
            startTime
            endTime
          }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    run_data = gql(status_query, {"runId": run_id}, env=env).get("runOrError", {})

    if "message" in run_data:
        return run_data

    status = run_data.get("status", "")
    if status not in ("FAILURE", "CANCELED"):
        return {"run_id": run_id, "status": status, "message": "Run did not fail."}

    # 2. Collect error events from logs (paginate up to 500 events)
    error_events: list[dict] = []
    cursor = None
    for _ in range(5):
        log_query = """
        query FailureLogs($runId: ID!, $afterCursor: String) {
          logsForRun(runId: $runId, afterCursor: $afterCursor, limit: 100) {
            ... on EventConnection {
              cursor
              hasMore
              events {
                __typename
                ... on ExecutionStepFailureEvent {
                  timestamp
                  stepKey
                  error { message causes { message } }
                }
                ... on RunFailureEvent {
                  timestamp
                  error { message causes { message } }
                }
                ... on ExecutionStepUpForRetryEvent {
                  timestamp
                  stepKey
                  secondsToWait
                  error { message causes { message } }
                }
              }
            }
            ... on RunNotFoundError { message }
          }
        }
        """
        log_data = gql(log_query, {"runId": run_id, "afterCursor": cursor}, env=env).get("logsForRun", {})
        events = log_data.get("events", [])
        for e in events:
            if e.get("__typename") in (
                "ExecutionStepFailureEvent", "RunFailureEvent", "ExecutionStepUpForRetryEvent"
            ):
                error_events.append(e)
        if not log_data.get("hasMore"):
            break
        cursor = log_data.get("cursor")

    # 3. Build step durations
    step_stats = run_data.get("stepStats", [])
    all_step_durations = []
    for s in step_stats:
        dur = None
        if s.get("startTime") and s.get("endTime"):
            dur = round(s["endTime"] - s["startTime"], 2)
        all_step_durations.append({
            "step_key": s["stepKey"],
            "status": s["status"],
            "duration_seconds": dur,
        })

    # 4. Build failed steps with errors
    failed_step_keys = {s["stepKey"] for s in step_stats if s["status"] == "FAILURE"}
    step_errors: dict[str, dict] = {}
    for e in error_events:
        sk = e.get("stepKey")
        if sk and sk in failed_step_keys and sk not in step_errors:
            step_errors[sk] = e.get("error", {})

    failed_steps = []
    for s in step_stats:
        if s["stepKey"] in failed_step_keys:
            dur = None
            if s.get("startTime") and s.get("endTime"):
                dur = round(s["endTime"] - s["startTime"], 2)
            failed_steps.append({
                "step_key": s["stepKey"],
                "duration_seconds": dur,
                "error": step_errors.get(s["stepKey"], {}),
            })

    # 5. Root cause error (run-level failure or first step failure)
    root_cause = None
    run_failure = [e for e in error_events if e.get("__typename") == "RunFailureEvent"]
    if run_failure:
        root_cause = run_failure[0].get("error", {})
    elif failed_steps:
        root_cause = failed_steps[0].get("error", {})

    # 6. Suggestions
    suggestions: list[str] = []
    retries = [e for e in error_events if e.get("__typename") == "ExecutionStepUpForRetryEvent"]
    if retries:
        retry_keys = {e["stepKey"] for e in retries}
        suggestions.append(f"Steps retried before failing: {', '.join(sorted(retry_keys))}")
    if len(failed_steps) > 1:
        suggestions.append(
            f"Multiple steps failed ({len(failed_steps)}). "
            f"First failure: {failed_steps[0]['step_key']} — downstream failures may be cascading."
        )
    if status == "CANCELED":
        suggestions.append("Run was canceled, not all steps may have executed.")

    run_dur = None
    if run_data.get("startTime") and run_data.get("endTime"):
        run_dur = round(run_data["endTime"] - run_data["startTime"], 2)

    return {
        "run_id": run_id,
        "status": status,
        "job_name": run_data.get("jobName"),
        "duration_seconds": run_dur,
        "failed_steps": failed_steps,
        "root_cause_error": root_cause,
        "all_step_durations": all_step_durations,
        "suggestions": suggestions,
    }


# ── Assets ────────────────────────────────────────────────────────────────────


@mcp.tool()
def get_recent_materializations(
    asset_key: str,
    limit: int = 5,
    env: str | None = None,
) -> list[dict]:
    """Get the most recent materializations for an asset, with metadata.

    Returns a list of materializations, each with: runId, timestamp,
    assetKey, and metadataEntries (labels, numeric values, text).

    - asset_key: the asset name as a string (e.g. 'my_daily_report')
    - limit: max materializations to return (default 5)

    When to use: to check when an asset was last materialized, track
    materialization frequency, or inspect metadata from recent runs.
    For a broader health view (including staleness and freshness),
    use get_asset_health instead.
    """
    query = """
    query AssetRuns($assetKey: AssetKeyInput!, $limit: Int!) {
      assetOrError(assetKey: $assetKey) {
        ... on Asset {
          assetMaterializations(limit: $limit) {
            runId
            timestamp
            assetKey { path }
            metadataEntries {
              label
              ... on IntMetadataEntry { intValue }
              ... on FloatMetadataEntry { floatValue }
              ... on TextMetadataEntry { text }
            }
          }
        }
      }
    }
    """
    data = gql(query, {"assetKey": {"path": [asset_key]}, "limit": limit}, env=env)
    asset = data.get("assetOrError", {})
    return asset.get("assetMaterializations", [])


@mcp.tool()
def get_asset_details(asset_keys: list[str], env: str | None = None) -> list[dict]:
    """Get detailed metadata for one or more assets: description, lineage, and partitions.

    - asset_keys: list of asset name strings (e.g. ['my_extract', 'my_load'])

    Returns per asset: assetKey, description, groupName, op name,
    isObservable, isPartitioned, partitionDefinition, dependencyKeys
    (upstream assets), dependedByKeys (downstream assets), and the
    latest materialization (runId + timestamp).

    When to use: to understand an asset's lineage (what it depends on
    and what depends on it), check if it's partitioned, or get its
    description. Use search_assets first if you don't know the exact key.
    """
    query = """
    query AssetDetails($assetKeys: [AssetKeyInput!]!) {
      assetNodes(assetKeys: $assetKeys) {
        assetKey { path }
        description
        groupName
        op { name }
        isObservable
        isPartitioned
        partitionDefinition { description }
        dependencyKeys { path }
        dependedByKeys { path }
        assetMaterializations(limit: 1) {
          runId
          timestamp
        }
      }
    }
    """
    keys = [{"path": [k]} for k in asset_keys]
    data = gql(query, {"assetKeys": keys}, env=env)
    return data.get("assetNodes", [])


@mcp.tool()
def search_assets(
    prefix: str | None = None,
    group: str | None = None,
    env: str | None = None,
) -> list[dict]:
    """Search and list assets by name prefix or group. Use this to discover assets.

    Returns per asset: assetKey, groupName, description, isPartitioned, op name.

    - prefix: case-insensitive substring match on any part of the asset key
      (e.g. 'raw_' finds 'raw_orders', 'raw_users')
    - group: exact match on groupName (case-insensitive, e.g. 'analytics')
    - Both filters can be combined.
    - If neither is passed, returns ALL assets.

    When to use: to discover available assets before calling get_asset_details
    or get_asset_health. Use prefix for fuzzy search, group for scoped listing.
    """
    query = """
    query AllAssets {
      assetNodes {
        assetKey { path }
        groupName
        description
        isPartitioned
        op { name }
      }
    }
    """
    data = gql(query, env=env)
    nodes = data.get("assetNodes", [])
    if prefix:
        prefix_lower = prefix.lower()
        nodes = [n for n in nodes if any(prefix_lower in p.lower() for p in n["assetKey"]["path"])]
    if group:
        group_lower = group.lower()
        nodes = [n for n in nodes if (n.get("groupName") or "").lower() == group_lower]
    return nodes


@mcp.tool()
def get_asset_health(asset_key_or_group: str, env: str | None = None) -> list[dict]:
    """Get a consolidated health view for a single asset or all assets in a group.

    This is the BEST tool to assess whether assets are healthy and up-to-date.

    - asset_key_or_group: pass either a single asset key (e.g. 'my_report')
      or a group name (e.g. 'analytics'). If it matches a group, returns
      health for ALL assets in that group.

    Returns per asset:
    - asset_key, group, description
    - last_materialization: {run_id, timestamp, status} of the latest run
    - freshness_policy: {maximum_lag_minutes, cron_schedule} if defined
    - staleness: {is_stale, reasons[]} explaining why the asset is stale

    When to use: to check if critical assets are fresh, find stale assets
    in a group, or verify that recent materializations succeeded.
    Prefer this over get_recent_materializations when you need a health
    assessment rather than raw materialization history.
    """
    # First try as a group — fetch all assets and filter
    all_query = """
    query AllAssets {
      assetNodes {
        assetKey { path }
        groupName
      }
    }
    """
    all_data = gql(all_query, env=env)
    all_nodes = all_data.get("assetNodes", [])

    # Check if it's a group name
    group_keys = [
        n["assetKey"]["path"]
        for n in all_nodes
        if (n.get("groupName") or "").lower() == asset_key_or_group.lower()
    ]

    if group_keys:
        asset_keys_input = [{"path": k} for k in group_keys]
    else:
        asset_keys_input = [{"path": [asset_key_or_group]}]

    # Fetch health details
    health_query = """
    query AssetHealth($assetKeys: [AssetKeyInput!]!) {
      assetNodes(assetKeys: $assetKeys) {
        assetKey { path }
        groupName
        freshnessPolicy { maximumLagMinutes cronSchedule }
        staleCauses { key { path } reason dependency { path } }
        assetMaterializations(limit: 1) {
          runId
          timestamp
        }
      }
    }
    """
    health_data = gql(health_query, {"assetKeys": asset_keys_input}, env=env)
    nodes = health_data.get("assetNodes", [])

    if not nodes:
        return [{"asset_key": asset_key_or_group, "message": "Asset not found."}]

    # For each asset, get the latest run status if there's a materialization
    run_ids = set()
    for n in nodes:
        mats = n.get("assetMaterializations", [])
        if mats:
            run_ids.add(mats[0]["runId"])

    run_statuses: dict[str, str] = {}
    if run_ids:
        runs_query = """
        query RunStatuses($filter: RunsFilter) {
          runsOrError(filter: $filter, limit: 100) {
            ... on Runs {
              results { runId status }
            }
          }
        }
        """
        runs_data = gql(runs_query, {"filter": {"runIds": list(run_ids)}}, env=env)
        for r in runs_data.get("runsOrError", {}).get("results", []):
            run_statuses[r["runId"]] = r["status"]

    results = []
    for n in nodes:
        mats = n.get("assetMaterializations", [])
        last_mat = None
        latest_run_status = None
        if mats:
            last_mat = {"run_id": mats[0]["runId"], "timestamp": mats[0]["timestamp"]}
            latest_run_status = run_statuses.get(mats[0]["runId"])

        fp = n.get("freshnessPolicy")
        freshness_policy = None
        if fp:
            freshness_policy = {
                "max_lag_minutes": fp.get("maximumLagMinutes"),
                "cron": fp.get("cronSchedule"),
            }

        stale_causes = n.get("staleCauses", [])
        results.append({
            "asset_key": n["assetKey"]["path"],
            "group": n.get("groupName"),
            "last_materialization": last_mat,
            "latest_run_status": latest_run_status,
            "freshness_policy": freshness_policy,
            "stale": len(stale_causes) > 0,
            "stale_causes": [c.get("reason", "") for c in stale_causes],
        })

    return results


# ── Jobs & Schedules & Sensors ────────────────────────────────────────────────


@mcp.tool()
def list_jobs(env: str | None = None) -> list[dict]:
    """List all jobs across all code locations. Use this to discover available jobs.

    Returns per job: repository name, code location name, job name, and description.

    When to use: as a starting point to explore what jobs exist, or to find the
    exact job name and repository_location needed for launch_job.
    """
    query = """
    query ListJobs {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            jobs {
              name
              description
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for job in repo.get("jobs", []):
            result.append({
                "repository": repo["name"],
                "location": repo["location"]["name"],
                "job": job["name"],
                "description": job.get("description", ""),
            })
    return result


@mcp.tool()
def list_schedules(env: str | None = None) -> list[dict]:
    """List all schedules with their status, cron expression, target job, and next tick.

    Returns per schedule: name, cron expression, status (RUNNING/STOPPED),
    next_tick timestamp, target job name, repository, and code location.

    When to use: to check which schedules are active, verify cron timing,
    or find schedules that are stopped and might need attention.
    If a schedule is RUNNING but jobs aren't executing, use
    get_tick_history to inspect recent ticks for errors.
    """
    query = """
    query ListSchedules {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            schedules {
              name
              cronSchedule
              scheduleState { status }
              futureTicks(limit: 1) { results { timestamp } }
              pipelineName
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for sched in repo.get("schedules", []):
            next_ticks = sched.get("futureTicks", {}).get("results", [])
            result.append({
                "repository": repo["name"],
                "location": repo["location"]["name"],
                "schedule": sched["name"],
                "cron": sched.get("cronSchedule"),
                "status": sched.get("scheduleState", {}).get("status"),
                "next_tick": next_ticks[0]["timestamp"] if next_ticks else None,
                "job": sched.get("pipelineName"),
            })
    return result


@mcp.tool()
def list_sensors(env: str | None = None) -> list[dict]:
    """List all sensors with their status and target jobs.

    Returns per sensor: name, status (RUNNING/STOPPED), list of target job names,
    repository, and code location.

    When to use: to check which sensors are active and what jobs they trigger.
    If a sensor is RUNNING but not producing runs, use get_tick_history to
    inspect recent ticks — it will show skipped ticks, errors, or runs launched.
    """
    query = """
    query ListSensors {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            sensors {
              name
              sensorState { status }
              targets { pipelineName }
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, env=env)
    repos = data.get("repositoriesOrError", {}).get("nodes", [])
    result = []
    for repo in repos:
        for sensor in repo.get("sensors", []):
            targets = [t["pipelineName"] for t in sensor.get("targets", [])]
            result.append({
                "repository": repo["name"],
                "location": repo["location"]["name"],
                "sensor": sensor["name"],
                "status": sensor.get("sensorState", {}).get("status"),
                "targets": targets,
            })
    return result


@mcp.tool()
def get_tick_history(
    instigator_name: str,
    instigator_type: str,
    limit: int = 20,
    env: str | None = None,
) -> dict:
    """Get recent tick history for a schedule or sensor — essential for detecting silent failures.

    - instigator_name: exact name of the schedule or sensor (from list_schedules/list_sensors)
    - instigator_type: 'SCHEDULE' or 'SENSOR'
    - limit: max ticks to return (default 20)

    Returns per tick: tick_id, status (SUCCESS/FAILURE/SKIPPED), timestamp,
    error message (if failed), and run_ids (runs launched by this tick).

    When to use: when a schedule or sensor is RUNNING but data is not being
    produced. Common patterns to look for:
    - All ticks SKIPPED: sensor condition not met, or misconfigured
    - Ticks with FAILURE status: the schedule/sensor code is erroring
    - Ticks with SUCCESS but empty run_ids: sensor evaluated but decided not to launch
    - Missing ticks: daemon may be unhealthy (check get_instance_status)
    """
    instigator_type = instigator_type.upper()
    if instigator_type not in ("SCHEDULE", "SENSOR"):
        raise ValueError("instigator_type must be 'SCHEDULE' or 'SENSOR'.")

    query = """
    query TickHistory($instigatorType: InstigationType!, $limit: Int!) {
      instigationStatesOrError(instigationType: $instigatorType) {
        ... on InstigationStates {
          results {
            name
            instigationType
            ticks(limit: $limit) {
              tickId
              status
              timestamp
              error { message }
              runIds
            }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"instigatorType": instigator_type, "limit": limit}, env=env)
    states = data.get("instigationStatesOrError", {})

    if "message" in states:
        return states

    results = states.get("results", [])
    for r in results:
        if r["name"] == instigator_name:
            return {
                "name": r["name"],
                "instigator_type": r["instigationType"],
                "ticks": [
                    {
                        "tick_id": t["tickId"],
                        "status": t["status"],
                        "timestamp": t["timestamp"],
                        "error": t.get("error", {}).get("message") if t.get("error") else None,
                        "run_ids": t.get("runIds", []),
                    }
                    for t in r.get("ticks", [])
                ],
            }

    return {"name": instigator_name, "instigator_type": instigator_type,
            "message": f"{instigator_type.capitalize()} '{instigator_name}' not found."}


# ── Code Locations ────────────────────────────────────────────────────────────


@mcp.tool()
def list_code_locations(env: str | None = None) -> list[dict]:
    """List all code locations and their load status.

    Returns per location: name, loadStatus (LOADED/LOADING), and either the
    repositories within it or a PythonError if loading failed.

    When to use: after a deployment to verify code locations loaded correctly,
    or when get_instance_status reports code location errors.
    If a location failed to load, use reload_code_location to retry.
    """
    query = """
    query CodeLocations {
      workspaceOrError {
        ... on Workspace {
          locationEntries {
            name
            loadStatus
            locationOrLoadError {
              ... on RepositoryLocation {
                name
                repositories { name }
              }
              ... on PythonError { message }
            }
          }
        }
      }
    }
    """
    data = gql(query, env=env)
    workspace = data.get("workspaceOrError", {})
    return workspace.get("locationEntries", [])


@mcp.tool()
def get_instance_status(env: str | None = None) -> dict:
    """Get a global health check of the Dagster instance. START HERE for any monitoring workflow.

    Returns:
    - healthy: boolean — true only if all required daemons are healthy AND
      no code locations have errors
    - daemons: list of {type, healthy, last_heartbeat, required} for each daemon
      (scheduler, sensor, run coordinator, etc.)
    - queued_runs_count: number of runs waiting in queue (high count = bottleneck)
    - code_location_errors: list of {name, error} for locations that failed to load

    When to use: as the FIRST call in any diagnostic or monitoring flow.
    If healthy=false, check daemons for unhealthy entries and
    code_location_errors for loading failures.
    Follow up with list_code_locations or get_runs as needed.
    """
    query = """
    query InstanceStatus {
      instance {
        daemonHealth {
          allDaemonStatuses {
            daemonType
            required
            healthy
            lastHeartbeatTime
          }
        }
      }
      runsOrError(filter: {statuses: [QUEUED]}, limit: 100) {
        ... on Runs {
          results { runId }
        }
        ... on PythonError { message }
      }
      workspaceOrError {
        ... on Workspace {
          locationEntries {
            name
            loadStatus
            locationOrLoadError {
              ... on PythonError { message }
            }
          }
        }
      }
    }
    """
    data = gql(query, env=env)

    # Daemons
    daemon_statuses = (
        data.get("instance", {})
        .get("daemonHealth", {})
        .get("allDaemonStatuses", [])
    )
    daemons = [
        {
            "type": d["daemonType"],
            "healthy": d["healthy"],
            "last_heartbeat": d.get("lastHeartbeatTime"),
            "required": d["required"],
        }
        for d in daemon_statuses
    ]

    # Queued runs
    runs_or_error = data.get("runsOrError", {})
    queued_runs = runs_or_error.get("results", [])
    queued_count = len(queued_runs)

    # Code location errors
    location_entries = (
        data.get("workspaceOrError", {}).get("locationEntries", [])
    )
    code_location_errors = []
    for loc in location_entries:
        err = loc.get("locationOrLoadError", {})
        if "message" in err:
            code_location_errors.append({"name": loc["name"], "error": err["message"]})

    all_required_healthy = all(
        d["healthy"] for d in daemons if d["required"]
    )
    healthy = all_required_healthy and len(code_location_errors) == 0

    return {
        "healthy": healthy,
        "daemons": daemons,
        "queued_runs_count": queued_count,
        "code_location_errors": code_location_errors,
    }


def reload_code_location(location_name: str, env: str | None = None) -> dict:
    """Reload a code location to pick up new code (e.g. after a deploy).

    - location_name: exact name of the code location (from list_code_locations)

    Returns the new load status. If the location is not found or reload
    is not supported, returns an error message.

    When to use: after deploying new code, or when list_code_locations shows
    a location in an error state. This is equivalent to clicking 'Reload'
    in the Dagster UI.
    """
    query = """
    mutation ReloadLocation($location: String!) {
      reloadRepositoryLocation(repositoryLocationName: $location) {
        ... on WorkspaceLocationEntry {
          name
          loadStatus
          locationOrLoadError {
            ... on RepositoryLocation { name }
            ... on PythonError { message }
          }
        }
        ... on ReloadNotSupported { message }
        ... on RepositoryLocationNotFound { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"location": location_name}, env=env)
    return data.get("reloadRepositoryLocation", {})


# ── Backfills ─────────────────────────────────────────────────────────────────


@mcp.tool()
def list_backfills(limit: int = 10, env: str | None = None) -> list[dict]:
    """List recent asset backfills with their status and partition progress.

    Returns per backfill: backfillId, status, numPartitions, timestamp,
    partitionNames, and partitionSetName.

    - limit: max backfills to return (default 10)

    When to use: to monitor in-progress backfills or review recent ones.
    """
    query = """
    query Backfills($limit: Int!, $cursor: String) {
      partitionBackfillsOrError(cursor: $cursor, limit: $limit) {
        ... on PartitionBackfills {
          results {
            backfillId
            status
            numPartitions
            timestamp
            partitionNames
            partitionSetName
          }
        }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"limit": limit}, env=env)
    return data.get("partitionBackfillsOrError", {}).get("results", [])


# ── Actions ───────────────────────────────────────────────────────────────────


def terminate_run(run_id: str, env: str | None = None) -> dict:
    """Terminate a running or queued Dagster run.

    - run_id: the runId to terminate (get it from get_runs)

    Returns the run's final status on success, or an error message if the
    run was not found or could not be terminated.

    When to use: to stop a stuck, hung, or runaway run. Only works on runs
    with status STARTED or QUEUED. Already-finished runs cannot be terminated.
    """
    query = """
    mutation TerminateRun($runId: String!) {
      terminateRun(runId: $runId) {
        ... on TerminateRunSuccess { run { runId status } }
        ... on TerminateRunFailure { message }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id}, env=env)
    return data.get("terminateRun", {})


def launch_job(
    job_name: str,
    repository_location: str,
    repository_name: str = "__repository__",
    asset_keys: list[str] | None = None,
    tags: dict[str, str] | None = None,
    env: str | None = None,
) -> dict:
    """Launch a job or materialize specific assets. Use list_jobs first to find valid names.

    Required parameters:
    - job_name: name of the job (from list_jobs, e.g. 'my_etl_job')
    - repository_location: code location name (from list_jobs, e.g. 'my_project')
    - repository_name: defaults to '__repository__', override if you have
      multiple repositories in a single code location

    Optional parameters:
    - asset_keys: list of asset key strings to materialize. Use this with the
      job that targets them (often '__ASSET_JOB' or a custom asset job name).
      Example: ['raw_orders', 'clean_orders']
    - tags: dict of key-value tags to attach to the run.
      Example: {'triggered_by': 'dataops_agent', 'priority': 'high'}

    Returns the launched run's runId and status on success, or an error message.

    When to use: to re-run a failed job, trigger an ad-hoc materialization,
    or launch a job with custom tags for tracking. After launching, use
    get_run_status or get_runs to monitor progress.
    """
    execution_metadata: dict = {}
    if tags:
        execution_metadata["tags"] = [
            {"key": k, "value": v} for k, v in tags.items()
        ]

    solid_selection: list[str] | None = None
    if asset_keys:
        solid_selection = asset_keys

    query = """
    mutation LaunchJob(
      $locationName: String!,
      $repoName: String!,
      $jobName: String!,
      $solidSelection: [String!],
      $executionMetadata: ExecutionMetadata
    ) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: $locationName,
          repositoryName: $repoName,
          jobName: $jobName,
          solidSelection: $solidSelection
        },
        runConfigData: {},
        executionMetadata: $executionMetadata
      }) {
        ... on LaunchRunSuccess { run { runId status } }
        ... on InvalidSubsetError { message }
        ... on PythonError { message }
        ... on PresetNotFoundError { message }
        ... on ConflictingExecutionParamsError { message }
        ... on RunConfigValidationInvalid { errors { message } }
      }
    }
    """
    variables = {
        "locationName": repository_location,
        "repoName": repository_name,
        "jobName": job_name,
        "solidSelection": solid_selection,
        "executionMetadata": execution_metadata or None,
    }
    data = gql(query, variables, env=env)
    return data.get("launchRun", {})


# ── Write tools (only registered when DAGSTER_READ_ONLY=false) ────────────────

if not READ_ONLY:
    mcp.tool()(reload_code_location)
    mcp.tool()(terminate_run)
    mcp.tool()(launch_job)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
