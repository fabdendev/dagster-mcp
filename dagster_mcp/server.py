"""Dagster MCP server — GraphQL wrapper for self-hosted and Dagster Cloud instances."""

import bisect
import json
import os
import httpx
from fastmcp import FastMCP

from dagster_mcp.asset_selection import (
    AssetSelectionSyntaxError,
    evaluate_asset_selection,
    parse_asset_selection,
)

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
    f"Available environments: {', '.join(_ENVS)}. Pass env=<name> to each tool. " if _ENVS else ""
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
        raise RuntimeError(f"Unknown Dagster env '{name}'. Available: {', '.join(_ENVS)}.")

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
                '(example: \'{"Authorization":"Bearer token"}\').'
            ) from exc

        if not isinstance(extra_headers, dict):
            raise RuntimeError("DAGSTER_EXTRA_HEADERS must be a JSON object.")

        invalid_pairs = [
            (key, value)
            for key, value in extra_headers.items()
            if not isinstance(key, str) or not isinstance(value, str)
        ]
        if invalid_pairs:
            raise RuntimeError("DAGSTER_EXTRA_HEADERS keys and values must be strings.")

        headers.update(extra_headers)
    return headers


# ---------------------------------------------------------------------------
# RunsFilter introspection — detect whether the instance uses "jobName" or
# "pipelineName" as the filter field (varies across Dagster versions).
# ---------------------------------------------------------------------------

_runs_filter_job_field: dict[str, str] = {}  # graphql_url -> field name
_type_fields: dict[tuple[str, str, str], frozenset[str]] = {}


def _get_runs_filter_job_field(env: str | None = None) -> str:
    """Return the correct RunsFilter field name for job filtering."""
    graphql_url, _, _ = _resolve_connection(env)

    if graphql_url in _runs_filter_job_field:
        return _runs_filter_job_field[graphql_url]

    try:
        fields = _get_type_fields("RunsFilter", env=env, input_type=True)
    except Exception:
        # Job filtering predates introspection support and historically used
        # jobName, so preserve that fallback without caching a transient failure.
        return "jobName"

    field = "pipelineName" if "pipelineName" in fields and "jobName" not in fields else "jobName"
    _runs_filter_job_field[graphql_url] = field
    return field


def _get_type_fields(
    type_name: str,
    env: str | None = None,
    *,
    input_type: bool = False,
) -> frozenset[str]:
    """Return GraphQL fields for a type, caching only valid introspection results."""
    graphql_url, _, _ = _resolve_connection(env)
    field_kind = "inputFields" if input_type else "fields"
    cache_key = (graphql_url, type_name, field_kind)
    if cache_key in _type_fields:
        return _type_fields[cache_key]

    query = """
    query TypeFields($typeName: String!) {
      __type(name: $typeName) {
        fields { name }
        inputFields { name }
      }
    }
    """
    data = gql(query, {"typeName": type_name}, env=env)
    type_info = data.get("__type")
    field_entries = type_info.get(field_kind) if isinstance(type_info, dict) else None
    if not isinstance(field_entries, list) or any(
        not isinstance(field, dict) or not isinstance(field.get("name"), str)
        for field in field_entries
    ):
        raise RuntimeError(
            f"Dagster returned invalid GraphQL introspection data for {type_name}.{field_kind}."
        )

    fields = frozenset(field["name"] for field in field_entries)
    _type_fields[cache_key] = fields
    return fields


_RESOLVE_ASSET_SELECTION_SCHEMA = {
    "Query": {"assetNodes"},
    "AssetNode": {
        "assetKey",
        "groupName",
        "tags",
        "kinds",
        "owners",
        "dependencyKeys",
        "jobNames",
        "repository",
        "isMaterializable",
        "isExecutable",
        "isObservable",
        "isPartitioned",
    },
}
_MATERIALIZE_ASSETS_SCHEMA = {
    "Query": {
        "assetNodes",
        "assetNodeAdditionalRequiredKeys",
        "assetNodeDefinitionCollisions",
    },
    "AssetNode": {
        "assetKey",
        "groupName",
        "jobNames",
        "isMaterializable",
        "isExecutable",
        "isObservable",
        "isPartitioned",
        "repository",
        "assetChecksOrError",
    },
}


def _dagster_19_compatibility_error(
    tool_name: str,
    required_fields: dict[str, set[str]],
    env: str | None = None,
) -> dict | None:
    """Return a structured error when a Dagster 1.9+ tool lacks schema support."""
    missing_fields = []
    for type_name, required in required_fields.items():
        available = _get_type_fields(type_name, env=env)
        missing_fields.extend(
            f"{type_name}.{field_name}" for field_name in sorted(required - available)
        )
    if not missing_fields:
        return None
    return {
        "message": (
            f"{tool_name} requires Dagster 1.9+; this instance is missing "
            f"GraphQL fields: {', '.join(missing_fields)}."
        )
    }


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
        raise RuntimeError(f"Request to Dagster at {base_url} timed out after 30s.")
    if response.status_code >= 400:
        raise RuntimeError(f"Dagster returned HTTP {response.status_code}: {response.text[:500]}")
    data = response.json()
    if "errors" in data:
        messages = [e.get("message", str(e)) for e in data["errors"]]
        raise RuntimeError("Dagster GraphQL error: " + "; ".join(messages))
    return data["data"]


# GraphQL selection for MetadataEntry — covers the common concrete types.
# Reused wherever a log event exposes metadataEntries.
_METADATA_ENTRIES_FRAGMENT = """
              metadataEntries {
                __typename
                label
                description
                ... on TextMetadataEntry { text }
                ... on UrlMetadataEntry { url }
                ... on PathMetadataEntry { path }
                ... on JsonMetadataEntry { jsonString }
                ... on MarkdownMetadataEntry { mdStr }
                ... on FloatMetadataEntry { floatValue }
                ... on IntMetadataEntry { intValue }
                ... on BoolMetadataEntry { boolValue }
                ... on PythonArtifactMetadataEntry { module name }
              }"""

# Maps each MetadataEntry __typename to the field holding its value.
_METADATA_VALUE_FIELDS = {
    "TextMetadataEntry": "text",
    "UrlMetadataEntry": "url",
    "PathMetadataEntry": "path",
    "JsonMetadataEntry": "jsonString",
    "MarkdownMetadataEntry": "mdStr",
    "FloatMetadataEntry": "floatValue",
    "IntMetadataEntry": "intValue",
    "BoolMetadataEntry": "boolValue",
}


def _flatten_metadata(entries: list[dict] | None) -> list[dict]:
    """Flatten raw metadataEntries into {label, description, value} dicts.

    Value is pulled from the type-specific field; PythonArtifact combines
    module + name; unknown types fall back to None.
    """
    flat = []
    for e in entries or []:
        typename = e.get("__typename")
        if typename == "PythonArtifactMetadataEntry":
            value = f"{e.get('module')}.{e.get('name')}"
        else:
            value = e.get(_METADATA_VALUE_FIELDS.get(typename, ""))
        flat.append(
            {"label": e.get("label"), "description": e.get("description"), "value": value}
        )
    return flat


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
    retries, materializations, and run-level events. EngineEvent events also carry
    metadataEntries — a list of {label, description, value} dicts (e.g. run worker
    image, k8s pod name, step keys) surfaced by the engine.

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
__METADATA_ENTRIES__
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
    query = query.replace("__METADATA_ENTRIES__", _METADATA_ENTRIES_FRAGMENT)
    data = gql(query, {"runId": run_id, "afterCursor": cursor, "limit": limit}, env=env)
    result = data.get("logsForRun", {})

    for event in result.get("events", []):
        if "metadataEntries" in event:
            event["metadataEntries"] = _flatten_metadata(event["metadataEntries"])

    if level_filter and "events" in result:
        upper = level_filter.upper()
        error_types = ("ExecutionStepFailureEvent", "RunFailureEvent")
        result["events"] = [
            e
            for e in result["events"]
            if e.get("level") == upper or (upper == "ERROR" and e.get("__typename") in error_types)
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
        log_data = gql(log_query, {"runId": run_id, "afterCursor": cursor}, env=env).get(
            "logsForRun", {}
        )
        events = log_data.get("events", [])
        for e in events:
            if e.get("__typename") in (
                "ExecutionStepFailureEvent",
                "RunFailureEvent",
                "ExecutionStepUpForRetryEvent",
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
        all_step_durations.append(
            {
                "step_key": s["stepKey"],
                "status": s["status"],
                "duration_seconds": dur,
            }
        )

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
            failed_steps.append(
                {
                    "step_key": s["stepKey"],
                    "duration_seconds": dur,
                    "error": step_errors.get(s["stepKey"], {}),
                }
            )

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
    data = gql(query, {"assetKey": {"path": asset_key.split("/")}, "limit": limit}, env=env)
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
    keys = [{"path": k.split("/")} for k in asset_keys]
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
def resolve_asset_selection(asset_selection: str, env: str | None = None) -> dict:
    """Resolve Dagster asset-selection syntax into concrete assets without launching a run.

    Supported syntax:
    - key predicates (``key:orders`` or bare ``orders``) with ``*`` wildcards
    - ``group:``, ``tag:``, ``kind:``, and ``owner:`` predicates
    - case-insensitive ``and``, ``or``, and ``not`` with parentheses
    - ``roots(...)`` and ``sinks(...)``
    - upstream/downstream traversal such as ``+orders``, ``2+orders``,
      ``orders+``, ``orders+2``, or ``1+orders+2``

    Returns ``asset_keys`` as slash-delimited strings ready to pass to
    materialize_assets or backfill_assets, plus compact GraphQL-shaped asset
    summaries. This tool is read-only and does not filter external, observable,
    non-executable, or partitioned matches.
    """
    try:
        expression = parse_asset_selection(asset_selection)
    except (AssetSelectionSyntaxError, TypeError) as exc:
        return {
            "selection": asset_selection,
            "asset_keys": [],
            "assets": [],
            "message": str(exc),
        }

    compatibility_error = _dagster_19_compatibility_error(
        "resolve_asset_selection",
        _RESOLVE_ASSET_SELECTION_SCHEMA,
        env=env,
    )
    if compatibility_error:
        return compatibility_error

    query = """
    query AssetSelectionGraph {
      assetNodes {
        assetKey { path }
        groupName
        tags { key value }
        kinds
        owners {
          __typename
          ... on TeamAssetOwner { team }
          ... on UserAssetOwner { email }
        }
        dependencyKeys { path }
        jobNames
        repository {
          name
          location { name }
        }
        isMaterializable
        isExecutable
        isObservable
        isPartitioned
      }
    }
    """
    nodes = gql(query, env=env).get("assetNodes", [])
    try:
        resolved = evaluate_asset_selection(nodes, expression)
    except Exception as exc:
        # Evaluation-time failures (e.g. unexpected node shape) return a
        # structured message rather than an unhandled tool error.
        return {
            "selection": asset_selection,
            "asset_keys": [],
            "assets": [],
            "message": f"Failed to evaluate selection: {exc}",
        }

    fields = (
        "assetKey",
        "groupName",
        "repository",
        "jobNames",
        "isMaterializable",
        "isExecutable",
        "isObservable",
        "isPartitioned",
    )
    assets = [{field: node.get(field) for field in fields} for node in resolved]
    return {
        "selection": asset_selection,
        "asset_keys": ["/".join(node["assetKey"]["path"]) for node in resolved],
        "assets": assets,
    }


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
        asset_keys_input = [{"path": asset_key_or_group.split("/")}]

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
        results.append(
            {
                "asset_key": n["assetKey"]["path"],
                "group": n.get("groupName"),
                "last_materialization": last_mat,
                "latest_run_status": latest_run_status,
                "freshness_policy": freshness_policy,
                "stale": len(stale_causes) > 0,
                "stale_causes": [c.get("reason", "") for c in stale_causes],
            }
        )

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
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "job": job["name"],
                    "description": job.get("description", ""),
                }
            )
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
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "schedule": sched["name"],
                    "cron": sched.get("cronSchedule"),
                    "status": sched.get("scheduleState", {}).get("status"),
                    "next_tick": next_ticks[0]["timestamp"] if next_ticks else None,
                    "job": sched.get("pipelineName"),
                }
            )
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
            result.append(
                {
                    "repository": repo["name"],
                    "location": repo["location"]["name"],
                    "sensor": sensor["name"],
                    "status": sensor.get("sensorState", {}).get("status"),
                    "targets": targets,
                }
            )
    return result


def _normalize_instigator_type(value: str) -> str:
    """Upper-case and validate an instigator type ('SCHEDULE' or 'SENSOR')."""
    normalized = value.upper()
    if normalized not in ("SCHEDULE", "SENSOR"):
        raise ValueError("instigator_type must be 'SCHEDULE' or 'SENSOR'.")
    return normalized


def _locate_instigators(
    instigator_name: str,
    instigator_type: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
    include_state: bool = False,
) -> list[dict]:
    """Locate every schedule/sensor matching a name across all repositories.

    Returns a list of {"repositoryName", "repositoryLocationName", "name",
    "state"} where "state" is the InstigationState ({id, selectorId}) when
    include_state is set, else {}. Instigator names are only unique within a
    repository, so several code locations may expose the same name; callers
    must handle >1 match. repository_name / location_name narrow the search
    when given. instigator_type must already be upper-cased and validated.

    Resolving instigation state hits the instance storage for every schedule
    and sensor in the workspace, so only the stop tools — which need the
    origin/selector ids — ask for it.
    """
    state_fields = " scheduleState { id selectorId }" if include_state else ""
    sensor_state_fields = " sensorState { id selectorId }" if include_state else ""
    locate = """
    query Locate {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            schedules { name%s }
            sensors { name%s }
          }
        }
        ... on PythonError { message }
      }
    }
    """ % (state_fields, sensor_state_fields)
    repos = gql(locate, env=env).get("repositoriesOrError", {}).get("nodes", [])
    field = "schedules" if instigator_type == "SCHEDULE" else "sensors"
    state_key = "scheduleState" if instigator_type == "SCHEDULE" else "sensorState"
    matches = []
    for repo in repos:
        repo_name = repo["name"]
        loc_name = repo["location"]["name"]
        if repository_name is not None and repo_name != repository_name:
            continue
        if location_name is not None and loc_name != location_name:
            continue
        for item in repo.get(field, []):
            if item.get("name") == instigator_name:
                matches.append(
                    {
                        "repositoryName": repo_name,
                        "repositoryLocationName": loc_name,
                        "name": instigator_name,
                        "state": item.get(state_key) or {},
                    }
                )
    return matches


def _resolve_instigator(
    instigator_name: str,
    instigator_type: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
    include_state: bool = False,
) -> tuple[dict | None, dict | None]:
    """Resolve a name to exactly one instigator.

    Returns (located, None) on a unique match, or (None, error_dict) when the
    instigator is missing or ambiguous across code locations. Never mutates.
    Pass include_state when the caller needs the instigation-state ids.
    """
    matches = _locate_instigators(
        instigator_name,
        instigator_type,
        repository_name=repository_name,
        location_name=location_name,
        env=env,
        include_state=include_state,
    )
    kind = instigator_type.capitalize()
    if not matches:
        return None, {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "message": f"{kind} '{instigator_name}' not found.",
        }
    if len(matches) > 1:
        candidates = [
            {
                "repository": m["repositoryName"],
                "location": m["repositoryLocationName"],
            }
            for m in matches
        ]
        listed = ", ".join(
            f"{c['location']}/{c['repository']}" for c in candidates
        )
        return None, {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "candidates": candidates,
            "message": (
                f"Ambiguous: {kind.lower()} '{instigator_name}' exists in "
                f"{listed} — pass repository_name/location_name to "
                "disambiguate. No change was made."
            ),
        }
    return matches[0], None


# ``repository_name``/``location_name`` follow ``env`` to preserve the existing
# positional call signature.
@mcp.tool()
def get_tick_history(
    instigator_name: str,
    instigator_type: str,
    limit: int = 20,
    env: str | None = None,
    repository_name: str | None = None,
    location_name: str | None = None,
) -> dict:
    """Get recent tick history for a schedule or sensor — essential for detecting silent failures.

    - instigator_name: exact name of the schedule or sensor (from list_schedules/list_sensors)
    - instigator_type: 'SCHEDULE' or 'SENSOR'
    - limit: max ticks to return (default 20)
    - repository_name / location_name: optional, to disambiguate when the same
      name exists in several code locations (see list_schedules/list_sensors)

    Returns per tick: tick_id, status (SUCCESS/FAILURE/SKIPPED), timestamp,
    error message (if failed), and run_ids (runs launched by this tick).

    When to use: when a schedule or sensor is RUNNING but data is not being
    produced. Common patterns to look for:
    - All ticks SKIPPED: sensor condition not met, or misconfigured
    - Ticks with FAILURE status: the schedule/sensor code is erroring
    - Ticks with SUCCESS but empty run_ids: sensor evaluated but decided not to launch
    - Missing ticks: daemon may be unhealthy (check get_instance_status)
    """
    instigator_type = _normalize_instigator_type(instigator_type)

    # Resolve repo + location for the named instigator (selector needs all three).
    located, error = _resolve_instigator(
        instigator_name,
        instigator_type,
        repository_name=repository_name,
        location_name=location_name,
        env=env,
    )
    if error is not None:
        return error
    selector = {
        key: located[key]
        for key in ("repositoryName", "repositoryLocationName", "name")
    }

    query = """
    query TickHistory($selector: InstigationSelector!, $limit: Int!) {
      instigationStateOrError(instigationSelector: $selector) {
        __typename
        ... on InstigationState {
          ticks(limit: $limit) {
            tickId
            status
            timestamp
            error { message }
            runIds
          }
        }
        ... on InstigationStateNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    state = gql(query, {"selector": selector, "limit": limit}, env=env).get(
        "instigationStateOrError", {}
    )
    if state.get("__typename") != "InstigationState":
        return {
            "name": instigator_name,
            "instigator_type": instigator_type,
            "message": state.get("message", "Unknown error"),
        }
    return {
        "name": instigator_name,
        "instigator_type": instigator_type,
        "ticks": [
            {
                "tick_id": t["tickId"],
                "status": t["status"],
                "timestamp": t["timestamp"],
                "error": t.get("error", {}).get("message") if t.get("error") else None,
                "run_ids": t.get("runIds", []),
            }
            for t in state.get("ticks", [])
        ],
    }


# ── Schedules & sensors (write) ───────────────────────────────────────────────


def _missing_state_ids_message(kind: str, name: str) -> str:
    return (
        f"Could not resolve instigation state ids for {kind.lower()} "
        f"'{name}'; this Dagster version may not expose "
        "InstigationState.id/selectorId."
    )


def start_schedule(
    schedule_name: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
) -> dict:
    """Start (enable) a Dagster schedule so it launches runs on its cron interval.

    - schedule_name: exact schedule name (from list_schedules)
    - repository_name / location_name: optional, to disambiguate when the same
      schedule name exists in several code locations
    - env: optional environment key; defaults to the configured instance

    Returns {name, instigator_type, repository, location, status} on success
    (status is the new InstigationStatus, normally RUNNING). On failure returns
    {name, instigator_type, message} — e.g. schedule not found, ambiguous
    across code locations (nothing is changed), or the API token lacks the
    START_SCHEDULE permission.

    When to use: to re-enable a schedule that was previously stopped. This is
    a persistent instance-level change that survives daemon restarts and code
    reloads. Ticks missed while the schedule was stopped are NOT backfilled —
    use backfill_assets or launch_job_with_partitions to catch up.
    """
    located, error = _resolve_instigator(
        schedule_name,
        "SCHEDULE",
        repository_name=repository_name,
        location_name=location_name,
        env=env,
    )
    if error is not None:
        return error

    selector = {
        "repositoryName": located["repositoryName"],
        "repositoryLocationName": located["repositoryLocationName"],
        "scheduleName": schedule_name,
    }
    query = """
    mutation StartSchedule($selector: ScheduleSelector!) {
      startSchedule(scheduleSelector: $selector) {
        __typename
        ... on ScheduleStateResult { scheduleState { id name status selectorId } }
        ... on Error { message }
      }
    }
    """
    result = gql(query, {"selector": selector}, env=env).get("startSchedule", {})
    typename = result.get("__typename")
    if typename == "ScheduleStateResult":
        state = result.get("scheduleState") or {}
        return {
            "name": schedule_name,
            "instigator_type": "SCHEDULE",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "status": state.get("status"),
            "message": f"Schedule '{schedule_name}' started.",
        }
    return {
        "name": schedule_name,
        "instigator_type": "SCHEDULE",
        "repository": located["repositoryName"],
        "location": located["repositoryLocationName"],
        "message": result.get("message", f"Unknown error ({typename})."),
    }


def stop_schedule(
    schedule_name: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
) -> dict:
    """Stop (disable) a Dagster schedule so it stops launching runs on its cron.

    - schedule_name: exact schedule name (from list_schedules)
    - repository_name / location_name: optional, to disambiguate when the same
      schedule name exists in several code locations
    - env: optional environment key; defaults to the configured instance

    Returns {name, instigator_type, repository, location, status} on success
    (status is the new InstigationStatus, normally STOPPED). On failure returns
    {name, instigator_type, message} — e.g. schedule not found, ambiguous
    across code locations (nothing is stopped), or the API token lacks the
    STOP_RUNNING_SCHEDULE permission.

    When to use: to halt a schedule that is launching bad or unwanted runs.
    This is a persistent instance-level change: the schedule stays stopped
    across daemon restarts and code reloads until someone calls
    start_schedule, and it overrides any default_status declared in code.
    It does NOT terminate runs the schedule already launched — use
    terminate_run for those. Cron ticks missed while stopped are never
    backfilled when the schedule is restarted.
    """
    located, error = _resolve_instigator(
        schedule_name,
        "SCHEDULE",
        repository_name=repository_name,
        location_name=location_name,
        env=env,
        include_state=True,
    )
    if error is not None:
        return error
    state = located["state"]
    if not state.get("id") or not state.get("selectorId"):
        return {
            "name": schedule_name,
            "instigator_type": "SCHEDULE",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "message": _missing_state_ids_message("schedule", schedule_name),
        }

    query = """
    mutation StopSchedule($originId: String!, $selectorId: String!) {
      stopRunningSchedule(scheduleOriginId: $originId, scheduleSelectorId: $selectorId) {
        __typename
        ... on ScheduleStateResult { scheduleState { id name status selectorId } }
        ... on Error { message }
      }
    }
    """
    variables = {"originId": state["id"], "selectorId": state["selectorId"]}
    result = gql(query, variables, env=env).get("stopRunningSchedule", {})
    typename = result.get("__typename")
    if typename == "ScheduleStateResult":
        new_state = result.get("scheduleState") or {}
        return {
            "name": schedule_name,
            "instigator_type": "SCHEDULE",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "status": new_state.get("status"),
            "message": f"Schedule '{schedule_name}' stopped.",
        }
    return {
        "name": schedule_name,
        "instigator_type": "SCHEDULE",
        "repository": located["repositoryName"],
        "location": located["repositoryLocationName"],
        "message": result.get("message", f"Unknown error ({typename})."),
    }


def start_sensor(
    sensor_name: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
) -> dict:
    """Start (enable) a Dagster sensor so it resumes evaluating and launching runs.

    - sensor_name: exact sensor name (from list_sensors)
    - repository_name / location_name: optional, to disambiguate when the same
      sensor name exists in several code locations
    - env: optional environment key; defaults to the configured instance

    Returns {name, instigator_type, repository, location, status} on success
    (status is the new InstigationStatus, normally RUNNING). On failure returns
    {name, instigator_type, message} — e.g. sensor not found, ambiguous across
    code locations (nothing is changed), or the API token lacks the
    EDIT_SENSOR permission.

    When to use: to re-enable a sensor that was previously stopped once the
    underlying issue is fixed. This is a persistent instance-level change that
    survives daemon restarts and code reloads. Use get_tick_history afterwards
    to confirm the sensor is ticking again.
    """
    located, error = _resolve_instigator(
        sensor_name,
        "SENSOR",
        repository_name=repository_name,
        location_name=location_name,
        env=env,
    )
    if error is not None:
        return error

    selector = {
        "repositoryName": located["repositoryName"],
        "repositoryLocationName": located["repositoryLocationName"],
        "sensorName": sensor_name,
    }
    query = """
    mutation StartSensor($selector: SensorSelector!) {
      startSensor(sensorSelector: $selector) {
        __typename
        ... on Sensor { name sensorState { id status selectorId } }
        ... on Error { message }
      }
    }
    """
    result = gql(query, {"selector": selector}, env=env).get("startSensor", {})
    typename = result.get("__typename")
    if typename == "Sensor":
        state = result.get("sensorState") or {}
        return {
            "name": sensor_name,
            "instigator_type": "SENSOR",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "status": state.get("status"),
            "message": f"Sensor '{sensor_name}' started.",
        }
    return {
        "name": sensor_name,
        "instigator_type": "SENSOR",
        "repository": located["repositoryName"],
        "location": located["repositoryLocationName"],
        "message": result.get("message", f"Unknown error ({typename})."),
    }


def stop_sensor(
    sensor_name: str,
    repository_name: str | None = None,
    location_name: str | None = None,
    env: str | None = None,
) -> dict:
    """Stop a Dagster sensor so it stops evaluating and launching runs.

    - sensor_name: sensor name (from list_sensors or get_tick_history)
    - repository_name / location_name: optional, to disambiguate when the same
      sensor name exists in several code locations
    - env: optional environment key; defaults to the configured instance

    Returns {name, instigator_type, repository, location, status} on success
    (status is the new InstigationStatus, normally STOPPED). On failure returns
    {name, instigator_type, message} — e.g. sensor not found, ambiguous across
    code locations (nothing is stopped), or the API token lacks the
    EDIT_SENSOR permission.

    When to use: to halt a runaway or erroring sensor found via
    get_tick_history or get_instance_status. This is a persistent
    instance-level change: the sensor stays stopped across daemon restarts and
    code reloads until someone calls start_sensor. It does NOT cancel runs the
    sensor already launched — use terminate_run for those. If the sensor
    declares a default_status in code, a stop recorded here overrides it.
    """
    located, error = _resolve_instigator(
        sensor_name,
        "SENSOR",
        repository_name=repository_name,
        location_name=location_name,
        env=env,
        include_state=True,
    )
    if error is not None:
        return error
    state = located["state"]
    if not state.get("id") or not state.get("selectorId"):
        return {
            "name": sensor_name,
            "instigator_type": "SENSOR",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "message": _missing_state_ids_message("sensor", sensor_name),
        }

    query = """
    mutation StopSensor($originId: String!, $selectorId: String!) {
      stopSensor(jobOriginId: $originId, jobSelectorId: $selectorId) {
        __typename
        ... on StopSensorMutationResult { instigationState { id name status selectorId } }
        ... on Error { message }
      }
    }
    """
    variables = {"originId": state["id"], "selectorId": state["selectorId"]}
    result = gql(query, variables, env=env).get("stopSensor", {})
    typename = result.get("__typename")
    if typename == "StopSensorMutationResult":
        new_state = result.get("instigationState") or {}
        return {
            "name": sensor_name,
            "instigator_type": "SENSOR",
            "repository": located["repositoryName"],
            "location": located["repositoryLocationName"],
            "status": new_state.get("status"),
            "message": f"Sensor '{sensor_name}' stopped.",
        }
    return {
        "name": sensor_name,
        "instigator_type": "SENSOR",
        "repository": located["repositoryName"],
        "location": located["repositoryLocationName"],
        "message": result.get("message", f"Unknown error ({typename})."),
    }


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
    daemon_statuses = data.get("instance", {}).get("daemonHealth", {}).get("allDaemonStatuses", [])
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
    location_entries = data.get("workspaceOrError", {}).get("locationEntries", [])
    code_location_errors = []
    for loc in location_entries:
        err = loc.get("locationOrLoadError", {})
        if "message" in err:
            code_location_errors.append({"name": loc["name"], "error": err["message"]})

    all_required_healthy = all(d["healthy"] for d in daemons if d["required"])
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
            backfillId: id
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


def _asset_key_input(asset_key: str) -> dict:
    return {"path": asset_key.split("/")}


def _asset_node_key(node: dict) -> str:
    return "/".join(node.get("assetKey", {}).get("path", []))


def _get_materialization_asset_nodes(
    asset_keys: list[str],
    env: str | None = None,
) -> list[dict]:
    query = """
    query MaterializationAssetNodes($assetKeys: [AssetKeyInput!]!) {
      assetNodes(assetKeys: $assetKeys) {
        assetKey { path }
        groupName
        jobNames
        isMaterializable
        isExecutable
        isObservable
        isPartitioned
        repository {
          name
          location { name }
        }
        assetChecksOrError {
          __typename
          ... on AssetChecks {
            checks {
              name
              jobNames
            }
          }
        }
      }
    }
    """
    return gql(
        query,
        {"assetKeys": [_asset_key_input(key) for key in asset_keys]},
        env=env,
    ).get("assetNodes", [])


def _get_materialization_requirements(
    asset_keys: list[str],
    env: str | None = None,
) -> dict:
    query = """
    query MaterializationRequirements($assetKeys: [AssetKeyInput!]!) {
      assetNodeAdditionalRequiredKeys(assetKeys: $assetKeys) {
        path
      }
      assetNodeDefinitionCollisions(assetKeys: $assetKeys) {
        assetKey { path }
        repositories {
          name
          location { name }
        }
      }
    }
    """
    return gql(
        query,
        {"assetKeys": [_asset_key_input(key) for key in asset_keys]},
        env=env,
    )


def _collision_message(collisions: list[dict]) -> str:
    details = []
    for collision in collisions:
        key = "/".join(collision.get("assetKey", {}).get("path", []))
        repositories = ", ".join(
            f"{repository.get('location', {}).get('name', '?')}/{repository.get('name', '?')}"
            for repository in collision.get("repositories", [])
        )
        details.append(f"{key} ({repositories})")
    return "Selected assets have definition collisions: " + "; ".join(details)


def materialize_assets(
    asset_keys: list[str],
    run_config: dict | None = None,
    tags: dict[str, str] | None = None,
    env: str | None = None,
) -> dict:
    """Materialize concrete, unpartitioned asset keys with optional run configuration.

    Use resolve_asset_selection first when starting from a Dagster selection
    expression. This tool deliberately accepts only concrete slash-delimited
    asset keys, then re-fetches their current definitions before launching.

    The selected assets must be materializable, executable, unpartitioned,
    defined in one repository, and share a common asset job. Dagster-required
    neighbors from non-subsettable multi-assets are included automatically and
    reported in ``required_asset_keys_added``.

    For partitioned assets, pass the resolved keys to backfill_assets instead.
    """
    if not asset_keys:
        return {"message": "asset_keys must contain at least one asset key."}
    if any(not isinstance(key, str) or not key for key in asset_keys):
        return {"message": "Every asset key must be a non-empty string."}
    invalid_key_paths = sorted(
        {
            key
            for key in asset_keys
            if key.startswith("/")
            or key.endswith("/")
            or any(not segment for segment in key.split("/"))
            or "*" in key
        }
    )
    if invalid_key_paths:
        return {
            "message": (
                "materialize_assets accepts concrete slash-delimited asset keys, "
                "not selection expressions or empty path segments. Invalid keys: "
                + ", ".join(invalid_key_paths)
                + ". Use resolve_asset_selection first."
            )
        }

    compatibility_error = _dagster_19_compatibility_error(
        "materialize_assets",
        _MATERIALIZE_ASSETS_SCHEMA,
        env=env,
    )
    if compatibility_error:
        return compatibility_error

    requested_keys = sorted(set(asset_keys))
    launched_key_set = set(requested_keys)
    required_key_set: set[str] = set()
    # Expand to a fixed point because a newly added non-subsettable multi-asset
    # neighbor can introduce additional required neighbors of its own.
    while True:
        launched_keys = sorted(launched_key_set)
        nodes = _get_materialization_asset_nodes(launched_keys, env=env)
        nodes_by_key = {_asset_node_key(node): node for node in nodes}
        missing_keys = sorted(set(launched_keys) - set(nodes_by_key))
        if missing_keys:
            return {
                "asset_keys": launched_keys,
                "required_asset_keys_added": sorted(required_key_set),
                "message": f"Asset definitions were not found for: {', '.join(missing_keys)}.",
            }

        # Dagster's requirement and collision resolvers assume every key exists,
        # so missing definitions must be rejected before calling them.
        requirements = _get_materialization_requirements(launched_keys, env=env)
        collisions = requirements.get("assetNodeDefinitionCollisions", [])
        if collisions:
            return {
                "asset_keys": launched_keys,
                "required_asset_keys_added": sorted(required_key_set),
                "message": _collision_message(collisions),
            }
        additional_required_keys = {
            "/".join(asset_key.get("path", []))
            for asset_key in requirements.get("assetNodeAdditionalRequiredKeys", [])
        } - launched_key_set
        if not additional_required_keys:
            break
        required_key_set |= additional_required_keys
        launched_key_set |= additional_required_keys

    required_keys = sorted(required_key_set)

    invalid_assets: list[str] = []
    for key in launched_keys:
        node = nodes_by_key[key]
        if node.get("isPartitioned"):
            reason = "partitioned; use backfill_assets"
        elif node.get("isObservable"):
            reason = "observable, not materializable"
        elif not node.get("isMaterializable"):
            reason = "external or otherwise non-materializable"
        elif not node.get("isExecutable"):
            reason = "not executable"
        else:
            continue
        invalid_assets.append(f"{key} ({reason})")
    if invalid_assets:
        return {
            "asset_keys": launched_keys,
            "required_asset_keys_added": required_keys,
            "message": "Cannot materialize selected assets: " + "; ".join(invalid_assets),
        }

    repositories = {
        (
            node.get("repository", {}).get("location", {}).get("name") or "(unknown)",
            node.get("repository", {}).get("name") or "(unknown)",
        )
        for node in nodes_by_key.values()
    }
    if len(repositories) != 1:
        details = ", ".join(
            f"{location}/{repository}" for location, repository in sorted(repositories)
        )
        return {
            "asset_keys": launched_keys,
            "required_asset_keys_added": required_keys,
            "message": (
                "Assets must be defined in one repository to launch a single run. "
                f"Found: {details}."
            ),
        }
    repository_location, repository_name = next(iter(repositories))

    # GraphQL asset launches still target a named job, so every selected asset
    # must belong to at least one job in common.
    common_jobs: set[str] | None = None
    jobs_by_asset: list[str] = []
    for key in launched_keys:
        jobs = set(nodes_by_key[key].get("jobNames") or [])
        common_jobs = jobs if common_jobs is None else common_jobs & jobs
        jobs_by_asset.append(f"{key}: {', '.join(sorted(jobs)) or '(none)'}")
    if not common_jobs:
        return {
            "asset_keys": launched_keys,
            "required_asset_keys_added": required_keys,
            "message": (
                "Selected assets do not share a common job. "
                + "; ".join(jobs_by_asset)
            ),
        }

    # Prefer Dagster's implicit asset job for ad hoc materialization, then use
    # deterministic name ordering when several compatible jobs remain.
    job_name = min(
        common_jobs,
        key=lambda name: (
            0 if name == "__ASSET_JOB" else 1 if name.startswith("__ASSET_JOB") else 2,
            name,
        ),
    )

    # Explicitly select only checks that can execute in the chosen asset job.
    asset_check_selection: list[dict] = []
    seen_checks: set[tuple[str, str]] = set()
    for key in launched_keys:
        checks_or_error = nodes_by_key[key].get("assetChecksOrError") or {}
        if checks_or_error.get("__typename") != "AssetChecks":
            continue
        for check in checks_or_error.get("checks", []):
            check_jobs = check.get("jobNames") or []
            check_name = check.get("name")
            if not check_name or (check_jobs and job_name not in check_jobs):
                continue
            check_key = (key, check_name)
            if check_key in seen_checks:
                continue
            seen_checks.add(check_key)
            asset_check_selection.append(
                {
                    "assetKey": _asset_key_input(key),
                    "name": check_name,
                }
            )

    execution_metadata = None
    if tags:
        execution_metadata = {
            "tags": [{"key": key, "value": value} for key, value in tags.items()]
        }

    mutation = """
    mutation MaterializeAssets(
      $locationName: String!,
      $repoName: String!,
      $jobName: String!,
      $assetSelection: [AssetKeyInput!]!,
      $assetCheckSelection: [AssetCheckHandleInput!],
      $executionMetadata: ExecutionMetadata,
      $runConfigData: RunConfigData
    ) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: $locationName,
          repositoryName: $repoName,
          jobName: $jobName,
          assetSelection: $assetSelection,
          assetCheckSelection: $assetCheckSelection
        },
        runConfigData: $runConfigData,
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
    launch_result = gql(
        mutation,
        {
            "locationName": repository_location,
            "repoName": repository_name,
            "jobName": job_name,
            "assetSelection": [_asset_key_input(key) for key in launched_keys],
            "assetCheckSelection": asset_check_selection,
            "runConfigData": run_config or {},
            "executionMetadata": execution_metadata,
        },
        env=env,
    ).get("launchRun", {})

    result = {
        "job_name": job_name,
        "repository_location": repository_location,
        "repository_name": repository_name,
        "requested_asset_keys": requested_keys,
        "asset_keys": launched_keys,
        "launched_asset_keys": launched_keys,
        "required_asset_keys_added": required_keys,
    }
    result.update(launch_result)
    return result


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
    run_config: dict | None = None,
    env: str | None = None,
) -> dict:
    """Launch a job or materialize specific assets. Use list_jobs first to find valid names.

    For new asset-centric workflows, prefer resolve_asset_selection followed by
    materialize_assets. ``asset_keys`` remains supported for compatibility and
    is sent to Dagster as an asset selection, not an op selection.

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
    - run_config: dict of run configuration to pass to the job. This is the
      same YAML/dict you would enter in the Dagster UI Launchpad.
      Example: {'ops': {'my_op': {'config': {'start_date': '2026-03-01'}}}}

    Returns the launched run's runId and status on success, or an error message.

    When to use: to re-run a failed job, trigger an ad-hoc materialization,
    or launch a job with custom config or tags. After launching, use
    get_run_status or get_runs to monitor progress.
    For partitioned ASSET backfills — especially an asset inside a multi-asset
    /dbt op, or one with a single_run backfill policy — use backfill_assets.
    """
    execution_metadata: dict = {}
    if tags:
        execution_metadata["tags"] = [{"key": k, "value": v} for k, v in tags.items()]

    asset_selection = (
        [_asset_key_input(asset_key) for asset_key in asset_keys]
        if asset_keys
        else None
    )

    query = """
    mutation LaunchJob(
      $locationName: String!,
      $repoName: String!,
      $jobName: String!,
      $assetSelection: [AssetKeyInput!],
      $executionMetadata: ExecutionMetadata,
      $runConfigData: RunConfigData
    ) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: $locationName,
          repositoryName: $repoName,
          jobName: $jobName,
          assetSelection: $assetSelection
        },
        runConfigData: $runConfigData,
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
        "assetSelection": asset_selection,
        "runConfigData": run_config or {},
        "executionMetadata": execution_metadata or None,
    }
    data = gql(query, variables, env=env)
    return data.get("launchRun", {})


def launch_job_with_partitions(
    job_name: str,
    repository_location: str,
    partition_keys: list[str],
    repository_name: str = "__repository__",
    partition_set_name: str | None = None,
    tags: dict[str, str] | None = None,
    from_failure: bool = False,
    env: str | None = None,
) -> dict:
    """Launch a partitioned job for one or more partition keys.

    Use list_jobs to find job names. Use get_asset_details to check if an asset
    is partitioned (isPartitioned field) and what partition definition it uses.

    Required parameters:
    - job_name: name of the partitioned job (from list_jobs)
    - repository_location: code location name (from list_jobs)
    - partition_keys: one or more partition key strings to run.
      Examples: ['2024-01-01'], ['2024-01-01', '2024-01-02', '2024-01-03']

    Optional parameters:
    - repository_name: defaults to '__repository__', override if you have
      multiple repositories in a single code location
    - partition_set_name: partition set name; defaults to '{job_name}_partition_set'.
      Override this if the job uses a non-standard partition set name.
    - tags: additional key-value tags to attach to the launched runs.
      Example: {'triggered_by': 'dataops_agent'}
    - from_failure: if True, only re-run the failed steps within the given
      partitions (useful for retrying partially-failed partitioned runs)

    Returns backfillId on success — even for a single partition, Dagster creates
    a backfill record. Use list_backfills to monitor progress.

    When to use: to run a job for a specific date/partition, backfill historical
    partitions, or retry failed partitions. For non-partitioned jobs, use launch_job.
    NOTE: this launches one run PER partition via the job's partition set. For
    asset-selection backfills that respect BackfillPolicy.single_run (one ranged
    run), use backfill_assets.
    """
    resolved_partition_set = partition_set_name or f"{job_name}_partition_set"
    tag_list = [{"key": k, "value": v} for k, v in tags.items()] if tags else []

    query = """
    mutation LaunchPartitionBackfill($backfillParams: LaunchBackfillParams!) {
      launchPartitionBackfill(backfillParams: $backfillParams) {
        ... on LaunchBackfillSuccess { backfillId }
        ... on PartitionSetNotFoundError { message }
        ... on PipelineNotFoundError { message }
        ... on PythonError { message }
        ... on UnauthorizedError { message }
        ... on RunConfigValidationInvalid { errors { message } }
      }
    }
    """
    variables = {
        "backfillParams": {
            "selector": {
                "repositorySelector": {
                    "repositoryLocationName": repository_location,
                    "repositoryName": repository_name,
                },
                "partitionSetName": resolved_partition_set,
            },
            "partitionNames": partition_keys,
            "tags": tag_list,
            "fromFailure": from_failure,
        }
    }
    data = gql(query, variables, env=env)
    return data.get("launchPartitionBackfill", {})


# ``run_config`` follows ``env`` to preserve the existing positional call signature.
def backfill_assets(
    asset_keys: list[str],
    partition_start: str | None = None,
    partition_end: str | None = None,
    partition_keys: list[str] | None = None,
    tags: dict[str, str] | None = None,
    env: str | None = None,
    run_config: dict | None = None,
) -> dict:
    """Launch a partition backfill for specific ASSETS (asset selection, not job).

    This is the MCP equivalent of the UI's "Materialize → partition range".
    Dagster resolves each asset's BackfillPolicy server-side: assets with
    BackfillPolicy.single_run() get ONE ranged run (vars min/max spanning the
    whole range); others get per-partition runs.

    Use this instead of:
    - launch_job(asset_keys=...): fails with DagsterInvalidSubsetError for an
      asset inside a multi-asset op (e.g. one dbt model inside a dbt_assets op),
      and cannot target partitions.
    - launch_job_with_partitions: partition-set (whole-job) selection that
      creates one run PER partition key — breaks single_run backfill policies.

    Required parameters:
    - asset_keys: asset key strings, e.g. ['clean_es_email_activity_detail'].
      Nested keys use '/' as the path separator (e.g. 'raw_chargebee_dlt/customer').
      Multiple assets must share a partition definition (same rule as the UI).

    Optional parameters:
    - partition_start / partition_end: inclusive bounds. Resolved against the
      asset's actual partition keys (fetched via GraphQL), so any partition
      definition works (daily/weekly/monthly/static). Omitted bounds default
      to the first / latest key. Use the SAME format the asset uses
      (e.g. '2026-05-25' for daily).
    - partition_keys: explicit key list; overrides partition_start/end.
    - tags: extra run tags, e.g. {'triggered_by': 'dataops_agent'}.
    - run_config: dict of run configuration applied to the backfill runs.
      This is the same structure used in the Dagster Launchpad.

    Returns {'backfillId': ...} on success or {'message': <error>} on failure.
    Monitor with list_backfills; runs carry the dagster/backfill tag.
    """
    if run_config is not None:
        input_fields = _get_type_fields(
            "LaunchBackfillParams",
            env=env,
            input_type=True,
        )
        if "runConfigData" not in input_fields:
            return {
                "message": (
                    "This Dagster instance does not expose "
                    "LaunchBackfillParams.runConfigData, so configured asset "
                    "backfills are not supported by its GraphQL API."
                )
            }

    if partition_keys is None:
        query = """
        query AssetPartitionKeys($assetKeys: [AssetKeyInput!]!) {
          assetNodes(assetKeys: $assetKeys) {
            partitionKeys
          }
        }
        """
        nodes = gql(
            query,
            {"assetKeys": [{"path": asset_keys[0].split("/")}]},
            env=env,
        ).get("assetNodes", [])
        all_keys: list[str] = nodes[0].get("partitionKeys", []) if nodes else []
        if not all_keys:
            return {
                "message": (
                    f"Asset '{asset_keys[0]}' is not partitioned (or not found) — "
                    "use materialize_assets for unpartitioned assets."
                )
            }

        def _bound_index(bound: str, default: int, side: str) -> int | dict:
            if bound is None:
                return default
            try:
                return all_keys.index(bound)
            except ValueError:
                # Best-effort "nearest keys" hint; assumes lexicographically
                # sorted partition keys (true for daily/monthly, not static).
                pos = bisect.bisect_left(all_keys, bound)
                nearest = all_keys[max(0, pos - 1) : pos + 2]
                return {
                    "message": (
                        f"partition_{side} '{bound}' is not a partition key of "
                        f"'{asset_keys[0]}'. Nearest keys: {nearest}."
                    )
                }

        start_idx = _bound_index(partition_start, 0, "start")
        if isinstance(start_idx, dict):
            return start_idx
        end_idx = _bound_index(partition_end, len(all_keys) - 1, "end")
        if isinstance(end_idx, dict):
            return end_idx
        if start_idx > end_idx:
            return {
                "message": (
                    f"partition_start '{all_keys[start_idx]}' is after "
                    f"partition_end '{all_keys[end_idx]}'."
                )
            }
        partition_keys = all_keys[start_idx : end_idx + 1]

    tag_list = [{"key": k, "value": v} for k, v in tags.items()] if tags else []

    query = """
    mutation LaunchAssetBackfill($backfillParams: LaunchBackfillParams!) {
      launchPartitionBackfill(backfillParams: $backfillParams) {
        ... on LaunchBackfillSuccess { backfillId }
        ... on PartitionSetNotFoundError { message }
        ... on PipelineNotFoundError { message }
        ... on PythonError { message }
        ... on UnauthorizedError { message }
        ... on InvalidSubsetError { message }
        ... on RunConfigValidationInvalid { errors { message } }
      }
    }
    """
    backfill_params = {
        "assetSelection": [_asset_key_input(key) for key in asset_keys],
        "partitionNames": partition_keys,
        "tags": tag_list,
    }
    if run_config is not None:
        backfill_params["runConfigData"] = run_config
    variables = {"backfillParams": backfill_params}
    data = gql(query, variables, env=env)
    return data.get("launchPartitionBackfill", {})


# ── Write tools (only registered when DAGSTER_READ_ONLY=false) ────────────────

if not READ_ONLY:
    mcp.tool()(reload_code_location)
    mcp.tool()(terminate_run)
    mcp.tool()(launch_job)
    mcp.tool()(materialize_assets)
    mcp.tool()(launch_job_with_partitions)
    mcp.tool()(backfill_assets)
    mcp.tool()(start_schedule)
    mcp.tool()(stop_schedule)
    mcp.tool()(start_sensor)
    mcp.tool()(stop_sensor)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
