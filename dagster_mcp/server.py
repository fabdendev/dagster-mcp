"""Dagster MCP server — GraphQL wrapper for self-hosted and Dagster Cloud instances."""

import os
import httpx
from fastmcp import FastMCP

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
GRAPHQL_URL = f"{DAGSTER_URL.rstrip('/')}/graphql"
DAGSTER_API_TOKEN = os.environ.get("DAGSTER_API_TOKEN", "")
READ_ONLY = os.environ.get("DAGSTER_READ_ONLY", "true").lower() in ("true", "1", "yes")

_mode = "read-only" if READ_ONLY else "read-write"
mcp = FastMCP(
    "dagster",
    instructions=(
        f"Use these tools to monitor and operate a running Dagster instance ({_mode} mode). "
        "Start with list_jobs or get_runs to explore what is available, then "
        "drill into specific runs, assets, schedules, or sensors as needed."
    ),
)


def _build_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    if DAGSTER_API_TOKEN:
        headers["Dagster-Cloud-Api-Token"] = DAGSTER_API_TOKEN
    return headers


def gql(query: str, variables: dict | None = None) -> dict:
    try:
        response = httpx.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables or {}},
            headers=_build_headers(),
            timeout=30,
        )
    except httpx.ConnectError:
        raise RuntimeError(
            f"Cannot connect to Dagster at {DAGSTER_URL}. "
            "Check that DAGSTER_URL is correct and the instance is running."
        )
    except httpx.TimeoutException:
        raise RuntimeError(
            f"Request to Dagster at {DAGSTER_URL} timed out after 30s."
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
) -> list[dict]:
    """Get recent Dagster runs, optionally filtered by job name and/or statuses.

    statuses examples: ['SUCCESS'], ['FAILURE', 'CANCELED'], ['STARTED', 'QUEUED']
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
        filter_var["jobName"] = job_name
    data = gql(query, {"limit": limit, "filter": filter_var or None})
    runs = data.get("runsOrError", {})
    return runs.get("results", [])


@mcp.tool()
def get_run_status(run_id: str) -> dict:
    """Get the status, config, and failure reason of a Dagster run by run ID."""
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
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """
    data = gql(query, {"runId": run_id})
    return data.get("runOrError", {})


@mcp.tool()
def get_run_logs(run_id: str, cursor: str | None = None, limit: int = 100) -> dict:
    """Get logs/events for a Dagster run. Use cursor for pagination.

    Captures step failures, run-level failures, retries, and general messages.
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
            ... on StepMaterializationEvent {
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
    data = gql(query, {"runId": run_id, "afterCursor": cursor, "limit": limit})
    return data.get("logsForRun", {})


@mcp.tool()
def get_run_stats(run_id: str) -> dict:
    """Get step-level statistics for a Dagster run (timing, materialization counts, expectations)."""
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
    data = gql(query, {"runId": run_id})
    return data.get("runOrError", {})


# ── Assets ────────────────────────────────────────────────────────────────────


@mcp.tool()
def get_recent_materializations(asset_key: str, limit: int = 5) -> list[dict]:
    """Get the most recent materializations for a given asset key (e.g. 'my_daily_report')."""
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
    data = gql(query, {"assetKey": {"path": [asset_key]}, "limit": limit})
    asset = data.get("assetOrError", {})
    return asset.get("assetMaterializations", [])


@mcp.tool()
def get_asset_details(asset_keys: list[str]) -> list[dict]:
    """Get details for a list of asset keys: description, dependencies, group, latest materialization."""
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
    data = gql(query, {"assetKeys": keys})
    return data.get("assetNodes", [])


@mcp.tool()
def search_assets(prefix: str | None = None, group: str | None = None) -> list[dict]:
    """Search/list all asset nodes. Optionally filter by key prefix or group name (client-side)."""
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
    data = gql(query)
    nodes = data.get("assetNodes", [])
    if prefix:
        prefix_lower = prefix.lower()
        nodes = [n for n in nodes if any(prefix_lower in p.lower() for p in n["assetKey"]["path"])]
    if group:
        group_lower = group.lower()
        nodes = [n for n in nodes if (n.get("groupName") or "").lower() == group_lower]
    return nodes


# ── Jobs & Schedules & Sensors ────────────────────────────────────────────────


@mcp.tool()
def list_jobs() -> list[dict]:
    """List all jobs/pipelines available in the Dagster instance."""
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
    data = gql(query)
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
def list_schedules() -> list[dict]:
    """List all schedules with their status, cron interval, and next tick."""
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
    data = gql(query)
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
def list_sensors() -> list[dict]:
    """List all sensors with their status and target jobs."""
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
    data = gql(query)
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


# ── Code Locations ────────────────────────────────────────────────────────────


@mcp.tool()
def list_code_locations() -> list[dict]:
    """List all code locations (repository locations) and their load status."""
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
    data = gql(query)
    workspace = data.get("workspaceOrError", {})
    return workspace.get("locationEntries", [])


def reload_code_location(location_name: str) -> dict:
    """Reload a code location by name (e.g. after a deploy). Returns the new load status."""
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
    data = gql(query, {"location": location_name})
    return data.get("reloadRepositoryLocation", {})


# ── Backfills ─────────────────────────────────────────────────────────────────


@mcp.tool()
def list_backfills(limit: int = 10) -> list[dict]:
    """List recent backfills with their status and progress."""
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
    data = gql(query, {"limit": limit})
    return data.get("partitionBackfillsOrError", {}).get("results", [])


# ── Actions ───────────────────────────────────────────────────────────────────


def terminate_run(run_id: str) -> dict:
    """Terminate/stop a running Dagster run by run ID."""
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
    data = gql(query, {"runId": run_id})
    return data.get("terminateRun", {})


def launch_job(
    job_name: str,
    repository_location: str,
    repository_name: str = "__repository__",
    asset_keys: list[str] | None = None,
    tags: dict[str, str] | None = None,
) -> dict:
    """Launch a Dagster job or materialize specific assets.

    For asset materialization, pass asset_keys with the job that targets them
    (often '__ASSET_JOB' or a custom asset job name).
    Example asset_keys: ['my_extract_asset', 'my_load_asset']
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
    data = gql(query, variables)
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
