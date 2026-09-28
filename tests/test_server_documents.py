"""Validate every GraphQL document the server actually sends against Dagster's schema.

The tool tests mock ``httpx.post``, so a document selecting a field Dagster does
not have passes all of them and fails on every real call. 0.11.0 and 0.12.0
shipped exactly that (#36): ``... on InvalidStepError { message }`` on a type
that has no ``message``, which made GraphQL reject the whole launch mutation.

Several documents are assembled at runtime (sentinel substitution, the aliased
batch query in ``list_jobs``, the ``include_state`` branches of instigator
lookup), so checking string literals in ``server.py`` is not enough. Instead,
``server.gql`` is replaced with a recorder that captures each document and
answers with just enough data to push multi-step flows onward, then every tool
is driven through its query-producing paths.

``test_every_operation_in_source_is_exercised`` runs without ``dagster-graphql``
and fails when an operation in ``server.py`` is never captured, so a new query
cannot slip past the schema check by simply not being driven here.
"""

import contextlib
import functools
import re
from importlib import metadata
from pathlib import Path

import pytest

from dagster_mcp import server
from tests.graphql_schema import AVAILABLE, schema_errors

_OPERATION = re.compile(r"\b(?:query|mutation)\s+([A-Z][A-Za-z0-9_]*)\s*[({]")

# Tools that refuse to run below Dagster 1.9 via _dagster_19_compatibility_error.
# Their documents are only valid against a 1.9+ schema, by design.
_REQUIRES_19 = {
    "AssetSelectionGraph",
    "MaterializationAssetNodes",
    "MaterializationRequirements",
    "MaterializeAssets",
}

_REPO = {"name": "repo", "location": {"name": "loc"}}
_ASSET_NODE = {
    "assetKey": {"path": ["a"]},
    "groupName": "g",
    "jobNames": ["__ASSET_JOB"],
    "isMaterializable": True,
    "isExecutable": True,
    "isObservable": False,
    "isPartitioned": False,
    "repository": _REPO,
    "assetChecksOrError": {"__typename": "AssetChecks", "checks": []},
    "dependencyKeys": [],
    "tags": [],
    "kinds": [],
    "owners": [],
}
_WORKSPACE = {
    "__typename": "Workspace",
    "locationEntries": [
        {
            "name": "loc",
            "loadStatus": "LOADED",
            "locationOrLoadError": {"__typename": "RepositoryLocation"},
        }
    ],
}
_INSTIGATOR_STATE = {"id": "origin", "selectorId": "selector"}

# Canned answers keyed by operation name. They only need to be plausible enough
# for the code to reach its next query; correctness is not what is under test.
_RESPONSES = {
    "Runs": {"runsOrError": {"__typename": "Runs", "results": []}},
    "RunStatus": {"runOrError": {"__typename": "Run", "runId": "r", "status": "FAILURE"}},
    "RunLogs": {"logsForRun": {"__typename": "EventConnection", "events": []}},
    "RunStats": {"runOrError": {"__typename": "Run", "runId": "r", "stepStats": []}},
    "FailureSummary": {
        "runOrError": {
            "__typename": "Run",
            "runId": "r",
            "status": "FAILURE",
            "jobName": "j",
            "stepStats": [],
        }
    },
    "FailureLogs": {"logsForRun": {"events": [], "hasMore": False}},
    "AssetRuns": {"assetOrError": {"assetMaterializations": []}},
    "AssetDetails": {"assetNodes": []},
    "AllAssets": {"assetNodes": [_ASSET_NODE]},
    "AssetSelectionGraph": {"assetNodes": [_ASSET_NODE]},
    # A materialization with a runId is what makes get_asset_health look up run
    # statuses, which is the only path that sends RunStatuses.
    "AssetHealth": {
        "assetNodes": [
            dict(_ASSET_NODE, assetMaterializations=[{"runId": "r", "timestamp": "1"}])
        ]
    },
    "RunStatuses": {"runsOrError": {"results": []}},
    "ListJobs": {"repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": []}},
    "ListJobRepositories": {
        "repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": [_REPO]},
        "workspaceOrError": _WORKSPACE,
    },
    "ListRepositoryJobs": {
        "repository0": {
            "__typename": "RepositoryConnection",
            "nodes": [dict(_REPO, jobs=[])],
        },
        "workspaceOrError": _WORKSPACE,
    },
    "ListSchedules": {
        "repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": []},
        "workspaceOrError": _WORKSPACE,
    },
    "ListSensors": {
        "repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": []},
        "workspaceOrError": _WORKSPACE,
    },
    "Locate": {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                dict(
                    _REPO,
                    schedules=[{"name": "s", "scheduleState": _INSTIGATOR_STATE}],
                    sensors=[{"name": "s", "sensorState": _INSTIGATOR_STATE}],
                )
            ],
        },
        "workspaceOrError": _WORKSPACE,
    },
    "TickHistory": {"instigationStateOrError": {"__typename": "InstigationState", "ticks": []}},
    "InstanceStatus": {
        "instance": {"daemonHealth": {"allDaemonStatuses": []}},
        "runsOrError": {"results": []},
        "workspaceOrError": _WORKSPACE,
    },
    "CodeLocations": {"workspaceOrError": _WORKSPACE},
    "Backfills": {"partitionBackfillsOrError": {"results": []}},
    "MaterializationAssetNodes": {"assetNodes": [_ASSET_NODE]},
    "MaterializationRequirements": {
        "assetNodeAdditionalRequiredKeys": [],
        "assetNodeDefinitionCollisions": [],
    },
    "AssetPartitionKeys": {"assetNodes": [{"partitionKeys": ["p1", "p2"]}]},
}


def _type_fields_response(variables):
    # Answer introspection permissively so feature-gated paths take the branch
    # that sends their document.
    names = {"jobName", "runConfigData"}
    for fields in (
        *server._RESOLVE_ASSET_SELECTION_SCHEMA.values(),
        *server._MATERIALIZE_ASSETS_SCHEMA.values(),
    ):
        names |= fields
    entries = [{"name": name} for name in sorted(names)]
    return {"__type": {"fields": entries, "inputFields": entries}}


def _operation_name(document: str) -> str:
    match = _OPERATION.search(document)
    assert match, f"GraphQL document without a named operation:\n{document}"
    return match.group(1)


def _drive_every_tool(captured: dict[str, set[str]]) -> None:
    def fake_gql(query, variables=None, env=None):
        name = _operation_name(query)
        captured.setdefault(name, set()).add(query)
        if name == "TypeFields":
            return _type_fields_response(variables)
        return _RESPONSES.get(name, {})

    calls = [
        lambda: server.get_runs(),
        lambda: server.get_runs(job_name="j", statuses=["FAILURE"]),
        lambda: server.get_run_status("r"),
        lambda: server.get_run_logs("r", level_filter="WARNING"),
        lambda: server.get_run_stats("r"),
        lambda: server.get_run_failure_summary("r"),
        lambda: server.search_assets(prefix="a"),
        lambda: server.search_assets(group="g"),
        lambda: server.resolve_asset_selection("a+"),
        lambda: server.get_asset_details(["a"]),
        lambda: server.get_recent_materializations("a"),
        lambda: server.get_asset_health("a"),
        lambda: server.get_asset_health("g"),
        lambda: server.list_jobs(),
        lambda: server.list_jobs(repository_name="repo"),
        lambda: server.list_jobs(repository_name="repo", location_name="loc"),
        lambda: server.list_schedules(),
        lambda: server.list_sensors(),
        lambda: server.get_tick_history("s", "SCHEDULE"),
        lambda: server.get_tick_history("s", "SENSOR"),
        lambda: server.get_instance_status(),
        lambda: server.list_code_locations(),
        lambda: server.list_backfills(),
        lambda: server.materialize_assets(["a"], run_config={}, tags={"t": "v"}),
        lambda: server.backfill_assets(["a"], partition_start="p1", partition_end="p2"),
        lambda: server.backfill_assets(["a"], partition_keys=["p1"], run_config={}),
        lambda: server.launch_job("j", "loc", asset_keys=["a"], tags={"t": "v"}),
        lambda: server.launch_job_with_partitions("j", "loc", ["p1"]),
        lambda: server.launch_job_with_partitions("j", "loc", ["p1"], from_failure=True),
        lambda: server.terminate_run("r"),
        lambda: server.start_schedule("s"),
        lambda: server.stop_schedule("s"),
        lambda: server.start_sensor("s"),
        lambda: server.stop_sensor("s"),
        lambda: server.reload_code_location("loc"),
    ]

    original = server.gql
    server.gql = fake_gql
    server._runs_filter_job_field.clear()
    server._type_fields.clear()
    try:
        for call in calls:
            # Only the documents sent matter here, not whether the tool succeeds.
            with contextlib.suppress(Exception):
                call()
    finally:
        server.gql = original
        server._runs_filter_job_field.clear()
        server._type_fields.clear()


@functools.cache
def _captured() -> dict[str, frozenset[str]]:
    captured: dict[str, set[str]] = {}
    _drive_every_tool(captured)
    return {name: frozenset(docs) for name, docs in captured.items()}


def test_every_operation_in_source_is_exercised():
    source = Path(server.__file__).read_text(encoding="utf-8")
    declared = set(_OPERATION.findall(source))
    missing = declared - set(_captured())

    assert declared, "found no operations in server.py; the pattern is stale"
    assert not missing, (
        "These operations in server.py are never sent by the driver in this "
        f"file, so their documents are not schema-checked: {sorted(missing)}. "
        "Add a call to _drive_every_tool that reaches them."
    )


@pytest.mark.skipif(
    not AVAILABLE,
    reason="dagster-graphql is not installed; CI runs this with uv run --with",
)
def test_every_sent_document_validates_against_the_schema():
    version = tuple(int(part) for part in metadata.version("dagster-graphql").split(".")[:2])
    failures = []
    for name, documents in sorted(_captured().items()):
        if version < (1, 9) and name in _REQUIRES_19:
            continue
        for document in documents:
            errors = schema_errors(document)
            if errors:
                failures.append(f"{name}: " + "; ".join(errors))

    assert not failures, (
        f"Documents rejected by dagster-graphql {metadata.version('dagster-graphql')}:\n  "
        + "\n  ".join(failures)
    )
