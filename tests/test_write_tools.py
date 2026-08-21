import json
import os
import subprocess
import sys
from unittest.mock import MagicMock

import httpx
import pytest

from dagster_mcp import server as server_module
from dagster_mcp.server import (
    backfill_assets,
    get_tick_history,
    launch_job,
    launch_job_with_partitions,
    materialize_assets,
    reload_code_location,
    start_schedule,
    start_sensor,
    stop_schedule,
    stop_sensor,
    terminate_run,
)


def _mock_response(data):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.text = json.dumps(data)
    return response


def _materializable_node(
    key,
    *,
    jobs=None,
    location="loc1",
    repository="repo1",
    materializable=True,
    executable=True,
    observable=False,
    partitioned=False,
    checks=None,
):
    return {
        "assetKey": {"path": key.split("/")},
        "groupName": "default",
        "jobNames": jobs if jobs is not None else ["__ASSET_JOB"],
        "isMaterializable": materializable,
        "isExecutable": executable,
        "isObservable": observable,
        "isPartitioned": partitioned,
        "repository": {"name": repository, "location": {"name": location}},
        "assetChecksOrError": {
            "__typename": "AssetChecks",
            "checks": checks or [],
        },
    }


def _asset_nodes(nodes):
    return {"data": {"assetNodes": nodes}}


def _requirements(*, required=None, collisions=None):
    return {
        "data": {
            "assetNodeAdditionalRequiredKeys": [
                {"path": key.split("/")} for key in required or []
            ],
            "assetNodeDefinitionCollisions": collisions or [],
        }
    }


class TestTerminateRun:
    def test_success(self, mock_gql):
        mock_gql({"data": {"terminateRun": {
            "run": {"runId": "r1", "status": "CANCELING"},
        }}})
        result = terminate_run("r1")
        assert result["run"]["status"] == "CANCELING"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"terminateRun": {"message": "Run not found"}}})
        result = terminate_run("r999")
        assert "message" in result


class TestReloadCodeLocation:
    def test_success(self, mock_gql):
        mock_gql({"data": {"reloadRepositoryLocation": {
            "name": "loc1", "loadStatus": "LOADED",
            "locationOrLoadError": {"name": "loc1"},
        }}})
        result = reload_code_location("loc1")
        assert result["loadStatus"] == "LOADED"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"reloadRepositoryLocation": {
            "message": "Location not found",
        }}})
        result = reload_code_location("missing")
        assert "message" in result


@pytest.mark.usefixtures("supported_asset_tool_schema")
class TestMaterializeAssets:
    def test_success_uses_asset_selection_config_tags_and_checks(self, monkeypatch):
        node = _materializable_node(
            "warehouse/benchmark",
            jobs=["custom_job", "__ASSET_JOB"],
            checks=[
                {"name": "quality", "jobNames": ["__ASSET_JOB"]},
                {"name": "other_job_check", "jobNames": ["custom_job"]},
            ],
        )
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([node])),
                _mock_response(_requirements()),
                _mock_response(
                    {
                        "data": {
                            "launchRun": {
                                "run": {"runId": "run1", "status": "STARTING"}
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)
        config = {"ops": {"benchmark": {"config": {"limit": 10}}}}

        result = materialize_assets(
            ["warehouse/benchmark"],
            run_config=config,
            tags={"triggered_by": "agent"},
        )

        assert result["run"]["runId"] == "run1"
        assert result["job_name"] == "__ASSET_JOB"
        assert result["requested_asset_keys"] == ["warehouse/benchmark"]
        assert result["asset_keys"] == ["warehouse/benchmark"]
        assert result["launched_asset_keys"] == ["warehouse/benchmark"]
        launch_payload = mock_post.call_args_list[2].kwargs["json"]
        assert launch_payload["variables"]["assetSelection"] == [
            {"path": ["warehouse", "benchmark"]}
        ]
        assert launch_payload["variables"]["assetCheckSelection"] == [
            {
                "assetKey": {"path": ["warehouse", "benchmark"]},
                "name": "quality",
            }
        ]
        assert launch_payload["variables"]["runConfigData"] == config
        assert launch_payload["variables"]["executionMetadata"]["tags"] == [
            {"key": "triggered_by", "value": "agent"}
        ]
        assert "solidSelection" not in launch_payload["query"]

    def test_adds_required_multi_asset_neighbors(self, monkeypatch):
        first = _materializable_node("a")
        second = _materializable_node("nested/b")
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([first])),
                _mock_response(_requirements(required=["nested/b"])),
                _mock_response(_asset_nodes([first, second])),
                _mock_response(_requirements()),
                _mock_response(
                    {
                        "data": {
                            "launchRun": {
                                "run": {"runId": "run2", "status": "STARTING"}
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a"])

        assert result["asset_keys"] == ["a", "nested/b"]
        assert result["launched_asset_keys"] == ["a", "nested/b"]
        assert result["required_asset_keys_added"] == ["nested/b"]
        expanded_payload = mock_post.call_args_list[2].kwargs["json"]
        assert expanded_payload["variables"]["assetKeys"] == [
            {"path": ["a"]},
            {"path": ["nested", "b"]},
        ]

    def test_adds_required_multi_asset_neighbors_until_closed(self, monkeypatch):
        first = _materializable_node("a")
        second = _materializable_node("b")
        third = _materializable_node("c")
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([first])),
                _mock_response(_requirements(required=["b"])),
                _mock_response(_asset_nodes([first, second])),
                _mock_response(_requirements(required=["c"])),
                _mock_response(_asset_nodes([first, second, third])),
                _mock_response(_requirements()),
                _mock_response(
                    {
                        "data": {
                            "launchRun": {
                                "run": {"runId": "run-closure", "status": "STARTING"}
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a"])

        assert result["asset_keys"] == ["a", "b", "c"]
        assert result["required_asset_keys_added"] == ["b", "c"]

    def test_missing_asset_fails_without_launch(self, monkeypatch):
        mock_post = MagicMock(return_value=_mock_response(_asset_nodes([])))
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["missing"])

        assert "not found" in result["message"]
        assert mock_post.call_count == 1

    def test_missing_required_neighbor_fails_before_requirement_resolvers(
        self,
        monkeypatch,
    ):
        first = _materializable_node("a")
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([first])),
                _mock_response(_requirements(required=["removed"])),
                _mock_response(_asset_nodes([first])),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a"])

        assert "not found" in result["message"]
        assert "removed" in result["message"]
        assert result["required_asset_keys_added"] == ["removed"]
        assert mock_post.call_count == 3
        assert "MaterializationAssetNodes" in mock_post.call_args.kwargs["json"]["query"]

    def test_collision_fails_without_launch(self, monkeypatch):
        node = _materializable_node("duplicate")
        collision = {
            "assetKey": {"path": ["duplicate"]},
            "repositories": [
                {"name": "repo1", "location": {"name": "loc1"}},
                {"name": "repo2", "location": {"name": "loc2"}},
            ],
        }
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([node])),
                _mock_response(_requirements(collisions=[collision])),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["duplicate"])

        assert "collisions" in result["message"]
        assert "loc1/repo1" in result["message"]
        assert mock_post.call_count == 2

    @pytest.mark.parametrize(
        ("node_kwargs", "expected"),
        [
            ({"partitioned": True}, "backfill_assets"),
            ({"observable": True}, "observable"),
            ({"executable": False}, "not executable"),
            ({"materializable": False}, "non-materializable"),
        ],
    )
    def test_invalid_asset_types_fail(self, monkeypatch, node_kwargs, expected):
        node = _materializable_node("invalid", **node_kwargs)
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([node])),
                _mock_response(_requirements()),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["invalid"])

        assert expected in result["message"]
        assert mock_post.call_count == 2

    def test_assets_in_multiple_repositories_fail(self, monkeypatch):
        nodes = [
            _materializable_node("a", location="loc1", repository="repo1"),
            _materializable_node("b", location="loc2", repository="repo2"),
        ]
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes(nodes)),
                _mock_response(_requirements()),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a", "b"])

        assert "one repository" in result["message"]
        assert "loc1/repo1" in result["message"]
        assert "loc2/repo2" in result["message"]
        assert mock_post.call_count == 2

    def test_assets_without_common_job_fail(self, monkeypatch):
        nodes = [
            _materializable_node("a", jobs=["job_a"]),
            _materializable_node("b", jobs=["job_b"]),
        ]
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes(nodes)),
                _mock_response(_requirements()),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a", "b"])

        assert "do not share a common job" in result["message"]
        assert "a: job_a" in result["message"]
        assert mock_post.call_count == 2

    def test_prefers_numbered_implicit_job_over_user_job(self, monkeypatch):
        node = _materializable_node("a", jobs=["user_job", "__ASSET_JOB_2"])
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([node])),
                _mock_response(_requirements()),
                _mock_response(
                    {
                        "data": {
                            "launchRun": {
                                "run": {"runId": "run3", "status": "STARTING"}
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a"])

        assert result["job_name"] == "__ASSET_JOB_2"

    def test_graphql_launch_error_keeps_preflight_context(self, monkeypatch):
        node = _materializable_node("a")
        mock_post = MagicMock(
            side_effect=[
                _mock_response(_asset_nodes([node])),
                _mock_response(_requirements()),
                _mock_response(
                    {
                        "data": {
                            "launchRun": {
                                "errors": [{"message": "Invalid config"}]
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["a"], run_config={"bad": True})

        assert result["errors"] == [{"message": "Invalid config"}]
        assert result["job_name"] == "__ASSET_JOB"
        assert result["asset_keys"] == ["a"]

    def test_empty_asset_list_fails(self):
        assert "message" in materialize_assets([])

    @pytest.mark.parametrize("asset_key", ["*", "/a", "a/", "a//b"])
    def test_rejects_non_concrete_asset_key(self, asset_key):
        result = materialize_assets([asset_key])

        assert "concrete slash-delimited asset keys" in result["message"]


class TestAssetToolCompatibility:
    def test_materializer_reports_unsupported_schema_without_preflight(
        self,
        monkeypatch,
    ):
        query_fields = server_module._MATERIALIZE_ASSETS_SCHEMA["Query"] - {
            "assetNodeAdditionalRequiredKeys"
        }
        asset_fields = server_module._MATERIALIZE_ASSETS_SCHEMA["AssetNode"]
        mock_post = MagicMock(
            side_effect=[
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [{"name": name} for name in query_fields]
                            }
                        }
                    }
                ),
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [{"name": name} for name in asset_fields]
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = materialize_assets(["asset_a"])

        assert "requires Dagster 1.9+" in result["message"]
        assert "Query.assetNodeAdditionalRequiredKeys" in result["message"]
        assert mock_post.call_count == 2
        assert all(
            "TypeFields" in call.kwargs["json"]["query"]
            for call in mock_post.call_args_list
        )

    def test_invalid_introspection_responses_are_not_cached(self, monkeypatch):
        mock_post = MagicMock(
            side_effect=[
                httpx.TimeoutException("temporary timeout"),
                _mock_response({"data": {"__type": None}}),
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [{"name": "assetNodes"}],
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        with pytest.raises(RuntimeError, match="timed out"):
            server_module._get_type_fields("Query")

        assert not server_module._type_fields
        with pytest.raises(RuntimeError, match="invalid GraphQL introspection"):
            server_module._get_type_fields("Query")

        assert not server_module._type_fields
        assert server_module._get_type_fields("Query") == frozenset({"assetNodes"})
        assert mock_post.call_count == 3


class TestLaunchJob:
    def test_launch_simple(self, mock_gql):
        mock_gql({"data": {"launchRun": {
            "run": {"runId": "r_new", "status": "STARTING"},
        }}})
        result = launch_job("my_job", "loc1", "repo1")
        assert result["run"]["runId"] == "r_new"

    def test_launch_with_assets(self, mock_gql):
        mock_post = mock_gql({"data": {"launchRun": {
            "run": {"runId": "r2", "status": "STARTING"},
        }}})
        launch_job("__ASSET_JOB", "loc1", "repo1",
                    asset_keys=["asset_a", "asset_b"])
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["assetSelection"] == [
            {"path": ["asset_a"]},
            {"path": ["asset_b"]},
        ]
        assert "solidSelection" not in payload["variables"]

    def test_launch_with_tags(self, mock_gql):
        mock_post = mock_gql({"data": {"launchRun": {
            "run": {"runId": "r3", "status": "STARTING"},
        }}})
        launch_job("my_job", "loc1", "repo1", tags={"env": "prod"})
        payload = mock_post.call_args.kwargs["json"]
        meta = payload["variables"]["executionMetadata"]
        assert meta["tags"] == [{"key": "env", "value": "prod"}]

    def test_launch_with_run_config(self, mock_gql):
        mock_post = mock_gql({"data": {"launchRun": {
            "run": {"runId": "r4", "status": "STARTING"},
        }}})
        config = {"ops": {"my_op": {"config": {"start_date": "2026-03-01"}}}}
        launch_job("my_job", "loc1", "repo1", run_config=config)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["runConfigData"] == config

    def test_launch_without_run_config_sends_empty(self, mock_gql):
        mock_post = mock_gql({"data": {"launchRun": {
            "run": {"runId": "r5", "status": "STARTING"},
        }}})
        launch_job("my_job", "loc1", "repo1")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["runConfigData"] == {}


class TestLaunchJobWithPartitions:
    def test_single_partition(self, mock_gql):
        mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}})
        result = launch_job_with_partitions("daily_job", "loc1", ["2024-01-01"])
        assert result["backfillId"] == "bf1"

    def test_multiple_partitions(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf2"}}})
        launch_job_with_partitions("daily_job", "loc1", ["2024-01-01", "2024-01-02"])
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["backfillParams"]["partitionNames"] == [
            "2024-01-01", "2024-01-02"
        ]

    def test_default_partition_set_name(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf3"}}})
        launch_job_with_partitions("daily_job", "loc1", ["2024-01-01"])
        payload = mock_post.call_args.kwargs["json"]
        selector = payload["variables"]["backfillParams"]["selector"]
        assert selector["partitionSetName"] == "daily_job_partition_set"

    def test_custom_partition_set_name(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf4"}}})
        launch_job_with_partitions(
            "daily_job", "loc1", ["2024-01-01"],
            partition_set_name="custom_partition_set",
        )
        payload = mock_post.call_args.kwargs["json"]
        selector = payload["variables"]["backfillParams"]["selector"]
        assert selector["partitionSetName"] == "custom_partition_set"

    def test_repository_selector(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf5"}}})
        launch_job_with_partitions(
            "daily_job", "my_location", ["2024-01-01"],
            repository_name="my_repo",
        )
        payload = mock_post.call_args.kwargs["json"]
        repo_selector = payload["variables"]["backfillParams"]["selector"]["repositorySelector"]
        assert repo_selector["repositoryLocationName"] == "my_location"
        assert repo_selector["repositoryName"] == "my_repo"

    def test_with_tags(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf6"}}})
        launch_job_with_partitions(
            "daily_job", "loc1", ["2024-01-01"],
            tags={"triggered_by": "agent"},
        )
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["backfillParams"]["tags"] == [
            {"key": "triggered_by", "value": "agent"}
        ]

    def test_from_failure(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf7"}}})
        launch_job_with_partitions(
            "daily_job", "loc1", ["2024-01-01"],
            from_failure=True,
        )
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["backfillParams"]["fromFailure"] is True

    def test_partition_set_not_found(self, mock_gql):
        mock_gql({"data": {"launchPartitionBackfill": {
            "message": "Partition set not found"
        }}})
        result = launch_job_with_partitions("bad_job", "loc1", ["2024-01-01"])
        assert "message" in result


class TestBackfillAssets:
    def test_run_config_is_forwarded_when_supported(self, monkeypatch):
        mock_post = MagicMock(
            side_effect=[
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "inputFields": [
                                    {"name": "assetSelection"},
                                    {"name": "runConfigData"},
                                ]
                            }
                        }
                    }
                ),
                _mock_response(
                    {
                        "data": {
                            "launchPartitionBackfill": {"backfillId": "configured"}
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)
        config = {"ops": {"benchmark": {"config": {"limit": 10}}}}

        result = backfill_assets(
            ["asset_a"],
            partition_keys=["p1"],
            run_config=config,
        )

        assert result["backfillId"] == "configured"
        launch_payload = mock_post.call_args_list[1].kwargs["json"]
        assert launch_payload["variables"]["backfillParams"]["runConfigData"] == config

    def test_run_config_reports_unsupported_schema(self, monkeypatch):
        mock_post = MagicMock(
            return_value=_mock_response(
                {
                    "data": {
                        "__type": {
                            "inputFields": [{"name": "assetSelection"}]
                        }
                    }
                }
            )
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = backfill_assets(
            ["asset_a"],
            partition_keys=["p1"],
            run_config={"ops": {}},
        )

        assert "does not expose" in result["message"]
        assert "runConfigData" in result["message"]
        assert mock_post.call_count == 1

    def test_explicit_partition_keys_skip_resolution(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}})
        result = backfill_assets(["asset_a"], partition_keys=["2026-07-01", "2026-07-02"])
        assert result["backfillId"] == "bf1"
        payload = mock_post.call_args.kwargs["json"]
        params = payload["variables"]["backfillParams"]
        assert params["partitionNames"] == ["2026-07-01", "2026-07-02"]
        assert params["assetSelection"] == [{"path": ["asset_a"]}]
        assert "partitionSetName" not in str(payload)

    def test_range_sliced_from_asset_partition_keys(self, mock_gql, monkeypatch):
        # Create two responses: partition keys query, then backfill launch
        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"]}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        backfill_resp = MagicMock()
        backfill_resp.status_code = 200
        backfill_resp.json.return_value = {"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}}
        backfill_resp.text = json.dumps(backfill_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp, backfill_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        result = backfill_assets(
            ["asset_a"], partition_start="2026-07-02", partition_end="2026-07-03"
        )
        assert result["backfillId"] == "bf1"
        payload = mock_post.call_args.kwargs["json"]
        params = payload["variables"]["backfillParams"]
        assert params["partitionNames"] == ["2026-07-02", "2026-07-03"]

    def test_range_defaults_to_full_key_list(self, mock_gql, monkeypatch):
        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": ["p1", "p2", "p3"]}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        backfill_resp = MagicMock()
        backfill_resp.status_code = 200
        backfill_resp.json.return_value = {"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}}
        backfill_resp.text = json.dumps(backfill_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp, backfill_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        backfill_assets(["asset_a"])
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["backfillParams"]["partitionNames"] == ["p1", "p2", "p3"]

    def test_bad_bound_reports_nearest_keys(self, mock_gql, monkeypatch):
        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": ["2026-07-01", "2026-07-02"]}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        result = backfill_assets(["asset_a"], partition_start="2026-06-30")
        assert "message" in result
        assert "2026-07-01" in result["message"]

    def test_unpartitioned_asset_errors(self, mock_gql, monkeypatch):
        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": []}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        result = backfill_assets(["asset_a"])
        assert "message" in result
        assert "not partitioned" in result["message"]
        assert "materialize_assets" in result["message"]

    def test_multi_asset_selection_and_tags(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf2"}}})
        backfill_assets(
            ["asset_a", "asset_b"],
            partition_keys=["p1"],
            tags={"triggered_by": "agent"},
        )
        payload = mock_post.call_args.kwargs["json"]
        params = payload["variables"]["backfillParams"]
        assert params["assetSelection"] == [{"path": ["asset_a"]}, {"path": ["asset_b"]}]
        assert params["tags"] == [{"key": "triggered_by", "value": "agent"}]

    def test_multi_segment_asset_keys_split_into_path(self, mock_gql):
        mock_post = mock_gql({"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}})
        backfill_assets(["raw_chargebee_dlt/customer"], partition_keys=["p1"])
        payload = mock_post.call_args.kwargs["json"]
        params = payload["variables"]["backfillParams"]
        assert params["assetSelection"] == [{"path": ["raw_chargebee_dlt", "customer"]}]

    def test_multi_segment_asset_key_resolution_split(self, monkeypatch):
        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": ["p1", "p2"]}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        backfill_resp = MagicMock()
        backfill_resp.status_code = 200
        backfill_resp.json.return_value = {"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}}
        backfill_resp.text = json.dumps(backfill_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp, backfill_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        backfill_assets(["raw_chargebee_dlt/customer"])
        lookup_payload = mock_post.call_args_list[0].kwargs["json"]
        assert lookup_payload["variables"]["assetKeys"] == [
            {"path": ["raw_chargebee_dlt", "customer"]}
        ]


def _locate_payload(name, field, sid="orig::sel", selector_id="sel", status="RUNNING"):
    """Canned repositoriesOrError response for the instigator locate query."""
    state = (
        {"id": sid, "selectorId": selector_id, "status": status}
        if sid is not None or selector_id is not None
        else None
    )
    return {"__typename": "RepositoryConnection", "nodes": [{
        "name": "repo",
        "location": {"name": "loc"},
        "schedules": (
            [{"name": name, "scheduleState": state}] if field == "schedules" else []
        ),
        "sensors": (
            [{"name": name, "sensorState": state}] if field == "sensors" else []
        ),
    }]}


def _empty_locate():
    return {"__typename": "RepositoryConnection", "nodes": [{
        "name": "repo", "location": {"name": "loc"},
        "schedules": [], "sensors": [],
    }]}


class TestStartSchedule:
    def test_success(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "startSchedule": {
                "__typename": "ScheduleStateResult",
                "scheduleState": {"id": "orig::sel", "name": "daily",
                                  "status": "RUNNING", "selectorId": "sel"},
            },
        }})
        result = start_schedule("daily")
        assert result["status"] == "RUNNING"
        assert result["instigator_type"] == "SCHEDULE"

    def test_sends_schedule_selector(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "startSchedule": {"__typename": "ScheduleStateResult",
                              "scheduleState": {"status": "RUNNING"}},
        }})
        start_schedule("daily")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["selector"] == {
            "repositoryName": "repo",
            "repositoryLocationName": "loc",
            "scheduleName": "daily",
        }

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": _empty_locate()}})
        result = start_schedule("daily")
        assert "not found" in result["message"]
        assert "status" not in result

    def test_locate_python_error_does_not_mutate(self, mock_gql):
        mock_post = mock_gql({"data": {"repositoriesOrError": {
            "__typename": "PythonError", "message": "location failed",
        }}})
        with pytest.raises(RuntimeError, match="location failed"):
            start_schedule("daily")
        mock_post.assert_called_once()

    def test_unauthorized(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "startSchedule": {"__typename": "UnauthorizedError", "message": "no perms"},
        }})
        result = start_schedule("daily")
        assert result["message"] == "no perms"
        assert "status" not in result

    def test_schedule_not_found_error_union(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "startSchedule": {"__typename": "ScheduleNotFoundError", "message": "gone"},
        }})
        assert start_schedule("daily")["message"] == "gone"

    def test_python_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "startSchedule": {"__typename": "PythonError", "message": "boom"},
        }})
        assert start_schedule("daily")["message"] == "boom"

    def test_already_running_is_not_an_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules", status="RUNNING"),
            "startSchedule": {"__typename": "ScheduleStateResult",
                              "scheduleState": {"status": "RUNNING"}},
        }})
        assert start_schedule("daily")["status"] == "RUNNING"


class TestStopSchedule:
    def test_success(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "stopRunningSchedule": {"__typename": "ScheduleStateResult",
                                    "scheduleState": {"status": "STOPPED"}},
        }})
        assert stop_schedule("daily")["status"] == "STOPPED"

    def test_sends_origin_and_selector_ids(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "stopRunningSchedule": {"__typename": "ScheduleStateResult",
                                    "scheduleState": {"status": "STOPPED"}},
        }})
        stop_schedule("daily")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"] == {"originId": "orig::sel", "selectorId": "sel"}
        # note: "scheduleSelectorId" legitimately contains "scheduleSelector",
        # so assert on the argument form "scheduleSelector:" instead.
        assert "scheduleSelector:" not in payload["query"]
        assert "stopRunningSchedule(id:" not in payload["query"]

    def test_not_found_does_not_mutate(self, monkeypatch):
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": _empty_locate()}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        result = stop_schedule("daily")
        assert mock_post.call_count == 1
        assert "not found" in result["message"]

    def test_missing_state_ids_does_not_mutate(self, monkeypatch):
        payload = _locate_payload("daily", "schedules", sid=None, selector_id=None)
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": payload}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        result = stop_schedule("daily")
        assert mock_post.call_count == 1
        assert "InstigationState.id" in result["message"]

    def test_unauthorized(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "stopRunningSchedule": {"__typename": "UnauthorizedError", "message": "no perms"},
        }})
        result = stop_schedule("daily")
        assert result["message"] == "no perms"
        assert "status" not in result

    def test_python_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "stopRunningSchedule": {"__typename": "PythonError", "message": "boom"},
        }})
        assert stop_schedule("daily")["message"] == "boom"


class TestStartSensor:
    def test_success(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "startSensor": {"__typename": "Sensor", "name": "s",
                            "sensorState": {"status": "RUNNING"}},
        }})
        result = start_sensor("s")
        assert result["status"] == "RUNNING"
        assert result["instigator_type"] == "SENSOR"

    def test_sends_sensor_selector(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "startSensor": {"__typename": "Sensor", "name": "s",
                            "sensorState": {"status": "RUNNING"}},
        }})
        start_sensor("s")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["selector"] == {
            "repositoryName": "repo",
            "repositoryLocationName": "loc",
            "sensorName": "s",
        }

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": _empty_locate()}})
        result = start_sensor("s")
        assert "not found" in result["message"]
        assert "status" not in result

    def test_sensor_not_found_error_union(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "startSensor": {"__typename": "SensorNotFoundError", "message": "gone"},
        }})
        assert start_sensor("s")["message"] == "gone"

    def test_unauthorized(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "startSensor": {"__typename": "UnauthorizedError", "message": "no perms"},
        }})
        assert start_sensor("s")["message"] == "no perms"

    def test_python_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "startSensor": {"__typename": "PythonError", "message": "boom"},
        }})
        assert start_sensor("s")["message"] == "boom"


class TestStopSensor:
    def test_success(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        assert stop_sensor("s")["status"] == "STOPPED"

    def test_null_instigation_state(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": None},
        }})
        result = stop_sensor("s")
        assert result["status"] is None
        assert "stopped" in result["message"]

    def test_sends_job_origin_and_selector_ids(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        stop_sensor("s")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"] == {"originId": "orig::sel", "selectorId": "sel"}
        assert "jobOriginId" in payload["query"]
        assert "sensorOriginId" not in payload["query"]
        assert "stopSensor(id:" not in payload["query"]

    def test_not_found_does_not_mutate(self, monkeypatch):
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": _empty_locate()}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        result = stop_sensor("s")
        assert mock_post.call_count == 1
        assert "not found" in result["message"]

    def test_missing_state_ids_does_not_mutate(self, monkeypatch):
        payload = _locate_payload("s", "sensors", sid=None, selector_id=None)
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": payload}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        result = stop_sensor("s")
        assert mock_post.call_count == 1
        assert "InstigationState.id" in result["message"]

    def test_unauthorized(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "stopSensor": {"__typename": "UnauthorizedError", "message": "no perms"},
        }})
        result = stop_sensor("s")
        assert result["message"] == "no perms"
        assert "status" not in result

    def test_python_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors"),
            "stopSensor": {"__typename": "PythonError", "message": "boom"},
        }})
        assert stop_sensor("s")["message"] == "boom"


def _multi_locate(name, field):
    """Two code locations exposing the same instigator name."""
    def node(repo, loc):
        state = {"id": f"{repo}::sel", "selectorId": f"sel-{repo}", "status": "RUNNING"}
        return {
            "name": repo,
            "location": {"name": loc},
            "schedules": (
                [{"name": name, "scheduleState": state}] if field == "schedules" else []
            ),
            "sensors": (
                [{"name": name, "sensorState": state}] if field == "sensors" else []
            ),
        }

    return {
        "__typename": "RepositoryConnection",
        "nodes": [node("repo_a", "loc_a"), node("repo_b", "loc_b")],
    }


class TestInstigatorDisambiguation:
    @pytest.mark.parametrize(
        "fn,name,field",
        [
            (start_schedule, "daily", "schedules"),
            (stop_schedule, "daily", "schedules"),
            (start_sensor, "s", "sensors"),
            (stop_sensor, "s", "sensors"),
        ],
    )
    def test_ambiguous_name_refuses_and_does_not_mutate(
        self, fn, name, field, monkeypatch
    ):
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": _multi_locate(name, field)}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        result = fn(name)
        assert mock_post.call_count == 1  # locate only, no mutation
        assert "Ambiguous" in result["message"]
        assert "loc_a/repo_a" in result["message"]
        assert "loc_b/repo_b" in result["message"]
        assert result["candidates"] == [
            {"repository": "repo_a", "location": "loc_a"},
            {"repository": "repo_b", "location": "loc_b"},
        ]
        assert "status" not in result

    def test_repository_filter_selects_the_right_one(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _multi_locate("s", "sensors"),
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        result = stop_sensor("s", repository_name="repo_b", location_name="loc_b")
        assert result["status"] == "STOPPED"
        assert result["repository"] == "repo_b"
        assert result["location"] == "loc_b"
        assert mock_post.call_args.kwargs["json"]["variables"] == {
            "originId": "repo_b::sel",
            "selectorId": "sel-repo_b",
        }

    def test_location_filter_used_for_start_schedule(self, mock_gql):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _multi_locate("daily", "schedules"),
            "startSchedule": {"__typename": "ScheduleStateResult",
                              "scheduleState": {"status": "RUNNING"}},
        }})
        result = start_schedule("daily", location_name="loc_b")
        assert result["status"] == "RUNNING"
        assert mock_post.call_args.kwargs["json"]["variables"]["selector"] == {
            "repositoryName": "repo_b",
            "repositoryLocationName": "loc_b",
            "scheduleName": "daily",
        }

    def test_filter_that_matches_nothing_is_not_found(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": _multi_locate("s", "sensors")}})
        result = start_sensor("s", repository_name="nope")
        assert "not found" in result["message"]

    def test_unavailable_location_prevents_selecting_apparently_unique_match(
        self, mock_gql
    ):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules"),
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {
                        "name": "loc",
                        "loadStatus": "LOADED",
                        "locationOrLoadError": {
                            "__typename": "RepositoryLocation",
                        },
                    },
                    {
                        "name": "broken_loc",
                        "loadStatus": "LOADED",
                        "locationOrLoadError": {
                            "__typename": "PythonError",
                            "message": "ImportError: boom",
                        },
                    },
                ],
            },
        }})
        with pytest.raises(RuntimeError, match="broken_loc.*ImportError: boom"):
            start_schedule("daily")
        mock_post.assert_called_once()
        query = mock_post.call_args.kwargs["json"]["query"]
        assert "__typename" in query
        assert "workspaceOrError" in query

    @pytest.mark.parametrize(
        "fn,name,field",
        [
            (start_schedule, "daily", "schedules"),
            (stop_schedule, "daily", "schedules"),
            (start_sensor, "s", "sensors"),
            (stop_sensor, "s", "sensors"),
        ],
    )
    def test_success_echoes_resolved_repository_and_location(
        self, fn, name, field, mock_gql
    ):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload(name, field),
            "startSchedule": {"__typename": "ScheduleStateResult",
                              "scheduleState": {"status": "RUNNING"}},
            "stopRunningSchedule": {"__typename": "ScheduleStateResult",
                                    "scheduleState": {"status": "STOPPED"}},
            "startSensor": {"__typename": "Sensor", "name": name,
                            "sensorState": {"status": "RUNNING"}},
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        result = fn(name)
        assert result["repository"] == "repo"
        assert result["location"] == "loc"


class TestMutationSchemaCompatibility:
    """Guards against fragments that do not validate on older Dagster schemas."""

    @pytest.mark.parametrize(
        "fn,name,field,mutation,success",
        [
            (start_schedule, "daily", "schedules", "startSchedule",
             {"__typename": "ScheduleStateResult",
              "scheduleState": {"status": "RUNNING"}}),
            (stop_schedule, "daily", "schedules", "stopRunningSchedule",
             {"__typename": "ScheduleStateResult",
              "scheduleState": {"status": "STOPPED"}}),
            (start_sensor, "s", "sensors", "startSensor",
             {"__typename": "Sensor", "name": "s",
              "sensorState": {"status": "RUNNING"}}),
            (stop_sensor, "s", "sensors", "stopSensor",
             {"__typename": "StopSensorMutationResult",
              "instigationState": {"status": "STOPPED"}}),
        ],
    )
    def test_error_members_use_interface_fragment(
        self, fn, name, field, mutation, success, mock_gql
    ):
        # ScheduleNotFoundError joined ScheduleMutationResult only in Dagster
        # 1.9; spreading it on 1.6-1.8 is a document validation error. Same
        # reasoning applies to the other error members, so all four mutations
        # use `... on Error { message }` instead of per-type fragments.
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload(name, field),
            mutation: success,
        }})
        fn(name)
        query = mock_post.call_args.kwargs["json"]["query"]
        assert "... on Error { message }" in query
        assert "ScheduleNotFoundError" not in query
        assert "SensorNotFoundError" not in query
        assert "UnauthorizedError" not in query
        assert "PythonError" not in query


class TestLocateQueryWeight:
    """Instigation state is resolved per instigator, so only ask when needed."""

    @pytest.mark.parametrize(
        "fn,name,field,mutation,success",
        [
            (start_schedule, "daily", "schedules", "startSchedule",
             {"__typename": "ScheduleStateResult",
              "scheduleState": {"status": "RUNNING"}}),
            (start_sensor, "s", "sensors", "startSensor",
             {"__typename": "Sensor", "name": "s",
              "sensorState": {"status": "RUNNING"}}),
        ],
    )
    def test_start_tools_do_not_resolve_instigation_state(
        self, fn, name, field, mutation, success, monkeypatch
    ):
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"repositoriesOrError": _locate_payload(name, field)}}),
            _mock_response({"data": {mutation: success}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        fn(name)
        locate_query = mock_post.call_args_list[0].kwargs["json"]["query"]
        assert "scheduleState" not in locate_query
        assert "sensorState" not in locate_query

    def test_get_tick_history_does_not_resolve_instigation_state(self, monkeypatch):
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {
                "repositoriesOrError": _locate_payload("daily", "schedules"),
            }}),
            _mock_response({"data": {"instigationStateOrError": {
                "__typename": "InstigationState", "ticks": [],
            }}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        get_tick_history("daily", "SCHEDULE")
        locate_query = mock_post.call_args_list[0].kwargs["json"]["query"]
        assert "scheduleState" not in locate_query
        assert "sensorState" not in locate_query

    @pytest.mark.parametrize(
        "fn,name,field,state_key",
        [
            (stop_schedule, "daily", "schedules", "scheduleState"),
            (stop_sensor, "s", "sensors", "sensorState"),
        ],
    )
    def test_stop_tools_do_resolve_instigation_state(
        self, fn, name, field, state_key, mock_gql
    ):
        mock_post = mock_gql({"data": {
            "repositoriesOrError": _locate_payload(name, field),
            "stopRunningSchedule": {"__typename": "ScheduleStateResult",
                                    "scheduleState": {"status": "STOPPED"}},
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        fn(name)
        locate_query = mock_post.call_args_list[0].kwargs["json"]["query"]
        assert f"{state_key} {{ id selectorId }}" in locate_query


class TestStopWhenAlreadyStopped:
    """Stopping an already-stopped instigator is reported, not treated as failure."""

    def test_schedule_already_stopped(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("daily", "schedules", status="STOPPED"),
            "stopRunningSchedule": {"__typename": "ScheduleStateResult",
                                    "scheduleState": {"status": "STOPPED"}},
        }})
        result = stop_schedule("daily")
        assert result["status"] == "STOPPED"
        assert "stopped" in result["message"]

    def test_sensor_already_stopped(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": _locate_payload("s", "sensors", status="STOPPED"),
            "stopSensor": {"__typename": "StopSensorMutationResult",
                           "instigationState": {"status": "STOPPED"}},
        }})
        result = stop_sensor("s")
        assert result["status"] == "STOPPED"
        assert "stopped" in result["message"]


class TestReadOnlyGating:
    def test_write_tools_not_registered_in_readonly(self):
        import asyncio
        from dagster_mcp.server import mcp, READ_ONLY

        tools = asyncio.run(mcp.list_tools())
        tool_names = [t.name for t in tools]
        if READ_ONLY:
            assert "resolve_asset_selection" in tool_names
            assert "materialize_assets" not in tool_names
            assert "reload_code_location" not in tool_names
            assert "terminate_run" not in tool_names
            assert "launch_job" not in tool_names
            assert "launch_job_with_partitions" not in tool_names
            assert "start_schedule" not in tool_names
            assert "stop_schedule" not in tool_names
            assert "start_sensor" not in tool_names
            assert "stop_sensor" not in tool_names

    def test_materializer_registered_in_readwrite_mode(self):
        script = """
import asyncio
from dagster_mcp.server import mcp
print("\\n".join(sorted(tool.name for tool in asyncio.run(mcp.list_tools()))))
"""
        env = os.environ.copy()
        env["DAGSTER_READ_ONLY"] = "false"

        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=os.getcwd(),
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        tool_names = set(result.stdout.splitlines())
        assert "resolve_asset_selection" in tool_names
        assert "materialize_assets" in tool_names
        assert "start_schedule" in tool_names
        assert "stop_schedule" in tool_names
        assert "start_sensor" in tool_names
        assert "stop_sensor" in tool_names
