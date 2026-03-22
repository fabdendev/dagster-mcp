import json
from unittest.mock import MagicMock

import httpx

from dagster_mcp.server import search_assets, get_asset_details, get_recent_materializations, get_asset_health


class TestSearchAssets:
    def test_no_filters(self, mock_gql):
        mock_gql({"data": {"assetNodes": [
            {"assetKey": {"path": ["my_asset"]}, "groupName": "default",
             "description": "desc", "isPartitioned": False, "op": {"name": "my_op"}},
        ]}})
        result = search_assets()
        assert len(result) == 1
        assert result[0]["assetKey"]["path"] == ["my_asset"]

    def test_prefix_filter(self, mock_gql):
        mock_gql({"data": {"assetNodes": [
            {"assetKey": {"path": ["orders_raw"]}, "groupName": "g1",
             "description": "", "isPartitioned": False, "op": None},
            {"assetKey": {"path": ["users_raw"]}, "groupName": "g1",
             "description": "", "isPartitioned": False, "op": None},
        ]}})
        result = search_assets(prefix="orders")
        assert len(result) == 1
        assert result[0]["assetKey"]["path"] == ["orders_raw"]

    def test_group_filter(self, mock_gql):
        mock_gql({"data": {"assetNodes": [
            {"assetKey": {"path": ["a1"]}, "groupName": "analytics",
             "description": "", "isPartitioned": False, "op": None},
            {"assetKey": {"path": ["a2"]}, "groupName": "ingestion",
             "description": "", "isPartitioned": False, "op": None},
        ]}})
        result = search_assets(group="analytics")
        assert len(result) == 1
        assert result[0]["groupName"] == "analytics"

    def test_empty_results(self, mock_gql):
        mock_gql({"data": {"assetNodes": []}})
        assert search_assets() == []


class TestGetAssetDetails:
    def test_single_asset(self, mock_gql):
        mock_gql({"data": {"assetNodes": [
            {"assetKey": {"path": ["my_asset"]}, "description": "A table",
             "groupName": "default", "op": {"name": "my_op"},
             "isObservable": False, "isPartitioned": True,
             "partitionDefinition": {"description": "daily"},
             "dependencyKeys": [{"path": ["upstream"]}],
             "dependedByKeys": [],
             "assetMaterializations": [{"runId": "r1", "timestamp": "1000"}]},
        ]}})
        result = get_asset_details(["my_asset"])
        assert len(result) == 1
        assert result[0]["isPartitioned"] is True

    def test_multiple_assets(self, mock_gql):
        mock_gql({"data": {"assetNodes": [
            {"assetKey": {"path": ["a1"]}, "description": "", "groupName": "g",
             "op": None, "isObservable": False, "isPartitioned": False,
             "partitionDefinition": None, "dependencyKeys": [],
             "dependedByKeys": [], "assetMaterializations": []},
            {"assetKey": {"path": ["a2"]}, "description": "", "groupName": "g",
             "op": None, "isObservable": False, "isPartitioned": False,
             "partitionDefinition": None, "dependencyKeys": [],
             "dependedByKeys": [], "assetMaterializations": []},
        ]}})
        result = get_asset_details(["a1", "a2"])
        assert len(result) == 2


class TestGetRecentMaterializations:
    def test_materializations(self, mock_gql):
        mock_gql({"data": {"assetOrError": {"assetMaterializations": [
            {"runId": "r1", "timestamp": "1000",
             "assetKey": {"path": ["my_asset"]},
             "metadataEntries": [{"label": "rows", "intValue": 42}]},
        ]}}})
        result = get_recent_materializations("my_asset")
        assert len(result) == 1
        assert result[0]["runId"] == "r1"

    def test_empty(self, mock_gql):
        mock_gql({"data": {"assetOrError": {"assetMaterializations": []}}})
        assert get_recent_materializations("missing") == []

    def test_custom_limit(self, mock_gql):
        mock_post = mock_gql({"data": {"assetOrError": {"assetMaterializations": []}}})
        get_recent_materializations("x", limit=3)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["limit"] == 3


def _make_mock_post(responses):
    call_count = 0

    def _post(*args, **kwargs):
        nonlocal call_count
        resp_data = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = resp_data
        mock_resp.text = json.dumps(resp_data)
        return mock_resp

    return _post


class TestGetAssetHealth:
    def test_single_asset_healthy(self, monkeypatch):
        responses = [
            # 1st: all assets (group check)
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["my_asset"]}, "groupName": "default"},
            ]}},
            # 2nd: health details
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["my_asset"]}, "groupName": "default",
                 "freshnessPolicy": None, "staleCauses": [],
                 "assetMaterializations": [{"runId": "r1", "timestamp": "1000"}]},
            ]}},
            # 3rd: run status
            {"data": {"runsOrError": {"results": [
                {"runId": "r1", "status": "SUCCESS"},
            ]}}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_asset_health("my_asset")
        assert len(result) == 1
        assert result[0]["asset_key"] == ["my_asset"]
        assert result[0]["latest_run_status"] == "SUCCESS"
        assert result[0]["stale"] is False

    def test_stale_asset(self, monkeypatch):
        responses = [
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["stale_asset"]}, "groupName": "g"},
            ]}},
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["stale_asset"]}, "groupName": "g",
                 "freshnessPolicy": {"maximumLagMinutes": 60, "cronSchedule": "0 * * * *"},
                 "staleCauses": [{"key": {"path": ["stale_asset"]},
                                  "reason": "outdated", "dependency": None}],
                 "assetMaterializations": [{"runId": "r2", "timestamp": "500"}]},
            ]}},
            {"data": {"runsOrError": {"results": [
                {"runId": "r2", "status": "SUCCESS"},
            ]}}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_asset_health("stale_asset")
        assert result[0]["stale"] is True
        assert result[0]["freshness_policy"]["max_lag_minutes"] == 60

    def test_group_query(self, monkeypatch):
        responses = [
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["a1"]}, "groupName": "analytics"},
                {"assetKey": {"path": ["a2"]}, "groupName": "analytics"},
                {"assetKey": {"path": ["b1"]}, "groupName": "other"},
            ]}},
            {"data": {"assetNodes": [
                {"assetKey": {"path": ["a1"]}, "groupName": "analytics",
                 "freshnessPolicy": None, "staleCauses": [],
                 "assetMaterializations": []},
                {"assetKey": {"path": ["a2"]}, "groupName": "analytics",
                 "freshnessPolicy": None, "staleCauses": [],
                 "assetMaterializations": []},
            ]}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_asset_health("analytics")
        assert len(result) == 2

    def test_not_found(self, monkeypatch):
        responses = [
            {"data": {"assetNodes": []}},
            {"data": {"assetNodes": []}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_asset_health("nonexistent")
        assert "not found" in result[0].get("message", "").lower()
