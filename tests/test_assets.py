from dagster_mcp.server import search_assets, get_asset_details, get_recent_materializations


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
