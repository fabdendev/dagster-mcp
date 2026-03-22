from dagster_mcp.server import list_code_locations, list_backfills


class TestListCodeLocations:
    def test_locations(self, mock_gql):
        mock_gql({"data": {"workspaceOrError": {"locationEntries": [
            {"name": "loc1", "loadStatus": "LOADED",
             "locationOrLoadError": {
                 "name": "loc1", "repositories": [{"name": "repo1"}]}},
        ]}}})
        result = list_code_locations()
        assert len(result) == 1
        assert result[0]["name"] == "loc1"

    def test_empty(self, mock_gql):
        mock_gql({"data": {"workspaceOrError": {"locationEntries": []}}})
        assert list_code_locations() == []


class TestListBackfills:
    def test_backfills(self, mock_gql):
        mock_gql({"data": {"partitionBackfillsOrError": {"results": [
            {"backfillId": "bf1", "status": "COMPLETED",
             "numPartitions": 10, "timestamp": "1000",
             "partitionNames": ["p1", "p2"],
             "partitionSetName": "my_set"},
        ]}}})
        result = list_backfills()
        assert len(result) == 1
        assert result[0]["backfillId"] == "bf1"

    def test_empty(self, mock_gql):
        mock_gql({"data": {"partitionBackfillsOrError": {"results": []}}})
        assert list_backfills() == []

    def test_custom_limit(self, mock_gql):
        mock_post = mock_gql({"data": {"partitionBackfillsOrError": {"results": []}}})
        list_backfills(limit=5)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["limit"] == 5
