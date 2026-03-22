from dagster_mcp.server import list_code_locations, list_backfills, get_instance_status


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


def _instance_response(daemons=None, queued_runs=None, locations=None):
    return {"data": {
        "instance": {"daemonHealth": {"allDaemonStatuses": daemons or []}},
        "runsOrError": {"results": queued_runs or []},
        "workspaceOrError": {"locationEntries": locations or []},
    }}


class TestGetInstanceStatus:
    def test_healthy(self, mock_gql):
        mock_gql(_instance_response(
            daemons=[
                {"daemonType": "SCHEDULER", "required": True,
                 "healthy": True, "lastHeartbeatTime": "1000"},
                {"daemonType": "SENSOR", "required": True,
                 "healthy": True, "lastHeartbeatTime": "1000"},
            ],
            locations=[
                {"name": "loc1", "loadStatus": "LOADED",
                 "locationOrLoadError": {"name": "loc1"}},
            ],
        ))
        result = get_instance_status()
        assert result["healthy"] is True
        assert len(result["daemons"]) == 2
        assert result["queued_runs_count"] == 0
        assert result["code_location_errors"] == []

    def test_unhealthy_daemon(self, mock_gql):
        mock_gql(_instance_response(
            daemons=[
                {"daemonType": "SCHEDULER", "required": True,
                 "healthy": False, "lastHeartbeatTime": "500"},
            ],
        ))
        result = get_instance_status()
        assert result["healthy"] is False
        assert result["daemons"][0]["healthy"] is False

    def test_code_location_error(self, mock_gql):
        mock_gql(_instance_response(
            daemons=[
                {"daemonType": "SCHEDULER", "required": True,
                 "healthy": True, "lastHeartbeatTime": "1000"},
            ],
            locations=[
                {"name": "broken_loc", "loadStatus": "LOADED",
                 "locationOrLoadError": {"message": "ImportError: no module"}},
            ],
        ))
        result = get_instance_status()
        assert result["healthy"] is False
        assert len(result["code_location_errors"]) == 1
        assert result["code_location_errors"][0]["name"] == "broken_loc"

    def test_queued_runs(self, mock_gql):
        mock_gql(_instance_response(
            daemons=[
                {"daemonType": "SCHEDULER", "required": True,
                 "healthy": True, "lastHeartbeatTime": "1000"},
            ],
            queued_runs=[{"runId": "r1"}, {"runId": "r2"}, {"runId": "r3"}],
        ))
        result = get_instance_status()
        assert result["queued_runs_count"] == 3
        assert result["healthy"] is True
