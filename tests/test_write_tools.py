from dagster_mcp.server import launch_job, terminate_run, reload_code_location


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
        assert payload["variables"]["solidSelection"] == ["asset_a", "asset_b"]

    def test_launch_with_tags(self, mock_gql):
        mock_post = mock_gql({"data": {"launchRun": {
            "run": {"runId": "r3", "status": "STARTING"},
        }}})
        launch_job("my_job", "loc1", "repo1", tags={"env": "prod"})
        payload = mock_post.call_args.kwargs["json"]
        meta = payload["variables"]["executionMetadata"]
        assert meta["tags"] == [{"key": "env", "value": "prod"}]


class TestReadOnlyGating:
    def test_write_tools_not_registered_in_readonly(self):
        import asyncio
        from dagster_mcp.server import mcp, READ_ONLY
        tools = asyncio.run(mcp.list_tools())
        tool_names = [t.name for t in tools]
        if READ_ONLY:
            assert "reload_code_location" not in tool_names
            assert "terminate_run" not in tool_names
            assert "launch_job" not in tool_names
