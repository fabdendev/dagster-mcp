from dagster_mcp.server import launch_job, launch_job_with_partitions, terminate_run, reload_code_location


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
            assert "launch_job_with_partitions" not in tool_names
