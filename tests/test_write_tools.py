from dagster_mcp.server import launch_job, launch_job_with_partitions, terminate_run, reload_code_location, backfill_assets


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


class TestBackfillAssets:
    def _mock_partition_keys(self, mock_gql, keys, backfill_ok=True):
        """First gql call resolves partitionKeys, second launches the backfill."""
        from unittest.mock import MagicMock, patch

        responses = [
            {"data": {"assetNodes": [{"partitionKeys": keys}]}},
        ]
        if backfill_ok:
            responses.append(
                {"data": {"launchPartitionBackfill": {"backfillId": "bf1"}}}
            )

        # Create mock responses
        mock_responses = []
        for resp_data in responses:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = resp_data
            import json
            mock_resp.text = json.dumps(resp_data)
            mock_responses.append(mock_resp)

        # Patch httpx.post with side_effect
        import httpx
        mock_post = MagicMock(side_effect=mock_responses)
        import dagster_mcp.server
        with patch.object(httpx, 'post', mock_post):
            pass  # The patch context manager is entered; tests will use it

        return mock_post

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
        from unittest.mock import MagicMock, patch
        import httpx
        import json

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
        from unittest.mock import MagicMock, patch
        import httpx
        import json

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
        from unittest.mock import MagicMock
        import httpx
        import json

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
        from unittest.mock import MagicMock
        import httpx
        import json

        keys_resp = MagicMock()
        keys_resp.status_code = 200
        keys_resp.json.return_value = {"data": {"assetNodes": [{"partitionKeys": []}]}}
        keys_resp.text = json.dumps(keys_resp.json.return_value)

        mock_post = MagicMock(side_effect=[keys_resp])
        monkeypatch.setattr(httpx, "post", mock_post)

        result = backfill_assets(["asset_a"])
        assert "message" in result
        assert "not partitioned" in result["message"]

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
