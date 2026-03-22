from dagster_mcp.server import get_runs, get_run_status, get_run_logs, get_run_stats


class TestGetRuns:
    def test_no_filters(self, mock_gql):
        mock_gql({"data": {"runsOrError": {"results": [
            {"runId": "r1", "status": "SUCCESS", "jobName": "job_a",
             "startTime": 1000, "endTime": 1100, "tags": []},
        ]}}})
        result = get_runs()
        assert len(result) == 1
        assert result[0]["runId"] == "r1"

    def test_with_job_name(self, mock_gql):
        mock_post = mock_gql({"data": {"runsOrError": {"results": []}}})
        get_runs(job_name="my_job")
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["filter"]["jobName"] == "my_job"

    def test_with_statuses(self, mock_gql):
        mock_post = mock_gql({"data": {"runsOrError": {"results": []}}})
        get_runs(statuses=["FAILURE", "CANCELED"])
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["filter"]["statuses"] == ["FAILURE", "CANCELED"]

    def test_empty_results(self, mock_gql):
        mock_gql({"data": {"runsOrError": {"results": []}}})
        assert get_runs() == []

    def test_custom_limit(self, mock_gql):
        mock_post = mock_gql({"data": {"runsOrError": {"results": []}}})
        get_runs(limit=5)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["limit"] == 5


class TestGetRunStatus:
    def test_found(self, mock_gql):
        mock_gql({"data": {"runOrError": {
            "runId": "r1", "status": "SUCCESS", "startTime": 1000,
            "endTime": 1100, "jobName": "job_a", "tags": [],
            "runConfigYaml": "{}",
        }}})
        result = get_run_status("r1")
        assert result["status"] == "SUCCESS"
        assert result["runId"] == "r1"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"runOrError": {"message": "Run r999 not found"}}})
        result = get_run_status("r999")
        assert "message" in result

    def test_python_error(self, mock_gql):
        mock_gql({"data": {"runOrError": {"message": "Internal error"}}})
        result = get_run_status("bad")
        assert result["message"] == "Internal error"


class TestGetRunLogs:
    def test_events_returned(self, mock_gql):
        mock_gql({"data": {"logsForRun": {
            "cursor": "c1", "hasMore": False,
            "events": [
                {"__typename": "RunStartEvent", "timestamp": "1000",
                 "message": "Started", "level": "INFO"},
                {"__typename": "ExecutionStepFailureEvent", "timestamp": "1100",
                 "message": "Step failed", "level": "ERROR", "stepKey": "step_a",
                 "error": {"message": "boom", "causes": []}},
            ],
        }}})
        result = get_run_logs("r1")
        assert result["hasMore"] is False
        assert len(result["events"]) == 2

    def test_pagination(self, mock_gql):
        mock_post = mock_gql({"data": {"logsForRun": {
            "cursor": "c2", "hasMore": True, "events": [],
        }}})
        result = get_run_logs("r1", cursor="c1", limit=50)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["variables"]["afterCursor"] == "c1"
        assert payload["variables"]["limit"] == 50
        assert result["hasMore"] is True

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"logsForRun": {"message": "Run not found"}}})
        result = get_run_logs("r999")
        assert "message" in result


class TestGetRunStats:
    def test_step_stats(self, mock_gql):
        mock_gql({"data": {"runOrError": {
            "runId": "r1", "status": "SUCCESS",
            "stepStats": [
                {"stepKey": "step_a", "status": "SUCCESS",
                 "startTime": 1000, "endTime": 1050,
                 "materializations": [{"label": "mat1"}],
                 "expectationResults": [{"success": True, "label": "exp1"}]},
            ],
        }}})
        result = get_run_stats("r1")
        assert result["runId"] == "r1"
        assert len(result["stepStats"]) == 1
        assert result["stepStats"][0]["stepKey"] == "step_a"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"runOrError": {"message": "Run not found"}}})
        result = get_run_stats("r999")
        assert result["message"] == "Run not found"
