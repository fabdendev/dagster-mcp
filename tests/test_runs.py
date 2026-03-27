import json
from unittest.mock import MagicMock

import httpx

from dagster_mcp.server import (
    get_runs, get_run_status, get_run_logs, get_run_stats, get_run_failure_summary,
    _runs_filter_job_field,
)


def _mock_response(data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = json.dumps(data)
    return resp


class TestGetRuns:
    def test_no_filters(self, mock_gql):
        mock_gql({"data": {"runsOrError": {"results": [
            {"runId": "r1", "status": "SUCCESS", "jobName": "job_a",
             "startTime": 1000, "endTime": 1100, "tags": []},
        ]}}})
        result = get_runs()
        assert len(result) == 1
        assert result[0]["runId"] == "r1"

    def test_with_job_name(self, monkeypatch):
        mock_post = MagicMock(side_effect=[
            # 1st call: introspection
            _mock_response({"data": {"__type": {"inputFields": [
                {"name": "jobName"}, {"name": "statuses"}, {"name": "runIds"},
            ]}}}),
            # 2nd call: actual get_runs query
            _mock_response({"data": {"runsOrError": {"results": []}}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        get_runs(job_name="my_job")
        query_payload = mock_post.call_args_list[1].kwargs["json"]
        assert query_payload["variables"]["filter"]["jobName"] == "my_job"

    def test_with_job_name_pipeline_field(self, monkeypatch):
        """On older/newer Dagster versions the field is pipelineName."""
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"__type": {"inputFields": [
                {"name": "pipelineName"}, {"name": "statuses"},
            ]}}}),
            _mock_response({"data": {"runsOrError": {"results": []}}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        get_runs(job_name="my_job")
        query_payload = mock_post.call_args_list[1].kwargs["json"]
        assert query_payload["variables"]["filter"]["pipelineName"] == "my_job"

    def test_introspection_cached(self, monkeypatch):
        """Introspection should only happen once per URL."""
        mock_post = MagicMock(side_effect=[
            _mock_response({"data": {"__type": {"inputFields": [{"name": "jobName"}]}}}),
            _mock_response({"data": {"runsOrError": {"results": []}}}),
            # 2nd get_runs call — no introspection, just the query
            _mock_response({"data": {"runsOrError": {"results": []}}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        get_runs(job_name="a")
        get_runs(job_name="b")
        assert mock_post.call_count == 3  # 1 introspection + 2 queries

    def test_introspection_fallback_on_error(self, monkeypatch):
        """If introspection fails, fall back to jobName."""
        mock_post = MagicMock(side_effect=[
            Exception("connection refused"),
            _mock_response({"data": {"runsOrError": {"results": []}}}),
        ])
        monkeypatch.setattr(httpx, "post", mock_post)
        get_runs(job_name="my_job")
        query_payload = mock_post.call_args_list[1].kwargs["json"]
        assert query_payload["variables"]["filter"]["jobName"] == "my_job"

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
            "rootRunId": "r1", "parentRunId": None,
            "resolvedOpSelection": None,
        }}})
        result = get_run_status("r1")
        assert result["status"] == "SUCCESS"
        assert result["runId"] == "r1"
        assert result["rootRunId"] == "r1"
        assert result["parentRunId"] is None

    def test_reexecuted_run(self, mock_gql):
        mock_gql({"data": {"runOrError": {
            "runId": "r2", "status": "SUCCESS", "startTime": 2000,
            "endTime": 2100, "jobName": "job_a", "tags": [],
            "runConfigYaml": "{}",
            "rootRunId": "r1", "parentRunId": "r1",
            "resolvedOpSelection": ["step_a"],
        }}})
        result = get_run_status("r2")
        assert result["parentRunId"] == "r1"
        assert result["rootRunId"] == "r1"
        assert result["resolvedOpSelection"] == ["step_a"]

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

    def test_level_filter_error(self, mock_gql):
        mock_gql({"data": {"logsForRun": {
            "cursor": "c1", "hasMore": False,
            "events": [
                {"__typename": "RunStartEvent", "timestamp": "1000",
                 "message": "Started", "level": "INFO"},
                {"__typename": "EngineEvent", "timestamp": "1050",
                 "message": "Warning msg", "level": "WARNING", "stepKey": None,
                 "error": None},
                {"__typename": "ExecutionStepFailureEvent", "timestamp": "1100",
                 "message": "Step failed", "level": "ERROR", "stepKey": "step_a",
                 "error": {"message": "boom", "causes": []}},
                {"__typename": "RunFailureEvent", "timestamp": "1200",
                 "message": "Run failed", "level": "CRITICAL",
                 "error": {"message": "steps failed", "causes": []}},
            ],
        }}})
        result = get_run_logs("r1", level_filter="ERROR")
        # Should include the ERROR-level event AND the RunFailureEvent (even though CRITICAL)
        assert len(result["events"]) == 2
        types = {e["__typename"] for e in result["events"]}
        assert "ExecutionStepFailureEvent" in types
        assert "RunFailureEvent" in types

    def test_level_filter_none_returns_all(self, mock_gql):
        mock_gql({"data": {"logsForRun": {
            "cursor": "c1", "hasMore": False,
            "events": [
                {"__typename": "RunStartEvent", "timestamp": "1000",
                 "message": "Started", "level": "INFO"},
                {"__typename": "ExecutionStepFailureEvent", "timestamp": "1100",
                 "message": "Failed", "level": "ERROR", "stepKey": "s",
                 "error": {"message": "x", "causes": []}},
            ],
        }}})
        result = get_run_logs("r1", level_filter=None)
        assert len(result["events"]) == 2

    def test_level_filter_warning(self, mock_gql):
        mock_gql({"data": {"logsForRun": {
            "cursor": "c1", "hasMore": False,
            "events": [
                {"__typename": "EngineEvent", "timestamp": "1000",
                 "message": "warn", "level": "WARNING", "stepKey": None,
                 "error": None},
                {"__typename": "RunStartEvent", "timestamp": "900",
                 "message": "start", "level": "INFO"},
            ],
        }}})
        result = get_run_logs("r1", level_filter="WARNING")
        assert len(result["events"]) == 1
        assert result["events"][0]["level"] == "WARNING"


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


def _make_mock_post(responses):
    """Create a mock httpx.post that returns different responses on successive calls."""
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


class TestGetRunFailureSummary:
    def test_failed_run_two_steps(self, monkeypatch):
        responses = [
            # 1st call: status + stats
            {"data": {"runOrError": {
                "runId": "r1", "status": "FAILURE", "jobName": "job_a",
                "startTime": 1000, "endTime": 1200,
                "stepStats": [
                    {"stepKey": "extract", "status": "SUCCESS",
                     "startTime": 1000, "endTime": 1050},
                    {"stepKey": "transform", "status": "FAILURE",
                     "startTime": 1050, "endTime": 1100},
                    {"stepKey": "load", "status": "FAILURE",
                     "startTime": 1100, "endTime": 1150},
                ],
            }}},
            # 2nd call: logs
            {"data": {"logsForRun": {
                "cursor": "c1", "hasMore": False,
                "events": [
                    {"__typename": "ExecutionStepFailureEvent",
                     "timestamp": "1100", "stepKey": "transform",
                     "error": {"message": "NullPointerError", "causes": [{"message": "col X is null"}]}},
                    {"__typename": "ExecutionStepFailureEvent",
                     "timestamp": "1150", "stepKey": "load",
                     "error": {"message": "Upstream failed", "causes": []}},
                    {"__typename": "RunFailureEvent",
                     "timestamp": "1200",
                     "error": {"message": "Steps failed", "causes": []}},
                ],
            }}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_run_failure_summary("r1")

        assert result["status"] == "FAILURE"
        assert result["job_name"] == "job_a"
        assert result["duration_seconds"] == 200.0
        assert len(result["failed_steps"]) == 2
        assert result["failed_steps"][0]["step_key"] == "transform"
        assert result["failed_steps"][0]["error"]["message"] == "NullPointerError"
        assert result["root_cause_error"]["message"] == "Steps failed"
        assert len(result["all_step_durations"]) == 3
        assert any("Multiple steps failed" in s for s in result["suggestions"])

    def test_successful_run(self, mock_gql):
        mock_gql({"data": {"runOrError": {
            "runId": "r2", "status": "SUCCESS", "jobName": "j",
            "startTime": 1000, "endTime": 1100,
            "stepStats": [],
        }}})
        result = get_run_failure_summary("r2")
        assert result["message"] == "Run did not fail."

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"runOrError": {"message": "Run not found"}}})
        result = get_run_failure_summary("r999")
        assert result["message"] == "Run not found"

    def test_canceled_run(self, monkeypatch):
        responses = [
            {"data": {"runOrError": {
                "runId": "r3", "status": "CANCELED", "jobName": "j",
                "startTime": 1000, "endTime": 1100,
                "stepStats": [],
            }}},
            {"data": {"logsForRun": {
                "cursor": None, "hasMore": False, "events": [],
            }}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_run_failure_summary("r3")
        assert result["status"] == "CANCELED"
        assert any("canceled" in s.lower() for s in result["suggestions"])

    def test_retry_suggestion(self, monkeypatch):
        responses = [
            {"data": {"runOrError": {
                "runId": "r4", "status": "FAILURE", "jobName": "j",
                "startTime": 1000, "endTime": 1200,
                "stepStats": [
                    {"stepKey": "flaky", "status": "FAILURE",
                     "startTime": 1000, "endTime": 1200},
                ],
            }}},
            {"data": {"logsForRun": {
                "cursor": None, "hasMore": False,
                "events": [
                    {"__typename": "ExecutionStepUpForRetryEvent",
                     "timestamp": "1100", "stepKey": "flaky",
                     "secondsToWait": 30,
                     "error": {"message": "timeout", "causes": []}},
                    {"__typename": "ExecutionStepFailureEvent",
                     "timestamp": "1200", "stepKey": "flaky",
                     "error": {"message": "timeout again", "causes": []}},
                ],
            }}},
        ]
        monkeypatch.setattr(httpx, "post", _make_mock_post(responses))
        result = get_run_failure_summary("r4")
        assert any("retried" in s.lower() for s in result["suggestions"])
