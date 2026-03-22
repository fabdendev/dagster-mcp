import pytest

from dagster_mcp.server import list_jobs, list_schedules, list_sensors, get_tick_history


class TestListJobs:
    def test_jobs_across_repos(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": [
            {"name": "repo1", "location": {"name": "loc1"}, "jobs": [
                {"name": "job_a", "description": "Job A"},
                {"name": "job_b", "description": ""},
            ]},
            {"name": "repo2", "location": {"name": "loc2"}, "jobs": [
                {"name": "job_c", "description": "Job C"},
            ]},
        ]}}})
        result = list_jobs()
        assert len(result) == 3
        assert result[0] == {
            "repository": "repo1", "location": "loc1",
            "job": "job_a", "description": "Job A",
        }

    def test_empty(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": []}}})
        assert list_jobs() == []


class TestListSchedules:
    def test_schedules(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": [
            {"name": "repo1", "location": {"name": "loc1"}, "schedules": [
                {"name": "daily_sched", "cronSchedule": "0 0 * * *",
                 "scheduleState": {"status": "RUNNING"},
                 "futureTicks": {"results": [{"timestamp": "9999"}]},
                 "pipelineName": "job_a"},
            ]},
        ]}}})
        result = list_schedules()
        assert len(result) == 1
        assert result[0]["cron"] == "0 0 * * *"
        assert result[0]["next_tick"] == "9999"

    def test_no_future_ticks(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": [
            {"name": "r", "location": {"name": "l"}, "schedules": [
                {"name": "s", "cronSchedule": "0 * * * *",
                 "scheduleState": {"status": "STOPPED"},
                 "futureTicks": {"results": []},
                 "pipelineName": "j"},
            ]},
        ]}}})
        result = list_schedules()
        assert result[0]["next_tick"] is None


class TestListSensors:
    def test_sensors(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": [
            {"name": "repo1", "location": {"name": "loc1"}, "sensors": [
                {"name": "my_sensor",
                 "sensorState": {"status": "RUNNING"},
                 "targets": [{"pipelineName": "job_a"}]},
            ]},
        ]}}})
        result = list_sensors()
        assert len(result) == 1
        assert result[0]["targets"] == ["job_a"]

    def test_empty(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": []}}})
        assert list_sensors() == []


class TestGetTickHistory:
    def test_schedule_ticks(self, mock_gql):
        mock_gql({"data": {"instigationStatesOrError": {"results": [
            {"name": "daily_sched", "instigationType": "SCHEDULE", "ticks": [
                {"tickId": "t1", "status": "SUCCESS", "timestamp": "1000",
                 "error": None, "runIds": ["r1"]},
                {"tickId": "t2", "status": "SKIPPED", "timestamp": "900",
                 "error": None, "runIds": []},
            ]},
        ]}}})
        result = get_tick_history("daily_sched", "SCHEDULE", limit=10)
        assert result["name"] == "daily_sched"
        assert len(result["ticks"]) == 2
        assert result["ticks"][0]["status"] == "SUCCESS"
        assert result["ticks"][0]["run_ids"] == ["r1"]

    def test_sensor_ticks_with_error(self, mock_gql):
        mock_gql({"data": {"instigationStatesOrError": {"results": [
            {"name": "my_sensor", "instigationType": "SENSOR", "ticks": [
                {"tickId": "t1", "status": "FAILURE", "timestamp": "1000",
                 "error": {"message": "Connection refused"}, "runIds": []},
            ]},
        ]}}})
        result = get_tick_history("my_sensor", "sensor")
        assert result["ticks"][0]["error"] == "Connection refused"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"instigationStatesOrError": {"results": [
            {"name": "other_sensor", "instigationType": "SENSOR", "ticks": []},
        ]}}})
        result = get_tick_history("missing_sensor", "SENSOR")
        assert "not found" in result["message"]

    def test_invalid_type(self, mock_gql):
        with pytest.raises(ValueError, match="must be 'SCHEDULE' or 'SENSOR'"):
            get_tick_history("x", "INVALID")

    def test_python_error(self, mock_gql):
        mock_gql({"data": {"instigationStatesOrError": {"message": "Something broke"}}})
        result = get_tick_history("x", "SCHEDULE")
        assert result["message"] == "Something broke"
