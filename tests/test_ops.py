from dagster_mcp.server import list_jobs, list_schedules, list_sensors


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
