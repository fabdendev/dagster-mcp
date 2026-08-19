import asyncio
from unittest.mock import MagicMock

import pytest

from dagster_mcp import server
from dagster_mcp.server import get_tick_history, list_jobs, list_schedules, list_sensors


class TestListJobs:
    @staticmethod
    def _repository(name: str, location: str, *jobs: str) -> dict[str, object]:
        return {
            "name": name,
            "location": {"name": location},
            "jobs": [
                {"name": job, "description": f"Description for {job}"} for job in jobs
            ],
        }

    @staticmethod
    def _connection(*repositories: dict[str, object]) -> dict[str, object]:
        return {"__typename": "RepositoryConnection", "nodes": list(repositories)}

    @staticmethod
    def _assert_union_fields(query: str) -> None:
        assert "__typename" in query
        assert "... on RepositoryNotFoundError { message }" in query
        assert "... on PythonError { message }" in query

    def test_jobs_across_repos(self, mock_gql) -> None:
        mock_post = mock_gql(
            {
                "data": {
                    "repositoriesOrError": self._connection(
                        {
                            "name": "repo1",
                            "location": {"name": "loc1"},
                            "jobs": [
                                {"name": "job_a", "description": "Job A"},
                                {"name": "job_b", "description": ""},
                            ],
                        },
                        {
                            "name": "repo2",
                            "location": {"name": "loc2"},
                            "jobs": [
                                {"name": "job_c", "description": "Job C"},
                            ],
                        },
                    )
                }
            }
        )
        result = list_jobs()
        assert len(result) == 3
        assert result[0] == {
            "repository": "repo1",
            "location": "loc1",
            "job": "job_a",
            "description": "Job A",
        }
        self._assert_union_fields(mock_post.call_args.kwargs["json"]["query"])

    def test_empty(self, mock_gql) -> None:
        mock_gql({"data": {"repositoriesOrError": self._connection()}})
        assert list_jobs() == []

    def test_repository_filter_batches_exact_matches_in_stable_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repositories = [
            self._repository("example_repo", "north"),
            self._repository("example_repo", "south"),
            self._repository("example_repo_archive", "north"),
        ]
        mock = MagicMock(
            side_effect=[
                {"repositoriesOrError": self._connection(*repositories)},
                {
                    "repository1": {
                        "__typename": "RepositoryConnection",
                        "nodes": [
                            self._repository("example_repo", "south", "south_job")
                        ],
                    },
                    "repository0": {
                        "__typename": "RepositoryConnection",
                        "nodes": [
                            self._repository("example_repo", "north", "north_job")
                        ],
                    },
                },
            ]
        )
        monkeypatch.setattr(server, "gql", mock)

        result = list_jobs(env="staging", repository_name="example_repo")

        assert [job["job"] for job in result] == ["north_job", "south_job"]
        assert mock.call_count == 2
        assert [call.kwargs["env"] for call in mock.call_args_list] == [
            "staging",
            "staging",
        ]
        assert "jobs {" not in mock.call_args_list[0].args[0]
        query, variables = mock.call_args_list[1].args
        assert "repository0: repositoriesOrError" in query
        assert "repository1: repositoriesOrError" in query
        assert "$repositorySelector0: RepositorySelector!" in query
        assert "$repositorySelector1: RepositorySelector!" in query
        self._assert_union_fields(query)
        self._assert_union_fields(mock.call_args_list[0].args[0])
        assert variables == {
            "repositorySelector0": {
                "repositoryName": "example_repo",
                "repositoryLocationName": "north",
            },
            "repositorySelector1": {
                "repositoryName": "example_repo",
                "repositoryLocationName": "south",
            },
        }

    def test_location_filter_batches_each_exact_match(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repositories = [
            self._repository("alpha", "example_location"),
            self._repository("beta", "example_location"),
            self._repository("gamma", "example_location_archive"),
        ]
        mock = MagicMock(
            side_effect=[
                {"repositoriesOrError": self._connection(*repositories)},
                {
                    "repository0": {
                        "__typename": "RepositoryConnection",
                        "nodes": [
                            self._repository("alpha", "example_location", "alpha_job")
                        ],
                    },
                    "repository1": {
                        "__typename": "RepositoryConnection",
                        "nodes": [
                            self._repository("beta", "example_location", "beta_job")
                        ],
                    },
                },
            ]
        )
        monkeypatch.setattr(server, "gql", mock)

        result = list_jobs(location_name="example_location")

        assert [job["job"] for job in result] == ["alpha_job", "beta_job"]
        assert mock.call_count == 2
        variables = mock.call_args_list[1].args[1]
        assert variables == {
            "repositorySelector0": {
                "repositoryName": "alpha",
                "repositoryLocationName": "example_location",
            },
            "repositorySelector1": {
                "repositoryName": "beta",
                "repositoryLocationName": "example_location",
            },
        }

    def test_combined_filters_use_one_server_side_selector(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = MagicMock(
            return_value={
                "repository0": {
                    "__typename": "RepositoryConnection",
                    "nodes": [
                        self._repository(
                            "example_repo", "example_location", "selected_job"
                        )
                    ],
                }
            }
        )
        monkeypatch.setattr(server, "gql", mock)

        result = list_jobs(
            repository_name="example_repo",
            location_name="example_location",
        )

        assert [job["job"] for job in result] == ["selected_job"]
        query, variables = mock.call_args.args
        assert "repository0: repositoriesOrError" in query
        assert variables == {
            "repositorySelector0": {
                "repositoryName": "example_repo",
                "repositoryLocationName": "example_location",
            }
        }

    def test_combined_filters_with_no_match_return_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = MagicMock(
            return_value={
                "repository0": {
                    "__typename": "RepositoryNotFoundError",
                    "message": "Repository not found",
                }
            }
        )
        monkeypatch.setattr(server, "gql", mock)

        result = list_jobs(
            repository_name="missing_repo",
            location_name="missing_location",
        )

        assert result == []
        mock.assert_called_once()

    @pytest.mark.parametrize(
        "kwargs, context",
        [
            pytest.param({}, "listing jobs", id="unfiltered"),
            pytest.param(
                {"repository_name": "example_repo"},
                "discovering repositories for list_jobs with repository 'example_repo'",
                id="one-filter-discovery",
            ),
        ],
    )
    def test_python_error_during_listing_or_discovery_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kwargs: dict[str, str],
        context: str,
    ) -> None:
        mock = MagicMock(
            return_value={
                "repositoriesOrError": {
                    "__typename": "PythonError",
                    "message": "User code failed to load",
                }
            }
        )
        monkeypatch.setattr(server, "gql", mock)

        with pytest.raises(
            RuntimeError,
            match=f"Dagster failed while {context}: User code failed to load",
        ):
            list_jobs(**kwargs)

    def test_python_error_for_one_of_multiple_selectors_does_not_return_partial_jobs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repositories = [
            self._repository("example_repo", "north"),
            self._repository("example_repo", "south"),
        ]
        mock = MagicMock(
            side_effect=[
                {"repositoriesOrError": self._connection(*repositories)},
                {
                    "repository0": {
                        "__typename": "RepositoryConnection",
                        "nodes": [
                            self._repository("example_repo", "north", "north_job")
                        ],
                    },
                    "repository1": {
                        "__typename": "PythonError",
                        "message": "South location import failed",
                    },
                },
            ]
        )
        monkeypatch.setattr(server, "gql", mock)

        with pytest.raises(
            RuntimeError,
            match=(
                "Dagster failed while loading repository 'example_repo' at location "
                "'south': South location import failed"
            ),
        ):
            list_jobs(repository_name="example_repo")

    @pytest.mark.parametrize(
        "response, expected",
        [
            pytest.param(
                None,
                "Malformed Dagster repositoriesOrError response",
                id="non-mapping",
            ),
            pytest.param(
                {"__typename": "RepositoryConnection"},
                "Malformed Dagster RepositoryConnection",
                id="connection-without-nodes",
            ),
            pytest.param(
                {"__typename": "FutureRepositoryError"},
                "Unexpected Dagster repositoriesOrError typename 'FutureRepositoryError'",
                id="unknown-typename",
            ),
        ],
    )
    def test_unexpected_repository_union_response_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        response: object,
        expected: str,
    ) -> None:
        monkeypatch.setattr(
            server,
            "gql",
            MagicMock(return_value={"repository0": response}),
        )

        with pytest.raises(RuntimeError, match=expected):
            list_jobs(repository_name="repo", location_name="location")

    def test_single_filter_with_no_matches_does_not_fetch_jobs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock = MagicMock(
            return_value={
                "repositoriesOrError": self._connection(
                    self._repository("available_repo", "example_location")
                )
            }
        )
        monkeypatch.setattr(server, "gql", mock)

        assert list_jobs(repository_name="missing_repo") == []
        mock.assert_called_once()

    def test_empty_description_is_normalized_to_empty_string(self, mock_gql) -> None:
        repository = self._repository("repo", "location")
        repository["jobs"] = [{"name": "job", "description": None}]
        mock_gql({"data": {"repositoriesOrError": self._connection(repository)}})

        assert list_jobs()[0]["description"] == ""

    def test_fastmcp_publishes_exact_output_schema(self) -> None:
        tools = asyncio.run(server.mcp.list_tools())
        tool = next(tool for tool in tools if tool.name == "list_jobs")
        result_schema = tool.output_schema["properties"]["result"]

        assert result_schema["type"] == "array"
        assert result_schema["items"]["properties"] == {
            "repository": {"type": "string"},
            "location": {"type": "string"},
            "job": {"type": "string"},
            "description": {"type": "string"},
        }
        assert result_schema["items"]["required"] == [
            "repository",
            "location",
            "job",
            "description",
        ]


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
    # get_tick_history makes two gql calls: first resolves the instigator's repo +
    # location (repositoriesOrError), then fetches ticks (instigationStateOrError).
    # mock_gql reuses one response for both, so combine both keys in a single dict.
    @staticmethod
    def _locate(name, field):
        return {"nodes": [
            {"name": "repo", "location": {"name": "loc"},
             "schedules": [{"name": name}] if field == "schedules" else [],
             "sensors": [{"name": name}] if field == "sensors" else []},
        ]}

    def test_schedule_ticks(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": self._locate("daily_sched", "schedules"),
            "instigationStateOrError": {"__typename": "InstigationState", "ticks": [
                {"tickId": "t1", "status": "SUCCESS", "timestamp": "1000",
                 "error": None, "runIds": ["r1"]},
                {"tickId": "t2", "status": "SKIPPED", "timestamp": "900",
                 "error": None, "runIds": []},
            ]},
        }})
        result = get_tick_history("daily_sched", "SCHEDULE", limit=10)
        assert result["name"] == "daily_sched"
        assert len(result["ticks"]) == 2
        assert result["ticks"][0]["status"] == "SUCCESS"
        assert result["ticks"][0]["run_ids"] == ["r1"]

    def test_sensor_ticks_with_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": self._locate("my_sensor", "sensors"),
            "instigationStateOrError": {"__typename": "InstigationState", "ticks": [
                {"tickId": "t1", "status": "FAILURE", "timestamp": "1000",
                 "error": {"message": "Connection refused"}, "runIds": []},
            ]},
        }})
        result = get_tick_history("my_sensor", "sensor")
        assert result["ticks"][0]["error"] == "Connection refused"

    def test_not_found(self, mock_gql):
        mock_gql({"data": {"repositoriesOrError": {"nodes": [
            {"name": "repo", "location": {"name": "loc"},
             "schedules": [], "sensors": [{"name": "other_sensor"}]},
        ]}}})
        result = get_tick_history("missing_sensor", "SENSOR")
        assert "not found" in result["message"]

    def test_invalid_type(self, mock_gql):
        with pytest.raises(ValueError, match="must be 'SCHEDULE' or 'SENSOR'"):
            get_tick_history("x", "INVALID")

    def test_python_error(self, mock_gql):
        mock_gql({"data": {
            "repositoriesOrError": self._locate("x", "schedules"),
            "instigationStateOrError": {"__typename": "PythonError", "message": "Something broke"},
        }})
        result = get_tick_history("x", "SCHEDULE")
        assert result["message"] == "Something broke"

    def test_instigation_state_not_found(self, mock_gql):
        # Selector resolves locally but the backend has no state for it yet.
        mock_gql({"data": {
            "repositoriesOrError": self._locate("x", "schedules"),
            "instigationStateOrError": {
                "__typename": "InstigationStateNotFoundError",
                "message": "No instigation state found",
            },
        }})
        result = get_tick_history("x", "SCHEDULE")
        assert result["message"] == "No instigation state found"
