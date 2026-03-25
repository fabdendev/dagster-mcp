"""Tests for multi-environment support (DAGSTER_ENVS)."""

import json
import pytest
from unittest.mock import MagicMock

import httpx

from dagster_mcp import server
from dagster_mcp.server import gql, get_runs, list_jobs, get_instance_status


_TWO_ENVS = json.dumps({
    "prod": {"url": "https://prod.dagster.io", "token": "prod-token"},
    "dev": {"url": "http://dev.dagster.io:3000"},
})

_ONE_ENV = json.dumps({
    "staging": {"url": "https://stg.dagster.io", "token": "stg-token"},
})


class TestParseEnvs:
    def test_empty_string_returns_empty_dict(self):
        from dagster_mcp.server import _parse_dagster_envs
        assert _parse_dagster_envs("") == {}

    def test_valid_json_parsed(self):
        from dagster_mcp.server import _parse_dagster_envs
        result = _parse_dagster_envs(_TWO_ENVS)
        assert "prod" in result
        assert result["prod"]["url"] == "https://prod.dagster.io"

    def test_invalid_json_raises(self):
        from dagster_mcp.server import _parse_dagster_envs
        with pytest.raises(RuntimeError, match="valid JSON object"):
            _parse_dagster_envs("{not json}")

    def test_non_dict_raises(self):
        from dagster_mcp.server import _parse_dagster_envs
        with pytest.raises(RuntimeError, match="JSON object"):
            _parse_dagster_envs('["prod", "dev"]')


class TestResolveConnection:
    def test_single_env_mode_uses_module_globals(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", {})
        monkeypatch.setattr(server, "DAGSTER_URL", "http://single:3000")
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "tok")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", "")
        url, token, extra = server._resolve_connection(None)
        assert url == "http://single:3000/graphql"
        assert token == "tok"
        assert extra == ""

    def test_multi_env_resolves_named_env(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "")
        url, token, extra = server._resolve_connection("prod")
        assert url == "https://prod.dagster.io/graphql"
        assert token == "prod-token"

    def test_multi_env_uses_default_env(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "dev")
        url, token, _ = server._resolve_connection(None)
        assert url == "http://dev.dagster.io:3000/graphql"
        assert token == ""

    def test_multi_env_single_entry_auto_selected(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_ONE_ENV))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "")
        url, token, _ = server._resolve_connection(None)
        assert url == "https://stg.dagster.io/graphql"
        assert token == "stg-token"

    def test_multi_env_no_env_no_default_raises(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "")
        with pytest.raises(RuntimeError, match="no env specified"):
            server._resolve_connection(None)

    def test_multi_env_unknown_env_raises(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        with pytest.raises(RuntimeError, match="Unknown Dagster env 'nope'"):
            server._resolve_connection("nope")

    def test_trailing_slash_stripped(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", {
            "x": {"url": "https://example.com/dagster/"}
        })
        url, _, _ = server._resolve_connection("x")
        assert url == "https://example.com/dagster/graphql"


class TestGqlMultiEnv:
    def _make_mock(self, monkeypatch, data):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": data}
        mock_response.text = json.dumps({"data": data})
        mock_post = MagicMock(return_value=mock_response)
        monkeypatch.setattr(httpx, "post", mock_post)
        return mock_post

    def test_gql_routes_to_correct_url_per_env(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "")
        mock_post = self._make_mock(monkeypatch, {"result": "ok"})

        gql("query { result }", env="prod")

        call_url = mock_post.call_args.args[0]
        assert call_url == "https://prod.dagster.io/graphql"

    def test_gql_sends_correct_token_per_env(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        monkeypatch.setattr(server, "_DAGSTER_DEFAULT_ENV", "prod")
        mock_post = self._make_mock(monkeypatch, {"result": "ok"})

        gql("query { result }", env="prod")

        headers = mock_post.call_args.kwargs["headers"]
        assert headers.get("Dagster-Cloud-Api-Token") == "prod-token"

    def test_gql_dev_env_no_token(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        mock_post = self._make_mock(monkeypatch, {"result": "ok"})

        gql("query { result }", env="dev")

        headers = mock_post.call_args.kwargs["headers"]
        assert "Dagster-Cloud-Api-Token" not in headers


class TestToolsWithEnvParam:
    def _mock_post(self, monkeypatch, data):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": data}
        mock_response.text = json.dumps({"data": data})
        mock_post = MagicMock(return_value=mock_response)
        monkeypatch.setattr(httpx, "post", mock_post)
        return mock_post

    def test_get_runs_passes_env_to_gql(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        mock_post = self._mock_post(monkeypatch, {"runsOrError": {"results": []}})

        get_runs(env="prod")

        call_url = mock_post.call_args.args[0]
        assert "prod.dagster.io" in call_url

    def test_list_jobs_passes_env_to_gql(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        mock_post = self._mock_post(monkeypatch, {"repositoriesOrError": {"nodes": []}})

        list_jobs(env="dev")

        call_url = mock_post.call_args.args[0]
        assert "dev.dagster.io" in call_url

    def test_get_instance_status_passes_env(self, monkeypatch):
        monkeypatch.setattr(server, "_ENVS", json.loads(_TWO_ENVS))
        mock_post = self._mock_post(monkeypatch, {
            "instance": {"daemonHealth": {"allDaemonStatuses": []}},
            "runsOrError": {"results": []},
            "workspaceOrError": {"locationEntries": []},
        })

        get_instance_status(env="prod")

        call_url = mock_post.call_args.args[0]
        assert "prod.dagster.io" in call_url

    def test_env_none_in_single_env_mode(self, monkeypatch):
        """env=None falls through to single-env mode without error."""
        monkeypatch.setattr(server, "_ENVS", {})
        monkeypatch.setattr(server, "DAGSTER_URL", "http://solo:3000")
        mock_post = self._mock_post(monkeypatch, {"runsOrError": {"results": []}})

        get_runs(env=None)

        call_url = mock_post.call_args.args[0]
        assert "solo:3000" in call_url
