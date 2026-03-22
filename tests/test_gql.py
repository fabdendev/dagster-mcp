import pytest
from unittest.mock import MagicMock

import httpx

from dagster_mcp.server import gql


class TestGql:
    def test_successful_query(self, mock_gql):
        mock_gql({"data": {"runsOrError": {"results": []}}})
        result = gql("query { runsOrError { results { runId } } }")
        assert result == {"runsOrError": {"results": []}}

    def test_connect_error(self, monkeypatch):
        monkeypatch.setattr(
            httpx, "post", MagicMock(side_effect=httpx.ConnectError("refused"))
        )
        with pytest.raises(RuntimeError, match="Cannot connect to Dagster"):
            gql("query { }")

    def test_timeout_error(self, monkeypatch):
        monkeypatch.setattr(
            httpx, "post", MagicMock(side_effect=httpx.TimeoutException("timed out"))
        )
        with pytest.raises(RuntimeError, match="timed out after 30s"):
            gql("query { }")

    def test_http_error(self, mock_gql):
        mock_gql({"error": "Internal Server Error"}, status_code=500)
        with pytest.raises(RuntimeError, match="HTTP 500"):
            gql("query { }")

    def test_graphql_errors(self, mock_gql):
        mock_gql({
            "errors": [{"message": "Field 'foo' not found"}],
            "data": None,
        })
        with pytest.raises(RuntimeError, match="GraphQL error.*Field 'foo' not found"):
            gql("query { foo }")

    def test_passes_variables(self, mock_gql):
        mock_post = mock_gql({"data": {"result": "ok"}})
        gql("query Q($id: ID!) { result }", {"id": "123"})
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["variables"] == {"id": "123"}

    def test_empty_variables_default(self, mock_gql):
        mock_post = mock_gql({"data": {"result": "ok"}})
        gql("query { result }")
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["variables"] == {}
