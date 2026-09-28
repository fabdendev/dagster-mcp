from unittest.mock import MagicMock

import httpx
import pytest

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

    def test_auth_redirect_reports_destination_without_query_string(self, monkeypatch):
        response = httpx.Response(
            302,
            headers={"location": "https://auth.wellfound.com/login?state=secret"},
        )
        monkeypatch.setattr(httpx, "post", MagicMock(return_value=response))
        with pytest.raises(RuntimeError, match="HTTP 302 redirect to auth.wellfound.com") as exc:
            gql("query { }")
        assert "refresh the configured authentication" in str(exc.value)
        assert "secret" not in str(exc.value)

    def test_auth_redirect_reports_destination_without_userinfo(self, monkeypatch):
        response = httpx.Response(
            302,
            headers={"location": "https://svc:s3cret@auth.internal:8443/login"},
        )
        monkeypatch.setattr(httpx, "post", MagicMock(return_value=response))
        with pytest.raises(RuntimeError, match="HTTP 302 redirect to auth.internal:8443") as exc:
            gql("query { }")
        message = str(exc.value)
        assert "svc" not in message
        assert "s3cret" not in message
        assert "@" not in message

    def test_non_json_success_reports_content_type(self, monkeypatch):
        response = httpx.Response(
            200,
            headers={"content-type": "text/html"},
            text="<html>Sign in</html>",
        )
        monkeypatch.setattr(httpx, "post", MagicMock(return_value=response))
        with pytest.raises(RuntimeError, match="HTTP 200 with a non-JSON response.*text/html"):
            gql("query { }")

    def test_missing_graphql_data_reports_invalid_response(self, mock_gql):
        mock_gql({"message": "OK"})
        with pytest.raises(RuntimeError, match="missing data field"):
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
