import json
from unittest.mock import MagicMock

import httpx
import pytest

from dagster_mcp import server as _server_mod


@pytest.fixture(autouse=True)
def env_defaults(monkeypatch):
    """Set clean env vars before every test (module globals are already evaluated,
    so these only matter for code that reads os.environ at call time)."""
    monkeypatch.setenv("DAGSTER_URL", "http://test-dagster:3000")
    monkeypatch.delenv("DAGSTER_API_TOKEN", raising=False)
    monkeypatch.delenv("DAGSTER_EXTRA_HEADERS", raising=False)
    monkeypatch.setenv("DAGSTER_READ_ONLY", "true")
    # Clear introspection cache between tests
    _server_mod._runs_filter_job_field.clear()


@pytest.fixture
def mock_gql(monkeypatch):
    """Returns a helper that patches httpx.post to return a given JSON response.

    Usage:
        mock_post = mock_gql({"data": {"runsOrError": {"results": []}}})
        result = get_runs()
        mock_post.assert_called_once()
    """
    def _setup(response_data, status_code=200):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = response_data
        mock_response.text = json.dumps(response_data)
        mock_post = MagicMock(return_value=mock_response)
        monkeypatch.setattr(httpx, "post", mock_post)
        return mock_post

    return _setup
