import json
from unittest.mock import MagicMock

import httpx
import pytest

from dagster_mcp import server as _server_mod
from tests import graphql_schema


@pytest.fixture(autouse=True)
def graphql_documents_match_dagster_schema(monkeypatch):
    """Fail any test whose tool sends a document Dagster's schema would reject.

    Wraps ``server.gql`` rather than ``httpx.post``, because tests replace
    ``httpx.post`` with their own mocks. Tests that replace ``server.gql`` itself
    send nothing and are not checked. Errors are collected and reported at
    teardown because some tools catch exceptions raised during a call.
    """
    if not graphql_schema.AVAILABLE:
        yield
        return

    invalid: list[str] = []
    real_gql = _server_mod.gql

    def checked_gql(query, *args, **kwargs):
        invalid.extend(graphql_schema.schema_errors(query))
        return real_gql(query, *args, **kwargs)

    monkeypatch.setattr(_server_mod, "gql", checked_gql)
    yield
    if invalid:
        pytest.fail(
            "GraphQL document rejected by Dagster's schema:\n"
            + "\n".join(dict.fromkeys(invalid))
        )


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
    _server_mod._type_fields.clear()


@pytest.fixture
def supported_asset_tool_schema(monkeypatch):
    """Skip schema introspection in tests focused on post-compatibility behavior."""
    monkeypatch.setattr(
        _server_mod,
        "_dagster_19_compatibility_error",
        lambda tool_name, required_fields, env=None: None,
    )


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
