import pytest

from dagster_mcp import server


class TestBuildHeaders:
    def test_no_token(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", "")
        headers = server._build_headers()
        assert headers == {}

    def test_with_token(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "my-token")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", "")
        headers = server._build_headers()
        assert headers == {"Dagster-Cloud-Api-Token": "my-token"}

    def test_extra_headers_valid(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "")
        monkeypatch.setattr(
            server, "DAGSTER_EXTRA_HEADERS",
            '{"Authorization": "Bearer abc", "X-Custom": "val"}'
        )
        headers = server._build_headers()
        assert headers == {"Authorization": "Bearer abc", "X-Custom": "val"}

    def test_extra_headers_merged_with_token(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "my-token")
        monkeypatch.setattr(
            server, "DAGSTER_EXTRA_HEADERS",
            '{"X-Custom": "val"}'
        )
        headers = server._build_headers()
        assert headers == {
            "Dagster-Cloud-Api-Token": "my-token",
            "X-Custom": "val",
        }

    def test_extra_headers_malformed_json(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", "{not valid json}")
        with pytest.raises(RuntimeError, match="valid JSON object"):
            server._build_headers()

    def test_extra_headers_not_a_dict(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", '["a", "b"]')
        with pytest.raises(RuntimeError, match="must be a JSON object"):
            server._build_headers()

    def test_extra_headers_non_string_values(self, monkeypatch):
        monkeypatch.setattr(server, "DAGSTER_API_TOKEN", "")
        monkeypatch.setattr(server, "DAGSTER_EXTRA_HEADERS", '{"key": 123}')
        with pytest.raises(RuntimeError, match="must be strings"):
            server._build_headers()
