import json
from unittest.mock import MagicMock

import httpx
import pytest

from dagster_mcp import server as server_module
from dagster_mcp.asset_selection import (
    AssetSelectionSyntaxError,
    resolve_asset_selection_nodes,
)
from dagster_mcp.server import resolve_asset_selection


def _node(
    key,
    *,
    group="default",
    deps=None,
    tags=None,
    kinds=None,
    owners=None,
    materializable=True,
    executable=True,
    observable=False,
    partitioned=False,
):
    return {
        "assetKey": {"path": key.split("/")},
        "groupName": group,
        "dependencyKeys": [{"path": dep.split("/")} for dep in deps or []],
        "tags": [{"key": tag_key, "value": value} for tag_key, value in (tags or {}).items()],
        "kinds": kinds or [],
        "owners": owners or [],
        "jobNames": ["__ASSET_JOB"] if executable else [],
        "repository": {"name": "__repository__", "location": {"name": "loc"}},
        "isMaterializable": materializable,
        "isExecutable": executable,
        "isObservable": observable,
        "isPartitioned": partitioned,
    }


@pytest.fixture
def asset_nodes():
    return [
        _node(
            "warehouse/raw_orders",
            group="raw",
            tags={"priority": "high", "present": ""},
            kinds=["python"],
            owners=[{"__typename": "TeamAssetOwner", "team": "data"}],
        ),
        _node(
            "warehouse/clean_orders",
            group="analytics",
            deps=["warehouse/raw_orders"],
            tags={"priority": "low"},
            kinds=["dbt"],
            owners=[{"__typename": "UserAssetOwner", "email": "owner@example.com"}],
        ),
        _node(
            "warehouse/order_report",
            group="analytics",
            deps=["warehouse/clean_orders"],
            kinds=["python"],
        ),
        _node(
            "warehouse/external_orders",
            group="analytics",
            materializable=False,
            executable=False,
        ),
        _node("special/key", group="group with spaces", partitioned=True),
    ]


def _keys(nodes):
    return ["/".join(node["assetKey"]["path"]) for node in nodes]


class TestAssetSelectionParser:
    def test_key_bare_key_and_wildcard(self, asset_nodes):
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "warehouse/raw_orders")) == [
            "warehouse/raw_orders"
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "key:warehouse/*orders")) == [
            "warehouse/clean_orders",
            "warehouse/external_orders",
            "warehouse/raw_orders",
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "*")) == [
            "special/key",
            "warehouse/clean_orders",
            "warehouse/external_orders",
            "warehouse/order_report",
            "warehouse/raw_orders",
        ]

    def test_boolean_precedence_and_parentheses(self, asset_nodes):
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "group:raw or group:analytics and kind:dbt",
            )
        ) == ["warehouse/clean_orders", "warehouse/raw_orders"]
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "(group:raw or group:analytics) and kind:python",
            )
        ) == ["warehouse/order_report", "warehouse/raw_orders"]

    def test_case_insensitive_boolean_and_not(self, asset_nodes):
        selected = resolve_asset_selection_nodes(
            asset_nodes,
            "group:analytics AND NOT kind:dbt",
        )
        assert _keys(selected) == [
            "warehouse/external_orders",
            "warehouse/order_report",
        ]

    def test_tag_kind_owner_and_quoted_value(self, asset_nodes):
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "tag:present")) == [
            "warehouse/raw_orders"
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "tag:priority=high")) == [
            "warehouse/raw_orders"
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "tag:priority")) == []
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "kind:dbt")) == [
            "warehouse/clean_orders"
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, 'owner:"team:data"')) == [
            "warehouse/raw_orders"
        ]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, 'owner:"owner@example.com"')
        ) == ["warehouse/clean_orders"]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "owner:<null>")) == [
            "special/key",
            "warehouse/external_orders",
            "warehouse/order_report",
        ]
        assert _keys(resolve_asset_selection_nodes(asset_nodes, "kind:<null>")) == [
            "special/key",
            "warehouse/external_orders",
        ]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, 'group:"group with spaces"')
        ) == ["special/key"]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, 'tag:"priority"="high"')
        ) == ["warehouse/raw_orders"]

    def test_upstream_and_downstream_traversals(self, asset_nodes):
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, "+key:warehouse/order_report")
        ) == [
            "warehouse/clean_orders",
            "warehouse/order_report",
            "warehouse/raw_orders",
        ]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, "1+key:warehouse/order_report")
        ) == ["warehouse/clean_orders", "warehouse/order_report"]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, "key:warehouse/raw_orders+1")
        ) == ["warehouse/clean_orders", "warehouse/raw_orders"]
        assert _keys(
            resolve_asset_selection_nodes(asset_nodes, "key:warehouse/raw_orders+")
        ) == [
            "warehouse/clean_orders",
            "warehouse/order_report",
            "warehouse/raw_orders",
        ]
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "1+key:warehouse/clean_orders+1",
            )
        ) == [
            "warehouse/clean_orders",
            "warehouse/order_report",
            "warehouse/raw_orders",
        ]

    def test_roots_and_sinks(self, asset_nodes):
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "roots(+key:warehouse/order_report)",
            )
        ) == ["warehouse/raw_orders"]
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "sinks(+key:warehouse/order_report)",
            )
        ) == ["warehouse/order_report"]

    def test_stable_deduplicated_order(self, asset_nodes):
        assert _keys(
            resolve_asset_selection_nodes(
                asset_nodes,
                "key:warehouse/order_report or key:warehouse/*orders",
            )
        ) == [
            "warehouse/clean_orders",
            "warehouse/external_orders",
            "warehouse/order_report",
            "warehouse/raw_orders",
        ]

    @pytest.mark.parametrize(
        "selection",
        [
            "",
            "group:",
            "group:analytics and",
            "(group:analytics",
            "status:missing",
            'key:"unterminated',
        ],
    )
    def test_invalid_syntax_has_position(self, asset_nodes, selection):
        with pytest.raises(AssetSelectionSyntaxError, match="position"):
            resolve_asset_selection_nodes(asset_nodes, selection)

    def test_syntax_error_reports_zero_based_character_position(self, asset_nodes):
        with pytest.raises(
            AssetSelectionSyntaxError,
            match=r"position 6: expected a value after group:",
        ):
            resolve_asset_selection_nodes(asset_nodes, "group:")


@pytest.mark.usefixtures("supported_asset_tool_schema")
class TestResolveAssetSelectionTool:
    def test_returns_keys_and_summaries_without_filtering(self, mock_gql, asset_nodes):
        mock_post = mock_gql({"data": {"assetNodes": asset_nodes}})

        result = resolve_asset_selection("group:analytics")

        assert result["asset_keys"] == [
            "warehouse/clean_orders",
            "warehouse/external_orders",
            "warehouse/order_report",
        ]
        assert result["assets"][1]["isMaterializable"] is False
        assert result["assets"][0]["repository"]["location"]["name"] == "loc"
        assert "mutation" not in mock_post.call_args.kwargs["json"]["query"].lower()

    def test_syntax_error_is_structured(self, mock_gql, asset_nodes):
        mock_post = mock_gql({"data": {"assetNodes": asset_nodes}})

        result = resolve_asset_selection("group:")

        assert result["asset_keys"] == []
        assert result["assets"] == []
        assert "position" in result["message"]
        mock_post.assert_not_called()


def _mock_response(data):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = data
    response.text = json.dumps(data)
    return response


class TestResolveAssetSelectionCompatibility:
    def test_reports_unsupported_schema_without_fetching_asset_graph(
        self,
        monkeypatch,
    ):
        query_fields = server_module._RESOLVE_ASSET_SELECTION_SCHEMA["Query"]
        asset_fields = server_module._RESOLVE_ASSET_SELECTION_SCHEMA["AssetNode"] - {
            "kinds"
        }
        mock_post = MagicMock(
            side_effect=[
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [{"name": name} for name in query_fields]
                            }
                        }
                    }
                ),
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [{"name": name} for name in asset_fields]
                            }
                        }
                    }
                ),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = resolve_asset_selection("group:analytics")

        assert "requires Dagster 1.9+" in result["message"]
        assert "AssetNode.kinds" in result["message"]
        assert mock_post.call_count == 2
        assert all(
            "TypeFields" in call.kwargs["json"]["query"]
            for call in mock_post.call_args_list
        )

    def test_compatible_schema_proceeds_to_asset_graph(self, monkeypatch, asset_nodes):
        mock_post = MagicMock(
            side_effect=[
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [
                                    {"name": name}
                                    for name in server_module._RESOLVE_ASSET_SELECTION_SCHEMA[
                                        "Query"
                                    ]
                                ]
                            }
                        }
                    }
                ),
                _mock_response(
                    {
                        "data": {
                            "__type": {
                                "fields": [
                                    {"name": name}
                                    for name in server_module._RESOLVE_ASSET_SELECTION_SCHEMA[
                                        "AssetNode"
                                    ]
                                ]
                            }
                        }
                    }
                ),
                _mock_response({"data": {"assetNodes": asset_nodes}}),
            ]
        )
        monkeypatch.setattr(httpx, "post", mock_post)

        result = resolve_asset_selection("group:analytics")

        assert result["asset_keys"] == [
            "warehouse/clean_orders",
            "warehouse/external_orders",
            "warehouse/order_report",
        ]
        assert mock_post.call_count == 3
