"""Lightweight Dagster asset-selection parsing and graph evaluation.

This module intentionally implements only the lineage-focused subset exposed by
``resolve_asset_selection``. It has no Dagster runtime dependency; selections
are evaluated against AssetNode dictionaries returned by Dagster GraphQL.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import TypeAlias


class AssetSelectionSyntaxError(ValueError):
    """Raised when an asset-selection expression cannot be parsed."""


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    position: int


@dataclass(frozen=True)
class _Predicate:
    field: str
    value: str
    tag_value: str | None = None


@dataclass(frozen=True)
class _Not:
    child: "_Expression"


@dataclass(frozen=True)
class _Binary:
    operator: str
    left: "_Expression"
    right: "_Expression"


@dataclass(frozen=True)
class _Function:
    name: str
    child: "_Expression"


@dataclass(frozen=True)
class _Traversal:
    child: "_Expression"
    upstream_depth: int | None = None
    downstream_depth: int | None = None


_Expression: TypeAlias = _Predicate | _Not | _Binary | _Function | _Traversal

_SUPPORTED_ATTRIBUTES = {"key", "group", "tag", "kind", "owner"}
# ``None`` means no traversal on that side, so use a sentinel for unlimited depth.
_UNBOUNDED = -1


def _syntax_error(query: str, position: int, message: str) -> AssetSelectionSyntaxError:
    pointer = " " * max(position, 0) + "^"
    return AssetSelectionSyntaxError(
        f"Invalid asset selection at position {position}: {message}\n{query}\n{pointer}"
    )


def _tokenize(query: str) -> list[_Token]:
    tokens: list[_Token] = []
    index = 0
    punctuation = {
        "(": "LPAREN",
        ")": "RPAREN",
        ":": "COLON",
        "=": "EQUAL",
        "+": "PLUS",
    }

    while index < len(query):
        char = query[index]
        if char.isspace():
            index += 1
            continue
        if char in punctuation:
            tokens.append(_Token(punctuation[char], char, index))
            index += 1
            continue
        if char in ('"', "'"):
            quote = char
            start = index
            index += 1
            value: list[str] = []
            while index < len(query):
                char = query[index]
                if char == "\\":
                    index += 1
                    if index >= len(query):
                        raise _syntax_error(query, start, "unterminated escape in quoted value")
                    value.append(query[index])
                    index += 1
                    continue
                if char == quote:
                    index += 1
                    break
                value.append(char)
                index += 1
            else:
                raise _syntax_error(query, start, "unterminated quoted value")
            tokens.append(_Token("VALUE", "".join(value), start))
            continue

        start = index
        while (
            index < len(query)
            and not query[index].isspace()
            and query[index] not in punctuation
            and query[index] not in ('"', "'")
        ):
            index += 1
        if start == index:
            raise _syntax_error(query, index, f"unexpected character {query[index]!r}")
        tokens.append(_Token("WORD", query[start:index], start))

    tokens.append(_Token("EOF", "", len(query)))
    return tokens


class _Parser:
    def __init__(self, query: str):
        self.query = query
        self.tokens = _tokenize(query)
        self.index = 0

    @property
    def current(self) -> _Token:
        return self.tokens[self.index]

    def _peek(self, offset: int = 1) -> _Token:
        return self.tokens[min(self.index + offset, len(self.tokens) - 1)]

    def _advance(self) -> _Token:
        token = self.current
        self.index += 1
        return token

    def _accept(self, kind: str) -> _Token | None:
        if self.current.kind == kind:
            return self._advance()
        return None

    def _expect(self, kind: str, description: str) -> _Token:
        token = self._accept(kind)
        if token is None:
            raise _syntax_error(self.query, self.current.position, f"expected {description}")
        return token

    def _is_keyword(self, keyword: str) -> bool:
        return self.current.kind == "WORD" and self.current.value.lower() == keyword

    def parse(self) -> _Expression:
        if self.current.kind == "EOF":
            raise _syntax_error(self.query, 0, "selection cannot be empty")
        expression = self._parse_or()
        if self.current.kind != "EOF":
            raise _syntax_error(
                self.query,
                self.current.position,
                f"unexpected token {self.current.value!r}",
            )
        return expression

    def _parse_or(self) -> _Expression:
        expression = self._parse_and()
        while self._is_keyword("or"):
            self._advance()
            expression = _Binary("or", expression, self._parse_and())
        return expression

    def _parse_and(self) -> _Expression:
        expression = self._parse_not()
        while self._is_keyword("and"):
            self._advance()
            expression = _Binary("and", expression, self._parse_not())
        return expression

    def _parse_not(self) -> _Expression:
        if self._is_keyword("not"):
            self._advance()
            return _Not(self._parse_not())
        return self._parse_traversal()

    def _parse_traversal(self) -> _Expression:
        upstream_depth: int | None = None
        if self._accept("PLUS"):
            upstream_depth = _UNBOUNDED
        elif (
            self.current.kind == "WORD"
            and self.current.value.isdigit()
            and self._peek().kind == "PLUS"
        ):
            upstream_depth = int(self._advance().value)
            self._advance()

        expression = self._parse_primary()

        downstream_depth: int | None = None
        if self._accept("PLUS"):
            if self.current.kind == "WORD" and self.current.value.isdigit():
                downstream_depth = int(self._advance().value)
            else:
                downstream_depth = _UNBOUNDED

        if upstream_depth is not None or downstream_depth is not None:
            return _Traversal(expression, upstream_depth, downstream_depth)
        return expression

    def _parse_primary(self) -> _Expression:
        if self._accept("LPAREN"):
            expression = self._parse_or()
            self._expect("RPAREN", "')'")
            return expression

        if (
            self.current.kind == "WORD"
            and self.current.value.lower() in {"roots", "sinks"}
            and self._peek().kind == "LPAREN"
        ):
            name = self._advance().value.lower()
            self._advance()
            expression = self._parse_or()
            self._expect("RPAREN", "')'")
            return _Function(name, expression)

        token = self.current
        if token.kind not in {"WORD", "VALUE"}:
            raise _syntax_error(self.query, token.position, "expected an asset predicate")
        self._advance()

        if self._accept("COLON"):
            field = token.value.lower()
            if field not in _SUPPORTED_ATTRIBUTES:
                supported = ", ".join(sorted(_SUPPORTED_ATTRIBUTES))
                raise _syntax_error(
                    self.query,
                    token.position,
                    f"unsupported attribute {token.value!r}; supported attributes: {supported}",
                )
            value = self._parse_value(f"a value after {field}:")
            tag_value = None
            if field == "tag" and self._accept("EQUAL"):
                tag_value = self._parse_value("a tag value after '='")
            elif self.current.kind == "EQUAL":
                raise _syntax_error(
                    self.query,
                    self.current.position,
                    "'=' is only valid in tag:key=value predicates",
                )
            return _Predicate(field, value, tag_value)

        if token.kind == "WORD" and token.value.lower() in {"and", "or", "not"}:
            raise _syntax_error(
                self.query,
                token.position,
                f"expected an asset key before {token.value!r}",
            )
        return _Predicate("key", token.value)

    def _parse_value(self, description: str) -> str:
        token = self.current
        if token.kind not in {"WORD", "VALUE"}:
            raise _syntax_error(self.query, token.position, f"expected {description}")
        self._advance()
        return token.value


def parse_asset_selection(query: str) -> _Expression:
    """Parse a lineage-core Dagster asset-selection expression."""

    if not isinstance(query, str):
        raise TypeError("asset_selection must be a string")
    return _Parser(query).parse()


def _asset_key(node: dict) -> str:
    return "/".join(node.get("assetKey", {}).get("path", []))


def _wildcard_matches(pattern: str, value: str) -> bool:
    regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
    return re.match(regex, value) is not None


class _GraphEvaluator:
    def __init__(self, nodes: list[dict]):
        self.nodes_by_key = {_asset_key(node): node for node in nodes}
        self.universe = set(self.nodes_by_key)
        self.upstream: dict[str, set[str]] = {}
        self.downstream: dict[str, set[str]] = {key: set() for key in self.universe}

        for key, node in self.nodes_by_key.items():
            parents = {
                "/".join(parent.get("path", []))
                for parent in node.get("dependencyKeys", [])
                if "/".join(parent.get("path", [])) in self.universe
            }
            self.upstream[key] = parents
            for parent in parents:
                self.downstream[parent].add(key)

    def evaluate(self, expression: _Expression) -> set[str]:
        if isinstance(expression, _Predicate):
            return self._evaluate_predicate(expression)
        if isinstance(expression, _Not):
            return self.universe - self.evaluate(expression.child)
        if isinstance(expression, _Binary):
            left = self.evaluate(expression.left)
            right = self.evaluate(expression.right)
            return left & right if expression.operator == "and" else left | right
        if isinstance(expression, _Function):
            selected = self.evaluate(expression.child)
            graph = self.upstream if expression.name == "roots" else self.downstream
            return {key for key in selected if not (graph.get(key, set()) & selected)}
        if isinstance(expression, _Traversal):
            selected = self.evaluate(expression.child)
            result = set(selected)
            if expression.upstream_depth is not None:
                result |= self._traverse(selected, self.upstream, expression.upstream_depth)
            if expression.downstream_depth is not None:
                result |= self._traverse(selected, self.downstream, expression.downstream_depth)
            return result
        raise AssertionError(f"Unknown expression type: {type(expression)}")

    def _evaluate_predicate(self, predicate: _Predicate) -> set[str]:
        if predicate.field == "key":
            return {key for key in self.universe if _wildcard_matches(predicate.value, key)}

        selected: set[str] = set()
        for key, node in self.nodes_by_key.items():
            if predicate.field == "group":
                if _wildcard_matches(predicate.value, node.get("groupName") or ""):
                    selected.add(key)
            elif predicate.field == "kind":
                kinds = node.get("kinds") or []
                if (
                    predicate.value == "<null>"
                    and not kinds
                    or predicate.value in kinds
                ):
                    selected.add(key)
            elif predicate.field == "owner":
                owners = self._owner_values(node)
                if (
                    predicate.value == "<null>"
                    and not owners
                    or predicate.value in owners
                ):
                    selected.add(key)
            elif predicate.field == "tag":
                if self._tag_matches(node, predicate.value, predicate.tag_value):
                    selected.add(key)
        return selected

    @staticmethod
    def _owner_values(node: dict) -> set[str]:
        values: set[str] = set()
        for owner in node.get("owners") or []:
            if isinstance(owner, str):
                values.add(owner)
                continue
            if not isinstance(owner, dict):
                # Defensive: GraphQL should only return strings or owner objects.
                continue
            if owner.get("__typename") == "TeamAssetOwner" and owner.get("team") is not None:
                # GraphQL returns the bare team name; selection syntax uses ``team:<name>``.
                values.add(f"team:{owner['team']}")
            elif owner.get("email") is not None:
                values.add(owner["email"])
        return values

    @staticmethod
    def _tag_matches(node: dict, key: str, value: str | None) -> bool:
        # In Dagster, ``tag:key`` is shorthand for ``tag:key=""``, not any value.
        expected_value = "" if value is None else value
        for tag in node.get("tags") or []:
            if tag.get("key") == key and tag.get("value") == expected_value:
                return True
        return False

    @staticmethod
    def _traverse(
        selected: set[str],
        graph: dict[str, set[str]],
        depth: int,
    ) -> set[str]:
        result = set(selected)
        frontier = set(selected)
        levels = 0
        while frontier and (depth == _UNBOUNDED or levels < depth):
            next_frontier = set().union(*(graph.get(key, set()) for key in frontier)) - result
            if not next_frontier:
                break
            result |= next_frontier
            frontier = next_frontier
            levels += 1
        return result


def evaluate_asset_selection(nodes: list[dict], expression: _Expression) -> list[dict]:
    """Resolve an already-parsed selection against GraphQL AssetNode dictionaries."""

    evaluator = _GraphEvaluator(nodes)
    selected_keys = evaluator.evaluate(expression)
    return [evaluator.nodes_by_key[key] for key in sorted(selected_keys)]


def resolve_asset_selection_nodes(nodes: list[dict], query: str) -> list[dict]:
    """Parse ``query`` and resolve it against GraphQL AssetNode dictionaries."""

    return evaluate_asset_selection(nodes, parse_asset_selection(query))
