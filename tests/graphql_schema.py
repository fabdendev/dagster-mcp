"""Validate GraphQL documents against Dagster's real schema, when it is installed.

The tool tests mock ``httpx.post``, so a document selecting a field Dagster does
not have passes every test and fails on every real call — issue #36 shipped
exactly that way. ``dagster-graphql`` is not a dev dependency because it pulls in
all of dagster; CI installs it with ``uv run --with``, and locally the check is
skipped unless it is present.
"""

import functools

try:
    from dagster_graphql.schema import create_schema
    from graphql import GraphQLError, parse, validate
except ImportError:
    create_schema = None

AVAILABLE = create_schema is not None


@functools.cache
def _schema():
    return create_schema().graphql_schema


@functools.cache
def schema_errors(document: str) -> tuple[str, ...]:
    """Return the validation errors for ``document``; empty when it is valid."""
    try:
        parsed = parse(document)
    except GraphQLError as exc:
        return (exc.message,)
    return tuple(error.message for error in validate(_schema(), parsed))
