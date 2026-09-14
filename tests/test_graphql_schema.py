import pytest

from tests.graphql_schema import AVAILABLE, schema_errors

pytestmark = pytest.mark.skipif(
    not AVAILABLE,
    reason="dagster-graphql is not installed; CI runs this with uv run --with",
)

_LAUNCH_SELECTING = """
mutation Launch($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on InvalidStepError { %s }
  }
}
"""


def test_validator_rejects_a_field_the_type_lacks():
    # The selection that shipped in 0.11.0 and 0.12.0 (issue #36).
    errors = schema_errors(_LAUNCH_SELECTING % "message")

    assert "Cannot query field 'message' on type 'InvalidStepError'." in errors


def test_validator_accepts_the_real_field():
    assert schema_errors(_LAUNCH_SELECTING % "invalidStepKey") == ()
