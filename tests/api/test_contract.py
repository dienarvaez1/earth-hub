"""Contract tests: Schemathesis generates requests from the OpenAPI schema and checks that
responses never 500, match their documented schemas, and use documented status codes."""

import os

import pytest
import schemathesis
from hypothesis import HealthCheck, settings
from schemathesis.specs.openapi.checks import positive_data_acceptance

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def api_schema(client):
    return schemathesis.openapi.from_asgi("/openapi.json", client.app)


schema = schemathesis.pytest.from_fixture("api_schema")


@schema.parametrize()
@settings(
    # 60 keeps the suite fast; set CONTRACT_EXAMPLES=1000 for a deeper run.
    max_examples=int(os.environ.get("CONTRACT_EXAMPLES", "60")),
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_api_conforms_to_its_openapi_schema(case):
    # positive_data_acceptance is excluded on purpose: some rules compare two parameters
    # (min_magnitude <= max_magnitude, start_time < end_time, hourly range <= 31 days),
    # which OpenAPI cannot express, so Schemathesis sends schema-valid requests that
    # must be rejected. Those rules have their own tests in tests/unit/test_api_validation.py.
    case.call_and_validate(excluded_checks=[positive_data_acceptance])
