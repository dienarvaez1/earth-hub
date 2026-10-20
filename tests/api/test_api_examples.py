"""The requests in docs/api.md, replayed on the documented dataset: each must return exactly
the status, headers and body the document shows."""

import pytest
from support.doc_examples import api_examples

pytestmark = pytest.mark.integration

EXAMPLES = api_examples()


def test_the_document_has_examples_of_every_endpoint(documented_client):
    documented = {e.path.split("?")[0] for e in EXAMPLES}
    paths = documented_client.get("/openapi.json").json()["paths"]
    assert {path for path in paths if "{" not in path} <= documented
    assert any(e.path.startswith("/api/v1/earthquakes/") and e.status == 200 for e in EXAMPLES)
    assert {422, 404, 405} <= {e.status for e in EXAMPLES}


@pytest.mark.req("REQ-DOC-002")
@pytest.mark.parametrize("example", EXAMPLES, ids=lambda e: f"line {e.line}")
def test_each_request_returns_the_documented_response(documented_client, example):
    response = documented_client.request(example.method, example.path)
    assert response.status_code == example.status
    for name, value in example.headers.items():
        assert response.headers[name] == value
    assert response.json() == example.body
