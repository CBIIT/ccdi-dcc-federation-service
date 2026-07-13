"""
Contract test: File endpoint injection guard.

Verifies that the file endpoint correctly rejects hostile metadata.unharmonized
filters with HTTP 400 (not 200-empty, not 500), and that the error envelope
does not echo the payload back.
"""

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

FILE_PATH = "/api/v1/file"


def test_hostile_unharmonized_field_returns_400_not_empty_200():
    """Test that a hostile SQL payload in unharmonized filter returns 400, not 200-empty."""
    payload = "guid) RETURN sf UNION MATCH (n) DETACH DELETE n //"
    resp = client.get(FILE_PATH, params={f"metadata.unharmonized.{payload}": "x"})

    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    body = resp.json()
    assert "errors" in body, f"Expected 'errors' field in response, got: {body}"

    # Verify the payload is never leaked back in the response
    assert payload not in resp.text, f"Payload leaked in response: {resp.text}"


def test_wellformed_unallowlisted_unharmonized_field_returns_400():
    """A well-formed but unallowlisted unharmonized field returns 400 (file allows only file_name)."""
    resp = client.get(FILE_PATH, params={"metadata.unharmonized.batch_id": "x"})

    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    body = resp.json()
    assert "errors" in body, f"Expected 'errors' field in response, got: {body}"
    assert len(body["errors"]) > 0, f"Expected at least one error, got: {body}"

    # Verify error kind is UnsupportedField or InvalidParameters
    error_kind = body["errors"][0].get("kind")
    assert error_kind in ["UnsupportedField", "InvalidParameters"], \
        f"Expected UnsupportedField or InvalidParameters, got: {error_kind}"


def test_allowlisted_file_name_filter_is_accepted():
    """The advertised metadata.unharmonized.file_name filter is allowlisted, so it is NOT
    rejected as 400 — it flows through to the query (200 with data or empty)."""
    resp = client.get(FILE_PATH, params={"metadata.unharmonized.file_name": "UTYE.fastq"})

    assert resp.status_code != 400, \
        f"Allowlisted file_name filter should not be rejected, got 400: {resp.text}"
