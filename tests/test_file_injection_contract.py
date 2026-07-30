"""
Contract test: File endpoint injection guard.

Verifies that the file endpoint correctly rejects hostile metadata.unharmonized
filters with HTTP 400 (not 200-empty, not 500), and that the error envelope
does not echo the payload back.

Uses a stubbed DB session so CI (no Memgraph) still exercises the allowlist /
injection guards before any query runs.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.main import setup_exception_handlers, setup_routers
from app.api.v1 import deps as api_deps
from app.core.config import Settings
from app.lib.field_allowlist import get_field_allowlist

FILE_PATH = "/api/v1/file"


class _DummyResult:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def __aiter__(self):
        async def _gen():
            for r in self._rows:
                yield r

        return _gen()

    async def consume(self):
        return None


class _DummySession:
    async def run(self, *args, **kwargs):
        return _DummyResult([])


def _client() -> TestClient:
    """App with stubbed session + real FieldAllowlist (file_name only)."""
    app = FastAPI()
    setup_exception_handlers(app)
    setup_routers(app)

    async def _fake_db_session():
        yield _DummySession()

    app.dependency_overrides[api_deps.get_database_session] = _fake_db_session
    app.dependency_overrides[api_deps.get_app_settings] = lambda: Settings()
    app.dependency_overrides[api_deps.get_allowlist] = get_field_allowlist
    app.dependency_overrides[api_deps.check_rate_limit] = lambda: None
    return TestClient(app)


def test_hostile_unharmonized_field_returns_400_not_empty_200():
    """Test that a hostile SQL payload in unharmonized filter returns 400, not 200-empty."""
    client = _client()
    payload = "guid) RETURN sf UNION MATCH (n) DETACH DELETE n //"
    resp = client.get(FILE_PATH, params={f"metadata.unharmonized.{payload}": "x"})

    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    body = resp.json()
    assert "errors" in body, f"Expected 'errors' field in response, got: {body}"

    # Verify the payload is never leaked back in the response
    assert payload not in resp.text, f"Payload leaked in response: {resp.text}"


def test_wellformed_unallowlisted_unharmonized_field_returns_400():
    """A well-formed but unallowlisted unharmonized field returns 400 (file allows only file_name)."""
    client = _client()
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
    client = _client()
    resp = client.get(FILE_PATH, params={"metadata.unharmonized.file_name": "UTYE.fastq"})

    assert resp.status_code != 400, \
        f"Allowlisted file_name filter should not be rejected, got 400: {resp.text}"
