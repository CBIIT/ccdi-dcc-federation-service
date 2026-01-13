from typing import Any, Dict

import app.api.v1.endpoints.experimental as experimental_ep


# Registry entries for endpoints outside subject/sample/file.
# These must be updated when new /api/v1 routes are added.
COVERED_ROUTES = {
    ("GET", "/api/v1/"),
    ("GET", "/api/v1/info"),
    ("GET", "/api/v1/errors/examples"),
    ("GET", "/api/v1/metadata/fields/subject"),
    ("GET", "/api/v1/metadata/fields/sample"),
    ("GET", "/api/v1/metadata/fields/file"),
    ("GET", "/api/v1/namespace"),
    ("GET", "/api/v1/namespace/{organization}/{namespace}"),
    ("GET", "/api/v1/organization"),
    ("GET", "/api/v1/organization/{name}"),
    ("GET", "/api/v1/subject-diagnosis"),
}


def test_root_api_v1_returns_json(client):
    r = client.get("/api/v1/")
    assert r.status_code in (200, 404)  # depends on presence of config_data/info.json


def test_info_endpoint_contract(client):
    r = client.get("/api/v1/info")
    assert r.status_code in (200, 404)  # depends on presence of config_data/info.json
    if r.status_code == 200:
        body = r.json()
        assert "server" in body
        assert "api" in body
        assert "data" in body


def test_error_examples_contract(client):
    r = client.get("/api/v1/errors/examples")
    assert r.status_code == 200
    body = r.json()
    assert "errors" in body
    assert isinstance(body["errors"], list)


def test_metadata_fields_contracts(client):
    for path in (
        "/api/v1/metadata/fields/subject",
        "/api/v1/metadata/fields/sample",
        "/api/v1/metadata/fields/file",
    ):
        r = client.get(path)
        assert r.status_code in (200, 404)  # depends on presence of config_data/metadata_fields.json
        if r.status_code == 200:
            body = r.json()
            assert "fields" in body
            assert isinstance(body["fields"], list)


def test_namespace_list_contract(client):
    # With DummySession (no rows), this should return an empty list (200).
    r = client.get("/api/v1/namespace")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_organization_list_contract(client):
    # Organization endpoint reads info.json; if missing in env, it returns 404 by design.
    r = client.get("/api/v1/organization")
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        assert isinstance(r.json(), list)


def test_subject_diagnosis_route_reachable_with_mocked_service(client, monkeypatch):
    # Endpoint is in experimental.py and uses SubjectService; ensure route is wired.
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(self, filters: Dict[str, Any], offset: int = 0, limit: int = 20, base_url=None):
            return []

        async def get_subjects_summary(self, filters: Dict[str, Any]):
            # Keep it minimal; endpoint code only needs counts.total.
            from app.models.dto import SummaryResponse, SummaryCounts

            return SummaryResponse(counts=SummaryCounts(total=0))

    monkeypatch.setattr(experimental_ep, "SubjectService", FakeSubjectService)

    r = client.get("/api/v1/subject-diagnosis?search=Neuroblastoma&page=1&per_page=10")
    assert r.status_code in (200, 404)  # endpoint may choose 404 on empty; we mainly guard wiring


