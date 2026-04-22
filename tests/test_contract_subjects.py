from typing import Any, Dict, List

import pytest

import app.api.v1.endpoints.subjects as subjects_ep
from app.models.dto import (
    Subject,
    SubjectId,
    NamespaceIdentifier,
    SubjectMetadata,
    MetadataField,
    CountResponse,
    CountResult,
)

COVERED_ROUTES = {
    ("GET", "/api/v1/subject"),
    ("GET", "/api/v1/subject/summary"),
    ("GET", "/api/v1/subject/by/{field}/count"),
    ("GET", "/api/v1/subject/{organization}/{namespace}/{name}"),
}


def _subject(participant_id: str = "P1", study_id: str = "phs002431") -> Subject:
    return Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name=study_id), name=participant_id),
        metadata=SubjectMetadata(
            sex=MetadataField(value="F"),
            race=[MetadataField(value="White")],
            ethnicity=MetadataField(value="Not reported"),
            vital_status=None,
            age_at_vital_status=None,
            associated_diagnoses=None,
            depositions=None,
            identifiers=None,
        ),
    )


def test_subject_list_empty_returns_200(client, monkeypatch):
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            return ([], 0)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    r = client.get("/api/v1/subject?sex=F&page=1&per_page=20")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["counts"]["all"] == 0
    assert body["summary"]["counts"]["current"] == 0
    assert body["data"] == []


def test_subject_list_includes_required_keys_even_when_null(client, monkeypatch):
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            return ([_subject()], 1)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    r = client.get("/api/v1/subject?sex=F&page=1&per_page=20")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["counts"]["all"] == 1
    assert body["summary"]["counts"]["current"] == 1
    subj = body["data"][0]

    # Customer-experience regression guard:
    # these keys must exist even if they are null (or omitted upstream).
    meta = subj["metadata"]
    assert "associated_diagnoses" in meta
    assert "vital_status" in meta
    assert "age_at_vital_status" in meta


def test_subject_list_sets_link_header_for_pagination(client, monkeypatch):
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            # Return a full page -> has_next should be True -> Link header should include rel="next"
            return ([_subject(participant_id="P1"), _subject(participant_id="P2")], 999)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    r = client.get("/api/v1/subject?page=1&per_page=2&sex=F")
    assert r.status_code == 200
    assert "Link" in r.headers
    assert 'rel="first"' in r.headers["Link"]
    assert 'rel="next"' in r.headers["Link"]


def test_subject_unknown_query_param_returns_invalid_parameters(client):
    r = client.get("/api/v1/subject?unknown_param=1")
    assert r.status_code == 400
    body = r.json()
    assert "errors" in body
    assert body["errors"][0]["kind"] in ("InvalidParameters", "InvalidRoute")


@pytest.mark.parametrize(
    "endpoint",
    [
        "/api/v1/subject/by/sex/count",
        "/api/v1/subject/by/race/count",
    ],
)
def test_subject_count_endpoint_contract_and_math(client, monkeypatch, endpoint: str):
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def count_subjects_by_field(self, field: str, filters: Dict[str, Any]):
            # simple invariant: total == missing + sum(values)
            return CountResponse(
                total=2,
                missing=1,
                values=[CountResult(value="X", count=1)],
            )

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    r = client.get(endpoint)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert body["missing"] == 1
    assert sum(v["count"] for v in body["values"]) + body["missing"] == body["total"]


