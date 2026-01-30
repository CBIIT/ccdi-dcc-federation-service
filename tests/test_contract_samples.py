from typing import Any, Dict

import pytest

import app.api.v1.endpoints.samples as samples_ep
import app.api.v1.endpoints.experimental as experimental_ep
from app.models.dto import (
    Sample,
    SampleMetadata,
    ValueField,
    SummaryResponse,
    SummaryCounts,
    CountResponse,
    CountResult,
)

COVERED_ROUTES = {
    ("GET", "/api/v1/sample"),
    ("GET", "/api/v1/sample/summary"),
    ("GET", "/api/v1/sample/by/{field}/count"),
    ("GET", "/api/v1/sample/{organization}/{namespace}/{name}"),
    ("GET", "/api/v1/sample-diagnosis"),
}


def _sample(sample_id: str = "S1", study_id: str = "phs002431") -> Sample:
    # NOTE: dto.py defines NamespaceIdentifier twice; SampleIdentifier captures the first one.
    # Passing a dict avoids type-mismatch validation errors caused by the second class.
    return Sample(
        id={"namespace": {"organization": "CCDI-DCC", "name": study_id}, "name": sample_id},
        subject=None,
        metadata=SampleMetadata(
            preservation_method=ValueField(value="OCT"),
            library_strategy=None,
            disease_phase=None,
            depositions=None,
            identifiers=None,
        ),
    )


def test_sample_list_basic_contract(client, monkeypatch):
    class FakeSampleService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_samples(self, filters: Dict[str, Any], offset: int = 0, limit: int = 20, return_total: bool = False):
            samples = [_sample()]
            if return_total:
                return (samples, 1)
            return samples

        async def get_samples_summary(self, filters: Dict[str, Any]):
            return SummaryResponse(counts=SummaryCounts(total=1))

    monkeypatch.setattr(samples_ep, "SampleService", FakeSampleService)

    r = client.get("/api/v1/sample?page=1&per_page=20")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["counts"]["all"] == 1
    assert body["summary"]["counts"]["current"] == 1
    assert isinstance(body["data"], list) and len(body["data"]) == 1


def test_sample_unknown_query_param_rejected(client):
    # /sample explicitly rejects 'search' (reserved for /sample-diagnosis)
    r = client.get("/api/v1/sample?search=Neuroblastoma")
    assert r.status_code == 400
    body = r.json()
    assert "errors" in body
    assert body["errors"][0]["kind"] == "InvalidParameters"


def test_sample_summary_contract(client, monkeypatch):
    class FakeSampleService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_samples_summary(self, filters: Dict[str, Any]):
            return SummaryResponse(counts=SummaryCounts(total=123))

    monkeypatch.setattr(samples_ep, "SampleService", FakeSampleService)

    r = client.get("/api/v1/sample/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["counts"]["total"] == 123


def test_sample_count_endpoint_contract_and_math(client, monkeypatch):
    class FakeSampleService:
        def __init__(self, *args, **kwargs):
            pass

        async def count_samples_by_field(self, field: str, filters: Dict[str, Any]):
            return CountResponse(
                total=10,
                missing=2,
                values=[CountResult(value="WXS", count=8)],
            )

    monkeypatch.setattr(samples_ep, "SampleService", FakeSampleService)

    r = client.get("/api/v1/sample/by/library_strategy/count")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 10
    assert body["missing"] == 2
    assert sum(v["count"] for v in body["values"]) + body["missing"] == body["total"]


def test_sample_diagnosis_endpoint_contract(client, monkeypatch):
    class FakeSampleService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_samples(self, filters: Dict[str, Any], offset: int = 0, limit: int = 20, return_total: bool = False):
            samples = [_sample()]
            if return_total:
                return (samples, 1)
            return samples

        async def get_samples_summary(self, filters: Dict[str, Any]):
            return SummaryResponse(counts=SummaryCounts(total=1))

    monkeypatch.setattr(experimental_ep, "SampleService", FakeSampleService)

    r = client.get("/api/v1/sample-diagnosis?search=Neuroblastoma&page=1&per_page=10")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["counts"]["all"] == 1
    assert body["summary"]["counts"]["current"] == 1


