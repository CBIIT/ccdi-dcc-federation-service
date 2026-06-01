from typing import Any, Dict

import pytest

import app.api.v1.endpoints.files as files_ep
from app.models.dto import CountResponse, CountResult, SummaryResponse, SummaryCounts

COVERED_ROUTES = {
    ("GET", "/api/v1/file"),
    ("GET", "/api/v1/file/summary"),
    ("GET", "/api/v1/file/by/{field}/count"),
    ("GET", "/api/v1/file/{organization}/{namespace}/{name}"),
}


def test_file_list_contract(client, monkeypatch):
    class FakeFileService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_files(self, filters: Dict[str, Any], offset: int = 0, limit: int = 20):
            files = [
                {
                    "id": {"namespace": {"organization": "CCDI-DCC", "name": "phs002431"}, "name": "f1"},
                    "metadata": {"type": {"value": "BAM"}},
                }
            ]
            return files, 1

    monkeypatch.setattr(files_ep, "FileService", FakeFileService)

    r = client.get("/api/v1/file?page=1&per_page=20")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["counts"]["all"] == 1
    assert body["summary"]["counts"]["current"] == 1
    assert isinstance(body["data"], list) and len(body["data"]) == 1


def test_file_summary_contract(client, monkeypatch):
    class FakeFileService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_files_summary(self, filters: Dict[str, Any]):
            return SummaryResponse(counts=SummaryCounts(total=42))

    monkeypatch.setattr(files_ep, "FileService", FakeFileService)

    r = client.get("/api/v1/file/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["counts"]["total"] == 42


@pytest.mark.parametrize("endpoint", ["/api/v1/file/by/type/count", "/api/v1/file/by/depositions/count"])
def test_file_count_endpoint_contract(client, monkeypatch, endpoint: str):
    class FakeFileService:
        def __init__(self, *args, **kwargs):
            pass

        async def count_files_by_field(self, field: str, filters: Dict[str, Any]):
            return CountResponse(total=5, missing=1, values=[CountResult(value="X", count=4)])

    monkeypatch.setattr(files_ep, "FileService", FakeFileService)

    r = client.get(endpoint)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 5
    assert body["missing"] == 1
    assert sum(v["count"] for v in body["values"]) + body["missing"] == body["total"]


