import logging
import pytest

import app.api.v1.endpoints.subjects as subjects_ep
from app.core.gateways import _load_gateways_config, get_gateways
from app.models.dto import SubjectResponse


@pytest.fixture(autouse=True)
def _clear_gateway_cache():
    _load_gateways_config.cache_clear()
    yield
    _load_gateways_config.cache_clear()


def test_subject_gateways_loaded_in_order():
    gws = get_gateways("subjects")
    assert [g["name"] for g in gws] == [
        "CCDI DCC model",
        "Childhood Cancer Clinical Data Commons",
    ]
    assert gws[0]["link"]["kind"] == "Approximate"
    assert "instructions" in gws[0]["link"]
    assert "status" not in gws[0]
    assert "available_at" not in gws[0]


def test_unknown_entity_returns_empty():
    assert get_gateways("nope") == []


def test_malformed_gateways_config_top_level_list_returns_empty(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: ["not", "a", "dict"],
    )
    with caplog.at_level(logging.WARNING):
        assert get_gateways("subjects") == []


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "Broken Open", "kind": "Open"},
        {"name": "Broken Registered", "kind": "Registered"},
        {"name": "Broken Controlled", "kind": "Controlled"},
        {
            "name": "Broken Approximate",
            "kind": "Open",
            "link": {"kind": "Approximate", "url": "https://example.com"},
        },
    ],
)
def test_invalid_gateway_shapes_are_skipped(entry, monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {
            "subjects": [
                entry,
                {
                    "name": "Valid",
                    "kind": "Open",
                    "link": {
                        "kind": "Approximate",
                        "url": "https://example.com",
                        "instructions": "Go",
                    },
                },
            ]
        },
    )
    with caplog.at_level(logging.WARNING):
        gws = get_gateways("subjects")
    assert len(gws) == 1
    assert gws[0]["name"] == "Valid"


def test_open_gateway_without_link_is_skipped(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {"subjects": [{"name": "Broken", "kind": "Open"}]},
    )
    with caplog.at_level(logging.WARNING):
        gws = get_gateways("subjects")
    assert gws == []
    assert "invalid gateway" in caplog.text.lower()


def test_approximate_link_without_instructions_is_skipped(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {
            "subjects": [
                {
                    "name": "Bad approximate",
                    "kind": "Open",
                    "link": {
                        "kind": "Approximate",
                        "url": "https://example.com",
                    },
                },
                {
                    "name": "Valid",
                    "kind": "Open",
                    "link": {
                        "kind": "Approximate",
                        "url": "https://example.com",
                        "instructions": "Go",
                    },
                },
            ]
        },
    )
    with caplog.at_level(logging.WARNING):
        gws = get_gateways("subjects")
    assert len(gws) == 1
    assert gws[0]["name"] == "Valid"
    assert "invalid gateway" in caplog.text.lower()


def test_invalid_gateway_entry_is_skipped(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {
            "subjects": [
                {"name": "x"},
                {
                    "name": "Valid",
                    "kind": "Open",
                    "link": {
                        "kind": "Approximate",
                        "url": "https://example.com",
                        "instructions": "Go",
                    },
                },
            ]
        },
    )
    with caplog.at_level(logging.WARNING):
        gws = get_gateways("subjects")
    assert len(gws) == 1
    assert gws[0]["name"] == "Valid"
    assert "invalid gateway" in caplog.text.lower()


def test_subject_response_serializes_gateways_only_when_set():
    assert "gateways" not in SubjectResponse(summary={}, data=[]).model_dump()
    dumped = SubjectResponse(summary={}, data=[], gateways=[{"name": "g"}]).model_dump()
    assert dumped["gateways"] == [{"name": "g"}]


@pytest.mark.asyncio
async def test_empty_subject_list_has_no_gateways(client, monkeypatch):
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(self, *args, **kwargs):
            return ([], 0)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)
    r = client.get("/api/v1/subject?page=1&per_page=5")
    assert r.status_code == 200
    assert "gateways" not in r.json()


@pytest.mark.asyncio
async def test_subject_diagnosis_list_has_gateways(client, monkeypatch):
    import app.api.v1.endpoints.experimental as experimental_ep
    from app.models.dto import MetadataField, NamespaceIdentifier, Subject, SubjectId, SubjectMetadata

    subject = Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name="phs001"), name="P1"),
        metadata=SubjectMetadata(sex=MetadataField(value="M")),
    )

    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects_for_diagnosis_endpoint(self, *args, **kwargs):
            return ([subject], 1)

    monkeypatch.setattr(experimental_ep, "SubjectService", FakeSubjectService)
    r = client.get("/api/v1/subject-diagnosis?search=cancer&page=1&per_page=5")
    assert r.status_code == 200
    assert r.json()["gateways"] == get_gateways("subjects")


@pytest.mark.asyncio
async def test_subject_list_has_gateways(client, monkeypatch):
    from app.models.dto import MetadataField, NamespaceIdentifier, Subject, SubjectId, SubjectMetadata

    subject = Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name="phs001"), name="P1"),
        metadata=SubjectMetadata(sex=MetadataField(value="M")),
    )

    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(self, *args, **kwargs):
            return ([subject], 1)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)
    r = client.get("/api/v1/subject?page=1&per_page=5")
    assert r.status_code == 200
    assert r.json()["gateways"] == get_gateways("subjects")
