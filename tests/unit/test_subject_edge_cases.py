"""Subject edge cases: diagnosis category, unharmonized details config, sex fallback, single subject."""

import pytest

import app.api.v1.endpoints.subjects as subjects_ep
from app.core import diagnosis_category as dc
from app.core.unharmonized_details import (
    _load_metadata_fields,
    build_unharmonized_details,
)
from app.models.dto import (
    MetadataField,
    NamespaceIdentifier,
    Subject,
    SubjectId,
    SubjectMetadata,
)
from tests.unit.test_subject_diagnosis_category import _base_record, _make_repo

# ---------------------------------------------------------------------------
# diagnosis_category edge branches
# ---------------------------------------------------------------------------


def test_mapping_keys_empty_when_field_config_missing(monkeypatch):
    monkeypatch.setattr(dc, "_find_field_config", lambda field: None)
    assert dc._diagnosis_category_mapping_keys() == frozenset()


def test_classify_blank_token_is_other():
    assert dc.classify_diagnosis_category_token("   ") == ("other", "", None)


def test_classify_token_mapping_to_none_is_other(monkeypatch):
    monkeypatch.setattr(dc, "map_field_value", lambda field, value: None)
    assert dc.classify_diagnosis_category_token("Medulloblastoma") == (
        "other",
        "Medulloblastoma",
        None,
    )


# ---------------------------------------------------------------------------
# unharmonized details: configured field without a details block
# ---------------------------------------------------------------------------


@pytest.fixture
def _no_details_config(monkeypatch):
    _load_metadata_fields.cache_clear()
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: {
            "subjects": {
                "fields": [{"path": "unharmonized.dcc_sex_at_birth", "harmonized": False}]
            }
        },
    )
    yield
    _load_metadata_fields.cache_clear()


def test_details_none_when_field_has_no_details(_no_details_config):
    assert build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female") is None


# ---------------------------------------------------------------------------
# sex normalization fallback when settings carry no sex_value_mappings
# ---------------------------------------------------------------------------


def _subject_without_sex_mappings(raw):
    repo = _make_repo()
    repo.settings.sex_value_mappings = {}
    record = _base_record([])
    record["sex"] = raw
    return repo._record_to_subject(record).model_dump()


@pytest.mark.parametrize(
    "raw, expected",
    [("female", "F"), ("F", "F"), ("MALE", "M"), ("m", "M"), ("Not Reported", "U")],
)
def test_sex_fallback_without_mappings(raw, expected):
    s = _subject_without_sex_mappings(raw)
    assert s["metadata"]["sex"]["value"] == expected


def test_sex_fallback_unknown_keeps_raw_under_unharmonized():
    s = _subject_without_sex_mappings("Not Reported")
    assert s["metadata"]["sex"]["ancestors"] == ["unharmonized.dcc_sex_at_birth"]
    assert s["metadata"]["unharmonized"]["dcc_sex_at_birth"]["value"] == "Not Reported"


def test_sex_fallback_already_api_value_has_no_unharmonized():
    s = _subject_without_sex_mappings("F")
    assert "ancestors" not in s["metadata"]["sex"]
    assert "dcc_sex_at_birth" not in (s["metadata"]["unharmonized"] or {})


# ---------------------------------------------------------------------------
# single-subject endpoint: no top-level gateways, empty-result paths
# ---------------------------------------------------------------------------

_EMPTY = {"summary": {"counts": {"all": 0, "current": 0}}, "data": []}


def _patch_service(monkeypatch, *, result=None, error=None):
    calls = []

    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subject_by_identifier(self, organization, namespace, name, base_url=None):
            calls.append((organization, namespace, name))
            if error is not None:
                raise error
            return result

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)
    return calls


def test_single_subject_has_no_gateways(client, monkeypatch):
    subject = Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name="phs001"), name="P1"),
        metadata=SubjectMetadata(sex=MetadataField(value="M")),
    )
    calls = _patch_service(monkeypatch, result=subject)
    r = client.get("/api/v1/subject/CCDI-DCC/phs001/P1")
    assert r.status_code == 200
    body = r.json()
    assert "gateways" not in body
    assert body["id"]["name"] == "P1"
    assert calls == [("CCDI-DCC", "phs001", "P1")]


def test_single_subject_not_found_returns_empty(client, monkeypatch):
    _patch_service(monkeypatch, result=None)
    r = client.get("/api/v1/subject/CCDI-DCC/phs001/NOPE")
    assert r.status_code == 200
    assert r.json() == _EMPTY


def test_single_subject_service_error_returns_empty(client, monkeypatch):
    _patch_service(monkeypatch, error=RuntimeError("db exploded"))
    r = client.get("/api/v1/subject/CCDI-DCC/phs001/P1")
    assert r.status_code == 200
    assert r.json() == _EMPTY
    assert "db exploded" not in r.text


def test_single_subject_blank_name_returns_empty(client, monkeypatch):
    calls = _patch_service(monkeypatch, result=None)
    r = client.get("/api/v1/subject/CCDI-DCC/phs001/%20")
    assert r.status_code == 200
    assert r.json() == _EMPTY
    assert calls == []


def test_single_subject_comma_name_is_invalid(client, monkeypatch):
    _patch_service(monkeypatch, result=None)
    r = client.get("/api/v1/subject/CCDI-DCC/phs001/P1,P2")
    assert r.status_code == 400
    assert "P1,P2" not in r.text


def test_single_subject_other_organization_is_invalid(client, monkeypatch):
    _patch_service(monkeypatch, result=None)
    r = client.get("/api/v1/subject/OTHER-ORG/phs001/P1")
    assert r.status_code == 400
    assert "OTHER-ORG" not in r.text


# ---------------------------------------------------------------------------
# Diagnosis node order is stable (collect(DISTINCT d) has no ORDER BY)
# ---------------------------------------------------------------------------
from tests.unit.test_subject_diagnosis_category import _make_diag_node  # noqa: E402


def _category_view(nodes):
    record = _base_record(nodes)
    record["sex"] = "F"
    s = _make_repo()._record_to_subject(record).model_dump()
    return s["metadata"]["associated_diagnosis_categories"], s["metadata"]["unharmonized"]


def test_diagnosis_node_order_does_not_change_output():
    a = _make_diag_node(diagnosis_category="Medulloblastoma", guid="guid-a")
    b = _make_diag_node(diagnosis_category="Renal Tumors", guid="guid-b")
    assert _category_view([a, b]) == _category_view([b, a])


def test_lowest_guid_wins_harmonized_slot():
    # Input order is reversed on purpose: guid-a must win regardless of collect() order.
    renal = _make_diag_node(diagnosis_category="Renal Tumors", guid="guid-b")
    medullo = _make_diag_node(diagnosis_category="Medulloblastoma", guid="guid-a")
    harmonized, unharmonized = _category_view([renal, medullo])
    assert harmonized == {"value": "Medulloblastoma"}
    assert unharmonized["dcc_associated_diagnosis_category_1"]["value"] == "Renal Tumors"


def test_nodes_without_guid_fall_back_to_text_order():
    a = _make_diag_node(diagnosis_category="Renal Tumors")
    b = _make_diag_node(diagnosis_category="Medulloblastoma")
    assert _category_view([a, b]) == _category_view([b, a])
    harmonized, _ = _category_view([a, b])
    assert harmonized == {"value": "Medulloblastoma"}
