"""
Tests for diagnosis_category field on /sample and /sample-diagnosis endpoints.
Mirrors subject coverage where applicable, but sample uses singular
SampleDiagnosisCategoryField + dcc_diagnosis_category_* unharmonized keys.
"""

from app.models.dto import SampleDiagnosisCategoryField, SampleMetadata


def test_sample_metadata_has_singular_diagnosis_category_field():
    meta = SampleMetadata(
        diagnosis_category=SampleDiagnosisCategoryField(value="Medulloblastoma")
    )
    assert meta.diagnosis_category.value == "Medulloblastoma"
    assert meta.diagnosis_category.ancestors is None
    dumped = meta.diagnosis_category.model_dump()
    assert dumped == {"value": "Medulloblastoma"}
    assert "ancestors" not in dumped


def test_sample_diagnosis_category_ancestors_included_when_set():
    field = SampleDiagnosisCategoryField(
        value="Low-Grade Gliomas",
        ancestors=["unharmonized.dcc_diagnosis_category_1"],
    )
    assert field.model_dump() == {
        "value": "Low-Grade Gliomas",
        "ancestors": ["unharmonized.dcc_diagnosis_category_1"],
    }

def test_sample_metadata_unharmonized_is_serialized():
    """unharmonized must NOT be excluded from serialization (needed for dcc_* keys)."""
    meta = SampleMetadata(
        unharmonized={
            "dcc_diagnosis_category_1": {
                "value": "Custom ICD-O Value",
            }
        }
    )
    dumped = meta.model_dump(exclude_none=True)
    assert "unharmonized" in dumped
    assert "dcc_diagnosis_category_1" in dumped["unharmonized"]


def test_sample_metadata_diagnosis_category_defaults_to_none():
    meta = SampleMetadata()
    assert meta.diagnosis_category is None


# ---------------------------------------------------------------------------
# Task 2 — _record_to_sample extraction tests
# ---------------------------------------------------------------------------
from unittest.mock import MagicMock
from app.repositories.sample import SampleRepository


def _make_repo() -> SampleRepository:
    session = MagicMock()
    allowlist = MagicMock()
    settings = MagicMock()
    settings.sex_value_mappings = {"Male": "M", "Female": "F", "Not Reported": "U"}
    settings.identifier_server_url = "https://example.com"
    return SampleRepository(session, allowlist, settings)


def _min_sa() -> dict:
    return {"sample_id": "S001"}


def _min_st() -> dict:
    return {"study_id": "phs001"}


def _diag(diagnosis_category: str | None = None) -> dict:
    return {
        "diagnosis_category": diagnosis_category,
        "diagnosis": None,
        "disease_phase": None,
        "tumor_grade": None,
        "age_at_diagnosis": None,
        "tumor_classification": None,
    }


def test_harmonized_category_appears_in_diagnosis_category():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Medulloblastoma"),
    )
    assert sample.metadata.diagnosis_category is not None
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.diagnosis_category.ancestors is None
    assert sample.metadata.unharmonized is None


def test_unharmonized_category_goes_to_metadata_unharmonized():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Adenomas and adenocarcinomas"),
    )
    assert sample.metadata.diagnosis_category is None
    assert sample.metadata.unharmonized is not None
    assert sample.metadata.unharmonized["dcc_diagnosis_category_1"]["value"] == (
        "Adenomas and adenocarcinomas"
    )


def test_semicolon_delimited_splits_native_and_other():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Medulloblastoma;Adenomas and adenocarcinomas"),
    )
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.diagnosis_category.ancestors is None
    assert sample.metadata.unharmonized == {
        "dcc_diagnosis_category_1": {"value": "Adenomas and adenocarcinomas"}
    }


def test_extra_native_goes_to_dcc_canonical():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Medulloblastoma;Renal Tumors;Gliomas"),
    )
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.unharmonized == {
        "dcc_diagnosis_category_1": {"value": "Renal Tumors"},
        "dcc_diagnosis_category_2": {"value": "Gliomas"},
    }


def test_alias_only_promotes_with_ancestors_and_comment():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Low-grade Gliomas;Gliomas"),
    )
    assert sample.metadata.diagnosis_category.value == "Low-Grade Gliomas"
    assert sample.metadata.diagnosis_category.ancestors == [
        "unharmonized.dcc_diagnosis_category_1"
    ]
    assert sample.metadata.unharmonized == {
        "dcc_diagnosis_category_1": {
            "value": "Low-grade Gliomas",
            "comment": "Low-Grade Gliomas",
            "owned": True,
        },
        "dcc_diagnosis_category_2": {"value": "Gliomas"},
    }


def test_two_aliases_no_native_promotes_first_keeps_second_in_dcc():
    """Coverage gap: ≥2 field_mappings aliases, no native — first promote, rest in dcc_*."""
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Low-grade Gliomas;Myeloid leukemias"),
    )
    assert sample.metadata.diagnosis_category.value == "Low-Grade Gliomas"
    assert sample.metadata.diagnosis_category.ancestors == [
        "unharmonized.dcc_diagnosis_category_1"
    ]
    assert sample.metadata.unharmonized == {
        "dcc_diagnosis_category_1": {
            "value": "Low-grade Gliomas",
            "comment": "Low-Grade Gliomas",
            "owned": True,
        },
        "dcc_diagnosis_category_2": {
            "value": "Myeloid leukemias",
            "comment": "Myeloid Leukemia",
            "owned": True,
        },
    }


def test_alias_with_native_keeps_raw_in_dcc_not_promoted():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Medulloblastoma;Low-grade Gliomas"),
    )
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.diagnosis_category.ancestors is None
    assert sample.metadata.unharmonized == {
        "dcc_diagnosis_category_1": {
            "value": "Low-grade Gliomas",
            "comment": "Low-Grade Gliomas",
            "owned": True,
        }
    }


def test_no_diagnosis_node_leaves_diagnosis_category_none():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None, None
    )
    assert sample.metadata.diagnosis_category is None


def test_null_diagnosis_category_property_leaves_field_none():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category=None),
    )
    assert sample.metadata.diagnosis_category is None


def test_deduplication_via_semicolon_repeated_token():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Medulloblastoma;Medulloblastoma"),
    )
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.unharmonized is None


def test_case_insensitive_harmonization():
    """DB may store 'medulloblastoma' (lowercase) — should map to canonical 'Medulloblastoma'."""
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="medulloblastoma"),
    )
    assert sample.metadata.diagnosis_category is not None
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"


def test_whitespace_around_token_is_trimmed():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category=" Medulloblastoma ; Renal Tumors "),
    )
    assert sample.metadata.diagnosis_category.value == "Medulloblastoma"
    assert sample.metadata.unharmonized["dcc_diagnosis_category_1"]["value"] == "Renal Tumors"


# ---------------------------------------------------------------------------
# Task 3 — filter parameter tests
# ---------------------------------------------------------------------------
from app.api.v1.deps import get_sample_filters


def _mock_request(params: dict) -> MagicMock:
    req = MagicMock()
    qp = MagicMock()
    qp.keys = lambda: params.keys()
    qp.items = lambda: params.items()
    qp.getlist = lambda key: [params[key]] if key in params else []
    req.query_params = qp
    return req


def test_get_sample_filters_accepts_diagnosis_category():
    req = _mock_request({"diagnosis_category": "Medulloblastoma"})
    result = get_sample_filters(
        disease_phase=None, anatomical_sites=None, library_selection_method=None,
        library_strategy=None, library_source_material=None, preservation_method=None,
        tumor_grade=None, specimen_molecular_analyte_type=None, tissue_type=None,
        tumor_classification=None, age_at_diagnosis=None, age_at_collection=None,
        tumor_tissue_morphology=None, depositions=None, diagnosis=None,
        identifiers=None, diagnosis_category="Medulloblastoma",
        request=req,
    )
    assert result.get("diagnosis_category") == "Medulloblastoma"


def test_get_sample_filters_diagnosis_category_not_in_unknown_params():
    req = _mock_request({"diagnosis_category": "Medulloblastoma"})
    result = get_sample_filters(
        disease_phase=None, anatomical_sites=None, library_selection_method=None,
        library_strategy=None, library_source_material=None, preservation_method=None,
        tumor_grade=None, specimen_molecular_analyte_type=None, tissue_type=None,
        tumor_classification=None, age_at_diagnosis=None, age_at_collection=None,
        tumor_tissue_morphology=None, depositions=None, diagnosis=None,
        identifiers=None, diagnosis_category="Medulloblastoma",
        request=req,
    )
    assert "_unknown_parameters" not in result


# ---------------------------------------------------------------------------
# Task 4 — count dispatch (smoke)
# ---------------------------------------------------------------------------
import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_count_samples_by_diagnosis_category_dispatches():
    repo = _make_repo()
    repo._count_samples_by_diagnosis_category = AsyncMock(
        return_value={"total": 0, "values": []}
    )
    result = await repo.count_samples_by_field("diagnosis_category")
    repo._count_samples_by_diagnosis_category.assert_called_once()
    assert result["total"] == 0
