"""
Tests for diagnosis_category field on /sample and /sample-diagnosis endpoints.
Mirrors test_subject_diagnosis_category.py — same DB property (d.diagnosis_category),
different API field name ('diagnosis_category' on sample vs 'associated_diagnosis_categories' on subject).
"""

# ---------------------------------------------------------------------------
# Task 1 — DTO model tests
# ---------------------------------------------------------------------------
from app.models.dto import AssociatedDiagnosisCategoryField, SampleMetadata


def test_sample_metadata_has_diagnosis_category_field():
    meta = SampleMetadata(
        diagnosis_category=[
            AssociatedDiagnosisCategoryField(value="Neuroblastoma"),
            AssociatedDiagnosisCategoryField(value="Renal Tumors"),
        ]
    )
    assert meta.diagnosis_category[0].value == "Neuroblastoma"
    assert meta.diagnosis_category[1].value == "Renal Tumors"


def test_sample_metadata_unharmonized_is_serialized():
    """unharmonized must NOT be excluded from serialization (needed for unharmonized categories)."""
    meta = SampleMetadata(
        unharmonized={"diagnosis_category": [{"value": "Custom ICD-O Value"}]}
    )
    dumped = meta.model_dump(exclude_none=True)
    assert "unharmonized" in dumped
    assert "diagnosis_category" in dumped["unharmonized"]


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
        _diag(diagnosis_category="Neuroblastoma"),
    )
    assert sample.metadata.diagnosis_category is not None
    vals = [item.value for item in sample.metadata.diagnosis_category]
    assert "Neuroblastoma" in vals


def test_unharmonized_category_goes_to_metadata_unharmonized():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Adenomas and adenocarcinomas"),
    )
    assert sample.metadata.diagnosis_category is None
    assert sample.metadata.unharmonized is not None
    assert "diagnosis_category" in sample.metadata.unharmonized
    items = sample.metadata.unharmonized["diagnosis_category"]
    assert any(item["value"] == "Adenomas and adenocarcinomas" for item in items)


def test_semicolon_delimited_splits_harmonized_and_unharmonized():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="Neuroblastoma;Adenomas and adenocarcinomas"),
    )
    harmonized_vals = [item.value for item in sample.metadata.diagnosis_category]
    unharmonized_items = sample.metadata.unharmonized["diagnosis_category"]
    unharmonized_vals = [item["value"] for item in unharmonized_items]
    assert "Neuroblastoma" in harmonized_vals
    assert "Adenomas and adenocarcinomas" not in harmonized_vals
    assert "Adenomas and adenocarcinomas" in unharmonized_vals


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
        _diag(diagnosis_category="Neuroblastoma;Neuroblastoma"),
    )
    harmonized_vals = [item.value for item in sample.metadata.diagnosis_category]
    assert harmonized_vals.count("Neuroblastoma") == 1


def test_case_insensitive_harmonization():
    """DB may store 'neuroblastoma' (lowercase) — should map to canonical 'Neuroblastoma'."""
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category="neuroblastoma"),
    )
    assert sample.metadata.diagnosis_category is not None
    vals = [item.value for item in sample.metadata.diagnosis_category]
    assert "Neuroblastoma" in vals


def test_whitespace_around_token_is_trimmed():
    repo = _make_repo()
    sample = repo._record_to_sample(
        _min_sa(), None, _min_st(), None, None,
        _diag(diagnosis_category=" Neuroblastoma ; Renal Tumors "),
    )
    harmonized_vals = [item.value for item in sample.metadata.diagnosis_category]
    assert "Neuroblastoma" in harmonized_vals
    assert "Renal Tumors" in harmonized_vals


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
    req = _mock_request({"diagnosis_category": "Neuroblastoma"})
    result = get_sample_filters(
        disease_phase=None, anatomical_sites=None, library_selection_method=None,
        library_strategy=None, library_source_material=None, preservation_method=None,
        tumor_grade=None, specimen_molecular_analyte_type=None, tissue_type=None,
        tumor_classification=None, age_at_diagnosis=None, age_at_collection=None,
        tumor_tissue_morphology=None, depositions=None, diagnosis=None,
        identifiers=None, diagnosis_category="Neuroblastoma",
        request=req,
    )
    assert result.get("diagnosis_category") == "Neuroblastoma"


def test_get_sample_filters_diagnosis_category_not_in_unknown_params():
    req = _mock_request({"diagnosis_category": "Neuroblastoma"})
    result = get_sample_filters(
        disease_phase=None, anatomical_sites=None, library_selection_method=None,
        library_strategy=None, library_source_material=None, preservation_method=None,
        tumor_grade=None, specimen_molecular_analyte_type=None, tissue_type=None,
        tumor_classification=None, age_at_diagnosis=None, age_at_collection=None,
        tumor_tissue_morphology=None, depositions=None, diagnosis=None,
        identifiers=None, diagnosis_category="Neuroblastoma",
        request=req,
    )
    assert "_unknown_parameters" not in result


# ---------------------------------------------------------------------------
# Task 4 — count endpoint dispatch tests
# ---------------------------------------------------------------------------
import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_count_samples_by_diagnosis_category_dispatches():
    repo = _make_repo()
    repo._count_samples_by_diagnosis_category = AsyncMock(
        return_value={"total": 20, "missing": 3, "values": [{"value": "Neuroblastoma", "count": 8}]}
    )
    result = await repo.count_samples_by_field("diagnosis_category", {})
    repo._count_samples_by_diagnosis_category.assert_called_once()
    assert result["total"] == 20
