from app.core.diagnosis_category import HARMONIZED_DIAGNOSIS_CATEGORIES
from app.models.dto import AssociatedDiagnosisCategoryField, SubjectMetadata


def test_age_at_vital_status_zero_enables_survival_processing():
    """Integer 0 from deps must not be treated as absent (bool(0) is False)."""
    from unittest.mock import MagicMock
    from app.repositories.subject import SubjectRepository

    session = MagicMock()
    allowlist = MagicMock()
    settings = MagicMock()
    settings.sex_value_mappings = {"Male": "M", "Female": "F", "Not Reported": "U"}
    settings.identifier_server_url = "https://example.com"
    settings.subject_count_fields = []
    repo = SubjectRepository(session, allowlist, settings)

    fs = repo._build_subject_where({"age_at_vital_status": 0})
    assert fs.needs_survival_processing is True
    assert fs.derived_filters.get("age_at_vital_status") == 0


def test_harmonized_categories_is_frozenset():
    assert isinstance(HARMONIZED_DIAGNOSIS_CATEGORIES, frozenset)


def test_associated_diagnosis_category_field_model():
    field = AssociatedDiagnosisCategoryField(value="Medulloblastoma")
    assert field.value == "Medulloblastoma"


def test_subject_metadata_has_associated_diagnosis_categories():
    meta = SubjectMetadata(
        associated_diagnosis_categories=[
            AssociatedDiagnosisCategoryField(value="Medulloblastoma"),
            AssociatedDiagnosisCategoryField(value="Renal Tumors"),
        ]
    )
    assert meta.associated_diagnosis_categories[0].value == "Medulloblastoma"


def test_subject_metadata_unharmonized_is_serialized():
    meta = SubjectMetadata(
        unharmonized={
            "associated_diagnosis_category_1": {"value": "ICD-O Value"}
        }
    )
    dumped = meta.model_dump(exclude_none=True)
    assert "unharmonized" in dumped
    assert "associated_diagnosis_category_1" in dumped["unharmonized"]


# ---------------------------------------------------------------------------
# Task 3 — _record_to_subject extraction tests
# ---------------------------------------------------------------------------
from unittest.mock import MagicMock
from app.repositories.subject import SubjectRepository


def _make_repo() -> SubjectRepository:
    session = MagicMock()
    allowlist = MagicMock()
    settings = MagicMock()
    settings.sex_value_mappings = {"Male": "M", "Female": "F", "Not Reported": "U"}
    settings.identifier_server_url = "https://example.com"
    settings.subject_count_fields = [
        "sex", "race", "ethnicity", "vital_status", "age_at_vital_status",
        "associated_diagnoses", "associated_diagnosis_categories",
    ]
    return SubjectRepository(session, allowlist, settings)


def _make_diag_node(diagnosis_category: str | None = None, diagnosis: str | None = None):
    node = MagicMock()
    data = {"diagnosis_category": diagnosis_category, "diagnosis": diagnosis}
    node.get = lambda key, default=None: data.get(key, default)
    node.__getitem__ = lambda self, key: data[key]
    return node


def _base_record(diagnosis_nodes=None):
    return {
        "name": "P001",
        "namespace": "phs001",
        "depositions": ["phs001"],
        "race": None,
        "sex": "Male",
        "vital_status": None,
        "age_at_vital_status": None,
        "associated_diagnoses": None,
        "survival_records": None,
        "diagnosis_nodes": diagnosis_nodes or [],
    }


def test_harmonized_value_goes_to_associated_diagnosis_categories():
    repo = _make_repo()
    record = _base_record([_make_diag_node(diagnosis_category="Medulloblastoma")])
    subject = repo._record_to_subject(record)
    assert subject.metadata.associated_diagnosis_categories is not None
    harmonized_vals = [item.value for item in subject.metadata.associated_diagnosis_categories]
    assert "Medulloblastoma" in harmonized_vals
    assert subject.metadata.unharmonized is None or \
           not any(
               k.startswith("associated_diagnosis_category_")
               for k in (subject.metadata.unharmonized or {})
           )


def test_out_of_spec_value_goes_to_unharmonized():
    repo = _make_repo()
    record = _base_record([_make_diag_node(diagnosis_category="Adenomas and adenocarcinomas")])
    subject = repo._record_to_subject(record)
    assert subject.metadata.associated_diagnosis_categories is None
    assert subject.metadata.unharmonized is not None
    assert "associated_diagnosis_category_1" in subject.metadata.unharmonized
    assert subject.metadata.unharmonized["associated_diagnosis_category_1"]["value"] == (
        "Adenomas and adenocarcinomas"
    )


def test_mixed_values_split_correctly():
    repo = _make_repo()
    nodes = [
        _make_diag_node(diagnosis_category="Medulloblastoma"),
        _make_diag_node(diagnosis_category="Adenomas and adenocarcinomas"),
    ]
    record = _base_record(nodes)
    subject = repo._record_to_subject(record)
    harmonized_vals = [item.value for item in subject.metadata.associated_diagnosis_categories]
    unharmonized = subject.metadata.unharmonized
    unharmonized_vals = [
        unharmonized[k]["value"]
        for k in sorted(unharmonized)
        if k.startswith("associated_diagnosis_category_")
    ]
    assert "Medulloblastoma" in harmonized_vals
    assert "Adenomas and adenocarcinomas" not in harmonized_vals
    assert "Adenomas and adenocarcinomas" in unharmonized_vals


def test_no_diagnosis_nodes_leaves_fields_none():
    repo = _make_repo()
    record = _base_record([])
    subject = repo._record_to_subject(record)
    assert subject.metadata.associated_diagnosis_categories is None


def test_deduplication_of_same_category():
    repo = _make_repo()
    nodes = [
        _make_diag_node(diagnosis_category="Medulloblastoma"),
        _make_diag_node(diagnosis_category="Medulloblastoma"),
    ]
    record = _base_record(nodes)
    subject = repo._record_to_subject(record)
    harmonized_vals = [item.value for item in subject.metadata.associated_diagnosis_categories]
    assert harmonized_vals.count("Medulloblastoma") == 1


def test_semicolon_delimited_single_node_splits_correctly():
    repo = _make_repo()
    record = _base_record([_make_diag_node(diagnosis_category="Medulloblastoma;Adenomas and adenocarcinomas")])
    subject = repo._record_to_subject(record)
    harmonized_vals = [item.value for item in subject.metadata.associated_diagnosis_categories]
    unharmonized = subject.metadata.unharmonized
    unharmonized_vals = [
        unharmonized[k]["value"]
        for k in sorted(unharmonized)
        if k.startswith("associated_diagnosis_category_")
    ]
    assert "Medulloblastoma" in harmonized_vals
    assert "Adenomas and adenocarcinomas" in unharmonized_vals
    assert "Adenomas and adenocarcinomas" not in harmonized_vals


def test_multiple_unharmonized_categories_use_numbered_keys():
    repo = _make_repo()
    record = _base_record(
        [
            _make_diag_node(
                diagnosis_category=(
                    "Circumscribed Astrocytic Gliomas;Gliomas;"
                    "Gliomas, glioneuronal tumors, and neuronal tumors"
                )
            )
        ]
    )
    subject = repo._record_to_subject(record)
    unharmonized = subject.metadata.unharmonized
    assert unharmonized == {
        "associated_diagnosis_category_1": {"value": "Circumscribed Astrocytic Gliomas"},
        "associated_diagnosis_category_2": {"value": "Gliomas"},
        "associated_diagnosis_category_3": {
            "value": "Gliomas, glioneuronal tumors, and neuronal tumors"
        },
    }


# ---------------------------------------------------------------------------
# Task 4 — filter parameter tests
# ---------------------------------------------------------------------------
from app.api.v1.deps import get_subject_filters


def _mock_request(params: dict) -> MagicMock:
    req = MagicMock()
    qp = MagicMock()
    qp.keys = lambda: params.keys()
    qp.items = lambda: params.items()
    req.query_params = qp
    return req


def test_get_subject_filters_accepts_associated_diagnosis_categories():
    req = _mock_request({"associated_diagnosis_categories": "Medulloblastoma"})
    result = get_subject_filters(
        sex=None, race=None, ethnicity=None, identifiers=None,
        vital_status=None, age_at_vital_status=None, depositions=None,
        associated_diagnosis_categories="Medulloblastoma",
        request=req,
    )
    assert result.get("associated_diagnosis_categories") == "Medulloblastoma"
    assert "_unknown_parameters" not in result


def test_get_subject_filters_rejects_unknown_param():
    req = _mock_request({"not_a_real_field": "foo"})
    result = get_subject_filters(
        sex=None, race=None, ethnicity=None, identifiers=None,
        vital_status=None, age_at_vital_status=None, depositions=None,
        associated_diagnosis_categories=None,
        request=req,
    )
    assert "_unknown_parameters" in result


# ---------------------------------------------------------------------------
# Task 5 — count endpoint dispatch tests
# ---------------------------------------------------------------------------
import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_count_subjects_by_associated_diagnosis_categories_dispatches():
    repo = _make_repo()
    repo._count_subjects_by_diagnosis_category = AsyncMock(
        return_value={"total": 10, "missing": 2, "values": [{"value": "Medulloblastoma", "count": 5}]}
    )
    result = await repo.count_subjects_by_field("associated_diagnosis_categories", {})
    repo._count_subjects_by_diagnosis_category.assert_called_once()
    assert result["total"] == 10
