"""Spec matrix: harmonized values, unharmonized raw sources, and ancestors."""

import pytest

from app.core.diagnosis_category import build_diagnosis_category_fields
from tests.unit.test_subject_diagnosis_category import (
    _base_record,
    _make_diag_node,
    _make_repo,
)

SUBJECT_PREFIX = "dcc_associated_diagnosis_category"
_ANCESTOR_SEX = ["unharmonized.dcc_sex_at_birth"]


@pytest.mark.parametrize(
    "raw_sex, sex_value, expect_ancestors, expect_unharmonized_raw",
    [
        ("Female", "F", True, "Female"),
        ("Male", "M", True, "Male"),
        ("Not Reported", "U", True, "Not Reported"),
        ("Unknown", "U", True, "Unknown"),
        ("F", "F", False, None),
        ("female", "F", True, "female"),
    ],
)
def test_subject_sex_transformation_matrix(
    raw_sex, sex_value, expect_ancestors, expect_unharmonized_raw
):
    record = _base_record([])
    record["sex"] = raw_sex
    meta = _make_repo()._record_to_subject(record).model_dump()["metadata"]

    assert meta["sex"]["value"] == sex_value
    if expect_ancestors:
        assert meta["sex"]["ancestors"] == _ANCESTOR_SEX
        assert meta["unharmonized"]["dcc_sex_at_birth"]["value"] == expect_unharmonized_raw
        assert "details" in meta["unharmonized"]["dcc_sex_at_birth"]
    else:
        assert "ancestors" not in meta["sex"]
        unh = meta.get("unharmonized") or {}
        assert "dcc_sex_at_birth" not in unh


def test_subject_sex_null_defensive_unharmonized():
    record = _base_record([])
    record["sex"] = None
    meta = _make_repo()._record_to_subject(record).model_dump()["metadata"]
    assert meta["sex"] is None
    entry = meta["unharmonized"]["dcc_sex_at_birth"]
    assert entry["value"] is None
    assert "url" not in entry["details"]
    assert "ancestors" not in (meta["sex"] or {})


@pytest.mark.parametrize(
    "tokens, regular, unharmonized_fragment",
    [
        (
            ["Medulloblastoma"],
            {"value": "Medulloblastoma"},
            None,
        ),
        (
            ["medulloblastoma"],
            {
                "value": "Medulloblastoma",
                "ancestors": [f"unharmonized.{SUBJECT_PREFIX}_1"],
            },
            {f"{SUBJECT_PREFIX}_1": {"value": "medulloblastoma"}},
        ),
        (
            ["Myeloid leukemias"],
            {
                "value": "Myeloid Leukemia",
                "ancestors": [f"unharmonized.{SUBJECT_PREFIX}_1"],
            },
            {f"{SUBJECT_PREFIX}_1": {"value": "Myeloid leukemias"}},
        ),
        (
            ["Medulloblastoma", "Renal Tumors"],
            {"value": "Medulloblastoma"},
            {f"{SUBJECT_PREFIX}_1": {"value": "Renal Tumors"}},
        ),
    ],
)
def test_diagnosis_category_transformation_matrix(tokens, regular, unharmonized_fragment):
    got_regular, got_unh = build_diagnosis_category_fields(tokens, SUBJECT_PREFIX)
    assert got_regular == regular
    if unharmonized_fragment is None:
        assert got_unh is None
    else:
        for key, fragment in unharmonized_fragment.items():
            assert got_unh[key]["value"] == fragment["value"]
            if "comment" in fragment:
                assert got_unh[key]["comment"] == fragment["comment"]


def test_subject_record_diagnosis_wires_details_on_unharmonized():
    record = _base_record([_make_diag_node(diagnosis_category="Gliomas")])
    record["sex"] = "F"
    unh = _make_repo()._record_to_subject(record).model_dump()["metadata"]["unharmonized"]
    entry = unh["dcc_associated_diagnosis_category_1"]
    assert entry["value"] == "Gliomas"
    assert entry["details"]["harmonizer"] == "diagnosis_classification_system"
