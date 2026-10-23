"""Subject unharmonized sex_at_birth keeps the raw database string."""

import pytest

from tests.unit.test_subject_diagnosis_category import (
    _base_record,
    _make_diag_node,
    _make_repo,
)

_ANCESTOR = ["unharmonized.dcc_sex_at_birth"]


def _subject(sex):
    record = _base_record([])
    record["sex"] = sex
    return _make_repo()._record_to_subject(record).model_dump()


def test_sex_female_has_unharmonized_and_ancestors():
    s = _subject("Female")
    assert s["metadata"]["sex"] == {"value": "F", "ancestors": _ANCESTOR}
    entry = s["metadata"]["unharmonized"]["dcc_sex_at_birth"]
    assert entry["value"] == "Female"
    assert entry["details"]["harmonizer"] == "DomainExpert"
    assert entry["details"]["url"].endswith("sex_at_birth=Female")


def test_sex_male_keeps_raw_male():
    s = _subject("Male")
    assert s["metadata"]["sex"]["value"] == "M"
    assert s["metadata"]["unharmonized"]["dcc_sex_at_birth"]["value"] == "Male"


@pytest.mark.parametrize(
    "raw, encoded",
    [("Not Reported", "Not%20Reported"), ("Unknown", "Unknown")],
)
def test_sex_unmapped_raw_value_kept_as_unharmonized(raw, encoded):
    s = _subject(raw)
    assert s["metadata"]["sex"] == {"value": "U", "ancestors": _ANCESTOR}
    entry = s["metadata"]["unharmonized"]["dcc_sex_at_birth"]
    assert entry["value"] == raw
    assert entry["details"]["url"].endswith(f"sex_at_birth={encoded}")


def test_sex_lowercase_female_keeps_raw_spelling():
    s = _subject("female")
    assert s["metadata"]["sex"]["value"] == "F"
    assert s["metadata"]["unharmonized"]["dcc_sex_at_birth"]["value"] == "female"


def test_sex_null_raw_value():
    s = _subject(None)
    assert s["metadata"]["sex"] is None
    entry = s["metadata"]["unharmonized"]["dcc_sex_at_birth"]
    assert entry == {
        "value": None,
        "details": {"method": "Mapped", "harmonizer": "DomainExpert"},
    }
    assert "ancestors" not in (s["metadata"]["sex"] or {})


def test_sex_already_api_value_has_no_unharmonized():
    s = _subject("F")
    assert s["metadata"]["sex"] == {"value": "F"}
    assert "ancestors" not in s["metadata"]["sex"]
    unharmonized = s["metadata"]["unharmonized"] or {}
    assert "dcc_sex_at_birth" not in unharmonized


def test_two_natives_first_kept_second_unharmonized():
    record = _base_record([_make_diag_node(diagnosis_category="Medulloblastoma;Renal Tumors")])
    record["sex"] = "F"
    s = _make_repo()._record_to_subject(record).model_dump()
    assert s["metadata"]["associated_diagnosis_categories"] == {"value": "Medulloblastoma"}
    entry = s["metadata"]["unharmonized"]["dcc_associated_diagnosis_category_1"]
    assert entry["value"] == "Renal Tumors"
    assert entry["details"]["harmonizer"] == "diagnosis_classification_system"


def test_alias_promoted_with_ancestors():
    record = _base_record([_make_diag_node(diagnosis_category="Myeloid leukemias")])
    record["sex"] = "F"
    s = _make_repo()._record_to_subject(record).model_dump()
    assert s["metadata"]["associated_diagnosis_categories"] == {
        "value": "Myeloid Leukemia",
        "ancestors": ["unharmonized.dcc_associated_diagnosis_category_1"],
    }


def test_multiple_diagnosis_nodes_dedupe_and_number():
    record = _base_record(
        [
            _make_diag_node(diagnosis_category="Gliomas;Medulloblastoma"),
            _make_diag_node(diagnosis_category="Gliomas;Other X"),
        ]
    )
    record["sex"] = "F"
    s = _make_repo()._record_to_subject(record).model_dump()
    assert s["metadata"]["associated_diagnosis_categories"] == {"value": "Medulloblastoma"}
    unh = s["metadata"]["unharmonized"]
    assert set(unh) >= {
        "dcc_associated_diagnosis_category_1",
        "dcc_associated_diagnosis_category_2",
    }
    assert unh["dcc_associated_diagnosis_category_1"]["value"] == "Gliomas"
    assert unh["dcc_associated_diagnosis_category_2"]["value"] == "Other X"


def test_race_ethnicity_vital_status_have_no_ancestors_key():
    record = _base_record([])
    record["sex"] = "F"
    record["race"] = "White"
    record["vital_status"] = "Alive"
    s = _make_repo()._record_to_subject(record).model_dump()
    assert "ancestors" not in s["metadata"]["race"][0]
    assert "ancestors" not in s["metadata"]["ethnicity"]
    assert "ancestors" not in s["metadata"]["vital_status"]
