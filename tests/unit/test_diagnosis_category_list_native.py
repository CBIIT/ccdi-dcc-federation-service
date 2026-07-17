"""
Regression tests for Memgraph 3.11 list-native diagnosis_category handling.

In 3.11 the diagnosis_category graph property is a LIST, not a semicolon-delimited
string. split_diagnosis_category_tokens() and the response converters that call it
must handle a list without stringifying it (str(['A']) -> "['A']" would have produced
one garbled unharmonized token instead of the real harmonized/unharmonized split).
"""

import pytest

from app.core.diagnosis_category import (
    HARMONIZED_DIAGNOSIS_CATEGORIES,
    split_diagnosis_category_tokens,
)
from app.repositories.sample_converters import _build_diagnosis_result

# A real harmonized permissible value and a value that is not harmonized.
HARMONIZED_PV = sorted(HARMONIZED_DIAGNOSIS_CATEGORIES)[0]
UNHARMONIZED_TOKEN = "some custom diagnosis category"


@pytest.mark.unit
class TestSplitDiagnosisCategoryTokens:
    """split_diagnosis_category_tokens accepts LIST (3.11) and legacy STRING alike."""

    def test_list_input_matches_string_input(self):
        as_list = split_diagnosis_category_tokens([HARMONIZED_PV, UNHARMONIZED_TOKEN])
        as_string = split_diagnosis_category_tokens(f"{HARMONIZED_PV};{UNHARMONIZED_TOKEN}")
        assert as_list == as_string

    def test_single_element_list_harmonizes(self):
        harmonized, unharmonized = split_diagnosis_category_tokens([HARMONIZED_PV])
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == []

    def test_list_is_not_stringified_into_one_garbled_token(self):
        # The pre-fix bug: str([HARMONIZED_PV]) -> "['...']" as a single unharmonized token.
        _, unharmonized = split_diagnosis_category_tokens([HARMONIZED_PV])
        assert not any(tok.startswith("[") for tok in unharmonized)
        assert str([HARMONIZED_PV]) not in unharmonized

    def test_unharmonized_element_in_list(self):
        harmonized, unharmonized = split_diagnosis_category_tokens([UNHARMONIZED_TOKEN])
        assert harmonized == []
        assert unharmonized == [UNHARMONIZED_TOKEN]

    def test_none_returns_empty(self):
        assert split_diagnosis_category_tokens(None) == ([], [])

    def test_empty_list_returns_empty(self):
        assert split_diagnosis_category_tokens([]) == ([], [])

    def test_list_element_with_embedded_semicolon_is_flattened(self):
        harmonized, unharmonized = split_diagnosis_category_tokens(
            [f"{HARMONIZED_PV};{UNHARMONIZED_TOKEN}"]
        )
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == [UNHARMONIZED_TOKEN]

    def test_none_elements_skipped(self):
        harmonized, unharmonized = split_diagnosis_category_tokens([HARMONIZED_PV, None])
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == []

    def test_mapped_aliases_become_harmonized(self):
        """DB spellings from field_mappings.json land in the harmonized PV bucket."""
        harmonized, unharmonized = split_diagnosis_category_tokens(
            ["Low-grade Gliomas", "Myeloid leukemias"]
        )
        assert harmonized == ["Low-Grade Gliomas", "Myeloid Leukemia"]
        assert unharmonized == []

    def test_mapped_alias_case_fold_still_works(self):
        """Existing case-insensitive enum match still applies after mapping."""
        harmonized, unharmonized = split_diagnosis_category_tokens(["low-grade gliomas"])
        assert harmonized == ["Low-Grade Gliomas"]
        assert unharmonized == []


@pytest.mark.unit
class TestDiagnosisCategoryFilterDbValues:
    def test_myeloid_leukemia_expands_aliases(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        assert diagnosis_category_filter_db_values("Myeloid Leukemia") == [
            "Myeloid leukemias",
            "Myeloid Leukemia",
        ]

    def test_unmapped_value_passthrough(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        assert diagnosis_category_filter_db_values("Neuroblastoma") == ["Neuroblastoma"]


@pytest.mark.unit
class TestBuildDiagnosisResultListNative:
    """_build_diagnosis_result reads a LIST diagnosis_category from raw diagnosis nodes."""

    def test_list_diagnosis_category_harmonized(self):
        diagnoses = [{"diagnosis": "Some Dx", "diagnosis_category": [HARMONIZED_PV]}]
        _, _, harmonized, unharmonized = _build_diagnosis_result(diagnoses)
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == []

    def test_list_diagnosis_category_mixed(self):
        diagnoses = [
            {"diagnosis": "Some Dx", "diagnosis_category": [HARMONIZED_PV, UNHARMONIZED_TOKEN]}
        ]
        _, _, harmonized, unharmonized = _build_diagnosis_result(diagnoses)
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == [UNHARMONIZED_TOKEN]
        # Guard against the pre-fix stringified-list token leaking in.
        assert not any(tok.startswith("[") for tok in unharmonized)
