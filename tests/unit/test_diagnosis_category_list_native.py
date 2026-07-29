"""
Regression tests for Memgraph 3.11 list-native diagnosis_category handling.

In 3.11 the diagnosis_category graph property is a LIST, not a semicolon-delimited
string. split_diagnosis_category_tokens() and the response converters that call it
must handle a list without stringifying it (str(['A']) -> "['A']" would have produced
one garbled unharmonized token instead of the real harmonized/unharmonized split).
"""

import pytest
from unittest.mock import patch

from app.core.diagnosis_category import (
    HARMONIZED_DIAGNOSIS_CATEGORIES,
    canonical_diagnosis_category_token,
    diagnosis_category_filter_db_values,
    diagnosis_category_token_case_expr,
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

    def test_blank_tokens_skipped(self):
        """Empty / whitespace-only tokens after strip are ignored (line 95)."""
        harmonized, unharmonized = split_diagnosis_category_tokens(
            ["", "  ", HARMONIZED_PV, ";;"]
        )
        assert harmonized == [HARMONIZED_PV]
        assert unharmonized == []


@pytest.mark.unit
class TestCanonicalDiagnosisCategoryToken:
    """Edge cases for canonical_diagnosis_category_token (core coverage)."""

    def test_none_returns_none(self):
        assert canonical_diagnosis_category_token(None) is None

    def test_blank_returns_none(self):
        assert canonical_diagnosis_category_token("") is None
        assert canonical_diagnosis_category_token("   ") is None

    def test_harmonized_pv_round_trip(self):
        assert canonical_diagnosis_category_token(HARMONIZED_PV) == HARMONIZED_PV

    def test_map_field_value_none_returns_none(self):
        """Defensive path when map_field_value nulls a token (line 34)."""
        with patch(
            "app.core.diagnosis_category.map_field_value",
            return_value=None,
        ):
            assert canonical_diagnosis_category_token("anything") is None


@pytest.mark.unit
class TestDiagnosisCategoryTokenCaseExpr:
    def test_returns_case_statement_when_mappings_exist(self):
        expr = diagnosis_category_token_case_expr("token")
        assert "CASE" in expr
        assert "Myeloid leukemias" in expr

    def test_falls_back_to_variable_when_no_case_statement(self):
        with patch(
            "app.core.diagnosis_category.build_case_mapping_statement",
            return_value="",
        ):
            assert diagnosis_category_token_case_expr("tok") == "tok"


@pytest.mark.unit
class TestDiagnosisCategoryFilterDbValues:
    def test_myeloid_leukemia_expands_aliases(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        assert diagnosis_category_filter_db_values("Myeloid Leukemia") == [
            "myeloid leukemias",
            "myeloid leukemia",
        ]

    def test_unmapped_value_passthrough(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        assert diagnosis_category_filter_db_values("Neuroblastoma") == ["neuroblastoma"]

    def test_empty_and_none_return_empty_list(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        assert diagnosis_category_filter_db_values(None) == []
        assert diagnosis_category_filter_db_values("") == []
        assert diagnosis_category_filter_db_values("   ") == []

    def test_low_grade_gliomas_alias_expands_lowered(self):
        from app.core.diagnosis_category import diagnosis_category_filter_db_values

        vals = diagnosis_category_filter_db_values("Low-Grade Gliomas")
        assert vals == ["low-grade gliomas"]
        assert all(v == v.lower() for v in vals)

    def test_falsy_reverse_map_falls_back_to_raw_token(self):
        """When reverse_map returns None/empty, use the API token as-is (line 66)."""
        with patch(
            "app.core.diagnosis_category.reverse_map_field_value",
            return_value=None,
        ):
            assert diagnosis_category_filter_db_values("Neuroblastoma") == ["neuroblastoma"]


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
