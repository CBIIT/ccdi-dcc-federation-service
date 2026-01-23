"""
Enhanced unit tests for field mapping utilities.

Tests missing helper utilities and edge cases.
"""

import json
import pytest
from unittest.mock import patch, mock_open

from app.core.field_mappings import (
    is_null_mapped_value,
    get_null_mappings,
    build_invalid_value_filter,
    build_invalid_value_list_filter,
    build_invalid_value_all_clause,
    build_case_mapping_statement,
    get_mapped_db_values,
    load_sequencing_file_enum,
    load_sample_enum,
    get_field_mapping_info,
)


@pytest.mark.unit
class TestNullMappingsHelpers:
    """Test null mapping helper functions."""

    @patch("app.core.field_mappings._find_field_config")
    def test_get_null_mappings_empty_when_missing(self, mock_find):
        mock_find.return_value = None
        assert get_null_mappings("unknown_field") == []

    @patch("app.core.field_mappings._find_field_config")
    def test_get_null_mappings_returns_list(self, mock_find):
        mock_find.return_value = ("sample", {"null_mappings": ["-999", "Not Reported"]})
        assert get_null_mappings("library_strategy") == ["-999", "Not Reported"]

    @patch("app.core.field_mappings._find_field_config")
    def test_is_null_mapped_value_true(self, mock_find):
        mock_find.return_value = ("sample", {"null_mappings": ["-999"]})
        assert is_null_mapped_value("library_strategy", "-999") is True

    @patch("app.core.field_mappings._find_field_config")
    def test_is_null_mapped_value_false_when_missing(self, mock_find):
        mock_find.return_value = None
        assert is_null_mapped_value("library_strategy", "-999") is False

    def test_is_null_mapped_value_empty_or_none(self):
        assert is_null_mapped_value("library_strategy", None) is False
        assert is_null_mapped_value("library_strategy", "") is False
        assert is_null_mapped_value("library_strategy", "   ") is False


@pytest.mark.unit
class TestInvalidValueFilters:
    """Test invalid value filter builders."""

    @patch("app.core.field_mappings.get_null_mappings")
    def test_build_invalid_value_filter_includes_null_mappings(self, mock_nulls):
        mock_nulls.return_value = ["-999", "Not Reported", "Unknown"]
        result = build_invalid_value_filter("sf.library_strategy", "library_strategy")
        assert "sf.library_strategy <> '-999'" in result
        assert "sf.library_strategy <> 'Not Reported'" in result
        assert "sf.library_strategy <> 'Unknown'" in result

    @patch("app.core.field_mappings.get_null_mappings")
    def test_build_invalid_value_list_filter_includes_null_mappings(self, mock_nulls):
        mock_nulls.return_value = ["-999", "Not Reported", "Unknown"]
        result = build_invalid_value_list_filter("library_strategy")
        assert "val <> '-999'" in result
        assert "val <> 'Not Reported'" in result
        assert "val <> 'Unknown'" in result

    @patch("app.core.field_mappings.get_null_mappings")
    def test_build_invalid_value_all_clause_includes_null_mappings(self, mock_nulls):
        mock_nulls.return_value = ["-999", "Not Reported", "Unknown"]
        result = build_invalid_value_all_clause("library_strategy")
        assert "toString(val) = '-999'" in result
        assert "toString(val) = 'Not Reported'" in result
        assert "toString(val) = 'Unknown'" in result


@pytest.mark.unit
class TestCaseMappingStatement:
    """Test CASE statement generation."""

    @patch("app.core.field_mappings._find_field_config")
    def test_build_case_mapping_statement_empty(self, mock_find):
        mock_find.return_value = ("sample", {"mappings": {}})
        assert build_case_mapping_statement("field") == ""

    @patch("app.core.field_mappings._find_field_config")
    def test_build_case_mapping_statement_with_mappings(self, mock_find):
        mock_find.return_value = ("sample", {"mappings": {"Transcriptomic": "RNA"}})
        result = build_case_mapping_statement("specimen_molecular_analyte_type", "value")
        assert "WHEN value = 'Transcriptomic' THEN 'RNA'" in result
        assert "ELSE value" in result

    @patch("app.core.field_mappings._find_field_config")
    def test_build_case_mapping_statement_escapes_quotes(self, mock_find):
        mock_find.return_value = ("sample", {"mappings": {"O'Connor": "Value's"}})
        result = build_case_mapping_statement("field", "val")
        assert "O\\'Connor" in result
        assert "Value\\'s" in result


@pytest.mark.unit
class TestMappedDbValues:
    """Test mapped DB values helper."""

    @patch("app.core.field_mappings._find_field_config")
    def test_get_mapped_db_values_empty(self, mock_find):
        mock_find.return_value = None
        assert get_mapped_db_values("field") == []

    @patch("app.core.field_mappings._find_field_config")
    def test_get_mapped_db_values(self, mock_find):
        mock_find.return_value = ("sample", {"mappings": {"A": "B", "C": "D"}})
        assert set(get_mapped_db_values("field")) == {"A", "C"}


@pytest.mark.unit
class TestEnumLoading:
    """Test enum loading helpers."""

    def test_load_sequencing_file_enum_missing_file(self):
        with patch("app.core.field_mappings.Path.open", side_effect=FileNotFoundError):
            assert load_sequencing_file_enum("library_strategy") == []

    def test_load_sample_enum_missing_file(self):
        with patch("app.core.field_mappings.Path.open", side_effect=FileNotFoundError):
            assert load_sample_enum("sample_tumor_status") == []

    def test_load_sequencing_file_enum_invalid_json(self):
        with patch("app.core.field_mappings.Path.open", mock_open(read_data="not-json")):
            assert load_sequencing_file_enum("library_strategy") == []

    def test_load_sample_enum_success(self):
        data = json.dumps({"sample_tumor_status": ["Tumor", "Normal"]})
        with patch("app.core.field_mappings.Path.open", mock_open(read_data=data)):
            assert load_sample_enum("sample_tumor_status") == ["Tumor", "Normal"]


@pytest.mark.unit
class TestFieldMappingInfo:
    """Test get_field_mapping_info helper."""

    @patch("app.core.field_mappings._find_field_config")
    def test_get_field_mapping_info_missing(self, mock_find):
        mock_find.return_value = None
        assert get_field_mapping_info("field") is None

    @patch("app.core.field_mappings._find_field_config")
    def test_get_field_mapping_info(self, mock_find):
        mock_find.return_value = ("sample", {"mappings": {"A": "B"}})
        result = get_field_mapping_info("field")
        assert result["node_type"] == "sample"
        assert result["field_config"]["mappings"]["A"] == "B"

