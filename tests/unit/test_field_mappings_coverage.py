"""
Additional unit tests for field_mappings.py to improve coverage.

Tests missing lines and edge cases not covered by existing tests.
"""

import pytest
from unittest.mock import patch, mock_open
import json
from pathlib import Path

from app.core.field_mappings import (
    _load_field_mappings,
    _get_field_mappings,
    map_field_value,
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
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
class TestLoadFieldMappings:
    """Test _load_field_mappings function."""

    @patch('builtins.open', new_callable=mock_open, read_data='{"sample": {"field1": {}}}')
    @patch('app.core.field_mappings.Path')
    def test_load_field_mappings_success(self, mock_path, mock_file):
        """Test loading field mappings from file."""
        mock_path.return_value.__truediv__.return_value = Path("/fake/path/field_mappings.json")
        
        result = _load_field_mappings()
        
        assert isinstance(result, dict)
        assert "sample" in result


@pytest.mark.unit
class TestGetFieldMappingsCache:
    """Test _get_field_mappings caching behavior."""

    @patch('app.core.field_mappings._load_field_mappings')
    def test_get_field_mappings_uses_cache(self, mock_load):
        """Test that _get_field_mappings uses cache on second call."""
        import app.core.field_mappings as fm_module
        
        # Reset cache
        fm_module._field_mappings_cache = None
        
        mock_load.return_value = {"sample": {"field1": {}}}
        
        # First call
        result1 = _get_field_mappings()
        # Second call should use cache
        result2 = _get_field_mappings()
        
        assert result1 == result2
        # Should only load once
        assert mock_load.call_count == 1
        
        # Reset cache
        fm_module._field_mappings_cache = None


@pytest.mark.unit
class TestMapFieldValueEdgeCases:
    """Test edge cases for map_field_value."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_field_value_with_empty_mappings_dict(self, mock_get_mappings):
        """Test map_field_value when mappings dict is empty."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "mappings": {},
                    "null_mappings": []
                }
            }
        }
        
        result = map_field_value("field1", "SomeValue")
        assert result == "SomeValue"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_field_value_with_mapping_but_not_null(self, mock_get_mappings):
        """Test map_field_value when value has mapping but is not in null_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "mappings": {
                        "DBValue": "APIValue"
                    },
                    "null_mappings": []
                }
            }
        }
        
        result = map_field_value("field1", "DBValue")
        assert result == "APIValue"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_field_value_no_field_config(self, mock_get_mappings):
        """Test map_field_value when field config is not found."""
        mock_get_mappings.return_value = {}
        
        result = map_field_value("unknown_field", "value")
        assert result == "value"


@pytest.mark.unit
class TestReverseMapFieldValueEdgeCases:
    """Test edge cases for reverse_map_field_value."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_field_value_no_reverse_mapping(self, mock_get_mappings):
        """Test reverse_map_field_value when no reverse mapping exists."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "reverse_mappings": {}
                }
            }
        }
        
        result = reverse_map_field_value("field1", "SomeValue")
        assert result == "SomeValue"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_field_value_single_value(self, mock_get_mappings):
        """Test reverse_map_field_value returns single value (not list)."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "reverse_mappings": {
                        "APIValue": "DBValue"
                    }
                }
            }
        }
        
        result = reverse_map_field_value("field1", "APIValue")
        assert result == "DBValue"
        assert not isinstance(result, list)


@pytest.mark.unit
class TestIsDatabaseOnlyValueEdgeCases:
    """Test edge cases for is_database_only_value."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_value_in_mappings_and_reverse(self, mock_get_mappings):
        """Test is_database_only_value when value is in mappings but also as a key in reverse_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "mappings": {
                        "DBValue": "APIValue"
                    },
                    "reverse_mappings": {
                        "DBValue": "SomeOtherValue"  # DBValue is a KEY in reverse_mappings
                    }
                }
            }
        }
        
        result = is_database_only_value("field1", "DBValue")
        # Value is in mappings AND as a key in reverse_mappings, so NOT database-only
        assert result is False

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_value_not_in_mappings(self, mock_get_mappings):
        """Test is_database_only_value when value is not in mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {
                    "mappings": {},
                    "reverse_mappings": {}
                }
            }
        }
        
        result = is_database_only_value("field1", "SomeValue")
        assert result is False


@pytest.mark.unit
class TestBuildInvalidValueAllClauseEdgeCases:
    """Test edge cases for build_invalid_value_all_clause."""

    @patch('app.core.field_mappings.get_null_mappings')
    def test_build_invalid_value_all_clause_without_not_reported(self, mock_get_null):
        """Test build_invalid_value_all_clause without 'Not Reported'."""
        mock_get_null.return_value = ["-999", "Unknown"]
        
        result = build_invalid_value_all_clause("field1")
        
        assert "toString(val) = '-999'" in result
        assert "toString(val) = 'Unknown'" in result
        assert "toString(val) = 'Not Reported'" not in result

    @patch('app.core.field_mappings.get_null_mappings')
    def test_build_invalid_value_all_clause_empty_null_mappings(self, mock_get_null):
        """Test build_invalid_value_all_clause with empty null_mappings."""
        mock_get_null.return_value = []
        
        result = build_invalid_value_all_clause("field1")
        
        # Should still have basic conditions
        assert "toString(val) = ''" in result
        assert "toString(val) = '-999'" in result


@pytest.mark.unit
class TestBuildCaseMappingStatementEdgeCases:
    """Test edge cases for build_case_mapping_statement."""

    @patch('app.core.field_mappings._find_field_config')
    def test_build_case_mapping_statement_with_multiple_mappings(self, mock_find):
        """Test build_case_mapping_statement with multiple mappings."""
        mock_find.return_value = (
            "sample",
            {
                "mappings": {
                    "Value1": "Mapped1",
                    "Value2": "Mapped2"
                }
            }
        )
        
        result = build_case_mapping_statement("field1", "val")
        
        assert "WHEN val = 'Value1' THEN 'Mapped1'" in result
        assert "WHEN val = 'Value2' THEN 'Mapped2'" in result
        assert "ELSE val" in result
        assert result.startswith("CASE")
        assert result.endswith("END")

    @patch('app.core.field_mappings._find_field_config')
    def test_build_case_mapping_statement_with_special_chars(self, mock_find):
        """Test build_case_mapping_statement with special characters in values."""
        mock_find.return_value = (
            "sample",
            {
                "mappings": {
                    "O'Connor": "Value's"
                }
            }
        )
        
        result = build_case_mapping_statement("field1", "val")
        
        # Should escape single quotes
        assert "O\\'Connor" in result
        assert "Value\\'s" in result


@pytest.mark.unit
class TestLoadEnumFunctions:
    """Test enum loading functions with various edge cases."""

    def test_load_sequencing_file_enum_success(self):
        """Test load_sequencing_file_enum with valid data."""
        test_data = {"library_strategy": ["WGS", "WXS", "RNA-Seq"]}
        json_data = json.dumps(test_data)
        
        with patch('pathlib.Path.open', mock_open(read_data=json_data)):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sequencing_file_enum("library_strategy")
                assert result == ["WGS", "WXS", "RNA-Seq"]

    def test_load_sequencing_file_enum_data_not_dict(self):
        """Test load_sequencing_file_enum when data is not a dict."""
        json_data = json.dumps(["not", "a", "dict"])
        
        with patch('pathlib.Path.open', mock_open(read_data=json_data)):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sequencing_file_enum("library_strategy")
                assert result == []

    def test_load_sample_enum_success(self):
        """Test load_sample_enum with valid data."""
        test_data = {"sample_tumor_status": ["Tumor", "Normal"]}
        json_data = json.dumps(test_data)
        
        with patch('pathlib.Path.open', mock_open(read_data=json_data)):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sample_enum("sample_tumor_status")
                assert result == ["Tumor", "Normal"]

    def test_load_sample_enum_data_not_dict(self):
        """Test load_sample_enum when data is not a dict."""
        json_data = json.dumps(["not", "a", "dict"])
        
        with patch('pathlib.Path.open', mock_open(read_data=json_data)):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sample_enum("sample_tumor_status")
                assert result == []

    def test_load_sequencing_file_enum_json_decode_error(self):
        """Test load_sequencing_file_enum with JSON decode error."""
        with patch('pathlib.Path.open', mock_open(read_data="invalid json")):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sequencing_file_enum("library_strategy")
                assert result == []

    def test_load_sample_enum_json_decode_error(self):
        """Test load_sample_enum with JSON decode error."""
        with patch('pathlib.Path.open', mock_open(read_data="invalid json")):
            with patch('pathlib.Path.exists', return_value=True):
                result = load_sample_enum("sample_tumor_status")
                assert result == []


@pytest.mark.unit
class TestGetMappedDbValues:
    """Test get_mapped_db_values function."""

    @patch('app.core.field_mappings._find_field_config')
    def test_get_mapped_db_values_with_mappings(self, mock_find):
        """Test get_mapped_db_values with mappings."""
        mock_find.return_value = (
            "sample",
            {
                "mappings": {
                    "DB1": "API1",
                    "DB2": "API2",
                    "DB3": "API3"
                }
            }
        )
        
        result = get_mapped_db_values("field1")
        
        assert len(result) == 3
        assert "DB1" in result
        assert "DB2" in result
        assert "DB3" in result

    @patch('app.core.field_mappings._find_field_config')
    def test_get_mapped_db_values_no_config(self, mock_find):
        """Test get_mapped_db_values when field config not found."""
        mock_find.return_value = None
        
        result = get_mapped_db_values("unknown_field")
        assert result == []

    @patch('app.core.field_mappings._find_field_config')
    def test_get_mapped_db_values_empty_mappings(self, mock_find):
        """Test get_mapped_db_values with empty mappings."""
        mock_find.return_value = (
            "sample",
            {
                "mappings": {}
            }
        )
        
        result = get_mapped_db_values("field1")
        assert result == []


@pytest.mark.unit
class TestGetFieldMappingInfo:
    """Test get_field_mapping_info function."""

    @patch('app.core.field_mappings._find_field_config')
    def test_get_field_mapping_info_success(self, mock_find):
        """Test get_field_mapping_info with valid field."""
        mock_find.return_value = (
            "sample",
            {
                "mappings": {"A": "B"},
                "null_mappings": ["-999"]
            }
        )
        
        result = get_field_mapping_info("field1")
        
        assert result is not None
        assert result["node_type"] == "sample"
        assert result["field_config"]["mappings"]["A"] == "B"
        assert result["field_config"]["null_mappings"] == ["-999"]

    @patch('app.core.field_mappings._find_field_config')
    def test_get_field_mapping_info_not_found(self, mock_find):
        """Test get_field_mapping_info when field not found."""
        mock_find.return_value = None
        
        result = get_field_mapping_info("unknown_field")
        assert result is None
