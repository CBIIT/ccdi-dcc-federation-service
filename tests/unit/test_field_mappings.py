"""
Unit tests for field mapping utilities.

Tests value mapping between database and API representations.
"""

import pytest
from unittest.mock import patch, mock_open
from app.core.field_mappings import (
    map_field_value,
    reverse_map_field_value,
    is_database_only_value,
    is_null_mapped_value,
    get_null_mappings,
    build_invalid_value_filter,
    _find_field_config,
    _get_field_mappings
)


@pytest.mark.unit
class TestMapFieldValue:
    """Test cases for map_field_value function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_value_with_mapping(self, mock_get_mappings):
        """Test mapping a value that has a mapping."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {
                        "PCR": "PCR"
                    }
                }
            }
        }
        
        result = map_field_value("library_selection_method", "PCR")
        
        assert result == "PCR"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_value_with_null_mapping(self, mock_get_mappings):
        """Test mapping a value that should become null."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "null_mappings": ["-999", "Not Reported"],
                    "mappings": {}
                }
            }
        }
        
        result = map_field_value("library_selection_method", "-999")
        
        assert result is None

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_value_no_mapping(self, mock_get_mappings):
        """Test mapping a value with no mapping."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {}
                }
            }
        }
        
        result = map_field_value("library_selection_method", "SomeValue")
        
        assert result == "SomeValue"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_value_field_not_found(self, mock_get_mappings):
        """Test mapping a value for field not in config."""
        mock_get_mappings.return_value = {}
        
        result = map_field_value("unknown_field", "value")
        
        assert result == "value"

    def test_map_value_none(self):
        """Test mapping None value."""
        result = map_field_value("any_field", None)
        
        assert result is None

    def test_map_value_empty_string(self):
        """Test mapping empty string."""
        result = map_field_value("any_field", "")
        
        assert result is None

    def test_map_value_whitespace_string(self):
        """Test mapping whitespace-only string."""
        result = map_field_value("any_field", "   ")
        
        assert result is None

    @patch('app.core.field_mappings._get_field_mappings')
    def test_map_value_priority_null_over_mapping(self, mock_get_mappings):
        """Test that null_mappings take priority over regular mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "null_mappings": ["-999"],
                    "mappings": {
                        "-999": "SomeValue"
                    }
                }
            }
        }
        
        result = map_field_value("library_selection_method", "-999")
        
        # null_mappings should take priority
        assert result is None


@pytest.mark.unit
class TestReverseMapFieldValue:
    """Test cases for reverse_map_field_value function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_with_mapping(self, mock_get_mappings):
        """Test reverse mapping a value."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {
                        "PCR": "PCR"
                    }
                }
            }
        }
        
        result = reverse_map_field_value("library_selection_method", "PCR")
        
        assert result == "PCR"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_no_mapping(self, mock_get_mappings):
        """Test reverse mapping with no mapping."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {}
                }
            }
        }
        
        result = reverse_map_field_value("library_selection_method", "SomeValue")
        
        assert result == "SomeValue"

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_field_not_found(self, mock_get_mappings):
        """Test reverse mapping for field not in config."""
        mock_get_mappings.return_value = {}
        
        result = reverse_map_field_value("unknown_field", "value")
        
        assert result == "value"

    def test_reverse_map_none(self):
        """Test reverse mapping None value."""
        result = reverse_map_field_value("any_field", None)
        
        assert result is None

    def test_reverse_map_empty_string(self):
        """Test reverse mapping empty string."""
        result = reverse_map_field_value("any_field", "")
        
        assert result is None

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_returns_list(self, mock_get_mappings):
        """Test reverse mapping that returns a list."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "reverse_mappings": {
                        "PCR": ["PCR", "PCR-based"]
                    }
                }
            }
        }
        
        result = reverse_map_field_value("library_selection_method", "PCR")
        
        assert isinstance(result, list)
        assert "PCR" in result
        assert "PCR-based" in result

    @patch('app.core.field_mappings._get_field_mappings')
    def test_reverse_map_whitespace_string(self, mock_get_mappings):
        """Test reverse mapping whitespace-only string."""
        mock_get_mappings.return_value = {}
        
        result = reverse_map_field_value("any_field", "   ")
        
        assert result is None


@pytest.mark.unit
class TestIsDatabaseOnlyValue:
    """Test cases for is_database_only_value function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_true(self, mock_get_mappings):
        """Test value that is database-only."""
        # database_only_values are values that appear in forward mappings but not in reverse
        # The value "-999" is in mappings (forward) but not in reverse_mappings
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {
                        "-999": "SomeValue"  # In forward mapping
                    },
                    "reverse_mappings": {
                        "SomeValue": "OtherValue"  # Reverse mapping exists but not for "-999"
                    }
                }
            }
        }
        
        result = is_database_only_value("library_selection_method", "-999")
        
        # "-999" is in mappings but not in reverse_mappings values, so it's database-only
        assert result is True

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_false(self, mock_get_mappings):
        """Test value that is not database-only."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "database_only_values": ["-999"]
                }
            }
        }
        
        result = is_database_only_value("library_selection_method", "PCR")
        
        assert result is False

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_field_not_found(self, mock_get_mappings):
        """Test database-only check for field not in config."""
        mock_get_mappings.return_value = {}
        
        result = is_database_only_value("unknown_field", "value")
        
        assert result is False

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_database_only_no_config(self, mock_get_mappings):
        """Test database-only check when config has no database_only_values."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {}
            }
        }
        
        result = is_database_only_value("library_selection_method", "value")
        
        assert result is False


@pytest.mark.unit
class TestFindFieldConfig:
    """Test cases for _find_field_config function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_find_existing_field(self, mock_get_mappings):
        """Test finding existing field config."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "mappings": {}
                }
            }
        }
        
        result = _find_field_config("library_selection_method")
        
        assert result is not None
        node_type, config = result
        assert node_type == "sample"
        assert "mappings" in config

    @patch('app.core.field_mappings._get_field_mappings')
    def test_find_nonexistent_field(self, mock_get_mappings):
        """Test finding nonexistent field."""
        mock_get_mappings.return_value = {
            "sample": {}
        }
        
        result = _find_field_config("unknown_field")
        
        assert result is None

    @patch('app.core.field_mappings._get_field_mappings')
    def test_find_field_in_multiple_nodes(self, mock_get_mappings):
        """Test finding field that exists in multiple node types."""
        mock_get_mappings.return_value = {
            "sample": {
                "field1": {}
            },
            "participant": {
                "field1": {}
            }
        }
        
        result = _find_field_config("field1")
        
        # Should return the first match
        assert result is not None
        node_type, _ = result
        assert node_type in ["sample", "participant"]


@pytest.mark.unit
class TestGetFieldMappings:
    """Test cases for _get_field_mappings function."""

    @patch('app.core.field_mappings._load_field_mappings')
    def test_get_cached_mappings(self, mock_load):
        """Test that mappings are cached."""
        import app.core.field_mappings
        app.core.field_mappings._field_mappings_cache = None
        mock_load.return_value = {"sample": {}}
        
        # First call should load
        result1 = _get_field_mappings()
        # Second call should use cache
        result2 = _get_field_mappings()
        
        assert result1 == result2
        # Should only load once
        assert mock_load.call_count == 1
        app.core.field_mappings._field_mappings_cache = None

    @patch('app.core.field_mappings._load_field_mappings')
    def test_get_mappings_loads_on_first_call(self, mock_load):
        """Test that mappings are loaded on first call."""
        # Reset the cache first
        import app.core.field_mappings
        app.core.field_mappings._field_mappings_cache = None
        
        mock_load.return_value = {"sample": {"field1": {}}}
        
        result = _get_field_mappings()
        
        assert result == {"sample": {"field1": {}}}
        mock_load.assert_called_once()
        
        # Reset cache for other tests
        app.core.field_mappings._field_mappings_cache = None


@pytest.mark.unit
class TestIsNullMappedValue:
    """Test cases for is_null_mapped_value function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_null_mapped_true(self, mock_get_mappings):
        """Test value that is in null_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "null_mappings": ["-999", "Not Reported"]
                }
            }
        }
        
        result = is_null_mapped_value("library_selection_method", "-999")
        
        assert result is True

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_null_mapped_false(self, mock_get_mappings):
        """Test value that is not in null_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "null_mappings": ["-999"]
                }
            }
        }
        
        result = is_null_mapped_value("library_selection_method", "PCR")
        
        assert result is False

    def test_is_null_mapped_none(self):
        """Test None value."""
        result = is_null_mapped_value("any_field", None)
        
        assert result is False

    def test_is_null_mapped_empty_string(self):
        """Test empty string."""
        result = is_null_mapped_value("any_field", "")
        
        assert result is False

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_null_mapped_field_not_found(self, mock_get_mappings):
        """Test null mapped check for field not in config."""
        mock_get_mappings.return_value = {}
        
        result = is_null_mapped_value("unknown_field", "value")
        
        assert result is False

    @patch('app.core.field_mappings._get_field_mappings')
    def test_is_null_mapped_no_null_mappings(self, mock_get_mappings):
        """Test null mapped check when config has no null_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {}
            }
        }
        
        result = is_null_mapped_value("library_selection_method", "value")
        
        assert result is False


@pytest.mark.unit
class TestGetNullMappings:
    """Test cases for get_null_mappings function."""

    @patch('app.core.field_mappings._get_field_mappings')
    def test_get_null_mappings_existing(self, mock_get_mappings):
        """Test getting null_mappings for existing field."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {
                    "null_mappings": ["-999", "Not Reported"]
                }
            }
        }
        
        result = get_null_mappings("library_selection_method")
        
        assert isinstance(result, list)
        assert "-999" in result
        assert "Not Reported" in result

    @patch('app.core.field_mappings._get_field_mappings')
    def test_get_null_mappings_field_not_found(self, mock_get_mappings):
        """Test getting null_mappings for field not in config."""
        mock_get_mappings.return_value = {}
        
        result = get_null_mappings("unknown_field")
        
        assert result == []

    @patch('app.core.field_mappings._get_field_mappings')
    def test_get_null_mappings_no_null_mappings(self, mock_get_mappings):
        """Test getting null_mappings when config has no null_mappings."""
        mock_get_mappings.return_value = {
            "sample": {
                "library_selection_method": {}
            }
        }
        
        result = get_null_mappings("library_selection_method")
        
        assert result == []


@pytest.mark.unit
class TestBuildInvalidValueFilter:
    """Test cases for build_invalid_value_filter function."""

    @patch('app.core.field_mappings.get_null_mappings')
    def test_build_invalid_value_filter_with_not_reported(self, mock_get_null):
        """Test building filter with 'Not Reported' in null_mappings."""
        mock_get_null.return_value = ["-999", "Not Reported"]
        
        result = build_invalid_value_filter("sf.library_strategy", "library_strategy")
        
        assert "sf.library_strategy IS NOT NULL" in result
        assert "sf.library_strategy <> ''" in result
        assert "sf.library_strategy <> '-999'" in result
        assert "sf.library_strategy <> 'Not Reported'" in result

    @patch('app.core.field_mappings.get_null_mappings')
    def test_build_invalid_value_filter_without_not_reported(self, mock_get_null):
        """Test building filter without 'Not Reported' in null_mappings."""
        mock_get_null.return_value = ["-999"]
        
        result = build_invalid_value_filter("sf.library_strategy", "library_strategy")
        
        assert "sf.library_strategy IS NOT NULL" in result
        assert "sf.library_strategy <> ''" in result
        assert "sf.library_strategy <> '-999'" in result
        assert "sf.library_strategy <> 'Not Reported'" not in result

    @patch('app.core.field_mappings.get_null_mappings')
    def test_build_invalid_value_filter_empty_null_mappings(self, mock_get_null):
        """Test building filter with empty null_mappings."""
        mock_get_null.return_value = []
        
        result = build_invalid_value_filter("sf.library_strategy", "library_strategy")
        
        assert "sf.library_strategy IS NOT NULL" in result
        assert "sf.library_strategy <> ''" in result
        assert "sf.library_strategy <> '-999'" in result
        assert "sf.library_strategy <> 'Not Reported'" not in result

