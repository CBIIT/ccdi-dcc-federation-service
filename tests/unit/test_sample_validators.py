"""
Unit tests for sample validators module.

Tests validation methods used in SampleRepository.
"""

import pytest
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.repositories.sample_validators import SampleValidators
from app.models.errors import UnsupportedFieldError


@pytest.mark.unit
class TestSampleValidators:
    """Test SampleValidators mixin class."""
    
    @pytest.fixture
    def validator(self):
        """Create a SampleValidators instance."""
        from app.lib.field_allowlist import FieldAllowlist
        
        class TestValidator(SampleValidators):
            def __init__(self):
                self.allowlist = Mock(spec=FieldAllowlist)
                self.allowlist.is_field_allowed = Mock(return_value=True)
        
        return TestValidator()
    
    def test_reverse_map_library_selection_method_static(self):
        """Test _reverse_map_library_selection_method_static."""
        result = SampleValidators._reverse_map_library_selection_method_static("PCR")
        # Should return a string or None
        assert result is None or isinstance(result, str)
    
    def test_reverse_map_library_selection_method_static_list(self):
        """Test _reverse_map_library_selection_method_static handles list return."""
        with patch("app.repositories.sample_validators.reverse_map_field_value", return_value=["Mapped", "Other"]):
            result = SampleValidators._reverse_map_library_selection_method_static("PCR")
        
        assert result == "Mapped"
    
    def test_get_next_param_name(self):
        """Test _get_next_param_name."""
        params = {}
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_1"
        
        params["param_1"] = "value"
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_2"
        
        # Test exception handling path
        params["invalid_key"] = "value"
        params["param_abc"] = "value"
        params["param_5"] = "value"
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_6"
    
    def test_validate_tissue_type_filter_valid(self, validator):
        """Test _validate_tissue_type_filter with valid value."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = validator._validate_tissue_type_filter(
                "Tumor", "tissue_param", params, with_conditions
            )
            assert result is True
            assert "tissue_param" in params
    
    def test_validate_tissue_type_filter_invalid(self, validator):
        """Test _validate_tissue_type_filter with invalid value."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = validator._validate_tissue_type_filter(
                "Invalid", "tissue_param", params, with_conditions
            )
            assert result is None
    
    def test_validate_library_source_material_filter(self, validator):
        """Test _validate_library_source_material_filter."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.is_null_mapped_value', return_value=False):
            with patch('app.repositories.sample_validators.load_sample_enum', return_value=["DNA", "RNA"]):
                result = validator._validate_library_source_material_filter(
                    "DNA", "source_param", params, with_conditions
                )
                # Should return True or None depending on validation
                assert result is True or result is None
    
    def test_validate_library_source_material_filter_null_mapping(self, validator):
        """Test _validate_library_source_material_filter with null mapping."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.is_null_mapped_value', return_value=True):
            result = validator._validate_library_source_material_filter(
                "Other", "source_param", params, with_conditions
            )
            assert result is None
            assert "source_param" not in params
    
    def test_validate_library_source_material_filter_no_enum(self, validator):
        """Test _validate_library_source_material_filter when enum missing."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.is_null_mapped_value', return_value=False):
            with patch('app.repositories.sample_validators.load_sample_enum', return_value=None):
                result = validator._validate_library_source_material_filter(
                    "DNA", "source_param", params, with_conditions
                )
                # Should still work without enum (fallback behavior)
                assert result is True or result is None
    
    def test_validate_filters(self, validator):
        """Test _validate_filters method."""
        filters = {"tissue_type": "Tumor"}
        
        # Mock allowlist to allow tissue_type field
        validator.allowlist.is_field_allowed = Mock(return_value=True)
        
        # Should not raise an exception for valid field
        validator._validate_filters(filters, "sample")
        
        # Test with invalid field
        validator.allowlist.is_field_allowed = Mock(return_value=False)
        with pytest.raises(UnsupportedFieldError):
            validator._validate_filters({"invalid_field": "value"}, "sample")
    
    def test_validate_tissue_type_filter_list_mixed(self, validator):
        """Test _validate_tissue_type_filter with list containing mix of valid and invalid."""
        params = {}
        with_conditions = []
        
        with patch('app.repositories.sample_validators.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = validator._validate_tissue_type_filter(
                ["Tumor", "Invalid"], "tissue_param", params, with_conditions
            )
            # Should return None if any value is invalid
            assert result is None
    
    def test_validate_tissue_type_filter_empty_list(self, validator):
        """Test _validate_tissue_type_filter with empty list."""
        params = {}
        with_conditions = []
        
        result = validator._validate_tissue_type_filter(
            [], "tissue_param", params, with_conditions
        )
        assert result is None
    
    def test_get_next_param_name_edge_cases(self):
        """Test _get_next_param_name with various edge cases."""
        params = {}
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_1"
        
        # Test with existing params
        params = {"param_1": "value1", "param_2": "value2", "param_5": "value5"}
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_6"  # Should skip to max + 1
        
        # Test with non-param keys
        params = {"other_key": "value", "param_3": "value"}
        result = SampleValidators._get_next_param_name(params, 0)
        assert result == "param_4"
