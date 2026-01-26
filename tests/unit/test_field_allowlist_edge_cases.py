"""
Unit tests for field allowlist edge cases.

Tests missing lines in field_allowlist.py for better coverage.
"""

import pytest
from app.lib.field_allowlist import FieldAllowlist, EntityType


@pytest.mark.unit
class TestFieldAllowlistEdgeCases:
    """Test edge cases in field allowlist."""

    def test_add_harmonized_field_new_entity_type(self):
        """Test add_harmonized_field when entity_type not in _harmonized_fields."""
        allowlist = FieldAllowlist()
        
        # Test line 175 - when entity_type not in dict
        allowlist.add_harmonized_field(EntityType.SUBJECT, "new_field")
        
        assert EntityType.SUBJECT in allowlist._harmonized_fields
        assert "new_field" in allowlist._harmonized_fields[EntityType.SUBJECT]

    def test_add_unharmonized_field_new_entity_type(self):
        """Test add_unharmonized_field when entity_type not in _unharmonized_fields."""
        allowlist = FieldAllowlist()
        
        # Test line 189 - when entity_type not in dict
        allowlist.add_unharmonized_field(EntityType.SAMPLE, "new_unharmonized_field")
        
        assert EntityType.SAMPLE in allowlist._unharmonized_fields
        assert "new_unharmonized_field" in allowlist._unharmonized_fields[EntityType.SAMPLE]

