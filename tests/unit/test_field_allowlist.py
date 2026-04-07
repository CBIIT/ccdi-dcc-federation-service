"""
Unit tests for field allowlist management.

Tests the FieldAllowlist class and field validation logic.
"""

import pytest
from app.lib.field_allowlist import (
    FieldAllowlist,
    EntityType,
    HARMONIZED_FIELDS,
    COMMON_UNHARMONIZED_PATTERNS,
    get_field_allowlist
)


@pytest.mark.unit
class TestEntityType:
    """Test cases for EntityType enum."""

    def test_entity_type_values(self):
        """Test that all entity types are accessible."""
        assert EntityType.SUBJECT.value == "subject"
        assert EntityType.SAMPLE.value == "sample"
        assert EntityType.FILE.value == "file"


@pytest.mark.unit
class TestFieldAllowlist:
    """Test cases for FieldAllowlist class."""

    @pytest.fixture
    def allowlist(self):
        """Create a fresh FieldAllowlist instance for each test."""
        return FieldAllowlist()

    def test_initialization(self, allowlist):
        """Test that FieldAllowlist initializes correctly."""
        assert allowlist is not None
        assert allowlist._loaded is False

    def test_is_harmonized_field_allowed_subject(self, allowlist):
        """Test harmonized field validation for subject entity."""
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "sex") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "race") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "ethnicity") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "vital_status") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "invalid_field") is False

    def test_is_harmonized_field_allowed_sample(self, allowlist):
        """Test harmonized field validation for sample entity."""
        assert allowlist.is_harmonized_field_allowed(EntityType.SAMPLE, "disease_phase") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SAMPLE, "anatomical_sites") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SAMPLE, "diagnosis") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.SAMPLE, "invalid_field") is False

    def test_is_harmonized_field_allowed_file(self, allowlist):
        """Test harmonized field validation for file entity."""
        assert allowlist.is_harmonized_field_allowed(EntityType.FILE, "type") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.FILE, "size") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.FILE, "checksums") is True
        assert allowlist.is_harmonized_field_allowed(EntityType.FILE, "invalid_field") is False

    def test_is_unharmonized_field_allowed_before_load(self, allowlist):
        """Test unharmonized field validation before loading from database."""
        # Before loading, should check against common patterns
        assert allowlist.is_unharmonized_field_allowed(EntityType.SUBJECT, "study_id") is True
        assert allowlist.is_unharmonized_field_allowed(EntityType.SUBJECT, "patient_id") is True
        assert allowlist.is_unharmonized_field_allowed(EntityType.SUBJECT, "invalid_field") is False

    def test_is_unharmonized_field_allowed_after_load(self, allowlist):
        """Test unharmonized field validation after loading from database."""
        allowlist.load_from_database()
        assert allowlist._loaded is True
        # After loading, should still allow common patterns
        assert allowlist.is_unharmonized_field_allowed(EntityType.SUBJECT, "study_id") is True

    def test_is_field_allowed_harmonized(self, allowlist):
        """Test is_field_allowed with harmonized fields."""
        assert allowlist.is_field_allowed(EntityType.SUBJECT, "sex") is True
        assert allowlist.is_field_allowed(EntityType.SUBJECT, "race") is True
        assert allowlist.is_field_allowed(EntityType.SUBJECT, "invalid_field") is False

    def test_is_field_allowed_unharmonized(self, allowlist):
        """Test is_field_allowed with unharmonized fields."""
        # Test with metadata.unharmonized prefix
        assert allowlist.is_field_allowed(
            EntityType.SUBJECT, 
            "metadata.unharmonized.study_id"
        ) is True
        assert allowlist.is_field_allowed(
            EntityType.SUBJECT,
            "metadata.unharmonized.invalid_field"
        ) is False

    def test_get_allowed_harmonized_fields(self, allowlist):
        """Test getting all allowed harmonized fields."""
        subject_fields = allowlist.get_allowed_harmonized_fields(EntityType.SUBJECT)
        assert isinstance(subject_fields, list)
        assert "sex" in subject_fields
        assert "race" in subject_fields
        assert "ethnicity" in subject_fields
        assert len(subject_fields) == len(HARMONIZED_FIELDS[EntityType.SUBJECT])

        sample_fields = allowlist.get_allowed_harmonized_fields(EntityType.SAMPLE)
        assert isinstance(sample_fields, list)
        assert "disease_phase" in sample_fields
        assert len(sample_fields) == len(HARMONIZED_FIELDS[EntityType.SAMPLE])

    def test_get_allowed_unharmonized_fields_before_load(self, allowlist):
        """Test getting unharmonized fields before loading."""
        fields = allowlist.get_allowed_unharmonized_fields(EntityType.SUBJECT)
        assert isinstance(fields, list)
        assert "study_id" in fields
        assert len(fields) == len(COMMON_UNHARMONIZED_PATTERNS)

    def test_get_allowed_unharmonized_fields_after_load(self, allowlist):
        """Test getting unharmonized fields after loading."""
        allowlist.load_from_database()
        fields = allowlist.get_allowed_unharmonized_fields(EntityType.SUBJECT)
        assert isinstance(fields, list)
        # After loading, should include common patterns
        assert "study_id" in fields

    def test_add_harmonized_field(self, allowlist):
        """Test adding a harmonized field to allowlist."""
        allowlist.add_harmonized_field(EntityType.SUBJECT, "new_field")
        assert allowlist.is_harmonized_field_allowed(EntityType.SUBJECT, "new_field") is True
        assert "new_field" in allowlist.get_allowed_harmonized_fields(EntityType.SUBJECT)

    def test_add_unharmonized_field(self, allowlist):
        """Test adding an unharmonized field to allowlist."""
        # Need to mark as loaded first, otherwise it checks common patterns
        allowlist._loaded = True
        allowlist.add_unharmonized_field(EntityType.SUBJECT, "new_unharmonized_field")
        assert allowlist.is_unharmonized_field_allowed(
            EntityType.SUBJECT, 
            "new_unharmonized_field"
        ) is True

    def test_load_from_database(self, allowlist):
        """Test loading fields from database."""
        assert allowlist._loaded is False
        allowlist.load_from_database()
        assert allowlist._loaded is True

    def test_validate_count_field_valid(self, allowlist):
        """Test validate_count_field with valid field."""
        # Should not raise exception
        allowlist.validate_count_field(EntityType.SUBJECT, "sex")

    def test_validate_count_field_invalid(self, allowlist):
        """Test validate_count_field with invalid field."""
        with pytest.raises(ValueError, match="not supported"):
            allowlist.validate_count_field(EntityType.SUBJECT, "invalid_field")

    def test_validate_filter_field_valid(self, allowlist):
        """Test validate_filter_field with valid field."""
        # Should not raise exception
        allowlist.validate_filter_field(EntityType.SUBJECT, "sex")

    def test_validate_filter_field_invalid(self, allowlist):
        """Test validate_filter_field with invalid field."""
        with pytest.raises(ValueError, match="not supported"):
            allowlist.validate_filter_field(EntityType.SUBJECT, "invalid_field")


@pytest.mark.unit
class TestGetFieldAllowlist:
    """Test cases for get_field_allowlist function."""

    def test_get_field_allowlist_returns_instance(self):
        """Test that get_field_allowlist returns a FieldAllowlist instance."""
        allowlist = get_field_allowlist()
        assert isinstance(allowlist, FieldAllowlist)

    def test_get_field_allowlist_singleton(self):
        """Test that get_field_allowlist returns the same instance."""
        allowlist1 = get_field_allowlist()
        allowlist2 = get_field_allowlist()
        # Should be the same instance (singleton pattern)
        assert allowlist1 is allowlist2

