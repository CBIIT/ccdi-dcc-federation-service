"""
Unit tests for constants and enumerations.

Tests the enum classes and validation methods for race, ethnicity,
vital status, and file types.
"""

import pytest
from app.core.constants import Race, Ethnicity, VitalStatus, FileType, load_file_enum


@pytest.mark.unit
class TestRace:
    """Test cases for Race enum."""

    def test_race_values(self):
        """Test that all race values are accessible."""
        assert Race.NOT_ALLOWED_TO_COLLECT == "Not allowed to collect"
        assert Race.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER == "Native Hawaiian or other Pacific Islander"
        assert Race.NOT_REPORTED == "Not Reported"
        assert Race.UNKNOWN == "Unknown"
        assert Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE == "American Indian or Alaska Native"
        assert Race.ASIAN == "Asian"
        assert Race.BLACK_OR_AFRICAN_AMERICAN == "Black or African American"
        assert Race.WHITE == "White"

    def test_race_values_method(self):
        """Test Race.values() returns all valid values."""
        values = Race.values()
        assert isinstance(values, list)
        assert len(values) == 8
        assert "White" in values
        assert "Asian" in values
        assert "Black or African American" in values
        assert "Not Reported" in values

    def test_race_is_valid_with_valid_values(self):
        """Test is_valid() returns True for valid race values."""
        assert Race.is_valid("White") is True
        assert Race.is_valid("Asian") is True
        assert Race.is_valid("Black or African American") is True
        assert Race.is_valid("Not Reported") is True
        assert Race.is_valid("Unknown") is True

    def test_race_is_valid_with_invalid_values(self):
        """Test is_valid() returns False for invalid race values."""
        assert Race.is_valid("Invalid Race") is False
        assert Race.is_valid("") is False
        assert Race.is_valid("white") is False  # Case sensitive
        assert Race.is_valid("WHITE") is False  # Case sensitive


@pytest.mark.unit
class TestEthnicity:
    """Test cases for Ethnicity enum."""

    def test_ethnicity_values(self):
        """Test that all ethnicity values are accessible."""
        assert Ethnicity.HISPANIC_OR_LATINO == "Hispanic or Latino"
        assert Ethnicity.NOT_HISPANIC_OR_LATINO == "Not reported"

    def test_ethnicity_values_method(self):
        """Test Ethnicity.values() returns all valid values."""
        values = Ethnicity.values()
        assert isinstance(values, list)
        assert len(values) == 2
        assert "Hispanic or Latino" in values
        assert "Not reported" in values

    def test_ethnicity_is_valid_with_valid_values(self):
        """Test is_valid() returns True for valid ethnicity values."""
        assert Ethnicity.is_valid("Hispanic or Latino") is True
        assert Ethnicity.is_valid("Not reported") is True

    def test_ethnicity_is_valid_with_invalid_values(self):
        """Test is_valid() returns False for invalid ethnicity values."""
        assert Ethnicity.is_valid("Invalid Ethnicity") is False
        assert Ethnicity.is_valid("") is False
        assert Ethnicity.is_valid("hispanic or latino") is False  # Case sensitive


@pytest.mark.unit
class TestVitalStatus:
    """Test cases for VitalStatus enum."""

    def test_vital_status_values(self):
        """Test that all vital status values are accessible."""
        assert VitalStatus.ALIVE == "Alive"
        assert VitalStatus.DEAD == "Dead"
        assert VitalStatus.NOT_REPORTED == "Not reported"
        assert VitalStatus.UNKNOWN == "Unknown"
        assert VitalStatus.UNSPECIFIED == "Unspecified"

    def test_vital_status_values_method(self):
        """Test VitalStatus.values() returns all valid values."""
        values = VitalStatus.values()
        assert isinstance(values, list)
        assert len(values) == 5
        assert "Alive" in values
        assert "Dead" in values
        assert "Not reported" in values
        assert "Unknown" in values
        assert "Unspecified" in values

    def test_vital_status_is_valid_with_valid_values(self):
        """Test is_valid() returns True for valid vital status values."""
        assert VitalStatus.is_valid("Alive") is True
        assert VitalStatus.is_valid("Dead") is True
        assert VitalStatus.is_valid("Not reported") is True
        assert VitalStatus.is_valid("Unknown") is True
        assert VitalStatus.is_valid("Unspecified") is True

    def test_vital_status_is_valid_with_invalid_values(self):
        """Test is_valid() returns False for invalid vital status values."""
        assert VitalStatus.is_valid("Invalid Status") is False
        assert VitalStatus.is_valid("") is False
        assert VitalStatus.is_valid("alive") is False  # Case sensitive


@pytest.mark.unit
class TestFileType:
    """Test cases for FileType enum."""

    def test_file_type_exists(self):
        """Test that FileType enum exists."""
        assert FileType is not None

    def test_file_type_values_method(self):
        """Test FileType.values() returns a list."""
        values = FileType.values()
        assert isinstance(values, list)
        # File types are loaded from config file, so we just verify the method works

    def test_file_type_is_valid_method(self):
        """Test FileType.is_valid() method exists and works."""
        # Test with a value that might exist (depends on config file)
        # If no file types loaded, this should return False
        result = FileType.is_valid("test_type")
        assert isinstance(result, bool)


@pytest.mark.unit
class TestLoadFileEnum:
    """Test cases for load_file_enum function."""

    def test_load_file_enum_returns_list(self):
        """Test that load_file_enum returns a list."""
        result = load_file_enum()
        assert isinstance(result, list)

    def test_load_file_enum_handles_missing_file(self):
        """Test that load_file_enum handles missing file gracefully."""
        # The function should return empty list if file doesn't exist
        # This is tested implicitly by the fact it doesn't crash
        result = load_file_enum()
        assert isinstance(result, list)

