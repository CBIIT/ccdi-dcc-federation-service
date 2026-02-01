"""
Additional unit tests for deps.py to improve coverage.

Tests missing edge cases and error paths.
"""

import pytest
from unittest.mock import Mock, MagicMock
from fastapi import Request
from app.api.v1.deps import (
    get_subject_filters,
    get_subject_summary_filters,
    get_sample_filters,
    get_database_session,
    get_app_settings,
    get_allowlist,
)
from app.models.errors import InvalidParametersError


@pytest.mark.unit
class TestGetSubjectFiltersCoverage:
    """Test cases for get_subject_filters to improve coverage."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        request.query_params.getlist = Mock(return_value=[])
        return request

    def test_get_subject_filters_age_at_vital_status_negative(self, mock_request):
        """Test age_at_vital_status with negative value."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="-10",
            depositions=None,
            request=mock_request
        )
        
        assert "_invalid_age_at_vital_status" in result
        assert result["_invalid_age_at_vital_status"] == "-10"
        assert "_age_at_vital_status_reason" in result

    def test_get_subject_filters_age_at_vital_status_too_large(self, mock_request):
        """Test age_at_vital_status with value exceeding maximum."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="80000",  # > 73000 days
            depositions=None,
            request=mock_request
        )
        
        assert "_invalid_age_at_vital_status" in result
        assert result["_invalid_age_at_vital_status"] == "80000"

    def test_get_subject_filters_age_at_vital_status_invalid_format(self, mock_request):
        """Test age_at_vital_status with invalid integer format."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="not_a_number",
            depositions=None,
            request=mock_request
        )
        
        assert "_invalid_age_at_vital_status" in result
        assert result["_invalid_age_at_vital_status"] == "not_a_number"
        assert "_age_at_vital_status_reason" in result

    def test_get_subject_filters_age_at_vital_status_empty_string(self, mock_request):
        """Test age_at_vital_status with empty string."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="   ",
            depositions=None,
            request=mock_request
        )
        
        # Empty string should not be added to filters
        assert "age_at_vital_status" not in result
        assert "_invalid_age_at_vital_status" not in result

    def test_get_subject_filters_identifiers_with_empty_parts(self, mock_request):
        """Test identifiers with empty parts after splitting."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="id1||  ||id2||",
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        # Should filter out empty parts
        assert "identifiers" in result
        assert isinstance(result["identifiers"], list)
        assert "id1" in result["identifiers"]
        assert "id2" in result["identifiers"]
        assert "" not in result["identifiers"]

    def test_get_subject_filters_unharmonized_fields(self, mock_request):
        """Test unharmonized fields are included."""
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.field1", "value1"),
            ("metadata.unharmonized.field2", "value2"),
        ])
        
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        assert "metadata.unharmonized.field1" in result
        assert result["metadata.unharmonized.field1"] == "value1"
        assert "metadata.unharmonized.field2" in result
        assert result["metadata.unharmonized.field2"] == "value2"


@pytest.mark.unit
class TestGetSubjectSummaryFiltersCoverage:
    """Test cases for get_subject_summary_filters to improve coverage."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        request.query_params.getlist = Mock(return_value=[])
        return request

    def test_get_subject_summary_filters_unknown_parameters(self, mock_request):
        """Test get_subject_summary_filters with unknown parameters."""
        mock_request.query_params.keys = Mock(return_value=["unknown_param", "sex"])
        
        result = get_subject_summary_filters(
            sex="M",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        assert "_unknown_parameters" in result
        assert "unknown_param" in result["_unknown_parameters"]

    def test_get_subject_summary_filters_unharmonized_allowed(self, mock_request):
        """Test that unharmonized fields are allowed in summary filters."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.field1"])
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.field1", "value1"),
        ])
        
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        # Should not have unknown_parameters error
        assert "_unknown_parameters" not in result

    def test_get_subject_summary_filters_race_with_url_encoding(self, mock_request):
        """Test race filter with URL-encoded || delimiter."""
        result = get_subject_summary_filters(
            sex=None,
            race="Asian%7C%7CWhite",  # URL-encoded ||
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        # Should decode %7C%7C to ||
        assert "race" in result
        assert isinstance(result["race"], list)
        assert "Asian" in result["race"]
        assert "White" in result["race"]

    def test_get_subject_summary_filters_race_partially_invalid(self, mock_request):
        """Test race filter with some valid and some invalid values."""
        from app.core.constants import Race
        
        # Use a mix of valid and invalid race values
        valid_race = list(Race)[0].value
        invalid_race = f"InvalidRace||{valid_race}"
        
        result = get_subject_summary_filters(
            sex=None,
            race=invalid_race,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        
        # Should filter out invalid races and keep only valid ones
        assert "race" in result
        # If only one valid race remains, it might be a string, not a list
        if isinstance(result["race"], list):
            assert len(result["race"]) == 1
            assert result["race"][0] == valid_race
            assert all(r in Race.values() for r in result["race"])
        else:
            # Single valid race is stored as string
            assert result["race"] == valid_race
            assert result["race"] in Race.values()


@pytest.mark.unit
class TestGetSampleFiltersCoverage:
    """Test cases for get_sample_filters to improve coverage."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        request.query_params.getlist = Mock(return_value=[])
        return request

    def test_get_sample_filters_duplicate_parameters(self, mock_request):
        """Test get_sample_filters with duplicate parameters."""
        # Mock getlist to return multiple values for a parameter
        def mock_getlist(param):
            if param == "identifiers":
                return ["id1", "id2"]
            return []
        
        mock_request.query_params.getlist = mock_getlist
        
        with pytest.raises(InvalidParametersError) as exc_info:
            get_sample_filters(
                disease_phase=None,
                anatomical_sites=None,
                library_selection_method=None,
                library_strategy=None,
                library_source_material=None,
                preservation_method=None,
                tumor_grade=None,
                specimen_molecular_analyte_type=None,
                tissue_type=None,
                tumor_classification=None,
                age_at_diagnosis=None,
                age_at_collection=None,
                tumor_tissue_morphology=None,
                depositions=None,
                diagnosis=None,
                identifiers=None,
                request=mock_request
            )
        
        assert "Duplicate parameters" in str(exc_info.value.reason)

    def test_get_sample_filters_anatomical_site_singular(self, mock_request):
        """Test get_sample_filters rejects singular 'anatomical_site'."""
        mock_request.query_params.items = Mock(return_value=[
            ("anatomical_site", "value1"),  # Singular form - should be rejected
        ])
        
        with pytest.raises(InvalidParametersError):
            get_sample_filters(
                disease_phase=None,
                anatomical_sites=None,
                library_selection_method=None,
                library_strategy=None,
                library_source_material=None,
                preservation_method=None,
                tumor_grade=None,
                specimen_molecular_analyte_type=None,
                tissue_type=None,
                tumor_classification=None,
                age_at_diagnosis=None,
                age_at_collection=None,
                tumor_tissue_morphology=None,
                depositions=None,
                diagnosis=None,
                identifiers=None,
                request=mock_request
            )

    def test_get_sample_filters_identifiers_empty_after_split(self, mock_request):
        """Test identifiers with only empty parts after splitting."""
        result = get_sample_filters(
            disease_phase=None,
            anatomical_sites=None,
            library_selection_method=None,
            library_strategy=None,
            library_source_material=None,
            preservation_method=None,
            tumor_grade=None,
            specimen_molecular_analyte_type=None,
            tissue_type=None,
            tumor_classification=None,
            age_at_diagnosis=None,
            age_at_collection=None,
            tumor_tissue_morphology=None,
            depositions=None,
            diagnosis=None,
            identifiers="  ||  ||  ",
            request=mock_request
        )
        
        # Should not add identifiers if all parts are empty
        assert "identifiers" not in result

    def test_get_sample_filters_empty_string_filters_excluded(self, mock_request):
        """Test that empty string filters are excluded."""
        result = get_sample_filters(
            disease_phase="",
            anatomical_sites="   ",
            library_selection_method="",
            library_strategy=None,
            library_source_material=None,
            preservation_method=None,
            tumor_grade=None,
            specimen_molecular_analyte_type=None,
            tissue_type=None,
            tumor_classification=None,
            age_at_diagnosis=None,
            age_at_collection=None,
            tumor_tissue_morphology=None,
            depositions=None,
            diagnosis=None,
            identifiers=None,
            request=mock_request
        )
        
        # Empty strings should not be added to filters
        assert "disease_phase" not in result
        assert "anatomical_sites" not in result
        assert "library_selection_method" not in result

    def test_get_sample_filters_unharmonized_fields(self, mock_request):
        """Test unharmonized fields are included."""
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.field1", "value1"),
        ])
        
        result = get_sample_filters(
            disease_phase=None,
            anatomical_sites=None,
            library_selection_method=None,
            library_strategy=None,
            library_source_material=None,
            preservation_method=None,
            tumor_grade=None,
            specimen_molecular_analyte_type=None,
            tissue_type=None,
            tumor_classification=None,
            age_at_diagnosis=None,
            age_at_collection=None,
            tumor_tissue_morphology=None,
            depositions=None,
            diagnosis=None,
            identifiers=None,
            request=mock_request
        )
        
        assert "metadata.unharmonized.field1" in result
        assert result["metadata.unharmonized.field1"] == "value1"


@pytest.mark.unit
class TestCoreDependenciesCoverage:
    """Test cases for core dependencies to improve coverage."""

    def test_get_app_settings(self):
        """Test get_app_settings returns settings."""
        settings = get_app_settings()
        assert settings is not None

    def test_get_allowlist(self):
        """Test get_allowlist returns allowlist."""
        allowlist = get_allowlist()
        assert allowlist is not None

    async def test_get_database_session(self):
        """Test get_database_session is a generator."""
        # This is an async generator, so we need to test it properly
        session_gen = get_database_session()
        assert hasattr(session_gen, '__aiter__')
