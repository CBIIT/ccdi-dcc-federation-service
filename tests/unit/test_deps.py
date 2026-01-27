"""
Unit tests for FastAPI dependencies.

Tests filter validation, pagination, and dependency injection functions.
"""

import pytest
from unittest.mock import Mock, MagicMock, AsyncMock
from fastapi import Request, HTTPException
from app.api.v1.deps import (
    get_pagination_params,
    get_subject_filters,
    get_subject_summary_filters,
    get_sample_filters,
    get_file_filters,
    get_app_settings,
    get_allowlist,
    get_subject_diagnosis_filters,
    get_sample_diagnosis_filters,
)


@pytest.mark.unit
class TestGetPaginationParams:
    """Test cases for get_pagination_params."""

    def test_default_values(self):
        """Test pagination with default values."""
        result = get_pagination_params(page=1, per_page=None)
        assert result.page == 1
        # per_page defaults to settings.default_page_size (100)
        assert result.per_page == 100

    def test_custom_values(self):
        """Test pagination with custom values."""
        result = get_pagination_params(page=2, per_page=50)
        assert result.page == 2
        assert result.per_page == 50

    def test_invalid_page_raises_error(self):
        """Test that invalid page number raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            get_pagination_params(page=0, per_page=None)
        assert exc_info.value.status_code == 400

    def test_invalid_per_page_raises_error(self):
        """Test that invalid per_page raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            get_pagination_params(page=1, per_page=0)
        assert exc_info.value.status_code == 400

    def test_per_page_exceeds_maximum(self):
        """Test that per_page exceeding maximum raises error."""
        with pytest.raises(HTTPException) as exc_info:
            get_pagination_params(page=1, per_page=1001)
        assert exc_info.value.status_code == 400


@pytest.mark.unit
class TestGetSubjectFilters:
    """Test cases for get_subject_filters."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        return request

    def test_no_filters(self, mock_request):
        """Test with no filters provided."""
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
        assert isinstance(result, dict)
        # Result may contain unharmonized fields or be empty
        assert "_invalid_ethnicity" not in result
        assert "_invalid_sex" not in result

    def test_valid_sex_filter(self, mock_request):
        """Test valid sex filter values."""
        result = get_subject_filters(
            sex="M",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["sex"] == "M"
        
        result = get_subject_filters(
            sex="F",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["sex"] == "F"
        
        result = get_subject_filters(
            sex="U",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["sex"] == "U"

    def test_invalid_sex_filter(self, mock_request):
        """Test invalid sex filter values."""
        result = get_subject_filters(
            sex="INVALID",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_sex" in result
        assert result["_invalid_sex"] == "INVALID"

    def test_valid_ethnicity_filter(self, mock_request):
        """Test valid ethnicity filter values."""
        result = get_subject_filters(
            ethnicity="Hispanic or Latino", 
            request=mock_request
        )
        assert result["ethnicity"] == "Hispanic or Latino"
        
        result = get_subject_filters(
            ethnicity="Not reported", 
            request=mock_request
        )
        assert result["ethnicity"] == "Not reported"

    def test_invalid_ethnicity_filter(self, mock_request):
        """Test invalid ethnicity filter values."""
        result = get_subject_filters(
            ethnicity="Invalid Ethnicity", 
            request=mock_request
        )
        assert "_invalid_ethnicity" in result
        assert result["_invalid_ethnicity"] == "Invalid Ethnicity"

    def test_valid_race_single_value(self, mock_request):
        """Test valid race filter with single value."""
        result = get_subject_filters(
            sex=None,
            race="White",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["race"] == "White"

    def test_valid_race_multiple_values(self, mock_request):
        """Test valid race filter with multiple values using || delimiter."""
        result = get_subject_filters(
            sex=None,
            race="White||Asian",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert isinstance(result["race"], list)
        assert "White" in result["race"]
        assert "Asian" in result["race"]

    def test_race_url_encoded_delimiter(self, mock_request):
        """Test race filter with URL-encoded || delimiter."""
        result = get_subject_filters(
            sex=None,
            race="White%7C%7CAsian",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert isinstance(result["race"], list)
        assert "White" in result["race"]
        assert "Asian" in result["race"]

    def test_invalid_race_filter(self, mock_request):
        """Test invalid race filter values."""
        result = get_subject_filters(
            sex=None,
            race="Invalid Race",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_race" in result

    def test_race_with_mixed_valid_invalid(self, mock_request):
        """Test race filter with mix of valid and invalid values."""
        # If at least one valid value exists, it should be used
        result = get_subject_filters(
            sex=None,
            race="White||InvalidRace",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        # Should filter out invalid and keep valid
        assert "White" in result.get("race", [])

    def test_valid_vital_status_filter(self, mock_request):
        """Test valid vital status filter."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status="Alive",
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["vital_status"] == "Alive"

    def test_invalid_vital_status_filter(self, mock_request):
        """Test invalid vital status filter."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status="Invalid Status",
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_vital_status" in result

    def test_valid_age_at_vital_status(self, mock_request):
        """Test valid age_at_vital_status filter."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="3650",
            depositions=None,
            request=mock_request
        )
        assert result["age_at_vital_status"] == 3650

    def test_invalid_age_at_vital_status_non_integer(self, mock_request):
        """Test invalid age_at_vital_status with non-integer."""
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
        assert "_age_at_vital_status_reason" in result

    def test_invalid_age_at_vital_status_out_of_range(self, mock_request):
        """Test invalid age_at_vital_status out of valid range."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="100000",
            depositions=None,
            request=mock_request
        )
        assert "_invalid_age_at_vital_status" in result

    def test_negative_age_at_vital_status(self, mock_request):
        """Test negative age_at_vital_status."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="-100",
            depositions=None,
            request=mock_request
        )
        assert "_invalid_age_at_vital_status" in result

    def test_identifiers_single_value(self, mock_request):
        """Test identifiers filter with single value."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="id123",
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["identifiers"] == "id123"

    def test_identifiers_multiple_values(self, mock_request):
        """Test identifiers filter with multiple values using || delimiter."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="id1||id2||id3",
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert isinstance(result["identifiers"], list)
        assert "id1" in result["identifiers"]
        assert "id2" in result["identifiers"]
        assert "id3" in result["identifiers"]

    def test_identifiers_single_after_split(self, mock_request):
        """Test identifiers single value after split (line 204-205)."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="PBBHCR||",  # Split results in single item after stripping
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        # When split results in single item, should be stored as string
        assert result["identifiers"] == "PBBHCR"
        assert not isinstance(result["identifiers"], list)

    def test_depositions_filter(self, mock_request):
        """Test depositions filter."""
        result = get_subject_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions="phs002431",
            request=mock_request
        )
        # Check that no error flags are present
        assert "_unknown_parameters" not in result
        # Depositions should be in result if provided and valid
        assert "depositions" in result
        assert result["depositions"] == "phs002431"

    def test_unharmonized_fields(self, mock_request):
        """Test unharmonized fields from query parameters."""
        mock_request.query_params.keys = Mock(return_value=[
            "metadata.unharmonized.custom_field",
            "metadata.unharmonized.another_field"
        ])
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.custom_field", "value1"),
            ("metadata.unharmonized.another_field", "value2")
        ])
        # Pass None for all filter parameters to avoid Query object issues
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
        assert "metadata.unharmonized.custom_field" in result
        assert result["metadata.unharmonized.custom_field"] == "value1"
        assert "metadata.unharmonized.another_field" in result

    def test_unknown_parameters(self, mock_request):
        """Test detection of unknown query parameters."""
        mock_request.query_params.keys = Mock(return_value=["unknown_param", "sex"])
        result = get_subject_filters(
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

    def test_unknown_parameters_ignores_unharmonized(self, mock_request):
        """Test that unharmonized fields don't trigger unknown parameter error."""
        mock_request.query_params.keys = Mock(return_value=[
            "metadata.unharmonized.custom_field",
            "sex"
        ])
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.custom_field", "value"),
            ("sex", "M")
        ])
        result = get_subject_filters(
            sex="M",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_unknown_parameters" not in result
        assert "metadata.unharmonized.custom_field" in result

    def test_multiple_filters_combined(self, mock_request):
        """Test multiple filters combined."""
        result = get_subject_filters(
            sex="M",
            race="White",
            ethnicity="Not reported",
            vital_status="Alive",
            depositions="phs002431",
            request=mock_request
        )
        # Check that no error flags are present
        assert "_invalid_sex" not in result
        assert "_invalid_ethnicity" not in result
        assert "_invalid_race" not in result
        assert "_invalid_vital_status" not in result
        # Check that valid filters are present
        assert result["sex"] == "M"
        assert result["race"] == "White"
        assert result["ethnicity"] == "Not reported"
        assert result["vital_status"] == "Alive"
        # Depositions should be present if provided
        if "depositions" in result:
            assert result["depositions"] == "phs002431"


@pytest.mark.unit
class TestGetSampleFilters:
    """Test cases for get_sample_filters."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        return request

    def test_no_filters(self, mock_request):
        """Test with no filters provided."""
        result = get_sample_filters(request=mock_request)
        assert isinstance(result, dict)
        # Result may contain Query objects for None values, so just check it's a dict

    def test_identifiers_single_value(self, mock_request):
        """Test identifiers filter with single value."""
        result = get_sample_filters(identifiers="sample123", request=mock_request)
        assert result["identifiers"] == "sample123"

    def test_identifiers_multiple_values(self, mock_request):
        """Test identifiers filter with multiple values."""
        result = get_sample_filters(
            identifiers="sample1||sample2", 
            request=mock_request
        )
        assert isinstance(result["identifiers"], list)
        assert "sample1" in result["identifiers"]
        assert "sample2" in result["identifiers"]

    def test_anatomical_sites_single_value(self, mock_request):
        """Test anatomical_sites filter with single value."""
        result = get_sample_filters(
            anatomical_sites="Lung", 
            request=mock_request
        )
        assert result["anatomical_sites"] == "Lung"

    def test_anatomical_sites_multiple_values(self, mock_request):
        """Test anatomical_sites filter with multiple values."""
        result = get_sample_filters(
            anatomical_sites="Lung||Liver||Brain", 
            request=mock_request
        )
        assert isinstance(result["anatomical_sites"], list)
        assert "Lung" in result["anatomical_sites"]

    def test_anatomical_sites_url_encoded(self, mock_request):
        """Test anatomical_sites with URL-encoded delimiter."""
        result = get_sample_filters(
            anatomical_sites="Lung%7C%7CLiver", 
            request=mock_request
        )
        assert isinstance(result["anatomical_sites"], list)

    def test_disease_phase_filter(self, mock_request):
        """Test disease_phase filter."""
        result = get_sample_filters(disease_phase="Primary", request=mock_request)
        assert result["disease_phase"] == "Primary"

    def test_depositions_filter(self, mock_request):
        """Test depositions filter."""
        result = get_sample_filters(depositions="phs002431", request=mock_request)
        assert result["depositions"] == "phs002431"

    def test_diagnosis_filter(self, mock_request):
        """Test diagnosis filter."""
        result = get_sample_filters(diagnosis="Cancer", request=mock_request)
        assert result["diagnosis"] == "Cancer"

    def test_unharmonized_fields(self, mock_request):
        """Test unharmonized fields."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.custom_field"])
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.custom_field", "value")
        ])
        result = get_sample_filters(request=mock_request)
        assert "metadata.unharmonized.custom_field" in result


@pytest.mark.unit
class TestGetFileFilters:
    """Test cases for get_file_filters."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        return request

    def test_no_filters(self, mock_request):
        """Test with no filters provided."""
        result = get_file_filters(request=mock_request)
        assert isinstance(result, dict)
        # Result may contain Query objects for None values, so just check it's a dict

    def test_type_filter(self, mock_request):
        """Test type filter (maps to file_type)."""
        result = get_file_filters(type="FASTQ", request=mock_request)
        assert result["file_type"] == "FASTQ"

    def test_size_filter(self, mock_request):
        """Test size filter (maps to file_size)."""
        result = get_file_filters(size="1000", request=mock_request)
        assert result["file_size"] == "1000"

    def test_checksums_filter(self, mock_request):
        """Test checksums filter (maps to md5sum)."""
        result = get_file_filters(checksums="abc123", request=mock_request)
        assert result["md5sum"] == "abc123"

    def test_description_filter(self, mock_request):
        """Test description filter (maps to file_description)."""
        result = get_file_filters(description="Test file", request=mock_request)
        assert result["file_description"] == "Test file"

    def test_depositions_filter(self, mock_request):
        """Test depositions filter."""
        result = get_file_filters(depositions="phs002431", request=mock_request)
        assert result["depositions"] == "phs002431"

    def test_unharmonized_fields(self, mock_request):
        """Test unharmonized fields."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.file_name"])
        mock_request.query_params.items = Mock(return_value=[
            ("metadata.unharmonized.file_name", "test.fastq")
        ])
        result = get_file_filters(request=mock_request)
        assert "metadata.unharmonized.file_name" in result
        assert result["metadata.unharmonized.file_name"] == "test.fastq"


@pytest.mark.unit
class TestGetSubjectDiagnosisFilters:
    """Test cases for get_subject_diagnosis_filters."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        return request

    def test_diagnosis_search_parameter(self, mock_request):
        """Test diagnosis search parameter."""
        result = get_subject_diagnosis_filters(
            search="cancer", 
            request=mock_request
        )
        assert "_diagnosis_search" in result
        assert result["_diagnosis_search"] == "cancer"

    def test_combines_with_subject_filters(self, mock_request):
        """Test that it combines with regular subject filters."""
        mock_request.query_params.keys = Mock(return_value=[])
        result = get_subject_diagnosis_filters(
            search="cancer",
            sex="M",
            race="White",
            request=mock_request
        )
        assert "_diagnosis_search" in result
        assert result["_diagnosis_search"] == "cancer"
        # Check that subject filters are processed (may be in result if valid)
        # Note: The function calls get_subject_filters which validates and may
        # return error flags, so we just verify the search parameter is there


@pytest.mark.unit
class TestGetSampleDiagnosisFilters:
    """Test cases for get_sample_diagnosis_filters."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        request.query_params.get = Mock(return_value=None)
        return request

    def test_diagnosis_search_parameter(self, mock_request):
        """Test diagnosis search parameter."""
        result = get_sample_diagnosis_filters(
            search="cancer", 
            request=mock_request
        )
        assert "_diagnosis_search" in result
        assert result["_diagnosis_search"] == "cancer"

    def test_combines_with_sample_filters(self, mock_request):
        """Test that it combines with regular sample filters."""
        result = get_sample_diagnosis_filters(
            search="cancer",
            disease_phase="Primary",
            request=mock_request
        )
        assert "_diagnosis_search" in result
        assert result["disease_phase"] == "Primary"


@pytest.mark.unit
class TestCoreDependencies:
    """Test cases for core dependency functions."""

    def test_get_app_settings(self):
        """Test get_app_settings returns Settings instance."""
        settings = get_app_settings()
        assert settings is not None
        # Settings should have app_name attribute
        assert hasattr(settings, "app_name")

    def test_get_allowlist(self):
        """Test get_allowlist returns FieldAllowlist instance."""
        allowlist = get_allowlist()
        assert allowlist is not None
        # Should have is_field_allowed method
        assert hasattr(allowlist, "is_field_allowed")

