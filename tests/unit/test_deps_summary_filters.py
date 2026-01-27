"""
Unit tests for get_subject_summary_filters and other uncovered filter functions.

Tests filter parsing logic for summary endpoints and count endpoints.
"""

import pytest
from unittest.mock import Mock
from fastapi import Request
from app.api.v1.deps import (
    get_subject_summary_filters,
    get_sample_filters_no_descriptions,
    get_file_filters_no_descriptions,
)


@pytest.mark.unit
class TestGetSubjectSummaryFilters:
    """Test cases for get_subject_summary_filters function."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        return request

    def test_no_filters(self, mock_request):
        """Test with no filters provided."""
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
        assert isinstance(result, dict)
        assert result == {}

    def test_valid_sex_filter(self, mock_request):
        """Test valid sex filter values."""
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
        assert result["sex"] == "M"

        result = get_subject_summary_filters(
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

        result = get_subject_summary_filters(
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
        result = get_subject_summary_filters(
            sex="X",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_sex" in result
        assert result["_invalid_sex"] == "X"

    def test_valid_ethnicity_filter(self, mock_request):
        """Test valid ethnicity filter values."""
        from app.core.constants import Ethnicity
        
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity="Not reported",
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["ethnicity"] == "Not reported"

        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity="Hispanic or Latino",
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["ethnicity"] == "Hispanic or Latino"

    def test_invalid_ethnicity_filter(self, mock_request):
        """Test invalid ethnicity filter values."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity="Invalid",
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_ethnicity" in result
        assert result["_invalid_ethnicity"] == "Invalid"

    def test_valid_race_filter_single(self, mock_request):
        """Test valid single race filter."""
        from app.core.constants import Race
        
        result = get_subject_summary_filters(
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

    def test_valid_race_filter_multiple(self, mock_request):
        """Test valid multiple race filter with || delimiter."""
        from app.core.constants import Race
        
        result = get_subject_summary_filters(
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

    def test_race_filter_url_encoded(self, mock_request):
        """Test race filter with URL-encoded || (%7C%7C)."""
        from app.core.constants import Race
        
        result = get_subject_summary_filters(
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
        result = get_subject_summary_filters(
            sex=None,
            race="InvalidRace",
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_race" in result

    def test_valid_identifiers_single(self, mock_request):
        """Test valid single identifier."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="PBBHCR",
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["identifiers"] == "PBBHCR"

    def test_valid_identifiers_multiple(self, mock_request):
        """Test valid multiple identifiers with || delimiter."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers="PBBHCR||PBBHIL",
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert isinstance(result["identifiers"], list)
        assert "PBBHCR" in result["identifiers"]
        assert "PBBHIL" in result["identifiers"]

    def test_valid_vital_status_filter(self, mock_request):
        """Test valid vital_status filter values."""
        from app.core.constants import VitalStatus
        
        result = get_subject_summary_filters(
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

        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status="Dead",
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert result["vital_status"] == "Dead"

    def test_invalid_vital_status_filter(self, mock_request):
        """Test invalid vital_status filter values."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status="Invalid",
            age_at_vital_status=None,
            depositions=None,
            request=mock_request
        )
        assert "_invalid_vital_status" in result
        assert result["_invalid_vital_status"] == "Invalid"

    def test_valid_age_at_vital_status_filter(self, mock_request):
        """Test valid age_at_vital_status filter."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="45",
            depositions=None,
            request=mock_request
        )
        assert result["age_at_vital_status"] == 45

    def test_invalid_age_at_vital_status_negative(self, mock_request):
        """Test invalid negative age_at_vital_status."""
        result = get_subject_summary_filters(
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

    def test_invalid_age_at_vital_status_too_large(self, mock_request):
        """Test invalid age_at_vital_status exceeding maximum."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status="80000",
            depositions=None,
            request=mock_request
        )
        assert "_invalid_age_at_vital_status" in result
        assert result["_invalid_age_at_vital_status"] == "80000"

    def test_invalid_age_at_vital_status_not_integer(self, mock_request):
        """Test invalid age_at_vital_status that's not an integer."""
        result = get_subject_summary_filters(
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

    def test_valid_depositions_filter(self, mock_request):
        """Test valid depositions filter."""
        result = get_subject_summary_filters(
            sex=None,
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions="phs002431",
            request=mock_request
        )
        assert result["depositions"] == "phs002431"

    def test_unknown_parameters(self, mock_request):
        """Test that unknown parameters are rejected."""
        mock_request.query_params.keys = Mock(return_value=["page", "per_page", "search"])
        
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
        assert "_unknown_parameters" in result
        assert "page" in result["_unknown_parameters"]
        assert "per_page" in result["_unknown_parameters"]
        assert "search" in result["_unknown_parameters"]

    def test_unharmonized_fields_allowed(self, mock_request):
        """Test that unharmonized fields are allowed."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.custom_field"])
        mock_request.query_params.items = Mock(return_value=[("metadata.unharmonized.custom_field", "value")])
        
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
        assert "metadata.unharmonized.custom_field" in result
        assert result["metadata.unharmonized.custom_field"] == "value"

    def test_multiple_filters(self, mock_request):
        """Test multiple valid filters together."""
        result = get_subject_summary_filters(
            sex="M",
            race="White",
            ethnicity="Not reported",
            identifiers="PBBHCR",
            vital_status="Alive",
            age_at_vital_status="45",
            depositions="phs002431",
            request=mock_request
        )
        assert result["sex"] == "M"
        assert result["race"] == "White"
        assert result["ethnicity"] == "Not reported"
        assert result["identifiers"] == "PBBHCR"
        assert result["vital_status"] == "Alive"
        assert result["age_at_vital_status"] == 45
        assert result["depositions"] == "phs002431"

    def test_no_request_object(self):
        """Test function works without request object."""
        result = get_subject_summary_filters(
            sex="M",
            race=None,
            ethnicity=None,
            identifiers=None,
            vital_status=None,
            age_at_vital_status=None,
            depositions=None,
            request=None
        )
        assert result["sex"] == "M"


@pytest.mark.unit
class TestGetSampleFiltersNoDescriptions:
    """Test cases for get_sample_filters_no_descriptions function."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        return request

    def test_identifiers_single(self, mock_request):
        """Test single identifier (line 579-589)."""
        result = get_sample_filters_no_descriptions(
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
            identifiers="SAMP001",
            request=mock_request
        )
        assert result["identifiers"] == "SAMP001"

    def test_identifiers_multiple(self, mock_request):
        """Test multiple identifiers with || delimiter (line 582-586)."""
        result = get_sample_filters_no_descriptions(
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
            identifiers="SAMP001||SAMP002",
            request=mock_request
        )
        assert isinstance(result["identifiers"], list)
        assert "SAMP001" in result["identifiers"]
        assert "SAMP002" in result["identifiers"]

    def test_anatomical_sites_url_encoded(self, mock_request):
        """Test anatomical_sites with URL-encoded || (line 599-600)."""
        result = get_sample_filters_no_descriptions(
            disease_phase=None,
            anatomical_sites="Brain%7C%7CSpine",
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
        assert isinstance(result["anatomical_sites"], list)
        assert "Brain" in result["anatomical_sites"]
        assert "Spine" in result["anatomical_sites"]

    def test_anatomical_sites_single_after_split(self, mock_request):
        """Test anatomical_sites single value after split (line 607-608)."""
        result = get_sample_filters_no_descriptions(
            disease_phase=None,
            anatomical_sites="Brain",
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
        assert result["anatomical_sites"] == "Brain"

    def test_anatomical_site_singular_rejected(self, mock_request):
        """Test that singular 'anatomical_site' is rejected (line 645-648)."""
        mock_request.query_params.keys = Mock(return_value=["anatomical_site"])
        mock_request.query_params.items = Mock(return_value=[("anatomical_site", "Brain")])
        
        from app.models.errors import InvalidParametersError
        
        with pytest.raises(InvalidParametersError):
            get_sample_filters_no_descriptions(
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


@pytest.mark.unit
class TestGetFileFiltersNoDescriptions:
    """Test cases for get_file_filters_no_descriptions function."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock Request object."""
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        return request

    def test_type_filter_mapping(self, mock_request):
        """Test type filter maps to file_type (line 731)."""
        result = get_file_filters_no_descriptions(
            type="fastq",
            size=None,
            checksums=None,
            description=None,
            depositions=None,
            request=mock_request
        )
        assert result["file_type"] == "fastq"
        assert "type" not in result

    def test_size_filter_mapping(self, mock_request):
        """Test size filter maps to file_size (line 733)."""
        result = get_file_filters_no_descriptions(
            type=None,
            size="12345",
            checksums=None,
            description=None,
            depositions=None,
            request=mock_request
        )
        assert result["file_size"] == "12345"
        assert "size" not in result

    def test_checksums_filter_mapping(self, mock_request):
        """Test checksums filter maps to md5sum (line 735)."""
        result = get_file_filters_no_descriptions(
            type=None,
            size=None,
            checksums="abc123",
            description=None,
            depositions=None,
            request=mock_request
        )
        assert result["md5sum"] == "abc123"
        assert "checksums" not in result

    def test_description_filter_mapping(self, mock_request):
        """Test description filter maps to file_description (line 737)."""
        result = get_file_filters_no_descriptions(
            type=None,
            size=None,
            checksums=None,
            description="Test description",
            depositions=None,
            request=mock_request
        )
        assert result["file_description"] == "Test description"
        assert "description" not in result

    def test_depositions_filter(self, mock_request):
        """Test depositions filter (line 739)."""
        result = get_file_filters_no_descriptions(
            type=None,
            size=None,
            checksums=None,
            description=None,
            depositions="phs002431",
            request=mock_request
        )
        assert result["depositions"] == "phs002431"

    def test_unharmonized_fields(self, mock_request):
        """Test unharmonized fields are included (line 742-745)."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.custom_field"])
        mock_request.query_params.items = Mock(return_value=[("metadata.unharmonized.custom_field", "value")])
        
        result = get_file_filters_no_descriptions(
            type=None,
            size=None,
            checksums=None,
            description=None,
            depositions=None,
            request=mock_request
        )
        assert "metadata.unharmonized.custom_field" in result
        assert result["metadata.unharmonized.custom_field"] == "value"

    def test_all_filters_together(self, mock_request):
        """Test all filters together."""
        result = get_file_filters_no_descriptions(
            type="fastq",
            size="12345",
            checksums="abc123",
            description="Test",
            depositions="phs002431",
            request=mock_request
        )
        assert result["file_type"] == "fastq"
        assert result["file_size"] == "12345"
        assert result["md5sum"] == "abc123"
        assert result["file_description"] == "Test"
        assert result["depositions"] == "phs002431"

    def test_identifiers_single_after_split(self, mock_request):
        """Test identifiers single value after split (line 365-366)."""
        result = get_subject_summary_filters(
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

    def test_anatomical_sites_single_after_split(self, mock_request):
        """Test anatomical_sites single value after split (line 607-608)."""
        result = get_sample_filters_no_descriptions(
            disease_phase=None,
            anatomical_sites="Brain||",  # Split results in single item after stripping
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
        # When split results in single item, should be stored as string
        assert result["anatomical_sites"] == "Brain"
        assert not isinstance(result["anatomical_sites"], list)

    def test_sample_filters_unharmonized_fields(self, mock_request):
        """Test unharmonized fields in sample filters (line 640-643)."""
        mock_request.query_params.keys = Mock(return_value=["metadata.unharmonized.custom_field"])
        mock_request.query_params.items = Mock(return_value=[("metadata.unharmonized.custom_field", "value")])
        
        result = get_sample_filters_no_descriptions(
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
        assert "metadata.unharmonized.custom_field" in result
        assert result["metadata.unharmonized.custom_field"] == "value"

    def test_sample_filters_all_fields(self, mock_request):
        """Test all sample filter fields to cover lines 593-643."""
        result = get_sample_filters_no_descriptions(
            disease_phase="Phase 1",
            anatomical_sites="Brain",
            library_selection_method="PCR",
            library_strategy="WXS",
            library_source_material="DNA",
            preservation_method="FFPE",
            tumor_grade="G1",
            specimen_molecular_analyte_type="DNA",
            tissue_type="Tumor",
            tumor_classification="Primary",
            age_at_diagnosis="45",
            age_at_collection="50",
            tumor_tissue_morphology="Adenocarcinoma",
            depositions="phs002431",
            diagnosis="C50.9",
            identifiers="SAMP001",
            request=mock_request
        )
        assert result["disease_phase"] == "Phase 1"
        assert result["anatomical_sites"] == "Brain"
        assert result["library_selection_method"] == "PCR"
        assert result["library_strategy"] == "WXS"
        assert result["library_source_material"] == "DNA"
        assert result["preservation_method"] == "FFPE"
        assert result["tumor_grade"] == "G1"
        assert result["specimen_molecular_analyte_type"] == "DNA"
        assert result["tissue_type"] == "Tumor"
        assert result["tumor_classification"] == "Primary"
        assert result["age_at_diagnosis"] == "45"
        assert result["age_at_collection"] == "50"
        assert result["tumor_tissue_morphology"] == "Adenocarcinoma"
        assert result["depositions"] == "phs002431"
        assert result["diagnosis"] == "C50.9"
        assert result["identifiers"] == "SAMP001"

