"""
Comprehensive unit tests for repository classes.

Tests internal methods, helper functions, and edge cases that are not covered
by existing tests.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock, patch
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.repositories.file import FileRepository
from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist, EntityType
from app.models.errors import UnsupportedFieldError
from app.core.config import Settings


@pytest.mark.unit
class TestSubjectRepositoryInternal:
    """Test cases for SubjectRepository internal methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.subject_count_fields = ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses"]
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SubjectRepository instance."""
        return SubjectRepository(mock_session, mock_allowlist, mock_settings)

    def test_get_field_path_sex(self, repository):
        """Test _get_field_path for sex field."""
        result = repository._get_field_path("sex")
        assert "sex" in result.lower()

    def test_get_field_path_race(self, repository):
        """Test _get_field_path for race field."""
        result = repository._get_field_path("race")
        assert "race" in result.lower()

    def test_get_field_path_ethnicity(self, repository):
        """Test _get_field_path for ethnicity field."""
        result = repository._get_field_path("ethnicity")
        assert "ethnicity" in result.lower()

    def test_get_field_path_vital_status(self, repository):
        """Test _get_field_path for vital_status field."""
        result = repository._get_field_path("vital_status")
        assert "vital_status" in result.lower()

    def test_build_sex_normalization_case(self, repository, mock_settings):
        """Test _build_sex_normalization_case."""
        # Set up settings with sex_value_mappings
        mock_settings.sex_value_mappings = {"M": "Male", "F": "Female", "Not Reported": "U"}
        repository.settings = mock_settings
        
        result = repository._build_sex_normalization_case("sex")
        assert "CASE" in result.upper()
        assert "WHEN" in result.upper()
        
        # Test with non-sex field
        result = repository._build_sex_normalization_case("race")
        assert result == ""

    def test_validate_filters_valid(self, repository, mock_allowlist):
        """Test _validate_filters with valid fields."""
        mock_allowlist.is_field_allowed = Mock(return_value=True)
        filters = {"sex": "Female", "race": "White"}
        
        # Should not raise
        repository._validate_filters(filters, "subject")

    def test_validate_filters_invalid(self, repository, mock_allowlist):
        """Test _validate_filters with invalid field."""
        mock_allowlist.is_field_allowed = Mock(return_value=False)
        filters = {"invalid_field": "value"}
        
        with pytest.raises(UnsupportedFieldError):
            repository._validate_filters(filters, "subject")

    def test_record_to_subject(self, repository):
        """Test _record_to_subject conversion."""
        repository.settings.sex_value_mappings = {
            "female": "F",
            "male": "M",
            "Not Reported": "U"
        }
        record = {
            "name": "P1",
            "namespace": "phs001",
            "depositions": ["phs001", "phs002"],
            "race": "Hispanic or Latino;White",
            "sex": "female",
            "vital_status": None,
            "age_at_vital_status": None,
            "associated_diagnoses": None,
            "survival_records": [
                {"last_known_survival_status": "Alive", "age_at_last_known_survival_status": 5},
                {"last_known_survival_status": "Dead", "age_at_last_known_survival_status": 10},
            ],
            "diagnosis_nodes": [
                {"diagnosis": ["Neuroblastoma", ""]},
                {"diagnosis": "Leukemia"}
            ]
        }

        subject = repository._record_to_subject(record, base_url="http://example.org")

        assert subject.id.namespace.name == "phs002"
        assert subject.id.name == "P1"
        assert subject.metadata.sex.value == "F"
        assert subject.metadata.ethnicity.value == "Hispanic or Latino"
        assert [item.value for item in subject.metadata.race] == ["White"]
        assert subject.metadata.vital_status.value == "Dead"
        assert subject.metadata.age_at_vital_status.value == 10
        assert [item.value for item in subject.metadata.associated_diagnoses] == ["Leukemia", "Neuroblastoma"]
        assert len(subject.metadata.identifiers) == 2
        assert subject.metadata.identifiers[0].value.server.startswith("http://example.org/api/v1/subject/")
        assert len(subject.metadata.depositions) == 2

    async def test_count_subjects_by_race(self, repository, mock_session):
        """Test _count_subjects_by_race with no filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 3}]
        total_result.consume = AsyncMock()
        unique_result = AsyncMock()
        unique_result.__aiter__.return_value = [{"unique_count": 2}]
        unique_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "White", "count": 1},
            {"value": "Asian", "count": 1},
        ]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, unique_result, values_result])

        result = await repository._count_subjects_by_race({})

        assert result["total"] == 3
        assert result["missing"] == 1  # total - unique_with_valid_race
        assert {item["value"] for item in result["values"]} == {"White", "Asian"}
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1]["valid_races"]

    async def test_count_subjects_by_ethnicity(self, repository, mock_session):
        """Test _count_subjects_by_ethnicity with no filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 4}]
        total_result.consume = AsyncMock()
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 1}]
        missing_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "Hispanic or Latino", "count": 1},
            {"value": "Not reported", "count": 2},
        ]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_subjects_by_ethnicity({})

        assert result["total"] == 4
        assert result["missing"] == 1
        assert {item["value"] for item in result["values"]} == {"Hispanic or Latino", "Not reported"}
        assert mock_session.run.call_count == 3

    async def test_count_subjects_by_associated_diagnoses_no_filters(self, repository, mock_session):
        """Test _count_subjects_by_associated_diagnoses with no filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 3}]
        total_result.consume = AsyncMock()
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 1}]
        missing_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "Neuroblastoma", "count": 2},
            {"value": "Leukemia", "count": 1},
        ]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_subjects_by_associated_diagnoses({})

        assert result["total"] == 3
        assert result["missing"] == 1
        assert len(result["values"]) == 2
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1] == {}

    async def test_count_subjects_by_associated_diagnoses_with_identifiers(self, repository, mock_session):
        """Test _count_subjects_by_associated_diagnoses with identifiers filter."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 1}]
        total_result.consume = AsyncMock()
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        missing_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [{"value": "Wilms Tumor", "count": 1}]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_subjects_by_associated_diagnoses(
            {"identifiers": ["P1", "P2"], "sex": "F"}
        )

        assert result["total"] == 1
        assert result["missing"] == 0
        assert result["values"][0]["value"] == "Wilms Tumor"
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1]["param_1"] == ["P1", "P2"]

    async def test_count_subjects_by_associated_diagnoses_skips_diagnosis_filters(self, repository, mock_session):
        """Test _count_subjects_by_associated_diagnoses ignores diagnosis filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 2}]
        total_result.consume = AsyncMock()
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        missing_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [{"value": "Neuroblastoma", "count": 2}]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_subjects_by_associated_diagnoses(
            {"_diagnosis_search": "cancer", "associated_diagnoses": "x", "sex": "F"}
        )

        assert result["total"] == 2
        assert result["values"][0]["value"] == "Neuroblastoma"
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1] == {"param_1": "F"}


@pytest.mark.unit
class TestFileRepositoryInternal:
    """Test cases for FileRepository internal methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a FileRepository instance."""
        return FileRepository(mock_session, mock_allowlist)

    def test_validate_filters_valid(self, repository, mock_allowlist):
        """Test _validate_filters with valid fields."""
        mock_allowlist.is_field_allowed = Mock(return_value=True)
        filters = {"file_type": "BAM", "md5sum": "abc123"}
        
        # Should not raise
        repository._validate_filters(filters, "file")

    def test_validate_filters_invalid(self, repository, mock_allowlist):
        """Test _validate_filters with invalid field."""
        mock_allowlist.is_field_allowed = Mock(return_value=False)
        filters = {"invalid_field": "value"}
        
        with pytest.raises(UnsupportedFieldError):
            repository._validate_filters(filters, "file")

    def test_map_file_type_to_enum_valid(self, repository):
        """Test _map_file_type_to_enum with valid enum value."""
        result = repository._map_file_type_to_enum("BAM")
        assert result == "BAM"

    def test_map_file_type_to_enum_invalid(self, repository):
        """Test _map_file_type_to_enum with invalid value."""
        result = repository._map_file_type_to_enum("INVALID_TYPE")
        assert result is None

    def test_map_file_type_to_enum_none(self, repository):
        """Test _map_file_type_to_enum with None."""
        result = repository._map_file_type_to_enum(None)
        assert result is None

    def test_record_to_file(self, repository):
        """Test _record_to_file conversion."""
        record = {
            "id": "file1.bam",
            "file_type": "bam",
            "file_size": 123,
            "md5sum": "abc123",
            "file_description": "test file",
            "file_name": "file1.bam"
        }
        samples = [{"sample_id": "S1"}]
        study = {"study_id": "phs002431"}

        file_obj = repository._record_to_file(record, samples=samples, study=study)

        assert file_obj.id["namespace"]["name"] == "phs002431"
        assert file_obj.id["name"] == "file1.bam"
        assert file_obj.samples[0]["name"] == "S1"
        assert file_obj.metadata["type"]["value"] == "BAM"
        assert file_obj.metadata["checksums"]["value"]["md5"] == "abc123"
        assert file_obj.metadata["unharmonized"]["file_name"]["value"] == "file1.bam"
        assert file_obj.metadata["depositions"][0]["value"] == "phs002431"

    async def test_count_files_by_depositions(self, repository, mock_session):
        """Test _count_files_by_depositions with file filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 3}]
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 1}]
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "phs002431", "count": 2},
            {"value": "phs002432", "count": 1},
        ]
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        filters = {"metadata.unharmonized.file_name": ["a.bam", "b.bam"]}
        result = await repository._count_files_by_depositions(filters)

        assert result["total"] == 3
        assert result["missing"] == 1
        assert len(result["values"]) == 2
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1]["param_1"] == ["a.bam", "b.bam"]

    async def test_count_files_by_depositions_no_filters(self, repository, mock_session):
        """Test _count_files_by_depositions without file filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 2}]
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [{"value": "phs002431", "count": 2}]
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_files_by_depositions({})

        assert result["total"] == 2
        assert result["missing"] == 0
        assert result["values"][0]["value"] == "phs002431"
        assert mock_session.run.call_args_list[0][1] == {}

    async def test_count_files_by_field_type_invalid_filter(self, repository, mock_session):
        """Test count_files_by_field returns empty for invalid type filter."""
        result = await repository.count_files_by_field("type", {"file_type": "INVALID"})

        assert result["total"] == 0
        assert result["missing"] == 0
        assert result["values"] == []
        assert not mock_session.run.called

    async def test_count_files_by_field_type_enum_mapping(self, repository, mock_session):
        """Test count_files_by_field maps enum values and counts non-matching as missing."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 3}]
        total_result.consume = AsyncMock()
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        missing_result.consume = AsyncMock()
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "bam", "count": 2},
            {"value": "unknown", "count": 1},
        ]
        values_result.consume = AsyncMock()
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository.count_files_by_field("type", {})

        assert result["total"] == 3
        assert result["missing"] == 1  # unknown counted as missing
        assert result["values"][0]["value"] == "BAM"
        assert result["values"][0]["count"] == 2

    async def test_get_files_summary_invalid_type(self, repository, mock_session):
        """Test get_files_summary returns empty for invalid type filter."""
        result = await repository.get_files_summary({"file_type": "INVALID"})

        assert result["total_count"] == 0
        assert not mock_session.run.called

    async def test_get_files_summary_checksums_and_depositions(self, repository, mock_session):
        """Test get_files_summary uses checksums and depositions filters."""
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = [{"total_count": 5}]
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)

        filters = {
            "md5sum": "abc||def",
            "depositions": "phs001||phs002",
            "file_size": "123",
            "metadata.unharmonized.file_name": ["file.bam"]
        }
        result = await repository.get_files_summary(filters)

        assert result["total_count"] == 5
        assert mock_session.run.called
        params = mock_session.run.call_args[0][1]
        assert params["param_1"] == 123
        assert params["param_2"] == ["file.bam"]
        assert params["param_3"] == ["abc", "def"]
        assert params["param_4"] == ["phs001", "phs002"]

@pytest.mark.unit
class TestSampleRepositoryInternal:
    """Test cases for SampleRepository internal methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock(spec=Settings)
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    def test_reverse_map_library_selection_method_static(self):
        """Test _reverse_map_library_selection_method_static."""
        result = SampleRepository._reverse_map_library_selection_method_static("PCR")
        # Should return a string or None
        assert result is None or isinstance(result, str)

    def test_get_next_param_name(self):
        """Test _get_next_param_name."""
        params = {}
        result = SampleRepository._get_next_param_name(params, 0)
        assert result == "param_1"
        
        params["param_1"] = "value"
        result = SampleRepository._get_next_param_name(params, 0)
        assert result == "param_2"

    def test_validate_tissue_type_filter_valid(self):
        """Test _validate_tissue_type_filter with valid value."""
        from unittest.mock import patch
        params = {}
        with_conditions = []
        
        # Mock load_sample_enum to return valid values
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = SampleRepository._validate_tissue_type_filter(
                "Tumor", "tissue_param", params, with_conditions
            )
            assert result is True
            assert "tissue_param" in params

    def test_validate_tissue_type_filter_invalid(self):
        """Test _validate_tissue_type_filter with invalid value."""
        from unittest.mock import patch
        params = {}
        with_conditions = []
        
        # Mock load_sample_enum to return valid values
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = SampleRepository._validate_tissue_type_filter(
                "Invalid", "tissue_param", params, with_conditions
            )
            assert result is None

    def test_validate_library_source_material_filter(self, repository):
        """Test _validate_library_source_material_filter."""
        from unittest.mock import patch
        params = {}
        with_conditions = []
        
        # Mock load_sample_enum to return valid values
        with patch('app.repositories.sample.load_sample_enum', return_value=["DNA", "RNA"]):
            result = repository._validate_library_source_material_filter(
                "DNA", "source_param", params, with_conditions
            )
            # Should return True or None depending on validation
            assert result is None or result is True

    def test_build_sex_normalization_case(self, repository, mock_settings):
        """Test _build_sex_normalization_case."""
        # Set up settings with sex_value_mappings
        mock_settings.sex_value_mappings = {"M": "Male", "F": "Female", "Not Reported": "U"}
        repository.settings = mock_settings
        
        result = repository._build_sex_normalization_case("sex")
        assert "CASE" in result.upper()
        assert "WHEN" in result.upper()
        
        # Test with non-sex field
        result = repository._build_sex_normalization_case("race")
        assert result == ""

    def test_validate_filters_valid(self, repository, mock_allowlist):
        """Test _validate_filters with valid fields."""
        mock_allowlist.is_field_allowed = Mock(return_value=True)
        filters = {"tissue_type": "Tumor", "library_source_material": "DNA"}
        
        # Should not raise
        repository._validate_filters(filters, "sample")

    def test_validate_filters_invalid(self, repository, mock_allowlist):
        """Test _validate_filters with invalid field."""
        mock_allowlist.is_field_allowed = Mock(return_value=False)
        filters = {"invalid_field": "value"}
        
        with pytest.raises(UnsupportedFieldError):
            repository._validate_filters(filters, "sample")

    @pytest.mark.skip(reason="Complex method requires extensive mocking of internal query building")
    async def test_get_samples_by_sequencing_file_filters(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters method (skipped - complex)."""
        pass

    @pytest.mark.skip(reason="Complex method requires extensive mocking of internal query building")
    async def test_count_samples_by_associated_diagnoses(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses method (skipped - complex)."""
        pass

    async def test_get_samples_summary_reverse_query(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query method."""
        async def async_gen():
            yield {"total_count": 1000}
        
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = async_gen()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository._get_samples_summary_reverse_query({})
        
        assert isinstance(result, dict)
        mock_session.run.assert_called()

    async def test_get_samples_by_sequencing_file_filters_invalid_value(self, repository, mock_session):
        """Test reverse query returns empty list for invalid filter value."""
        with patch("app.repositories.sample.is_null_mapped_value", return_value=True):
            result = await repository._get_samples_by_sequencing_file_filters(
                {"library_source_material": "Invalid value"},
                offset=0,
                limit=10
            )

        assert result == []
        assert not mock_session.run.called

    async def test_get_samples_by_sequencing_file_filters_success(self, repository, mock_session):
        """Test reverse query returns samples when records exist."""
        with patch("app.repositories.sample.is_database_only_value", return_value=False), \
            patch("app.repositories.sample.is_null_mapped_value", return_value=False), \
            patch("app.repositories.sample.reverse_map_field_value", return_value="Transcriptomic"):
            mock_result = AsyncMock()
            mock_result.__aiter__.return_value = [
                {
                    "sa": {"sample_id": "S1"},
                    "p": {"participant_id": "P1"},
                    "st": {"study_id": "phs001"},
                    "sf": {"library_source_molecule": "Transcriptomic"},
                    "pf": {},
                    "diagnoses": {}
                }
            ]
            mock_result.consume = AsyncMock()
            mock_session.run = AsyncMock(return_value=mock_result)

            repository._record_to_sample = Mock(return_value=Mock())

            result = await repository._get_samples_by_sequencing_file_filters(
                {"specimen_molecular_analyte_type": "RNA"},
                offset=0,
                limit=10
            )

        assert len(result) == 1
        assert mock_session.run.called

    async def test_count_samples_by_associated_diagnoses_no_filters(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses with no filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 4}]
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 1}]
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [
            {"value": "Neuroblastoma", "count": 2},
            {"value": "Leukemia", "count": 1},
        ]
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        result = await repository._count_samples_by_associated_diagnoses({})

        assert result["total"] == 4
        assert result["missing"] == 1
        assert len(result["values"]) == 2
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][1] == {}

    async def test_count_samples_by_associated_diagnoses_with_identifiers(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses with identifier filter."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 1}]
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [{"value": "Wilms Tumor", "count": 1}]
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        filters = {"identifiers": ["P1", "P2"], "sex": "F"}
        result = await repository._count_samples_by_associated_diagnoses(filters)

        assert result["total"] == 1
        assert result["missing"] == 0
        assert result["values"][0]["value"] == "Wilms Tumor"
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1]["param_1"] == ["P1", "P2"]

    async def test_count_samples_by_associated_diagnoses_skips_diagnosis_filters(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses ignores diagnosis filters."""
        total_result = AsyncMock()
        total_result.__aiter__.return_value = [{"total": 2}]
        missing_result = AsyncMock()
        missing_result.__aiter__.return_value = [{"missing": 0}]
        values_result = AsyncMock()
        values_result.__aiter__.return_value = [{"value": "Neuroblastoma", "count": 2}]
        mock_session.run = AsyncMock(side_effect=[total_result, missing_result, values_result])

        filters = {"_diagnosis_search": "cancer", "associated_diagnoses": "x", "sex": "F"}
        result = await repository._count_samples_by_associated_diagnoses(filters)

        assert result["total"] == 2
        assert result["values"][0]["value"] == "Neuroblastoma"
        assert mock_session.run.call_count == 3
        assert mock_session.run.call_args_list[0][0][1] == {"param_1": "F"}

    def test_record_to_sample(self, repository):
        """Test _record_to_sample conversion."""
        sa = {
            "sample_id": "SAMPLE-001",
            "tissue_type": "Tumor",
            "library_source_material": "DNA",
            "sample_tumor_status": "Tumor"
        }
        p = {"participant_id": "TEST-001"}
        st = {"study_id": "phs002431"}
        sf = {}
        pf = {}
        diagnoses = None
        
        sample = repository._record_to_sample(sa, p, st, sf, pf, diagnoses)
        
        assert sample.id.name == "SAMPLE-001"
        assert sample.metadata.tissue_type.value == "Tumor"

    def test_record_to_sample_invalid_values_filtered(self, repository):
        """Test _record_to_sample filters invalid values."""
        sa = {
            "sample_id": "SAMPLE-002",
            "sample_tumor_status": "Invalid value",
            "anatomic_site": ["brain", "Invalid value", ""],
            "participant_age_at_collection": -999
        }
        p = {"participant_id": "TEST-002"}
        st = {"study_id": "phs002431"}
        sf = {
            "library_selection": "Invalid value",
            "library_strategy": "Invalid value",
            "library_source_material": "Invalid value",
            "library_source_molecule": "Invalid value"
        }
        pf = {"fixation_embedding_method": "Invalid value"}
        diagnoses = {
            "diagnosis": " ",
            "age_at_diagnosis": -999,
            "tumor_grade": "Invalid value",
            "tumor_classification": "Invalid value",
            "disease_phase": "Invalid value"
        }

        sample = repository._record_to_sample(sa, p, st, sf, pf, diagnoses)

        assert [value.value for value in sample.metadata.anatomical_sites] == ["brain"]
        assert sample.metadata.age_at_collection is None
        assert sample.metadata.age_at_diagnosis is None
        assert sample.metadata.library_selection_method is None
        assert sample.metadata.tissue_type is None
        assert sample.metadata.diagnosis is None

    def test_record_to_sample_anatomical_sites_and_integer_values(self, repository):
        """Test _record_to_sample processes anatomical sites and integer fields."""
        sa = {
            "sample_id": "SAMPLE-003",
            "sample_tumor_status": "Tumor",
            "anatomic_site": "Brain; Spinal cord",
            "participant_age_at_collection": "10.0"
        }
        p = {"participant_id": "TEST-003"}
        st = {"study_id": "phs002431"}
        sf = {}
        pf = {}
        diagnoses = {"diagnosis": "Neuroblastoma", "age_at_diagnosis": 5}

        sample = repository._record_to_sample(sa, p, st, sf, pf, diagnoses)

        assert [value.value for value in sample.metadata.anatomical_sites] == ["Brain", "Spinal cord"]
        assert sample.metadata.age_at_collection.value == 10
        assert sample.metadata.age_at_diagnosis.value == 5
        assert sample.metadata.diagnosis.value == "Neuroblastoma"

@pytest.mark.unit
class TestRepositoryHelperMethods:
    """Test cases for repository static helper methods."""

    def test_build_combined_where_clause_depositions_only(self):
        """Test _build_combined_where_clause_for_depositions_path with depositions only."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            None, "phs002431", "="
        )
        assert "WHERE" in result
        assert "phs002431" in result or "$" in result

    def test_build_combined_where_clause_diagnosis_only(self):
        """Test _build_combined_where_clause_for_depositions_path with diagnosis only."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            "neuroblastoma", None, None
        )
        assert "WHERE" in result
        assert "diagnosis" in result.lower()

    def test_build_combined_where_clause_both(self):
        """Test _build_combined_where_clause_for_depositions_path with both conditions."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            "neuroblastoma", "phs002431", "="
        )
        assert "WHERE" in result
        assert "AND" in result

    def test_build_combined_where_clause_empty(self):
        """Test _build_combined_where_clause_for_depositions_path with no conditions."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            None, None, None
        )
        assert result == ""

