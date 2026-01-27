"""
Enhanced unit tests for repository classes.

Tests additional edge cases, error handling, and complex scenarios
with comprehensive mocking.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.repositories.file import FileRepository
from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist, EntityType
from app.models.errors import UnsupportedFieldError
from app.models.dto import Subject, File, Sample


@pytest.mark.unit
class TestSubjectRepositoryEnhanced:
    """Enhanced test cases for SubjectRepository."""

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
        settings.sex_value_mappings = {}
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SubjectRepository instance."""
        return SubjectRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_subjects_with_multiple_filters(self, repository, mock_session):
        """Test get_subjects with multiple filters."""
        async def async_gen():
            yield {
                "name": "test_id",
                "participant_id": "test_id",
                "namespace": "phs002431",
                "sex": "F",
                "race": ["White"],
                "ethnicity": "Not Hispanic or Latino",
                "vital_status": "Alive",
                "age_at_vital_status": 45,
                "associated_diagnoses": [],
                "depositions": ["phs002431"]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"sex": "F", "vital_status": "Alive", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_race_list_filter(self, repository, mock_session):
        """Test get_subjects with race as a list."""
        # This test covers the race filter path but may not cover line 129 (empty race_list)
        # Let's add a test for that
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"race": ["White", "Black or African American"]},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_diagnosis_search(self, repository, mock_session):
        """Test get_subjects with diagnosis search filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"search": "cancer"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_identifiers_filter(self, repository, mock_session):
        """Test get_subjects with identifiers filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"identifiers": ["id1", "id2", "id3"]},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_age_at_vital_status_filter(self, repository, mock_session):
        """Test get_subjects with age_at_vital_status filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"age_at_vital_status": "45"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    @pytest.mark.skip(reason="Retry logic testing requires complex async mock setup")
    async def test_get_subjects_retry_logic(self, repository, mock_session):
        """Test get_subjects retry logic on error."""
        # This test requires complex async exception handling
        # Skipped for now - retry logic is tested in integration scenarios
        pass

    async def test_get_subjects_empty_result(self, repository, mock_session):
        """Test get_subjects with empty result."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert len(result) == 0

    async def test_get_subject_by_identifier_not_found(self, repository, mock_session):
        """Test get_subject_by_identifier when subject not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None
        assert mock_session.run.called

    async def test_get_subject_by_identifier_with_base_url(self, repository, mock_session):
        """Test get_subject_by_identifier with base_url parameter."""
        mock_result = AsyncMock()
        mock_record = {
            "participant_id": "test_id",
            "p": {
                "participant_id": "test_id",
                "sex_at_birth": "M",
                "race": ["White"]
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_id",
            base_url="https://test.example.com"
        )
        
        assert mock_session.run.called

    async def test_count_subjects_by_field_sex(self, repository, mock_session):
        """Test count_subjects_by_field with sex field."""
        # count_subjects_by_field uses complex query patterns
        # Mock async iteration for values and single() for totals
        async def async_gen():
            yield {"value": "M", "count": 10}
            yield {"value": "F", "count": 15}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.single = AsyncMock(return_value={"total": 25, "missing": 0})
        
        # The method may call run multiple times
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.count_subjects_by_field("sex", {})
        
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
        assert "missing" in result
        assert mock_session.run.called

    async def test_count_subjects_by_field_with_filters(self, repository, mock_session):
        """Test count_subjects_by_field with additional filters."""
        async def async_gen():
            yield {"value": "Alive", "count": 20}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.count_subjects_by_field(
            "vital_status",
            {"sex": "F"}
        )
        
        assert isinstance(result, dict)
        assert "total" in result

    async def test_count_subjects_by_field_invalid_field(self, repository, mock_allowlist, mock_session):
        """Test count_subjects_by_field with invalid field raises error."""
        # Field not in allowed count fields
        with pytest.raises(UnsupportedFieldError):
            await repository.count_subjects_by_field("invalid_field", {})

    async def test_get_subjects_summary(self, repository, mock_session):
        """Test get_subjects_summary."""
        # get_subjects_summary uses async iteration, returns {"total_count": ...}
        async def async_gen():
            yield {"total_count": 100}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects_summary(filters={})
        
        assert isinstance(result, dict)
        # The result structure is {"total_count": ...}
        assert "total_count" in result

    async def test_get_subjects_summary_with_filters(self, repository, mock_session):
        """Test get_subjects_summary with filters."""
        async def async_gen():
            yield {"total_count": 50}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects_summary(
            filters={"sex": "F", "vital_status": "Alive"}
        )
        
        assert isinstance(result, dict)
        # The result structure is {"total_count": ...}
        assert "total_count" in result

    async def test_get_subjects_with_empty_race_list(self, repository, mock_session):
        """Test get_subjects with empty race list (covers line 129)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with race filter that results in empty list (line 129)
        result = await repository.get_subjects(
            filters={"race": ""},  # Empty string should result in empty race_list
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_subjects_with_race_not_reported(self, repository, mock_session):
        """Test get_subjects with race filter including 'Not Reported' (covers line 145)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with race filter that includes "Not Reported" (line 145)
        result = await repository.get_subjects(
            filters={"race": "Not Reported"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_subjects_with_ethnicity_filter(self, repository, mock_session):
        """Test get_subjects with ethnicity filter (covers lines 255-268)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with ethnicity filter (covers the ethnicity handling path)
        result = await repository.get_subjects(
            filters={"ethnicity": "Hispanic or Latino"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        
        # Test with "Not reported" ethnicity
        result2 = await repository.get_subjects(
            filters={"ethnicity": "Not reported"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result2, list)


@pytest.mark.unit
class TestFileRepositoryEnhanced:
    """Enhanced test cases for FileRepository."""

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

    async def test_get_files_with_multiple_filters(self, repository, mock_session):
        """Test get_files with multiple filters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files(
            filters={"file_type": "BAM", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_file_by_identifier(self, repository, mock_session):
        """Test get_file_by_identifier."""
        mock_result = AsyncMock()
        mock_record = {
            "file_id": "test_file",
            "f": {
                "file_id": "test_file",
                "file_type": "BAM"
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_file"
        )
        
        assert mock_session.run.called

    async def test_get_file_by_identifier_not_found(self, repository, mock_session):
        """Test get_file_by_identifier when file not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None

    async def test_get_files_summary(self, repository, mock_session):
        """Test get_files_summary."""
        async def async_gen():
            yield {"total_count": 200}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files_summary(filters={})
        
        assert isinstance(result, dict)
        # The summary returns a dict with total_count key
        assert "total_count" in result


@pytest.mark.unit
class TestSampleRepositoryEnhanced:
    """Enhanced test cases for SampleRepository."""

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
        settings.sample_count_fields = ["tissue_type", "diagnosis", "disease_phase"]
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_samples_with_multiple_filters(self, repository, mock_session):
        """Test get_samples with multiple filters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor", "diagnosis": "Cancer", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_sample_by_identifier(self, repository, mock_session):
        """Test get_sample_by_identifier."""
        mock_result = AsyncMock()
        mock_record = {
            "sample_id": "test_sample",
            "sa": {
                "sample_id": "test_sample",
                "tissue_type": "Tumor"
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_sample"
        )
        
        assert mock_session.run.called

    async def test_get_sample_by_identifier_not_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None

    async def test_get_samples_summary(self, repository, mock_session):
        """Test get_samples_summary."""
        async def async_gen():
            yield {"total": 150}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={})
        
        assert isinstance(result, dict)
        assert "counts" in result

