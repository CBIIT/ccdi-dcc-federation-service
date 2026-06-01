"""
Unit tests for service classes.

Tests business logic layer including caching, validation,
error handling, and coordination with repositories.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock, patch
from neo4j import AsyncSession

from app.services.subject import SubjectService
from app.services.file import FileService
from app.config_data.file_node_registry import FILE_NODE_REGISTRY
from app.services.sample import SampleService
from app.core.config import Settings, get_settings
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Subject, File, Sample
from app.models.errors import ValidationError, NotFoundError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestSubjectService:
    """Test cases for SubjectService class."""

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
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.cache = Mock()
        settings.cache.count_ttl = 300
        settings.cache.summary_ttl = 600
        return settings

    @pytest.fixture
    def mock_cache_service(self):
        """Create a mock cache service."""
        cache = AsyncMock()
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock(return_value=True)
        return cache

    @pytest.fixture
    def service(self, mock_session, mock_allowlist, mock_settings, mock_cache_service):
        """Create a SubjectService instance."""
        return SubjectService(
            mock_session,
            mock_allowlist,
            mock_settings,
            cache_service=mock_cache_service
        )

    @pytest.fixture
    def service_no_cache(self, mock_session, mock_allowlist, mock_settings):
        """Create a SubjectService instance without cache."""
        return SubjectService(
            mock_session,
            mock_allowlist,
            mock_settings,
            cache_service=None
        )

    def test_initialization(self, service, mock_session, mock_allowlist, mock_settings):
        """Test service initialization."""
        assert service.repository is not None
        assert service.settings is mock_settings
        assert service.cache_service is not None

    async def test_get_subjects_success(self, service):
        """Test get_subjects with successful repository call."""
        # Mock repository response
        mock_subjects = [
            Mock(spec=Subject, name="subject1"),
            Mock(spec=Subject, name="subject2")
        ]
        service.repository.get_subjects = AsyncMock(return_value=mock_subjects)
        
        result = await service.get_subjects(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert len(result) == 2
        service.repository.get_subjects.assert_called_once()

    async def test_get_subjects_pagination_limit_enforced(self, service, mock_settings):
        """Test that pagination limit is enforced."""
        mock_settings.pagination.max_page_size = 100
        service.repository.get_subjects = AsyncMock(return_value=[])
        
        # Request limit exceeds max
        await service.get_subjects(filters={}, offset=0, limit=200)
        
        # Should be called with max_page_size instead of 200
        call_args = service.repository.get_subjects.call_args
        assert call_args[0][2] == 100  # limit parameter

    async def test_get_subjects_database_connection_error(self, service):
        """Test get_subjects handles database connection errors gracefully."""
        service.repository.get_subjects = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        
        result = await service.get_subjects(filters={}, offset=0, limit=20)
        
        # Should return empty list instead of raising
        assert result == []
        assert isinstance(result, list)

    async def test_get_subjects_retry_on_transient_error(self, service):
        """Test get_subjects retries on transient errors."""
        # First call fails with transient error, second succeeds
        service.repository.get_subjects = AsyncMock(
            side_effect=[
                Exception("Connection timeout"),
                [Mock(spec=Subject, name="subject1")]
            ]
        )
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            result = await service.get_subjects(filters={}, offset=0, limit=20)
        
        assert len(result) == 1
        # Should have been called twice (retry)
        assert service.repository.get_subjects.call_count == 2

    async def test_get_subjects_non_transient_error_raises(self, service):
        """Test get_subjects raises non-transient errors immediately."""
        service.repository.get_subjects = AsyncMock(
            side_effect=ValueError("Invalid filter")
        )
        
        with pytest.raises(ValueError):
            await service.get_subjects(filters={}, offset=0, limit=20)

    async def test_get_subject_by_identifier_success(self, service):
        """Test get_subject_by_identifier with successful lookup."""
        mock_subject = Mock(spec=Subject, name="test_subject")
        service.repository.get_subject_by_identifier = AsyncMock(
            return_value=mock_subject
        )
        
        result = await service.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_id"
        )
        
        assert result is not None
        assert result == mock_subject
        service.repository.get_subject_by_identifier.assert_called_once()

    async def test_get_subject_by_identifier_not_found(self, service):
        """Test get_subject_by_identifier when subject not found."""
        service.repository.get_subject_by_identifier = AsyncMock(return_value=None)
        
        result = await service.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None

    async def test_get_subject_by_identifier_validation_error(self, service):
        """Test get_subject_by_identifier with invalid parameters."""
        # Empty organization should raise validation error
        with pytest.raises(ValidationError):
            await service.get_subject_by_identifier(
                organization="",
                namespace="phs002431",
                name="test_id"
            )

    async def test_count_subjects_by_field_success(self, service, mock_cache_service):
        """Test count_subjects_by_field with successful count."""
        # Mock repository response
        mock_count_result = {
            "total": 100,
            "missing": 5,
            "values": [
                {"value": "M", "count": 50},
                {"value": "F", "count": 45}
            ]
        }
        service.repository.count_subjects_by_field = AsyncMock(
            return_value=mock_count_result
        )
        mock_cache_service.get = AsyncMock(return_value=None)  # Cache miss
        
        result = await service.count_subjects_by_field("sex", filters={})
        
        assert result.total == 100
        assert result.missing == 5
        assert len(result.values) == 2
        # Should have tried to cache the result
        mock_cache_service.set.assert_called_once()

    async def test_count_subjects_by_field_cache_hit(self, service, mock_cache_service):
        """Test count_subjects_by_field returns cached result."""
        from app.models.dto import CountResponse
        cached_result = {
            "total": 100,
            "missing": 5,
            "values": [{"value": "M", "count": 50}]
        }
        mock_cache_service.get = AsyncMock(return_value=cached_result)
        # Mock repository method to track calls
        service.repository.count_subjects_by_field = AsyncMock()
        
        result = await service.count_subjects_by_field("sex", filters={})
        
        assert isinstance(result, CountResponse)
        assert result.total == 100
        assert result.missing == 5
        # Repository should not be called when cache hit
        service.repository.count_subjects_by_field.assert_not_called()

    async def test_count_subjects_by_field_no_cache(self, service_no_cache):
        """Test count_subjects_by_field without cache service."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "White", "count": 50}]
        }
        service_no_cache.repository.count_subjects_by_field = AsyncMock(
            return_value=mock_count_result
        )
        
        result = await service_no_cache.count_subjects_by_field("race", filters={})
        
        assert result.total == 50
        service_no_cache.repository.count_subjects_by_field.assert_called_once()

    async def test_count_subjects_by_field_database_error(self, service):
        """Test count_subjects_by_field handles database errors."""
        service.repository.count_subjects_by_field = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        mock_cache_service = service.cache_service
        mock_cache_service.get = AsyncMock(return_value=None)
        
        result = await service.count_subjects_by_field("sex", filters={})
        
        # Should return empty count response
        assert result.total == 0
        assert result.missing == 0
        assert result.values == []

    async def test_get_subjects_summary_success(self, service, mock_cache_service):
        """Test get_subjects_summary with successful summary."""
        from app.models.dto import SummaryResponse
        mock_summary_result = {
            "total_count": 1000,
            "by_sex": {"M": 500, "F": 500},
            "by_race": {"White": 600, "Asian": 400}
        }
        # Mock repository method
        service.repository.get_subjects_summary = AsyncMock(
            return_value=mock_summary_result
        )
        mock_cache_service.get = AsyncMock(return_value=None)  # Cache miss
        
        result = await service.get_subjects_summary(filters={})
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 1000
        mock_cache_service.set.assert_called_once()

    async def test_get_subjects_summary_cache_hit(self, service, mock_cache_service):
        """Test get_subjects_summary returns cached result."""
        from app.models.dto import SummaryResponse
        cached_summary = {
            "total_count": 500
        }
        mock_cache_service.get = AsyncMock(return_value=cached_summary)
        service.repository.get_subjects_summary = AsyncMock()
        
        result = await service.get_subjects_summary(filters={})
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500
        service.repository.get_subjects_summary.assert_not_called()

    async def test_get_subjects_summary_cache_new_format(self, service, mock_cache_service):
        """Test get_subjects_summary handles new cache format with counts."""
        from app.models.dto import SummaryResponse, SummaryCounts
        cached_summary = {
            "counts": {"total": 250}
        }
        mock_cache_service.get = AsyncMock(return_value=cached_summary)
        service.repository.get_subjects_summary = AsyncMock()

        result = await service.get_subjects_summary(filters={})

        assert isinstance(result, SummaryResponse)
        assert result.counts == SummaryCounts(total=250)
        service.repository.get_subjects_summary.assert_not_called()

    async def test_get_subjects_summary_database_connection_error(self, service, mock_cache_service):
        """Test get_subjects_summary handles database connection error."""
        service.repository.get_subjects_summary = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        mock_cache_service.get = AsyncMock(return_value=None)

        result = await service.get_subjects_summary(filters={})

        assert result.counts.total == 0

    async def test_get_subjects_summary_retries_transient_error(self, service, mock_cache_service):
        """Test get_subjects_summary retries on transient errors."""
        service.repository.get_subjects_summary = AsyncMock(
            side_effect=[
                Exception("Connection timeout"),
                {"total_count": 10}
            ]
        )
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch('asyncio.sleep', new_callable=AsyncMock):
            result = await service.get_subjects_summary(filters={})

        assert result.counts.total == 10
        assert service.repository.get_subjects_summary.call_count == 2

    def test_validate_identifier_params_invalid_characters(self, service):
        """Test _validate_identifier_params rejects invalid characters."""
        with pytest.raises(ValidationError):
            service._validate_identifier_params("CCDI-DCC", "phs/002431", "name")
        with pytest.raises(ValidationError):
            service._validate_identifier_params("CCDI-DCC", "phs002431", "bad name")

    def test_build_cache_key_normalizes_filters(self, service):
        """Test _build_cache_key sorts and normalizes filters."""
        filters = {"race": ["White", "Black"], "sex": "F", "empty": None}
        key = service._build_cache_key("subject_count", "race", filters)
        assert key.startswith("subject_count:race:")
        assert "race:Black,White" in key
        assert "sex:F" in key


@pytest.mark.unit
class TestFileService:
    """Test cases for FileService class."""

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
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        return settings

    @pytest.fixture
    def service(self, mock_session, mock_allowlist, mock_settings):
        """Create a FileService instance."""
        return FileService(mock_session, mock_allowlist, mock_settings)

    def test_initialization(self, service, mock_session, mock_allowlist, mock_settings):
        """Test service initialization."""
        assert service._repos is not None
        assert len(service._repos) == len(FILE_NODE_REGISTRY)
        assert service.settings is mock_settings
        assert service.materialized_view_service is not None

    async def test_get_files_success(self, service):
        """Test get_files with successful repository call."""
        mock_files = [
            Mock(spec=File, id=Mock(name="file1")),
            Mock(spec=File, id=Mock(name="file2"))
        ]
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(return_value=2)
        mock_repo.get_files = AsyncMock(return_value=mock_files)

        with patch.object(service, '_repos', [mock_repo]):
            files, total = await service.get_files(filters={}, offset=0, limit=20)

        assert isinstance(files, list)
        assert len(files) == 2
        assert total == 2
        mock_repo.get_files.assert_called_once()

    async def test_get_files_pagination_limit_enforced(self, service, mock_settings):
        """Test that pagination limit is enforced when get_files is called."""
        mock_settings.pagination.max_page_size = 100
        mock_files = [Mock(spec=File, id=Mock(name=f"file{i}")) for i in range(5)]
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(return_value=500)
        mock_repo.get_files = AsyncMock(return_value=mock_files)

        with patch.object(service, '_repos', [mock_repo]):
            files, total = await service.get_files(filters={}, offset=0, limit=200)

        # Verify limit was capped at max_page_size (100), not passed as 200
        mock_repo.get_files.assert_called_once_with({}, 0, 100)
        assert isinstance(files, list)
        assert total == 500

    async def test_get_files_database_connection_error(self, service):
        """Test get_files handles database connection errors gracefully."""
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )

        with patch.object(service, '_repos', [mock_repo]):
            files, total = await service.get_files(filters={}, offset=0, limit=20)

        assert files == []
        assert total == 0

    async def test_get_file_by_identifier_success(self, service):
        """Test get_file_by_identifier with successful lookup."""
        mock_file = Mock(spec=File, id=Mock(name="file1"))
        mock_repo = AsyncMock()
        mock_repo.get_file_by_identifier = AsyncMock(return_value=mock_file)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.get_file_by_identifier(
                organization="CCDI-DCC",
                namespace="phs002431",
                name="file1"
            )

        assert result is not None
        mock_repo.get_file_by_identifier.assert_called_once()

    async def test_get_file_by_identifier_not_found(self, service):
        """Test get_file_by_identifier when file not found."""
        mock_repo = AsyncMock()
        mock_repo.get_file_by_identifier = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            with pytest.raises(NotFoundError):
                await service.get_file_by_identifier(
                    organization="CCDI-DCC",
                    namespace="phs002431",
                    name="nonexistent"
                )


@pytest.mark.unit
class TestSampleService:
    """Test cases for SampleService class."""

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
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        return settings

    @pytest.fixture
    def service(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleService instance."""
        return SampleService(mock_session, mock_allowlist, mock_settings)

    def test_initialization(self, service, mock_session, mock_allowlist, mock_settings):
        """Test service initialization."""
        assert service.repository is not None
        assert service.settings is mock_settings

    async def test_get_samples_success(self, service):
        """Test get_samples with successful repository call."""
        mock_samples = [
            Mock(spec=Sample, id=Mock(name="sample1")),
            Mock(spec=Sample, id=Mock(name="sample2"))
        ]
        service.repository.get_samples = AsyncMock(return_value=mock_samples)
        
        result = await service.get_samples(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert len(result) == 2
        service.repository.get_samples.assert_called_once()

    async def test_get_samples_pagination_limit_enforced(self, service, mock_settings):
        """Test that pagination limit is enforced."""
        mock_settings.pagination.max_page_size = 100
        service.repository.get_samples = AsyncMock(return_value=[])
        
        await service.get_samples(filters={}, offset=0, limit=200)
        
        call_args = service.repository.get_samples.call_args
        assert call_args[0][2] == 100  # limit parameter

    async def test_get_samples_database_connection_error(self, service):
        """Test get_samples raises NotFoundError on database connection errors."""
        from app.models.errors import NotFoundError
        service.repository.get_samples = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        
        with pytest.raises(NotFoundError) as exc_info:
            await service.get_samples(filters={}, offset=0, limit=20)
        assert exc_info.value.entity == "Samples"

    async def test_get_sample_by_identifier_success(self, service):
        """Test get_sample_by_identifier with successful lookup."""
        mock_sample = Mock(spec=Sample, id=Mock(name="sample1"))
        service.repository.get_sample_by_identifier = AsyncMock(
            return_value=mock_sample
        )
        
        result = await service.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="sample1"
        )
        
        assert result is not None
        service.repository.get_sample_by_identifier.assert_called_once()

    async def test_get_sample_by_identifier_not_found(self, service):
        """Test get_sample_by_identifier when sample not found."""
        service.repository.get_sample_by_identifier = AsyncMock(return_value=None)
        
        with pytest.raises(NotFoundError):
            await service.get_sample_by_identifier(
                organization="CCDI-DCC",
                namespace="phs002431",
                name="nonexistent"
            )

