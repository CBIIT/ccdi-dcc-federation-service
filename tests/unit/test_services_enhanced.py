"""
Enhanced unit tests for service classes.

Tests additional edge cases, error handling, and complex scenarios
for FileService and SampleService.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, MagicMock, patch
from neo4j import AsyncSession

from app.services.file import FileService
from app.services.sample import SampleService
from app.core.config import Settings
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import CountResponse, SummaryResponse, SummaryCounts
from app.models.errors import ValidationError, NotFoundError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestFileServiceEnhanced:
    """Enhanced test cases for FileService class."""

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
        settings.query_timeout = 60
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
        """Create a FileService instance with cache."""
        return FileService(mock_session, mock_allowlist, mock_settings, mock_cache_service)

    @pytest.fixture
    def service_no_cache(self, mock_session, mock_allowlist, mock_settings):
        """Create a FileService instance without cache."""
        return FileService(mock_session, mock_allowlist, mock_settings)

    async def test_count_files_by_field_cache_hit(self, service, mock_cache_service):
        """Test count_files_by_field returns cached result."""
        cached_result = {
            "total": 100,
            "missing": 5,
            "values": [{"value": "BAM", "count": 50}]
        }
        mock_cache_service.get = AsyncMock(return_value=cached_result)
        mock_repo = AsyncMock()

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("type", {})

        assert isinstance(result, CountResponse)
        assert result.total == 100
        mock_repo.count_files_by_field.assert_not_called()

    async def test_count_files_by_field_cache_miss(self, service, mock_cache_service):
        """Test count_files_by_field with cache miss."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "BAM", "count": 50}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("type", {})

        assert result.total == 50
        mock_cache_service.set.assert_called_once()

    async def test_count_files_by_field_materialized_view_type(self, service, mock_cache_service):
        """Test count_files_by_field always uses repository (materialized view disabled)."""
        mock_count_result = {
            "total": 100,
            "missing": 0,
            "values": [{"value": "BAM", "count": 100}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("type", {})

        assert result.total == 100
        mock_repo.count_files_by_field.assert_called_once_with("type", {})

    async def test_count_files_by_field_materialized_view_depositions(self, service, mock_cache_service):
        """Test count_files_by_field always uses repository for depositions (materialized view disabled)."""
        mock_count_result = {
            "total": 200,
            "missing": 0,
            "values": [{"value": "phs002431", "count": 200}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("depositions", {})

        assert result.total == 200
        mock_repo.count_files_by_field.assert_called_once_with("depositions", {})

    async def test_count_files_by_field_materialized_view_fallback(self, service, mock_cache_service):
        """Test count_files_by_field always uses repository (materialized view disabled, no fallback needed)."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "BAM", "count": 50}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("type", {})

        assert result.total == 50
        mock_repo.count_files_by_field.assert_called_once_with("type", {})

    async def test_count_files_by_field_with_filters_uses_repository(self, service, mock_cache_service):
        """Test count_files_by_field always uses repository (materialized view disabled)."""
        mock_count_result = {
            "total": 30,
            "missing": 0,
            "values": [{"value": "BAM", "count": 30}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.count_files_by_field("type", {"file_type": "BAM"})

        assert result.total == 30
        mock_repo.count_files_by_field.assert_called_once_with("type", {"file_type": "BAM"})

    async def test_count_files_by_field_timeout(self, service, mock_cache_service):
        """Test count_files_by_field handles query timeout."""
        async def slow_count(*args, **kwargs):
            await asyncio.sleep(10)
            return {"total": 0, "missing": 0, "values": []}

        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = slow_count
        mock_cache_service.get = AsyncMock(return_value=None)

        # Set a very short timeout for testing
        service.settings.query_timeout = 0.01

        with patch.object(service, '_repos', [mock_repo]):
            with pytest.raises(ValidationError) as exc_info:
                await service.count_files_by_field("type", {})

        assert "timeout" in str(exc_info.value).lower()

    async def test_count_files_by_field_no_cache(self, service_no_cache):
        """Test count_files_by_field without cache service."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "BAM", "count": 50}]
        }
        mock_repo = AsyncMock()
        mock_repo.count_files_by_field = AsyncMock(return_value=mock_count_result)

        with patch.object(service_no_cache, '_repos', [mock_repo]):
            result = await service_no_cache.count_files_by_field("type", {})

        assert result.total == 50
        mock_repo.count_files_by_field.assert_called_once_with("type", {})

    async def test_get_files_summary_cache_hit(self, service, mock_cache_service):
        """Test get_files_summary returns cached result."""
        cached_summary = {
            "counts": {"total": 500}
        }
        mock_cache_service.get = AsyncMock(return_value=cached_summary)
        mock_repo = AsyncMock()

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.get_files_summary({})

        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500
        mock_repo.count_for_pagination.assert_not_called()

    async def test_get_files_summary_cache_miss(self, service, mock_cache_service):
        """Test get_files_summary with cache miss."""
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(return_value=1000)
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.get_files_summary({})

        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 1000
        mock_cache_service.set.assert_called_once()

    async def test_get_files_summary_database_error(self, service, mock_cache_service):
        """Test get_files_summary handles database connection errors."""
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        mock_cache_service.get = AsyncMock(return_value=None)

        with patch.object(service, '_repos', [mock_repo]):
            result = await service.get_files_summary({})

        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 0

    async def test_get_files_summary_no_cache(self, service_no_cache):
        """Test get_files_summary without cache service."""
        mock_repo = AsyncMock()
        mock_repo.count_for_pagination = AsyncMock(return_value=500)

        with patch.object(service_no_cache, '_repos', [mock_repo]):
            result = await service_no_cache.get_files_summary({})

        assert result.counts.total == 500
        mock_repo.count_for_pagination.assert_called_once()

    async def test_get_file_by_identifier_validation_error(self, service):
        """Test get_file_by_identifier with invalid parameters."""
        with pytest.raises(ValidationError):
            await service.get_file_by_identifier("", "namespace", "name")
        
        with pytest.raises(ValidationError):
            await service.get_file_by_identifier("org", "", "name")
        
        with pytest.raises(ValidationError):
            await service.get_file_by_identifier("org", "namespace", "")


@pytest.mark.unit
class TestSampleServiceEnhanced:
    """Enhanced test cases for SampleService class."""

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
        settings.cache_ttl_summary_endpoints = 600
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
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
        """Create a SampleService instance with cache."""
        return SampleService(mock_session, mock_allowlist, mock_settings, mock_cache_service)

    @pytest.fixture
    def service_no_cache(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleService instance without cache."""
        return SampleService(mock_session, mock_allowlist, mock_settings)

    async def test_count_samples_by_field_cache_hit(self, service, mock_cache_service):
        """Test count_samples_by_field returns cached result."""
        cached_result = {
            "total": 100,
            "missing": 5,
            "values": [{"value": "Tumor", "count": 50}]
        }
        mock_cache_service.get = AsyncMock(return_value=cached_result)
        service.repository.count_samples_by_field = AsyncMock()
        
        result = await service.count_samples_by_field("tissue_type", {})
        
        assert isinstance(result, CountResponse)
        assert result.total == 100
        service.repository.count_samples_by_field.assert_not_called()

    async def test_count_samples_by_field_cache_miss(self, service, mock_cache_service):
        """Test count_samples_by_field with cache miss."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "Tumor", "count": 50}]
        }
        service.repository.count_samples_by_field = AsyncMock(return_value=mock_count_result)
        mock_cache_service.get = AsyncMock(return_value=None)
        
        result = await service.count_samples_by_field("tissue_type", {})
        
        assert result.total == 50
        mock_cache_service.set.assert_called_once()

    async def test_count_samples_by_field_no_cache(self, service_no_cache):
        """Test count_samples_by_field without cache service."""
        mock_count_result = {
            "total": 50,
            "missing": 0,
            "values": [{"value": "Tumor", "count": 50}]
        }
        service_no_cache.repository.count_samples_by_field = AsyncMock(
            return_value=mock_count_result
        )
        
        result = await service_no_cache.count_samples_by_field("tissue_type", {})
        
        assert result.total == 50
        service_no_cache.repository.count_samples_by_field.assert_called_once()

    async def test_get_samples_summary_cache_hit(self, service, mock_cache_service):
        """Test get_samples_summary returns cached result."""
        cached_summary = {
            "counts": {"total": 500}
        }
        mock_cache_service.get = AsyncMock(return_value=cached_summary)
        service.repository.get_samples_summary = AsyncMock()
        
        result = await service.get_samples_summary({})
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500
        service.repository.get_samples_summary.assert_not_called()

    async def test_get_samples_summary_cache_miss(self, service, mock_cache_service):
        """Test get_samples_summary with cache miss."""
        mock_summary_result = {
            "counts": {"total": 1000}
        }
        service.repository.get_samples_summary = AsyncMock(return_value=mock_summary_result)
        mock_cache_service.get = AsyncMock(return_value=None)
        
        result = await service.get_samples_summary({})
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 1000
        mock_cache_service.set.assert_called_once()

    async def test_get_samples_summary_database_error(self, service, mock_cache_service):
        """Test get_samples_summary raises NotFoundError on database connection errors."""
        from app.models.errors import NotFoundError
        service.repository.get_samples_summary = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        mock_cache_service.get = AsyncMock(return_value=None)
        
        with pytest.raises(NotFoundError) as exc_info:
            await service.get_samples_summary({})
        assert exc_info.value.entity == "Samples"

    async def test_get_samples_summary_no_cache(self, service_no_cache):
        """Test get_samples_summary without cache service."""
        mock_summary_result = {
            "counts": {"total": 500}
        }
        service_no_cache.repository.get_samples_summary = AsyncMock(
            return_value=mock_summary_result
        )
        
        result = await service_no_cache.get_samples_summary({})
        
        assert result.counts.total == 500
        service_no_cache.repository.get_samples_summary.assert_called_once()

    async def test_get_samples_summary_total_count_format(self, service, mock_cache_service):
        """Test get_samples_summary handles different repository response formats."""
        # Test with "total_count" format
        mock_summary_result = {
            "total_count": 750
        }
        service.repository.get_samples_summary = AsyncMock(return_value=mock_summary_result)
        mock_cache_service.get = AsyncMock(return_value=None)
        
        result = await service.get_samples_summary({})
        
        assert result.counts.total == 750

    async def test_get_sample_by_identifier_validation_error(self, service):
        """Test get_sample_by_identifier with invalid parameters."""
        with pytest.raises(ValidationError):
            await service.get_sample_by_identifier("", "namespace", "name")
        
        with pytest.raises(ValidationError):
            await service.get_sample_by_identifier("org", "", "name")
        
        with pytest.raises(ValidationError):
            await service.get_sample_by_identifier("org", "namespace", "")

    async def test_get_samples_for_diagnosis_endpoint_success(self, service):
        """Test dedicated diagnosis endpoint path returns samples and total."""
        mock_samples = [Mock()]
        service.repository.get_samples_for_diagnosis_endpoint = AsyncMock(return_value=(mock_samples, 42))

        samples, total_count = await service.get_samples_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "glioma"},
            offset=5,
            limit=10,
        )

        assert samples == mock_samples
        assert total_count == 42
        service.repository.get_samples_for_diagnosis_endpoint.assert_awaited_once()

    async def test_get_samples_for_diagnosis_endpoint_diagnosis_category_only_forwards_filters(
        self, service
    ):
        """Test diagnosis_category-only requests are forwarded unchanged to repository path."""
        mock_samples = [Mock()]
        service.repository.get_samples_for_diagnosis_endpoint = AsyncMock(return_value=(mock_samples, 7))

        samples, total_count = await service.get_samples_for_diagnosis_endpoint(
            filters={
                "diagnosis_category": "Glioma",
                "_sample_diagnosis_category_substring": True,
            },
            offset=0,
            limit=15,
        )

        assert samples == mock_samples
        assert total_count == 7
        call_kwargs = service.repository.get_samples_for_diagnosis_endpoint.await_args.kwargs
        assert call_kwargs["filters"]["diagnosis_category"] == "Glioma"
        assert call_kwargs["filters"]["_sample_diagnosis_category_substring"] is True
        assert call_kwargs["offset"] == 0
        assert call_kwargs["limit"] == 15

    async def test_get_samples_for_diagnosis_endpoint_database_error(self, service):
        """Test dedicated diagnosis endpoint path maps DB errors to NotFoundError."""
        service.repository.get_samples_for_diagnosis_endpoint = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )

        with pytest.raises(NotFoundError) as exc_info:
            await service.get_samples_for_diagnosis_endpoint(filters={"_diagnosis_search": "glioma"})

        assert exc_info.value.entity == "Samples"

