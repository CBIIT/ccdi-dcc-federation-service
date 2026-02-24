"""
Enhanced unit tests for SubjectService class.

Tests additional edge cases, error handling, caching, and complex scenarios.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.services.subject import SubjectService
from app.core.config import Settings
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import SummaryResponse, SummaryCounts
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestSubjectServiceEnhanced:
    """Enhanced test cases for SubjectService class."""

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
        settings.subject_count_fields = ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status"]
        settings.cache = Mock()
        settings.cache.summary_ttl = 600
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
        """Create a SubjectService instance with cache."""
        return SubjectService(mock_session, mock_allowlist, mock_settings, mock_cache_service)

    @pytest.fixture
    def service_no_cache(self, mock_session, mock_allowlist, mock_settings):
        """Create a SubjectService instance without cache."""
        return SubjectService(mock_session, mock_allowlist, mock_settings)

    async def test_get_subjects_summary_for_diagnosis_endpoint_cache_hit(self, service, mock_cache_service):
        """Test get_subjects_summary_for_diagnosis_endpoint returns cached result."""
        cached_result = {
            "counts": {"total": 150}
        }
        mock_cache_service.get = AsyncMock(return_value=cached_result)
        service.repository.get_subjects_summary_for_diagnosis_endpoint = AsyncMock()
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Neuroblastoma"}
        )
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 150
        service.repository.get_subjects_summary_for_diagnosis_endpoint.assert_not_called()

    async def test_get_subjects_summary_for_diagnosis_endpoint_cache_miss(self, service, mock_cache_service):
        """Test get_subjects_summary_for_diagnosis_endpoint with cache miss."""
        mock_summary_result = {"total_count": 75}
        service.repository.get_subjects_summary_for_diagnosis_endpoint = AsyncMock(
            return_value=mock_summary_result
        )
        mock_cache_service.get = AsyncMock(return_value=None)
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Glioma"}
        )
        
        assert result.counts.total == 75
        mock_cache_service.set.assert_called_once()

    async def test_get_subjects_summary_for_diagnosis_endpoint_fallback_no_search(self, service):
        """Test get_subjects_summary_for_diagnosis_endpoint falls back when _diagnosis_search missing."""
        mock_summary = SummaryResponse(counts=SummaryCounts(total=200))
        service.get_subjects_summary = AsyncMock(return_value=mock_summary)
        service.repository.get_subjects_summary_for_diagnosis_endpoint = AsyncMock()
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(
            filters={"sex": "F"}
        )
        
        assert result.counts.total == 200
        service.get_subjects_summary.assert_called_once_with({"sex": "F"})
        service.repository.get_subjects_summary_for_diagnosis_endpoint.assert_not_called()

    async def test_get_subjects_summary_for_diagnosis_endpoint_database_error(self, service):
        """Test get_subjects_summary_for_diagnosis_endpoint handles DatabaseConnectionError."""
        service.repository.get_subjects_summary_for_diagnosis_endpoint = AsyncMock(
            side_effect=DatabaseConnectionError("Connection failed")
        )
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Cancer"}
        )
        
        assert result.counts.total == 0

    async def test_get_subjects_summary_for_diagnosis_endpoint_other_error_raises(self, service):
        """Test get_subjects_summary_for_diagnosis_endpoint raises non-connection errors."""
        service.repository.get_subjects_summary_for_diagnosis_endpoint = AsyncMock(
            side_effect=ValueError("Invalid filter")
        )
        
        with pytest.raises(ValueError):
            await service.get_subjects_summary_for_diagnosis_endpoint(
                filters={"_diagnosis_search": "Cancer"}
            )

    async def test_get_subjects_summary_for_diagnosis_endpoint_cache_format_total_count(self, service, mock_cache_service):
        """Test get_subjects_summary_for_diagnosis_endpoint handles cache with total_count format."""
        cached_result = {
            "total_count": 300
        }
        mock_cache_service.get = AsyncMock(return_value=cached_result)
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Leukemia"}
        )
        
        assert result.counts.total == 300

    async def test_get_subjects_summary_for_diagnosis_endpoint_empty_filters(self, service):
        """Test get_subjects_summary_for_diagnosis_endpoint with empty filters falls back."""
        mock_summary = SummaryResponse(counts=SummaryCounts(total=0))
        service.get_subjects_summary = AsyncMock(return_value=mock_summary)
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(filters={})
        
        assert result.counts.total == 0
        service.get_subjects_summary.assert_called_once_with({})

    async def test_get_subjects_summary_for_diagnosis_endpoint_none_filters(self, service):
        """Test get_subjects_summary_for_diagnosis_endpoint with None filters falls back."""
        mock_summary = SummaryResponse(counts=SummaryCounts(total=0))
        service.get_subjects_summary = AsyncMock(return_value=mock_summary)
        
        result = await service.get_subjects_summary_for_diagnosis_endpoint(filters=None)
        
        assert result.counts.total == 0
        service.get_subjects_summary.assert_called_once_with({})
