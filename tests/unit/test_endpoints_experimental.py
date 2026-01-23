"""
Unit tests for experimental API endpoints.

Tests experimental diagnosis search endpoints for subjects and samples.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.experimental import (
    search_samples_by_diagnosis,
    search_subjects_by_diagnosis,
    router as experimental_router
)
from app.models.dto import SamplesResponse, SubjectResponse
from app.models.errors import ErrorKind, InvalidParametersError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestExperimentalEndpoints:
    """Test cases for experimental API endpoints."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        return settings

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock()
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.url.path = "/experimental/sample-diagnosis"
        # Create a dict-like object for query_params that works with dict() conversion
        class QueryParams:
            def __init__(self, params):
                self._params = params
            
            def keys(self):
                return self._params.keys()
            
            def __iter__(self):
                return iter(self._params.items())
        
        request.query_params = QueryParams({"search": "cancer", "page": "1", "per_page": "20"})
        return request

    @pytest.fixture
    def mock_response(self):
        """Create a mock response."""
        response = Mock(spec=Response)
        response.headers = {}
        return response

    @pytest.fixture
    def mock_pagination(self):
        """Create mock pagination params."""
        from app.core.pagination import PaginationParams
        return PaginationParams(page=1, per_page=20)

    async def test_search_samples_by_diagnosis_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_samples_by_diagnosis returns samples."""
        from app.models.dto import Sample, SummaryResponse, SummaryCounts
        
        # Create a proper mock that has model_dump method
        class MockSample:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {"id": {"name": "sample1"}}
        
        mock_samples = [MockSample()]
        
        mock_summary = SummaryResponse(counts=SummaryCounts(total=100))
        
        with patch('app.api.v1.endpoints.experimental.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples = AsyncMock(return_value=mock_samples)
            mock_service.get_samples_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.experimental.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
                    result = await search_samples_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SamplesResponse)
        assert len(result.data) == 1
        assert result.summary.counts.all == 100
        assert result.summary.counts.current == 1

    async def test_search_samples_by_diagnosis_invalid_params(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_samples_by_diagnosis rejects invalid parameters."""
        # Override query_params for this test
        class QueryParams:
            def __init__(self, params):
                self._params = params
            
            def keys(self):
                return self._params.keys()
        
        mock_request.query_params = QueryParams({"invalid_param": "value", "search": "cancer"})
        
        with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await search_samples_by_diagnosis(
                    request=mock_request,
                    response=mock_response,
                    filters={"search": "cancer"},
                    pagination=mock_pagination,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
            
            # Should raise HTTPException with 400 status (InvalidParameters)
            assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_search_samples_by_diagnosis_summary_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_samples_by_diagnosis handles summary errors gracefully."""
        # Create a proper mock that has model_dump method
        class MockSample:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {"id": {"name": "sample1"}}
        
        mock_samples = [MockSample()]
        
        with patch('app.api.v1.endpoints.experimental.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples = AsyncMock(return_value=mock_samples)
            # Summary fails but should not crash the endpoint
            mock_service.get_samples_summary = AsyncMock(side_effect=Exception("Summary error"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.experimental.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
                    result = await search_samples_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        # Should still return results with total_count = 0 when summary fails
        assert isinstance(result, SamplesResponse)
        assert result.summary.counts.all == 0
        assert len(result.data) == 1  # Samples should still be returned

    async def test_search_subjects_by_diagnosis_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_subjects_by_diagnosis returns subjects."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        # Create a proper mock that has model_dump method
        # The endpoint code accesses subject.model_dump with specific parameters
        class MockSubject:
            def model_dump(self, exclude=None, exclude_none=False, exclude_unset=False):
                # Return a dict that matches what the endpoint expects
                return {
                    "id": {"name": "subject1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "kind": "Participant",
                    "metadata": {
                        "associated_diagnoses": [],
                        "vital_status": None,
                        "age_at_vital_status": None,
                        "sex": {"value": "F"},
                        "race": [{"value": "White"}]
                    }
                }
        
        mock_subjects = [MockSubject()]
        
        mock_summary = SummaryResponse(counts=SummaryCounts(total=200))
        
        with patch('app.api.v1.endpoints.experimental.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(return_value=mock_subjects)
            mock_service.get_subjects_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.experimental.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
                    result = await search_subjects_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 1
        assert result.summary.counts.all == 200
        assert result.summary.counts.current == 1

    async def test_search_subjects_by_diagnosis_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_subjects_by_diagnosis handles database connection errors."""
        with patch('app.api.v1.endpoints.experimental.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.experimental.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await search_subjects_by_diagnosis(
                            request=mock_request,
                            response=mock_response,
                            filters={"search": "cancer"},
                            pagination=mock_pagination,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_search_subjects_by_diagnosis_invalid_params(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_subjects_by_diagnosis rejects invalid parameters."""
        # Override query_params for this test
        class QueryParams:
            def __init__(self, params):
                self._params = params
            
            def keys(self):
                return self._params.keys()
        
        mock_request.query_params = QueryParams({"invalid_param": "value", "search": "cancer"})
        
        with patch('app.api.v1.endpoints.experimental.check_rate_limit', return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await search_subjects_by_diagnosis(
                    request=mock_request,
                    response=mock_response,
                    filters={"search": "cancer"},
                    pagination=mock_pagination,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
            
            # Should raise HTTPException with 400 status (InvalidParameters)
            assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

