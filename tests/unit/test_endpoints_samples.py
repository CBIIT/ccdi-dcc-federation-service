"""
Unit tests for sample API endpoints.

Tests sample listing, retrieval, counting, and summary endpoints.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.samples import (
    list_samples,
    get_sample,
    count_samples_by_field,
    get_samples_summary,
    router as samples_router
)
from app.models.dto import SamplesResponse, SampleResponse, CountResponse, SummaryResponse
from app.models.errors import ErrorKind, InvalidParametersError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestSampleEndpoints:
    """Test cases for sample API endpoints."""

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
        request.url.path = "/sample"
        # Create a dict-like object that supports both dict() conversion and .keys()
        class QueryParams(dict):
            def __init__(self, params):
                super().__init__(params)
                self._params = params
            
            def keys(self):
                return self._params.keys()
        
        request.query_params = QueryParams({"page": "1", "per_page": "20"})
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

    async def test_list_samples_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_samples returns samples successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        class MockSample:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {
                    "id": {"name": "sample1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "subject": {
                        "name": "subject1",
                        "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}
                    },
                    "metadata": {}
                }
        
        mock_samples = [MockSample()]
        mock_summary = SummaryResponse(counts=SummaryCounts(total=100))
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples = AsyncMock(return_value=mock_samples)
            mock_service.get_samples_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    result = await list_samples(
                        request=mock_request,
                        response=mock_response,
                        filters={},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SamplesResponse)
        assert len(result.data) == 1
        # SamplesResponse summary is a dict
        assert isinstance(result.summary, dict)
        # Access summary counts
        assert result.summary["counts"]["all"] == 100

    async def test_list_samples_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_samples returns 404 (No data found) on database connection errors."""
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await list_samples(
                            request=mock_request,
                            response=mock_response,
                            filters={},
                            pagination=mock_pagination,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert exc_info.value.detail["errors"][0]["kind"] == "NotFound"
        assert exc_info.value.detail["errors"][0]["entity"] == "Samples"

    async def test_get_sample_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_sample returns a single sample."""
        class MockSample:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {
                    "id": {"name": "sample1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "subject": {
                        "name": "subject1",
                        "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}
                    },
                    "metadata": {}
                }
        
        mock_sample = MockSample()
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_sample_by_identifier = AsyncMock(return_value=mock_sample)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    result = await get_sample(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="sample1",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, dict)
        assert result["id"]["name"] == "sample1"

    async def test_get_sample_not_found(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_sample raises NotFoundError when sample not found."""
        from app.models.errors import NotFoundError
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_sample_by_identifier = AsyncMock(return_value=None)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    # The endpoint raises NotFoundError which becomes HTTPException
                    with pytest.raises(HTTPException) as exc_info:
                        await get_sample(
                            organization="CCDI-DCC",
                            namespace="phs002431",
                            name="nonexistent",
                            request=mock_request,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_count_samples_by_field_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test count_samples_by_field returns count successfully."""
        from app.models.dto import CountResponse
        
        # The count endpoint doesn't accept query parameters
        # Create a new request without query params
        class QueryParams(dict):
            def __init__(self, params):
                super().__init__(params)
                self._params = params
            
            def keys(self):
                return self._params.keys()
        
        mock_request.query_params = QueryParams({})  # Empty query params
        
        # The service returns a CountResponse object with total, missing, and values attributes
        mock_count_response = CountResponse(
            total=1000,
            missing=0,
            values=[{"value": "Tumor", "count": 500}, {"value": "Normal", "count": 500}]
        )
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.count_samples_by_field = AsyncMock(return_value=mock_count_response)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    result = await count_samples_by_field(
                        field="tissue_type",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, CountResponse)
        assert result.total == 1000
        assert len(result.values) == 2
        # CountResult is a Pydantic model, access as object attributes
        assert result.values[0].value == "Tumor"
        assert result.values[0].count == 500

    async def test_get_samples_summary_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_samples_summary returns summary successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        mock_summary = SummaryResponse(counts=SummaryCounts(total=500))
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    result = await get_samples_summary(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500

    async def test_get_samples_summary_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_samples_summary handles database connection errors."""
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_samples_summary = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.samples.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_samples_summary(
                            request=mock_request,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

