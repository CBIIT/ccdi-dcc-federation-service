"""
Enhanced unit tests for sample API endpoints.

Tests additional edge cases, complex scenarios, and error handling paths
that are not covered by existing tests.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.samples import (
    list_samples,
    get_sample,
    count_samples_by_field,
    get_samples_summary
)
from app.models.dto import SamplesResponse, SampleResponse, CountResponse, SummaryResponse, SummaryCounts
from app.models.errors import ErrorKind, InvalidParametersError, UnsupportedFieldError
from app.db.memgraph import DatabaseConnectionError
from app.core.pagination import PaginationParams


@pytest.mark.unit
class TestSampleEndpointsEnhanced:
    """Enhanced test cases for sample endpoints."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.method = "GET"
        request.url.path = "/api/v1/sample"
        request.url.scheme = "http"
        request.url.netloc = "localhost:8000"
        request.query_params = {}
        return request

    @pytest.fixture
    def mock_response(self):
        """Create a mock response."""
        response = Mock(spec=Response)
        response.headers = {}
        return response

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        return settings

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock()
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_pagination(self):
        """Create mock pagination params."""
        pagination = Mock(spec=PaginationParams)
        pagination.page = 1
        pagination.per_page = 20
        pagination.offset = 0
        return pagination

    async def test_list_samples_invalid_parameters(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_samples with invalid query parameters."""
        # Make query_params dict-like with invalid param
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=["invalid_param"])
        mock_request.query_params = mock_query_params
        
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
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_list_samples_summary_database_error(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_samples returns 404 when summary raises database connection error."""
        from app.services.sample import SampleService
        
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_request.query_params = mock_query_params
        
        # Mock service
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_sample = Mock()
            mock_sample.model_dump = Mock(return_value={
                "id": {
                    "namespace": {"organization": "CCDI-DCC", "name": "phs002431"},
                    "name": "SAMPLE-001"
                },
                "metadata": {}
            })
            mock_service.get_samples = AsyncMock(return_value=[mock_sample])
            mock_service.get_samples_summary = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
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

    async def test_list_samples_summary_connection_error(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_samples returns 404 when summary raises connection-related error."""
        from app.services.sample import SampleService
        
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_request.query_params = mock_query_params
        
        # Mock service - need proper Sample structure
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            # Create a proper mock sample with required fields
            mock_sample = Mock()
            mock_sample.model_dump = Mock(return_value={
                "id": {
                    "namespace": {"organization": "CCDI-DCC", "name": "phs002431"},
                    "name": "SAMPLE-001"
                },
                "metadata": {}
            })
            mock_service.get_samples = AsyncMock(return_value=[mock_sample])
            mock_service.get_samples_summary = AsyncMock(
                side_effect=Exception("Database connection timeout")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
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

    async def test_list_samples_summary_other_error(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_samples returns 404 when summary raises other error."""
        from app.services.sample import SampleService
        
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_request.query_params = mock_query_params
        
        # Mock service - need proper Sample structure
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            # Create a proper mock sample with required fields
            mock_sample = Mock()
            mock_sample.model_dump = Mock(return_value={
                "id": {
                    "namespace": {"organization": "CCDI-DCC", "name": "phs002431"},
                    "name": "SAMPLE-001"
                },
                "metadata": {}
            })
            mock_service.get_samples = AsyncMock(return_value=[mock_sample])
            mock_service.get_samples_summary = AsyncMock(
                side_effect=Exception("Some other error")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
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

    async def test_count_samples_by_field_unsupported_field_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_samples_by_field handles UnsupportedFieldError."""
        from app.services.sample import SampleService
        
        # Mock service to raise UnsupportedFieldError
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.count_samples_by_field = AsyncMock(
                side_effect=UnsupportedFieldError(field="invalid_field", entity_type="sample")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_samples_by_field(
                        request=mock_request,
                        field="invalid_field",
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                # UnsupportedFieldError is converted to HTTPException
                assert exc_info.value.status_code in [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]

    async def test_list_samples_query_error_path(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_samples handles query/parameter errors (lines 335-344)."""
        from app.services.sample import SampleService
        
        # Mock query_params
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_request.query_params = mock_query_params
        
        # Mock service to raise an error that matches query error keywords
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_samples = AsyncMock(
                side_effect=Exception("unbound variable 'x' in query")
            )
            mock_service.get_samples_summary = AsyncMock(
                return_value={"counts": {"total": 0}}
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
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
                
                # Should return 404 for query errors
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
                error_detail = exc_info.value.detail.get("errors", [])[0] if isinstance(exc_info.value.detail, dict) else None
                if error_detail:
                    assert error_detail.get("kind") == "NotFound"
                    assert "Query or parameter error" in error_detail.get("reason", "")

    async def test_count_samples_by_field_query_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_samples_by_field handles query errors."""
        from app.services.sample import SampleService
        
        # Mock service to raise query error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.count_samples_by_field = AsyncMock(
                side_effect=Exception("Unbound variable: x")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_samples_by_field(
                        request=mock_request,
                        field="tissue_type",
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_count_samples_by_field_other_error_with_fallback(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_samples_by_field handles other errors with fallback."""
        from app.services.sample import SampleService
        
        # Mock service to raise non-query error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.count_samples_by_field = AsyncMock(
                side_effect=Exception("Some other error")
            )
            mock_service.get_samples_summary = AsyncMock(
                return_value=SummaryResponse(counts=SummaryCounts(total=100))
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                result = await count_samples_by_field(
                    request=mock_request,
                    field="tissue_type",
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                # Should return empty result with all counted as missing
                assert isinstance(result, CountResponse)
                assert result.total == 100
                assert result.missing == 100
                assert len(result.values) == 0

    async def test_count_samples_by_field_fallback_summary_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_samples_by_field fallback when summary also fails."""
        from app.services.sample import SampleService
        
        # Mock service to raise error, and summary also fails
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.count_samples_by_field = AsyncMock(
                side_effect=Exception("Some other error")
            )
            mock_service.get_samples_summary = AsyncMock(
                side_effect=Exception("Summary also failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_samples_by_field(
                        request=mock_request,
                        field="tissue_type",
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                # Should return 404 when fallback also fails
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_sample_database_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_sample handles database connection errors."""
        from app.services.sample import SampleService
        
        # Mock service to raise database error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_sample_by_identifier = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                # get_sample raises HTTPException on error
                with pytest.raises(HTTPException) as exc_info:
                    await get_sample(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="SAMPLE-001",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_sample_connection_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_sample handles connection-related errors."""
        from app.services.sample import SampleService
        
        # Mock service to raise connection error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_sample_by_identifier = AsyncMock(
                side_effect=Exception("Database connection timeout")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.samples.get_cache_service', return_value=None):
                # get_sample raises HTTPException on error
                with pytest.raises(HTTPException) as exc_info:
                    await get_sample(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="SAMPLE-001",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_samples_summary_rejects_parameters(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_samples_summary rejects any query parameters."""
        # Mock request with query parameters - need to support len() check
        class QueryParamsWithLibraryStrategy(dict):
            def __init__(self):
                super().__init__({"library_strategy": "WXS"})
            
            def keys(self):
                return ["library_strategy"]
            
            def __len__(self):
                return 1
        
        mock_request.query_params = QueryParamsWithLibraryStrategy()
        
        # The endpoint should reject any parameters and raise InvalidParametersError
        with pytest.raises(HTTPException) as exc_info:
            await get_samples_summary(
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        error_detail = exc_info.value.detail.get("errors", [])[0] if isinstance(exc_info.value.detail, dict) else None
        if error_detail:
            assert error_detail.get("kind") == "InvalidParameters"
            assert "does not accept any query parameters" in error_detail.get("reason", "")

    async def test_get_samples_summary_no_parameters(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_samples_summary works correctly with no parameters."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
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

    async def test_get_samples_summary_database_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_samples_summary handles database connection errors."""
        from app.services.sample import SampleService
        
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        # Mock service to raise database error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
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

    async def test_get_samples_summary_connection_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_samples_summary handles connection-related errors."""
        from app.services.sample import SampleService
        
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        # Mock service to raise connection error
        with patch('app.api.v1.endpoints.samples.SampleService') as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_samples_summary = AsyncMock(
                side_effect=Exception("Database connection timeout")
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

