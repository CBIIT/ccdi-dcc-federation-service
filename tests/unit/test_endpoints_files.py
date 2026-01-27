"""
Unit tests for file API endpoints.

Tests file listing, retrieval, counting, and summary endpoints.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.files import (
    list_files,
    get_file,
    count_files_by_field,
    get_files_summary,
    router as files_router
)
from app.models.dto import FileResponse, CountResponse, SummaryResponse
from app.models.errors import ErrorKind, InvalidParametersError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestFileEndpoints:
    """Test cases for file API endpoints."""

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
        request.url.path = "/file"
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

    async def test_list_files_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_files returns files successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        class MockFile:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {
                    "id": {"name": "file1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "samples": [{"name": "sample1"}],
                    "metadata": {}
                }
        
        mock_files = [MockFile()]
        mock_summary = SummaryResponse(counts=SummaryCounts(total=100))
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_files = AsyncMock(return_value=mock_files)
            mock_service.get_files_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    result = await list_files(
                        request=mock_request,
                        response=mock_response,
                        filters={},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        # File endpoint returns a dict structure
        assert isinstance(result, dict)
        assert "data" in result
        assert len(result["data"]) == 1
        assert result["summary"]["counts"]["all"] == 100

    async def test_list_files_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_files handles database connection errors."""
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_files = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await list_files(
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

    async def test_get_file_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_file returns a single file."""
        class MockFile:
            def model_dump(self, exclude=None, exclude_none=None, exclude_unset=None):
                return {
                    "id": {"name": "file1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "samples": [{"name": "sample1"}],
                    "metadata": {}
                }
        
        mock_file = MockFile()
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_file_by_identifier = AsyncMock(return_value=mock_file)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    result = await get_file(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="file1",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, dict)
        assert result["id"]["name"] == "file1"

    async def test_get_file_not_found(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_file raises NotFoundError when file not found."""
        from app.models.errors import NotFoundError
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_file_by_identifier = AsyncMock(return_value=None)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    # The endpoint raises NotFoundError which becomes HTTPException
                    with pytest.raises(HTTPException) as exc_info:
                        await get_file(
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

    async def test_count_files_by_field_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test count_files_by_field returns count successfully."""
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
            values=[{"value": "BAM", "count": 500}, {"value": "FASTQ", "count": 500}]
        )
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.count_files_by_field = AsyncMock(return_value=mock_count_response)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    result = await count_files_by_field(
                        field="type",
                        request=mock_request,
                        filters={},
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, CountResponse)
        assert result.total == 1000
        assert len(result.values) == 2
        # CountResult is a Pydantic model, access as object attributes
        assert result.values[0].value == "BAM"
        assert result.values[0].count == 500

    async def test_get_files_summary_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_files_summary returns summary successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        mock_summary = SummaryResponse(counts=SummaryCounts(total=500))
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_files_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    result = await get_files_summary(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500

    async def test_get_files_summary_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_files_summary handles database connection errors."""
        # Mock request with no query parameters
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_files_summary = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.files.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_files_summary(
                            request=mock_request,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

