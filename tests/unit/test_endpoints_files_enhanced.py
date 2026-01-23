"""
Enhanced unit tests for file API endpoints.

Tests additional edge cases, complex scenarios, and error handling paths
that are not covered by existing tests.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.files import (
    list_files,
    get_file,
    count_files_by_field,
    get_files_summary
)
from app.models.dto import CountResponse, SummaryResponse, SummaryCounts
from app.models.errors import InvalidParametersError, InvalidRouteError, UnsupportedFieldError, ValidationError
from app.db.memgraph import DatabaseConnectionError
from app.core.pagination import PaginationParams


@pytest.mark.unit
class TestFileEndpointsEnhanced:
    """Enhanced test cases for file endpoints."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.method = "GET"
        request.url.path = "/api/v1/file"
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

    async def test_list_files_invalid_parameters(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_files with invalid query parameters."""
        mock_request.query_params = {"bad_param": "1"}

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

        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_list_files_database_error(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_files handles database connection errors."""
        from app.services.file import FileService

        mock_request.query_params = {}

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.get_files = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
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

    async def test_list_files_connection_error(self, mock_request, mock_response, mock_session, mock_settings, mock_allowlist, mock_pagination):
        """Test list_files handles connection-related errors."""
        from app.services.file import FileService

        mock_request.query_params = {}

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.get_files = AsyncMock(side_effect=Exception("Database connection timeout"))
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
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

    async def test_count_files_by_field_invalid_field(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_files_by_field with unsupported field."""
        mock_request.query_params = {}

        with pytest.raises(UnsupportedFieldError):
            await count_files_by_field(
                request=mock_request,
                field="invalid_field",
                filters={},
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )

    async def test_count_files_by_field_with_query_params(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_files_by_field rejects query parameters."""
        mock_request.query_params = {"foo": "bar"}
        mock_request.url.path = "/api/v1/file/by/type/count"

        with pytest.raises(InvalidRouteError):
            await count_files_by_field(
                request=mock_request,
                field="type",
                filters={},
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )

    async def test_count_files_by_field_invalid_parameters_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_files_by_field handles InvalidParametersError."""
        from app.services.file import FileService

        mock_request.query_params = {}

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.count_files_by_field = AsyncMock(
                side_effect=InvalidParametersError(parameters=[])
            )
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_files_by_field(
                        request=mock_request,
                        field="type",
                        filters={},
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )

                assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_count_files_by_field_validation_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_files_by_field handles validation errors."""
        from app.services.file import FileService

        mock_request.query_params = {}

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.count_files_by_field = AsyncMock(
                side_effect=ValidationError("Invalid input")
            )
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_files_by_field(
                        request=mock_request,
                        field="type",
                        filters={},
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )

                assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_get_file_invalid_organization(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_file with invalid organization (skipped - code bug)."""
        with pytest.raises(InvalidParametersError):
            await get_file(
                organization="INVALID",
                namespace="phs002431",
                name="file-1",
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )

    async def test_get_file_invalid_route(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_file with malformed count route."""
        mock_request.url.path = "/api/v1/file/by/type/count"

        with pytest.raises(InvalidRouteError):
            await get_file(
                organization="CCDI-DCC",
                namespace="by",
                name="count",
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )

    async def test_get_file_database_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_file handles database errors."""
        from app.services.file import FileService

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.get_file_by_identifier = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await get_file(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="file-1",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )

                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_files_summary_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_files_summary handles errors."""
        from app.services.file import FileService

        mock_request.query_params = {}

        with patch('app.api.v1.endpoints.files.FileService') as mock_service_class:
            mock_service = AsyncMock(spec=FileService)
            mock_service.get_files_summary = AsyncMock(
                side_effect=Exception("Summary failed")
            )
            mock_service_class.return_value = mock_service

            with patch('app.api.v1.endpoints.files.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await get_files_summary(
                        request=mock_request,
                        filters={},
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )

                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

