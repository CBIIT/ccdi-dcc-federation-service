"""
Unit tests for main application module.

Tests application creation, middleware setup, exception handlers, and route configuration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from fastapi import FastAPI, Request, status
from fastapi.exceptions import HTTPException, RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response

from app.main import (
    create_app,
    lifespan,
    setup_middleware,
    setup_routers,
    setup_exception_handlers,
    setup_health_check,
    setup_custom_docs_endpoint,
    _suggest_correct_path,
    app
)
from app.models.errors import ErrorKind, ErrorDetail, ErrorsResponse, CCDIException
from app.db.memgraph import DatabaseConnectionError
from app.core.config import Settings


@pytest.mark.unit
class TestMainApp:
    """Test cases for main application creation."""

    @patch('app.main.get_settings')
    @patch('app.main.memgraph_lifespan')
    @patch('app.main.redis_lifespan')
    def test_create_app(self, mock_redis_lifespan, mock_memgraph_lifespan, mock_get_settings):
        """Test application creation."""
        mock_settings = Mock(spec=Settings)
        mock_settings.cors.enabled = True
        mock_settings.cors.allowed_origins = ["*"]
        mock_settings.cors.allow_credentials = True
        mock_settings.cors.allowed_methods = ["*"]
        mock_settings.cors.allowed_headers = ["*"]
        mock_get_settings.return_value = mock_settings
        
        # Mock lifespan context managers
        mock_memgraph_lifespan.return_value.__aenter__ = AsyncMock()
        mock_memgraph_lifespan.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_redis_lifespan.return_value.__aenter__ = AsyncMock()
        mock_redis_lifespan.return_value.__aexit__ = AsyncMock(return_value=None)
        
        app_instance = create_app()
        
        assert isinstance(app_instance, FastAPI)
        assert app_instance.title == "CCDI Federation Service"

    @patch('app.main.get_settings')
    def test_create_app_cors_disabled(self, mock_get_settings):
        """Test application creation with CORS disabled."""
        mock_settings = Mock(spec=Settings)
        mock_settings.cors.enabled = False
        mock_get_settings.return_value = mock_settings
        
        app_instance = create_app()
        
        assert isinstance(app_instance, FastAPI)

    @patch('app.main.get_settings')
    @patch('app.main.memgraph_lifespan')
    @patch('app.main.redis_lifespan')
    async def test_lifespan(self, mock_redis_lifespan, mock_memgraph_lifespan, mock_get_settings):
        """Test application lifespan context manager."""
        mock_settings = Mock(spec=Settings)
        mock_get_settings.return_value = mock_settings
        
        mock_app = Mock(spec=FastAPI)
        
        # Mock lifespan context managers
        mock_memgraph_ctx = AsyncMock()
        mock_memgraph_ctx.__aenter__ = AsyncMock()
        mock_memgraph_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_memgraph_lifespan.return_value = mock_memgraph_ctx
        
        mock_redis_ctx = AsyncMock()
        mock_redis_ctx.__aenter__ = AsyncMock()
        mock_redis_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_redis_lifespan.return_value = mock_redis_ctx
        
        async with lifespan(mock_app):
            pass
        
        mock_memgraph_lifespan.assert_called_once_with(mock_settings)
        mock_redis_lifespan.assert_called_once_with(mock_settings)


@pytest.mark.unit
class TestSuggestCorrectPath:
    """Test cases for path typo detection."""

    def test_suggest_correct_path_by2_race2(self):
        """Test detecting typo pattern: /subject/by2/race2/count."""
        path = "/api/v1/subject/by2/race2/count"
        result = _suggest_correct_path(path)
        
        assert result == "/api/v1/subject/by/race/count"

    def test_suggest_correct_path_by2_only(self):
        """Test detecting typo pattern: /subject/by2/race/count."""
        path = "/api/v1/subject/by2/race/count"
        result = _suggest_correct_path(path)
        
        assert result == "/api/v1/subject/by/race/count"

    def test_suggest_correct_path_b1y(self):
        """Test detecting typo pattern: /subject/b1y/sex/count."""
        path = "/api/v1/subject/b1y/sex/count"
        result = _suggest_correct_path(path)
        
        assert result == "/api/v1/subject/by/sex/count"

    def test_suggest_correct_path_by1(self):
        """Test detecting typo pattern: /subject/by1/race/count."""
        path = "/api/v1/subject/by1/race/count"
        result = _suggest_correct_path(path)
        
        assert result == "/api/v1/subject/by/race/count"

    def test_suggest_correct_path_no_typo(self):
        """Test path with no typo returns None."""
        # Use a path that doesn't match any typo pattern
        path = "/api/v1/subject/list"
        result = _suggest_correct_path(path)
        
        assert result is None

    def test_suggest_correct_path_invalid_pattern(self):
        """Test path that doesn't match any pattern returns None."""
        path = "/api/v1/subject/invalid/path"
        result = _suggest_correct_path(path)
        
        assert result is None


@pytest.mark.unit
class TestSetupMiddleware:
    """Test cases for middleware setup."""

    @patch('app.main.get_settings')
    def test_setup_middleware_cors_enabled(self, mock_get_settings):
        """Test middleware setup with CORS enabled."""
        mock_settings = Mock(spec=Settings)
        mock_settings.cors.enabled = True
        mock_settings.cors.allowed_origins = ["*"]
        mock_settings.cors.allow_credentials = True
        mock_settings.cors.allowed_methods = ["*"]
        mock_settings.cors.allowed_headers = ["*"]
        mock_get_settings.return_value = mock_settings
        
        mock_app = Mock(spec=FastAPI)
        setup_middleware(mock_app, mock_settings)
        
        # Should add CORS and GZip middleware
        assert mock_app.add_middleware.call_count >= 2

    @patch('app.main.get_settings')
    def test_setup_middleware_cors_disabled(self, mock_get_settings):
        """Test middleware setup with CORS disabled."""
        mock_settings = Mock(spec=Settings)
        mock_settings.cors.enabled = False
        mock_get_settings.return_value = mock_settings
        
        mock_app = Mock(spec=FastAPI)
        setup_middleware(mock_app, mock_settings)
        
        # Should only add GZip middleware
        assert mock_app.add_middleware.call_count >= 1


@pytest.mark.unit
class TestSetupRouters:
    """Test cases for router setup."""

    def test_setup_routers(self):
        """Test router setup."""
        mock_app = Mock(spec=FastAPI)
        setup_routers(mock_app)
        
        # Should include multiple routers
        assert mock_app.include_router.call_count >= 8


@pytest.mark.unit
class TestSetupHealthCheck:
    """Test cases for health check setup."""

    def test_setup_health_check(self):
        """Test health check endpoint setup."""
        mock_app = Mock(spec=FastAPI)
        setup_health_check(mock_app)
        
        # Should add health check endpoints
        assert mock_app.get.call_count >= 3


@pytest.mark.unit
class TestExceptionHandlers:
    """Test cases for exception handlers."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.method = "GET"
        request.url.path = "/api/v1/subject/by/race/count"
        request.url.scheme = "http"
        request.url.netloc = "localhost:8000"
        request.headers = {}
        return request

    async def test_request_validation_exception_handler(self, mock_request):
        """Test RequestValidationError handler."""
        app_instance = create_app()
        
        exc = RequestValidationError(errors=[])
        response = await app_instance.exception_handlers[RequestValidationError](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert isinstance(response, Response)

    async def test_database_connection_error_handler(self, mock_request):
        """Test DatabaseConnectionError handler."""
        app_instance = create_app()
        
        exc = DatabaseConnectionError("Connection failed")
        response = await app_instance.exception_handlers[DatabaseConnectionError](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)

    async def test_ccdi_exception_handler(self, mock_request):
        """Test CCDIException handler."""
        app_instance = create_app()
        
        exc = CCDIException(
            kind=ErrorKind.NOT_FOUND,
            status_code=status.HTTP_404_NOT_FOUND
        )
        response = await app_instance.exception_handlers[CCDIException](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)

    async def test_starlette_http_exception_handler_404(self, mock_request):
        """Test StarletteHTTPException handler for 404."""
        app_instance = create_app()
        
        exc = StarletteHTTPException(status_code=404, detail="Not found")
        response = await app_instance.exception_handlers[StarletteHTTPException](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)

    async def test_starlette_http_exception_handler_400(self, mock_request):
        """Test StarletteHTTPException handler for 400."""
        app_instance = create_app()
        
        exc = StarletteHTTPException(status_code=400, detail="Bad request")
        response = await app_instance.exception_handlers[StarletteHTTPException](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert isinstance(response, Response)

    async def test_http_exception_handler_404(self, mock_request):
        """Test HTTPException handler for 404."""
        app_instance = create_app()
        
        exc = HTTPException(status_code=404, detail="Not found")
        response = await app_instance.exception_handlers[HTTPException](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)

    async def test_http_exception_handler_400(self, mock_request):
        """Test HTTPException handler for 400."""
        app_instance = create_app()
        
        # Use a path that won't trigger typo detection
        mock_request.url.path = "/api/v1/subject/list"
        
        exc = HTTPException(status_code=400, detail="Bad request")
        response = await app_instance.exception_handlers[HTTPException](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert isinstance(response, Response)

    async def test_generic_exception_handler(self, mock_request):
        """Test generic Exception handler."""
        app_instance = create_app()
        
        exc = ValueError("Test error")
        response = await app_instance.exception_handlers[Exception](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)

    async def test_request_validation_exception_handler_count_endpoint_enum_error(self, mock_request):
        """Test RequestValidationError handler for count endpoint enum error."""
        app_instance = create_app()
        
        mock_request.url.path = "/api/v1/subject/by/invalid_field/count"
        
        exc = RequestValidationError(errors=[{
            "type": "enum",
            "loc": ("path", "field"),
            "msg": "Invalid enum value"
        }])
        
        response = await app_instance.exception_handlers[RequestValidationError](
            mock_request, exc
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert isinstance(response, Response)

    async def test_http_exception_handler_path_typo(self, mock_request):
        """Test HTTPException handler with path typo detection."""
        app_instance = create_app()
        
        mock_request.url.path = "/api/v1/subject/by2/race2/count"
        
        exc = HTTPException(status_code=400, detail="Bad request")
        response = await app_instance.exception_handlers[HTTPException](
            mock_request, exc
        )
        
        # Should detect typo and return 404 InvalidRoute
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert isinstance(response, Response)


@pytest.mark.unit
class TestSetupCustomDocsEndpoint:
    """Test cases for custom documentation endpoints."""

    @patch('pathlib.Path.open')
    @patch('pathlib.Path.exists')
    def test_setup_custom_docs_endpoint(self, mock_exists, mock_open):
        """Test custom docs endpoint setup."""
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_file.read.return_value = "<html></html>"
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        mock_open.return_value = mock_file
        
        mock_app = Mock(spec=FastAPI)
        setup_custom_docs_endpoint(mock_app)
        
        # Should add /docs and /docs-embedded endpoints
        assert mock_app.get.call_count >= 2

    @patch('pathlib.Path.open')
    @patch('pathlib.Path.exists')
    async def test_serve_custom_docs_success(self, mock_exists, mock_open):
        """Test serving custom docs successfully."""
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_file.read.return_value = "url: 'https://cbiit.github.io/ccdi-dcc-federation-service/docs/swagger.yml',"
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        mock_open.return_value = mock_file
        
        app_instance = create_app()
        
        mock_request = Mock(spec=Request)
        mock_request.url.scheme = "http"
        mock_request.url.netloc = "localhost:8000"
        mock_request.headers = {}
        
        # Get the endpoint function
        for route in app_instance.routes:
            if hasattr(route, 'path') and route.path == "/docs":
                endpoint_func = route.endpoint
                response = await endpoint_func(mock_request)
                assert response.status_code == 200
                break

    @patch('pathlib.Path.open')
    @patch('pathlib.Path.exists')
    async def test_serve_custom_docs_file_not_found(self, mock_exists, mock_open):
        """Test serving custom docs when file not found."""
        mock_exists.return_value = False
        mock_open.side_effect = FileNotFoundError()
        
        app_instance = create_app()
        
        mock_request = Mock(spec=Request)
        mock_request.url.scheme = "http"
        mock_request.url.netloc = "localhost:8000"
        mock_request.headers = {}
        
        # Get the endpoint function
        for route in app_instance.routes:
            if hasattr(route, 'path') and route.path == "/docs":
                endpoint_func = route.endpoint
                with pytest.raises(HTTPException):
                    await endpoint_func(mock_request)
                break

