"""
CCDI Federation Service - Main Application

This is the main FastAPI application that provides REST endpoints
for querying the CCDI-DCC  graph database.
"""

from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, Request, status, Depends
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import re
from pathlib import Path

from app.core.config import get_settings, Settings
from app.core.logging import configure_logging, get_logger
from app.core.cache import redis_lifespan
from app.db.memgraph import memgraph_lifespan, DatabaseConnectionError
from app.api.v1.endpoints.subjects import router as subjects_router
from app.api.v1.endpoints.samples import router as samples_router
from app.api.v1.endpoints.files import router as files_router
from app.api.v1.endpoints.metadata import router as metadata_router
from app.api.v1.endpoints.namespaces import router as namespaces_router
from app.api.v1.endpoints.errors import router as errors_router
from app.api.v1.endpoints.root import router as root_router
from app.api.v1.endpoints.info import router as info_router
from app.api.v1.endpoints.organizations import router as organizations_router
from app.models.errors import ErrorsResponse, ErrorDetail, ErrorKind, CCDIException

# Configure logging before creating the logger
configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.
    Handles startup and shutdown events.
    """
    settings = get_settings()
    
    logger.info("Starting CCDI Federation Service")
    
    # Initialize database connection
    async with memgraph_lifespan(settings):
        # Initialize Redis cache
        async with redis_lifespan(settings):
            logger.info("All services initialized successfully")
            yield
    
    logger.info("CCDI Federation Service shut down")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    
    # Create FastAPI app
    app = FastAPI(
        title="CCDI Federation Service",
        description="REST API for querying CCDI-DCC  graph database",
        version="1.0.0",
        openapi_url="/openapi.json",
        docs_url="/docs-api",  # Default FastAPI Swagger UI at /docs-api
        redoc_url=None,  # Disable default ReDoc, using custom endpoint instead
        lifespan=lifespan
    )
    
    # Add middleware
    setup_middleware(app, settings)
    
    # Add custom docs endpoints FIRST (before exception handlers and routers) to ensure they're registered
    setup_custom_docs_endpoint(app)
    
    # Add exception handlers
    setup_exception_handlers(app)
    
    # Add routers
    setup_routers(app)
    
    # Add health check
    setup_health_check(app)
    
    logger.info("FastAPI application created")
    return app


def _suggest_correct_path(path: str) -> str:
    """
    Detect common path typos and suggest the correct path.
    
    Examples:
    - /api/v1/subject/by2/race2/count -> /api/v1/subject/by/race/count
    - /api/v1/subject/by3/sex2/count -> /api/v1/subject/by/sex/count
    - /api/v1/subject/by2/race/count -> /api/v1/subject/by/race/count
    - /api/v1/subject/b1y/sex/count -> /api/v1/subject/by/sex/count
    - /api/v1/subject/by1/race/count -> /api/v1/subject/by/race/count
    """
    # Match pattern: /subject/by{number}/{field}{number}/count
    # Pattern: /api/v1/subject/by2/race2/count
    pattern = r'(/api/v1/subject/)(by)(\d+)(/)([^/]+?)(\d+)(/count)'
    match = re.search(pattern, path)
    if match:
        prefix = match.group(1)  # /api/v1/subject/
        by_part = match.group(2)  # by
        slash = match.group(4)  # /
        field_part = match.group(5)  # race, sex, etc.
        suffix = match.group(7)  # /count
        return f"{prefix}{by_part}{slash}{field_part}{suffix}"
    
    # Also handle cases where only 'by' has a typo: /subject/by2/{field}/count
    # Pattern: /api/v1/subject/by2/race/count
    pattern_by_only = r'(/api/v1/subject/)(by)(\d+)(/[^/]+/count)'
    match_by = re.search(pattern_by_only, path)
    if match_by:
        prefix = match_by.group(1)
        by_part = match_by.group(2)
        rest = match_by.group(4)
        return f"{prefix}{by_part}{rest}"
    
    # Handle cases where 'by' has a typo with number in middle: /subject/b1y/{field}/count
    # Pattern: /api/v1/subject/b1y/sex/count or /api/v1/subject/by1/race/count
    pattern_by_typo = r'(/api/v1/subject/)(b)(\d+)(y)(/[^/]+/count)'
    match_typo = re.search(pattern_by_typo, path)
    if match_typo:
        prefix = match_typo.group(1)  # /api/v1/subject/
        b_part = match_typo.group(2)  # b
        number = match_typo.group(3)  # 1, 2, etc.
        y_part = match_typo.group(4)  # y
        rest = match_typo.group(5)  # /sex/count
        return f"{prefix}{b_part}{y_part}{rest}"
    
    # Handle pattern: /subject/{typo}/field/count where typo looks like "by"
    # Pattern: /api/v1/subject/b1y/sex/count
    pattern_typo_field_count = r'(/api/v1/subject/)([^/]+)/([^/]+)/(count)'
    match_typo_field = re.search(pattern_typo_field_count, path)
    if match_typo_field:
        prefix = match_typo_field.group(1)  # /api/v1/subject/
        first_seg = match_typo_field.group(2)  # b1y, by2, etc.
        field_seg = match_typo_field.group(3)  # sex, race, etc.
        count_seg = match_typo_field.group(4)  # count
        
        # Check if first segment looks like a typo of "by" (contains 'b' and 'y' with numbers)
        if re.match(r'^b.*y.*$', first_seg, re.IGNORECASE) and count_seg == "count":
            # Valid field names for count endpoint
            valid_fields = {"sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses"}
            if field_seg.lower() in valid_fields:
                return f"{prefix}by/{field_seg}/{count_seg}"
    
    return None


def setup_middleware(app: FastAPI, settings) -> None:
    """Set up application middleware."""
    
    # CORS middleware
    if settings.cors.enabled:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors.allowed_origins,
            allow_credentials=settings.cors.allow_credentials,
            allow_methods=settings.cors.allowed_methods,
            allow_headers=settings.cors.allowed_headers,
        )
        logger.info("CORS middleware enabled")
    
    # GZip compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    logger.info("GZip middleware enabled")


def setup_routers(app: FastAPI) -> None:
    """Set up API routers."""
    
    # Add subject routes
    app.include_router(subjects_router, prefix="/api/v1")
    
    # Add sample routes (commented out - not shown in Swagger docs)
    # app.include_router(samples_router, prefix="/api/v1")
    
    # Add file routes (commented out - not shown in Swagger docs)
    # app.include_router(files_router, prefix="/api/v1")
    
    # Add metadata routes
    app.include_router(metadata_router, prefix="/api/v1")
    
    # Add namespace routes
    app.include_router(namespaces_router, prefix="/api/v1")
    
    # Add organization routes
    app.include_router(organizations_router, prefix="/api/v1")
    
    # Add error examples routes
    app.include_router(errors_router, prefix="/api/v1")
    
    # Add root API routes - available at both /api/v1/ and /
    app.include_router(root_router, prefix="/api/v1")
    app.include_router(root_router)
    
    # Add info routes
    app.include_router(info_router, prefix="/api/v1")
    
    logger.info("API routers configured")


def setup_exception_handlers(app: FastAPI) -> None:
    """Set up global exception handlers for consistent error responses."""
    
    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle FastAPI request validation errors - convert to InvalidParameters format."""
        # Log the validation error details for debugging
        logger.warning(
            "Request validation error",
            method=request.method,
            path=str(request.url.path),
            errors=exc.errors()
        )
        
        # Return sanitized InvalidParameters error
        error_detail = ErrorDetail(
            kind=ErrorKind.INVALID_PARAMETERS,
            parameters=[],  # Empty array - don't expose parameter names
            message="Invalid query parameter(s) provided.",
            reason="Unknown query parameter(s)"
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    @app.exception_handler(DatabaseConnectionError)
    async def database_connection_error_handler(request: Request, exc: DatabaseConnectionError):
        """Handle database connection errors - return 404 Not Found (service unavailable)."""
        logger.error(
            "Database connection error",
            path=str(request.url.path),
            method=request.method,
            error=str(exc)
        )
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Resource",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    @app.exception_handler(CCDIException)
    async def ccdi_exception_handler(request: Request, exc: CCDIException):
        """Handle CCDI custom exceptions - log full details but sanitize InvalidRoute responses."""
        # If this is an InvalidRoute error, log the full details (including route)
        if exc.kind == ErrorKind.INVALID_ROUTE:
            logger.warning(
                "Invalid route requested",
                method=exc.method,
                route=exc.route or str(request.url.path),
                path=str(request.url.path)
            )
            # Return sanitized error detail (no route in response)
            error_detail = exc.to_error_detail()
        else:
            error_detail = exc.to_error_detail()
        
        return JSONResponse(
            status_code=exc.status_code,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
        """Handle Starlette HTTP exceptions (including 404s for non-existent routes)."""
        # If detail is already a structured error response, return it
        if isinstance(exc.detail, dict) and "errors" in exc.detail:
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail
            )
        
        # Exclude documentation endpoints from 404 handling
        path = str(request.url.path)
        if path in ["/redoc", "/docs", "/docs-api", "/openapi.json"]:
            # Let FastAPI handle these routes normally
            raise exc
        
        # Handle 404 errors - all 404s are treated as InvalidRoute
        if exc.status_code == 404:
            # Log the full details (including the actual route)
            logger.warning(
                "Invalid route requested",
                method=request.method,
                route=str(request.url.path),
                path=str(request.url.path)
            )
            
            # Return sanitized error detail (no route in response, generic message)
            error_detail = ErrorDetail(
                kind=ErrorKind.INVALID_ROUTE,
                method=request.method,
                route=None,  # Don't include route in response
                message="Invalid route requested."
            )
            
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # For other status codes, convert to appropriate error format
        if exc.status_code in (400, 422):
            # Sanitize error message - don't expose internal details or user inputs
            error_detail = ErrorDetail(
                kind=ErrorKind.INVALID_PARAMETERS,
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,  # Always return 400 for InvalidParameters
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # Default NotFound for other 4xx errors
        if 400 <= exc.status_code < 500:
            error_detail = ErrorDetail(
                kind=ErrorKind.NOT_FOUND,
                entity="Resource",
                message="Unable to find data for your request.",
                reason="No data found."
            )
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,  # Always return 404 for NotFound
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # For 500+ errors, convert to 404 NotFound (no 500 errors allowed)
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Resource",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """Handle FastAPI HTTP exceptions - convert to structured error format."""
        # If detail is already a structured error response, return it
        if isinstance(exc.detail, dict) and "errors" in exc.detail:
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail
            )
        
        # Exclude documentation endpoints from 404 handling
        path = str(request.url.path)
        if path in ["/redoc", "/docs", "/docs-api", "/openapi.json"]:
            # Let FastAPI handle these routes normally
            raise exc
        
        # Handle 404 errors - check if invalid route or missing resource
        if exc.status_code == 404:
            # Log the full details (including the actual route) for debugging
            logger.warning(
                "Invalid route requested",
                method=request.method,
                route=str(request.url.path),
                path=str(request.url.path)
            )
            
            # Return sanitized error detail (no route in response, generic message)
            error_detail = ErrorDetail(
                kind=ErrorKind.INVALID_ROUTE,
                method=request.method,
                route=None,  # Don't include route in response
                message="Invalid route requested."
            )
            
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # Convert to InvalidParameters error for 400/422 errors
        if exc.status_code in (400, 422):
            # Check if this might be a path typo (e.g., /subject/by2/race2/count)
            path = str(request.url.path)
            suggested_path = _suggest_correct_path(path)
            
            # If we detected a typo pattern, treat as InvalidRoute
            if suggested_path:
                # Log the full details (including the actual route and suggestion) for debugging
                logger.warning(
                    "Invalid route requested (typo detected)",
                    method=request.method,
                    route=path,
                    suggested_path=suggested_path
                )
                # Return sanitized error detail (no route in response, generic message)
                error_detail = ErrorDetail(
                    kind=ErrorKind.INVALID_ROUTE,
                    method=request.method,
                    route=None,  # Don't include route in response
                    message="Invalid route requested."
                )
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
                )
            
            # Check if error detail suggests organization/namespace validation (path typo pattern)
            error_detail_dict = exc.detail if isinstance(exc.detail, dict) else {}
            if isinstance(error_detail_dict, dict) and "errors" in error_detail_dict:
                errors_list = error_detail_dict.get("errors", [])
                # Check if any error mentions organization/namespace parameters
                for err in errors_list:
                    if isinstance(err, dict):
                        params = err.get("parameters", [])
                        if "organization" in params or "namespace" in params:
                            # Check again with the path
                            suggested_path = _suggest_correct_path(path)
                            if suggested_path:
                                # Log the full details (including the actual route and suggestion) for debugging
                                logger.warning(
                                    "Invalid route requested (typo detected)",
                                    method=request.method,
                                    route=path,
                                    suggested_path=suggested_path
                                )
                                # Return sanitized error detail (no route in response, generic message)
                                error_detail = ErrorDetail(
                                    kind=ErrorKind.INVALID_ROUTE,
                                    method=request.method,
                                    route=None,  # Don't include route in response
                                    message="Invalid route requested."
                                )
                                return JSONResponse(
                                    status_code=status.HTTP_404_NOT_FOUND,
                                    content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
                                )
            
            # Sanitize error message - don't expose internal details or user inputs
            error_detail = ErrorDetail(
                kind=ErrorKind.INVALID_PARAMETERS,
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,  # Always return 400 for InvalidParameters
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # Default NotFound for other 4xx errors
        if 400 <= exc.status_code < 500:
            error_detail = ErrorDetail(
                kind=ErrorKind.NOT_FOUND,
                entity="Resource",
                message="Unable to find data for your request.",
                reason="No data found."
            )
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,  # Always return 404 for NotFound
                content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        
        # For 500+ errors, convert to 404 NotFound (no 500 errors allowed)
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Resource",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        """Handle all unhandled exceptions - return 404 NotFound (no 500 errors allowed)."""
        logger.error(
            "Unhandled exception",
            path=str(request.url.path),
            method=request.method,
            error=str(exc),
            exc_info=True
        )
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Resource",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    logger.info("Exception handlers configured")


def setup_health_check(app: FastAPI) -> None:
    """Set up health check endpoint."""
    
    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "service": "ccdi-federation-service"}
    
    @app.get("/ping", tags=["ping"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "pong"}

    @app.get("/version", tags=["version"])
    async def version(settings: Settings = Depends(get_settings)):
        """Version endpoint."""
        # Use API_VERSION environment variable if set, otherwise fall back to settings.app_version
        api_version = os.getenv("API_VERSION", settings.app_version)
        return {"version": api_version}

    # Root endpoint is now handled by root_router (returns API root JSON)
    # Available at both /api/v1/ and /
    
    logger.info("Health check endpoints configured")


def setup_custom_docs_endpoint(app: FastAPI) -> None:
    """Set up custom Swagger documentation endpoint using embedded.html.
    
    This serves a self-contained Swagger UI with:
    - Custom styling and branding
    - Embedded OpenAPI spec (works standalone, e.g., for GitHub Pages)
    - Custom Swagger UI configuration
    """
    
    # Path to embedded.html - from app/main.py, go up 1 level to project root, then docs/
    embedded_html_path = Path(__file__).resolve().parents[1] / "docs" / "embedded.html"
    
    @app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
    async def serve_custom_docs():
        """
        Serve the custom Swagger UI documentation page at /docs.
        
        This endpoint serves the embedded.html file which contains a self-contained
        Swagger UI with the OpenAPI specification embedded inline.
        Features:
        - Custom styling and branding
        - Embedded OpenAPI spec (works standalone, e.g., for GitHub Pages)
        - Custom Swagger UI configuration
        
        The default FastAPI Swagger UI is also available at /docs-api.
        """
        try:
            with embedded_html_path.open("r", encoding="utf-8") as f:
                html_content = f.read()
            return HTMLResponse(content=html_content)
        except FileNotFoundError:
            logger.error(f"Embedded HTML file not found at {embedded_html_path}")
            raise HTTPException(
                status_code=500,
                detail="Documentation file not found"
            )
        except Exception as e:
            logger.error(f"Error serving documentation: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Error loading documentation"
            )
    
    # Custom ReDoc endpoint to ensure it works properly
    @app.get("/redoc", include_in_schema=False)
    async def serve_redoc(request: Request):
        """
        Serve ReDoc documentation page at /redoc.
        
        This is a custom implementation to ensure ReDoc works properly
        with the OpenAPI 3.1.0 specification.
        """
        logger.debug(f"Serving ReDoc at /redoc, base_url: {request.base_url}")
        try:
            # Build absolute OpenAPI URL from the request
            openapi_url = app.openapi_url
            if not openapi_url.startswith("http"):
                # Make it absolute based on the request
                base_url = str(request.base_url).rstrip("/")
                openapi_url = f"{base_url}{openapi_url}"
            
            logger.debug(f"ReDoc OpenAPI URL: {openapi_url}")
            
            # Create custom ReDoc HTML with explicit configuration
            # Using ReDoc latest version with JavaScript API for better OpenAPI 3.1.0 support
            redoc_html = f"""<!DOCTYPE html>
<html>
  <head>
    <title>{app.title} - ReDoc</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <style>
      body {{
        margin: 0;
        padding: 0;
      }}
    </style>
  </head>
  <body>
    <div id="redoc-container"></div>
    <script src="https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js"></script>
    <script>
      // Initialize ReDoc with JavaScript API
      window.addEventListener('load', function() {{
        try {{
          Redoc.init("{openapi_url}", {{
            scrollYOffset: 0,
            hideDownloadButton: false,
            expandResponses: "200,201",
            pathInMiddlePanel: true,
            theme: {{
              typography: {{
                fontSize: "14px",
                lineHeight: "1.5em",
                code: {{
                  fontSize: "13px"
                }}
              }},
              colors: {{
                primary: {{
                  main: "#32329f"
                }}
              }}
            }}
          }}, document.getElementById("redoc-container"));
          console.log("ReDoc initialized successfully");
        }} catch (error) {{
          console.error("Error initializing ReDoc:", error);
          document.getElementById("redoc-container").innerHTML = 
            '<div style="padding: 20px; color: red;">Error loading ReDoc documentation. Please check the console for details.</div>';
        }}
      }});
      
      // Error handling for ReDoc
      window.addEventListener('error', function(e) {{
        console.error('ReDoc error:', e);
      }});
    </script>
  </body>
</html>"""
            return HTMLResponse(content=redoc_html)
        except Exception as e:
            logger.error(f"Error serving ReDoc: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Error loading ReDoc documentation"
            )
    
    logger.info("Custom /docs endpoint configured (default FastAPI docs available at /docs-api)")
    logger.info("Custom /redoc endpoint configured")


# Create the application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    
    # Run the application
    uvicorn.run(
        "app.main:app",
        host=settings.app.host,
        port=settings.app.port,
        reload=settings.app.debug,
        log_config=None,  # We handle logging ourselves
        access_log=False  # We handle access logging ourselves
    )
