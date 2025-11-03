"""
CCDI Federation Service - Main Application

This is the main FastAPI application that provides REST endpoints
for querying the CCDI graph database.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.core.cache import redis_lifespan
from app.db.memgraph import memgraph_lifespan
from app.api.v1.endpoints.subjects import router as subjects_router
from app.api.v1.endpoints.samples import router as samples_router
from app.api.v1.endpoints.files import router as files_router
from app.api.v1.endpoints.metadata import router as metadata_router
from app.api.v1.endpoints.namespaces import router as namespaces_router

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
        description="REST API for querying CCDI graph database",
        version="1.0.0",
        openapi_url="/openapi.json",
        docs_url="/docs" if settings.app.debug else None,
        redoc_url="/redoc" if settings.app.debug else None,
        lifespan=lifespan
    )
    
    # Add middleware
    setup_middleware(app, settings)
    
    # Add routers
    setup_routers(app)
    
    # Add health check
    setup_health_check(app)
    
    logger.info("FastAPI application created")
    return app


def setup_middleware(app: FastAPI, settings) -> None:
    """Set up application middleware."""
    
    # # CORS middleware
    # if settings.cors.enabled:
    #     app.add_middleware(
    #         CORSMiddleware,
    #         allow_origins=settings.cors.allowed_origins,
    #         allow_credentials=settings.cors.allow_credentials,
    #         allow_methods=settings.cors.allowed_methods,
    #         allow_headers=settings.cors.allowed_headers,
    #     )
    #     logger.info("CORS middleware enabled")
    
    # GZip compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    logger.info("GZip middleware enabled")


def setup_routers(app: FastAPI) -> None:
    """Set up API routers."""
    
    # Add subject routes
    app.include_router(subjects_router, prefix="/api/v1")
    
    # Add sample routes
    app.include_router(samples_router, prefix="/api/v1")
    
    # Add file routes
    app.include_router(files_router, prefix="/api/v1")
    
    # Add metadata routes
    app.include_router(metadata_router, prefix="/api/v1")
    
    # Add namespace routes
    app.include_router(namespaces_router, prefix="/api/v1")
    
    logger.info("API routers configured")


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

    @app.get("/", tags=["health"])
    async def root():
        """Root endpoint."""
        return {
            "service": "CCDI Federation Service",
            "version": "1.0.0",
            "status": "running",
            "docs": "/docs"
        }
    
    logger.info("Health check endpoints configured")


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
