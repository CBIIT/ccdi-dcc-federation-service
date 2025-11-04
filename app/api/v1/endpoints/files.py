"""
File API routes for the CCDI Federation Service.

This module provides REST endpoints for file operations
including listing, individual retrieval, counting, and summaries.
"""

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    get_allowlist,
    get_pagination_params,
    get_file_filters,
    check_rate_limit
)
from app.core.config import Settings
from app.core.pagination import PaginationParams, PaginationInfo, build_link_header
from app.core.cache import get_cache_service
from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import (
    File,
    FileResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import NotFoundError
from app.services.file import FileService

logger = get_logger(__name__)

router = APIRouter(prefix="/file", tags=["files"])


# ============================================================================
# File Listing
# ============================================================================

@router.get(
    "",
    response_model=FileResponse,
    summary="List files",
    description="Get a paginated list of files with optional filtering"
)
async def list_files(
    request: Request,
    response: Response,
    filters: Dict[str, Any] = Depends(get_file_filters),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List files with pagination and filtering."""
    logger.info(
        "List files request",
        filters=filters,
        page=pagination.page,
        per_page=pagination.per_page,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get files
        files = await service.get_files(
            filters=filters,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Build pagination info
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_count=None
        )
        
        # Add Link header for pagination
        link_header = build_link_header(
            request=request,
            pagination=pagination_info
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        # Build response
        result = FileResponse(
            files=files,
            pagination=pagination_info
        )
        
        logger.info(
            "List files response",
            file_count=len(files),
            page=pagination.page
        )
        
        return result
        
    except Exception as e:
        logger.error("Error listing files", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Individual File Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=File,
    summary="Get file by identifier",
    description="Get a specific file by organization, namespace, and name"
)
async def get_file(
    organization: str,
    namespace: str,
    name: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a specific file by identifier."""
    logger.info(
        "Get file request",
        organization=organization,
        namespace=namespace,
        name=name,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get file
        file = await service.get_file_by_identifier(organization, namespace, name)
        
        logger.info(
            "Get file response",
            organization=organization,
            namespace=namespace,
            name=name,
            file_data=getattr(file, 'id', str(file)[:50])  # Flexible logging
        )
        
        return file
        
    except NotFoundError as e:
        logger.warning("File not found", organization=organization, namespace=namespace, name=name)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error getting file", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# File Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Count files by field",
    description="Get counts of files grouped by a specific field value"
)
async def count_files_by_field(
    field: str,
    request: Request,
    filters: Dict[str, Any] = Depends(get_file_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count files grouped by a specific field."""
    logger.info(
        "Count files by field request",
        field=field,
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get counts
        result = await service.count_files_by_field(field, filters)
        
        logger.info(
            "Count files by field response",
            field=field,
            count_items=len(result.counts)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting files by field", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# File Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Get files summary",
    description="Get summary statistics for files"
)
async def get_files_summary(
    request: Request,
    filters: Dict[str, Any] = Depends(get_file_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for files."""
    logger.info(
        "Get files summary request",
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get summary
        result = await service.get_files_summary(filters)
        
        logger.info(
            "Get files summary response",
            total_count=result.total_count
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting files summary", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")
