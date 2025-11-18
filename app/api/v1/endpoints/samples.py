"""
Sample API routes for the CCDI Federation Service.

This module provides REST endpoints for sample operations
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
    get_sample_filters,
    get_sample_diagnosis_filters,
    check_rate_limit
)
from app.core.config import Settings
from app.core.pagination import PaginationParams, PaginationInfo, build_link_header
from app.core.cache import get_cache_service
from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import (
    Sample,
    SampleResponse,
    SamplesResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import NotFoundError
from app.services.sample import SampleService

logger = get_logger(__name__)

router = APIRouter(prefix="/sample", tags=["samples"])


# ============================================================================
# Sample Listing
# ============================================================================

@router.get(
    "",
    response_model=SamplesResponse,
    summary="List samples",
    description="Get a paginated list of samples with optional filtering"
)
async def list_samples(
    request: Request,
    response: Response,
    filters: Dict[str, Any] = Depends(get_sample_filters),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List samples with pagination and filtering."""
    logger.info(
        "List samples request",
        filters=filters,
        page=pagination.page,
        per_page=pagination.per_page,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get samples
        samples = await service.get_samples(
            filters=filters,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Build pagination info
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_items=len(samples),
            has_next=len(samples) == pagination.per_page,  # If we got a full page, there might be more
            has_prev=pagination.page > 1
        )
        
        # Add Link header for pagination
        link_header = build_link_header(
            request=request,
            pagination=pagination_info,
            extra_params=dict(request.query_params)
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        logger.info(
            "List samples response",
            sample_count=len(samples),
            page=pagination.page
        )
        
        # Exclude gateways from individual samples (keep as placeholder in code)
        samples_dicts = [sample.model_dump(exclude={'gateways'}) if hasattr(sample, 'model_dump') else {k: v for k, v in (sample if isinstance(sample, dict) else sample.__dict__).items() if k != 'gateways'} for sample in samples]
        
        # Build response
        result = SamplesResponse(
            samples=samples_dicts
        )
        
        return result
        
    except Exception as e:
        logger.error("Error listing samples", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Individual Sample Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=Sample,
    summary="Get sample by identifier",
    description="Get a specific sample by organization, namespace, and name"
)
async def get_sample(
    organization: str,
    namespace: str,
    name: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a specific sample by identifier."""
    logger.info(
        "Get sample request",
        organization=organization,
        namespace=namespace,
        name=name,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get sample
        sample = await service.get_sample_by_identifier(organization, namespace, name)
        
        logger.info(
            "Get sample response",
            organization=organization,
            namespace=namespace,
            name=name,
            sample_data=getattr(sample, 'id', str(sample)[:50])  # Flexible logging
        )
        
        # Return sample dict excluding gateways (keep as placeholder in code)
        return sample.model_dump(exclude={'gateways'})
        
    except NotFoundError as e:
        logger.warning("Sample not found", organization=organization, namespace=namespace, name=name)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error getting sample", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Sample Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Count samples by field",
    description="Get counts of samples grouped by a specific field value"
)
async def count_samples_by_field(
    field: str,
    request: Request,
    filters: Dict[str, Any] = Depends(get_sample_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count samples grouped by a specific field."""
    logger.info(
        "Count samples by field request",
        field=field,
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get counts
        result = await service.count_samples_by_field(field, filters)
        
        logger.info(
            "Count samples by field response",
            field=field,
            count_items=len(result.counts)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting samples by field", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Sample Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Get samples summary",
    description="Get summary statistics for samples"
)
async def get_samples_summary(
    request: Request,
    filters: Dict[str, Any] = Depends(get_sample_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for samples."""
    logger.info(
        "Get samples summary request",
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get summary
        result = await service.get_samples_summary(filters)
        
        logger.info(
            "Get samples summary response",
            total_count=result.total_count
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting samples summary", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Sample Diagnosis Search Endpoints
# ============================================================================

@router.get(
    "/diagnosis/search",
    response_model=SampleResponse,
    summary="Search samples by diagnosis",
    description="Search samples with diagnosis filtering"
)
async def search_samples_by_diagnosis(
    request: Request,
    response: Response,
    filters: Dict[str, Any] = Depends(get_sample_diagnosis_filters),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Search samples with diagnosis filtering."""
    logger.info(
        "Search samples by diagnosis request",
        filters=filters,
        page=pagination.page,
        per_page=pagination.per_page,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get samples
        samples = await service.get_samples(
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
        
        # Add Link header
        link_header = build_link_header(
            request=request,
            pagination=pagination_info
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        # Build response
        result = SampleResponse(
            samples=samples,
            pagination=pagination_info
        )
        
        logger.info(
            "Search samples by diagnosis response",
            sample_count=len(samples),
            page=pagination.page
        )
        
        return result
        
    except Exception as e:
        logger.error("Error searching samples by diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/diagnosis/by/{field}/count",
    response_model=CountResponse,
    summary="Count samples by field with diagnosis search",
    description="Count samples by field with diagnosis filtering"
)
async def count_samples_by_field_with_diagnosis(
    field: str,
    request: Request,
    filters: Dict[str, Any] = Depends(get_sample_diagnosis_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count samples by field with diagnosis filtering."""
    logger.info(
        "Count samples by field with diagnosis request",
        field=field,
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get counts
        result = await service.count_samples_by_field(field, filters)
        
        logger.info(
            "Count samples by field with diagnosis response",
            field=field,
            count_items=len(result.counts)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting samples by field with diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/diagnosis/summary",
    response_model=SummaryResponse,
    summary="Get samples summary with diagnosis search",
    description="Get summary statistics for samples with diagnosis filtering"
)
async def get_samples_summary_with_diagnosis(
    request: Request,
    filters: Dict[str, Any] = Depends(get_sample_diagnosis_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for samples with diagnosis filtering."""
    logger.info(
        "Get samples summary with diagnosis request",
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get summary
        result = await service.get_samples_summary(filters)
        
        logger.info(
            "Get samples summary with diagnosis response",
            total_count=result.total_count
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting samples summary with diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")
