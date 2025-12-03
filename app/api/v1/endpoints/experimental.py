"""
Experimental API routes for the CCDI Federation Service.

This module provides experimental REST endpoints for diagnosis search operations
on samples and subjects. These endpoints are marked as experimental and may change.
"""

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    get_allowlist,
    get_pagination_params,
    get_sample_diagnosis_filters,
    get_subject_diagnosis_filters,
    check_rate_limit
)
from app.core.config import Settings
from app.core.pagination import PaginationParams, PaginationInfo, build_link_header
from app.core.cache import get_cache_service
from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import (
    SampleResponse,
    SubjectResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind
from app.services.sample import SampleService
from app.services.subject import SubjectService

logger = get_logger(__name__)

router = APIRouter(tags=["Experimental"],  include_in_schema=False)


# ============================================================================
# Sample Diagnosis Search Endpoints
# ============================================================================

@router.get(
    "/sample-diagnosis",
    response_model=SampleResponse,
    summary="Search samples by diagnosis (Experimental)",
    description="Search samples with diagnosis filtering. This is an experimental endpoint and may change.",
    operation_id="sample_diagnosis_search"
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
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Samples",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )


# ============================================================================
# Subject Diagnosis Search Endpoints
# ============================================================================

@router.get(
    "/subject-diagnosis",
    response_model=SubjectResponse,
    summary="Search subjects by diagnosis (Experimental)",
    description="Search subjects with diagnosis filtering. This is an experimental endpoint and may change.",
    operation_id="subject_diagnosis_search"
)
async def search_subjects_by_diagnosis(
    request: Request,
    response: Response,
    filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Search subjects with diagnosis filtering."""
    logger.info(
        "Search subjects by diagnosis request",
        filters=filters,
        page=pagination.page,
        per_page=pagination.per_page,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get subjects
        subjects = await service.get_subjects(
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
        result = SubjectResponse(
            subjects=subjects,
            pagination=pagination_info
        )
        
        logger.info(
            "Search subjects by diagnosis response",
            subject_count=len(subjects),
            page=pagination.page
        )
        
        return result
        
    except Exception as e:
        logger.error("Error searching subjects by diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Subjects",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

