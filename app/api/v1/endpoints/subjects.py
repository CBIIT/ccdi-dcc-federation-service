"""
Subject API routes for the CCDI Federation Service.

This module provides REST endpoints for subject operations
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
    get_subject_filters,
    get_subject_diagnosis_filters,
    check_rate_limit
)
from app.core.config import Settings
from app.core.pagination import PaginationParams, PaginationInfo, build_link_header, calculate_pagination_info
from app.core.cache import get_cache_service
from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import (
    Subject,
    SubjectResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import NotFoundError
from app.services.subject import SubjectService

logger = get_logger(__name__)

router = APIRouter(prefix="/subject", tags=["subjects"])


# ============================================================================
# Subject Listing
# ============================================================================

@router.get(
    "",
    response_model=SubjectResponse,
    summary="List subjects",
    description="Get a paginated list of subjects with optional filtering"
)
async def list_subjects(
    request: Request,
    response: Response,
    filters: Dict[str, Any] = Depends(get_subject_filters),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List subjects with pagination and filtering."""
    logger.info(
        "List subjects request",
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
        
        # Get total count for summary
        summary_result = await service.get_subjects_summary(filters)
        total_count = summary_result.counts.total
        
        # Calculate pagination info using the utility function
        pagination_info = calculate_pagination_info(
            page=pagination.page,
            per_page=pagination.per_page,
            total_items=total_count
        )
        
        # Add Link header for pagination
        link_header = build_link_header(
            request=request,
            pagination=pagination_info
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        # Build nested response structure
        result = SubjectResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique participants
                    "current": len(subjects)
                }
            },
            data=subjects,
            pagination=pagination_info
        )
        
        logger.info(
            "List subjects response",
            subject_count=len(subjects),
            page=pagination.page
        )
        
        return result
        
    except Exception as e:
        logger.error("Error listing subjects", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Individual Subject Retrieval
# ============================================================================

@router.get(
    "/{org}/{ns}/{name}",
    response_model=None,  # Will return different types based on input
    summary="Get subject by identifier",
    description="Get a specific subject by organization, namespace, and name. For CCDI-DCC/phs (case-insensitive) with multiple participant IDs, returns SubjectResponse format."
)
async def get_subject(
    org: str,
    ns: str,
    name: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a specific subject by identifier."""
    logger.info(
        "Get subject request",
        org=org,
        ns=ns,
        name=name,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Check if this is a participant ID search (org must be CCDI-DCC, ns must be phs case-insensitive)
        if org == "CCDI-DCC" and ns.lower() == "phs":
            # Use participant ID search logic
            logger.info("Using participant ID search for CCDI-DCC/phs")
            
            # Parse participant IDs (split by comma and clean up)
            participant_id_list = [pid.strip() for pid in name.split(',') if pid.strip()]
            
            if not participant_id_list:
                raise HTTPException(status_code=400, detail="At least one participant ID must be provided")
            
            # Create filters for participant IDs
            filters = {"identifiers": participant_id_list}
            
            # Get subjects using the search method
            subjects = await service.get_subjects(
                filters=filters,
                offset=0,
                limit=100  # Allow multiple results
            )
            
            if not subjects:
                raise NotFoundError(f"Subject not found: {org}.{ns}.{name}")
            
            # If only one participant ID, return single Subject
            if len(participant_id_list) == 1:
                subject = subjects[0]
                logger.info(
                    "Get subject response (single participant ID)",
                    org=org,
                    ns=ns,
                    name=name,
                    subject_data=getattr(subject, 'id', str(subject)[:50])
                )
                return subject
            else:
                # If multiple participant IDs, return SubjectResponse format
                from app.core.pagination import calculate_pagination_info
                from app.models.dto import SubjectResponse
                
                # Build pagination based on matched subjects (single page)
                matched_count = len(subjects)
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=matched_count if matched_count > 0 else 0,
                    total_items=matched_count
                )
                
                # Build nested response structure
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": matched_count,
                            "current": matched_count
                        }
                    },
                    data=subjects,
                    pagination=pagination_info
                )
                
                logger.info(
                    "Get subject response (multiple participant IDs)",
                    org=org,
                    ns=ns,
                    name=name,
                    subject_count=len(subjects)
                )
                
                return result
        else:
            # Use original identifier search logic
            logger.info("Using identifier search for non-CCDI-DCC")
            
            # Get subject
            subject = await service.get_subject_by_identifier(org, ns, name)
            
            logger.info(
                "Get subject response",
                org=org,
                ns=ns,
                name=name,
                subject_data=getattr(subject, 'id', str(subject)[:50])  # Flexible logging
            )
            
            return subject
        
    except NotFoundError as e:
        logger.warning("Subject not found", org=org, ns=ns, name=name)
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error getting subject", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Subject Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Count subjects by field",
    description="Get counts of subjects grouped by a specific field value"
)
async def count_subjects_by_field(
    field: str,
    request: Request,
    filters: Dict[str, Any] = Depends(get_subject_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count subjects grouped by a specific field."""
    logger.info(
        "Count subjects by field request",
        field=field,
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get counts
        result = await service.count_subjects_by_field(field, filters)
        
        logger.info(
            "Count subjects by field response",
            field=field,
            count_items=len(result.counts)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting subjects by field", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Subject Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Get subjects summary",
    description="Get summary statistics for subjects"
)
async def get_subjects_summary(
    request: Request,
    filters: Dict[str, Any] = Depends(get_subject_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for subjects."""
    logger.info(
        "Get subjects summary request",
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get summary
        result = await service.get_subjects_summary(filters)
        
        logger.info(
            "Get subjects summary response",
            total_count=result.counts.total
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting subjects summary", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Subject Diagnosis Search Endpoints
# ============================================================================

@router.get(
    "/diagnosis/search",
    response_model=SubjectResponse,
    summary="Search subjects by diagnosis",
    description="Search subjects with diagnosis filtering"
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
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/diagnosis/by/{field}/count",
    response_model=CountResponse,
    summary="Count subjects by field with diagnosis search",
    description="Count subjects by field with diagnosis filtering"
)
async def count_subjects_by_field_with_diagnosis(
    field: str,
    request: Request,
    filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count subjects by field with diagnosis filtering."""
    logger.info(
        "Count subjects by field with diagnosis request",
        field=field,
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get counts
        result = await service.count_subjects_by_field(field, filters)
        
        logger.info(
            "Count subjects by field with diagnosis response",
            field=field,
            count_items=len(result.counts)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting subjects by field with diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/diagnosis/summary",
    response_model=SummaryResponse,
    summary="Get subjects summary with diagnosis search",
    description="Get summary statistics for subjects with diagnosis filtering"
)
async def get_subjects_summary_with_diagnosis(
    request: Request,
    filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for subjects with diagnosis filtering."""
    logger.info(
        "Get subjects summary with diagnosis request",
        filters=filters,
        path=request.url.path
    )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get summary
        result = await service.get_subjects_summary(filters)
        
        logger.info(
            "Get subjects summary with diagnosis response",
            total_count=result.total_count
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting subjects summary with diagnosis", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/search/{org}/{ns}/{name}",
    response_model=SubjectResponse,
    summary="Search subjects by participant ID(s)",
    description="Search for subjects by one or more participant IDs. Multiple IDs can be separated by commas (,). Organization and namespace are hardcoded to CCDI-DCC."
)
async def search_by_participant_id(
    org: str,
    ns: str,
    name: str,
    pagination: PaginationParams = Depends(get_pagination_params),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    request: Request = None,
    response: Response = None
) -> SubjectResponse:
    """
    Search subjects by participant ID(s).
    
    Args:
        org: Organization name
        ns: Namespace name
        participant_ids: Participant ID(s) to search for, separated by commas (,)
        pagination: Pagination parameters
        session: Database session
        settings: Application settings
        allowlist: Field allowlist
        request: HTTP request
        response: HTTP response
        
    Returns:
        SubjectResponse with matching subjects
    """
    try:
        # Check rate limit
        await check_rate_limit(request)
        
        # Validate hardcoded values
        if org != "CCDI-DCC":
            raise HTTPException(status_code=400, detail="Organization must be CCDI-DCC")
        if ns != "CCDI-DCC":
            raise HTTPException(status_code=400, detail="Namespace must be CCDI-DCC")
        
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Log the search request
        logger.info(f"Searching subjects by participant IDs: org={org}, ns={ns}, participant_ids={name}, page={pagination.page}, per_page={pagination.per_page}")
        
        # Parse participant IDs (split by comma and clean up)
        participant_id_list = [pid.strip() for pid in name.split(',') if pid.strip()]
        
        if not participant_id_list:
            raise HTTPException(status_code=400, detail="At least one participant ID must be provided")
        
        # Create filters for participant IDs (use identifiers filter which maps to participant_id)
        filters = {"identifiers": participant_id_list}
        
        # Get subjects
        subjects = await service.get_subjects(
            filters=filters,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Get total count for summary
        summary_result = await service.get_subjects_summary(filters)
        total_count = summary_result.counts.total
        
        # Calculate pagination info using the utility function
        pagination_info = calculate_pagination_info(
            page=pagination.page,
            per_page=pagination.per_page,
            total_items=total_count
        )
        
        # Add Link header for pagination
        link_header = build_link_header(
            request=request,
            pagination=pagination_info
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        # Build nested response structure
        result = SubjectResponse(
            source="CCDI-DCC",
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique participants
                    "current": len(subjects)
                }
            },
            data=subjects,
            pagination=pagination_info
        )
        
        logger.info(
            "Search by participant ID response",
            org=org,
            ns=ns,
            participant_ids=participant_id_list,
            subject_count=len(subjects),
            page=pagination.page
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error searching by participant ID", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")
