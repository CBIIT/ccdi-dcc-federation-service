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
from app.models.errors import ErrorsResponse
from app.models.dto import (
    Subject,
    SubjectResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import NotFoundError, InvalidParametersError, InvalidRouteError
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
        # Check for invalid parameter values - return empty result instead of error
        # Check if any invalid filter value was provided
        invalid_ethnicity_value = filters.get("_invalid_ethnicity")
        invalid_sex_value = filters.get("_invalid_sex")
        invalid_race_value = filters.get("_invalid_race")
        invalid_vital_status_value = filters.get("_invalid_vital_status")
        invalid_age_value = filters.get("_invalid_age_at_vital_status")
        
        # If any invalid value is present, return empty result
        if invalid_ethnicity_value or invalid_sex_value or invalid_race_value or invalid_vital_status_value or invalid_age_value:
            # Return empty response with zero counts
            pagination_info = calculate_pagination_info(
                page=pagination.page,
                per_page=pagination.per_page,
                total_items=0
            )
            
            result = SubjectResponse(
                summary={
                    "counts": {
                        "all": 0,
                        "current": 0
                    }
                },
                data=[],
                pagination=pagination_info
            )
            
            logger.info(
                "Invalid filter value detected, returning empty result",
                invalid_ethnicity=invalid_ethnicity_value,
                invalid_sex=invalid_sex_value,
                invalid_race=invalid_race_value,
                invalid_vital_status=invalid_vital_status_value,
                invalid_age=invalid_age_value
            )
            
            return result
        
        # Remove the markers if present
        filters.pop("_invalid_ethnicity", None)
        filters.pop("_invalid_sex", None)
        filters.pop("_invalid_race", None)
        filters.pop("_invalid_vital_status", None)
        filters.pop("_invalid_age_at_vital_status", None)
        filters.pop("_age_at_vital_status_reason", None)
        
        # Handle depositions filter
        # If depositions=db_gap, return all values (remove filter)
        # If depositions has any other value, return empty []
        depositions_value = filters.get("depositions")
        if depositions_value is not None:
            depositions_str = str(depositions_value).strip()
            if depositions_str.lower() != "db_gap":
                # Return empty result for any value other than db_gap
                pagination_info = calculate_pagination_info(
                    page=pagination.page,
                    per_page=pagination.per_page,
                    total_items=0
                )
                
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[],
                    pagination=pagination_info
                )
                
                logger.info(
                    "Depositions filter with invalid value, returning empty result",
                    depositions=depositions_str
                )
                
                return result
            else:
                # Remove depositions filter since db_gap means return all
                filters.pop("depositions", None)
        
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Make a copy of filters for get_subjects (it modifies the dict by popping race/identifiers)
        filters_copy = filters.copy()
        
        # Get subjects
        subjects = await service.get_subjects(
            filters=filters_copy,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Get total count for summary (use original filters, not the modified copy)
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
        
    except HTTPException:
        # Re-raise HTTPException as-is (already properly formatted)
        raise
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError to let the exception handler process it
        raise e.to_http_exception()
    except Exception as e:
        logger.error("Error listing subjects", error=str(e), exc_info=True)
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
            total=result.total,
            missing=result.missing,
            values_count=len(result.values)
        )
        
        return result
        
    except Exception as e:
        logger.error("Error counting subjects by field", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Individual Subject Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=None,  # Will return different types based on input
    summary="Get subject by identifier or filter by field",
    description="Get a specific subject by organization, namespace, and name. Organization defaults to 'CCDI-DCC'. Namespace is the study_id value from the database. 'name' is the participant ID."
)
async def get_subject(
    organization: str,
    namespace: str,
    name: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a specific subject by identifier.
    
    Organization defaults to 'CCDI-DCC'.
    Namespace is the study_id value from the database (e.g., 'phs000465').
    Name is the participant ID.
    """
    
    logger.info(
        "Get subject request",
        organization=organization,
        namespace=namespace,
        name=name,
        path=request.url.path
    )
    
    try:
        # Normalize and validate organization (defaults to CCDI-DCC)
        if not organization or not organization.strip():
            organization = "CCDI-DCC"
        
        if organization.strip().upper() != "CCDI-DCC":
            raise InvalidParametersError(
                parameters=["organization"],
                reason="Organization accepted is 'CCDI-DCC'"
            )
        
        organization = "CCDI-DCC"
        
        # Namespace is the study_id value (no validation, used as-is)
        namespace = namespace.strip() if namespace and namespace.strip() else None
        
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Field-based filter request (only for CCDI-DCC)
        valid_field_names = {
            "sex", "race", "ethnicity", "identifiers", "vital_status", 
            "age_at_vital_status", "depositions", "associated_diagnoses"
        }
        
        if organization == "CCDI-DCC" and name and name.lower() in valid_field_names:
            # This is a filter request by field name
            logger.info(f"Using field-based filter: {name}")
            
            # Get query parameters to extract the field value
            # For example, /subject/CCDI-DCC/phs/sex?value=M would filter by sex=M
            field_name = name.lower()
            field_value = None
            
            # Get field value from query parameters
            query_params = dict(request.query_params)
            # Check if there's a 'value' parameter or if the field name itself is used as a query param
            if 'value' in query_params:
                field_value = query_params['value']
            elif field_name in query_params:
                field_value = query_params[field_name]
            
            # Create filter for this field
            filters = {}
            if field_value:
                filters[field_name] = field_value
            
            # Get subjects with this filter
            # If no value provided, we could return all subjects (but that's not useful)
            # Or we could return an error - let's return filtered results if value provided
            if filters:
                subjects = await service.get_subjects(
                    filters=filters,
                    offset=0,
                    limit=100
                )
                
                # Get total count for summary
                summary_result = await service.get_subjects_summary(filters)
                total_count = summary_result.counts.total
                
                # Build pagination
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=len(subjects),
                    total_items=total_count
                )
                
                # Build response
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": total_count,
                            "current": len(subjects)
                        }
                    },
                    data=subjects,
                    pagination=pagination_info
                )
                
                logger.info(
                    "Field filter response",
                    field=field_name,
                    value=field_value,
                    subject_count=len(subjects)
                )
                
                return result
            else:
                # No value provided for field filter - return empty result
                from app.core.pagination import calculate_pagination_info
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=0,
                    total_items=0
                )
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[],
                    pagination=pagination_info
                )
                logger.info(
                    "Field filter with no value, returning empty result",
                    field=field_name,
                    organization=organization,
                    namespace=namespace
                )
                return result
        
        # Participant ID search (organization must be CCDI-DCC)
        if organization == "CCDI-DCC":
            # Use participant ID search logic
            logger.info("Using participant ID search", organization=organization, namespace=namespace)
            
            # Parse participant IDs (split by comma and clean up)
            participant_id_list = [pid.strip() for pid in name.split(',') if pid.strip()]
            
            if not participant_id_list:
                # No participant IDs provided - return empty result
                from app.core.pagination import calculate_pagination_info
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=0,
                    total_items=0
                )
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[],
                    pagination=pagination_info
                )
                logger.info(
                    "No participant IDs provided, returning empty result",
                    organization=organization,
                    namespace=namespace,
                    name=name
                )
                return result
            
            # Create filters for participant IDs
            filters = {"identifiers": participant_id_list}
            
            # If namespace is provided, filter by it using get_subject_by_identifier for each participant ID
            # This ensures we only get participants that belong to the specified study_id
            if namespace:
                subjects = []
                for participant_id in participant_id_list:
                    subject = await service.get_subject_by_identifier(organization, namespace, participant_id)
                    if subject:
                        subjects.append(subject)
            else:
                # Get subjects using the search method (no namespace filter)
                subjects = await service.get_subjects(
                    filters=filters,
                    offset=0,
                    limit=100  # Allow multiple results
                )
            
            # Return empty result if no subjects found (instead of raising NotFoundError)
            if not subjects:
                # Return empty SubjectResponse
                from app.core.pagination import calculate_pagination_info
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=0,
                    total_items=0
                )
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[],
                    pagination=pagination_info
                )
                logger.info(
                    "Get subject response (no matches found)",
                    organization=organization,
                    namespace=namespace,
                    name=name
                )
                return result
            
            # If only one participant ID, return single Subject
            if len(participant_id_list) == 1:
                if subjects:
                    subject = subjects[0]
                    logger.info(
                        "Get subject response (single participant ID)",
                        organization=organization,
                        namespace=namespace,
                        name=name,
                        subject_data=getattr(subject, 'id', str(subject)[:50])
                    )
                    return subject
                else:
                    # Return empty result for single participant ID
                    from app.core.pagination import calculate_pagination_info
                    pagination_info = calculate_pagination_info(
                        page=1,
                        per_page=0,
                        total_items=0
                    )
                    result = SubjectResponse(
                        summary={
                            "counts": {
                                "all": 0,
                                "current": 0
                            }
                        },
                        data=[],
                        pagination=pagination_info
                    )
                    logger.info(
                        "Get subject response (single participant ID, no match)",
                        organization=organization,
                        namespace=namespace,
                        name=name
                    )
                    return result
            else:
                # If multiple participant IDs, return SubjectResponse format
                
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
                    organization=organization,
                    namespace=namespace,
                    name=name,
                    subject_count=len(subjects)
                )
                
                return result
        else:
            # Use original identifier search logic
            logger.info("Using identifier search for non-CCDI-DCC")
            
            # Get subject
            subject = await service.get_subject_by_identifier(organization, namespace, name)
            
            # Return empty result if not found (instead of raising NotFoundError)
            if not subject:
                from app.core.pagination import calculate_pagination_info
                pagination_info = calculate_pagination_info(
                    page=1,
                    per_page=0,
                    total_items=0
                )
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[],
                    pagination=pagination_info
                )
                logger.info(
                    "Get subject response (not found)",
                    organization=organization,
                    namespace=namespace,
                    name=name
                )
                return result
            
            logger.info(
                "Get subject response",
                organization=organization,
                namespace=namespace,
                name=name,
                subject_data=getattr(subject, 'id', str(subject)[:50])  # Flexible logging
            )
            
            return subject
        
    except Exception as e:
        # For any error (not found, invalid parameters, etc.), return empty result
        logger.warning(
            "Error getting subject, returning empty result",
            organization=organization,
            namespace=namespace,
            name=name,
            error=str(e)
        )
        from app.core.pagination import calculate_pagination_info
        pagination_info = calculate_pagination_info(
            page=1,
            per_page=0,
            total_items=0
        )
        result = SubjectResponse(
            summary={
                "counts": {
                    "all": 0,
                    "current": 0
                }
            },
            data=[],
            pagination=pagination_info
        )
        return result


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

# @router.get(
#     "/diagnosis/search",
#     response_model=SubjectResponse,
#     summary="Search subjects by diagnosis",
#     description="Search subjects with diagnosis filtering"
# )
# async def search_subjects_by_diagnosis(
#     request: Request,
#     response: Response,
#     filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
#     pagination: PaginationParams = Depends(get_pagination_params),
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Search subjects with diagnosis filtering."""
#     logger.info(
#         "Search subjects by diagnosis request",
#         filters=filters,
#         page=pagination.page,
#         per_page=pagination.per_page,
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         cache_service = get_cache_service()
#         service = SubjectService(session, allowlist, settings, cache_service)
#         
#         # Get subjects
#         subjects = await service.get_subjects(
#             filters=filters,
#             offset=pagination.offset,
#             limit=pagination.per_page
#         )
#         
#         # Build pagination info
#         pagination_info = PaginationInfo(
#             page=pagination.page,
#             per_page=pagination.per_page,
#             total_pages=None,
#             total_count=None
#         )
#         
#         # Add Link header
#         link_header = build_link_header(
#             request=request,
#             pagination=pagination_info
#         )
#         
#         if link_header:
#             response.headers["Link"] = link_header
#         
#         # Build response
#         result = SubjectResponse(
#             subjects=subjects,
#             pagination=pagination_info
#         )
#         
#         logger.info(
#             "Search subjects by diagnosis response",
#             subject_count=len(subjects),
#             page=pagination.page
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error searching subjects by diagnosis", error=str(e), exc_info=True)
#         if hasattr(e, 'to_http_exception'):
#             raise e.to_http_exception()
#         raise HTTPException(status_code=500, detail="Internal server error")


# @router.get(
#     "/diagnosis/by/{field}/count",
#     response_model=CountResponse,
#     summary="Count subjects by field with diagnosis search",
#     description="Count subjects by field with diagnosis filtering"
# )
# async def count_subjects_by_field_with_diagnosis(
#     field: str,
#     request: Request,
#     filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Count subjects by field with diagnosis filtering."""
#     logger.info(
#         "Count subjects by field with diagnosis request",
#         field=field,
#         filters=filters,
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         cache_service = get_cache_service()
#         service = SubjectService(session, allowlist, settings, cache_service)
#         
#         # Get counts
#         result = await service.count_subjects_by_field(field, filters)
#         
#         logger.info(
#             "Count subjects by field with diagnosis response",
#             field=field,
#             count_items=len(result.counts)
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error counting subjects by field with diagnosis", error=str(e), exc_info=True)
#         if hasattr(e, 'to_http_exception'):
#             raise e.to_http_exception()
#         raise HTTPException(status_code=500, detail="Internal server error")


# @router.get(
#     "/diagnosis/summary",
#     response_model=SummaryResponse,
#     summary="Get subjects summary with diagnosis search",
#     description="Get summary statistics for subjects with diagnosis filtering"
# )
# async def get_subjects_summary_with_diagnosis(
#     request: Request,
#     filters: Dict[str, Any] = Depends(get_subject_diagnosis_filters),
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Get summary statistics for subjects with diagnosis filtering."""
#     logger.info(
#         "Get subjects summary with diagnosis request",
#         filters=filters,
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         cache_service = get_cache_service()
#         service = SubjectService(session, allowlist, settings, cache_service)
#         
#         # Get summary
#         result = await service.get_subjects_summary(filters)
#         
#         logger.info(
#             "Get subjects summary with diagnosis response",
#             total_count=result.total_count
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error getting subjects summary with diagnosis", error=str(e), exc_info=True)
#         if hasattr(e, 'to_http_exception'):
#             raise e.to_http_exception()
#         raise HTTPException(status_code=500, detail="Internal server error")


# @router.get(
#     "/search/{organization}/{namespace}/{name}",
#     response_model=SubjectResponse,
#     summary="Search subjects by participant ID(s)",
#     description="Search for subjects by one or more participant IDs. Multiple IDs can be separated by commas (,). Organization and namespace are hardcoded to CCDI-DCC."
# )
# async def search_by_participant_id(
#     organization: str,
#     namespace: str,
#     name: str,
#     pagination: PaginationParams = Depends(get_pagination_params),
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     request: Request = None,
#     response: Response = None
# ) -> SubjectResponse:
#     """
#     Search subjects by participant ID(s).
#     
#     Args:
#         organization: Organization name
#         namespace: Namespace name
#         participant_ids: Participant ID(s) to search for, separated by commas (,)
#         pagination: Pagination parameters
#         session: Database session
#         settings: Application settings
#         allowlist: Field allowlist
#         request: HTTP request
#         response: HTTP response
#         
#     Returns:
#         SubjectResponse with matching subjects
#     """
#     try:
#         # Check rate limit
#         await check_rate_limit(request)
#         
#         # Validate hardcoded values
#         if organization != "CCDI-DCC":
#             raise InvalidParametersError(
#                 parameters=["organization"],
#                 reason="Organization must be CCDI-DCC"
#             )
#         if namespace != "CCDI-DCC":
#             raise InvalidParametersError(
#                 parameters=["namespace"],
#                 reason="Namespace must be CCDI-DCC"
#             )
#         
#         # Create service
#         cache_service = get_cache_service()
#         service = SubjectService(session, allowlist, settings, cache_service)
#         
#         # Log the search request
#         logger.info(f"Searching subjects by participant IDs: organization={organization}, namespace={namespace}, participant_ids={name}, page={pagination.page}, per_page={pagination.per_page}")
#         
#         # Parse participant IDs (split by comma and clean up)
#         participant_id_list = [pid.strip() for pid in name.split(',') if pid.strip()]
#         
#         if not participant_id_list:
#             raise InvalidParametersError(
#                 parameters=["name"],
#                 reason="At least one participant ID must be provided"
#             )
#         
#         # Create filters for participant IDs (use identifiers filter which maps to participant_id)
#         filters = {"identifiers": participant_id_list}
#         
#         # Get subjects
#         subjects = await service.get_subjects(
#             filters=filters,
#             offset=pagination.offset,
#             limit=pagination.per_page
#         )
#         
#         # Get total count for summary
#         summary_result = await service.get_subjects_summary(filters)
#         total_count = summary_result.counts.total
#         
#         # Calculate pagination info using the utility function
#         pagination_info = calculate_pagination_info(
#             page=pagination.page,
#             per_page=pagination.per_page,
#             total_items=total_count
#         )
#         
#         # Add Link header for pagination
#         link_header = build_link_header(
#             request=request,
#             pagination=pagination_info
#         )
#         
#         if link_header:
#             response.headers["Link"] = link_header
#         
#         # Build nested response structure
#         result = SubjectResponse(
#             source="CCDI-DCC",
#             summary={
#                 "counts": {
#                     "all": total_count,  # Total number of unique participants
#                     "current": len(subjects)
#                 }
#             },
#             data=subjects,
#             pagination=pagination_info
#         )
#         
#         logger.info(
#             "Search by participant ID response",
#             organization=organization,
#             namespace=namespace,
#             participant_ids=participant_id_list,
#             subject_count=len(subjects),
#             page=pagination.page
#         )
#         
#         return result
#         
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error("Error searching by participant ID", error=str(e), exc_info=True)
#         if hasattr(e, 'to_http_exception'):
#             raise e.to_http_exception()
#         raise HTTPException(status_code=500, detail="Internal server error")
