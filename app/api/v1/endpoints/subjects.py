"""
Subject API routes for the CCDI Federation Service.

This module provides REST endpoints for subject operations
including listing, individual retrieval, counting, and summaries.
"""

from typing import Dict, Any, List
import copy
import re

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status, Path
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
from app.models.errors import ErrorsResponse, ErrorDetail, ErrorKind
from app.models.dto import (
    Subject,
    SubjectResponse,
    CountResponse,
    SummaryResponse,
    SummaryCounts,
    NamedGateway
)
from app.models.errors import NotFoundError, InvalidParametersError, InvalidRouteError
from app.services.subject import SubjectService
from app.db.memgraph import DatabaseConnectionError

logger = get_logger(__name__)

router = APIRouter(prefix="/subject", tags=["Subject"])


def prepare_subjects_for_response(subjects: List[Subject]) -> List[Dict[str, Any]]:
    """
    Prepare subjects for response, excluding gateways from output.
    
    Args:
        subjects: List of Subject objects
        
    Returns:
        List of subject dicts without gateways field
    """
    subjects_dicts = []
    
    for subject in subjects:
        # Create subject dict excluding gateways field (keep as placeholder in code)
        # CRITICAL: Keep schema stable. Always include keys even when values are null/empty.
        subject_dict = subject.model_dump(exclude={'gateways'}, exclude_none=False, exclude_unset=False)
        subjects_dicts.append(subject_dict)
    
    return subjects_dicts


# ============================================================================
# Subject Listing
# ============================================================================

@router.get(
    "",
    response_model=SubjectResponse,
    summary="List subjects",
    description="Get a paginated list of subjects with optional filtering",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "example": {
                        "summary": {
                            "counts": {
                                "all": 500,
                                "current": 20
                            }
                        },
                        "data": [
                            {
                                "id": {
                                    "namespace": {
                                        "organization": "CCDI-DCC",
                                        "name": "phs002430"
                                    },
                                    "name": "SUBJECT-001"
                                },
                                "kind": "Participant",
                                "metadata": {
                                    "sex": {"value": "F"},
                                    "race": [
                                        {"value": "White"}
                                    ],
                                    "ethnicity": {"value": "Not reported"},
                                    "identifiers": [
                                        {
                                            "value": {
                                            "namespace": {
                                                "organization": "CCDI-DCC",
                                                "name": "phs002430"
                                            },
                                                "name": "SUBJECT-001",
                                                "type": "Linked",
                                                "server": "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002430/SUBJECT-001"
                                            }
                                        }
                                    ],
                                    "vital_status": {"value": "Alive"},
                                    "age_at_vital_status": {"value": 45},
                                    "associated_diagnoses": [
                                        {
                                            "value": "Neuroblastoma","comment": "null"
                                        }
                                    ],
                                    "associated_diagnosis_categories": [
                                        {"value": "Brain and Spinal Cord Tumors"}
                                    ],
                                    "depositions": [
                                        {
                                            "kind": "dbGaP",
                                            "value": "phs002430"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        },
        404: {
            "description": "Not found.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorsResponse"},
                    "example": {
                        "errors": [
                            {
                                "kind": "NotFound",
                                "entity": "Subjects",
                                "message": "Unable to find data for your request.",
                                "reason": "No data found."
                            }
                        ]
                    }
                }
            }
        }
    }
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
        # Validate that no unknown query parameters are provided
        allowed_params = {
            "sex", "race", "ethnicity", "identifiers", "vital_status",
            "age_at_vital_status", "depositions",
            "associated_diagnosis_categories",
            "page", "per_page"
        }
        
        unknown_params = []
        for key in request.query_params.keys():
            if key not in allowed_params:
                unknown_params.append(key)
        
        if unknown_params:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
        
        # Check for unknown parameters from filter dependency (backup check)
        unknown_params_from_filter = filters.get("_unknown_parameters")
        if unknown_params_from_filter:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
        
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
            # pagination_info = calculate_pagination_info(
            #     page=pagination.page,
            #     per_page=pagination.per_page,
            #     total_items=0
            # )
            
            result = SubjectResponse(
                summary={
                    "counts": {
                        "all": 0,
                        "current": 0
                    }
                },
                data=[]
                # pagination=pagination_info
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
        # Depositions filter now accepts study_id value (e.g., phs002431)
        # The filter will be processed in the repository to match participants by study_id
        
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Make a copy of filters for get_subjects (it modifies the dict by popping race/identifiers)
        filters_copy = copy.deepcopy(filters)
        
        # Use configured identifier server URL for all identifier server values
        base_url = settings.identifier_server_url.rstrip("/")
        
        # Get subjects with total count in one round trip
        result = await service.get_subjects(
            filters=filters_copy,
            offset=pagination.offset,
            limit=pagination.per_page,
            base_url=base_url,
            return_total=True,
        )
        subjects, total_count = result
        
        # Build pagination info (match /sample behavior: do not require total_pages)
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_items=total_count,  # Use total_count from summary, not len(subjects)
            has_next=len(subjects) == pagination.per_page,  # If we got a full page, there might be more
            has_prev=pagination.page > 1,
        )
        
        # Add Link header for pagination (consistent with /sample)
        link_header = build_link_header(
            request=request,
            pagination=pagination_info,
            extra_params=dict(request.query_params),
        )
        if link_header:
            response.headers["link"] = link_header
        
        # Prepare subjects for response (exclude gateways from output)
        subjects_dicts = prepare_subjects_for_response(subjects)
        
        # Build nested response structure
        result = SubjectResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique participants
                    "current": len(subjects)
                }
            },
            data=subjects_dicts  # List of dicts without gateways
            # pagination=pagination_info
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
    except DatabaseConnectionError as e:
        # Database connection error - log clearly for AWS cloud monitoring
        logger.error(
            "Database connection error in list_subjects endpoint - returning empty result",
            error=str(e),
            error_type=type(e).__name__,
            filters=filters,
            page=pagination.page,
            per_page=pagination.per_page,
            is_database_connection_error=True,
            will_return_404=True,
            aws_cloudwatch_alert=True
        )
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
    except Exception as e:
        # Check if this is a connection-related error
        error_str = str(e).lower()
        is_connection_error = any(keyword in error_str for keyword in [
            'connection', 'database', 'unavailable', 'timeout', 'network',
            'service unavailable', 'broken pipe', 'connection reset', 'connection closed'
        ])
        
        if is_connection_error:
            # Connection-related error - log clearly for AWS cloud monitoring
            logger.error(
                "Database connection issue in list_subjects endpoint - returning empty result",
                error=str(e),
                error_type=type(e).__name__,
                filters=filters,
                page=pagination.page,
                per_page=pagination.per_page,
                is_connection_related=True,
                will_return_404=True,
                aws_cloudwatch_alert=True,
                exc_info=True
            )
        else:
            logger.error("Error listing subjects", error=str(e), exc_info=True)
        
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


# ============================================================================
# Subject Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Groups the subjects by the specified metadata field and returns counts.",
    description="Groups the subjects by the specified metadata field and returns counts.",
    operation_id="subjects_by_count",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/CountResponse"}
                }
            }
        },
        400: {
            "description": "Unsupported field.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorsResponse"},
                    "example": {
                        "errors": [
                            {
                                "kind": "UnsupportedField",
                                "field": "wrong field",
                                "message": "Field is not supported for subjects.",
                                "reason": "This field is not present for subjects."
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def count_subjects_by_field(
    request: Request,
    field: str = Path(
        ...,
        description="The field to group by and count with.",
        enum=["sex", "race", "ethnicity", "vital_status", "age_at_vital_status",
              "associated_diagnoses", "associated_diagnosis_categories"],
        examples={
            "sex": {"value": "sex", "summary": "Count by sex"},
            "race": {"value": "race", "summary": "Count by race"},
            "ethnicity": {"value": "ethnicity", "summary": "Count by ethnicity"},
            "vital_status": {"value": "vital_status", "summary": "Count by vital status"},
            "age_at_vital_status": {"value": "age_at_vital_status", "summary": "Count by age at vital status"},
            "associated_diagnoses": {"value": "associated_diagnoses", "summary": "Count by associated diagnoses"},
            "associated_diagnosis_categories": {"value": "associated_diagnosis_categories", "summary": "Count by diagnosis category"}
        }
    ),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Groups the subjects by the specified metadata field and returns counts."""
    logger.info(
        "Count subjects by field request",
        field=field,
        path=request.url.path
    )
    
    try:
        # Validate that no query parameters are provided
        # Check if there are any query parameters (request.query_params is always truthy, need to check length)
        if len(request.query_params) > 0:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Count endpoint does not accept any query parameters"
            )

        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get counts (no filters - returns counts for all subjects)
        result = await service.count_subjects_by_field(field, {})
        
        logger.info(
            "Count subjects by field response",
            field=field,
            total=result.total,
            missing=result.missing,
            values_count=len(result.values)
        )
        
        return result
        
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError with proper HTTP exception
        logger.error("Invalid parameters in count_subjects_by_field request", error=str(e), exc_info=True)
        raise e.to_http_exception()
    except DatabaseConnectionError as e:
        # Database connection error - log clearly for AWS cloud monitoring
        logger.error(
            "Database connection error in count_subjects_by_field endpoint - returning empty result",
            error=str(e),
            error_type=type(e).__name__,
            field=field,
            filters={},
            is_database_connection_error=True,
            will_return_404=True,
            aws_cloudwatch_alert=True
        )
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
    except Exception as e:
        # Check if this is a connection-related error
        error_str = str(e).lower()
        is_connection_error = any(keyword in error_str for keyword in [
            'connection', 'database', 'unavailable', 'timeout', 'network',
            'service unavailable', 'broken pipe', 'connection reset', 'connection closed'
        ])
        
        if is_connection_error:
            # Connection-related error - log clearly for AWS cloud monitoring
            logger.error(
                "Database connection issue in count_subjects_by_field endpoint - returning empty result",
                error=str(e),
                error_type=type(e).__name__,
                field=field,
                filters={},
                is_connection_related=True,
                will_return_404=True,
                aws_cloudwatch_alert=True,
                exc_info=True
            )
        else:
            logger.error("Error counting subjects by field", error=str(e), exc_info=True)
        
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


# ============================================================================
# Individual Subject Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=None,  # Will return different types based on input
    summary="Get subject by identifier",
    description="Get a specific subject by organization, namespace, and name. Organization defaults to 'CCDI-DCC'. Namespace is the study_id value from the database. 'name' is the participant ID. This endpoint does not accept query parameters.",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "example": {
                        "id": {
                            "namespace": {
                                "organization": "CCDI-DCC",
                                "name": "phs002430"
                            },
                            "name": "TARGET-10-DCC001"
                        },
                        "kind": "Participant",
                        "metadata": {
                            "sex": {"value": "M"},
                            "race": [
                                {"value": "White"}
                            ],
                            "ethnicity": {"value": "Not reported"},
                            "identifiers": [
                                {
                                    "value": {
                                        "namespace": {
                                            "organization": "CCDI-DCC",
                                            "name": "phs002430"
                                        },
                                        "name": "TARGET-10-DCC001",
                                        "type": "Linked",
                                        "server": "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002430/TARGET-10-DCC001"
                                    }
                                }
                            ],
                            "vital_status": {"value": "Alive"},
                            "age_at_vital_status": {"value": 15},
                            "associated_diagnoses": [
                                {
                                    "value": "Neuroblastoma","comment": "null"
                                }
                            ],
                            "associated_diagnosis_categories": [
                                {"value": "Brain and Spinal Cord Tumors"}
                            ],
                            "depositions": [
                                {
                                    "kind": "dbGaP",
                                    "value": "phs002430"
                                }
                            ]
                        }
                    }
                }
            }
        },
        404: {
            "description": "Not found.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorsResponse"},
                    "example": {
                        "errors": [
                            {
                                "kind": "NotFound",
                                "entity": "Subjects",
                                "message": "Unable to find data for your request.",
                                "reason": "No data found."
                            }
                        ]
                    }
                }
            }
        }
    }
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
        # Check if this looks like a typo for /by/{field}/count route
        # Pattern: /subject/{typo}/field/count where typo might be "b1y", "by2", etc.
        if name == "count":
            # Check if organization looks like a typo of "by"
            org_lower = organization.lower() if organization else ""
            if re.match(r'^b.*y.*$', org_lower) and org_lower != "by":
                # Valid field names for count endpoint
                valid_fields = {"sex", "race", "ethnicity", "vital_status", "age_at_vital_status",
                               "associated_diagnoses", "associated_diagnosis_categories"}
                if namespace.lower() in valid_fields:
                    # This is likely a typo for /subject/by/{field}/count
                    suggested_path = f"/api/v1/subject/by/{namespace}/count"
                    # Log the suggested path but don't include it in the response
                    logger.info(
                        "Invalid route detected, possible typo",
                        method=request.method,
                        route=str(request.url.path),
                        suggested_path=suggested_path
                    )
                    raise InvalidRouteError(
                        method=request.method,
                        route=str(request.url.path),
                        message="Invalid route requested."
                    )
        
        # Normalize and validate organization (defaults to CCDI-DCC)
        if not organization or not organization.strip():
            organization = "CCDI-DCC"
        
        if organization.strip().upper() != "CCDI-DCC":
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
        
        organization = "CCDI-DCC"
        
        # Namespace is the study_id value (no validation, used as-is)
        namespace = namespace.strip() if namespace and namespace.strip() else None
        
        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Participant ID search (organization must be CCDI-DCC)
        if organization == "CCDI-DCC":
            # Use participant ID search logic - only accepts single participant ID
            logger.info("Using participant ID search", organization=organization, namespace=namespace)
            
            # Validate that name is a single participant ID (no comma-separated list)
            if ',' in name:
                raise InvalidParametersError(
                    parameters=["name"],
                    message="Invalid participant ID format.",
                    reason="Only a single participant ID is allowed. Multiple IDs are not supported."
                )
            
            participant_id = name.strip()
            
            if not participant_id:
                # No participant ID provided - return empty result
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[]
                )
                logger.info(
                    "No participant ID provided, returning empty result",
                    organization=organization,
                    namespace=namespace,
                    name=name
                )
                return result
            
            # Use configured identifier server URL for all identifier server values
            base_url = settings.identifier_server_url.rstrip("/")
            
            # Get subject by identifier (handles namespace if provided)
            subject = await service.get_subject_by_identifier(organization, namespace, participant_id, base_url=base_url)
            
            if not subject:
                # No match found - return empty result
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[]
                )
                logger.info(
                    "Get subject response (not found)",
                    organization=organization,
                    namespace=namespace,
                    name=name
                )
                return result
            
            # Return subject dict excluding gateways
            subject_dict = subject.model_dump(exclude={'gateways'}, exclude_none=False, exclude_unset=False)
            logger.info(
                "Get subject response (single participant ID)",
                organization=organization,
                namespace=namespace,
                name=name,
                subject_data=getattr(subject, 'id', str(subject)[:50])
            )
            return subject_dict
        else:
            # Use original identifier search logic
            logger.info("Using identifier search for non-CCDI-DCC")
            
            # Use configured identifier server URL for all identifier server values
            base_url = settings.identifier_server_url.rstrip("/")
            
            # Get subject
            subject = await service.get_subject_by_identifier(organization, namespace, name, base_url=base_url)
            
            # Return empty result if not found (instead of raising NotFoundError)
            if not subject:
                # from app.core.pagination import calculate_pagination_info
                # pagination_info = calculate_pagination_info(
                #     page=1,
                #     per_page=0,
                #     total_items=0
                # )
                result = SubjectResponse(
                    summary={
                        "counts": {
                            "all": 0,
                            "current": 0
                        }
                    },
                    data=[]
                    # pagination=pagination_info
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
            
            # Return subject dict excluding gateways (keep as placeholder in code)
            return subject.model_dump(exclude={'gateways'}, exclude_none=False, exclude_unset=False)
        
    except (InvalidRouteError, InvalidParametersError) as e:
        # Re-raise route/parameter errors to let the exception handler process them
        raise e.to_http_exception()
    except Exception as e:
        # For any error (not found, invalid parameters, etc.), return empty result
        logger.warning(
            "Error getting subject, returning empty result",
            organization=organization,
            namespace=namespace,
            name=name,
            error=str(e)
        )
        # from app.core.pagination import calculate_pagination_info
        # pagination_info = calculate_pagination_info(
        #     page=1,
        #     per_page=0,
        #     total_items=0
        # )
        result = SubjectResponse(
            summary={
                "counts": {
                    "all": 0,
                    "current": 0
                }
            },
            data=[]
            # pagination=pagination_info
        )
        return result


# ============================================================================
# Subject Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Reports summary information for the subjects known by this server.",
    description="Returns the total count of all subjects. This endpoint does not accept any query parameters.",
    operation_id="subject_summary",
    responses={
        200: {
            "description": "Summary counts successfully returned",
            "content": {
                "application/json": {
                    "example": {
                        "counts": {
                            "total": 25696
                        }
                    }
                }
            }
        }
    }
)
async def get_subjects_summary(
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """
    Reports summary information for the subjects known by this server.
    
    Returns the total count of all subjects. This endpoint does not accept any query parameters.
    For filtered counts, use the /subject endpoint with filters.
    """
    logger.info(
        "Get subjects summary request",
        path=request.url.path
    )
    
    try:
        # Validate that no query parameters are provided
        # Check if there are any query parameters (request.query_params is always truthy, need to check length)
        if len(request.query_params) > 0:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Summary endpoint does not accept any query parameters"
            )

        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Get summary (no filters - returns total count of all subjects)
        result = await service.get_subjects_summary({})
        
        logger.info(
            "Get subjects summary response",
            total_count=result.counts.total
        )
        
        return result
        
    except DatabaseConnectionError as e:
        # Database connection error - log clearly for AWS cloud monitoring
        logger.error(
            "Database connection error in get_subjects_summary endpoint - returning empty result",
            error=str(e),
            error_type=type(e).__name__,
            is_database_connection_error=True,
            will_return_404=True,
            aws_cloudwatch_alert=True
        )
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
    except Exception as e:
        # Check if this is a connection-related error
        error_str = str(e).lower()
        is_connection_error = any(keyword in error_str for keyword in [
            'connection', 'database', 'unavailable', 'timeout', 'network',
            'service unavailable', 'broken pipe', 'connection reset', 'connection closed'
        ])
        
        if is_connection_error:
            # Connection-related error - log clearly for AWS cloud monitoring
            logger.error(
                "Database connection issue in get_subjects_summary endpoint - returning empty result",
                error=str(e),
                error_type=type(e).__name__,
                is_connection_related=True,
                will_return_404=True,
                aws_cloudwatch_alert=True,
                exc_info=True
            )
        else:
            logger.error("Error getting subjects summary", error=str(e), exc_info=True)
        
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
#         # Return 404 instead of 500 - no 500 errors allowed
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
#         # Return 404 instead of 500 - no 500 errors allowed
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
#         # Return 404 instead of 500 - no 500 errors allowed
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
#         # Return 404 instead of 500 - no 500 errors allowed
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
