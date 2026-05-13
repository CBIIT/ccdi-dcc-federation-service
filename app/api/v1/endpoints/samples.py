"""
Sample API routes for the CCDI Federation Service.

This module provides REST endpoints for sample operations
including listing, individual retrieval, counting, and summaries.
"""

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status, Path
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    get_allowlist,
    get_pagination_params,
    get_sample_filters,
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
from app.models.errors import NotFoundError, ErrorDetail, ErrorsResponse, ErrorKind, InvalidParametersError, UnsupportedFieldError
from app.services.sample import SampleService
from app.db.memgraph import DatabaseConnectionError

logger = get_logger(__name__)

router = APIRouter(prefix="/sample", tags=["Sample"])


# ============================================================================
# Sample Listing
# ============================================================================

@router.get(
    "",
    response_model=SamplesResponse,
    summary="Gets the samples known by this server.",
    description="""Gets the samples known by this server.

### Pagination

This endpoint is paginated. Users may override the default pagination
parameters by providing one or more of the pagination-related query
parameters below.

### Filtering

All harmonized (top-level) and unharmonized (nested under the
`metadata.unharmonized` key) metadata fields are filterable. To achieve
this, you can provide the field name as a [`String`]. Filtering follows the
following rules:

* For single-value metadata field, the sample is included in the results if
its value _exactly_ matches the query string. Matches are case-sensitive.
* For multiple-value metadata fields, the sample is included in the results
if any of its values for the field _exactly_ match the query string (a
logical OR (`||`)). Matches are case-sensitive.
* When the metadata field is `null` (in the case of singular or
multiple-valued metadata fields) or empty, the sample is not included.
* When multiple fields are provided as filters, a logical AND (`&&`) strings
together the predicates. In other words, all filters must match for a
sample to be returned. Note that this means that servers do not natively
support logical OR (`||`) across multiple fields: that must be done by
calling this endpoint with each of your desired queries and performing a
set union of those samples out of band.

### Ordering

This endpoint has default ordering requirements—those details are documented
in the `responses::Samples` schema.""",
    operation_id="sample_index",
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
                                    "name": "TARGET-10-DCC001-03A-01R"
                                },
                                "subject": {
                                    "namespace": {
                                        "organization": "CCDI-DCC",
                                        "name": "phs002430"
                                    },
                                    "name": "TARGET-10-DCC001"
                                },
                                "metadata": {
                                    "disease_phase": {"value": "Initial Diagnosis"},
                                    "diagnosis": {"value": "Neuroblastoma","comment": "null" },
                                    "diagnosis_category": [
                                        {"value": "Brain and Spinal Cord Tumors"}
                                    ],
                                    "age_at_diagnosis": {"value": 10},
                                    "anatomical_sites": [
                                        {"value": "C71.9 : Brain, NOS"}
                                    ],
                                    "tissue_type": {"value": "Tumor"},
                                    "tumor_classification": {"value": "Primary"},
                                    "library_strategy": {"value": "WXS"},
                                    "library_source_material": {"value": "Genomic DNA"},
                                    "depositions": [
                                        {
                                            "kind": "dbGaP",
                                            "value": "phs002430"
                                        }
                                    ],
                                    "identifiers": [
                                        {
                                            "value": {
                                                "namespace": {
                                                    "organization": "CCDI-DCC",
                                                    "name": "phs002430"
                                                },
                                                "name": "TARGET-10-DCC001-03A-01R",
                                                "server": "https://dcc.ccdi.cancer.gov/api/v1/sample/CCDI-DCC/phs002430/TARGET-10-DCC001-03A-01R",
                                                "type": "Linked"
                                            }
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
                                "entity": "Samples",
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
        # Validate query parameters - check for unknown parameters (especially "search" which is only for /sample-diagnosis)
        allowed_params = {"disease_phase", "anatomical_sites", "library_selection_method",
                         "library_strategy", "library_source_material", "preservation_method", "tumor_grade",
                         "specimen_molecular_analyte_type", "tissue_type", "tumor_classification",
                         "age_at_diagnosis", "age_at_collection", "tumor_tissue_morphology",
                         "depositions", "diagnosis", "identifiers", "diagnosis_category", "page", "per_page"}
        # Add support for metadata.unharmonized.* fields
        allowed_params.update({k for k in request.query_params.keys() if k.startswith("metadata.unharmonized.")})
        
        unknown_params = []
        for key in request.query_params.keys():
            if key not in allowed_params and not key.startswith("metadata.unharmonized."):
                unknown_params.append(key)
        
        if unknown_params:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
        
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Make a copy of filters for get_samples (it will modify the dict by popping identifiers)
        # so that get_samples_summary gets the original filters dict if needed
        filters_copy = filters.copy()
        
        # Get samples with total count (optimized: uses same filter state when possible)
        # This avoids a separate get_samples_summary call for most cases
        result = await service.get_samples(
            filters=filters_copy,
            offset=pagination.offset,
            limit=pagination.per_page,
            return_total=True
        )
        
        # Handle tuple return (samples, total_count) or list return (samples only)
        if isinstance(result, tuple):
            samples, total_count = result
            logger.debug("Using total count from get_samples (optimized path)", total_count=total_count)
        else:
            # Repository didn't return total (e.g. sequencing_file-only reverse query path)
            # Fall back to get_samples_summary
            samples = result
            try:
                summary_result = await service.get_samples_summary(filters)
                total_count = summary_result.counts.total
            except (DatabaseConnectionError, NotFoundError) as summary_error:
                # DB / no-data error - re-raise so outer handler returns 404
                logger.error(
                    "Error getting samples summary (backend/DB) - returning 404",
                    error=str(summary_error),
                    error_type=type(summary_error).__name__,
                    filters=filters,
                    exc_info=True
                )
                if isinstance(summary_error, NotFoundError):
                    raise summary_error
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
            except Exception as summary_error:
                # Other errors (e.g. connection-related) - re-raise so outer handler returns 404
                logger.error(
                    "Error getting samples summary - returning 404",
                    error=str(summary_error),
                    error_type=type(summary_error).__name__,
                    filters=filters,
                    exc_info=True
                )
                if hasattr(summary_error, 'to_http_exception'):
                    raise summary_error.to_http_exception()
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
        
        # Build pagination info
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_items=total_count,  # Use total_count from summary, not len(samples)
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
            response.headers["link"] = link_header
        
        logger.info(
            "List samples response",
            sample_count=len(samples),
            total_count=total_count,
            page=pagination.page
        )
        
        # Exclude gateways from individual samples (keep as placeholder in code)
        samples_dicts = [sample.model_dump(exclude={'gateways'}) if hasattr(sample, 'model_dump') else {k: v for k, v in (sample if isinstance(sample, dict) else sample.__dict__).items() if k != 'gateways'} for sample in samples]
        
        # Build response with summary first, then data
        # Always return 200 with empty data if query succeeded but no results found
        # Calculate summary counts:
        # - all = total (total count from summary/query)
        # - current = actual items returned in this page (may be less than per_page on last page)
        current_count = len(samples)
        
        result = SamplesResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique samples (calculated at pagination point)
                    "current": current_count  # Items in current page
                }
            },
            data=samples_dicts
        )
        
        return result
        
    except Exception as e:
        # Re-raise HTTPException (e.g. 404 from summary failure) as-is
        if isinstance(e, HTTPException):
            raise e
        # Check if this is a parameter validation error or query error
        if isinstance(e, (InvalidParametersError, UnsupportedFieldError)):
            # This is a parameter validation error - raise it directly
            logger.error("Invalid parameters in request", error=str(e), exc_info=True)
            raise e.to_http_exception()
        if isinstance(e, NotFoundError):
            # No data found (e.g. DB error) - return 404
            raise e.to_http_exception()
        
        # Check if this is a query error (like UnboundVariable, syntax error) that indicates a real problem
        error_str = str(e).lower()
        is_query_error = any(keyword in error_str for keyword in [
            "unbound variable", "syntax error", "type error", "mismatched input",
            "invalidparameterserror", "unsupportedfielderror"
        ])
        
        if is_query_error:
            # This is an actual query/parameter error - log and return 404
            logger.error("Error listing samples (query/parameter error)", error=str(e), exc_info=True)
            if hasattr(e, 'to_http_exception'):
                raise e.to_http_exception()
            error_detail = ErrorDetail(
                kind=ErrorKind.NOT_FOUND,
                entity="Samples",
                message="Unable to find data for your request.",
                reason="Query or parameter error."
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
            )
        # For other errors (e.g. DB/backend error), return 404 NotFound, not empty data
        logger.error(
            "Error listing samples (backend/DB error), returning 404",
            error=str(e),
            exc_info=True
        )
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
# Sample Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Groups the samples by the specified metadata field and returns counts.",
    description="Groups the samples by the specified metadata field and returns counts.",
    operation_id="samples_by_count",
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
                                "message": "Field is not supported for samples.",
                                "reason": "This field is not present for samples."
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def count_samples_by_field(
    request: Request,
    field: str = Path(
        ...,
        description="The field to group by and count with.",
        enum=[
            # Sample metadata fields only
            "disease_phase", "anatomical_sites", "library_selection_method", "library_strategy",
            "library_source_material", "preservation_method", "tumor_grade", "specimen_molecular_analyte_type",
            "tissue_type", "tumor_classification", "age_at_diagnosis", "age_at_collection",
            "tumor_tissue_morphology", "diagnosis", "diagnosis_category"
        ],
    ),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count samples grouped by a specific field."""
    logger.info(
        "Count samples by field request",
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
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get counts (no filters - returns counts for all samples)
        result = await service.count_samples_by_field(field, {})
        
        logger.info(
            "Count samples by field response",
            field=field,
            count_items=len(result.values)
        )
        
        return result
        
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError with proper HTTP exception
        logger.error("Invalid parameters in count_samples_by_field request", error=str(e), exc_info=True)
        raise e.to_http_exception()
    except UnsupportedFieldError as e:
        # Re-raise UnsupportedFieldError with proper HTTP exception
        raise e.to_http_exception()
    except Exception as e:
        # Check if this is a query error (like UnboundVariable) that indicates a real problem
        # vs a scenario where we should return empty results
        error_str = str(e).lower()
        is_query_error = any(keyword in error_str for keyword in [
            "unbound variable", "syntax error", "type error"
        ])
        
        if is_query_error:
            # This is an actual query error - log and return 404
            logger.error(
                "Query error counting samples by field", 
                error=str(e), 
                error_type=type(e).__name__,
                field=field,
                exc_info=True
            )
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
        else:
            # For other errors, try to return empty result with all counted as missing
            # This handles cases where the field is valid but has no data
            logger.warning(
                "Error in count_samples_by_field, attempting to return empty result",
                error=str(e),
                error_type=type(e).__name__,
                field=field
            )
            try:
                # Try to get total count from summary
                cache_service = get_cache_service()
                service = SampleService(session, allowlist, settings, cache_service)
                summary_result = await service.get_samples_summary({})
                total = summary_result.counts.total if summary_result else 0
                
                # Return empty result with all counted as missing
                return CountResponse(
                    total=total,
                    missing=total,
                    values=[]
                )
            except Exception as e2:
                # If we can't even get the total, return 404
                logger.error(
                    "Error getting summary for empty result fallback", 
                    error=str(e2),
                    exc_info=True
                )
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
# Individual Sample Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=Sample,
    summary="Gets the sample matching the provided name (if the sample exists).",
    description="Gets the sample matching the provided name (if the sample exists). Organization defaults to CCDI-DCC.",
    operation_id="sample_show",
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
                            "name": "TARGET-10-DCC001-03A-01R"
                        },
                        "subject": {
                            "namespace": {
                                "organization": "CCDI-DCC",
                                "name": "phs002430"
                            },
                            "name": "TARGET-10-DCC001"
                        },
                        "metadata": {
                            "disease_phase": {"value": "Initial Diagnosis"},
                            "diagnosis": {"value": "Neuroblastoma","comment": "null" },
                            "diagnosis_category": [
                                {"value": "Brain and Spinal Cord Tumors"}
                            ],
                            "age_at_diagnosis": {"value": 10},
                            "anatomical_sites": [
                                {"value": "C71.9 : Brain, NOS"}
                            ],
                            "tissue_type": {"value": "Tumor"},
                            "tumor_classification": {"value": "Primary"},
                            "library_strategy": {"value": "WXS"},
                            "library_source_material": {"value": "Genomic DNA"},
                            "depositions": [
                                {
                                    "kind": "dbGaP",
                                    "value": "phs002430"
                                }
                            ],
                            "identifiers": [
                                {
                                    "value": {
                                        "namespace": {
                                            "organization": "CCDI-DCC",
                                            "name": "phs002430"
                                        },
                                        "name": "TARGET-10-DCC001-03A-01R",
                                        "server": "https://dcc.ccdi.cancer.gov/api/v1/sample/CCDI-DCC/phs002430/TARGET-10-DCC001-03A-01R",
                                        "type": "Linked"
                                    }
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
                                "entity": "Samples",
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
    """Get a specific sample by identifier. Organization must be CCDI-DCC."""
    # Validate organization - must be CCDI-DCC
    if organization != "CCDI-DCC":
        from app.models.errors import InvalidParametersError
        raise InvalidParametersError(parameters=[])
    
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
        # Re-raise to let the exception handler process it with proper format
        raise e.to_http_exception()
    except Exception as e:
        logger.error("Error getting sample", error=str(e), exc_info=True)
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
# Sample Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Reports summary information for the samples known by this server.",
    description="Reports summary information for the samples known by this server.",
    operation_id="sample_summary",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/SummaryResponse"},
                    "example": {
                        "counts": {
                            "total": 50000
                        }
                    }
                }
            }
        }
    }
)
async def get_samples_summary(
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for samples."""
    logger.info(
        "Get samples summary request",
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
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Get summary (no filters - returns total count of all samples)
        result = await service.get_samples_summary({})
        
        logger.info(
            "Get samples summary response",
            total_count=result.counts.total
        )
        
        return result
        
    except DatabaseConnectionError as e:
        # Database connection error - log clearly for AWS cloud monitoring
        logger.error(
            "Database connection error in get_samples_summary endpoint - returning empty result",
            error=str(e),
            error_type=type(e).__name__,
            is_database_connection_error=True,
            will_return_404=True,
            aws_cloudwatch_alert=True
        )
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
                "Database connection issue in get_samples_summary endpoint - returning empty result",
                error=str(e),
                error_type=type(e).__name__,
                is_connection_related=True,
                will_return_404=True,
                aws_cloudwatch_alert=True,
                exc_info=True
            )
        else:
            logger.error("Error getting samples summary", error=str(e), exc_info=True)
        
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
