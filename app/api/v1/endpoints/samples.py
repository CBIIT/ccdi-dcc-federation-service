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
    get_sample_filters_no_descriptions,
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
                         "depositions", "diagnosis", "identifiers", "page", "per_page"}
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
        # so that get_samples_summary gets the original filters dict
        filters_copy = filters.copy()
        
        # Get samples
        samples = await service.get_samples(
            filters=filters_copy,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Get total count for summary (use original filters dict)
        # If summary fails, use 0 as total (empty results)
        try:
            summary_result = await service.get_samples_summary(filters)
            total_count = summary_result.counts.total
        except Exception as summary_error:
            # If summary fails, log but don't fail the request - return 0 as total
            logger.warning(
                "Error getting samples summary, using 0 as total",
                error=str(summary_error),
                exc_info=True
            )
            total_count = 0
        
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
            total_count=total_count,
            page=pagination.page
        )
        
        # Exclude gateways from individual samples (keep as placeholder in code)
        samples_dicts = [sample.model_dump(exclude={'gateways'}) if hasattr(sample, 'model_dump') else {k: v for k, v in (sample if isinstance(sample, dict) else sample.__dict__).items() if k != 'gateways'} for sample in samples]
        
        # Build response with summary first, then data
        # Always return 200 with empty data if query succeeded but no results found
        result = SamplesResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique samples
                    "current": len(samples)
                }
            },
            data=samples_dicts
        )
        
        return result
        
    except Exception as e:
        # Check if this is a parameter validation error or query error
        if isinstance(e, (InvalidParametersError, UnsupportedFieldError)):
            # This is a parameter validation error - raise it directly
            logger.error("Invalid parameters in request", error=str(e), exc_info=True)
            raise e.to_http_exception()
        
        # Check if this is a query error (like UnboundVariable, syntax error) that indicates a real problem
        # vs a scenario where we should return empty results
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
        else:
            # For other errors, log but return 200 with empty results (don't expose internal errors)
            logger.warning(
                "Error listing samples, returning empty results",
                error=str(e),
                exc_info=True
            )
            # Return 200 with empty data instead of 404
            result = SamplesResponse(
                summary={
                    "counts": {
                        "all": 0,
                        "current": 0
                    }
                },
                data=[]
            )
            return result


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
                                "field": "handedness",
                                "message": "Field 'handedness' is not supported: this field is not present for samples."
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
            "tumor_tissue_morphology", "diagnosis"
        ],
    ),
    filters: Dict[str, Any] = Depends(get_sample_filters_no_descriptions),
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
            count_items=len(result.values)
        )
        
        return result
        
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
                filters=filters,
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
                    "schema": {"$ref": "#/components/schemas/SummaryResponse"}
                }
            }
        }
    }
)
async def get_samples_summary(
    request: Request,
    filters: Dict[str, Any] = Depends(get_sample_filters_no_descriptions),
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
            total_count=result.counts.total
        )
        
        return result
        
    except Exception as e:
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
