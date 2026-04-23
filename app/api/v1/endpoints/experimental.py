"""
Experimental Diagnosis Search API routes for the CCDI Federation Service.

This module provides REST endpoints for experimental diagnosis search operations
on samples and subjects using case-insensitive substring matching.
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
    SamplesResponse,
    SubjectResponse,
    CountResponse,
    SummaryResponse
)
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind, InvalidParametersError
from app.services.sample import SampleService
from app.services.subject import SubjectService
from app.db.memgraph import DatabaseConnectionError

logger = get_logger(__name__)

router = APIRouter(tags=["Experimental"],  include_in_schema=True)


# ============================================================================
# Sample Diagnosis Search Endpoints
# ============================================================================


@router.get(
    "/sample-diagnosis",
    response_model=SamplesResponse,
    summary="Experimental: Filter the samples known by this server by free-text diagnosis search.",
    description="""Experimental: Filter the samples known by this server by free-text diagnosis search.

### Diagnosis Filtering

This endpoint supports the experimental `search` parameter.
For this parameter, the sample is included in the results if the value of its
its `diagnosis` field _contains_ the query string, or if an unharmonized field
treated by the implementer as a diagnosis field contains that query string.
Matches are case-insensitive.

### Pagination

This endpoint is paginated. Users may override the default pagination
parameters by providing one or more of the pagination-related query
parameters below.

### Additional Filtering

All harmonized (top-level) and unharmonized (nested under the
`metadata.unharmonized` key) metadata fields are filterable. To achieve
this, you can provide the field name as a [`String`]. Filtering follows the
following rules:

* For single-value metadata field, the sample is included in the results if
its value _exactly_ matches the query string. Matches are case-sensitive.
* For multiple-value metadata fields, the sample is included in the results
if any of its values for the field _exactly_ match the query string (a
logical OR [`||`]). Matches are case-sensitive.
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
in the `responses::Samples` schema.

Note: This API is experimental and is subject to change without being considered
as a breaking change.""",
    operation_id="sample_diagnosis_search",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "example": {
                        "summary": {"counts": {"all": 150, "current": 10}},
                        "data": [
                            {
                                "id": {
                                    "namespace": {
                                        "organization": "CCDI-DCC",
                                        "name": "phs002430",
                                    },
                                    "name": "TARGET-10-DCC001-03A-01R",
                                },
                                "subject": {
                                    "namespace": {
                                        "organization": "CCDI-DCC",
                                        "name": "phs002430",
                                    },
                                    "name": "TARGET-10-DCC001",
                                },
                                "metadata": {
                                    "disease_phase": {"value": "Initial Diagnosis"},
                                    "diagnosis": {"value": "Neuroblastoma","comment": "null" },
                                    "age_at_diagnosis": {"value": 10},
                                    "age_at_collection": {"value": 10},
                                    "anatomical_sites": [
                                        {"value": "C71.9 : Brain, NOS"}
                                    ],
                                    "tissue_type": {"value": "Tumor"},
                                    "tumor_classification": {"value": "Primary"},
                                    "tumor_grade": {"value": "G2 Moderately Differentiated"},
                                    "tumor_tissue_morphology": {"value": "Neuroblastoma"},
                                    "library_strategy": {"value": "WXS"},
                                    "library_selection_method": {"value": "Hybrid Selection"},
                                    "library_source_material": {"value": "Genomic DNA"},
                                    "preservation_method": {"value": "Frozen"},
                                    "specimen_molecular_analyte_type": {"value": "DNA"},
                                    "depositions": [
                                        {"kind": "dbGaP", "value": "phs002430"}
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
                                },
                            }
                        ],
                    }
                }
            },
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
                                "reason": "No data found.",
                            }
                        ]
                    },
                }
            },
        },
    },
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
        # Validate query parameters - check for unknown parameters
        # Note: "diagnosis" is NOT included - use "search" parameter for diagnosis filtering
        # When "search" is not provided, endpoint behaves like /sample for all other parameters
        allowed_params = {"search", "disease_phase", "anatomical_sites", "library_selection_method", 
                         "library_strategy", "library_source_material", "preservation_method", "tumor_grade",
                         "specimen_molecular_analyte_type", "tissue_type", "tumor_classification", 
                         "age_at_diagnosis", "age_at_collection", "tumor_tissue_morphology", 
                         "depositions", "identifiers", "page", "per_page"}
        
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
        # Create service
        cache_service = get_cache_service()
        service = SampleService(session, allowlist, settings, cache_service)
        
        # Dedicated /sample-diagnosis path: fetch data + total together
        samples, total_count = await service.get_samples_for_diagnosis_endpoint(
            filters=filters.copy(),
            offset=pagination.offset,
            limit=pagination.per_page,
        )
        # Build pagination info
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_items=total_count,  # # Use total_count from diagnosis endpoint path
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
        
        # Exclude gateways from individual samples
        samples_dicts = [sample.model_dump(exclude={'gateways'}) if hasattr(sample, 'model_dump') else {k: v for k, v in (sample if isinstance(sample, dict) else sample.__dict__).items() if k != 'gateways'} for sample in samples]
        
        # Build response with summary first, then data
        result = SamplesResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique samples
                    "current": len(samples)
                }
            },
            data=samples_dicts
        )
        
        logger.info(
            "Search samples by diagnosis response",
            sample_count=len(samples),
            total_count=total_count,
            page=pagination.page
        )
        
        return result
        
    except HTTPException:
        # Re-raise HTTPException as-is
        raise
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError to let the exception handler process it
        raise e.to_http_exception()
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
    summary="Experimental: Filter the subjects known by this server by free-text diagnosis search.",
    description="""Experimental: filter subjects using diagnosis nodes (`diagnosis`, `diagnosis_category`)
alongside the usual `GET /subject` query parameters. Supports harmonized and unharmonized
diagnosis category values at the data node, in line with the
[CCDI Federation API aggregation](https://cbiit.github.io/ccdi-federation-api-aggregation/) subject model (v1.3+).

`search`: case-insensitive substring on `diagnosis` (or `diagnosis_comment` when diagnosis is `see diagnosis_comment`).
`associated_diagnosis_categories`: substring on the full `diagnosis_category` on diagnosis node. Both together apply as AND on one diagnosis node.

Paginated. Experimental—may change.""",
    operation_id="subject_diagnosis_search",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "example": {
                        "summary": {
                            "counts": {
                                "all": 300,
                                "current": 15
                            }
                        },
                        "data": [
                            {
                                "id": {
                                    "namespace": {
                                        "organization": "CCDI-DCC",
                                        "name": "phs002430"
                                    },
                                    "name": "TARGET-10-DCC001"
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
                                                "name": "TARGET-10-DCC001",
                                                "type": "Linked",
                                                "server": "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002430/TARGET-10-DCC001"
                                            }
                                        }
                                    ],
                                    "vital_status": {"value": "Alive"},
                                    "age_at_vital_status": {"value": 12},
                                    "associated_diagnoses": [
                                        {"value": "Neuroblastoma","comment": "null" }
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
        # Validate query parameters - check for unknown parameters
        allowed_params = {
            "search",
            "associated_diagnosis_categories",
            "sex",
            "race",
            "ethnicity",
            "identifiers",
            "vital_status",
            "age_at_vital_status",
            "depositions",
            "page",
            "per_page",
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
        invalid_ethnicity_value = filters.get("_invalid_ethnicity")
        invalid_sex_value = filters.get("_invalid_sex")
        invalid_race_value = filters.get("_invalid_race")
        invalid_vital_status_value = filters.get("_invalid_vital_status")
        invalid_age_value = filters.get("_invalid_age_at_vital_status")

        if (
            invalid_ethnicity_value
            or invalid_sex_value
            or invalid_race_value
            or invalid_vital_status_value
            or invalid_age_value
        ):
            logger.info(
                "Invalid filter value detected in subject-diagnosis, returning empty result",
                invalid_ethnicity=invalid_ethnicity_value,
                invalid_sex=invalid_sex_value,
                invalid_race=invalid_race_value,
                invalid_vital_status=invalid_vital_status_value,
                invalid_age=invalid_age_value
            )

            return SubjectResponse(
                summary={
                    "counts": {
                        "all": 0,
                        "current": 0
                    }
                },
                data=[]
            )

        # Remove internal marker keys before repository/service handling
        filters.pop("_unknown_parameters", None)
        filters.pop("_invalid_ethnicity", None)
        filters.pop("_invalid_sex", None)
        filters.pop("_invalid_race", None)
        filters.pop("_invalid_vital_status", None)
        filters.pop("_invalid_age_at_vital_status", None)
        filters.pop("_age_at_vital_status_reason", None)

        # Create service
        cache_service = get_cache_service()
        service = SubjectService(session, allowlist, settings, cache_service)
        
        # Single round-trip: data + total together
        subjects, total_count = await service.get_subjects_for_diagnosis_endpoint(
            filters=filters,
            offset=pagination.offset,
            limit=pagination.per_page,
        )

        # Build pagination info
        # NOTE: total_count is the number of unique subjects matching filters (not just the current page size)
        # This is needed for accurate Link headers and client-side pagination.
        total_pages = None
        if total_count is not None and pagination.per_page:
            try:
                total_pages = (int(total_count) + int(pagination.per_page) - 1) // int(pagination.per_page)
            except Exception:
                total_pages = None

        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=total_pages,
            total_items=total_count,
            has_next=(pagination.page * pagination.per_page) < total_count if total_count is not None else (len(subjects) == pagination.per_page),
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
        
        # Exclude gateways from individual subjects and always include required metadata keys
        # (`associated_diagnoses`, `vital_status`, `age_at_vital_status`) even when null.
        subjects_dicts = [
            subject.model_dump(exclude={'gateways'}, exclude_none=False, exclude_unset=False)
            if hasattr(subject, 'model_dump')
            else {k: v for k, v in (subject if isinstance(subject, dict) else subject.__dict__).items() if k != 'gateways'}
            for subject in subjects
        ]
        
        # Build response with summary first, then data
        result = SubjectResponse(
            summary={
                "counts": {
                    "all": total_count,  # Total number of unique subjects
                    "current": len(subjects)
                }
            },
            data=subjects_dicts
        )
        
        logger.info(
            "Search subjects by diagnosis response",
            subject_count=len(subjects),
            total_count=total_count,
            page=pagination.page
        )
        
        return result
        
    except HTTPException:
        # Re-raise HTTPException as-is
        raise
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError to let the exception handler process it
        raise e.to_http_exception()
    except DatabaseConnectionError as e:
        # Database connection error - log clearly for AWS cloud monitoring
        logger.error(
            "Database connection error in subject-diagnosis endpoint - returning empty result",
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
                "Database connection issue in subject-diagnosis endpoint - returning empty result",
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
