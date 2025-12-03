"""
File API routes for the CCDI Federation Service.

This module provides REST endpoints for sequencing file operations
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
    get_file_filters,
    get_file_filters_no_descriptions,
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
from app.models.errors import NotFoundError, ValidationError, InvalidParametersError, UnsupportedFieldError, InvalidRouteError, ErrorDetail, ErrorsResponse, ErrorKind
from app.services.file import FileService

logger = get_logger(__name__)

router = APIRouter(prefix="/file", tags=["File"])


# ============================================================================
# File Listing
# ============================================================================

@router.get(
    "",
    response_model=None,  # Returning custom dict structure
    summary="Gets the sequencing files known by this server.",
    description="""Gets the sequencing files known by this server.

### Pagination

This endpoint is paginated. Users may override the default pagination
parameters by providing one or more of the pagination-related query
parameters below.

### Filtering

All harmonized (top-level) and unharmonized (nested under the
`metadata.unharmonized` key) metadata fields are filterable. To achieve
this, you can provide the field name as a [`String`]. Filtering follows the
following rules:

* For single-value metadata field, the sequencing file is included in the results if
its value _exactly_ matches the query string. Matches are case-sensitive.
* For multiple-value metadata fields, the sequencing file is included in the results
if any of its values for the field _exactly_ match the query string (a
logical OR (`||`)). Matches are case-sensitive.
* When the metadata field is `null` (in the case of singular or
multiple-valued metadata fields) or empty, the sequencing file is not included.
* When multiple fields are provided as filters, a logical AND (`&&`) strings
together the predicates. In other words, all filters must match for a
sequencing file to be returned. Note that this means that servers do not natively
support logical OR (`||`) across multiple fields: that must be done by
calling this endpoint with each of your desired queries and performing a
set union of those files out of band.

### Ordering

This endpoint has default ordering requirements—those details are documented
in the `responses::Files` schema.""",
    operation_id="file_index",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "example": {
                        "summary": {
                            "counts": {
                                "all": 1000,
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
                                    "name": "2af63d08-a883-535f-906f-87d862f400d2"
                                },
                                "samples": [
                                    {
                                        "namespace": {
                                            "organization": "CCDI-DCC",
                                            "name": "phs002430"
                                        },
                                        "name": "sample1-WGS"
                                    }
                                ],
                                "metadata": {
                                    "size": {"value": 74880783442},
                                    "type": {"value": "Sequence Record Format"},
                                    "checksums": {"value": "2f6a4ef817eb907bdbeee8c35c40ca09"},
                                    "description": {"value": "Sequence Record"},
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
                                "entity": "Files",
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
    """List sequencing files with pagination and filtering."""
    logger.info(
        "List sequencing files request",
        filters=filters,
        page=pagination.page,
        per_page=pagination.per_page,
        path=request.url.path
    )
    
    try:
        # Validate that no unknown query parameters are provided
        allowed_params = {
            "type", "size", "checksums", "description", "depositions", 
            "page", "per_page"
        }
        
        unknown_params = []
        for key in request.query_params.keys():
            if not key.startswith("metadata.unharmonized.") and key not in allowed_params:
                unknown_params.append(key)
        
        if unknown_params:
            raise InvalidParametersError(
                parameters=[],  # Empty array - don't expose parameter names
                message="Invalid query parameter(s) provided.",
                reason="Unknown query parameter(s)"
            )
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get sequencing files
        files = await service.get_files(
            filters=filters,
            offset=pagination.offset,
            limit=pagination.per_page
        )
        
        # Get total count for summary
        summary_result = await service.get_files_summary(filters)
        total_count = summary_result.counts.total
        
        # Build pagination info for Link header
        pagination_info = PaginationInfo(
            page=pagination.page,
            per_page=pagination.per_page,
            total_pages=None,
            total_count=total_count
        )
        
        # Add Link header for pagination
        link_header = build_link_header(
            request=request,
            pagination=pagination_info,
            extra_params=dict(request.query_params)
        )
        
        if link_header:
            response.headers["Link"] = link_header
        
        # Convert files to dict format (exclude gateways)
        files_dicts = [file.model_dump(exclude={'gateways'}) if hasattr(file, 'model_dump') else {k: v for k, v in (file if isinstance(file, dict) else file.__dict__).items() if k != 'gateways'} for file in files]
        
        # Build response with summary (counts) and data structure
        result = {
            "summary": {
                "counts": {
                    "all": total_count,
                    "current": len(files)
                }
            },
            "data": files_dicts
        }
        
        logger.info(
            "List sequencing files response",
            file_count=len(files),
            total_count=total_count,
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
        logger.error("Error listing sequencing files", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Files",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )


# ============================================================================
# File Counting by Field
# ============================================================================

@router.get(
    "/by/{field}/count",
    response_model=CountResponse,
    summary="Groups the sequencing files by the specified metadata field and returns counts.",
    description="Groups the sequencing files by the specified metadata field and returns counts. Only 'type' and 'depositions' are supported.",
    operation_id="files_by_count",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/CountResponse"},
                    "example": {
                        "total": 1000,
                        "missing": 50,
                        "values": [
                            {
                                "value": "fastq",
                                "count": 500
                            },
                            {
                                "value": "WGS",
                                "count": 300
                            },
                            {
                                "value": "Sequence Record Format",
                                "count": 200
                            }
                        ]
                    }
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
                                "message": "Field 'handedness' is not supported: this field is not present for files."
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def count_files_by_field(
    request: Request,
    field: str = Path(
        ...,
        description="The field to group by and count. Only 'type' and 'depositions' are supported.",
        example="type"
    ),
    filters: Dict[str, Any] = Depends(get_file_filters_no_descriptions),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Count sequencing files grouped by a specific field."""
    logger.info(
        "Count sequencing files by field request",
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
            "Count sequencing files by field response",
            field=field,
            count_items=len(result.values)
        )
        
        return result
        
    except UnsupportedFieldError as e:
        logger.warning("Unsupported field error counting sequencing files by field", error=str(e), field=field)
        # Re-raise to let the exception handler process it with proper format
        raise e.to_http_exception()
    except InvalidParametersError as e:
        logger.warning("Invalid parameters error counting sequencing files by field", error=str(e), field=field)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        error_detail = ErrorDetail(
            kind=ErrorKind.INVALID_PARAMETERS,
            entity="Files",
            message=str(e),
            reason="Invalid parameter provided."
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    except ValidationError as e:
        logger.warning("Validation error counting sequencing files by field", error=str(e), field=field)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Files",
            message=str(e),
            reason="Query validation or timeout error."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    except Exception as e:
        logger.error("Error counting sequencing files by field", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Files",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

# ============================================================================
# Individual File Retrieval
# ============================================================================

@router.get(
    "/{organization}/{namespace}/{name}",
    response_model=File,
    summary="Gets the sequencing file matching the provided name (if the file exists).",
    description="Gets the sequencing file matching the provided name (if the file exists). Organization must be 'CCDI-DCC'. Namespace is the study_id value from the database. Name is the file_id field value.",
    operation_id="file_show",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/File"},
                    "example": {
                        "id": {
                            "namespace": {
                                "organization": "CCDI-DCC",
                                "name": "phs002430"
                            },
                            "name": "b51fd7ab-e464-5012-b418-3502af28980d"
                        },
                        "samples": [
                            {
                                "namespace": {
                                    "organization": "CCDI-DCC",
                                    "name": "phs002430"
                                },
                                "name": "SJSA_sample"
                            }
                        ],
                        "metadata": {
                            "depositions": [
                                {
                                    "kind": "dbGaP",
                                    "value": "phs002430"
                                }
                            ],
                            "type": {"value": "fastq"},
                            "size": {"value": 12340},
                            "checksums": {"value": "608449b55fe5480b3562a967eb7dc2b0"},
                            "description": {"value": "WGS FASTQ Files R2"}
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
                                "entity": "Files",
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
    """Get a specific sequencing file by identifier."""
    logger.info(
        "Get sequencing file request",
        organization=organization,
        namespace=namespace,
        name=name,
        path=request.url.path
    )
    
    # Check if this looks like a malformed request for /by/{field}/count endpoint
    # Paths like /by1/.../count or /by/.../count suggest it was meant for count endpoint
    path = request.url.path
    if "/by" in path.lower() or name == "count" or path.endswith("/count"):
        # Log the invalid path but don't include it in the response
        logger.warning(
            "Invalid route detected - possible malformed path for count endpoint",
            path=path,
            organization=organization,
            namespace=namespace,
            name=name
        )
        raise InvalidRouteError(
            method=request.method,
            route=path,
            message="Invalid route requested."
        )
    
    # Validate organization
    if organization != "CCDI-DCC":
        # Log the invalid value but don't include it in the response
        logger.warning(
            "Invalid organization provided",
            organization=organization,
            namespace=namespace,
            name=name,
            path=request.url.path
        )
        # Return InvalidParameters error instead of organization-specific message
        raise InvalidParametersError(
            entity="Files",
            message="Invalid query parameter(s) provided.",
            reason="Unknown query parameter(s)"
        )
    
    try:
        # Create service
        cache_service = get_cache_service()
        service = FileService(session, allowlist, settings, cache_service)
        
        # Get sequencing file
        file = await service.get_file_by_identifier(organization, namespace, name)
        
        logger.info(
            "Get sequencing file response",
            organization=organization,
            namespace=namespace,
            name=name,
            file_data=getattr(file, 'id', str(file)[:50])  # Flexible logging
        )
        
        # Return file dict excluding gateways (keep as placeholder in code)
        return file.model_dump(exclude={'gateways'})
        
    except InvalidRouteError as e:
        # Re-raise to let the exception handler process it with proper format
        raise e.to_http_exception()
    except NotFoundError as e:
        logger.warning("Sequencing file not found", organization=organization, namespace=namespace, name=name)
        # Re-raise to let the exception handler process it with proper format
        raise e.to_http_exception()
    except Exception as e:
        logger.error("Error getting sequencing file", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Files",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

# ============================================================================
# File Summary
# ============================================================================

@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Reports summary information for the sequencing files known by this server.",
    description="Reports summary information for the sequencing files known by this server.",
    operation_id="file_summary",
    responses={
        200: {
            "description": "Successful operation.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/SummaryResponse"},
                    "example": {
                        "counts": {
                            "total": 123456
                        }
                    }
                }
            }
        }
    }
)
async def get_files_summary(
    request: Request,
    filters: Dict[str, Any] = Depends(get_file_filters_no_descriptions),
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    allowlist: FieldAllowlist = Depends(get_allowlist),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get summary statistics for sequencing files."""
    logger.info(
        "Get sequencing files summary request",
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
            "Get sequencing files summary response",
            total_count=result.counts.total
        )
        
        return result
        
    except Exception as e:
        logger.error("Error getting sequencing files summary", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Files",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

