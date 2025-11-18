"""
Error examples API routes for the CCDI Federation Service.

This module provides endpoints for viewing error response examples,
helping developers understand the error response format.
"""

from typing import Optional
from fastapi import APIRouter, Query
from app.core.logging import get_logger
from app.models.errors import ErrorsResponse, ErrorDetail, ErrorKind

logger = get_logger(__name__)

router = APIRouter(prefix="/errors", tags=["Errors"])


@router.get(
    "/examples",
    response_model=ErrorsResponse,
    summary="Error response examples",
    description="""
    This endpoint provides example error responses for different error types.
    This is a reference endpoint showing the structure of error responses
    that may be returned by other endpoints.
    
    **Error Types:**
    - `InvalidRoute`: Returned when requesting a non-existent route
    - `InvalidParameters`: Returned when request parameters are invalid
    - `NotFound`: Returned when a requested resource is not found
    - `UnshareableData`: Returned when data cannot be shared due to agreements
    - `UnsupportedField`: Returned when a field is not supported for operations
    
    For 400 or 404 errors, always use a customized response to avoid exposing
    any internal error messages or user inputs. Any error not covered by the
    categories below should return a 404 NotFound error.
    """
)
async def get_error_examples(
    error_type: Optional[str] = Query(
        default="all",
        description="Type of error example to return",
        enum=["InvalidRoute", "InvalidParameters", "NotFound", "UnshareableData", "UnsupportedField", "all"]
    )
) -> ErrorsResponse:
    """
    Get error response examples.
    
    Args:
        error_type: Type of error example to return, or 'all' for all examples
        
    Returns:
        ErrorsResponse containing example error details
    """
    errors = []
    
    if error_type == "all" or error_type == "InvalidRoute":
        errors.append(ErrorDetail(
            kind=ErrorKind.INVALID_ROUTE,
            method="GET",
            route="/foobar",
            message="Invalid route: GET /foobar"
        ))
    
    if error_type == "all" or error_type == "InvalidParameters":
        errors.append(ErrorDetail(
            kind=ErrorKind.INVALID_PARAMETERS,
            parameters=[],  # Empty array - don't expose parameter names
            message="Invalid query parameter(s) provided.",
            reason="Unknown query parameter(s)"
        ))
    
    if error_type == "all" or error_type == "NotFound":
        errors.append(ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Samples",
            message="Unable to find data for your request.",
            reason="No data found."
        ))
    
    if error_type == "all" or error_type == "UnshareableData":
        errors.append(ErrorDetail(
            kind=ErrorKind.UNSHAREABLE_DATA,
            entity="Sample",
            message="Our agreement with data providers prohibits us from sharing line-level data.",
            reason="Data sharing is restricted by agreement with data providers."
        ))
    
    if error_type == "all" or error_type == "UnsupportedField":
        errors.append(ErrorDetail(
            kind=ErrorKind.UNSUPPORTED_FIELD,
            field="wrong field",
            message="Field is not supported for subjects.",
            reason="This field is not present for subjects."
        ))
    
    logger.debug(
        "Returning error examples",
        error_type=error_type,
        count=len(errors)
    )
    
    response = ErrorsResponse(errors=errors)
    return response

