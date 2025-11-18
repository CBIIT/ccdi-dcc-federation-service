"""
Error handling and exception classes for the CCDI Federation Service.

This module provides custom exceptions and error responses according to
the OpenAPI specification.
"""

from typing import List, Optional

from fastapi import HTTPException, status
from pydantic import BaseModel, ConfigDict


class ErrorKind:
    """Error kinds as defined in the OpenAPI specification."""
    INVALID_ROUTE = "InvalidRoute"
    INVALID_PARAMETERS = "InvalidParameters"
    UNSUPPORTED_FIELD = "UnsupportedField"
    NOT_FOUND = "NotFound"
    UNSHAREABLE_DATA = "UnshareableData"
    INTERNAL_SERVER_ERROR = "InternalServerError"


class ErrorDetail(BaseModel):
    """Individual error detail model matching OpenAPI specification.
    
    Fields vary by error kind:
    - InvalidRoute: kind, method, route, message
    - InvalidParameters: kind, parameters, message, reason
    - NotFound: kind, entity, message, reason
    - UnshareableData: kind, entity, message, reason
    - UnsupportedField: kind, field, message, reason
    """
    
    model_config = ConfigDict(exclude_none=True)
    
    kind: str
    message: Optional[str] = None
    # InvalidRoute fields
    method: Optional[str] = None
    route: Optional[str] = None
    # InvalidParameters fields
    parameters: Optional[List[str]] = None
    # NotFound/UnshareableData fields
    entity: Optional[str] = None
    # UnsupportedField fields
    field: Optional[str] = None
    reason: Optional[str] = None


class ErrorsResponse(BaseModel):
    """Error response model matching OpenAPI specification."""
    model_config = ConfigDict(exclude_none=True)
    
    errors: List[ErrorDetail]


class CCDIException(Exception):
    """Base exception for CCDI Federation Service."""
    
    def __init__(
        self, 
        kind: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        method: Optional[str] = None,
        route: Optional[str] = None,
        parameters: Optional[List[str]] = None,
        field: Optional[str] = None,
        entity: Optional[str] = None,
        message: Optional[str] = None,
        reason: Optional[str] = None
    ):
        """Initialize CCDI exception."""
        self.kind = kind
        self.status_code = status_code
        self.method = method
        self.route = route
        self.parameters = parameters or []
        self.field = field
        self.entity = entity
        self.message = message or self._generate_message()
        self.reason = reason
        super().__init__(self.message)
    
    def _generate_message(self) -> str:
        """Generate error message from error details."""
        if self.kind == ErrorKind.INVALID_ROUTE:
            return f"Invalid route: {self.method} {self.route}"
        elif self.kind == ErrorKind.INVALID_PARAMETERS:
            param_list = "', '".join(self.parameters) if self.parameters else ""
            return f"Invalid parameter{'s' if len(self.parameters) > 1 else ''} '{param_list}'"
        elif self.kind == ErrorKind.NOT_FOUND:
            return f"{self.entity or 'Resource'} not found."
        elif self.kind == ErrorKind.UNSHAREABLE_DATA:
            return f"Unable to share data for {self.entity or 'resource'}"
        elif self.kind == ErrorKind.UNSUPPORTED_FIELD:
            return f"Field '{self.field}' is not supported"
        else:
            return "An error occurred."
    
    def to_error_detail(self) -> ErrorDetail:
        """Convert exception to error detail."""
        return ErrorDetail(
            kind=self.kind,
            method=self.method,
            route=self.route,
            parameters=self.parameters if self.parameters else None,
            field=self.field,
            entity=self.entity,
            message=self.message,
            reason=self.reason
        )
    
    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException."""
        return HTTPException(
            status_code=self.status_code,
            detail=ErrorsResponse(errors=[self.to_error_detail()]).model_dump(exclude_none=True)
        )


class InvalidRouteError(CCDIException):
    """Invalid API route error."""
    
    def __init__(
        self, 
        method: str,
        route: str,
        status_code: int = status.HTTP_404_NOT_FOUND,
        message: Optional[str] = None
    ):
        if message is None:
            message = f"Invalid route: {method} {route}"
        super().__init__(
            kind=ErrorKind.INVALID_ROUTE,
            status_code=status_code,
            method=method,
            route=route,
            message=message
        )


class InvalidParametersError(CCDIException):
    """Invalid query or path parameters error."""
    
    def __init__(
        self, 
        parameters: List[str], 
        message: Optional[str] = None,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        method: Optional[str] = None,
        route: Optional[str] = None,
        reason: Optional[str] = None
    ):
        if message is None:
            message = "Invalid query parameter(s) provided."
        if reason is None:
            reason = "The parameter value is invalid or incorrectly formatted."
        super().__init__(
            kind=ErrorKind.INVALID_PARAMETERS,
            status_code=status_code,
            parameters=parameters,
            method=method,
            route=route,
            message=message,
            reason=reason
        )


class ValidationError(CCDIException):
    """General validation error for invalid input parameters."""
    
    def __init__(
        self, 
        message: str,
        status_code: int = status.HTTP_400_BAD_REQUEST
    ):
        super().__init__(
            kind=ErrorKind.INVALID_PARAMETERS,
            status_code=status_code,
            parameters=["input"],
            message=message
        )


class UnsupportedFieldError(CCDIException):
    """Unsupported field error for count/filter operations."""
    
    def __init__(
        self, 
        field: str, 
        entity_type: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        message: Optional[str] = None,
        reason: Optional[str] = None
    ):
        if message is None:
            message = f"Field is not supported: a field is not present for {entity_type}."
        if reason is None:
            reason = "The requested field is not found."
        
        super().__init__(
            kind=ErrorKind.UNSUPPORTED_FIELD,
            status_code=status_code,
            field=field,
            message=message,
            reason=reason
        )
    
    def to_error_detail(self) -> ErrorDetail:
        """Convert exception to error detail, using 'wrong field' instead of actual field value."""
        return ErrorDetail(
            kind=self.kind,
            method=self.method,
            route=self.route,
            parameters=self.parameters if self.parameters else None,
            field="wrong field",  # Use "wrong field" instead of actual field value
            entity=self.entity,
            message=self.message,
            reason=self.reason
        )


class NotFoundError(CCDIException):
    """Entity not found error."""
    
    def __init__(
        self, 
        entity: str,
        status_code: int = status.HTTP_404_NOT_FOUND,
        message: Optional[str] = None,
        reason: Optional[str] = None
    ):
        if message is None:
            message = f"{entity} not found."
        if reason is None:
            reason = "The requested resource does not exist."
        super().__init__(
            kind=ErrorKind.NOT_FOUND,
            status_code=status_code,
            entity=entity,
            message=message,
            reason=reason
        )


class UnshareableDataError(CCDIException):
    """Data cannot be shared error."""
    
    def __init__(
        self, 
        entity: str,
        message: str = "Our agreement with data providers prohibits us from sharing line-level data.",
        status_code: int = status.HTTP_404_NOT_FOUND,
        reason: Optional[str] = None
    ):
        if reason is None:
            reason = "Data sharing is restricted by agreement with data providers."
        super().__init__(
            kind=ErrorKind.UNSHAREABLE_DATA,
            status_code=status_code,
            entity=entity,
            message=message,
            reason=reason
        )


class InternalServerError(CCDIException):
    """Internal server error."""
    
    def __init__(
        self, 
        message: Optional[str] = None
    ):
        if message is None:
            message = "An internal server error occurred"
        super().__init__(
            kind=ErrorKind.INTERNAL_SERVER_ERROR,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            message=message
        )


# Utility functions for common error scenarios

def create_pagination_error(page: Optional[int] = None, per_page: Optional[int] = None) -> InvalidParametersError:
    """Create a pagination parameter error."""
    parameters = []
    if page is not None and page < 1:
        parameters.append("page")
    if per_page is not None and per_page < 1:
        parameters.append("per_page")
    
    param_list = "', '".join(parameters) if parameters else "page and per_page"
    reason = f"Invalid value for parameter{'s' if len(parameters) > 1 else ''} '{param_list}': unable to calculate offset."
    
    return InvalidParametersError(
        parameters=parameters or ["page", "per_page"],
        message="Invalid query parameter(s) provided.",
        reason=reason
    )


def create_unsupported_field_error(field: str, entity_type: str) -> UnsupportedFieldError:
    """Create an unsupported field error."""
    return UnsupportedFieldError(field, entity_type)


def create_entity_not_found_error(
    entity_type: str, 
    organization: Optional[str] = None,
    namespace: Optional[str] = None, 
    name: Optional[str] = None
) -> NotFoundError:
    """Create an entity not found error."""
    if organization and namespace and name:
        entity = f"{entity_type} with namespace '{namespace}' and name '{name}'"
    else:
        entity = entity_type.title() + "s"
    
    return NotFoundError(entity)


def create_unshareable_data_error(entity_type: str) -> UnshareableDataError:
    """Create an unshareable data error."""
    return UnshareableDataError(entity_type.title() + "s")
