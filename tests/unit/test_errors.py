"""
Unit tests for error models and helpers.
"""

import pytest
from fastapi import HTTPException, status

from app.models.errors import (
    ErrorKind,
    ErrorDetail,
    ErrorsResponse,
    CCDIException,
    InvalidRouteError,
    InvalidParametersError,
    ValidationError,
    UnsupportedFieldError,
    NotFoundError,
    UnshareableDataError,
    InternalServerError,
    create_pagination_error,
    create_unsupported_field_error,
    create_entity_not_found_error,
    create_unshareable_data_error,
    create_message_only_error,
)


@pytest.mark.unit
def test_ccdi_exception_message_generation():
    """Test CCDIException message generation for each kind."""
    exc = CCDIException(kind=None)
    assert exc.message == "Unable to find data for your request."

    exc = CCDIException(kind=ErrorKind.INVALID_ROUTE, method="GET", route="/bad")
    assert "Invalid route: GET /bad" in exc.message

    exc = CCDIException(kind=ErrorKind.INVALID_PARAMETERS, parameters=["a", "b"])
    assert "Invalid parameters" in exc.message

    exc = CCDIException(kind=ErrorKind.NOT_FOUND, entity="Subject")
    assert exc.message == "Subject not found."

    exc = CCDIException(kind=ErrorKind.UNSHAREABLE_DATA, entity="Files")
    assert exc.message == "Unable to share data for Files"

    exc = CCDIException(kind=ErrorKind.UNSUPPORTED_FIELD, field="race")
    assert exc.message == "Field 'race' is not supported"

    exc = CCDIException(kind="Other")
    assert exc.message == "An error occurred."


@pytest.mark.unit
def test_ccdi_exception_to_error_detail_and_http_exception():
    """Test CCDIException conversions."""
    exc = CCDIException(
        kind=ErrorKind.INVALID_PARAMETERS,
        status_code=status.HTTP_400_BAD_REQUEST,
        parameters=["secret"],
        reason="Bad params"
    )
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.INVALID_PARAMETERS
    assert "parameters" in detail

    http_exc = exc.to_http_exception()
    assert isinstance(http_exc, HTTPException)
    assert http_exc.status_code == status.HTTP_400_BAD_REQUEST
    assert "errors" in http_exc.detail


@pytest.mark.unit
def test_invalid_route_error_detail_sanitized():
    """Test InvalidRouteError sanitizes route."""
    exc = InvalidRouteError(method="GET", route="/bad")
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.INVALID_ROUTE
    assert detail["route"] == "Invalid route requested."


@pytest.mark.unit
def test_invalid_parameters_error_ignores_parameters():
    """Test InvalidParametersError hides parameters."""
    exc = InvalidParametersError(parameters=["secret"])
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.INVALID_PARAMETERS
    assert "parameters" not in detail


@pytest.mark.unit
def test_validation_error_sets_kind():
    """Test ValidationError uses InvalidParameters kind."""
    exc = ValidationError("Invalid input")
    assert exc.kind == ErrorKind.INVALID_PARAMETERS
    assert "Invalid input" in exc.message


@pytest.mark.unit
def test_unsupported_field_error_sanitizes_field():
    """Test UnsupportedFieldError hides field name in detail."""
    exc = UnsupportedFieldError("race", "subjects")
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.UNSUPPORTED_FIELD
    assert detail["field"] == "wrong field"
    assert "subjects" in detail["message"]


@pytest.mark.unit
def test_not_found_and_unshareable_errors():
    """Test NotFoundError and UnshareableDataError defaults."""
    exc = NotFoundError("Subjects")
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.NOT_FOUND
    assert detail["entity"] == "Subjects"

    exc = UnshareableDataError("Files")
    detail = exc.to_error_detail().model_dump(exclude_none=True)
    assert detail["kind"] == ErrorKind.UNSHAREABLE_DATA
    assert detail["entity"] == "Files"


@pytest.mark.unit
def test_internal_server_error_defaults():
    """Test InternalServerError default message."""
    exc = InternalServerError()
    assert exc.kind == ErrorKind.INTERNAL_SERVER_ERROR
    assert exc.message == "An internal server error occurred"


@pytest.mark.unit
def test_error_helpers():
    """Test helper functions create correct error types."""
    exc = create_pagination_error(page=0, per_page=0)
    assert isinstance(exc, InvalidParametersError)
    assert exc.reason == "Unknown query parameter(s)"

    exc = create_unsupported_field_error("field", "subjects")
    assert isinstance(exc, UnsupportedFieldError)

    exc = create_entity_not_found_error("subject", "org", "ns", "name")
    assert isinstance(exc, NotFoundError)
    assert exc.entity == "Subjects"

    exc = create_unshareable_data_error("file")
    assert isinstance(exc, UnshareableDataError)
    assert exc.entity == "Files"

    detail = create_message_only_error()
    payload = detail.model_dump(exclude_none=True)
    assert payload == {"message": "Unable to find data for your request."}

