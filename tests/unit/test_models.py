"""
Unit tests for data models and DTOs.
"""

import pytest
from pydantic import ValidationError
from app.models.dto import Subject, CountResponse, SummaryResponse
from app.models.errors import ErrorDetail, ErrorKind, ErrorsResponse



@pytest.mark.unit
def test_error_detail_model():
    """Test ErrorDetail model."""
    error = ErrorDetail(
        kind=ErrorKind.NOT_FOUND,
        entity="Subject",
        message="Subject not found",
        reason="No subject with the given ID exists"
    )
    
    assert error.kind == ErrorKind.NOT_FOUND
    assert error.entity == "Subject"
    assert error.message == "Subject not found"


@pytest.mark.unit
def test_errors_response_model():
    """Test ErrorsResponse model."""
    error1 = ErrorDetail(
        kind=ErrorKind.NOT_FOUND,
        entity="Subject",
        message="Not found"
    )
    error2 = ErrorDetail(
        kind=ErrorKind.INVALID_PARAMETERS,
        entity="Query",
        message="Invalid parameter"
    )
    
    errors_response = ErrorsResponse(errors=[error1, error2])
    
    assert len(errors_response.errors) == 2
    assert errors_response.errors[0].kind == ErrorKind.NOT_FOUND
    assert errors_response.errors[1].kind == ErrorKind.INVALID_PARAMETERS


@pytest.mark.unit
def test_error_detail_serialization():
    """Test ErrorDetail serialization."""
    error = ErrorDetail(
        kind=ErrorKind.NOT_FOUND,
        entity="Subject",
        message="Subject not found"
    )
    
    error_dict = error.model_dump()
    
    assert error_dict["kind"] == "NotFound"
    assert error_dict["entity"] == "Subject"
    assert error_dict["message"] == "Subject not found"
