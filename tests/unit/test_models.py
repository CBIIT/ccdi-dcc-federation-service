"""
Unit tests for data models and DTOs.
"""

import pytest
from pydantic import ValidationError
from app.models.dto import (
    CountResponse,
    File,
    NamespaceIdentifier,
    NamespaceMetadata,
    Sample,
    SampleIdentifier,
    SampleMetadata,
    Subject,
    SubjectId,
    SubjectMetadata,
    SummaryResponse,
)
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


@pytest.mark.unit
def test_subject_defaults_kind_and_gateways():
    """Test Subject defaults for kind and gateways."""
    subject = Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name="phs1"), name="P1"),
        metadata=SubjectMetadata(),
    )

    assert subject.kind == "Participant"
    assert subject.gateways == []
    payload = subject.model_dump()
    assert "gateways" not in payload


@pytest.mark.unit
def test_sample_defaults_gateways():
    """Test Sample defaults for gateways."""
    sample = Sample(
        id=SampleIdentifier(namespace={"organization": "CCDI-DCC", "name": "phs1"}, name="S1"),
        metadata=SampleMetadata(),
    )

    assert sample.gateways == []
    payload = sample.model_dump()
    assert "gateways" not in payload


@pytest.mark.unit
def test_file_allows_extra_fields():
    """Test File model allows extra fields."""
    file_obj = File(custom_field="value")

    assert file_obj.model_dump()["custom_field"] == "value"


@pytest.mark.unit
def test_namespace_metadata_includes_nulls():
    """Test NamespaceMetadata includes None values in output."""
    metadata = NamespaceMetadata()
    payload = metadata.model_dump()

    assert "study_short_title" in payload
    assert "study_id" in payload
