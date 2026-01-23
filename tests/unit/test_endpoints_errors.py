"""
Unit tests for error endpoints.

Tests error response examples and error handling.
"""

import pytest
from unittest.mock import Mock
from fastapi import Request
from app.api.v1.endpoints.errors import (
    get_error_examples,
    router as errors_router
)
from app.models.errors import ErrorKind, ErrorsResponse


@pytest.mark.unit
class TestErrorEndpoints:
    """Test cases for error API endpoints."""

    async def test_get_error_examples_all(self):
        """Test getting all error examples."""
        result = await get_error_examples(error_type="all")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 5  # All error types
        error_kinds = [error.kind for error in result.errors]
        assert ErrorKind.INVALID_ROUTE in error_kinds
        assert ErrorKind.INVALID_PARAMETERS in error_kinds
        assert ErrorKind.NOT_FOUND in error_kinds
        assert ErrorKind.UNSHAREABLE_DATA in error_kinds
        assert ErrorKind.UNSUPPORTED_FIELD in error_kinds

    async def test_get_error_examples_invalid_route(self):
        """Test getting InvalidRoute error example."""
        result = await get_error_examples(error_type="InvalidRoute")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 1
        assert result.errors[0].kind == ErrorKind.INVALID_ROUTE
        assert result.errors[0].route == "/foobar"

    async def test_get_error_examples_invalid_parameters(self):
        """Test getting InvalidParameters error example."""
        result = await get_error_examples(error_type="InvalidParameters")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 1
        assert result.errors[0].kind == ErrorKind.INVALID_PARAMETERS
        assert result.errors[0].parameters == []

    async def test_get_error_examples_not_found(self):
        """Test getting NotFound error example."""
        result = await get_error_examples(error_type="NotFound")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 1
        assert result.errors[0].kind == ErrorKind.NOT_FOUND
        assert result.errors[0].entity == "Samples"

    async def test_get_error_examples_unshareable_data(self):
        """Test getting UnshareableData error example."""
        result = await get_error_examples(error_type="UnshareableData")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 1
        assert result.errors[0].kind == ErrorKind.UNSHAREABLE_DATA
        assert result.errors[0].entity == "Sample"

    async def test_get_error_examples_unsupported_field(self):
        """Test getting UnsupportedField error example."""
        result = await get_error_examples(error_type="UnsupportedField")
        
        assert isinstance(result, ErrorsResponse)
        assert len(result.errors) == 1
        assert result.errors[0].kind == ErrorKind.UNSUPPORTED_FIELD
        assert result.errors[0].field == "wrong field"

    async def test_get_error_examples_default(self):
        """Test getting error examples with default parameter."""
        # Default parameter is "all" from Query()
        result = await get_error_examples(error_type="all")
        
        assert isinstance(result, ErrorsResponse)
        # Default is "all"
        assert len(result.errors) == 5

