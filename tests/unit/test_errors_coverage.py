"""
Additional unit tests for errors.py endpoint to improve coverage.

Tests missing logger.debug call and edge cases.
"""

import pytest
from unittest.mock import patch, MagicMock
from app.api.v1.endpoints.errors import get_error_examples
from app.models.errors import ErrorKind, ErrorsResponse


@pytest.mark.unit
class TestErrorEndpointsCoverage:
    """Test cases for error endpoints to improve coverage."""

    @patch('app.api.v1.endpoints.errors.logger')
    async def test_get_error_examples_logs_debug(self, mock_logger):
        """Test that get_error_examples logs debug message."""
        result = await get_error_examples(error_type="InvalidRoute")
        
        # Verify logger.debug was called
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args
        
        # Verify the log message contains expected fields
        assert "error_type" in call_args.kwargs
        assert "count" in call_args.kwargs
        assert call_args.kwargs["error_type"] == "InvalidRoute"
        assert call_args.kwargs["count"] == 1

    @patch('app.api.v1.endpoints.errors.logger')
    async def test_get_error_examples_logs_debug_all(self, mock_logger):
        """Test that get_error_examples logs debug message for all errors."""
        result = await get_error_examples(error_type="all")
        
        # Verify logger.debug was called
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args
        
        # Verify the log message contains expected fields
        assert call_args.kwargs["error_type"] == "all"
        assert call_args.kwargs["count"] == 5
