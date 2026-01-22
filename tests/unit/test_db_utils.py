"""
Unit tests for database utility functions.

Tests database connection error detection and retry logic.
"""

import pytest
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.db.memgraph import is_retryable_error, DatabaseConnectionError


@pytest.mark.unit
class TestDatabaseUtils:
    """Test cases for database utility functions."""

    def test_is_retryable_error_service_unavailable(self):
        """Test is_retryable_error detects ServiceUnavailable."""
        error = ServiceUnavailable("Service unavailable")
        assert is_retryable_error(error) is True

    def test_is_retryable_error_transient_error(self):
        """Test is_retryable_error detects TransientError."""
        error = TransientError("Transient error")
        assert is_retryable_error(error) is True

    def test_is_retryable_error_session_expired(self):
        """Test is_retryable_error detects SessionExpired."""
        error = SessionExpired("Session expired")
        assert is_retryable_error(error) is True

    def test_is_retryable_error_auth_error(self):
        """Test is_retryable_error does not retry AuthError."""
        error = AuthError("Authentication failed")
        assert is_retryable_error(error) is False

    def test_is_retryable_error_connection_keywords(self):
        """Test is_retryable_error detects connection-related keywords."""
        error = Exception("Connection timeout occurred")
        assert is_retryable_error(error) is True
        
        error = Exception("Network error")
        assert is_retryable_error(error) is True
        
        error = Exception("Broken pipe")
        assert is_retryable_error(error) is True
        
        error = Exception("Connection reset")
        assert is_retryable_error(error) is True

    def test_is_retryable_error_non_retryable(self):
        """Test is_retryable_error returns False for non-retryable errors."""
        error = ValueError("Invalid value")
        assert is_retryable_error(error) is False
        
        error = KeyError("Missing key")
        assert is_retryable_error(error) is False

    def test_is_retryable_error_error_type_name(self):
        """Test is_retryable_error checks error type name."""
        class ServiceUnavailableError(Exception):
            pass
        
        error = ServiceUnavailableError("Service unavailable")
        assert is_retryable_error(error) is True

    def test_database_connection_error(self):
        """Test DatabaseConnectionError can be raised."""
        error = DatabaseConnectionError("Connection failed")
        assert str(error) == "Connection failed"
        assert isinstance(error, Exception)

