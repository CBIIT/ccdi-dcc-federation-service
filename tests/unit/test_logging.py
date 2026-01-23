"""
Unit tests for logging configuration utilities.
"""

import logging
import sys
from unittest.mock import Mock, patch

import pytest
import structlog

from app.core.logging import configure_logging, get_logger, add_request_context


@pytest.mark.unit
class TestLoggingConfiguration:
    """Test cases for logging configuration."""

    @patch("app.core.logging.get_settings")
    @patch("app.core.logging.logging.basicConfig")
    @patch("app.core.logging.structlog.configure")
    @patch("app.core.logging.structlog.get_logger")
    def test_configure_logging_json(self, mock_get_logger, mock_configure, mock_basic_config, mock_get_settings):
        """Test configure_logging with JSON format."""
        mock_settings = Mock()
        mock_settings.log_level = "INFO"
        mock_settings.log_format = "json"
        mock_get_settings.return_value = mock_settings

        configure_logging()

        mock_basic_config.assert_called_once_with(
            format="%(message)s",
            stream=sys.stdout,
            level=logging.INFO,
        )
        processors = mock_configure.call_args[1]["processors"]
        assert any(isinstance(p, structlog.processors.JSONRenderer) for p in processors)

    @patch("app.core.logging.get_settings")
    @patch("app.core.logging.structlog.configure")
    def test_configure_logging_console(self, mock_configure, mock_get_settings):
        """Test configure_logging with console format."""
        mock_settings = Mock()
        mock_settings.log_level = "DEBUG"
        mock_settings.log_format = "console"
        mock_get_settings.return_value = mock_settings

        configure_logging()

        processors = mock_configure.call_args[1]["processors"]
        assert any(isinstance(p, structlog.dev.ConsoleRenderer) for p in processors)
        assert not any(isinstance(p, structlog.processors.JSONRenderer) for p in processors)

    @patch("app.core.logging.structlog.get_logger")
    def test_get_logger_with_name(self, mock_get_logger):
        """Test get_logger with a name."""
        get_logger("test")
        mock_get_logger.assert_called_once_with("test")

    @patch("app.core.logging.structlog.get_logger")
    def test_get_logger_default(self, mock_get_logger):
        """Test get_logger without a name."""
        get_logger()
        mock_get_logger.assert_called_once_with()

    def test_add_request_context(self):
        """Test add_request_context binds extra context."""
        logger = Mock()
        logger.bind = Mock(return_value="bound_logger")
        result = add_request_context(logger, request_id="abc", user="test")
        logger.bind.assert_called_once_with(request_id="abc", user="test")
        assert result == "bound_logger"

