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
    @patch("app.core.logging.logging.FileHandler")
    @patch("app.core.logging.structlog.configure")
    @patch("app.core.logging.structlog.get_logger")
    @patch("pathlib.Path")
    @patch("app.core.logging.structlog.stdlib.ProcessorFormatter")
    def test_configure_logging_json(self, mock_processor_formatter, mock_path, mock_get_logger, mock_configure, mock_file_handler, mock_basic_config, mock_get_settings):
        """Test configure_logging with JSON format."""
        mock_settings = Mock()
        mock_settings.log_level = "INFO"
        mock_settings.log_format = "json"
        mock_settings.debug = False
        mock_get_settings.return_value = mock_settings
        
        # Mock Path.exists() to return False (no logs directory)
        mock_path_instance = Mock()
        mock_path_instance.exists.return_value = False
        mock_path_instance.mkdir = Mock()
        mock_path_instance.__truediv__ = Mock(return_value=Mock(__str__=Mock(return_value="logs/app.log")))
        mock_path.return_value = mock_path_instance

        configure_logging()

        # Check that basicConfig was called with handlers (actual implementation uses handlers, not format/stream)
        mock_basic_config.assert_called_once()
        # Get call arguments - call_args is a tuple (args, kwargs)
        call_args, call_kwargs = mock_basic_config.call_args
        assert "handlers" in call_kwargs
        assert call_kwargs["level"] == logging.INFO
        assert call_kwargs.get("force") is True
        
        # Check that ProcessorFormatter was called with JSONRenderer
        mock_processor_formatter.assert_called_once()
        formatter_call_args = mock_processor_formatter.call_args
        # The processor argument should be JSONRenderer for JSON format
        processor_arg = formatter_call_args[1].get("processor") if formatter_call_args[1] else None
        assert processor_arg is not None
        assert isinstance(processor_arg, structlog.processors.JSONRenderer) or processor_arg == structlog.processors.JSONRenderer

    @patch("app.core.logging.get_settings")
    @patch("app.core.logging.logging.basicConfig")
    @patch("app.core.logging.logging.FileHandler")
    @patch("app.core.logging.structlog.configure")
    @patch("app.core.logging.structlog.get_logger")
    @patch("pathlib.Path")
    @patch("app.core.logging.structlog.stdlib.ProcessorFormatter")
    def test_configure_logging_console(self, mock_processor_formatter, mock_path, mock_get_logger, mock_configure, mock_file_handler, mock_basic_config, mock_get_settings):
        """Test configure_logging with console format."""
        mock_settings = Mock()
        mock_settings.log_level = "DEBUG"
        mock_settings.log_format = "console"
        mock_settings.debug = False
        mock_get_settings.return_value = mock_settings
        
        # Mock Path.exists() to return False (no logs directory)
        mock_path_instance = Mock()
        mock_path_instance.exists.return_value = False
        mock_path_instance.mkdir = Mock()
        mock_path_instance.__truediv__ = Mock(return_value=Mock(__str__=Mock(return_value="logs/app.log")))
        mock_path.return_value = mock_path_instance

        configure_logging()

        # Check that basicConfig was called
        mock_basic_config.assert_called_once()
        call_args, call_kwargs = mock_basic_config.call_args
        assert "handlers" in call_kwargs
        assert call_kwargs["level"] == logging.DEBUG
        assert call_kwargs.get("force") is True
        
        # Check that ProcessorFormatter was called with ConsoleRenderer (not JSONRenderer)
        mock_processor_formatter.assert_called_once()
        formatter_call_args = mock_processor_formatter.call_args
        # The processor argument should be ConsoleRenderer for console format
        processor_arg = formatter_call_args[1].get("processor") if formatter_call_args[1] else None
        assert processor_arg is not None
        assert isinstance(processor_arg, structlog.dev.ConsoleRenderer) or processor_arg == structlog.dev.ConsoleRenderer
        assert not (isinstance(processor_arg, structlog.processors.JSONRenderer) or processor_arg == structlog.processors.JSONRenderer)

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

