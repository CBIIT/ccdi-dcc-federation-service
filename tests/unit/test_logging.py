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
        
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        configure_logging()

        # Check that basicConfig was called with format and stream (server-side version)
        mock_basic_config.assert_called_once()
        call_args, call_kwargs = mock_basic_config.call_args
        assert "format" in call_kwargs
        assert call_kwargs["format"] == "%(message)s"
        assert "stream" in call_kwargs
        assert call_kwargs["stream"] == sys.stdout
        assert call_kwargs["level"] == logging.INFO
        
        # Check that structlog.configure was called
        mock_configure.assert_called_once()
        configure_call_args = mock_configure.call_args
        # Check that JSONRenderer is in the processors (it's instantiated in the code)
        processors = configure_call_args[1].get("processors", [])
        # Find the renderer processor (should be JSONRenderer instance)
        renderer = None
        for p in processors:
            if isinstance(p, structlog.processors.JSONRenderer):
                renderer = p
                break
            # Also check by class name in case of mocking
            if hasattr(p, '__class__') and 'JSONRenderer' in p.__class__.__name__:
                renderer = p
                break
        assert renderer is not None, f"JSONRenderer not found in processors: {[type(p).__name__ for p in processors]}"
        
        # Check that PrintLoggerFactory is used (server-side version, instantiated)
        logger_factory = configure_call_args[1].get("logger_factory")
        assert logger_factory is not None
        # PrintLoggerFactory() creates an instance, so check the type
        assert isinstance(logger_factory, structlog.PrintLoggerFactory) or logger_factory == structlog.PrintLoggerFactory

    @patch("app.core.logging.get_settings")
    @patch("app.core.logging.logging.basicConfig")
    @patch("app.core.logging.structlog.configure")
    @patch("app.core.logging.structlog.get_logger")
    def test_configure_logging_console(self, mock_get_logger, mock_configure, mock_basic_config, mock_get_settings):
        """Test configure_logging with console format."""
        mock_settings = Mock()
        mock_settings.log_level = "DEBUG"
        mock_settings.log_format = "console"
        mock_get_settings.return_value = mock_settings
        
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        configure_logging()

        # Check that basicConfig was called with format and stream (server-side version)
        mock_basic_config.assert_called_once()
        call_args, call_kwargs = mock_basic_config.call_args
        assert "format" in call_kwargs
        assert call_kwargs["format"] == "%(message)s"
        assert "stream" in call_kwargs
        assert call_kwargs["stream"] == sys.stdout
        assert call_kwargs["level"] == logging.DEBUG
        
        # Check that structlog.configure was called
        mock_configure.assert_called_once()
        configure_call_args = mock_configure.call_args
        # Check that ConsoleRenderer is in the processors (not JSONRenderer)
        processors = configure_call_args[1].get("processors", [])
        # Find the renderer processor (should be ConsoleRenderer instance)
        renderer = None
        for p in processors:
            if isinstance(p, structlog.dev.ConsoleRenderer):
                renderer = p
                break
            # Also check by class name in case of mocking
            if hasattr(p, '__class__') and 'ConsoleRenderer' in p.__class__.__name__:
                renderer = p
                break
        assert renderer is not None, f"ConsoleRenderer not found in processors: {[type(p).__name__ for p in processors]}"
        
        # Verify JSONRenderer is NOT in processors
        json_renderer_found = any(
            isinstance(p, structlog.processors.JSONRenderer)
            or (hasattr(p, '__class__') and 'JSONRenderer' in p.__class__.__name__)
            for p in processors
        )
        assert not json_renderer_found, f"JSONRenderer should not be in processors: {[type(p).__name__ for p in processors]}"
        
        # Check that PrintLoggerFactory is used (server-side version, instantiated)
        logger_factory = configure_call_args[1].get("logger_factory")
        assert logger_factory is not None
        # PrintLoggerFactory() creates an instance, so check the type
        assert isinstance(logger_factory, structlog.PrintLoggerFactory) or logger_factory == structlog.PrintLoggerFactory

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

