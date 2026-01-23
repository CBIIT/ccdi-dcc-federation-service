from unittest.mock import Mock, patch

import logging
import structlog

from app.core.logging import add_request_context, configure_logging, get_logger


def test_configure_logging_json_format():
    settings = Mock()
    settings.log_level = "info"
    settings.log_format = "json"
    mock_logger = Mock()
    wrapper_class = object()

    with patch("app.core.logging.get_settings", return_value=settings), \
        patch("app.core.logging.logging.basicConfig") as basic_config, \
        patch("app.core.logging.structlog.configure") as configure, \
        patch("app.core.logging.structlog.make_filtering_bound_logger", return_value=wrapper_class), \
        patch("app.core.logging.structlog.get_logger", return_value=mock_logger):
        result = configure_logging()

    assert result is mock_logger
    basic_config.assert_called_once()
    assert basic_config.call_args.kwargs["level"] == logging.INFO

    processors = configure.call_args.kwargs["processors"]
    assert any(isinstance(p, structlog.processors.JSONRenderer) for p in processors)
    assert not any(isinstance(p, structlog.dev.ConsoleRenderer) for p in processors)
    assert configure.call_args.kwargs["wrapper_class"] is wrapper_class


def test_configure_logging_console_format():
    settings = Mock()
    settings.log_level = "debug"
    settings.log_format = "console"
    mock_logger = Mock()
    wrapper_class = object()

    with patch("app.core.logging.get_settings", return_value=settings), \
        patch("app.core.logging.logging.basicConfig") as basic_config, \
        patch("app.core.logging.structlog.configure") as configure, \
        patch("app.core.logging.structlog.make_filtering_bound_logger", return_value=wrapper_class), \
        patch("app.core.logging.structlog.get_logger", return_value=mock_logger):
        result = configure_logging()

    assert result is mock_logger
    basic_config.assert_called_once()
    assert basic_config.call_args.kwargs["level"] == logging.DEBUG

    processors = configure.call_args.kwargs["processors"]
    assert any(isinstance(p, structlog.dev.ConsoleRenderer) for p in processors)
    assert not any(isinstance(p, structlog.processors.JSONRenderer) for p in processors)
    assert configure.call_args.kwargs["wrapper_class"] is wrapper_class


def test_get_logger_with_name():
    logger = Mock()
    with patch("app.core.logging.structlog.get_logger", return_value=logger) as get_logger_mock:
        result = get_logger("api")

    assert result is logger
    get_logger_mock.assert_called_once_with("api")


def test_get_logger_default():
    logger = Mock()
    with patch("app.core.logging.structlog.get_logger", return_value=logger) as get_logger_mock:
        result = get_logger()

    assert result is logger
    get_logger_mock.assert_called_once_with()


def test_add_request_context():
    logger = Mock()
    logger.bind.return_value = "bound"

    result = add_request_context(logger, request_id="abc123")

    assert result == "bound"
    logger.bind.assert_called_once_with(request_id="abc123")
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

