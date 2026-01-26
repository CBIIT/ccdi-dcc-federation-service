"""
Unit tests for config edge cases.

Tests missing lines in config.py for better coverage.
"""

import pytest
from unittest.mock import patch, mock_open
from pathlib import Path
import json


@pytest.mark.unit
class TestConfigEdgeCases:
    """Test edge cases in config module."""

    def test_load_info_json_returns_empty_on_file_not_found(self):
        """Test load_info_json returns empty dict on FileNotFoundError."""
        from app.core.config import load_info_json
        
        # Test FileNotFoundError (line 25)
        with patch('pathlib.Path.open', side_effect=FileNotFoundError):
            result = load_info_json()
            assert result == {}

    def test_load_info_json_returns_empty_on_json_decode_error(self):
        """Test load_info_json returns empty dict on JSONDecodeError."""
        from app.core.config import load_info_json
        
        # Test JSONDecodeError (line 25)
        with patch('pathlib.Path.open', mock_open(read_data='invalid json')):
            with patch('json.load', side_effect=json.JSONDecodeError("", "", 0)):
                result = load_info_json()
                assert result == {}

    def test_get_settings_from_env_file_oserror(self):
        """Test get_settings handles OSError."""
        from app.core.config import get_settings
        
        # Clear cache first
        get_settings.cache_clear()
        
        # Test OSError (line 339) - when Path.is_file() raises OSError
        with patch('pathlib.Path.is_file', side_effect=OSError("Permission denied")):
            result = get_settings()
            # Should return Settings (with _env_file=None internally on error)
            assert result is not None
        
        # Clear cache after test
        get_settings.cache_clear()

