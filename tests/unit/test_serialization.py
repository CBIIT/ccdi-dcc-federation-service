"""
Unit tests for serialization utilities.

Tests the global date/time conversion functions that handle ZONED_DATE_TIME
and other date/time objects from Memgraph/Neo4j.
"""

import pytest
from datetime import datetime, date, time
from typing import Any
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.core.serialization import convert_date_time_to_string, sanitize_for_json


@pytest.mark.unit
class TestConvertDateTimeToString:
    """Test convert_date_time_to_string function."""
    
    def test_converts_datetime_object(self):
        """Test conversion of datetime object to ISO format string."""
        dt = datetime(2025, 12, 22, 21, 4, 27, 798862)
        result = convert_date_time_to_string(dt)
        assert isinstance(result, str)
        assert result == dt.isoformat()
    
    def test_converts_date_object(self):
        """Test conversion of date object to ISO format string."""
        d = date(2025, 12, 22)
        result = convert_date_time_to_string(d)
        assert isinstance(result, str)
        assert result == d.isoformat()
    
    def test_converts_time_object(self):
        """Test conversion of time object to ISO format string."""
        t = time(21, 4, 27)
        result = convert_date_time_to_string(t)
        assert isinstance(result, str)
        assert result == t.isoformat()
    
    def test_converts_mock_zoned_date_time(self):
        """Test conversion of mock ZONED_DATE_TIME object."""
        class MockZonedDateTime:
            """Mock ZONED_DATE_TIME from Memgraph/Neo4j."""
            def __init__(self):
                self.value = "2025-12-22T21:04:27.798862+00:00[Etc/UTC]"
            
            def isoformat(self):
                return self.value
        
        zoned_dt = MockZonedDateTime()
        result = convert_date_time_to_string(zoned_dt)
        assert isinstance(result, str)
        assert result == "2025-12-22T21:04:27.798862+00:00[Etc/UTC]"
    
    def test_converts_object_with_time_in_type_name(self):
        """Test conversion of object with 'time' in type name."""
        class MockTimeObject:
            """Mock object with 'time' in class name."""
            def __init__(self):
                self.value = "2025-12-22T21:04:27"
            
            def __str__(self):
                return self.value
        
        time_obj = MockTimeObject()
        result = convert_date_time_to_string(time_obj)
        assert isinstance(result, str)
        assert result == "2025-12-22T21:04:27"
    
    def test_preserves_none(self):
        """Test that None is preserved."""
        result = convert_date_time_to_string(None)
        assert result is None
    
    def test_preserves_string(self):
        """Test that strings are preserved unchanged."""
        value = "2025-12-22T21:04:27"
        result = convert_date_time_to_string(value)
        assert result == value
        assert isinstance(result, str)
    
    def test_preserves_integer(self):
        """Test that integers are preserved unchanged."""
        value = 42
        result = convert_date_time_to_string(value)
        assert result == value
        assert isinstance(result, int)
    
    def test_preserves_boolean(self):
        """Test that booleans are preserved unchanged."""
        value = True
        result = convert_date_time_to_string(value)
        assert result == value
        assert isinstance(result, bool)
    
    def test_handles_isoformat_exception(self):
        """Test handling when isoformat() raises an exception."""
        class BadDateTime:
            def isoformat(self):
                raise ValueError("Cannot format")
        
        bad_dt = BadDateTime()
        result = convert_date_time_to_string(bad_dt)
        assert isinstance(result, str)  # Should fall back to str()


@pytest.mark.unit
class TestSanitizeForJson:
    """Test sanitize_for_json function."""
    
    def test_sanitizes_datetime_in_dict(self):
        """Test sanitization of datetime objects in dictionary."""
        dt = datetime(2025, 12, 22, 21, 4, 27, 798862)
        obj = {
            "created": dt,
            "name": "Test",
            "age": 25
        }
        result = sanitize_for_json(obj)
        assert isinstance(result["created"], str)
        assert result["created"] == dt.isoformat()
        assert result["name"] == "Test"
        assert result["age"] == 25
    
    def test_sanitizes_datetime_in_list(self):
        """Test sanitization of datetime objects in list."""
        dt1 = datetime(2025, 12, 22, 21, 4, 27)
        dt2 = datetime(2025, 12, 23, 10, 0, 0)
        obj = [dt1, dt2, "string", 42]
        result = sanitize_for_json(obj)
        assert isinstance(result, list)
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)
        assert result[0] == dt1.isoformat()
        assert result[1] == dt2.isoformat()
        assert result[2] == "string"
        assert result[3] == 42
    
    def test_sanitizes_nested_dict(self):
        """Test sanitization of nested dictionaries."""
        dt = datetime(2025, 12, 22, 21, 4, 27)
        obj = {
            "level1": {
                "level2": {
                    "created": dt,
                    "name": "Nested"
                }
            }
        }
        result = sanitize_for_json(obj)
        assert isinstance(result["level1"]["level2"]["created"], str)
        assert result["level1"]["level2"]["created"] == dt.isoformat()
        assert result["level1"]["level2"]["name"] == "Nested"
    
    def test_sanitizes_datetime_in_list_in_dict(self):
        """Test sanitization of datetime objects in list within dict."""
        dt1 = datetime(2025, 12, 22, 21, 4, 27)
        dt2 = datetime(2025, 12, 23, 10, 0, 0)
        obj = {
            "dates": [dt1, dt2],
            "name": "Test"
        }
        result = sanitize_for_json(obj)
        assert isinstance(result["dates"], list)
        assert isinstance(result["dates"][0], str)
        assert isinstance(result["dates"][1], str)
        assert result["dates"][0] == dt1.isoformat()
        assert result["dates"][1] == dt2.isoformat()
    
    def test_sanitizes_tuple(self):
        """Test sanitization of tuples (converted to list)."""
        dt = datetime(2025, 12, 22, 21, 4, 27)
        obj = (dt, "string", 42)
        result = sanitize_for_json(obj)
        assert isinstance(result, list)  # Tuples become lists
        assert isinstance(result[0], str)
        assert result[0] == dt.isoformat()
    
    def test_preserves_none(self):
        """Test that None is preserved."""
        result = sanitize_for_json(None)
        assert result is None
    
    def test_preserves_simple_dict(self):
        """Test that simple dicts without dates are preserved."""
        obj = {"name": "Test", "age": 25, "active": True}
        result = sanitize_for_json(obj)
        assert result == obj
    
    def test_preserves_empty_dict(self):
        """Test that empty dicts are preserved."""
        obj = {}
        result = sanitize_for_json(obj)
        assert result == {}
    
    def test_preserves_empty_list(self):
        """Test that empty lists are preserved."""
        obj = []
        result = sanitize_for_json(obj)
        assert result == []
    
    def test_handles_complex_nested_structure(self):
        """Test sanitization of complex nested structure."""
        dt1 = datetime(2025, 12, 22, 21, 4, 27)
        dt2 = datetime(2025, 12, 23, 10, 0, 0)
        obj = {
            "diagnosis": {
                "created": dt1,
                "updated": dt2,
                "tags": ["tag1", "tag2"],
                "metadata": {
                    "nested_date": dt1
                }
            },
            "samples": [
                {"created": dt1},
                {"created": dt2}
            ]
        }
        result = sanitize_for_json(obj)
        assert isinstance(result["diagnosis"]["created"], str)
        assert isinstance(result["diagnosis"]["updated"], str)
        assert isinstance(result["diagnosis"]["metadata"]["nested_date"], str)
        assert isinstance(result["samples"][0]["created"], str)
        assert isinstance(result["samples"][1]["created"], str)
        assert result["diagnosis"]["tags"] == ["tag1", "tag2"]
