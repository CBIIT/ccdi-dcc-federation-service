"""
Global serialization utilities for handling date/time objects and other non-serializable types.

This module provides utilities to convert date/time objects (like ZONED_DATE_TIME, LocalDateTime)
from Memgraph/Neo4j into JSON-serializable strings, preventing serialization errors.
"""

from typing import Any


def convert_date_time_to_string(value: Any) -> Any:
    """
    Convert date/time objects to ISO format strings.
    
    Handles various date/time types from Memgraph/Neo4j:
    - ZONED_DATE_TIME
    - LocalDateTime
    - Date
    - Time
    - Any object with isoformat() method (datetime-like)
    - Any object with 'time' in its type name
    
    Args:
        value: Any value that might be a date/time object
        
    Returns:
        ISO format string if value is a date/time object, otherwise returns value unchanged
    """
    if value is None:
        return None
    
    # Check for datetime-like objects with isoformat() method
    if hasattr(value, 'isoformat'):
        try:
            return value.isoformat()
        except Exception:
            # If isoformat() fails, fall back to str()
            return str(value)
    
    # Check for other time-related objects by type name
    if hasattr(value, '__class__'):
        type_name = str(type(value)).lower()
        if 'time' in type_name or 'date' in type_name:
            # Try to convert to string
            try:
                if hasattr(value, '__str__'):
                    return str(value)
            except Exception:
                pass
    
    # Not a date/time object, return as-is
    return value


def sanitize_for_json(obj: Any) -> Any:
    """
    Recursively sanitize an object for JSON serialization.
    
    Converts date/time objects to strings and handles nested structures
    (dicts, lists, tuples).
    
    Args:
        obj: Object to sanitize (can be dict, list, tuple, or any value)
        
    Returns:
        JSON-serializable version of the object
    """
    if obj is None:
        return None
    
    # Handle date/time objects first
    converted = convert_date_time_to_string(obj)
    if converted is not obj:
        return converted
    
    # Handle dictionaries
    if isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}
    
    # Handle lists and tuples
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(item) for item in obj]
    
    # For other types, try date/time conversion one more time
    # (in case it's a custom object that wasn't caught earlier)
    return convert_date_time_to_string(obj)
