"""
Field mapping utilities for converting database values to API values and vice versa.

This module provides centralized mapping functions for sample field values,
loaded from config_data/field_mappings.json. This allows easy updates to
mapping rules without code changes.
"""

import json
from pathlib import Path
from typing import Dict, Optional, List, Any


# Cache for loaded mappings
_field_mappings_cache: Optional[Dict[str, Any]] = None


def _load_field_mappings() -> Dict[str, Any]:
    """
    Load field mappings from config_data/field_mappings.json.
    
    Returns:
        Dictionary of field mappings organized by node type. Returns empty dict if file not found or invalid.
        Structure: { "node_type": { "field_name": { ... } } }
    """
    global _field_mappings_cache
    
    if _field_mappings_cache is not None:
        return _field_mappings_cache
    
    field_mappings_path = Path(__file__).resolve().parents[1] / "config_data" / "field_mappings.json"
    
    try:
        with field_mappings_path.open("r", encoding="utf-8") as f:
            _field_mappings_cache = json.load(f)
            return _field_mappings_cache
    except (FileNotFoundError, json.JSONDecodeError) as e:
        # Return empty dict if file not found or invalid
        return {}


def _find_field_config(field_name: str) -> Optional[tuple[str, Dict[str, Any]]]:
    """
    Find the field configuration and its node type.
    
    Args:
        field_name: Name of the field (e.g., "library_selection_method")
        
    Returns:
        Tuple of (node_type, field_config) if found, None otherwise
    """
    mappings = _load_field_mappings()
    
    # Search through all nodes to find the field
    for node_type, node_fields in mappings.items():
        if isinstance(node_fields, dict) and field_name in node_fields:
            return (node_type, node_fields[field_name])
    
    return None


def map_field_value(field_name: str, db_value: Any) -> Optional[str]:
    """
    Map a database value to an API value for a given field.
    
    Args:
        field_name: Name of the field (e.g., "library_selection_method")
        db_value: Database value to map
        
    Returns:
        Mapped API value, or None if value should be null, or original value if no mapping
    """
    if db_value is None:
        return None
    
    str_value = str(db_value).strip()
    if not str_value:
        return None
    
    # Find field configuration
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return str_value
    
    _, field_config = field_config_result
    
    # Check null_mappings first (values that should become null)
    null_mappings = field_config.get("null_mappings", [])
    if str_value in null_mappings:
        return None
    
    # Check regular mappings
    value_mappings = field_config.get("mappings", {})
    if str_value in value_mappings:
        return value_mappings[str_value]
    
    # No mapping found, return as-is
    return str_value


def reverse_map_field_value(field_name: str, api_value: Any) -> Optional[str | List[str]]:
    """
    Reverse map an API value to database value(s) for a given field.
    
    Used for filtering - maps API values back to DB values.
    
    Args:
        field_name: Name of the field (e.g., "library_selection_method")
        api_value: API value to reverse map
        
    Returns:
        Database value(s) to use in filter, or None if no mapping
        Can return a list if multiple DB values map to the same API value
    """
    if api_value is None:
        return None
    
    str_value = str(api_value).strip()
    if not str_value:
        return None
    
    # Find field configuration
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return str_value
    
    _, field_config = field_config_result
    reverse_mappings = field_config.get("reverse_mappings", {})
    
    if str_value in reverse_mappings:
        mapped_value = reverse_mappings[str_value]
        # If it's a list, return as-is (for cases like disease_phase where multiple DB values map to one API value)
        if isinstance(mapped_value, list):
            return mapped_value
        return mapped_value
    
    # No reverse mapping found, return as-is
    return str_value


def is_null_mapped_value(field_name: str, value: Any) -> bool:
    """
    Check if a value is in the null_mappings for a given field.
    
    Values in null_mappings are treated as NULL/missing and should not
    be valid filter values.
    
    Args:
        field_name: Name of the field (e.g., "library_source_material")
        value: Value to check
        
    Returns:
        True if the value is in null_mappings, False otherwise
    """
    if value is None:
        return False
    
    str_value = str(value).strip()
    if not str_value:
        return False
    
    # Find field configuration
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return False
    
    _, field_config = field_config_result
    null_mappings = field_config.get("null_mappings", [])
    
    return str_value in null_mappings


def is_database_only_value(field_name: str, value: Any) -> bool:
    """
    Check if a value is a database-only value (not a valid API value).
    
    Database-only values are those that appear in the forward mappings
    (database -> API) but NOT in reverse_mappings (API -> database).
    These should not be accepted as filter values.
    
    Args:
        field_name: Name of the field (e.g., "disease_phase")
        value: Value to check
        
    Returns:
        True if the value is a database-only value, False otherwise
    """
    if value is None:
        return False
    
    str_value = str(value).strip()
    if not str_value:
        return False
    
    # Find field configuration
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return False
    
    _, field_config = field_config_result
    mappings = field_config.get("mappings", {})
    reverse_mappings = field_config.get("reverse_mappings", {})
    
    # If the value is in the forward mappings (as a database value that gets mapped to API value)
    # but NOT in reverse_mappings (as a valid API value), it's a database-only value
    if str_value in mappings and str_value not in reverse_mappings:
        return True
    
    return False


def get_field_mapping_info(field_name: str) -> Optional[Dict[str, Any]]:
    """
    Get mapping configuration for a specific field.
    
    Args:
        field_name: Name of the field
        
    Returns:
        Dictionary with mapping configuration including node type, or None if field not found
    """
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return None
    
    node_type, field_config = field_config_result
    # Include node type in the returned config
    result = field_config.copy()
    result["source_node"] = node_type
    return result


def reload_mappings():
    """
    Reload field mappings from the JSON file.
    
    Useful for testing or when the config file is updated at runtime.
    """
    global _field_mappings_cache
    _field_mappings_cache = None
    _load_field_mappings()

