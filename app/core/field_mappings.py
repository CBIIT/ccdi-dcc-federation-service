"""
Field mapping utilities for converting between database and API values.

This module provides functions to map field values between database and API representations,
and to check if values should be treated as null or invalid.
"""

from typing import Any, Dict, List, Optional
from pathlib import Path
import json


def _load_field_mappings() -> Dict[str, Any]:
    """Load field mappings from JSON configuration file."""
    import json
    from pathlib import Path
    
    config_path = Path(__file__).parent.parent / "config_data" / "field_mappings.json"
    with open(config_path, 'r') as f:
        return json.load(f)


# Cache the field mappings
_field_mappings_cache: Optional[Dict[str, Any]] = None


def _get_field_mappings() -> Dict[str, Any]:
    """Get field mappings, loading from cache if available."""
    global _field_mappings_cache
    if _field_mappings_cache is None:
        _field_mappings_cache = _load_field_mappings()
    return _field_mappings_cache


def _find_field_config(field_name: str) -> Optional[tuple[str, Dict[str, Any]]]:
    """
    Find the field configuration for a given field name.
    
    Args:
        field_name: Name of the field (e.g., "library_selection_method")
        
    Returns:
        Tuple of (node_type, field_config) if found, None otherwise
    """
    field_mappings = _get_field_mappings()
    
    # Search through all node types
    for node_type, node_fields in field_mappings.items():
        if field_name in node_fields:
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
    Map an API value to database value(s) for a given field.
    
    Args:
        field_name: Name of the field (e.g., "library_selection_method")
        api_value: API value to reverse map
        
    Returns:
        Database value(s), or None if no mapping found
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
    
    # Check reverse mappings
    reverse_mappings = field_config.get("reverse_mappings", {})
    if str_value in reverse_mappings:
        mapped_value = reverse_mappings[str_value]
        # Reverse mappings can be a single value or a list
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


def get_null_mappings(field_name: str) -> List[str]:
    """
    Get the list of null_mappings for a given field.
    
    Values in null_mappings should be filtered out as invalid.
    
    Args:
        field_name: Name of the field (e.g., "library_strategy")
        
    Returns:
        List of values that should be treated as null/invalid
    """
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return []
    
    _, field_config = field_config_result
    return field_config.get("null_mappings", [])


def build_invalid_value_filter(node_field: str, field_name: str) -> str:
    """
    Build WHERE clause conditions to filter out invalid values for a field.
    
    This function checks the null_mappings configuration for the field and
    only filters values that are actually marked as invalid, rather than
    hardcoding a universal list.
    
    Args:
        node_field: The field reference in Cypher (e.g., 'sf.library_strategy')
        field_name: The field name for config lookup (e.g., 'library_strategy')
        
    Returns:
        String with WHERE conditions for invalid values
    """
    conditions = [
        f"{node_field} IS NOT NULL",
        f"{node_field} <> ''",
        f"{node_field} <> '-999'"
    ]
    
    # Get null_mappings for this specific field
    null_mappings = get_null_mappings(field_name)
    
    # Only add 'Not Reported' filter if it's in null_mappings
    if "Not Reported" in null_mappings:
        conditions.append(f"{node_field} <> 'Not Reported'")
    
    # Add any other null_mappings values
    for invalid_val in null_mappings:
        if invalid_val not in ['', '-999', 'Not Reported']:
            conditions.append(f"{node_field} <> '{invalid_val}'")
    
    return " AND ".join(conditions)


def build_invalid_value_list_filter(field_name: str) -> str:
    """
    Build list filter conditions for filtering invalid values in Cypher list comprehensions.
    
    This is used in queries like:
    [val IN field_values WHERE val <> '' AND val <> '-999' AND val <> 'Not Reported']
    
    Args:
        field_name: The field name for config lookup (e.g., 'library_strategy')
        
    Returns:
        String with list filter conditions (without the WHERE keyword)
    """
    conditions = [
        "val <> ''",
        "val <> '-999'"
    ]
    
    # Get null_mappings for this specific field
    null_mappings = get_null_mappings(field_name)
    
    # Only add 'Not Reported' filter if it's in null_mappings
    if "Not Reported" in null_mappings:
        conditions.append("val <> 'Not Reported'")
    
    # Add any other null_mappings values
    for invalid_val in null_mappings:
        if invalid_val not in ['', '-999', 'Not Reported']:
            conditions.append(f"val <> '{invalid_val}'")
    
    return " AND ".join(conditions)


def build_case_mapping_statement(field_name: str, variable_name: str = "value") -> str:
    """
    Build a Cypher CASE statement for mapping database values to API values.
    
    This is used in queries where we need to map DB values to API values directly in Cypher,
    such as for specimen_molecular_analyte_type where 'Transcriptomic' maps to 'RNA'.
    
    Args:
        field_name: The field name for config lookup (e.g., 'specimen_molecular_analyte_type')
        variable_name: The Cypher variable name to use in the CASE statement (default: 'value')
        
    Returns:
        Cypher CASE statement string, or empty string if no mappings exist
    """
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return ""
    
    _, field_config = field_config_result
    mappings = field_config.get("mappings", {})
    
    if not mappings:
        return ""
    
    # Build CASE statement from mappings
    case_parts = []
    for db_value, api_value in mappings.items():
        # Escape single quotes in values
        db_value_escaped = db_value.replace("'", "\\'")
        api_value_escaped = api_value.replace("'", "\\'")
        case_parts.append(f"WHEN {variable_name} = '{db_value_escaped}' THEN '{api_value_escaped}'")
    
    # If no mappings, return empty string
    if not case_parts:
        return ""
    
    # Add ELSE clause to return original value if no mapping found
    case_parts.append(f"ELSE {variable_name}")
    
    return f"CASE {' '.join(case_parts)} END"


def get_mapped_db_values(field_name: str) -> List[str]:
    """
    Get list of database values that have mappings for a field.
    
    This is useful for building IN clauses that filter to only mapped values.
    
    Args:
        field_name: The field name for config lookup (e.g., 'specimen_molecular_analyte_type')
        
    Returns:
        List of database values that have mappings
    """
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return []
    
    _, field_config = field_config_result
    mappings = field_config.get("mappings", {})
    
    return list(mappings.keys())


def load_sequencing_file_enum(field_name: str) -> List[str]:
    """
    Load enum values for a sequencing_file field from sequencing_file_enum.json.
    
    Args:
        field_name: The field name (e.g., 'library_strategy', 'library_source_material')
        
    Returns:
        List of enum values, or empty list if file not found or field not present
    """
    enum_path = Path(__file__).parent.parent / "config_data" / "sequencing_file_enum.json"
    
    try:
        with enum_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and field_name in data:
                enum_values = data[field_name]
                if isinstance(enum_values, list):
                    return enum_values
            return []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_sample_enum(field_name: str) -> List[str]:
    """
    Load enum values for a sample field from sample_enum.json.
    
    Args:
        field_name: The field name (e.g., 'sample_tumor_status')
        
    Returns:
        List of enum values, or empty list if file not found or field not present
    """
    enum_path = Path(__file__).parent.parent / "config_data" / "sample_enum.json"
    
    try:
        with enum_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and field_name in data:
                enum_values = data[field_name]
                if isinstance(enum_values, list):
                    return enum_values
            return []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def build_invalid_value_all_clause(field_name: str) -> str:
    """
    Build ALL clause conditions for filtering invalid values in Cypher ALL() expressions.
    
    This is used in queries like:
    ALL(val IN non_null_values WHERE toString(val) = '' OR toString(val) = '-999' OR toString(val) = 'Not Reported')
    
    Args:
        field_name: The field name for config lookup (e.g., 'disease_phase')
        
    Returns:
        String with ALL clause conditions (OR-separated, for use in ALL() WHERE clause)
    """
    conditions = [
        "toString(val) = ''",
        "trim(toString(val)) = ''",
        "toString(val) = '-999'",
        "trim(toString(val)) = '-999'"
    ]
    
    # Get null_mappings for this specific field
    null_mappings = get_null_mappings(field_name)
    
    # Only add 'Not Reported' filter if it's in null_mappings
    if "Not Reported" in null_mappings:
        conditions.append("toString(val) = 'Not Reported'")
        conditions.append("trim(toString(val)) = 'Not Reported'")
    
    # Add any other null_mappings values
    for invalid_val in null_mappings:
        if invalid_val not in ['', '-999', 'Not Reported']:
            conditions.append(f"toString(val) = '{invalid_val}'")
            conditions.append(f"trim(toString(val)) = '{invalid_val}'")
    
    return " OR ".join(conditions)


def get_field_mapping_info(field_name: str) -> Optional[Dict[str, Any]]:
    """
    Get full field mapping configuration for a given field.
    
    Args:
        field_name: Name of the field
        
    Returns:
        Field configuration dict, or None if not found
    """
    field_config_result = _find_field_config(field_name)
    if field_config_result is None:
        return None
    
    node_type, field_config = field_config_result
    return {
        "node_type": node_type,
        "field_config": field_config
    }
