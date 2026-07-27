"""File-endpoint-only DB↔API file_type mapping helpers.

Loads ``app/config_data/file_type_mappings.json``. Not used by subject/sample
``field_mappings.json`` machinery.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config_data" / "file_type_mappings.json"

_cache: Optional[Dict[str, Any]] = None
_mappings_lower_cache: Optional[Dict[str, str]] = None
_null_mappings_lower_cache: Optional[set] = None


def _load_config() -> Dict[str, Any]:
    global _cache
    if _cache is None:
        with _CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        _cache = data.get("file_type", data)
    return _cache


def _load_mappings_lower() -> Dict[str, str]:
    """Lowercase-keyed {db_value: api_value} index, built once from config."""
    global _mappings_lower_cache
    if _mappings_lower_cache is None:
        config = _load_config()
        _mappings_lower_cache = {
            str(db_key).lower(): api_value
            for db_key, api_value in config.get("mappings", {}).items()
        }
    return _mappings_lower_cache


def _load_null_mappings_lower() -> set:
    global _null_mappings_lower_cache
    if _null_mappings_lower_cache is None:
        config = _load_config()
        _null_mappings_lower_cache = {str(v).lower() for v in config.get("null_mappings", [])}
    return _null_mappings_lower_cache


def clear_file_type_mappings_cache() -> None:
    """Clear cached config (for tests)."""
    global _cache, _mappings_lower_cache, _null_mappings_lower_cache
    _cache = None
    _mappings_lower_cache = None
    _null_mappings_lower_cache = None


def map_file_type_db_to_api(db_value: Any) -> Optional[str]:
    """
    Map a database file_type value to an API (col D) value.

    Lookup is case-insensitive on the DB value. Returns None for null_mappings
    and for values with no mapping entry (no enum fallback).
    """
    if db_value is None:
        return None

    str_value = str(db_value).strip()
    if not str_value:
        return None

    key = str_value.lower()

    if key in _load_null_mappings_lower():
        return None

    return _load_mappings_lower().get(key)


def get_db_values_for_api_file_type(api_value: str) -> List[str]:
    """
    Reverse-map an API PV to lowercase DB value(s) for case-insensitive Cypher IN.

    If the PV has reverse_mappings, return those DB keys (lowercased).
    Otherwise return ``[api_value.lower()]`` for legacy enum-only PVs (option B).
    """
    config = _load_config()
    reverse = config.get("reverse_mappings", {})
    if api_value in reverse:
        mapped = reverse[api_value]
        if isinstance(mapped, list):
            return [str(v).lower() for v in mapped]
        return [str(mapped).lower()]
    return [str(api_value).lower()]


def get_mappable_db_values_lower() -> List[str]:
    """Lowercase DB keys that map to an API (col D) value — for count values/missing filters."""
    config = _load_config()
    return sorted({str(k).lower() for k in config.get("mappings", {}).keys()})
