"""Build file metadata.unharmonized entries from a file node and the type map."""

import json
import logging
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

EXPLORE_FILES_URL = "https://clinicalcommons.ccdi.cancer.gov/exploreFiles"
DATA_CATEGORY_URL_SEPARATOR = "|"

_MAP_PATH = Path(__file__).resolve().parents[1] / "config_data" / "unharmonized_file_type_map.json"
_KEY_ORDER = (
    "dcc_file_name",
    "dcc_file_access",
    "dcc_file_type",
    "dcc_data_category",
    "dcc_dcf_indexd_guid",
    "dcc_authz",
    "dcc_acl",
    "dcc_file_mapping_level",
)


@lru_cache(maxsize=1)
def _load_file_type_map() -> dict[str, dict[str, Any]]:
    """Lowercased raw file_type → inner dcc_file_type object. Empty on any load failure."""
    if not _MAP_PATH.is_file():
        logger.warning("Could not load file type map; omitting dcc_file_type: file not found")
        return {}
    try:
        with _MAP_PATH.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.warning("Could not load file type map; omitting dcc_file_type: %s", exc)
        return {}
    if not isinstance(data, dict):
        logger.warning("File type map must be a JSON object; omitting dcc_file_type")
        return {}

    # The ticket attachment nests raw types under a top-level "unharmonized" key.
    # The committed file is flat (raw type → entry). Unwrap only a map of entries,
    # not a single dcc_file_type object that happens to use that key.
    entries = data
    wrapped = data.get("unharmonized")
    if isinstance(wrapped, dict) and "dcc_file_type" not in wrapped:
        entries = wrapped

    indexed: dict[str, dict[str, Any]] = {}
    for raw_type, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        inner = entry.get("dcc_file_type")
        if isinstance(inner, dict):
            indexed[str(raw_type).strip().lower()] = inner
    if not indexed:
        logger.warning("File type map is empty; omitting dcc_file_type")
    return indexed


def clear_file_unharmonized_cache() -> None:
    _load_file_type_map.cache_clear()


def get_file_type_map_entry(raw_file_type: Any) -> dict[str, Any] | None:
    """Look up the lowercased, stripped raw type. Returns the inner object or None."""
    if raw_file_type is None:
        return None
    key = str(raw_file_type).strip().lower()
    if not key:
        return None
    return _load_file_type_map().get(key)


def delist_access_value(raw: Any) -> str | None:
    """Turn an acl/authz list-shaped string into a ';'-joined plain string.

    Never eval. A real list is joined the same way. Text that is not wrapped in
    brackets is returned as stored. Returns None when nothing is left.
    """
    if raw is None:
        return None
    if isinstance(raw, list | tuple):
        parts = [str(item).strip() for item in raw if item is not None and str(item).strip()]
        return ";".join(parts) or None
    if not isinstance(raw, str):
        return str(raw)

    text = raw.strip()
    if not text:
        return None
    if not (text.startswith("[") and text.endswith("]")):
        return text

    wrapped: list[str] = []
    for piece in text[1:-1].split(","):
        piece = piece.strip()
        if len(piece) >= 2 and piece[0] == piece[-1] and piece[0] in {"'", '"'}:
            piece = piece[1:-1]
        if piece:
            wrapped.append(piece)
    return ";".join(wrapped) or None


def _scalar(value: Any) -> Any | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value


def _data_category_parts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if item is not None and str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _explore_url(param: str, values: list[str]) -> str:
    encoded = DATA_CATEGORY_URL_SEPARATOR.join(quote(str(value), safe="") for value in values)
    return f"{EXPLORE_FILES_URL}?{param}={encoded}"


def build_file_unharmonized(node: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Eight dcc_ keys in spec order. Null or empty DB values are left out."""
    built: dict[str, dict[str, Any]] = {}

    file_name = _scalar(node.get("file_name"))
    if file_name is not None:
        built["dcc_file_name"] = {"value": file_name}

    file_access = _scalar(node.get("file_access"))
    if file_access is not None:
        built["dcc_file_access"] = {"value": file_access}

    raw_type = node.get("file_type")
    mapped = get_file_type_map_entry(raw_type)
    if mapped is not None and _scalar(raw_type) is not None:
        # Keep the raw spelling, including case. Blank types never reach here.
        entry: dict[str, Any] = {"value": raw_type}
        entry["details"] = {"url": _explore_url("file_type", [str(raw_type)])}
        comment = mapped.get("comment")
        if comment is not None:
            entry["comment"] = comment
        built["dcc_file_type"] = entry

    categories = _data_category_parts(node.get("data_category"))
    if categories:
        built["dcc_data_category"] = {
            "value": ";".join(categories),
            "details": {"url": _explore_url("data_category", categories)},
        }

    guid = _scalar(node.get("dcf_indexd_guid"))
    if guid is not None:
        built["dcc_dcf_indexd_guid"] = {"value": guid}

    authz = delist_access_value(node.get("authz"))
    if authz is not None:
        built["dcc_authz"] = {"value": authz}

    acl = delist_access_value(node.get("acl"))
    if acl is not None:
        built["dcc_acl"] = {"value": acl}

    level = _scalar(node.get("file_mapping_level"))
    if level is not None:
        built["dcc_file_mapping_level"] = {
            "value": level,
            "details": {"url": _explore_url("file_mapping_level", [str(level)])},
        }

    return {key: built[key] for key in _KEY_ORDER if key in built}
