"""Build the details block for unharmonized metadata entries from metadata_fields.json."""

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

logger = logging.getLogger(__name__)

_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config_data" / "metadata_fields.json"
)
_NUMBERED_KEY = re.compile(r"(.+)_\d+")


@lru_cache(maxsize=1)
def _load_metadata_fields() -> dict[str, Any]:
    try:
        with _CONFIG_PATH.open(encoding="utf-8") as handle:
            return cast(dict[str, Any], json.load(handle))
    except Exception as exc:
        logger.warning(
            "Could not load metadata field config for unharmonized details; omitting details: %s",
            exc,
        )
        return {}


def _field_for_key(entity: str, key: str) -> dict[str, Any] | None:
    fields = _load_metadata_fields().get(entity, {}).get("fields", [])
    numbered = _NUMBERED_KEY.fullmatch(key)
    if numbered:
        wanted = f"unharmonized.{numbered.group(1)}_<n>"
    else:
        wanted = f"unharmonized.{key}"
    for field in fields:
        if isinstance(field, dict) and field.get("path") == wanted:
            return field
    return None


def build_unharmonized_details(
    entity: str, key: str, value: Any
) -> dict[str, str] | None:
    """Return method/harmonizer/url for one unharmonized key, or None when not configured."""
    field = _field_for_key(entity, key)
    if field is None or field.get("harmonized") is True:
        return None
    details = field.get("details")
    if details is None:
        return None
    if not isinstance(details, dict):
        logger.warning(
            "Skipping unharmonized details for %s.%s: details must be an object",
            entity,
            key,
        )
        return None
    template = details.get("url_template")
    if "url_template" in details and not isinstance(template, str):
        logger.warning(
            "Skipping unharmonized details for %s.%s: url_template must be a string",
            entity,
            key,
        )
        return None

    built: dict[str, str] = {}
    method = details.get("method")
    harmonizer = details.get("harmonizer")
    if isinstance(method, str):
        built["method"] = method
    if isinstance(harmonizer, str):
        built["harmonizer"] = harmonizer
    if isinstance(template, str) and value is not None:
        built["url"] = template.replace("{value}", quote(str(value), safe=""))
    return built or None


def attach_unharmonized_details(
    entity: str,
    unharmonized: dict[str, dict[str, Any]] | None,
) -> dict[str, dict[str, Any]] | None:
    """Copy each entry and add details where the config defines them."""
    if not unharmonized:
        return unharmonized
    attached: dict[str, dict[str, Any]] = {}
    for key, entry in unharmonized.items():
        if not isinstance(entry, dict):
            attached[key] = entry
            continue
        updated = dict(entry)
        details = build_unharmonized_details(entity, key, entry.get("value"))
        if details is not None:
            updated["details"] = details
        attached[key] = updated
    return attached
