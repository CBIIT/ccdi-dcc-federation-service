"""Build sample metadata.unharmonized entries from the received attributes file.

The file is the authority for which raw values get an entry and for the
details block. A missing or malformed file yields no entries and one warning.
"""

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

SAMPLE_UNHARMONIZED_FIELDS: tuple[str, ...] = (
    "library_selection_method",
    "library_strategy",
    "preservation_method",
    "specimen_molecular_analyte_type",
    "disease_phase",
)

# Response key per field: the raw DB column name, prefixed with dcc_.
SAMPLE_UNHARMONIZED_KEYS: dict[str, str] = {
    "library_selection_method": "dcc_library_selection",
    "library_strategy": "dcc_library_strategy",
    "preservation_method": "dcc_fixation_embedding_method",
    "specimen_molecular_analyte_type": "dcc_library_source_molecule",
    "disease_phase": "dcc_disease_phase",
}

_CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config_data"
    / "unharmonized_sample_attrs.json"
)
_PLACEHOLDERS = {"", "string"}


def clear_sample_unharmonized_cache() -> None:
    """Drop the cached attributes file so the next call reads it again."""
    _load_sample_unharmonized_attrs.cache_clear()


@lru_cache(maxsize=1)
def _load_sample_unharmonized_attrs() -> dict[str, Any]:
    """Return the received file, or {} after one warning when it cannot be used."""
    if not _CONFIG_PATH.is_file():
        logger.warning(
            "Could not load sample unharmonized attrs; omitting entries: file not found"
        )
        return {}
    try:
        with _CONFIG_PATH.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.warning(
            "Could not load sample unharmonized attrs; omitting entries: %s", exc
        )
        return {}
    if not isinstance(data, dict):
        logger.warning(
            "Sample unharmonized attrs must be a JSON object; omitting entries"
        )
        return {}
    seen_bare_urls: set[str] = set()
    for key, value in data.items():
        if key not in SAMPLE_UNHARMONIZED_FIELDS:
            logger.warning("Ignoring unknown sample unharmonized field %s", key)
            continue
        if not isinstance(value, dict):
            logger.warning(
                "Skipping sample unharmonized field %s: value must be an object", key
            )
            continue
        for entry in value.values():
            if not isinstance(entry, dict):
                continue
            details = entry.get("details")
            url = details.get("url") if isinstance(details, dict) else None
            if (
                isinstance(url, str)
                and url not in _PLACEHOLDERS
                and "=" not in url
                and url not in seen_bare_urls
            ):
                seen_bare_urls.add(url)
                logger.warning(
                    "Sample unharmonized URL has no '='; passing through: %s", url
                )
    return data


def _kept_string(value: Any) -> str | None:
    """Keep a non-placeholder string. Empty and the literal 'string' are placeholders."""
    if isinstance(value, str) and value not in _PLACEHOLDERS:
        return value
    return None


def _build_url(url: Any, harmonized_value: str) -> str | None:
    kept = _kept_string(url)
    if kept is None:
        return None
    if "=" not in kept:
        return kept
    base = kept[: kept.rindex("=") + 1]
    return base + quote(harmonized_value, safe="")


def build_sample_unharmonized_entry(
    field: str, raw_value: Any, harmonized_value: str | None
) -> dict[str, Any] | None:
    """Return one unharmonized entry, or None when the gate fails.

    The gate requires a stripped raw value that is an exact key in the file and
    a harmonized value different from that stripped raw value. A harmonized
    value of None is allowed (the file maps the raw value to null, e.g.
    cDNA). The URL is then the file's URL as written, with nothing appended.
    """
    if field not in SAMPLE_UNHARMONIZED_FIELDS:
        return None
    if not isinstance(raw_value, str):
        return None
    raw = raw_value.strip()
    if not raw or harmonized_value == raw:
        return None

    field_map = _load_sample_unharmonized_attrs().get(field)
    if not isinstance(field_map, dict) or raw not in field_map:
        return None
    entry = field_map[raw]
    if not isinstance(entry, dict):
        logger.warning(
            "Skipping sample unharmonized entry %s.%s: entry must be an object",
            field,
            raw,
        )
        return None

    built: dict[str, Any] = {"value": raw}
    comment = _kept_string(entry.get("comment"))
    if comment is not None:
        built["comment"] = comment
    owned = entry.get("owned")
    if isinstance(owned, bool):
        built["owned"] = owned

    details_src = entry.get("details")
    details: dict[str, str] = {}
    if isinstance(details_src, dict):
        method = _kept_string(details_src.get("method"))
        harmonizer = _kept_string(details_src.get("harmonizer"))
        url = (
            _build_url(details_src.get("url"), harmonized_value)
            if harmonized_value is not None
            else _kept_string(details_src.get("url"))
        )
        if method is not None:
            details["method"] = method
        if harmonizer is not None:
            details["harmonizer"] = harmonizer
        if url is not None:
            details["url"] = url
    if details:
        built["details"] = details
    return built
