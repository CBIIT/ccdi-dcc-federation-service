"""Load list-level gateway definitions from config."""

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from app.models.dto import NamedGateway

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config_data" / "gateways.json"


@lru_cache(maxsize=1)
def _load_gateways_config() -> dict[str, Any]:
    if not _CONFIG_PATH.is_file():
        return {}
    with _CONFIG_PATH.open(encoding="utf-8") as handle:
        return cast(dict[str, Any], json.load(handle))


def get_gateways(entity: str) -> list[dict[str, Any]]:
    """Return validated gateway dicts for an entity key (e.g. ``subjects``)."""
    raw = _load_gateways_config().get(entity)
    if not isinstance(raw, list):
        return []

    validated: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            logger.warning("Skipping gateway entry %s for %s: not an object", index, entity)
            continue
        try:
            validated.append(
                NamedGateway.model_validate(item).model_dump(mode="json", exclude_none=True)
            )
        except Exception as exc:
            logger.warning("Skipping invalid gateway entry for %s: %s", entity, exc)
    return validated
