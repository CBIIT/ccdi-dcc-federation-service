import json
from pathlib import Path

_ENUM_PATH = Path(__file__).parent.parent / "config_data" / "diagnosis_enum.json"

with _ENUM_PATH.open() as _f:
    _data = json.load(_f)

HARMONIZED_DIAGNOSIS_CATEGORIES: frozenset[str] = frozenset(_data["diagnosis_category"])

# Lowercase PV -> canonical spelling from enum (for case-insensitive harmonization)
_CANONICAL_BY_LOWER: dict[str, str] = {pv.lower(): pv for pv in _data["diagnosis_category"]}


def canonical_diagnosis_category_token(token: str) -> str | None:
    """
    If token matches a harmonized PV ignoring case, return the canonical PV string; else None.
    """
    if token is None:
        return None
    t = str(token).strip()
    if not t:
        return None
    return _CANONICAL_BY_LOWER.get(t.lower())
