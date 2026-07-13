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


def split_diagnosis_category_tokens(raw: object) -> tuple[list[str], list[str]]:
    """
    Split a diagnosis_category value into (harmonized, unharmonized) lists.

    Accepts either a Memgraph 3.11 LIST property (list/tuple of tokens) or a
    legacy semicolon-delimited string. Returns deduplicated lists preserving
    insertion order.
    """
    harmonized: list[str] = []
    unharmonized: list[str] = []
    if raw is None:
        return harmonized, unharmonized

    if isinstance(raw, (list, tuple)):
        raw_tokens: list[str] = []
        for element in raw:
            if element is not None:
                raw_tokens.extend(str(element).split(";"))
    else:
        raw_tokens = str(raw).split(";")

    for token in raw_tokens:
        token = token.strip()
        if not token:
            continue
        canon = canonical_diagnosis_category_token(token)
        if canon is not None:
            harmonized.append(canon)
        else:
            unharmonized.append(token)
    return list(dict.fromkeys(harmonized)), list(dict.fromkeys(unharmonized))
