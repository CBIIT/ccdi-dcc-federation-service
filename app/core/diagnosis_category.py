import json
from pathlib import Path

from app.core.field_mappings import (
    build_case_mapping_statement,
    map_field_value,
    reverse_map_field_value,
)

_ENUM_PATH = Path(__file__).parent.parent / "config_data" / "diagnosis_enum.json"

with _ENUM_PATH.open() as _f:
    _data = json.load(_f)

HARMONIZED_DIAGNOSIS_CATEGORIES: frozenset[str] = frozenset(_data["diagnosis_category"])

# Lowercase PV -> canonical spelling from enum (for case-insensitive harmonization)
_CANONICAL_BY_LOWER: dict[str, str] = {pv.lower(): pv for pv in _data["diagnosis_category"]}


def canonical_diagnosis_category_token(token: str) -> str | None:
    """
    If token matches a harmonized PV (after field_mappings + case fold), return the
    canonical PV string; else None.
    """
    if token is None:
        return None
    t = str(token).strip()
    if not t:
        return None
    # Apply DB→API aliases from field_mappings.json (e.g. Myeloid leukemias → Myeloid Leukemia)
    mapped = map_field_value("diagnosis_category", t)
    if mapped is None:
        return None
    return _CANONICAL_BY_LOWER.get(str(mapped).lower())


def diagnosis_category_token_case_expr(variable_name: str = "token") -> str:
    """
    Cypher expression that maps a diagnosis_category token via field_mappings CASE,
    or returns the token unchanged when no mappings exist.
    """
    case_statement = build_case_mapping_statement("diagnosis_category", variable_name)
    return case_statement if case_statement else variable_name


def diagnosis_category_filter_db_values(api_value: str) -> list[str]:
    """
    Expand an API diagnosis_category filter to DB token spellings.

    Uses reverse_mappings so e.g. "Myeloid Leukemia" also matches DB "Myeloid leukemias".
    """
    if api_value is None:
        return []
    t = str(api_value).strip()
    if not t:
        return []
    reverse_mapped = reverse_map_field_value("diagnosis_category", t)
    if isinstance(reverse_mapped, list):
        return list(
            dict.fromkeys(str(v) for v in reverse_mapped if v is not None and str(v).strip())
        )
    if reverse_mapped:
        return [str(reverse_mapped)]
    return [t]


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
