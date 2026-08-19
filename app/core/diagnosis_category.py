import json
from pathlib import Path
from typing import Any, Literal, Optional

from app.core.field_mappings import (
    _find_field_config,
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

DiagnosisCategoryKind = Literal["native", "alias", "other"]


def _diagnosis_category_mapping_keys() -> frozenset[str]:
    """Exact keys from field_mappings.json diagnosis.diagnosis_category.mappings."""
    found = _find_field_config("diagnosis_category")
    if found is None:
        return frozenset()
    _, field_config = found
    mappings = field_config.get("mappings") or {}
    return frozenset(str(k) for k in mappings.keys())


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


def classify_diagnosis_category_token(
    token: str,
) -> tuple[DiagnosisCategoryKind, str, Optional[str]]:
    """
    Classify a raw diagnosis_category token for sample response assembly.

    Returns (kind, raw, transformed_or_canonical):
    - native: not a mappings key; case-fold matches a PV → canonical PV
    - alias: mappings key whose mapped value is a PV → transformed PV
    - other: no PV after mapping → transformed_or_canonical is None
    """
    raw = str(token).strip()
    if not raw:
        return "other", raw, None

    is_mapping_key = raw in _diagnosis_category_mapping_keys()
    mapped = map_field_value("diagnosis_category", raw)
    if mapped is None:
        return "other", raw, None
    canon = _CANONICAL_BY_LOWER.get(str(mapped).lower())

    if is_mapping_key and canon is not None:
        return "alias", raw, canon
    if not is_mapping_key and canon is not None:
        return "native", raw, canon
    return "other", raw, None


def iter_diagnosis_category_raw_tokens(raw: object) -> list[str]:
    """Flatten LIST or semicolon-delimited diagnosis_category into trimmed raw tokens."""
    if raw is None:
        return []

    if isinstance(raw, (list, tuple)):
        pieces: list[str] = []
        for element in raw:
            if element is not None:
                pieces.extend(str(element).split(";"))
    else:
        pieces = str(raw).split(";")

    tokens: list[str] = []
    for piece in pieces:
        t = piece.strip()
        if t:
            tokens.append(t)
    return tokens


def build_sample_diagnosis_category_fields(
    raw_tokens: list[str],
) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """
    Build sample singular diagnosis_category + dcc_* unharmonized map.

    Returns (regular_field_dict_or_None, unharmonized_dict_or_None).
    regular_field_dict has ``value`` and optionally ``ancestors``.
    Caller may wrap regular into SampleDiagnosisCategoryField.
    """
    # Dedupe by raw string, preserve order
    ordered = list(dict.fromkeys(raw_tokens))
    classified: list[tuple[DiagnosisCategoryKind, str, Optional[str]]] = [
        classify_diagnosis_category_token(t) for t in ordered
    ]

    natives = [(i, c) for i, c in enumerate(classified) if c[0] == "native"]
    aliases = [(i, c) for i, c in enumerate(classified) if c[0] == "alias"]

    regular: Optional[dict[str, Any]] = None
    skip_index: Optional[int] = None  # native chosen for regular is omitted from dcc_*

    if natives:
        skip_index, (_kind, _raw, canon) = natives[0]
        regular = {"value": canon}
    elif aliases:
        _idx, (_kind, _raw, transformed) = aliases[0]
        # dcc index assigned below; ancestors filled after numbering
        regular = {"value": transformed, "_promote_raw": _raw}

    unharmonized: dict[str, Any] = {}
    dcc_i = 0
    promote_raw = regular.pop("_promote_raw", None) if regular else None
    promote_dcc_key: Optional[str] = None

    for i, (kind, raw, transformed) in enumerate(classified):
        if skip_index is not None and i == skip_index:
            continue
        dcc_i += 1
        key = f"dcc_diagnosis_category_{dcc_i}"
        if kind == "native":
            unharmonized[key] = {"value": transformed}
        elif kind == "alias":
            entry = {
                "value": raw,
                "comment": transformed,
                "owned": True,
            }
            unharmonized[key] = entry
            if promote_raw is not None and raw == promote_raw and promote_dcc_key is None:
                promote_dcc_key = key
        else:
            unharmonized[key] = {"value": raw}

    if regular is not None and promote_raw is not None:
        if promote_dcc_key is None:
            # Should not happen if promote_raw was in classified aliases
            regular = {"value": regular["value"]}
        else:
            regular = {
                "value": regular["value"],
                "ancestors": [f"unharmonized.{promote_dcc_key}"],
            }

    return regular, (unharmonized or None)

def diagnosis_category_token_case_expr(variable_name: str = "token") -> str:
    """
    Cypher expression that maps a diagnosis_category token via field_mappings CASE,
    or returns the token unchanged when no mappings exist.
    """
    case_statement = build_case_mapping_statement("diagnosis_category", variable_name)
    return case_statement if case_statement else variable_name


def diagnosis_category_filter_db_values(api_value: str) -> list[str]:
    """
    Expand an API diagnosis_category filter to lowercased DB token spellings.

    Uses reverse_mappings so e.g. "Myeloid Leukemia" also matches DB "Myeloid leukemias".
    Values are lowercased for Cypher ``IN $diag_category_filters`` membership against
    ``toLower(trim(toString(token)))`` — callers must not re-lower.
    """
    if api_value is None:
        return []
    t = str(api_value).strip()
    if not t:
        return []
    reverse_mapped = reverse_map_field_value("diagnosis_category", t)
    if isinstance(reverse_mapped, list):
        raw = [str(v) for v in reverse_mapped if v is not None and str(v).strip()]
    elif reverse_mapped:
        raw = [str(reverse_mapped)]
    else:
        raw = [t]
    # Lowercase once here so all Cypher producers share one contract (ASCII PVs today).
    return list(dict.fromkeys(v.lower() for v in raw if v.strip()))


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
