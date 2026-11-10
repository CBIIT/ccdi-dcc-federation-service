"""Filters for the six sample unharmonized fields.

Query keys are ``metadata.unharmonized.dcc_<field>``. The match is the raw
node property, case-insensitive, the same way ``/file`` matches
``dcc_file_name``. ``||`` means any of. A blank value matches nothing.

``dcc_diagnosis_category_<n>`` is the response key. ``<n>`` is not a position
filter: every numbered key matches any raw ``diagnosis_category`` token.
A scalar value, and any list element that still contains ``;``, is split
the same way the sample response splits it.
"""

from typing import Any

_PREFIX = "metadata.unharmonized."

# Canonical name -> (categorize bucket, node property).
_SPECS: dict[str, tuple[str, str]] = {
    "dcc_library_selection_method": ("sequencing_file", "library_selection"),
    "dcc_library_strategy": ("sequencing_file", "library_strategy"),
    "dcc_specimen_molecular_analyte_type": (
        "sequencing_file",
        "library_source_molecule",
    ),
    "dcc_preservation_method": ("pathology_file", "fixation_embedding_method"),
    "dcc_disease_phase": ("diagnosis", "disease_phase"),
    "dcc_diagnosis_category": ("diagnosis", "diagnosis_category"),
}

SEQUENCING_DCC_FILTERS = frozenset(
    name for name, (bucket, _prop) in _SPECS.items() if bucket == "sequencing_file"
)


def canonical_dcc_filter_name(key: str) -> str | None:
    """Return the canonical dcc filter name, or None when ``key`` is not one of the six."""
    if not isinstance(key, str) or not key.startswith(_PREFIX):
        return None
    name = key[len(_PREFIX) :]
    if name in _SPECS:
        return name
    marker = "dcc_diagnosis_category_"
    if name.startswith(marker) and name[len(marker) :].isdigit():
        return "dcc_diagnosis_category"
    return None


def dcc_filter_values(value: Any) -> list[Any]:
    """One predicate per query key. A list is several keys for the same field, AND'd by the caller."""
    if isinstance(value, list):
        return value
    return [value]


def dcc_filter_category(name: str) -> str:
    """Node bucket used by ``_categorize_filters``."""
    return _SPECS[name][0]


def dcc_filter_selected_values(value: Any) -> list[str]:
    """Stripped, lowercased values for one dcc filter. An empty list matches nothing."""
    items = value if isinstance(value, list) else str(value).split("||")
    return [str(item).strip().lower() for item in items if str(item).strip()]


def dcc_match_condition(
    name: str,
    alias: str,
    value: Any,
    param_name: str,
    params: dict[str, Any],
) -> str:
    """Cypher predicate for one allowlisted dcc filter.

    ``alias`` is the Cypher node variable (``sf``, ``pf``, ``d``, ``diagnoses``).
    A blank value returns ``false`` and does not set a parameter.
    """
    selected = dcc_filter_selected_values(value)
    if not selected:
        return "false"
    params[param_name] = selected
    prop = _SPECS[name][1]
    if name == "dcc_diagnosis_category":
        # Match each token the response would show. List elements are already
        # tokens except where one element still holds a ';' separated string.
        return (
            f"ANY(token IN CASE WHEN valueType({alias}.diagnosis_category) = 'LIST' "
            f"THEN {alias}.diagnosis_category ELSE [coalesce({alias}.diagnosis_category, '')] END "
            f"WHERE ANY(part IN split(toString(token), ';') "
            f"WHERE toLower(trim(part)) IN ${param_name}))"
        )
    return f"toLower(trim(toString({alias}.{prop}))) IN ${param_name}"
