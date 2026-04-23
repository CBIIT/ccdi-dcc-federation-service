"""
Shared Cypher fragments for subject diagnosis filtering.

Used by SubjectRepository list queries and SubjectSummary (diagnosis endpoint).
Aligned with sample diagnosis search: `see diagnosis_comment` sentinel handling.
"""

from __future__ import annotations

from typing import Optional


def diagnosis_search_predicate(var: str) -> str:
    """
    Boolean Cypher expression: sample-style match on diagnosis text for one binding `var`.

    Requires params when search is active:
      $diagnosis_search_term_lower, $diagnosis_search_term_see_comment
    """
    return (
        f"( (toLower(trim(toString({var}.diagnosis))) <> $diagnosis_search_term_see_comment AND "
        f"ANY(diag IN CASE WHEN valueType({var}.diagnosis) = 'LIST' THEN {var}.diagnosis ELSE [{var}.diagnosis] END "
        f"WHERE toLower(toString(diag)) CONTAINS $diagnosis_search_term_lower)) OR "
        f"(toLower(trim(toString({var}.diagnosis))) = $diagnosis_search_term_see_comment AND "
        f"{var}.diagnosis_comment IS NOT NULL AND "
        f"toLower(toString({var}.diagnosis_comment)) CONTAINS $diagnosis_search_term_lower) )"
    )


def diagnosis_category_exact_token_predicate(var: str) -> str:
    """GET /subject style: exact token after ';' split. Requires $diag_category_filter."""
    return (
        f"any(token IN split(toString(coalesce({var}.diagnosis_category, '')), ';') "
        f"WHERE toLower(trim(token)) = toLower($diag_category_filter))"
    )


def diagnosis_category_contains_predicate(var: str) -> str:
    """Experimental /subject-diagnosis: full-string CONTAINS. Requires $diag_category_contains_term."""
    return (
        f"toLower(toString(coalesce({var}.diagnosis_category, ''))) "
        f"CONTAINS toLower($diag_category_contains_term)"
    )


def diagnosis_nodes_match_size_predicate(
    *,
    list_var: str,
    elem_var: str,
    diagnosis_search_term: Optional[str],
    diag_category_filter: Optional[str],
    diagnosis_category_contains: Optional[str],
) -> str:
    """
    `size([elem IN list_var WHERE ...]) > 0` for harmonized diagnosis_nodes / diag_cat_nodes lists.
    """
    parts: list[str] = []
    if diagnosis_search_term:
        parts.append(diagnosis_search_predicate(elem_var))
    if diag_category_filter:
        parts.append(diagnosis_category_exact_token_predicate(elem_var))
    if diagnosis_category_contains:
        parts.append(diagnosis_category_contains_predicate(elem_var))
    if not parts:
        return ""
    inner = " AND ".join(parts)
    return (
        f"size([{elem_var} IN {list_var} WHERE {elem_var} IS NOT NULL AND ({inner})]) > 0"
    )


def add_diagnosis_search_params(params: dict, diagnosis_search_term: Optional[str]) -> None:
    if diagnosis_search_term:
        params["diagnosis_search_term_lower"] = str(diagnosis_search_term).lower().strip()
        params["diagnosis_search_term_see_comment"] = "see diagnosis_comment"


def single_diagnosis_node_predicate(
    var: str,
    *,
    diagnosis_search_term: Optional[str],
    diag_category_filter: Optional[str],
    diagnosis_category_contains: Optional[str],
) -> str:
    """AND-joined predicates on one diagnosis binding `var` (same node for all parts)."""
    parts: list[str] = []
    if diagnosis_search_term:
        parts.append(f"({diagnosis_search_predicate(var)})")
    if diag_category_filter:
        parts.append(f"({diagnosis_category_exact_token_predicate(var)})")
    if diagnosis_category_contains:
        parts.append(f"({diagnosis_category_contains_predicate(var)})")
    return " AND ".join(parts) if parts else "true"
