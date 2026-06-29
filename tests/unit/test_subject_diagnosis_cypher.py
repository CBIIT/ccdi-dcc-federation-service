"""Unit tests for subject_diagnosis_cypher predicates."""
from app.repositories.subject_diagnosis_cypher import (
    diagnosis_search_predicate,
    diagnosis_category_exact_token_predicate,
    diagnosis_category_contains_predicate,
)


def test_diagnosis_search_predicate_uses_coalesce_for_diagnosis():
    """diagnosis.diagnosis must be wrapped in coalesce so null property → false, not null."""
    result = diagnosis_search_predicate("d")
    # Must not contain bare toString(d.diagnosis) — that produces null when property is null
    assert "toString(d.diagnosis)" not in result
    # Must use coalesce so null becomes '' → predicate evaluates to false
    assert "coalesce(d.diagnosis, '')" in result


def test_diagnosis_search_predicate_uses_coalesce_in_list_else_branch():
    """The ELSE [var.diagnosis] branch must also use coalesce."""
    result = diagnosis_search_predicate("d")
    assert "[d.diagnosis]" not in result
    assert "[coalesce(d.diagnosis, '')]" in result


def test_diagnosis_search_predicate_uses_coalesce_in_any_predicate():
    """toLower(toString(diag)) in the ANY must use coalesce(diag, '')."""
    result = diagnosis_search_predicate("d")
    assert "toLower(toString(diag))" not in result
    assert "toLower(toString(coalesce(diag, '')))" in result


def test_diagnosis_search_predicate_preserves_comment_branch_structure():
    """The diagnosis_comment branch relies on IS NOT NULL short-circuit — keep it intact."""
    result = diagnosis_search_predicate("d")
    assert "d.diagnosis_comment IS NOT NULL" in result
    assert "toLower(toString(d.diagnosis_comment)) CONTAINS $diagnosis_search_term_lower" in result


def test_diagnosis_category_exact_token_predicate_already_safe():
    """Confirm this predicate already uses coalesce — should not be touched."""
    result = diagnosis_category_exact_token_predicate("d")
    assert "coalesce(d.diagnosis_category, '')" in result


def test_diagnosis_category_contains_predicate_already_safe():
    """Confirm this predicate already uses coalesce — should not be touched."""
    result = diagnosis_category_contains_predicate("d")
    assert "coalesce(d.diagnosis_category, '')" in result
