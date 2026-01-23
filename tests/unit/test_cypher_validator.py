import pytest

from app.utils.cypher_validator import (
    _extract_with_variables,
    validate_cypher_query,
    validate_unwind_variable_scope,
)


def test_extract_with_variables_handles_as_and_distinct():
    clause = "WITH DISTINCT p AS person, st, toString(x) AS x_str WHERE st IS NOT NULL"
    variables = _extract_with_variables(clause)

    assert "person" in variables
    assert "st" in variables
    assert "x_str" in variables


def test_validate_unwind_requires_following_with():
    query = """
    MATCH (p:participant)
    WITH p, [1,2] AS ids
    UNWIND ids AS id
    RETURN p, id
    """.strip()

    is_valid, error, warnings = validate_unwind_variable_scope(query)

    assert is_valid is False
    assert error is not None
    assert any("UNWIND" in warning for warning in warnings)


def test_validate_unwind_warns_about_scope_loss():
    query = """
    MATCH (p:participant)
    WITH p, [1,2] AS ids
    UNWIND ids AS id
    WITH id
    RETURN id
    """.strip()

    is_valid, error, warnings = validate_unwind_variable_scope(query)

    assert is_valid is True
    assert error is None
    assert any("UNWIND" in warning for warning in warnings)


def test_validate_unwind_detects_study_ids_scope_error():
    query = """
    WITH [1,2] AS study_ids
    UNWIND study_ids AS study_id
    WITH study_id
    RETURN study_ids
    """.strip()

    is_valid, error, warnings = validate_unwind_variable_scope(query)

    assert is_valid is False
    assert "study_ids" in error
    assert warnings


def test_validate_cypher_query_returns_warnings():
    query = """
    MATCH (p:participant)
    WITH p, [1,2] AS ids
    UNWIND ids AS id
    WITH id
    RETURN id
    """.strip()

    is_valid, error, warnings = validate_cypher_query(query)

    assert is_valid is True
    assert error is None
    assert warnings

