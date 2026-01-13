import pytest

from app.utils.cypher_builder import (
    CypherWhereBuilder,
    combine_where_clauses,
    append_where_conditions,
    build_where_clause,
    validate_where_placement,
)


def test_where_builder_builds_empty_when_no_conditions():
    b = CypherWhereBuilder()
    assert b.build() == ""
    assert b.build_conditions_only() == ""


def test_where_builder_filters_blank_conditions():
    b = CypherWhereBuilder().add("").add("  ").add("a = 1")
    assert b.build() == "WHERE a = 1"


@pytest.mark.parametrize(
    "a,b,expected",
    [
        ("WHERE a = 1", "b = 2", "WHERE a = 1 AND b = 2"),
        ("", "b = 2", "WHERE b = 2"),
        ("WHERE a = 1", "", "WHERE a = 1"),
        ("", "", ""),
        ("WHERE a = 1 AND b = 2", "WHERE c = 3", "WHERE a = 1 AND b = 2 AND c = 3"),
    ],
)
def test_combine_where_clauses(a, b, expected):
    assert combine_where_clauses(a, b) == expected


def test_append_where_conditions():
    assert append_where_conditions("WHERE a = 1", "b = 2") == "WHERE a = 1 AND b = 2"
    assert append_where_conditions("", "a = 1") == "WHERE a = 1"


def test_build_where_clause():
    assert build_where_clause(["a = 1", "b = 2"]) == "WHERE a = 1 AND b = 2"
    assert build_where_clause([]) == ""


def test_validate_where_placement_catches_duplicate_where():
    q = """
    MATCH (n)
    WHERE n.id = 1
    WHERE n.name = 'x'
    RETURN n
    """.strip()
    ok, err = validate_where_placement(q)
    assert ok is False
    assert err

