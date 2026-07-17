from app.repositories.subject_diagnosis_cypher import (
    diagnosis_category_exact_token_predicate,
    diagnosis_category_contains_predicate,
)


def test_exact_token_predicate_is_list_native():
    pred = diagnosis_category_exact_token_predicate("dg")
    # no string-split idiom; iterate the list directly
    assert "SPLIT" not in pred.upper()
    assert "split(toString(coalesce(dg.diagnosis_category, '')), ';')" not in pred
    assert "coalesce(dg.diagnosis_category, [])" in pred
    assert "$diag_category_filters" in pred


def test_contains_predicate_is_element_wise_list_native():
    pred = diagnosis_category_contains_predicate("dg")
    # never toString() the whole list; iterate elements
    assert "toString(coalesce(dg.diagnosis_category, ''))" not in pred
    assert "coalesce(dg.diagnosis_category, [])" in pred
    assert "$diag_category_contains_term" in pred
