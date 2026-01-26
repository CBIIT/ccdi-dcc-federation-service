"""
Tests for Cypher query validation, especially variable scoping.
"""

import pytest
from app.utils.cypher_validator import (
    validate_unwind_variable_scope,
    validate_cypher_query,
    _extract_with_variables
)


def test_extract_with_variables_simple():
    """Test extracting variables from simple WITH clause."""
    assert _extract_with_variables("WITH p, st, study_id") == {"p", "st", "study_id"}
    assert _extract_with_variables("WITH p AS participant, st AS study") == {"participant", "study", "p", "st"}


def test_extract_with_variables_with_where():
    """Test extracting variables from WITH clause with WHERE."""
    result = _extract_with_variables("WITH p, st WHERE p.id = 1")
    assert "p" in result
    assert "st" in result


def test_validate_unwind_missing_variables():
    """Test detection of missing variables after UNWIND."""
    # This query has a bug: study_ids is used after UNWIND but not in WITH
    bad_query = """
    WITH collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH sid, study_ids
    RETURN sid
    """
    
    is_valid, error, warnings = validate_unwind_variable_scope(bad_query)
    # Should detect that study_ids is not in scope after UNWIND
    assert not is_valid or len(warnings) > 0


def test_validate_unwind_correct_usage():
    """Test that correct UNWIND usage passes validation."""
    # This query is correct: only uses sid after UNWIND
    good_query = """
    WITH collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH sid
    RETURN sid
    """
    
    is_valid, error, warnings = validate_unwind_variable_scope(good_query)
    # Should pass (warnings about study_ids being lost are OK)
    assert is_valid or error is None


def test_validate_unwind_with_other_variables():
    """Test UNWIND with other variables that need to be preserved."""
    # This query should preserve p and st after UNWIND
    query = """
    WITH p, st, collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH p, st, sid
    RETURN p.id, st.id, sid
    """
    
    is_valid, error, warnings = validate_unwind_variable_scope(query)
    assert is_valid or error is None


def test_validate_unwind_missing_preserved_variables():
    """Test detection when variables needed after UNWIND are missing."""
    # This query has a bug: p and st are needed but not in WITH after UNWIND
    bad_query = """
    WITH p, st, collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH sid
    RETURN p.id, st.id, sid
    """
    
    is_valid, error, warnings = validate_unwind_variable_scope(bad_query)
    # Should detect that p and st are referenced but not in scope
    assert not is_valid or len(warnings) > 0


def test_validate_complex_query_with_race_filter():
    """Test validation of complex query with race filter and UNWIND scoping."""
    # Note: This test case is based on a bug that was found and fixed
    # (variables used after UNWIND without being in WITH clause)
    # This is similar to the actual buggy query
    complex_query = """
    WITH p, st, collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
         ethnicity_value, study_ids,
         namespace,
         sid
    WHERE toString(sid) <> ''
    WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
         ethnicity_value, study_ids,
         namespace,
         collect(sid) AS study_ids_filtered
    RETURN participant_id
    """
    
    is_valid, error, warnings = validate_unwind_variable_scope(complex_query)
    # Should detect that study_ids is used after UNWIND but not in scope
    assert not is_valid or any("study_ids" in w for w in warnings)


def test_validate_cypher_query_integration():
    """Test the main validation function."""
    bad_query = """
    WITH collect(DISTINCT study_id) AS study_ids
    UNWIND study_ids AS sid
    WITH sid, study_ids
    RETURN sid
    """
    
    is_valid, error, warnings = validate_cypher_query(bad_query)
    # Should catch the issue
    assert not is_valid or len(warnings) > 0

