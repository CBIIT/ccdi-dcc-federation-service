"""
Unit tests for cypher builder validation edge cases.

Tests missing lines in cypher_builder.py validation functions.
"""

import pytest
from app.utils.cypher_builder import validate_where_placement


@pytest.mark.unit
class TestCypherBuilderValidation:
    """Test validation edge cases in cypher_builder."""

    def test_validate_where_placement_duplicate_where_error(self):
        """Test validate_where_placement detects duplicate WHERE (lines 230-232)."""
        # Test query with duplicate WHERE clauses where prev_line doesn't end properly
        # The prev_line check (line 231) requires prev_line to exist and not end with expected keywords
        query = """MATCH (n)
        WHERE n.id = 1
        n.some_prop = 'value'
        WHERE n.name = 'test'"""
        
        is_valid, error = validate_where_placement(query)
        # Should detect duplicate WHERE when prev_line doesn't end properly (lines 230-232)
        # Line 230: prev_line = lines[i-1].strip() if i > 0 else ""
        # Line 231: if prev_line and not prev_line.endswith(...)
        # Line 232: return False, ...
        assert is_valid is False
        assert "duplicate WHERE" in error.lower() or "WHERE" in error

    def test_validate_where_placement_with_keyword_after_where(self):
        """Test validate_where_placement when keyword appears after WHERE (lines 234-235)."""
        # Test query where WITH appears after WHERE (resets in_where flag)
        query = """MATCH (n)
        WHERE n.id = 1
        WITH n
        RETURN n"""
        
        is_valid, error = validate_where_placement(query)
        # Should be valid - WITH resets the in_where flag
        assert is_valid is True

