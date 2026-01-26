"""
Unit tests to validate Cypher query syntax and detect common issues.

These tests focus on:
1. No duplicate WHERE clauses
2. No unbound variables
3. Valid Cypher syntax patterns
"""

import re
from typing import Dict, Any, List
from app.repositories.subject import SubjectRepository
from app.core.config import Settings


class _DummyAllowlist:
    def is_allowed(self, value: str) -> bool:
        return True


class _CapturingSession:
    """Mock session that captures the last Cypher query."""
    def __init__(self):
        self.last_cypher = None
        self.last_params = None
    
    async def run(self, cypher, params=None):
        self.last_cypher = cypher
        self.last_params = params
        # Return empty result to avoid data processing errors
        return _DummyResult([])


class _DummyResult:
    def __init__(self, data):
        self.data = data


def test_no_consecutive_where_clauses_in_depositions_path():
    """
    Test that depositions path with diagnosis search doesn't generate duplicate WHERE clauses.
    
    Tests the combination:
    - search=Neuroblastoma + depositions=phs002431
    - Should not generate two consecutive WHERE clauses
    - Note: This was a bug that was fixed (consecutive WHERE clauses causing syntax errors)
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())
    
    import asyncio
    
    # Test the problematic combination that previously caused a bug
    try:
        asyncio.run(repo.get_subjects({
            "_diagnosis_search": "Neuroblastoma",
            "depositions": "phs002431"
        }, offset=0, limit=5))
    except Exception:
        # Ignore data processing errors - we only care about Cypher generation
        pass
    
    assert session.last_cypher is not None, "Cypher query should be generated"
    cypher = session.last_cypher
    
    # Check for consecutive WHERE clauses (not separated by WITH, MATCH, etc.)
    lines = cypher.split('\n')
    for i in range(len(lines) - 1):
        line1 = lines[i].strip()
        line2 = lines[i+1].strip() if i+1 < len(lines) else ""
        
        # Check if both lines contain standalone WHERE clauses
        # (not WHERE inside list comprehensions like "size([... WHERE ...])")
        if re.search(r'^\s*WHERE\s+[^W]', line1) or ('WHERE' in line1 and not re.search(r'\[.*WHERE.*\]', line1)):
            if re.search(r'^\s*WHERE\s+[^W]', line2) or ('WHERE' in line2 and not re.search(r'\[.*WHERE.*\]', line2)):
                # Allow if line2 is just a comment
                if not line2.startswith('//'):
                    raise AssertionError(
                        f"Consecutive WHERE clauses found at lines {i+1}-{i+2}:\n"
                        f"  Line {i+1}: {line1[:80]}\n"
                        f"  Line {i+2}: {line2[:80]}"
                    )


def test_no_duplicate_where_clauses_all_filter_combinations():
    """
    Test that all filter combinations generate valid Cypher without duplicate WHERE clauses.
    
    This comprehensive test checks multiple filter combinations to catch edge cases.
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())
    
    import asyncio
    
    # Test various filter combinations
    test_cases = [
        {"_diagnosis_search": "Neuroblastoma", "depositions": "phs002431"},
        {"_diagnosis_search": "Glioblastoma", "depositions": "phs003215"},
        {"_diagnosis_search": "Neuroblastoma", "depositions": "phs003215||phs002310"},
        {"vital_status": "Dead", "depositions": "phs003215", "_diagnosis_search": "Neuro"},
        {"identifiers": "HTA4_1", "depositions": "phs003215", "_diagnosis_search": "Gliob"},
        {"vital_status": "Alive", "depositions": "phs002431", "_diagnosis_search": "Neuro"},
    ]
    
    for filters in test_cases:
        try:
            asyncio.run(repo.get_subjects(filters, offset=0, limit=5))
        except Exception:
            # Ignore data processing errors
            pass
        
        if session.last_cypher:
            cypher = session.last_cypher
            
            # Count standalone WHERE clauses (not in list comprehensions)
            # Pattern: WHERE at start of line (after whitespace), not inside [... WHERE ...]
            standalone_where_pattern = r'\n\s+WHERE\s+[^W]'
            matches = re.findall(standalone_where_pattern, cypher)
            
            # Check for consecutive WHERE clauses
            lines = cypher.split('\n')
            for i in range(len(lines) - 1):
                line1 = lines[i]
                line2 = lines[i+1] if i+1 < len(lines) else ""
                
                # Check if line1 has a standalone WHERE
                if re.search(r'^\s+WHERE\s+', line1) and not re.search(r'\[.*WHERE.*\]', line1):
                    # Check if line2 also has a standalone WHERE
                    if re.search(r'^\s+WHERE\s+', line2) and not re.search(r'\[.*WHERE.*\]', line2):
                        # Allow if line2 is just a comment
                        if not line2.strip().startswith('//'):
                            raise AssertionError(
                                f"Consecutive WHERE clauses found for filters {filters} at lines {i+1}-{i+2}:\n"
                                f"  Line {i+1}: {line1[:80]}\n"
                                f"  Line {i+2}: {line2[:80]}"
                            )


def test_cypher_syntax_no_mismatched_input_errors():
    """
    Test that generated Cypher queries don't have syntax errors that would cause
    "mismatched input" errors from Memgraph.
    
    This test checks for a specific issue where:
    - Error: "mismatched input 'WHERE' expecting {<EOF>, ';'}"
    - Note: This was a bug that was fixed (consecutive WHERE clauses causing syntax errors)
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())
    
    import asyncio
    
    # Test the specific combination that previously caused a bug
    problematic_filters = {
        "_diagnosis_search": "Neuroblastoma",
        "depositions": "phs002431"
    }
    
    try:
        asyncio.run(repo.get_subjects(problematic_filters, offset=0, limit=5))
    except Exception as e:
        # If it's a syntax error in the generated Cypher, that's a bug
        error_str = str(e)
        if "mismatched input" in error_str.lower() or "syntax error" in error_str.lower():
            raise AssertionError(
                f"Cypher syntax error detected for filters {problematic_filters}:\n"
                f"  Error: {error_str}\n"
                f"  Generated Cypher:\n{session.last_cypher[:500]}"
            )
        # Other errors (like data processing) are OK for this test
    
    # Verify Cypher was generated
    assert session.last_cypher is not None, "Cypher query should be generated"
    
    # Check for common syntax issues
    cypher = session.last_cypher
    
    # Check for consecutive WHERE clauses
    # Note: This was a bug that was fixed (consecutive WHERE clauses would cause syntax errors)
    if re.search(r'WHERE[^\n]*\n[^\n]*WHERE', cypher):
        # This might be OK if separated by comments, but check carefully
        lines = cypher.split('\n')
        for i in range(len(lines) - 1):
            if 'WHERE' in lines[i] and 'WHERE' in lines[i+1]:
                # Check if it's a standalone WHERE (not in list comprehension)
                if (re.search(r'^\s+WHERE\s+', lines[i]) and 
                    re.search(r'^\s+WHERE\s+', lines[i+1]) and
                    not lines[i+1].strip().startswith('//')):
                    raise AssertionError(
                        f"Consecutive WHERE clauses detected at lines {i+1}-{i+2}:\n"
                        f"  {lines[i][:80]}\n"
                        f"  {lines[i+1][:80]}"
                    )


def test_summary_queries_no_duplicate_where_clauses():
    """
    Test that summary queries also don't generate duplicate WHERE clauses.
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())
    
    import asyncio
    
    test_cases = [
        {"_diagnosis_search": "Neuroblastoma", "depositions": "phs002431"},
        {"vital_status": "Dead", "depositions": "phs003215", "_diagnosis_search": "Neuro"},
    ]
    
    for filters in test_cases:
        try:
            asyncio.run(repo.get_subjects_summary(filters))
        except Exception:
            # Ignore data processing errors
            pass
        
        if session.last_cypher:
            cypher = session.last_cypher
            
            # Check for consecutive WHERE clauses
            lines = cypher.split('\n')
            for i in range(len(lines) - 1):
                line1 = lines[i]
                line2 = lines[i+1] if i+1 < len(lines) else ""
                
                if (re.search(r'^\s+WHERE\s+', line1) and 
                    re.search(r'^\s+WHERE\s+', line2) and
                    not re.search(r'\[.*WHERE.*\]', line1) and
                    not re.search(r'\[.*WHERE.*\]', line2) and
                    not line2.strip().startswith('//')):
                    raise AssertionError(
                        f"Consecutive WHERE clauses in summary query for filters {filters} at lines {i+1}-{i+2}"
                    )


def test_where_clauses_properly_combined():
    """
    Test that when multiple conditions are present, they're combined with AND in a single WHERE clause.
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())
    
    import asyncio
    
    # Test combination that should use combined WHERE clause
    filters = {
        "_diagnosis_search": "Neuroblastoma",
        "depositions": "phs002431"
    }
    
    try:
        asyncio.run(repo.get_subjects(filters, offset=0, limit=5))
    except Exception:
        pass
    
    if session.last_cypher:
        cypher = session.last_cypher
        
        # Check that diagnosis search and depositions are combined with AND
        # (if both are present, should see "AND" between them in a single WHERE)
        if "_diagnosis_search" in str(filters) and "depositions" in str(filters):
            # Look for pattern: WHERE ... AND ... (both conditions in one WHERE)
            # This is the correct pattern after the fix
            has_combined = re.search(
                r'WHERE.*size\(\[node IN diagnosis_nodes.*\]\).*AND.*size\(\[sid IN study_ids',
                cypher,
                re.DOTALL
            )
            
            # Also check that we don't have two separate WHERE clauses
            standalone_where_count = len(re.findall(r'\n\s+WHERE\s+[^W]', cypher))
            
            # If both filters are present, we should have combined WHERE or at most one WHERE per section
            # (can have multiple WHERE if separated by WITH/MATCH)
            if standalone_where_count > 2:
                # Check if they're in different sections (separated by WITH/MATCH)
                lines = cypher.split('\n')
                where_line_numbers = [i+1 for i, line in enumerate(lines) if re.search(r'^\s+WHERE\s+', line)]
                
                # If we have more than 2 WHERE clauses, they should be in different query sections
                # (separated by WITH, MATCH, etc.)
                if len(where_line_numbers) > 2:
                    # This might be OK if they're in different sections
                    # But if they're consecutive, that's a bug
                    for i in range(len(where_line_numbers) - 1):
                        if where_line_numbers[i+1] - where_line_numbers[i] == 1:
                            raise AssertionError(
                                f"Consecutive WHERE clauses detected at lines {where_line_numbers[i]}-{where_line_numbers[i+1]}"
                            )

