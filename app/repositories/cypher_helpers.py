"""
Helper functions for Cypher query construction and validation.

This module provides utilities to ensure consistent variable naming
and prevent common Cypher query issues like:
- Variable name mismatches (diagnoses vs d)
- Cartesian products (p vs p2)
- Missing WITH clauses
"""

from typing import Optional


def adapt_where_clause_for_query_pattern(
    where_clause: str,
    query_pattern: str,
    skip_fields: Optional[list] = None
) -> str:
    """
    Adapt a WHERE clause to match the variable naming convention of a query pattern.
    
    Args:
        where_clause: The original WHERE clause (may include 'WHERE' prefix)
        query_pattern: One of 'optimized_diagnosis_first', 'standard_query'
        skip_fields: List of field names to skip (already handled elsewhere)
    
    Returns:
        Adapted WHERE clause with correct variable names
    
    Examples:
        >>> adapt_where_clause_for_query_pattern(
        ...     "WHERE diagnoses.diagnosis = $param",
        ...     "optimized_diagnosis_first"
        ... )
        "WHERE d.diagnosis = $param"
        
        >>> adapt_where_clause_for_query_pattern(
        ...     "WHERE diagnoses.disease_phase = $param",
        ...     "optimized_diagnosis_first",
        ...     skip_fields=["disease_phase"]
        ... )
        ""
    """
    if not where_clause:
        return ""
    
    # Remove WHERE prefix if present (we'll add it back if needed)
    clause = where_clause.strip()
    if clause.startswith("WHERE "):
        clause = clause[6:]  # Remove "WHERE " prefix (6 characters)
    
    # Skip if empty after removing WHERE
    if not clause:
        return ""
    
    # Skip fields that are already handled
    if skip_fields:
        for field in skip_fields:
            if field in clause:
                return ""
    
    # Adapt based on query pattern
    if query_pattern == "optimized_diagnosis_first":
        # Replace 'diagnoses.' with 'd.' and 'diagnoses IS NOT NULL' with 'd IS NOT NULL'
        clause = clause.replace("diagnoses.", "d.")
        clause = clause.replace("diagnoses IS NOT NULL", "d IS NOT NULL")
        
        # IMPORTANT: Wrap participant filters (p.*) with NULL check
        # In optimized query, p comes from OPTIONAL MATCH, so it can be NULL
        # We need to ensure p IS NOT NULL before referencing p.*
        import re
        # Check if clause contains participant filters (p.sex, p.race, etc.)
        if re.search(r'\bp\.\w+', clause):
            # Split clause into parts to identify participant filters
            # Wrap participant filter conditions with (p IS NOT NULL AND ...)
            # Pattern: find conditions that reference p.*
            participant_pattern = r'\bp\.\w+'
            if re.search(participant_pattern, clause):
                # Check if clause already has p IS NOT NULL check
                if 'p IS NOT NULL' not in clause:
                    # Wrap the entire clause with p IS NOT NULL check if it contains p.*
                    # But be careful - we might have mixed conditions (p.* and non-p.*)
                    # For now, wrap the whole clause if it contains any p.* references
                    clause = f"(p IS NOT NULL AND ({clause}))"
    
    # Add WHERE prefix back
    if clause:
        return f"WHERE {clause}"
    
    return ""


def adapt_diagnosis_condition_for_query_pattern(
    diagnosis_condition: str,
    query_pattern: str
) -> str:
    """
    Adapt a diagnosis condition to match the variable naming convention.
    
    Args:
        diagnosis_condition: The original condition (e.g., "diagnoses.diagnosis = $param")
        query_pattern: One of 'optimized_diagnosis_first', 'standard_query'
    
    Returns:
        Adapted condition with correct variable names
    
    Examples:
        >>> adapt_diagnosis_condition_for_query_pattern(
        ...     "(diagnoses IS NOT NULL AND diagnoses.diagnosis = $param)",
        ...     "optimized_diagnosis_first"
        ... )
        "(d IS NOT NULL AND d.diagnosis = $param)"
    """
    if not diagnosis_condition:
        return ""
    
    if query_pattern == "optimized_diagnosis_first":
        # Remove "diagnoses IS NOT NULL AND" prefix and replace "diagnoses." with "d."
        condition = diagnosis_condition.replace("diagnoses IS NOT NULL AND", "").strip()
        condition = condition.replace("diagnoses.", "d.").strip()
        
        # Remove outer parentheses if present
        if condition.startswith("(") and condition.endswith(")"):
            condition = condition[1:-1].strip()
        
        return condition
    
    # For standard queries, return as-is (uses 'diagnoses' from head(collect(...)))
    return diagnosis_condition


def check_cartesian_product_risk(
    query_text: str,
    variable_name: str,
    existing_variables: list
) -> bool:
    """
    Check if a query has cartesian product risk.
    
    Args:
        query_text: The Cypher query text
        variable_name: The variable name to check (e.g., 'p2')
        existing_variables: List of existing variable names (e.g., ['p'])
    
    Returns:
        True if there's a cartesian product risk
    
    Examples:
        >>> check_cartesian_product_risk(
        ...     "OPTIONAL MATCH (p2:participant)...",
        ...     "p2",
        ...     ["p"]
        ... )
        True
    """
    # Check if variable_name is used when a similar variable exists
    if variable_name in existing_variables:
        return False
    
    # Check for patterns like p2 when p exists
    base_name = variable_name.rstrip('0123456789')
    if base_name in existing_variables:
        # Check if variable_name is used in OPTIONAL MATCH
        if f"({variable_name}" in query_text or f"{variable_name}:" in query_text:
            return True
    
    return False


def normalize_variable_names_in_query(
    query_text: str,
    query_pattern: str
) -> str:
    """
    Normalize variable names in a Cypher query based on the query pattern.
    
    This is a helper for debugging - it doesn't modify the original query,
    but can be used to generate corrected versions.
    
    Args:
        query_text: The original Cypher query
        query_pattern: One of 'optimized_diagnosis_first', 'standard_query'
    
    Returns:
        Query with normalized variable names
    """
    if query_pattern == "optimized_diagnosis_first":
        # Replace 'diagnoses.' with 'd.' throughout
        query_text = query_text.replace("diagnoses.", "d.")
        query_text = query_text.replace("diagnoses IS NOT NULL", "d IS NOT NULL")
    
    return query_text

