"""
Utility functions for safely building Cypher queries.

This module provides helper functions to prevent common Cypher syntax errors,
particularly around WHERE clause construction and combination, and variable scope management.
"""

from typing import List, Optional, Set, Dict, Any


class CypherWhereBuilder:
    """
    Builder class for safely constructing WHERE clauses in Cypher queries.
    
    Prevents common errors like:
    - Duplicate WHERE clauses
    - WHERE clauses in wrong positions
    - Empty WHERE clauses
    """
    
    def __init__(self):
        self.conditions: List[str] = []
    
    def add(self, condition: str) -> 'CypherWhereBuilder':
        """
        Add a condition to the WHERE clause.
        
        Args:
            condition: A Cypher condition string (without WHERE keyword)
            
        Returns:
            Self for method chaining
        """
        if condition and condition.strip():
            self.conditions.append(condition.strip())
        return self
    
    def add_multiple(self, conditions: List[str]) -> 'CypherWhereBuilder':
        """
        Add multiple conditions at once.
        
        Args:
            conditions: List of Cypher condition strings
            
        Returns:
            Self for method chaining
        """
        for condition in conditions:
            self.add(condition)
        return self
    
    def build(self) -> str:
        """
        Build the WHERE clause string.
        
        Returns:
            Empty string if no conditions, otherwise "WHERE condition1 AND condition2 ..."
        """
        if not self.conditions:
            return ""
        
        filtered = [c for c in self.conditions if c and c.strip()]
        if not filtered:
            return ""
        
        return "WHERE " + " AND ".join(filtered)
    
    def build_conditions_only(self) -> str:
        """
        Build just the conditions without the WHERE keyword.
        Useful for combining with existing WHERE clauses.
        
        Returns:
            Empty string if no conditions, otherwise "condition1 AND condition2 ..."
        """
        if not self.conditions:
            return ""
        
        filtered = [c for c in self.conditions if c and c.strip()]
        if not filtered:
            return ""
        
        return " AND ".join(filtered)
    
    def is_empty(self) -> bool:
        """Check if the builder has any conditions."""
        return len(self.conditions) == 0


def combine_where_clauses(*where_clauses: str) -> str:
    """
    Safely combine multiple WHERE clauses.
    
    Handles cases where:
    - Some clauses already have "WHERE" prefix
    - Some clauses are empty
    - Need to combine with AND
    
    Args:
        *where_clauses: Variable number of WHERE clause strings
        
    Returns:
        Combined WHERE clause string, or empty string if all are empty
        
    Examples:
        >>> combine_where_clauses("WHERE a = 1", "b = 2")
        'WHERE a = 1 AND b = 2'
        
        >>> combine_where_clauses("WHERE a = 1", "")
        'WHERE a = 1'
        
        >>> combine_where_clauses("", "")
        ''
    """
    # Extract conditions from each clause
    conditions: List[str] = []
    
    for clause in where_clauses:
        if not clause or not clause.strip():
            continue
        
        clause = clause.strip()
        
        # Remove WHERE prefix if present
        if clause.upper().startswith("WHERE"):
            clause = clause[5:].strip()
        
        # Skip if empty after removing WHERE
        if not clause:
            continue
        
        # Split by AND to handle multiple conditions in one clause
        # But be careful - we don't want to split AND inside function calls
        # Simple approach: split by " AND " (with spaces)
        parts = [p.strip() for p in clause.split(" AND ") if p.strip()]
        conditions.extend(parts)
    
    if not conditions:
        return ""
    
    return "WHERE " + " AND ".join(conditions)


def append_where_conditions(existing_where: str, *new_conditions: str) -> str:
    """
    Append new conditions to an existing WHERE clause.
    
    Args:
        existing_where: Existing WHERE clause (may be empty or include WHERE keyword)
        *new_conditions: New conditions to add (without WHERE keyword)
        
    Returns:
        Combined WHERE clause
        
    Examples:
        >>> append_where_conditions("WHERE a = 1", "b = 2", "c = 3")
        'WHERE a = 1 AND b = 2 AND c = 3'
        
        >>> append_where_conditions("", "a = 1")
        'WHERE a = 1'
    """
    # Extract conditions from existing WHERE
    existing_conditions: List[str] = []
    
    if existing_where and existing_where.strip():
        existing = existing_where.strip()
        if existing.upper().startswith("WHERE"):
            existing = existing[5:].strip()
        if existing:
            existing_conditions.extend([c.strip() for c in existing.split(" AND ") if c.strip()])
    
    # Add new conditions
    new_filtered = [c.strip() for c in new_conditions if c and c.strip()]
    existing_conditions.extend(new_filtered)
    
    if not existing_conditions:
        return ""
    
    return "WHERE " + " AND ".join(existing_conditions)


def build_where_clause(conditions: List[str]) -> str:
    """
    Build a WHERE clause from a list of conditions.
    
    Args:
        conditions: List of condition strings (without WHERE keyword)
        
    Returns:
        WHERE clause string, or empty string if no conditions
        
    Examples:
        >>> build_where_clause(["a = 1", "b = 2"])
        'WHERE a = 1 AND b = 2'
        
        >>> build_where_clause([])
        ''
    """
    filtered = [c.strip() for c in conditions if c and c.strip()]
    if not filtered:
        return ""
    return "WHERE " + " AND ".join(filtered)


def validate_where_placement(query: str) -> tuple[bool, Optional[str]]:
    """
    Validate that WHERE clauses are properly placed in a Cypher query.
    
    Checks for common errors:
    - Multiple WHERE clauses in a row
    - WHERE after WITH without proper structure
    
    Args:
        query: Cypher query string to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    lines = query.split('\n')
    in_where = False
    
    for i, line in enumerate(lines):
        stripped = line.strip().upper()
        
        # Check for WHERE keyword
        if 'WHERE' in stripped:
            # Check if this is a new WHERE clause
            if in_where and not stripped.startswith('AND'):
                # Check if previous line ended properly
                prev_line = lines[i-1].strip() if i > 0 else ""
                if prev_line and not prev_line.endswith(('WITH', 'MATCH', 'OPTIONAL MATCH', 'CALL', 'UNION')):
                    return False, f"Potential duplicate WHERE clause at line {i+1}: {line.strip()}"
            in_where = True
        elif stripped.startswith(('WITH', 'MATCH', 'OPTIONAL MATCH', 'RETURN', 'CALL', 'UNION')):
            in_where = False
    
    return True, None


def ensure_study_id_in_with(
    with_clause: str, 
    study_var: str = "st",
    output_var: str = "study_id",
    entity_type: str = "subject"
) -> str:
    """
    Ensure that study_id is included in a WITH clause.
    
    This helps prevent "Unbound variable: study_id" errors by ensuring
    that study_id is always included when needed.
    
    Supports different entity types:
    - subject: Uses st.study_id AS study_id (from participant->consent_group->study)
    - sample: Uses st.study_id AS study_id (from sample->IN_STUDY->study)
    - file: Uses st.study_id AS study_id_val (from coalesce(st1, st2))
    
    Args:
        with_clause: The WITH clause string (may or may not include WITH keyword)
        study_var: The variable name for the study node (default: "st")
        output_var: The output variable name (default: "study_id", use "study_id_val" for files)
        entity_type: Entity type - "subject", "sample", or "file" (default: "subject")
        
    Returns:
        Updated WITH clause with study_id included if not already present
        
    Examples:
        >>> ensure_study_id_in_with("WITH DISTINCT p.participant_id AS participant_id, p")
        'WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id'
        
        >>> ensure_study_id_in_with("WITH participant_id, p, study_id")  # Already has it
        'WITH participant_id, p, study_id'
        
        >>> ensure_study_id_in_with("WITH sf", entity_type="file", output_var="study_id_val")
        'WITH sf, st.study_id AS study_id_val'
    """
    # Check if output variable is already in the clause
    if output_var in with_clause:
        return with_clause
    
    # For files, check if study_id_val is already present
    if entity_type == "file" and "study_id_val" in with_clause:
        return with_clause
    
    # Remove WITH keyword if present to work with the content
    has_with_keyword = with_clause.strip().upper().startswith("WITH")
    content = with_clause[4:].strip() if has_with_keyword else with_clause.strip()
    
    # Determine the study variable pattern based on entity type
    if entity_type == "file":
        # Files use coalesce(st1, st2) AS st, so check for st variable
        study_expr = f"{study_var}.study_id AS {output_var}"
    else:
        # Subjects and samples use st directly
        study_expr = f"{study_var}.study_id AS {output_var}"
    
    # Add study_id
    if content:
        # Check if there's a comma at the end or if we need to add one
        if not content.rstrip().endswith(","):
            content += ", "
        content += study_expr
    else:
        content = study_expr
    
    # Reconstruct WITH clause
    if has_with_keyword:
        return f"WITH {content}"
    else:
        return content


def build_with_clause(
    variables: List[str],
    include_study_id: bool = True,
    distinct: bool = False,
    study_var: str = "st",
    entity_type: str = "subject",
    output_var: str = "study_id"
) -> str:
    """
    Build a WITH clause safely, ensuring study_id is included when needed.
    
    Supports different entity types:
    - subject: Uses st.study_id AS study_id
    - sample: Uses st.study_id AS study_id  
    - file: Uses st.study_id AS study_id_val
    
    Args:
        variables: List of variable expressions (e.g., ["p.participant_id AS participant_id", "p"])
        include_study_id: Whether to include study_id (default: True)
        distinct: Whether to use DISTINCT (default: False)
        study_var: The variable name for the study node (default: "st")
        entity_type: Entity type - "subject", "sample", or "file" (default: "subject")
        output_var: The output variable name (default: "study_id", use "study_id_val" for files)
        
    Returns:
        Complete WITH clause string
        
    Examples:
        >>> build_with_clause(["p.participant_id AS participant_id", "p"])
        'WITH p.participant_id AS participant_id, p, st.study_id AS study_id'
        
        >>> build_with_clause(["participant_id", "p"], include_study_id=False)
        'WITH participant_id, p'
        
        >>> build_with_clause(["sf"], entity_type="file", output_var="study_id_val")
        'WITH sf, st.study_id AS study_id_val'
    """
    parts = variables.copy()
    
    if include_study_id and output_var not in " ".join(parts):
        # For files, check if study_id_val is already present
        if entity_type == "file" and "study_id_val" not in " ".join(parts):
            parts.append(f"{study_var}.study_id AS {output_var}")
        elif entity_type != "file":
            parts.append(f"{study_var}.study_id AS {output_var}")
    
    distinct_str = "DISTINCT " if distinct else ""
    return f"WITH {distinct_str}{', '.join(parts)}"


class CypherQueryBuilder:
    """
    Builder class for constructing Cypher queries with automatic variable tracking.
    
    Automatically ensures required variables (like study_id) are included in WITH clauses.
    Supports different entity types: subject, sample, and file.
    """
    
    def __init__(
        self, 
        auto_include_study_id: bool = True, 
        study_var: str = "st",
        entity_type: str = "subject",
        output_var: str = "study_id"
    ):
        """
        Initialize the query builder.
        
        Args:
            auto_include_study_id: Automatically include study_id in WITH clauses (default: True)
            study_var: Variable name for study node (default: "st")
            entity_type: Entity type - "subject", "sample", or "file" (default: "subject")
            output_var: Output variable name for study_id (default: "study_id", use "study_id_val" for files)
        """
        self.auto_include_study_id = auto_include_study_id
        self.study_var = study_var
        self.entity_type = entity_type
        self.output_var = output_var
        self.query_parts: List[str] = []
        self.current_vars: Set[str] = set()
        self.required_vars: Set[str] = {output_var} if auto_include_study_id else set()
    
    def match(self, pattern: str) -> 'CypherQueryBuilder':
        """Add a MATCH clause."""
        self.query_parts.append(f"MATCH {pattern}")
        return self
    
    def optional_match(self, pattern: str) -> 'CypherQueryBuilder':
        """Add an OPTIONAL MATCH clause."""
        self.query_parts.append(f"OPTIONAL MATCH {pattern}")
        return self
    
    def with_clause(
        self,
        variables: List[str],
        distinct: bool = False,
        where_conditions: Optional[List[str]] = None,
        force_include_study_id: Optional[bool] = None
    ) -> 'CypherQueryBuilder':
        """
        Add a WITH clause, automatically including study_id if required.
        
        Args:
            variables: List of variable expressions
            distinct: Use DISTINCT keyword
            where_conditions: Optional WHERE conditions to append
            force_include_study_id: Force include study_id even if not auto-enabled (None = use auto_include_study_id)
        """
        # Ensure study_id is included if required
        vars_list = variables.copy()
        should_include = force_include_study_id if force_include_study_id is not None else self.auto_include_study_id
        
        if should_include and self.output_var not in " ".join(vars_list):
            # If output_var is already in scope from a previous WITH clause, just include it
            if self.output_var in self.current_vars:
                vars_list.append(self.output_var)
            else:
                # Check if study_var was matched in previous clauses (check query parts)
                study_var_available = False
                
                # Check if st is in any previous MATCH/OPTIONAL MATCH
                for part in self.query_parts:
                    if "MATCH" in part.upper() and f"({self.study_var}:" in part:
                        study_var_available = True
                        break
                
                # Also check if st is in current variables (might be passed directly)
                if not study_var_available:
                    study_var_available = any(
                        self.study_var in var or f"{self.study_var}." in var 
                        for var in vars_list
                    )
                
                if study_var_available:
                    vars_list.append(f"{self.study_var}.study_id AS {self.output_var}")
                elif should_include:
                    # If auto-include is enabled but st not available, include it anyway
                    # (it will be NULL if not matched, which is OK for OPTIONAL MATCH)
                    vars_list.append(f"{self.study_var}.study_id AS {self.output_var}")
        
        distinct_str = "DISTINCT " if distinct else ""
        with_part = f"WITH {distinct_str}{', '.join(vars_list)}"
        
        if where_conditions:
            with_part += f"\n        WHERE {' AND '.join(where_conditions)}"
        
        self.query_parts.append(with_part)
        
        # Update current variables
        for v in vars_list:
            if " AS " in v:
                self.current_vars.add(v.split(" AS ")[-1].strip())
            else:
                # Extract variable name (e.g., "p.participant_id" -> "participant_id", "p" -> "p")
                parts = v.split(".")
                self.current_vars.add(parts[-1].strip())
        
        return self
    
    def where(self, conditions: List[str]) -> 'CypherQueryBuilder':
        """Add a WHERE clause."""
        if conditions:
            self.query_parts.append(f"WHERE {' AND '.join(conditions)}")
        return self
    
    def return_clause(self, expressions: List[str], order_by: Optional[str] = None, skip: Optional[int] = None, limit: Optional[int] = None) -> 'CypherQueryBuilder':
        """Add a RETURN clause."""
        return_part = f"RETURN {', '.join(expressions)}"
        if order_by:
            return_part += f"\n        ORDER BY {order_by}"
        if skip is not None:
            return_part += f"\n        SKIP {skip}"
        if limit is not None:
            return_part += f"\n        LIMIT {limit}"
        self.query_parts.append(return_part)
        return self
    
    def build(self) -> str:
        """Build the complete query string."""
        return "\n        ".join(self.query_parts)
    
    def reset(self) -> 'CypherQueryBuilder':
        """Reset the builder to start a new query."""
        self.query_parts = []
        self.current_vars = set()
        return self


def validate_variable_scope(query: str, required_vars: List[str]) -> tuple[bool, Optional[str]]:
    """
    Validate that required variables are in scope before being used.
    
    This is a simple check that looks for variable usage without prior definition.
    Note: This is a basic check and may not catch all cases.
    
    Args:
        query: Cypher query string to validate
        required_vars: List of variable names that must be defined before use
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    lines = query.split('\n')
    defined_vars = set()
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Extract variables defined in WITH clauses
        if stripped.upper().startswith("WITH"):
            # Simple extraction - look for "AS variable_name" patterns
            import re
            as_pattern = r'\s+AS\s+(\w+)'
            matches = re.findall(as_pattern, stripped, re.IGNORECASE)
            defined_vars.update(matches)
            
            # Also check for direct variable references (e.g., "WITH p, st")
            var_pattern = r'\b([a-z_][a-z0-9_]*)\b'
            # Get variables after WITH/DISTINCT
            after_with = re.sub(r'^\s*WITH\s+(?:DISTINCT\s+)?', '', stripped, flags=re.IGNORECASE)
            direct_vars = re.findall(var_pattern, after_with.split("WHERE")[0] if "WHERE" in after_with else after_with)
            # Filter out keywords
            keywords = {"AS", "AND", "OR", "NOT", "NULL", "IS", "IN", "WHERE", "CASE", "WHEN", "THEN", "ELSE", "END"}
            direct_vars = [v for v in direct_vars if v.lower() not in keywords and not v.isdigit()]
            defined_vars.update(direct_vars)
        
        # Check for usage of required variables
        for var in required_vars:
            if var in stripped and var not in defined_vars:
                # Check if it's actually being used (not just in a comment or string)
                if not stripped.startswith("//") and f"AS {var}" not in stripped:
                    # Check if it's in a WITH clause definition (which is OK)
                    if not (stripped.upper().startswith("WITH") and f"AS {var}" in stripped):
                        return False, f"Variable '{var}' used at line {i+1} before being defined: {line.strip()}"
    
    return True, None

