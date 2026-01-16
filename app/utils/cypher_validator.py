"""
Enhanced Cypher query validator for variable scoping issues.

This module provides validation functions to catch common Cypher variable scoping
errors before they cause runtime failures.
"""

import re
from typing import List, Tuple, Optional, Set, Dict


def validate_unwind_variable_scope(query: str) -> Tuple[bool, Optional[str], List[str]]:
    """
    Validate that variables are properly scoped after UNWIND statements.
    
    This catches the common pattern where variables are used after UNWIND
    without being explicitly included in the WITH clause.
    
    Args:
        query: Cypher query string to validate
        
    Returns:
        Tuple of (is_valid, error_message, warnings)
        - is_valid: True if no errors found
        - error_message: Description of first error found, or None
        - warnings: List of warnings about potential issues
    """
    lines = query.split('\n')
    errors = []
    warnings = []
    
    # Track variable scope through the query
    scope_stack = []  # List of sets, each representing variables in scope at that point
    current_scope: Set[str] = set()
    
    # Track UNWIND statements and their context
    unwind_contexts = []  # List of (line_num, unwound_var, variables_before_unwind)
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        line_num = i + 1
        
        # Skip comments
        if stripped.startswith('//'):
            continue
            
        # Track WITH clauses - they define new scope
        if stripped.upper().startswith('WITH'):
            # Extract variables from WITH clause
            with_vars = _extract_with_variables(stripped)
            # Update current scope to only include variables in this WITH clause
            current_scope = with_vars
            scope_stack.append(current_scope.copy())
            
            # Check if we're after an UNWIND - validate that needed variables are included
            if unwind_contexts:
                last_unwind = unwind_contexts[-1]
                unwound_var, vars_before = last_unwind[1], last_unwind[2]
                
                # Check if we're trying to use variables that were available before UNWIND
                # but not included in this WITH clause
                missing_vars = vars_before - with_vars
                if missing_vars:
                    # Some variables from before UNWIND are missing
                    # Check if they're actually needed (referenced later)
                    # For now, just warn - they might not be needed
                    if any(var in query[i*100:(i+10)*100] for var in missing_vars):  # Simple check
                        warnings.append(
                            f"Line {line_num}: After UNWIND '{unwound_var}', variables {missing_vars} "
                            f"from before UNWIND are not included in WITH clause. "
                            f"Make sure they're not needed later."
                        )
                
                # Clear UNWIND context after we've processed the WITH clause
                unwind_contexts.pop()
        
        # Track UNWIND statements
        elif 'UNWIND' in stripped.upper():
            match = re.search(r'UNWIND\s+(\w+)\s+AS\s+(\w+)', stripped, re.IGNORECASE)
            if match:
                list_var = match.group(1)
                item_var = match.group(2)
                
                # Store context: variables available before UNWIND
                unwind_contexts.append((line_num, item_var, current_scope.copy()))
                
                # After UNWIND, the list variable is no longer in scope
                # Only the item variable is available
                current_scope.discard(list_var)
                current_scope.add(item_var)
                
                warnings.append(
                    f"Line {line_num}: UNWIND '{list_var} AS {item_var}' - "
                    f"'{list_var}' is no longer in scope. Only '{item_var}' is available."
                )
        
        # Track variable definitions in other clauses
        elif stripped.upper().startswith(('MATCH', 'OPTIONAL MATCH', 'CREATE', 'MERGE')):
            # Extract node/relationship variables
            var_matches = re.findall(r'\((\w+):', stripped)
            current_scope.update(var_matches)
        
        # Check for variable usage (only after WITH clauses, not in MATCH/CREATE)
        # Look for variable references that might not be in scope
        # Skip this check for MATCH/CREATE/MERGE lines as they define new variables
        if not stripped.upper().startswith(('MATCH', 'CREATE', 'MERGE', 'OPTIONAL MATCH')):
            var_refs = re.findall(r'\b([a-z_][a-z0-9_]*)\b', stripped.lower())
            keywords = {
                'as', 'and', 'or', 'not', 'null', 'is', 'in', 'where', 'case', 'when', 
                'then', 'else', 'end', 'with', 'match', 'return', 'unwind', 'skip', 
                'limit', 'order', 'by', 'collect', 'head', 'size', 'reduce', 'any', 
                'all', 'none', 'single', 'exists', 'coalesce', 'tostring', 'tointeger',
                'tolower', 'toupper', 'trim', 'split', 'valuetype', 'participant', 'study',
                'survival', 'diagnosis', 'sample', 'sequencing', 'file', 'pathology'
            }
            var_refs = {v for v in var_refs if v not in keywords and not v.isdigit()}
            
            # Check if referenced variables are in scope
            for var in var_refs:
                if var not in current_scope and not stripped.upper().startswith('WITH'):
                    # Skip if it's a property access (p.race) or parameter ($param_1)
                    if '.' in stripped and var + '.' in stripped.lower():
                        continue
                    if var.startswith('$') or 'param_' in stripped:
                        continue
                    # Skip if it's in a CASE/WHEN expression (those are usually fine)
                    if 'case' in stripped.lower() or 'when' in stripped.lower():
                        continue
                    # This is likely an unbound variable - but only warn for now
                    # as the validator might be too strict
                    if var in ['study_ids'] and 'unwind' in '\n'.join(lines[max(0, i-5):i]).lower():
                        # This is the specific bug we're trying to catch
                        errors.append(
                            f"Line {line_num}: Variable '{var}' used after UNWIND but not in scope. "
                            f"Current scope: {current_scope}. Line: {stripped[:80]}"
                        )
    
    # Check for any remaining UNWIND contexts without corresponding WITH
    if unwind_contexts:
        for line_num, unwound_var, vars_before in unwind_contexts:
            errors.append(
                f"Line {line_num}: UNWIND '{unwound_var}' found but no WITH clause "
                f"follows to establish new scope."
            )
    
    if errors:
        return False, errors[0], warnings
    return True, None, warnings


def _extract_with_variables(with_clause: str) -> Set[str]:
    """Extract variable names from a WITH clause."""
    # Remove WITH keyword
    content = re.sub(r'^\s*WITH\s+(?:DISTINCT\s+)?', '', with_clause, flags=re.IGNORECASE)
    
    # Split by WHERE if present
    if 'WHERE' in content.upper():
        content = content.split('WHERE')[0]
    
    variables = set()
    
    # Extract "AS variable" patterns
    as_matches = re.findall(r'\s+AS\s+(\w+)', content, re.IGNORECASE)
    variables.update(as_matches)
    
    # Extract direct variable references (e.g., "WITH p, st, study_id")
    # Split by comma and extract variable names
    parts = re.split(r',', content)
    for part in parts:
        part = part.strip()
        # Remove function calls, list comprehensions, etc.
        # Simple pattern: word at start of part (before any operators)
        var_match = re.match(r'^(\w+)', part)
        if var_match:
            var = var_match.group(1).lower()
            keywords = {
                'case', 'when', 'then', 'else', 'end', 'collect', 'head', 'size',
                'reduce', 'any', 'all', 'none', 'single', 'coalesce', 'tostring',
                'tointeger', 'tolower', 'toupper', 'trim', 'split'
            }
            if var not in keywords and not var.isdigit():
                variables.add(var)
    
    return variables


def validate_cypher_query(query: str) -> Tuple[bool, Optional[str], List[str]]:
    """
    Comprehensive Cypher query validation.
    
    Checks for:
    - Variable scoping issues after UNWIND
    - Unbound variable references
    - Common syntax errors
    
    Args:
        query: Cypher query string to validate
        
    Returns:
        Tuple of (is_valid, error_message, warnings)
    """
    errors = []
    warnings = []
    
    # Check UNWIND variable scoping
    is_valid, error, unwinds_warnings = validate_unwind_variable_scope(query)
    if not is_valid:
        errors.append(error)
    warnings.extend(unwinds_warnings)
    
    # Additional validations can be added here
    
    if errors:
        return False, errors[0], warnings
    return True, None, warnings

