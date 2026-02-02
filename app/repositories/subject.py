"""
Subject repository for the CCDI Federation Service.

This module provides data access operations for subjects
using Cypher queries to Memgraph.
"""

import asyncio
import re
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.core.constants import Race
from app.core.field_mappings import map_field_value, reverse_map_field_value, is_database_only_value
from app.lib.field_allowlist import FieldAllowlist
from app.lib.url_builder import build_identifier_server_url
from app.models.dto import Subject
from app.models.errors import UnsupportedFieldError
from app.utils.cypher_builder import combine_where_clauses, append_where_conditions

logger = get_logger(__name__)


class SubjectRepository:
    """Repository for subject data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings=None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings

    @staticmethod
    def _split_or_values(value: Any) -> Optional[List[str]]:
        """
        Normalize `||`-separated query params into a clean list of strings.

        Examples:
        - "a||b" -> ["a","b"]
        - "a" -> ["a"]
        - ["a", "b"] -> ["a", "b"]  # Already a list, return as-is
        - None / "" -> None
        """
        if value is None:
            return None
        # If value is already a list, return it as-is (don't convert to string)
        if isinstance(value, list):
            # Filter out empty strings and return
            items = [str(v).strip() for v in value if v]
            return items or None
        s = str(value).strip()
        if not s:
            return None
        if "||" in s:
            items = [v.strip() for v in s.split("||")]
            items = [v for v in items if v]
            return items or None
        return [s]
    
    @staticmethod
    def _build_combined_where_clause_for_depositions_path(diagnosis_search_term: Optional[str], dep_param: Optional[str], deposition_operator: Optional[str]) -> str:
        """Build a combined WHERE clause for diagnosis search and depositions filter.
        
        When both conditions are present, combine them with AND in a single WHERE clause.
        """
        conditions = []
        if diagnosis_search_term:
            conditions.append("size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0")
        if dep_param:
            conditions.append(f"size([sid IN study_ids WHERE sid IS NOT NULL AND sid {deposition_operator} ${dep_param}]) > 0")
        
        if conditions:
            return f"WHERE {' AND '.join(conditions)}"
        return ""
        
    async def get_subjects(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None
    ) -> List[Subject]:
        """
        Get paginated list of subjects with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of Subject objects
            
        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching subjects",
            filters=filters,
            offset=offset,
            limit=limit
        )

        # IMPORTANT: avoid mutating the caller's dict (endpoints/services may reuse it)
        filters = dict(filters or {})

        # Initialize cypher to avoid "variable not associated with value" error
        cypher = None
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        
        # Handle race parameter normalization
        # Race filter must be applied after WITH clause defines variables
        race_condition = ""
        race_filter_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                # Normalize race value to a list (handle both string and list inputs)
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []
                
                if race_list:
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    
                    # Check if "Not Reported" is in the filter - if so, also match "Hispanic or Latino" only records
                    includes_not_reported = any(r.strip() == "Not Reported" for r in race_list)
                    
                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    
                    if includes_not_reported:
                        # Match either: "Not Reported" in original values OR "Hispanic or Latino" only (which converts to "Not Reported")
                        race_filter_condition = """(reduce(found = false, tok IN race_tokens | found OR tok IN pr_tokens) OR 
                        (size(pr_tokens) > 0 AND reduce(all_hispanic = true, pt IN pr_tokens | all_hispanic AND pt = 'Hispanic or Latino') AND 'Not Reported' IN race_tokens))"""
                    else:
                        # Normal matching - exclude "Hispanic or Latino" from matching since it's removed in conversion
                        # Check if any race_token matches a pr_token that is not 'Hispanic or Latino'
                        # Use a simpler approach: check each token individually
                        race_filter_condition = "reduce(found = false, tok IN race_tokens | found OR (tok IN pr_tokens AND tok <> 'Hispanic or Latino'))"
        
        # Handle identifiers parameter normalization
        # Support || separator for OR logic (e.g., "SUBJ001 || SUBJ002")
        # OPTIMIZATION: Apply identifiers filter EARLY in MATCH WHERE clause to reduce dataset before OPTIONAL MATCHes
        identifiers_condition = ""
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            identifiers_list = self._split_or_values(identifiers_value)
            if identifiers_list:
                # Preserve legacy behavior: single value kept as string in params for cypher valueType branching
                identifiers_value = identifiers_list[0] if len(identifiers_list) == 1 else identifiers_list
                if identifiers_value:
                    param_counter += 1
                    id_param = f"param_{param_counter}"
                    params[id_param] = identifiers_value
                    identifiers_condition = f""",
                    // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                    CASE
                      WHEN ${id_param} IS NULL THEN NULL
                      WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                      WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                      ELSE []
                    END AS id_list"""
                    where_conditions.append("p.participant_id IN id_list")
                    # Set early filter for optimization (can be used in MATCH WHERE clause)
                    # For early filter, we need to handle both LIST and STRING cases
                    if isinstance(identifiers_value, list):
                        identifiers_early_filter = f"p.participant_id IN ${id_param}"
                    else:
                        identifiers_early_filter = f"p.participant_id = ${id_param}"
        
        # Handle depositions filter (study_id)
        # Support || separator for OR logic (e.g., "phs001 || phs002")
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            depositions_list = self._split_or_values(depositions_value)
            logger.debug(f"Depositions filter provided: {depositions_value}, parsed list: {depositions_list}")
            if depositions_list:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    # Filter by study_id - participants must belong to the specified study
                    if len(depositions_list) == 1:
                        params[dep_param] = depositions_list[0]
                        deposition_operator = "="
                    else:
                        params[dep_param] = depositions_list
                        deposition_operator = "IN"
                    logger.debug(f"Depositions filter: param={dep_param}, operator={deposition_operator}, value={params[dep_param]}")
                    # IMPORTANT:
                    # Do NOT append any `st.study_id ...` condition into the generic `where_conditions` list.
                    # `st` is only bound in query shapes that explicitly match participant -> consent_group -> study.
                    # The depositions filter must be applied only in those query blocks (after `st` is in scope),
                    # otherwise we get recurring "Unbound variable: st." / "mismatched input" errors.
            else:
                logger.debug("Depositions filter provided but depositions_list is empty/None")
        else:
            logger.debug("No depositions filter in filters dict")
        
        # Handle diagnosis search - will be applied after diagnosis collection
        diagnosis_search_term = None
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            params["diagnosis_search_term"] = diagnosis_search_term
            # Don't add to where_conditions yet - will be applied after diagnosis collection
        
        # Separate derived fields (calculated after WITH clauses) from direct participant fields
        derived_filters = {}
        derived_conditions = []
        
        # Map API sex values to database values (M -> Male, F -> Female, U -> Not Reported)
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            # If the value is a normalized API value, map it to database value
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]
        
        # Field name mapping for participant properties
        # Some API field names don't match the database property names
        field_name_mapping = {
            "sex": "sex_at_birth",
            # Add other mappings as needed
        }
        
        # OPTIMIZATION: Separate early filters (can be applied in MATCH WHERE) from late filters (need WITH clause)
        # Early filters: simple participant properties (sex, etc.) - can filter before OPTIONAL MATCH
        # Late filters: race (needs tokenization), derived fields (need calculation)
        early_participant_filters = []
        late_participant_filters = []
        
        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            # Ethnicity is derived from race but can be expressed safely as a predicate on p.race.
            # IMPORTANT: Do not treat it as a derived-field filter here; otherwise fast paths will skip it.
            if field == "ethnicity":
                desired = str(value).strip() if value is not None else ""
                desired_lower = desired.lower()
                param_counter += 1
                param_name = f"param_{param_counter}"
                # Use a single param for the Hispanic marker string
                params[param_name] = "Hispanic or Latino"
                if desired_lower == "hispanic or latino":
                    condition = f"(p.race IS NOT NULL AND toString(p.race) CONTAINS ${param_name})"
                else:
                    # "Not reported" means anything that does NOT contain Hispanic-or-Latino, including missing/empty race
                    condition = f"(p.race IS NULL OR trim(toString(p.race)) = '' OR NOT toString(p.race) CONTAINS ${param_name})"
                # This is a simple participant predicate, so apply early
                early_participant_filters.append(condition)
                continue

            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status"}:
                derived_filters[field] = value
                continue
            
            # Map API field name to database property name
            db_field = field_name_mapping.get(field, field)
                
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            condition = f"p.{db_field} IN ${param_name}" if isinstance(value, list) else f"p.{db_field} = ${param_name}"
            params[param_name] = value
            
            # Check if this is a simple participant property filter (can be applied early)
            # Common early filters: sex_at_birth (sex), and other simple participant properties
            # Race filters are handled separately, so they're not included here
            if field == "sex" or db_field == "sex_at_birth":
                # Simple participant property - can filter early (before OPTIONAL MATCH)
                early_participant_filters.append(condition)
            else:
                # Other filters - apply after WITH clause
                late_participant_filters.append(condition)
                where_conditions.append(condition)  # Keep for backward compatibility with existing code
        
        # Build WHERE clause for filters that need to be applied after WITH clause
        where_clause = ""
        if late_participant_filters:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in late_participant_filters if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)
        elif where_conditions:
            # Fallback: if early_participant_filters logic wasn't used, use original where_conditions
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)
        
        # Note: race_filter_condition is NOT added to where_clause here
        # It will be added separately to with_where_conditions where race_tokens is in scope
        # (race_tokens is defined in the WITH clause via race_condition)
        
        # Build WHERE clause for derived fields (after calculation)
        if derived_filters:
            for field, value in derived_filters.items():
                param_counter += 1
                param_name = f"derived_{field}_{param_counter}"
                if field == "vital_status":
                    # Check if value is a database-only value (e.g., "Not Reported" with capital R)
                    if is_database_only_value("vital_status", value):
                        # This value is a database-only value and is not valid for filtering
                        # Add impossible condition to return empty results
                        derived_conditions.append("false")
                    else:
                        # Case-insensitive match against DB values (do NOT match NULL/missing).
                        # This ensures we only count explicit "Not Reported" values, not missing data.
                        derived_conditions.append(
                            f"(final_vital_status IS NOT NULL AND toLower(toString(final_vital_status)) = toLower(toString(${param_name})))"
                        )
                        # Apply reverse mapping for vital_status filter (e.g., "Not reported" -> "Not Reported")
                        value = reverse_map_field_value("vital_status", value) if value else value
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                    # Convert age to integer since it's stored as int in database
                    try:
                        value = int(value) if value is not None else value
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
            # Also filter out any conditions that reference study_id (not defined yet in non-depositions path)
            filtered_derived = [c for c in filtered_derived if 'study_id' not in c.lower()]
            if filtered_derived:
                derived_where_clause = "WHERE " + " AND ".join(filtered_derived)
        
        # OPTIMIZATION: Apply pagination EARLY (before expensive survival/diagnosis processing)
        # This way we only process survival/diagnosis for the paginated subset, not all participants
        # Check if we need survival and diagnosis processing
        #
        # NOTE: dcc-dev returns populated `vital_status` / `age_at_vital_status` even when the client
        # is not filtering on those fields. Our previous "fast path" skipped survival processing entirely
        # and returned NULLs for these fields.
        #
        # We only *need* survival/diagnosis processing inside Cypher when we must FILTER on derived fields.
        # Otherwise, we prefer a safe Cypher that returns raw `survival_records` / `diagnosis_nodes` and
        # compute `vital_status` / `age_at_vital_status` / `associated_diagnoses` in Python.
        needs_survival_processing = bool(
            derived_filters.get("vital_status")
            or derived_filters.get("age_at_vital_status")
        )
        needs_diagnosis_processing = bool(diagnosis_search_term)
        needs_race_processing = bool(race_condition)
        
        # Fast path: No derived-field filtering, diagnosis-search filtering, or race token filtering needed.
        if not needs_survival_processing and not needs_diagnosis_processing and not needs_race_processing:
            # Simple query - just get participants with studies, no complex processing
            if dep_param:
                # Depositions filter present - use required MATCH
                early_where_conditions = [f"st.study_id {deposition_operator} ${dep_param}"]
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
                
                # For depositions list requests, we want to populate `vital_status`, `age_at_vital_status`,
                # and `associated_diagnoses`, but we MUST keep the Cypher very simple to avoid Memgraph
                # instability/crashes seen with complex `reduce()`/list-processing.
                #
                # Strategy: fetch survivals + diagnosis nodes as raw collections and compute derived fields in Python.
                cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        ORDER BY participant_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p,
             collect(s) AS survival_records,
             collect(DISTINCT d) AS diagnosis_nodes
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name)
        """.strip()
            else:
                # No depositions filter - use OPTIONAL MATCH for studies
                early_where_conditions = []
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""

                # If identifiers are present, return raw survival/diagnosis collections (computed in Python)
                # to match dcc-dev's populated output for identifier lookups, without requiring derived-field filtering.
                # IMPORTANT: Group by (participant_id, study_id) pairs to ensure consistency with individual endpoint
                if identifiers_early_filter:
                    cypher = f"""
        MATCH (p:participant){early_where_clause}
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        WHERE study_id IS NOT NULL
        WITH participant_id, study_id, p
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH participant_id, study_id, p, collect(s) AS survival_records
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Apply depositions filter if present
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        WHERE study_id IS NOT NULL{f" AND study_id {deposition_operator} ${dep_param}" if dep_param else ""}
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        // Get all study_ids for this participant for depositions array
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st_all:study)
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes, collect(DISTINCT st_all.study_id) AS all_study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          all_study_ids AS depositions
        ORDER BY toString(name), namespace
        """.strip()
                else:
                    # No identifiers filter - fetch survival and diagnosis records for Python computation
                    # Strategy: fetch survivals + diagnosis nodes as raw collections and compute derived fields in Python.
                    # IMPORTANT: Group by (participant_id, study_id) pairs to ensure consistency with individual endpoint
                    # This ensures that participants in multiple studies are returned with the correct study_id
                    cypher = f"""
        MATCH (p:participant){early_where_clause}
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        WHERE study_id IS NOT NULL
        WITH participant_id, study_id, p
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH participant_id, study_id, p, collect(s) AS survival_records
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Apply depositions filter if present
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        WHERE study_id IS NOT NULL{f" AND study_id {deposition_operator} ${dep_param}" if dep_param else ""}
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        // Get all study_ids for this participant for depositions array
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st_all:study)
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes, collect(DISTINCT st_all.study_id) AS all_study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          all_study_ids AS depositions
        ORDER BY toString(name), namespace
        """.strip()
        else:
            # Full processing path - use existing complex query logic
            # When depositions filter is present, we need required MATCH for studies
            # But we can't put MATCH after OPTIONAL MATCH, so we need to restructure
            # Check if dep_param is set AND exists in params (more robust check)
            condition_result = bool(dep_param and dep_param in params)
            if condition_result:
                logger.debug(f"Using depositions path: dep_param={dep_param}, operator={deposition_operator}")
                # Remove depositions filter from where_clause since it's applied in the MATCH WHERE
                dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                # Also remove identifiers condition if present (it's applied in MATCH WHERE when identifiers_condition exists)
                id_filter_str = "p.participant_id IN id_list"
                # Also remove early participant filters (sex, etc.) since they're applied in MATCH WHERE
                # Note: race_filter_condition is NOT in where_clause (it's handled separately)
                where_clause_no_dep = where_clause.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause else ""
                # Remove early participant filters if they're in where_clause
                for early_filter in early_participant_filters:
                    where_clause_no_dep = where_clause_no_dep.replace(f"WHERE {early_filter}", "").replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip() if where_clause_no_dep else ""
                # Remove identifiers condition if identifiers_condition is present
                if identifiers_condition:
                    where_clause_no_dep = where_clause_no_dep.replace(f"WHERE {id_filter_str}", "").replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip() if where_clause_no_dep else ""
                # Clean up any double WHERE or AND issues
                other_filters = ""
                if where_clause_no_dep:
                    where_clause_no_dep = where_clause_no_dep.replace("WHERE WHERE", "WHERE").replace("AND AND", "AND").replace("  ", " ").strip()
                    # Remove trailing AND
                    while where_clause_no_dep.endswith(" AND"):
                        where_clause_no_dep = where_clause_no_dep[:-4].strip()
                    # Remove leading AND
                    while where_clause_no_dep.startswith("AND "):
                        where_clause_no_dep = where_clause_no_dep[4:].strip()
                    # Remove WHERE prefix if present
                    if where_clause_no_dep.startswith("WHERE "):
                        other_filters = where_clause_no_dep[6:].strip()  # Remove "WHERE " prefix
                    elif where_clause_no_dep and where_clause_no_dep != "WHERE":
                        other_filters = where_clause_no_dep
                    # Final cleanup - remove any remaining trailing/leading AND
                    while other_filters.endswith(" AND"):
                        other_filters = other_filters[:-4].strip()
                    while other_filters.startswith("AND "):
                        other_filters = other_filters[4:].strip()
                    # If other_filters is empty or just whitespace/AND after cleanup, set to empty
                    if not other_filters or other_filters.strip() == "" or other_filters.strip() == "AND":
                        other_filters = ""
                    # Remove any references to study_id (the variable) since it's not defined yet in this query path
                    if other_filters:
                        other_filters = other_filters.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                        # Clean up any resulting "AND AND" or trailing/leading AND
                        other_filters = other_filters.replace("AND AND", "AND").strip()
                        while other_filters.startswith("AND "):
                            other_filters = other_filters[4:].strip()
                        while other_filters.endswith(" AND"):
                            other_filters = other_filters[:-4].strip()
                        if not other_filters or other_filters.strip() == "" or other_filters.strip() == "AND":
                            other_filters = ""
                
                # Build WITH clause with optional WHERE
                # Note: 'c' (consent_group) is no longer needed since we use participant -> consent_group -> study relationship
                with_clause = f"WITH p, s, d, st{race_condition}{identifiers_condition}"
                # Build WHERE conditions for WITH clause
                with_where_conditions = []
                # Don't apply identifiers filter here if it's already applied early in MATCH WHERE clause
                # (identifiers_early_filter is applied in MATCH WHERE, so id_list normalization is still needed but filter is already applied)
                if identifiers_condition and not identifiers_early_filter:
                    with_where_conditions.append("p.participant_id IN id_list")
                # Add other filters if any
                if other_filters and other_filters.strip() and other_filters != "AND":
                    with_where_conditions.append(other_filters)
                # Add race filter if it exists (race_tokens/pr_tokens are defined in WITH clause)
                if race_filter_condition:
                    with_where_conditions.append(race_filter_condition)
                # Apply WHERE clause if we have any conditions
                if with_where_conditions:
                    with_clause += f"\n        WHERE {' AND '.join(with_where_conditions)}"
                
                # For the depositions path, we need to apply derived filters AFTER calculation (line 381)
                # So we keep derived_where_clause as is for later application
                
                # Required study match - put it before OPTIONAL MATCHes
                # OPTIMIZATION: Apply filters early in MATCH WHERE clause to reduce dataset before expensive OPTIONAL MATCH operations
                # This includes: depositions, identifiers, and simple participant property filters (sex, etc.)
                early_where_conditions = [f"st.study_id {deposition_operator} ${dep_param}"]
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                # Add simple participant property filters (sex, etc.) for early filtering
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
                logger.debug(f"Depositions path - early_where_conditions: {early_where_conditions}")
                
                # CRITICAL: When we have derived filters (vital_status, age_at_vital_status) or diagnosis search,
                # we CANNOT paginate early because we need to compute/filter first, then paginate.
                # Early pagination would skip participants before filtering, causing incorrect results.
                use_early_pagination_dep = not needs_survival_processing and not needs_diagnosis_processing
                logger.debug(f"Depositions path - use_early_pagination_dep: {use_early_pagination_dep}, needs_survival_processing: {needs_survival_processing}, needs_diagnosis_processing: {needs_diagnosis_processing}")
                
                if use_early_pagination_dep:
                    # OPTIMIZATION: Apply pagination EARLY - before expensive survival/diagnosis processing
                    # Step 1: Match participants with studies, apply filters, and paginate
                    # Step 2: Process survival/diagnosis ONLY for the paginated subset
                    # Build simplified WITH clause for pagination (without survival/diagnosis, but keep race/identifiers variables)
                    pagination_with_clause = f"WITH p, st{race_condition}{identifiers_condition}"
                    pagination_with_where_conditions = []
                    if identifiers_condition and not identifiers_early_filter:
                        pagination_with_where_conditions.append("p.participant_id IN id_list")
                    if other_filters and other_filters.strip() and other_filters != "AND":
                        pagination_with_where_conditions.append(other_filters)
                    if race_filter_condition:
                        pagination_with_where_conditions.append(race_filter_condition)
                    if pagination_with_where_conditions:
                        pagination_with_clause += f"\n        WHERE {' AND '.join(pagination_with_where_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants with studies, apply filters, and paginate EARLY
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        {pagination_with_clause}
        // Group by participant_id + study_id, then paginate BEFORE processing survival/diagnosis
        WITH toString(p.participant_id) AS participant_id, st.study_id AS study_id, p, st{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        // Step 2: Now process survival/diagnosis ONLY for the paginated subset
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""},
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Group by (participant_id, study_id) to preserve pairs
        // IMPORTANT: The same participant_id string can represent different DB nodes (different participant.id)
        // when associated with different study_id values. We must preserve (participant_id, study_id) pairs.
        WITH toString(p.participant_id) AS participant_id, p, d, st, st.study_id AS study_id, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        WITH participant_id, study_id,
             // Use head to get first participant node (they're all the same per participant_id)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT d) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             // Collect all distinct study_ids for this participant (collect from study_id variable, not st.study_id since st is no longer in scope)
             collect(DISTINCT study_id) AS study_ids
        {self._build_combined_where_clause_for_depositions_path(diagnosis_search_term, dep_param, deposition_operator)}
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Aggregate all diagnoses from all diagnosis nodes
             // Extract diagnosis values from each node (d.diagnosis can be string or list)
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // All study_ids for depositions (filter out null values and apply depositions filter if present)
             [sid IN study_ids WHERE sid IS NOT NULL{f" AND sid {deposition_operator} ${dep_param}" if dep_param else ""}] AS study_ids_temp
        UNWIND study_ids_temp AS sid
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             sid
        WHERE toString(sid) <> ''
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Use filtered study_id (sid) for namespace to ensure it matches depositions filter
             sid AS namespace,
             collect(sid) AS study_ids_filtered
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids_filtered AS depositions
        ORDER BY toString(name)
                    """.strip()
                else:
                    # When we have derived filters (vital_status, age_at_vital_status), we MUST process survival/diagnosis first,
                    # then filter, then paginate. We cannot paginate early because the filter depends on computed values.
                    # Build a WITH clause for this path (without s and d, which aren't matched yet)
                    alt_with_clause = f"WITH p, st{race_condition}{identifiers_condition}"
                    alt_with_where_conditions = []
                    # Apply identifiers filter if present
                    # Note: Even if identifiers_early_filter is set (applied in MATCH WHERE), we still apply it here
                    # as a backup to ensure the filter is applied correctly after OPTIONAL MATCH operations
                    if identifiers_condition:
                        alt_with_where_conditions.append("p.participant_id IN id_list")
                    # Add race filter if present
                    if race_filter_condition:
                        alt_with_where_conditions.append(race_filter_condition)
                    if alt_with_where_conditions:
                        alt_with_clause += f"\n        WHERE {' AND '.join(alt_with_where_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants with studies and apply direct filters
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        {alt_with_clause}
        // Step 2: Process survival/diagnosis BEFORE filtering by derived fields
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""},
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Step 3: Keep (participant_id, study_id) pairs - do NOT group by participant_id only
        // This allows multiple rows per participant_id if they have multiple study_ids
        // IMPORTANT: We maintain (participant_id, study_id) pairs throughout to match the summary count
        WITH toString(p.participant_id) AS participant_id, p, d, st, st.study_id AS study_id, final_vital_status, final_age_at_vital_status,
             ethnicity_value{", race_tokens, pr_tokens" if race_condition else ""}
        // Group by (participant_id, study_id) to preserve pairs while aggregating other fields
        // IMPORTANT: The same participant_id string can represent different DB nodes (different participant.id)
        // when associated with different study_id values. We must preserve (participant_id, study_id) pairs.
        WITH participant_id, study_id,
             // Use head to get first participant node (within a (participant_id, study_id) pair, there should be one node)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT d) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value{", head(collect(DISTINCT race_tokens)) AS race_tokens, head(collect(DISTINCT pr_tokens)) AS pr_tokens" if race_condition else ""},
             // Keep study_id as a single value (not a list) since we're grouping by (participant_id, study_id)
             study_id AS study_ids_single
        // Apply diagnosis search filter (only if diagnosis_search_term is present)
        // Note: Depositions filter is already applied in the MATCH clause (WHERE st.study_id = $param_1)
        // Since we're grouping by (participant_id, study_id), all rows already match the depositions filter
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        // Keep (participant_id, study_id) pairs - do NOT group by participant_id only
        WITH participant_id, study_id, p, diagnosis_nodes, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single{", race_tokens, pr_tokens" if race_condition else ""},
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single{", race_tokens, pr_tokens" if race_condition else ""},
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single{", race_tokens, pr_tokens" if race_condition else ""},
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        // Step 4: NOW paginate AFTER filtering by derived fields
        // IMPORTANT: We paginate on (participant_id, study_id) pairs, not unique participant_ids
        // The same participant_id string can represent different DB nodes (different participant.id)
        // when associated with different study_id values. We must preserve (participant_id, study_id) pairs
        // and allow multiple rows per participant_id if they have different study_ids.
        // Filter by depositions if present
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Use study_id for namespace (for backward compatibility)
             study_ids_single AS namespace,
             study_ids_single AS sid{", race_tokens, pr_tokens" if race_condition else ""}
        WHERE toString(sid) <> ''{f" AND sid {deposition_operator} ${dep_param}" if dep_param else ""}
        // Step 5: Apply pagination AFTER filtering
        // Paginate on (participant_id, sid) pairs - do NOT group by participant_id
        // This allows multiple rows per participant_id if they have multiple study_ids
        // Each row represents one (participant_id, study_id) pair
        // Need WITH clause before ORDER BY to project variables
        WITH participant_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, namespace, sid{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id, sid
        SKIP $offset
        LIMIT $limit
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          [sid] AS depositions
        """.strip()
            else:
                # CRITICAL SAFEGUARD: If cypher is already set (from depositions path), skip this entire else block
                if 'cypher' in locals() and cypher:
                    logger.error("BUG: cypher is already set from depositions path, but else block is executing! This should never happen.")
                    logger.error("Skipping non-depositions path code since cypher is already set.")
                    # Cypher already set from depositions path above
                else:
                    # No depositions filter - use OPTIONAL MATCH for studies
                    # Collect relationships separately to avoid cartesian product
                    # Build WITH clause with optional WHERE
                    with_clause = f"WITH p{race_condition}{identifiers_condition}"
            # Build WHERE conditions for WITH clause
            with_where_conditions = []
            # Always apply identifiers filter if present (id_list is created in WITH clause)
            if identifiers_condition:
                with_where_conditions.append("p.participant_id IN id_list")
            # Add other filters from where_clause (excluding identifiers condition, race_filter_condition, and st.study_id conditions)
            # Note: st.study_id conditions are for depositions filter, which is not used in non-depositions path
            if where_clause:
                # Remove "WHERE " prefix if present
                where_conditions_str = where_clause.replace("WHERE ", "").strip()
                # Remove identifiers condition if present (already added above)
                id_filter_str = "p.participant_id IN id_list"
                where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                # Remove race_filter_condition if present (it will be added separately since race_tokens is defined in WITH clause)
                if race_filter_condition:
                    where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                # Remove early participant filters (sex, etc.) since they're applied in MATCH WHERE
                for early_filter in early_participant_filters:
                    where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                # Remove st.study_id conditions (depositions filter) - these are not applicable in non-depositions path
                # Remove patterns like "st.study_id = $param" or "st.study_id IN $param" with surrounding ANDs
                where_conditions_str = where_conditions_str.replace("st.study_id =", "").replace("st.study_id IN", "").replace("st.study_id=", "").replace("st.study_idIN", "")
                # Remove any references to study_id (the variable) since it's not defined yet in this query path
                # Remove patterns like "study_id = $param", "study_id IN $param", "study_id=$param", etc.
                # Use multiple passes to catch all variations - be very aggressive
                # First, remove complete conditions with study_id
                # Remove patterns like "study_id = $param_X" or "study_id IN $param_X" or "study_id=$param_X"
                where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                # Remove patterns like "$param_X = study_id" or "$param_X IN study_id"
                where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                # Remove standalone study_id references
                where_conditions_str = where_conditions_str.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                where_conditions_str = where_conditions_str.replace("study_id ", "").replace(" study_id", "").replace("(study_id", "").replace("study_id)", "")
                # Remove any remaining study_id references (be very aggressive)
                where_conditions_str = re.sub(r'\bstudy_id\b', '', where_conditions_str)
                # Remove any parameter references that might be left (e.g., "$param_1" if it was for st.study_id)
                # But be careful - we can't safely remove all $param references as they might be for other fields
                # Clean up any resulting "AND AND" or trailing/leading AND
                where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                while where_conditions_str.startswith("AND "):
                    where_conditions_str = where_conditions_str[4:].strip()
                while where_conditions_str.endswith(" AND"):
                    where_conditions_str = where_conditions_str[:-4].strip()
                if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND":
                    with_where_conditions.append(where_conditions_str)
            # Add race filter if it exists (race_tokens/pr_tokens are defined in WITH clause)
            if race_filter_condition:
                with_where_conditions.append(race_filter_condition)
            # Apply WHERE clause if we have any conditions
            # Final safety check: remove any study_id references that might have slipped through
            if with_where_conditions:
                # Filter out any conditions that reference study_id (not defined yet in non-depositions path)
                safe_conditions = []
                for condition in with_where_conditions:
                    # Check if condition contains study_id (case-insensitive)
                    if condition and 'study_id' not in condition.lower():
                        safe_conditions.append(condition)
                    else:
                        logger.warning(f"Removing condition with study_id reference (not defined yet): {condition}")
                if safe_conditions:
                    with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
            
            # OPTIMIZATION: Apply filters early in MATCH WHERE clause to reduce dataset before expensive OPTIONAL MATCH operations
            # This includes: identifiers and simple participant property filters (sex, etc.)
            early_where_conditions = []
            if identifiers_early_filter:
                early_where_conditions.append(identifiers_early_filter)
            # Add simple participant property filters (sex, etc.) for early filtering
            if early_participant_filters:
                early_where_conditions.extend(early_participant_filters)
            early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
            logger.debug(f"Non-depositions path - early_where_conditions: {early_where_conditions}")
            
            # CRITICAL SAFEGUARD: Only execute non-depositions path code if cypher is not already set
            # If cypher is already set from the depositions path, skip this entire block
            if 'cypher' in locals() and cypher:
                logger.debug("Skipping non-depositions path - cypher already set from depositions path")
                # Skip the rest - cypher is already set
                pass
            else:
                # CRITICAL: When we have derived filters (vital_status, age_at_vital_status), we CANNOT paginate early
                # because we need to compute final_vital_status first, then filter, then paginate.
                # Early pagination only works when we're filtering on direct participant properties (sex, identifiers, etc.)
                use_early_pagination = not needs_survival_processing
                logger.debug(f"Non-depositions path - use_early_pagination: {use_early_pagination}")
                
                if use_early_pagination:
                    # OPTIMIZATION: Apply pagination EARLY - before expensive survival/diagnosis processing
                    # Step 1: Match participants, apply filters, and paginate
                    # Step 2: Process survival/diagnosis/studies ONLY for the paginated subset
                    # Build simplified WITH clause for pagination (without survival/diagnosis, but keep race/identifiers variables)
                    pagination_with_clause = f"WITH p{race_condition}{identifiers_condition}"
                    pagination_with_where_conditions = []
                    if identifiers_condition:
                        pagination_with_where_conditions.append("p.participant_id IN id_list")
                    if where_clause:
                        where_conditions_str = where_clause.replace("WHERE ", "").strip()
                        id_filter_str = "p.participant_id IN id_list"
                        where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                        if race_filter_condition:
                            where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                        for early_filter in early_participant_filters:
                            where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                        # CRITICAL: Remove ALL st. references (depositions filter - not applicable in non-depositions path)
                        # Remove complete conditions with st.study_id
                        where_conditions_str = re.sub(r'\bst\.study_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                        where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bst\.study_id\b', '', where_conditions_str)
                        # Remove any partial st. references (dangling st.)
                        where_conditions_str = re.sub(r'\bAND\s+st\.\s*', ' AND ', where_conditions_str)  # Remove "AND st."
                        where_conditions_str = re.sub(r'\bst\.\s*AND', ' AND', where_conditions_str)  # Remove "st. AND"
                        where_conditions_str = re.sub(r'\bst\.\s*$', '', where_conditions_str)  # Remove trailing "st."
                        where_conditions_str = re.sub(r'^\s*st\.\s*', '', where_conditions_str)  # Remove leading "st."
                        where_conditions_str = re.sub(r'\bst\.\b', '', where_conditions_str)  # Remove any remaining "st."
                        # Remove study_id patterns (variable, not st.study_id)
                        where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                        where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                        # Clean up multiple ANDs and whitespace
                        where_conditions_str = re.sub(r'\s+', ' ', where_conditions_str)  # Normalize whitespace
                        where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                        while where_conditions_str.startswith("AND "):
                            where_conditions_str = where_conditions_str[4:].strip()
                        while where_conditions_str.endswith(" AND"):
                            where_conditions_str = where_conditions_str[:-4].strip()
                        # Final check: if the string is empty or just "AND", don't add it
                        if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND" and where_conditions_str.strip() != "AND":
                            # Final safety: ensure no st. references remain
                            if 'st.' not in where_conditions_str.lower():
                                pagination_with_where_conditions.append(where_conditions_str)
                    if race_filter_condition:
                        pagination_with_where_conditions.append(race_filter_condition)
                    if pagination_with_where_conditions:
                        safe_conditions = [c for c in pagination_with_where_conditions if c and 'study_id' not in c.lower()]
                        if safe_conditions:
                            pagination_with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants, apply filters, and paginate EARLY
        MATCH (p:participant){early_where_clause}
        {pagination_with_clause}
        // Group by participant_id, then paginate BEFORE processing survival/diagnosis
        WITH toString(p.participant_id) AS participant_id, p{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id
        SKIP $offset
        LIMIT $limit
        // Step 2: Now process survival/diagnosis/studies ONLY for the paginated subset
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, collect(s) AS survival_records
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Collect studies separately (no cartesian product)
        // Use participant -> consent_group -> study relationship
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)"}
        // Bind a scalar `study_id` for the row. Avoid carrying a LIST of study IDs through long WITH chains,
        // which Memgraph can mis-handle and report as "Unbound variable".
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes,
             st.study_id AS study_id
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        // Keep `participant_id` and `survival_records` in scope (Memgraph will error if we drop them and reference later)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes, study_id,
             // Keep only records with a status
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
             // Check if any record has 'Dead' status
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, participant_id, diagnosis_nodes, study_id, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Everything is already aggregated per participant_id (we collected survivals/diagnoses/studies separately).
        // Everything is already aggregated per (participant_id, study_id) for this row.
        WITH participant_id, study_id, p, diagnosis_nodes, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name)
                """.strip()
                else:
                    # Non-depositions path - should only be taken when dep_param is NOT set
                    # If dep_param is set, we should have taken the depositions path above
                    logger.debug(f"Taking non-depositions path (dep_param={dep_param})")
                # CRITICAL SAFEGUARD: If dep_param is set, we MUST NOT take the non-depositions path
                # This is a safeguard to prevent incorrect queries when dep_param is set but the condition above failed
                if dep_param and dep_param in params:
                    logger.error(
                        "Unexpected path selection: dep_param exists but non-depositions path taken",
                        dep_param=dep_param,
                        params_keys=list(params.keys())
                    )
                # When we have derived filters (vital_status, age_at_vital_status), we MUST process survival/diagnosis first,
                # then filter, then paginate. We cannot paginate early because the filter depends on computed values.
                # Build initial WITH clause (similar to early pagination path, but without pagination)
                initial_with_clause = f"WITH p{race_condition}{identifiers_condition}"
                initial_with_where_conditions = []
                # Apply identifiers filter if present
                # Note: Even if identifiers_early_filter is set (applied in MATCH WHERE), we still apply it here
                # as a backup to ensure the filter is applied correctly after OPTIONAL MATCH operations
                if identifiers_condition:
                    initial_with_where_conditions.append("p.participant_id IN id_list")
                if where_clause:
                    where_conditions_str = where_clause.replace("WHERE ", "").strip()
                    id_filter_str = "p.participant_id IN id_list"
                    where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                    if race_filter_condition:
                        where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                    for early_filter in early_participant_filters:
                        where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                    # Remove st.study_id and study_id references (not applicable before study match)
                    where_conditions_str = re.sub(r'\bst\.study_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bst\.study_id\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bst\.\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bstudy_id\b', '', where_conditions_str)
                    where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                    while where_conditions_str.startswith("AND "):
                        where_conditions_str = where_conditions_str[4:].strip()
                    while where_conditions_str.endswith(" AND"):
                        where_conditions_str = where_conditions_str[:-4].strip()
                    if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND":
                        if 'st.' not in where_conditions_str.lower() and 'study_id' not in where_conditions_str.lower():
                            initial_with_where_conditions.append(where_conditions_str)
                if race_filter_condition:
                    initial_with_where_conditions.append(race_filter_condition)
                if initial_with_where_conditions:
                    safe_conditions = [c for c in initial_with_where_conditions if c and 'study_id' not in c.lower()]
                    if safe_conditions:
                        initial_with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
                
                cypher = f"""
        // Step 1: Match participants and apply direct filters
        MATCH (p:participant){early_where_clause}
        {initial_with_clause}
        // Step 2: Process survival/diagnosis/studies BEFORE filtering by derived fields
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, collect(s) AS survival_records
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Collect studies separately (no cartesian product)
        // Use participant -> consent_group -> study relationship
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        // Bind a scalar `study_id` for the row. Avoid carrying a LIST of study IDs through long WITH chains,
        // which Memgraph can mis-handle and report as "Unbound variable".
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes,
             st.study_id AS study_id
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        // Keep `participant_id` and `survival_records` in scope (Memgraph will error if we drop them and reference later)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes, study_id,
             // Keep only records with a status
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
             // Check if any record has 'Dead' status
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Step 3: Group by participant to aggregate study_ids and handle multiple records per participant
        WITH toString(p.participant_id) AS participant_id, p, diagnosis_nodes, study_id, final_vital_status, final_age_at_vital_status,
             ethnicity_value{", race_tokens, pr_tokens" if race_condition else ""}
        WITH participant_id, study_id,
             // Use head to get first participant node (they're all the same per participant_id)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT diagnosis_nodes) AS diagnosis_nodes_list,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT study_id) AS study_ids{", head(collect(DISTINCT race_tokens)) AS race_tokens, head(collect(DISTINCT pr_tokens)) AS pr_tokens" if race_condition else ""}
        // Flatten diagnosis_nodes_list (it's a list of lists) and process diagnoses
        // Note: We need to preserve study_ids through the UNWIND, so we'll re-aggregate by participant_id
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids{", race_tokens, pr_tokens" if race_condition else ""},
             // Flatten the list of diagnosis node lists into a single list
             [node_list IN diagnosis_nodes_list | node_list] AS all_diagnosis_nodes
        UNWIND all_diagnosis_nodes AS diagnosis_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids{", race_tokens, pr_tokens" if race_condition else ""},
             diagnosis_nodes
        // Filter out NULL nodes and extract diagnosis values
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids{", race_tokens, pr_tokens" if race_condition else ""},
             // Extract diagnosis values from non-null nodes
             [node IN diagnosis_nodes WHERE node IS NOT NULL AND node.diagnosis IS NOT NULL | node.diagnosis] AS diagnoses_from_node
        // Handle empty diagnosis lists: if empty, use [null] to ensure participant is not dropped by UNWIND
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids{", race_tokens, pr_tokens" if race_condition else ""},
             CASE WHEN size(diagnoses_from_node) = 0 THEN [null] ELSE diagnoses_from_node END AS diagnoses_to_unwind
        UNWIND diagnoses_to_unwind AS diag
        // Re-aggregate by participant_id to preserve study_ids and flatten diagnoses
        WITH participant_id,
             head(collect(DISTINCT study_id)) AS study_id,
             head(collect(DISTINCT p)) AS p,
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             head(collect(DISTINCT final_age_at_vital_status)) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             head(collect(DISTINCT study_ids)) AS study_ids{", head(collect(DISTINCT race_tokens)) AS race_tokens, head(collect(DISTINCT pr_tokens)) AS pr_tokens" if race_condition else ""},
             // Filter out null values from diagnoses (from empty list handling)
             [d IN collect(DISTINCT diag) WHERE d IS NOT NULL] AS d
        // Step 4: NOW paginate AFTER filtering by derived fields
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Use first study_id for namespace (for backward compatibility)
             head(study_ids) AS namespace,
             // All study_ids for depositions (filter out null values)
             [sid IN study_ids WHERE sid IS NOT NULL] AS study_ids_temp{", race_tokens, pr_tokens" if race_condition else ""}
        UNWIND study_ids_temp AS sid
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             namespace,
             sid{", race_tokens, pr_tokens" if race_condition else ""}
        WHERE toString(sid) <> ''
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             namespace,
             collect(sid) AS study_ids_filtered{", race_tokens, pr_tokens" if race_condition else ""}
        // Step 5: Apply pagination AFTER filtering
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_filtered, namespace{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id
        SKIP $offset
        LIMIT $limit
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids_filtered AS depositions
        ORDER BY toString(name)
        """.strip()
        
        # Debug logging for query details (only in debug mode)
        if getattr(self.settings, "debug", False):
            logger.debug(
                "Cypher query details",
                cypher=cypher if 'cypher' in locals() else "not defined",
                params=params,
                where_clause=where_clause if 'where_clause' in locals() else None,
                race_filter_condition=race_filter_condition if 'race_filter_condition' in locals() else None,
                race_condition=race_condition if 'race_condition' in locals() else None
            )
        
        logger.debug(
            "Executing get_subjects Cypher query",
            filters=filters
        )

        # Memgraph can behave inconsistently with `// ...` inline comments in multi-line Cypher.
        # We keep comments for local debugging output, but strip them from the actual query we execute.
        cypher_to_run = "\n".join(
            line for line in (cypher or "").splitlines() if not line.lstrip().startswith("//")
        ).strip()
        
        # Execute query with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher_to_run, params)
                # Ensure the result is fully consumed - use async iteration for reliability
                records = []
                async for record in result:
                    records.append(dict(record))
                
                # Ensure result is fully consumed
                await result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if records or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_subjects query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_subjects query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    # Log the full query on error for debugging
                    # Log the full query for debugging
                    logger.error(
                        "Error executing get_subjects Cypher query after retries",
                        error=str(e),
                        error_type=type(e).__name__,
                        cypher=cypher if 'cypher' in locals() else None,
                        full_query=cypher if 'cypher' in locals() else None,  # Log full query
                        params_keys=list(params.keys()) if params else [],
                        params=params,  # Log params too
                        where_clause_original=where_clause if 'where_clause' in locals() else None,
                        dep_param=dep_param if 'dep_param' in locals() else None,
                        other_filters=other_filters if 'other_filters' in locals() else None,
                        exc_info=True
                    )
                    raise
        
        # Convert to Subject objects
        subjects = []
        for record in records:
            subjects.append(self._record_to_subject(record, base_url=base_url))
        
        logger.debug(
            "Found subjects",
            count=len(subjects),
            filters=filters
        )
        
        return subjects
    
    async def get_subject_by_identifier(
        self,
        organization: str,
        namespace: Optional[str],
        name: str,
        base_url: Optional[str] = None
    ) -> Optional[Subject]:
        """
        Get a specific subject by organization, namespace, and name.
        
        Args:
            organization: Organization identifier (defaults to CCDI-DCC)
            namespace: Namespace identifier (study_id value, optional)
            name: Subject name/participant ID
            
        Returns:
            Subject object or None if not found
        """
        logger.debug(
            "Fetching subject by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Build query parameters
        params = {"participant_id": name}
        
        # If namespace (study_id) is provided, require it to match the participant's study
        # This ensures namespace is a valid study_id for the participant
        if namespace:
            # When namespace is provided, match participant and study together with study_id filter
            # This ensures we only get results where the participant belongs to the specified study
            # IMPORTANT: The WHERE clause filters by study_id FIRST, so only participants
            # belonging to the exact requested study_id will be matched
            params["namespace"] = namespace
            cypher = f"""
        // First, find the study with the requested study_id
        MATCH (st:study)
        WHERE st.study_id = $namespace
        // Then find participants that belong to this specific study
        // The relationship path must be: participant -> consent_group -> study
        // This ensures we only match participants that are actually connected to the requested study
        MATCH (p:participant)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st)
        WHERE p.participant_id = $participant_id
        // Verify that the participant is connected to the requested study
        // Use DISTINCT to ensure we only get one row per participant-study combination
        WITH DISTINCT p, st
        // Verify st.study_id matches the requested namespace (defensive check)
        WHERE st.study_id = $namespace
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st, 
             // Collect all survival records for this participant
             collect(DISTINCT s) AS survival_records
        WITH p, d, st,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        // Now collect all study_ids for this participant (not just the filtered one)
        WITH toString(p.participant_id) AS participant_id, p, d, survs, final_vital_status, final_age_at_vital_status
        // Get all studies for this participant
        OPTIONAL MATCH (p)-[:of_participant]->(c_all:consent_group)-[:of_consent_group]->(st_all:study)
        WITH participant_id, p, d, survs, final_vital_status, final_age_at_vital_status,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT st_all.study_id) AS study_ids
        WITH participant_id, p, d, survs, final_vital_status, final_age_at_vital_status,
             // Use requested namespace for namespace (for backward compatibility)
             $namespace AS namespace,
             // All study_ids for depositions
             study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d.diagnosis AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids AS depositions
        LIMIT 1
        """
        else:
            # When namespace is not provided, use optional match for study
            cypher = f"""
        MATCH (p:participant)
        WHERE p.participant_id = $participant_id
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, d, c, st
        WITH p, d, c, st, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, c, st,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, c, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, c, st, survs,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             // If no record matches max_age, fall back to first available status
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        // Collect all study_ids for this participant
        WITH toString(p.participant_id) AS participant_id, p, d, survs, final_vital_status, final_age_at_vital_status,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT st.study_id) AS study_ids
        WITH participant_id, p, d, survs, final_vital_status, final_age_at_vital_status,
             // Use first study_id for namespace (for backward compatibility)
             head(study_ids) AS namespace,
             // All study_ids for depositions
             study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d.diagnosis AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids AS depositions
        LIMIT 1
        """
        
        logger.info(
            "Executing get_subject_by_identifier Cypher query",
            participant_id=name,
            namespace=namespace,
            params=params
        )

        # Execute query with proper error handling, result consumption, and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher, params)
                # Ensure the result is fully consumed - use async iteration for reliability
                # This ensures all records are fetched before proceeding
                records = []
                async for record in result:
                    records.append(dict(record))
                
                # Ensure result is fully consumed
                await result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if records or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_subject_by_identifier query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_subject_by_identifier query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error(
                        "Error executing get_subject_by_identifier Cypher query after retries",
                        error=str(e),
                        error_type=type(e).__name__,
                        participant_id=name,
                        namespace=namespace,
                        cypher=cypher[:500] if cypher else None,
                        exc_info=True
                    )
                    raise
        
        logger.debug(
            "Query execution completed",
            participant_id=name,
            namespace=namespace,
            records_count=len(records) if records else 0
        )
        
        if not records:
            logger.debug("Subject not found", participant_id=name, namespace=namespace)
            return None
        
        # Convert to Subject object
        subject = self._record_to_subject(records[0], base_url=base_url)
        
        logger.debug("Found subject", participant_id=name, namespace=namespace, subject_data=getattr(subject, 'name', str(subject)[:50]))
        
        return subject
    
    def _get_field_path(self, field: str) -> str:
        """
        Map field name to its database path for counting operations.
        
        Args:
            field: Field name to map
            
        Returns:
            Database path for the field
        """
        # Field name to database property mapping
        field_mapping = {
            "sex": "p.sex_at_birth",
            "race": "p.race",
            "vital_status": "final_vital_status",  # Derived from survival records
            "age_at_vital_status": "final_age_at_vital_status",  # Derived from survival records
            "associated_diagnoses": "d.diagnosis"  # From diagnosis nodes
        }
        return field_mapping.get(field, f"p.{field}")
    
    def _build_sex_normalization_case(self, field: str) -> str:
        """
        Build Cypher CASE statement for sex value normalization from config.
        
        Args:
            field: Field name to check
            
        Returns:
            Cypher CASE statement string for sex normalization, or empty string if not sex field
        """
        if field != "sex" or not self.settings or not hasattr(self.settings, 'sex_value_mappings'):
            return ""
        
        mappings = self.settings.sex_value_mappings
        if not mappings:
            return ""
        
        # Build CASE statement from config mappings
        case_parts = []
        for db_value, normalized_value in mappings.items():
            case_parts.append(f"WHEN toString(value) = '{db_value}' OR toString(value) = '{normalized_value}' THEN '{normalized_value}'")
        
        # Default fallback (prefer 'U' for unknown/Not Reported if available)
        default_value = mappings.get("Not Reported", "U") if "Not Reported" in mappings else list(mappings.values())[0] if mappings else "U"
        case_parts.append(f"ELSE '{default_value}'")
        
        return f"""
                 CASE 
                   {' '.join(case_parts)}
                 END"""
    
    async def count_subjects_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count subjects grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of count results)
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting subjects by field",
            field=field,
            filters=filters
        )
        
        # Validate field is allowed for count operations (case-sensitive)
        allowed_fields = set(self.settings.subject_count_fields if self.settings else ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses"])
        if field not in allowed_fields:
            raise UnsupportedFieldError(
                field=field,
                entity_type="subject"
            )
        
        # Special handling for race field
        if field == "race":
            return await self._count_subjects_by_race(filters)
        
        # Special handling for ethnicity field (derived from race)
        if field == "ethnicity":
            return await self._count_subjects_by_ethnicity(filters)
        
        # Special handling for associated_diagnoses field (list field from diagnosis nodes)
        if field == "associated_diagnoses":
            return await self._count_subjects_by_associated_diagnoses(filters)
        
        # Build WHERE conditions and parameters (for filtering, not for field null check yet)
        base_where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle race parameter normalization
        race_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                # Normalize race value to a list in Python (handle both string and list inputs)
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []
                
                if race_list:
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    
                    # Check if "Not Reported" is in the filter - if so, also match "Hispanic or Latino" only records
                    includes_not_reported = any(r.strip() == "Not Reported" for r in race_list)
                    
                    race_condition = f""",
                    // race tokens (already normalized to list in Python)
                    ${race_param} AS race_tokens,
                    // tokenize stored race (data is always string format with semicolon separator)
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    
                    if includes_not_reported:
                        # Match either: "Not Reported" in original values OR "Hispanic or Latino" only (which converts to "Not Reported")
                        base_where_conditions.append("""(ANY(tok IN race_tokens WHERE tok IN pr_tokens) OR 
                        (size(pr_tokens) > 0 AND all(tok IN pr_tokens WHERE tok = 'Hispanic or Latino') AND 'Not Reported' IN race_tokens))""")
                    else:
                        # Normal matching - exclude "Hispanic or Latino" from matching since it's removed in conversion
                        base_where_conditions.append("ANY(tok IN race_tokens WHERE tok IN [r IN pr_tokens WHERE r <> 'Hispanic or Latino'])")
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                base_where_conditions.append("p.participant_id IN id_list")
        
        # Handle depositions filter (study_id)
        dep_param = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            if depositions_value is not None and str(depositions_value).strip():
                param_counter += 1
                dep_param = f"param_{param_counter}"
                params[dep_param] = str(depositions_value).strip()
                # Filter by study_id - participants must belong to the specified study
                base_where_conditions.append("st.study_id = ${}".format(dep_param))
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            base_where_conditions.append("""(
                ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
                OR ANY(key IN keys(p.metadata.unharmonized) 
                       WHERE toLower(key) CONTAINS 'diagnos' 
                       AND toLower(toString(p.metadata.unharmonized[key])) CONTAINS toLower($diagnosis_search_term))
            )""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters
        for filter_field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                base_where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                base_where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build base WHERE clause (for filtering participants)
        if base_where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_base = [c for c in base_where_conditions if c and c.strip()]
            base_where_clause = "WHERE " + " AND ".join(filtered_base) if filtered_base else ""
        else:
            base_where_clause = ""
        
        # Check if field requires survival record processing
        is_survival_field = field in {"vital_status", "age_at_vital_status"}
        
        # Build survival processing logic if needed
        survival_processing = ""
        field_access = self._get_field_path(field)
        
        if is_survival_field:
            survival_processing = """
WITH p, d, c, st, 
     // Collect all survival records for this participant
     collect(s) AS survival_records
WITH p, d, c, st, survival_records,
     // Keep only records with a status
     [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
WITH p, d, c, st, survs,
     // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age,
     // Check if all ages are -999 (for counting as missing)
    all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
    // Check if Dead age is -999
    any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
WITH p, d, c, st,
     // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
     // If no record matches max_age, fall back to first available status
     CASE 
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead THEN 'Dead'
       ELSE CASE
         WHEN head([sr IN survs 
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                     | sr.last_known_survival_status]) IS NOT NULL
         THEN head([sr IN survs 
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                     | sr.last_known_survival_status])
         ELSE head([sr IN survs | sr.last_known_survival_status])
       END
     END AS final_vital_status,
     // Age: If no vital_status, age should also be NULL
     // If all ages are -999, or Dead age is -999, set to NULL (count as missing)
     // Otherwise use max Dead age or max age
     CASE 
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead AND dead_age_is_999 THEN NULL
       WHEN has_dead THEN max_dead_age
       WHEN all_ages_are_999 THEN NULL
       ELSE max_age
     END AS final_age_at_vital_status"""
        else:
            survival_processing = ""
        
        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Total count should match summary endpoint - use same logic (no survival processing for total)
        if is_survival_field:
            # For survival fields, we need survival processing but can still optimize study traversal
            # Optimize based on whether we have filters
            if base_where_clause or race_condition or identifiers_condition:
                combined_base_where = combine_where_clauses(base_where_clause, "st IS NOT NULL")
                total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {combined_base_where}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
            else:
                # No filters - use participant -> consent_group -> study relationship (still need survival processing)
                # Use participant -> consent_group -> study relationship for study traversal
                # Note: For total count, we don't need survival/diagnosis records (they're only needed for values/missing)
                # This avoids cartesian products from multiple survival/diagnosis records per participant
                total_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # For non-survival fields, optimize based on whether we have filters
            if base_where_clause or race_condition or identifiers_condition:
                # Has filters - need OPTIONAL MATCH for filters that might need survival/diagnosis
                # Use DISTINCT to avoid cartesian products from multiple survival/diagnosis records
                total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE st IS NOT NULL
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
            else:
                # No filters - use participant -> consent_group -> study relationship
                # Use participant -> consent_group -> study relationship, no unnecessary OPTIONAL MATCHes
                total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        
        # Query 2: Get count of participants with null field value
        null_check = f"{field_access} IS NULL" if not is_survival_field else f"final_{field} IS NULL"
        if is_survival_field:
            # For survival fields, missing logic depends on the field:
            # - vital_status: missing when final_vital_status IS NULL
            # - age_at_vital_status: missing when final_age_at_vital_status IS NULL (includes -999 cases)
            if field == "age_at_vital_status":
                # Optimize based on whether we have filters
                if base_where_clause or race_condition or identifiers_condition:
                    missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, final_vital_status, final_age_at_vital_status
        WITH participant_id, study_id,
             head(collect(final_vital_status)) as final_vital_status,
             head(collect(final_age_at_vital_status)) as final_age_at_vital_status
        WHERE final_age_at_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
                else:
                    # No filters: optimize by computing derived age ONCE per participant, then expanding to studies.
                    # This avoids repeating survival processing for each (participant, study) row.
                    missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, collect(s) AS survival_records
        WITH p, [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age 
                    END) AS max_age,
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id, final_age_at_vital_status
        WHERE final_age_at_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
            else:
                # For vital_status, missing means no vital_status
                # Optimize based on whether we have filters
                if base_where_clause or race_condition or identifiers_condition:
                    missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, final_vital_status
        WITH participant_id, study_id, head(collect(final_vital_status)) as final_vital_status
        WHERE final_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
                else:
                    # No filters - use participant -> consent_group -> study relationship (still need survival processing)
                    # Start from participant -> consent_group -> study, only match survival records (not diagnosis)
                    # Diagnosis is not needed for vital_status processing
                    missing_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, st,
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, st, survival_records,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age 
                    END) AS max_age
        WITH p, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status
        WITH p.participant_id AS participant_id, st.study_id AS study_id, final_vital_status
        WITH participant_id, study_id, head(collect(final_vital_status)) as final_vital_status
        WHERE final_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
        else:
            # For non-survival fields, optimize based on whether we have filters
            if base_where_clause or race_condition or identifiers_condition:
                # Has filters - need OPTIONAL MATCH for filters that might need survival/diagnosis
                # Use DISTINCT to avoid cartesian products from multiple survival/diagnosis records
                missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE st IS NOT NULL AND {null_check}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
            else:
                # No filters - use participant -> consent_group -> study relationship
                # Use participant -> consent_group -> study relationship, no unnecessary OPTIONAL MATCHes
                missing_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE {null_check}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        
        # Query 3: Get counts by field values
        not_null_check = f"{field_access} IS NOT NULL" if not is_survival_field else f"final_{field} IS NOT NULL"
        field_value_expr = f"{field_access}" if not is_survival_field else f"final_{field}"
        
        # Build sex normalization CASE statement from config
        sex_normalization = self._build_sex_normalization_case(field)
        
        # Build normalization expression: use sex normalization if field is sex and normalization exists
        if field == "sex" and sex_normalization:
            normalization_expr = sex_normalization
        else:
            normalization_expr = "toString(value)"
        
        if is_survival_field:
            # Optimize based on whether we have filters
            if base_where_clause or race_condition or identifiers_condition:
                values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, {field_value_expr} as field_val
        WITH participant_id, study_id, head(collect(field_val)) as field_val
        WHERE field_val IS NOT NULL
        WITH participant_id, study_id,
             CASE 
               WHEN field_val IS NULL THEN []
               ELSE [field_val]
             END as field_values
        UNWIND field_values as value
        WITH participant_id, study_id, value,
             toString(value) as normalized_value
        WITH DISTINCT participant_id, study_id, normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
            else:
                # No filters - use participant -> consent_group -> study relationship (still need survival processing)
                # Start from participant -> consent_group -> study, only match survival records (not diagnosis)
                # Diagnosis is not needed for vital_status processing
                if field == "age_at_vital_status":
                    # Compute derived age ONCE per participant, then expand to studies for counting.
                    values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, collect(s) AS survival_records
        WITH p, [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age 
                    END) AS max_age,
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id, final_age_at_vital_status as field_val
        WHERE field_val IS NOT NULL
        WITH DISTINCT participant_id, study_id, toString(field_val) AS normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
                else:
                    values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, st,
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, st, survival_records,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age 
                    END) AS max_age
        WITH p, st, survs, has_dead, max_dead_age, max_age,
             // Check if all ages are -999 (for counting as missing)
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             // Check if Dead age is -999
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If no vital_status, age should also be NULL
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        WITH p.participant_id AS participant_id, st.study_id AS study_id, {field_access} as field_val
        WITH participant_id, study_id, head(collect(field_val)) as field_val
        WHERE field_val IS NOT NULL
        WITH participant_id, study_id,
             CASE 
               WHEN field_val IS NULL THEN []
               ELSE [field_val]
             END as field_values
        UNWIND field_values as value
        WITH participant_id, study_id, value,
             toString(value) as normalized_value
        WITH DISTINCT participant_id, study_id, normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # For non-survival fields, optimize based on whether we have filters
            if base_where_clause or race_condition or identifiers_condition:
                # Has filters - need OPTIONAL MATCH for filters that might need survival/diagnosis
                values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE st IS NOT NULL AND {not_null_check}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id,
             CASE 
               WHEN {field_value_expr} IS NULL THEN []
               ELSE 
                 // Wrap in list for UNWIND - works for both strings and lists
                 [{field_value_expr}]
             END as field_values
        UNWIND field_values as value
        WITH participant_id, study_id, value,
             CASE 
               WHEN '{field}' = 'sex' THEN{normalization_expr}
               ELSE toString(value)
             END as normalized_value
        WITH DISTINCT participant_id, study_id, normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
            else:
                # No filters - use participant -> consent_group -> study relationship
                # Use participant -> consent_group -> study relationship, no unnecessary OPTIONAL MATCHes
                values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE {not_null_check}
        WITH p.participant_id AS participant_id, st.study_id AS study_id, 
             CASE 
               WHEN {field_value_expr} IS NULL THEN []
               ELSE 
                 // Wrap in list for UNWIND - works for both strings and lists
                 [{field_value_expr}]
             END as field_values
        UNWIND field_values as value
        WITH participant_id, study_id, value,
             CASE 
               WHEN '{field}' = 'sex' THEN{normalization_expr}
               ELSE toString(value)
             END as normalized_value
        WITH DISTINCT participant_id, study_id, normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_field Cypher queries",
            field=field,
            params_count=len(params)
        )
        
        # Execute all three queries with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records = []
        
        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0
                
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing_count = missing_records[0].get("missing", 0) if missing_records else 0
                
                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format values results
        counts = []
        for record in values_records:
            value = record.get("value")
            # Apply field mapping if needed (e.g., "Not Reported" -> "Not reported" for vital_status)
            if value is not None:
                mapped_value = map_field_value(field, value)
            else:
                mapped_value = value
            counts.append({
                "value": mapped_value,
                "count": record.get("count", 0)
            })
        
        logger.debug(
            "Completed subject count by field",
            field=field,
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def _count_subjects_by_race(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by race.
        
        For race values like "Asian;White", the participant is counted
        for both "Asian" and "White".
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of race counts)
        """
        logger.debug("Counting subjects by race with enum validation", filters=filters)
        
        # Get all valid race enum values
        valid_races = Race.values()
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            where_conditions.append("""(
                ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
                OR ANY(key IN keys(p.metadata.unharmonized) 
                       WHERE toLower(key) CONTAINS 'diagnos' 
                       AND toLower(toString(p.metadata.unharmonized[key])) CONTAINS toLower($diagnosis_search_term))
            )""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters (excluding race since we're counting by race)
        for filter_field, value in filters.items():
            if filter_field == "race":
                continue  # Skip race filter when counting by race
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            where_clause = "WHERE " + " AND ".join(filtered_conditions) if filtered_conditions else ""
        else:
            where_clause = ""
        
        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # When no filters, use required MATCH for study (same as summary query) to avoid duplicates
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(where_clause, "st IS NOT NULL")
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # No filters - use required MATCH for study (same as summary query)
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        
        # Query 2: Get count of participant + study combinations with null or empty race value
        # Note: Missing will be calculated as total - sum(values) to ensure accuracy
        # This query only counts NULL and empty strings for efficiency
        # Invalid race values (that don't match valid races) will be included in missing via calculation
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(
                where_clause, 
                "st IS NOT NULL AND (p.race IS NULL OR toString(p.race) = '' OR trim(toString(p.race)) = '')"
            )
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        else:
            # No filters - use required MATCH for study (same as summary query)
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            missing_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE (p.race IS NULL OR toString(p.race) = '' OR trim(toString(p.race)) = '')
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        
        # Query 3: Create a single query that counts distinct participant + study combinations for each valid race
        # Special handling: if race is only "Hispanic or Latino", count as "Not Reported"
        # Split race by semicolon, remove "Hispanic or Latino", then match against valid races
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        params["valid_races"] = valid_races
        
        # When no filters, use required MATCH for study (same as summary query) to avoid duplicates
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(
                where_clause,
                "st IS NOT NULL AND p.race IS NOT NULL AND toString(p.race) <> '' AND trim(toString(p.race)) <> ''"
            )
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Data is always string format with semicolon separator
             CASE 
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 WHEN race_raw IS NOT NULL THEN [trim(toString(race_raw))]
                 ELSE []
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id, race_raw, race_list_filtered, had_hispanic
        // Process race values: if only "Hispanic or Latino", convert to "Not Reported" if valid
        // Otherwise, use the filtered race values
        WITH participant_id, study_id,
             CASE 
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, race_candidate
        WHERE race_candidate IN $valid_races
        WITH DISTINCT participant_id, study_id, race_candidate as race_value
        RETURN race_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # No filters - use required MATCH for study (same as summary query)
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NOT NULL 
          AND toString(p.race) <> '' 
          AND trim(toString(p.race)) <> ''
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Data is always string format with semicolon separator
             CASE 
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 WHEN race_raw IS NOT NULL THEN [trim(toString(race_raw))]
                 ELSE []
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id, race_raw, race_list_filtered, had_hispanic
        // Process race values: if only "Hispanic or Latino", convert to "Not Reported" if valid
        // Otherwise, use the filtered race values
        WITH participant_id, study_id,
             CASE 
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, race_candidate
        WHERE race_candidate IN $valid_races
        WITH DISTINCT participant_id, study_id, race_candidate as race_value
        RETURN race_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        # Query 4: Count unique participant+study combinations with valid race values
        # This is needed to calculate missing correctly (total - unique_with_valid_race)
        # Count BEFORE UNWIND to avoid counting same participant multiple times
        if where_clause or identifiers_condition:
            # Combine where_clause with race conditions safely using utility
            race_conditions = "st IS NOT NULL AND p.race IS NOT NULL AND toString(p.race) <> '' AND trim(toString(p.race)) <> ''"
            combined_where = combine_where_clauses(where_clause, race_conditions)
            
            unique_with_valid_race_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Data is always string format with semicolon separator
             CASE 
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 WHEN race_raw IS NOT NULL THEN [trim(toString(race_raw))]
                 ELSE []
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id, race_raw, race_list_filtered, had_hispanic
        // Process race values: if only "Hispanic or Latino", convert to "Not Reported" if valid
        // Otherwise, use the filtered race values
        WITH participant_id, study_id,
             CASE 
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, race_candidate
        WHERE race_candidate IN $valid_races
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as unique_count
        """.strip()
        else:
            # No filters - use required MATCH for study (same as summary query)
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            unique_with_valid_race_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NOT NULL 
          AND toString(p.race) <> '' 
          AND trim(toString(p.race)) <> ''
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Data is always string format with semicolon separator
             CASE 
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 WHEN race_raw IS NOT NULL THEN [trim(toString(race_raw))]
                 ELSE []
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id, race_raw, race_list_filtered, had_hispanic
        // Process race values: if only "Hispanic or Latino", convert to "Not Reported" if valid
        // Otherwise, use the filtered race values
        WITH participant_id, study_id,
             CASE 
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, race_candidate
        WHERE race_candidate IN $valid_races
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as unique_count
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_race Cypher queries",
            race_count=len(valid_races),
            params_count=len(params)
        )
        
        # Execute queries with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        total_count = 0
        unique_with_valid_race_count = 0
        values_records = []
        
        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0
                
                # Count unique participant+study combinations with valid race values
                unique_result = await self.session.run(unique_with_valid_race_cypher, params)
                unique_records = []
                async for record in unique_result:
                    unique_records.append(dict(record))
                await unique_result.consume()
                unique_with_valid_race_count = unique_records[0].get("unique_count", 0) if unique_records else 0
                
                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results - only include races with count > 0
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}
        
        # Build final counts list - only include races with count > 0
        counts = []
        for race_value in valid_races:
            count = counts_by_value.get(race_value, 0)
            if count > 0:
                counts.append({
                    "value": race_value,
                    "count": count
                })
        
        # Sort by count descending (numeric), then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        # Calculate missing as total - unique participant+study combinations with valid race values
        # This is correct because:
        # - Participants with multiple race values (e.g., "Asian;White") are counted multiple times in values
        # - But we only want to count each participant+study combination ONCE when calculating missing
        # - So: missing = total - unique_with_valid_race_count
        calculated_missing = max(0, total_count - unique_with_valid_race_count)
        
        logger.info(
            "Completed subject count by race",
            total=total_count,
            unique_with_valid_race=unique_with_valid_race_count,
            missing_calculated=calculated_missing,
            sum_values=sum(counts_by_value.values()),
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": calculated_missing,  # Use calculated missing to ensure total = unique_with_valid_race + missing
            "values": counts
        }
    
    async def _count_subjects_by_ethnicity(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by ethnicity (derived from race field).
        
        Ethnicity is determined from race:
        - If race contains 'Hispanic or Latino' → 'Hispanic or Latino'
        - Otherwise → 'Not reported'
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list with only 2 ethnicity options)
        """
        logger.debug("Counting subjects by ethnicity (derived from race)", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            where_conditions.append("""(
                ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
                OR ANY(key IN keys(p.metadata.unharmonized) 
                       WHERE toLower(key) CONTAINS 'diagnos' 
                       AND toLower(toString(p.metadata.unharmonized[key])) CONTAINS toLower($diagnosis_search_term))
            )""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters (excluding ethnicity since we're counting by ethnicity)
        for filter_field, value in filters.items():
            if filter_field == "ethnicity":
                continue  # Skip ethnicity filter when counting by ethnicity
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            where_clause = "WHERE " + " AND ".join(filtered_conditions) if filtered_conditions else ""
        else:
            where_clause = ""
        
        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_total = combine_where_clauses(where_clause, "st IS NOT NULL")
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_total}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        
        # Query 2: Get count of participant + study combinations with null race (missing ethnicity)
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_missing = combine_where_clauses(where_clause, "st IS NOT NULL AND p.race IS NULL")
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_missing}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            missing_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        
        # Query 3: Count by ethnicity (derived from race)
        # Use participant_id + study_id as unique identifier
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_values = combine_where_clauses(where_clause, "st IS NOT NULL AND p.race IS NOT NULL")
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_values}
        WITH p.participant_id AS participant_id, st.study_id AS study_id, toString(p.race) as race
        WITH participant_id, study_id, race,
             CASE 
               WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        WITH DISTINCT participant_id, study_id, ethnicity_value
        RETURN ethnicity_value as value, count(*) as count
        ORDER BY value ASC
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            values_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             CASE 
               WHEN toString(race_raw) CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        WITH DISTINCT participant_id, study_id, ethnicity_value
        RETURN ethnicity_value as value, count(*) as count
        ORDER BY value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_ethnicity Cypher queries",
            params_count=len(params)
        )
        
        # Execute all three queries with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records = []
        
        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0
                
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing_count = missing_records[0].get("missing", 0) if missing_records else 0
                
                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results - ensure both ethnicity options are included (even with 0 count)
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}
        
        # Build final counts list with both ethnicity options
        ethnicity_options = ["Hispanic or Latino", "Not reported"]
        counts = []
        for ethnicity_value in ethnicity_options:
            counts.append({
                "value": ethnicity_value,
                "count": counts_by_value.get(ethnicity_value, 0)
            })
        
        # Sort by ethnicity value alphabetically
        counts.sort(key=lambda x: x["value"])
        
        logger.info(
            "Completed subject count by ethnicity",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def _count_subjects_by_associated_diagnoses(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by associated diagnoses.
        
        For participants with multiple diagnoses, the participant is counted
        for each diagnosis they have.
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of diagnosis counts)
        """
        logger.debug("Counting subjects by associated diagnoses", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")
        
        # Handle diagnosis search - we skip this for diagnosis counting since we're counting by diagnosis
        if "_diagnosis_search" in filters:
            filters.pop("_diagnosis_search")  # Remove it to avoid circular filtering
        
        # Add regular filters (excluding associated_diagnoses since we're counting by it)
        for filter_field, value in filters.items():
            if filter_field == "associated_diagnoses":
                continue  # Skip diagnosis filter when counting by diagnosis
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            where_clause = "WHERE " + " AND ".join(filtered_conditions) if filtered_conditions else ""
        else:
            where_clause = ""
        
        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Only include OPTIONAL MATCHes if we have filters that need them
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(where_clause, "st IS NOT NULL")
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # No filters - simple count
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        
        # Query 2: Get count of participant + study combinations with no diagnoses (missing)
        # Optimized to avoid unnecessary OPTIONAL MATCHes
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(where_clause, "st IS NOT NULL")
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        // combined_where includes "st IS NOT NULL", so st is guaranteed to be non-NULL here
        WITH p.participant_id AS participant_id, st.study_id AS study_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(*) as missing
        """.strip()
        else:
            # No filters - simple check for missing diagnoses
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            missing_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p.participant_id AS participant_id, st.study_id AS study_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(*) as missing
        """.strip()
        
        # Query 3: Count by diagnosis values
        # d.diagnosis is a STRING (not a list) - each diagnosis node has one diagnosis value
        # Multiple diagnosis nodes can link to one participant, so each contributes one value
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(p:participant)
        # Use participant_id + study_id as unique identifier
        if where_clause or (identifiers_condition and identifiers_condition.strip()):
            # Has filters - need to apply them but keep it efficient
            values_cypher = f"""
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, d.diagnosis as diagnosis_value
        MATCH (p2:participant {{participant_id: participant_id}})
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p2)
        OPTIONAL MATCH (p2)-[:of_participant]->(c2:consent_group)-[:of_consent_group]->(st2:study)
        WHERE st2.study_id = study_id
        WITH p2, s, c2, st2, participant_id, study_id, diagnosis_value{identifiers_condition}
        {combine_where_clauses(where_clause, "toString(diagnosis_value) <> ''")}
        WITH DISTINCT participant_id, study_id, toString(diagnosis_value) as value
        RETURN value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # No filters - use participant_id + study_id
            values_cypher = """
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, d.diagnosis as diagnosis_value
        WHERE toString(diagnosis_value) <> ''
        WITH DISTINCT participant_id, study_id, toString(diagnosis_value) as value
        RETURN value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_associated_diagnoses Cypher queries",
            values_query=values_cypher,
            params_count=len(params),
            has_identifiers_condition=bool(identifiers_condition),
            has_where_clause=bool(where_clause)
        )
        
        # Execute all three queries with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records = []
        
        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0
                
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing_count = missing_records[0].get("missing", 0) if missing_records else 0
                
                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results
        counts = []
        for record in values_records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })
        
        # Sort by count descending, then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        logger.info(
            "Completed subject count by associated diagnoses",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def get_subjects_summary(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for subjects.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting subjects summary", filters=filters)
        # Avoid mutating the caller's dict
        filters = dict(filters or {})
        
        # Initialize cypher to avoid "variable not associated with value" error
        cypher = None
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle race parameter normalization
        # Race filter must be applied after WITH clause defines variables
        race_condition = ""
        race_filter_condition = ""
        race_where_clause = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                # Normalize race value to a list (handle both string and list inputs)
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []
                
                if race_list:
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    
                    # Check if "Not Reported" is in the filter - if so, also match "Hispanic or Latino" only records
                    includes_not_reported = any(r.strip() == "Not Reported" for r in race_list)
                    
                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    
                    if includes_not_reported:
                        # Match either: "Not Reported" in original values OR "Hispanic or Latino" only (which converts to "Not Reported")
                        race_filter_condition = """(reduce(found = false, tok IN race_tokens | found OR tok IN pr_tokens) OR 
                        (size(pr_tokens) > 0 AND reduce(all_hispanic = true, pt IN pr_tokens | all_hispanic AND pt = 'Hispanic or Latino') AND 'Not Reported' IN race_tokens))"""
                    else:
                        # Normal matching - exclude "Hispanic or Latino" from matching since it's removed in conversion
                        # Check if any race_token matches a pr_token that is not 'Hispanic or Latino'
                        # Use a simpler approach: check each token individually
                        race_filter_condition = "reduce(found = false, tok IN race_tokens | found OR (tok IN pr_tokens AND tok <> 'Hispanic or Latino'))"
        if race_filter_condition:
            race_where_clause = f"WHERE {race_filter_condition}"
        
        # Handle identifiers parameter normalization
        # Support || separator for OR logic (e.g., "SUBJ001 || SUBJ002")
        # OPTIMIZATION: Apply identifiers filter EARLY in MATCH WHERE clause to reduce dataset before OPTIONAL MATCHes
        identifiers_condition = ""
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            identifiers_list = self._split_or_values(identifiers_value)
            if identifiers_list:
                identifiers_value = identifiers_list[0] if len(identifiers_list) == 1 else identifiers_list
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                    // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                    CASE
                      WHEN ${id_param} IS NULL THEN NULL
                      WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                      WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                      ELSE []
                    END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")
                # Set early filter for optimization (can be used in MATCH WHERE clause)
                # For early filter, we need to handle both LIST and STRING cases
                if isinstance(identifiers_value, list):
                    identifiers_early_filter = f"p.participant_id IN ${id_param}"
                else:
                    identifiers_early_filter = f"p.participant_id = ${id_param}"
        
        # Handle depositions filter (study_id)
        # Support || separator for OR logic (e.g., "phs001 || phs002")
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            depositions_list = self._split_or_values(depositions_value)
            if depositions_list:
                param_counter += 1
                dep_param = f"param_{param_counter}"
                # Filter by study_id - participants must belong to the specified study
                if len(depositions_list) == 1:
                    params[dep_param] = depositions_list[0]
                    deposition_operator = "="
                else:
                    params[dep_param] = depositions_list
                    deposition_operator = "IN"
        
        # Handle diagnosis search - will be applied after diagnosis collection
        diagnosis_search_term = None
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            params["diagnosis_search_term"] = diagnosis_search_term
            # Don't add to where_conditions yet - will be applied after diagnosis collection

        # Build diagnosis search fragment to be inserted before final count
        # This fragment assumes 'p' (participant node), 'participant_id', and 'study_id' are in scope
        diagnosis_search_fragment = ""
        if diagnosis_search_term:
            diagnosis_search_fragment = """
        // Apply diagnosis search filter
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(diag_search:diagnosis)
        WITH participant_id, study_id, collect(DISTINCT diag_search) AS diag_search_nodes
        WHERE size([dn IN diag_search_nodes WHERE dn IS NOT NULL AND ANY(diag IN CASE WHEN valueType(dn.diagnosis) = 'LIST' THEN dn.diagnosis ELSE [dn.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0"""

        # Map API sex values to database values (M -> Male, F -> Female, U -> Not Reported)
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            # If the value is a normalized API value, map it to database value
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]
        
        # Field name mapping for participant properties
        # Some API field names don't match the database property names
        field_name_mapping = {
            "sex": "sex_at_birth",
            # Add other mappings as needed
        }
        
        # Separate derived fields (calculated after WITH clauses) from direct participant fields
        derived_filters = {}
        derived_conditions = []
        
        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            # Ethnicity is derived from race but can be expressed safely as a predicate on p.race.
            # IMPORTANT: Do not treat it as a derived-field filter here; otherwise fast paths can skip it.
            if field == "ethnicity":
                desired = str(value).strip() if value is not None else ""
                desired_lower = desired.lower()
                param_counter += 1
                hisp_param = f"param_{param_counter}"
                params[hisp_param] = "Hispanic or Latino"
                if desired_lower == "hispanic or latino":
                    where_conditions.append(f"(p.race IS NOT NULL AND toString(p.race) CONTAINS ${hisp_param})")
                else:
                    where_conditions.append(f"(p.race IS NULL OR trim(toString(p.race)) = '' OR NOT toString(p.race) CONTAINS ${hisp_param})")
                continue

            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status"}:
                derived_filters[field] = value
                continue
            
            # Map API field name to database property name
            db_field = field_name_mapping.get(field, field)
                
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause for direct participant fields
        where_clause = ""
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)
        
        # Note: race_filter_condition is NOT added to where_clause here
        # It will be added separately to with_where_conditions where race_tokens is in scope
        # (race_tokens is defined in the WITH clause via race_condition)
        
        # Build WHERE clause for derived fields (after calculation)
        if derived_filters:
            for field, value in derived_filters.items():
                param_counter += 1
                param_name = f"derived_{field}_{param_counter}"
                if field == "vital_status":
                    # Check if value is a database-only value (e.g., "Not Reported" with capital R)
                    if is_database_only_value("vital_status", value):
                        # This value is a database-only value and is not valid for filtering
                        # Add impossible condition to return empty results
                        derived_conditions.append("false")
                    else:
                        # Case-insensitive match against DB values (do NOT match NULL/missing).
                        # This ensures we only count explicit "Not Reported" values, not missing data.
                        derived_conditions.append(
                            f"(final_vital_status IS NOT NULL AND toLower(toString(final_vital_status)) = toLower(toString(${param_name})))"
                        )
                        # Apply reverse mapping for vital_status filter (e.g., "Not reported" -> "Not Reported")
                        value = reverse_map_field_value("vital_status", value) if value else value
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                    # Convert age to integer since it's stored as int in database
                    try:
                        value = int(value) if value is not None else value
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
            # Also filter out any conditions that reference study_id (not defined yet in non-depositions path)
            filtered_derived = [c for c in filtered_derived if 'study_id' not in c.lower()]
            if filtered_derived:
                derived_where_clause = "WHERE " + " AND ".join(filtered_derived)
        
        # Helper function to build identifiers_with_clause
        def build_identifiers_with_clause(identifiers_condition):
            """Build the WITH clause for identifiers condition."""
            if not identifiers_condition:
                return ""
            if identifiers_condition.startswith(","):
                case_statement = identifiers_condition[1:].strip()
                return f"""
        WITH {case_statement}"""
            return identifiers_condition
        
        # Helper function to build filtered where clause (without identifiers)
        def build_filtered_where_clause(where_clause):
            """Build WHERE clause without identifiers condition."""
            if not where_clause:
                return ""
            filtered = where_clause.replace("WHERE p.participant_id IN id_list", "").replace("AND p.participant_id IN id_list", "").strip()
            if not filtered:
                return ""
            if not filtered.startswith("WHERE"):
                filtered = "WHERE " + filtered
            return filtered
        
        # Determine if we need survival processing
        needs_survival_processing = derived_filters and ("vital_status" in derived_filters or "age_at_vital_status" in derived_filters)
        needs_ethnicity_processing = derived_filters and "ethnicity" in derived_filters

        # Build survival processing logic if needed
        survival_processing = ""
        if needs_survival_processing:
            # IMPORTANT: This fragment must NOT reference `c` (consent_group). `get_subjects_summary`
            # uses the participant -> consent_group -> study relationship, so `c` is not bound in these queries.
            survival_processing = """
WITH p, d, st, 
     // Collect all survival records for this participant
     collect(s) AS survival_records
WITH p, d, st, survival_records,
     // Keep only records with a status
     [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
WITH p, d, st, survs,
     // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age,
     // Check if all ages are -999 (for counting as missing)
    all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
    // Check if Dead age is -999
    any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
WITH p, d, st,
     // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
     // If no record matches max_age, fall back to first available status
     CASE 
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead THEN 'Dead'
       ELSE CASE
         WHEN head([sr IN survs 
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                     | sr.last_known_survival_status]) IS NOT NULL
         THEN head([sr IN survs 
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                     | sr.last_known_survival_status])
         ELSE head([sr IN survs | sr.last_known_survival_status])
       END
     END AS final_vital_status,
     // Age: If no vital_status, age should also be NULL
     // If all ages are -999, or Dead age is -999, set to NULL (count as missing)
     // Otherwise use max Dead age or max age
     CASE 
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead AND dead_age_is_999 THEN NULL
       WHEN has_dead THEN max_dead_age
       WHEN all_ages_are_999 THEN NULL
       ELSE max_age
     END AS final_age_at_vital_status"""

        # Build query - if we have derived filters, we need to calculate them first
        if needs_survival_processing:
            # Calculate survival records for vital_status and age_at_vital_status filters
            # For identifiers filter, deduplicate early to avoid counting same participant multiple times
            if identifiers_condition:
                # Apply identifiers filter and deduplicate BEFORE OPTIONAL MATCH to avoid duplicates from multiple studies
                # Build WHERE clause without identifiers condition (already applied)
                where_clause_filtered = where_clause.replace("WHERE p.participant_id IN id_list", "").replace("AND p.participant_id IN id_list", "").replace("p.participant_id IN id_list AND", "").replace("p.participant_id IN id_list", "").strip() if where_clause else ""
                # Clean up any "WHERE AND" or "AND AND" issues - do this multiple times to catch all cases
                while "WHERE AND" in where_clause_filtered or "AND AND" in where_clause_filtered:
                    where_clause_filtered = where_clause_filtered.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                # Remove leading AND
                while where_clause_filtered.startswith("AND "):
                    where_clause_filtered = where_clause_filtered[4:].strip()
                # Remove trailing AND
                while where_clause_filtered.endswith(" AND"):
                    where_clause_filtered = where_clause_filtered[:-4].strip()
                # Ensure it starts with WHERE if it has content (but only if it's not empty)
                if where_clause_filtered and where_clause_filtered.strip() and where_clause_filtered != "WHERE" and where_clause_filtered != "AND":
                    if not where_clause_filtered.startswith("WHERE"):
                        where_clause_filtered = "WHERE " + where_clause_filtered
                elif not where_clause_filtered or where_clause_filtered == "WHERE" or where_clause_filtered == "AND":
                    where_clause_filtered = ""
                
                # Create a WITH clause version of identifiers_condition for this query
                # Extract the CASE statement from identifiers_condition and format it properly
                if identifiers_condition.startswith(","):
                    # Remove leading comma and create proper WITH clause
                    case_statement = identifiers_condition[1:].strip()  # Remove comma and whitespace
                    # First compute id_list, then match participants
                    identifiers_with_clause = f"""
        WITH {case_statement}"""
                else:
                    identifiers_with_clause = identifiers_condition
                
                # When depositions filter is present, use required MATCH for studies (before OPTIONAL MATCH)
                if dep_param:
                    # Remove depositions filter from where_clause_filtered since we're using required MATCH
                    # The depositions filter will be applied directly after the MATCH for studies
                    dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                    where_clause_no_dep = where_clause_filtered
                    if where_clause_no_dep:
                        # Remove the depositions filter condition
                        where_clause_no_dep = where_clause_no_dep.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip()
                        # Clean up any double WHERE or AND issues - do this multiple times to catch all cases
                        while "WHERE AND" in where_clause_no_dep or "AND AND" in where_clause_no_dep:
                            where_clause_no_dep = where_clause_no_dep.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                        # Remove leading AND
                        while where_clause_no_dep.startswith("AND "):
                            where_clause_no_dep = where_clause_no_dep[4:].strip()
                        # Remove trailing AND
                        while where_clause_no_dep.endswith(" AND"):
                            where_clause_no_dep = where_clause_no_dep[:-4].strip()
                        # If it's just "WHERE" or empty or "AND", make it empty
                        if where_clause_no_dep == "WHERE" or where_clause_no_dep == "AND" or not where_clause_no_dep.strip():
                            where_clause_no_dep = ""
                        # Ensure it starts with WHERE if it has content (but only if it's not empty)
                        elif where_clause_no_dep and not where_clause_no_dep.startswith("WHERE"):
                            where_clause_no_dep = "WHERE " + where_clause_no_dep
                    
                    # Combine depositions filter with other filters if any
                    combined_where = f"WHERE st.study_id {deposition_operator} ${dep_param}"
                    if where_clause_no_dep and where_clause_no_dep.strip() and where_clause_no_dep != "WHERE":
                        # Remove WHERE from where_clause_no_dep and combine with depositions filter
                        other_filters = where_clause_no_dep.replace("WHERE", "").strip()
                        # Clean up any leading/trailing AND operators
                        while other_filters.startswith("AND "):
                            other_filters = other_filters[4:].strip()
                        while other_filters.endswith(" AND"):
                            other_filters = other_filters[:-4].strip()
                        # Clean up "WHERE AND" if it somehow appears
                        other_filters = other_filters.replace("WHERE AND", "").replace("AND AND", "AND").strip()
                        # Remove any remaining leading/trailing AND after cleanup
                        while other_filters.startswith("AND "):
                            other_filters = other_filters[4:].strip()
                        while other_filters.endswith(" AND"):
                            other_filters = other_filters[:-4].strip()
                        # Only add if we have valid content
                        if other_filters and other_filters != "AND" and other_filters.strip():
                            combined_where = f"WHERE st.study_id {deposition_operator} ${dep_param} AND {other_filters}"
                    
                    cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        // Use participant -> consent_group -> study relationship
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        {combined_where}
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH participant_id, p, d, st{race_condition},
             collect(s) AS survival_records
        WITH participant_id, p, d, st,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, d, st, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE max_age 
                    END) AS max_age
        WITH participant_id, p, d, st,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        WITH DISTINCT participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                else:
                    # No depositions filter - use OPTIONAL MATCH for studies
                    cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH participant_id, p, collect(s) AS survival_records
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Bind studies and count per (participant_id, study_id) pair.
        // IMPORTANT: avoid carrying a LIST of study IDs through long WITH/aggregation chains
        // (Memgraph can drop list vars and report them as \"Unbound variable\").
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH participant_id, p, survival_records, diagnosis_nodes, st.study_id AS study_id
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        // Apply filters
        WITH participant_id, p, survival_records, diagnosis_nodes, study_id{race_condition}
        {where_clause_filtered}
        WITH participant_id, p, diagnosis_nodes, study_id,
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, diagnosis_nodes, study_id, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE max_age 
                    END) AS max_age
        WITH participant_id, study_id, p, survs, has_dead, max_dead_age, max_age,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
            else:
                # No identifiers condition - use original query
                # When depositions filter is present, use required MATCH for studies (before OPTIONAL MATCH)
                if dep_param:
                    # Clean where_clause to remove any study_id (variable) references since it's not defined yet
                    where_clause_clean_for_summary = where_clause
                    if where_clause_clean_for_summary:
                        # Remove any references to study_id (the variable) since it's not defined yet
                        where_clause_clean_for_summary = where_clause_clean_for_summary.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                        # Clean up any resulting "AND AND" or trailing/leading AND
                        where_clause_clean_for_summary = where_clause_clean_for_summary.replace("AND AND", "AND").strip()
                        while where_clause_clean_for_summary.startswith("AND "):
                            where_clause_clean_for_summary = where_clause_clean_for_summary[4:].strip()
                        while where_clause_clean_for_summary.endswith(" AND"):
                            where_clause_clean_for_summary = where_clause_clean_for_summary[:-4].strip()
                        if where_clause_clean_for_summary == "WHERE" or where_clause_clean_for_summary == "AND" or not where_clause_clean_for_summary.strip():
                            where_clause_clean_for_summary = ""
                    
                    cypher = f"""
        MATCH (p:participant)
        // Use participant -> consent_group -> study relationship
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH DISTINCT p.participant_id AS participant_id, p, s, d, st{race_condition}{identifiers_condition}
        {where_clause_clean_for_summary}
        WITH participant_id, p, d, st,
             collect(s) AS survival_records
        WITH participant_id, p, d, st,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, d, st, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999) 
                      ELSE max_age 
                    END) AS max_age
        WITH participant_id, p, d, st,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs 
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age 
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        WITH DISTINCT participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                else:
                    # No depositions filter - use OPTIONAL MATCH approach like count endpoint
                    # Clean where_clause to remove any study_id (variable) references since it's not defined yet
                    where_clause_clean_for_summary = where_clause
                    if where_clause_clean_for_summary:
                        # Remove any references to study_id (the variable) since it's not defined yet
                        where_clause_clean_for_summary = where_clause_clean_for_summary.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                        # Clean up any resulting "AND AND" or trailing/leading AND
                        where_clause_clean_for_summary = where_clause_clean_for_summary.replace("AND AND", "AND").strip()
                        while where_clause_clean_for_summary.startswith("AND "):
                            where_clause_clean_for_summary = where_clause_clean_for_summary[4:].strip()
                        while where_clause_clean_for_summary.endswith(" AND"):
                            where_clause_clean_for_summary = where_clause_clean_for_summary[:-4].strip()
                        if where_clause_clean_for_summary == "WHERE" or where_clause_clean_for_summary == "AND" or not where_clause_clean_for_summary.strip():
                            where_clause_clean_for_summary = ""
                    
                    cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, st{race_condition}
        {where_clause_clean_for_summary}{survival_processing}
        WHERE st IS NOT NULL
        WITH p.participant_id as participant_id, st.study_id as study_id, final_vital_status, final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        WITH participant_id, study_id,
             head(collect(final_vital_status)) AS final_vital_status,
             head(collect(final_age_at_vital_status)) AS final_age_at_vital_status,
             head(collect(ethnicity_value)) AS ethnicity_value
        {derived_where_clause}
        // Note: Cannot apply diagnosis search here as 'p' is no longer in scope after head(collect())
        // Diagnosis search would need to be applied earlier in the query if needed with survival filters
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
        else:
            # Simple query without survival processing
            # Deduplicate by participant_id only to avoid duplicates from OPTIONAL MATCH relationships
            if where_clause or race_condition or identifiers_condition or derived_where_clause:
                # If we have ethnicity filter, we need to calculate it first
                if "ethnicity" in derived_filters:
                    # When identifiers are used, ensure we count unique participants
                    # Deduplicate early to avoid counting the same participant multiple times
                    if identifiers_condition:
                        # Build WHERE clause without identifiers condition (already applied)
                        where_clause_filtered = where_clause.replace("WHERE p.participant_id IN id_list", "").replace("AND p.participant_id IN id_list", "").replace("p.participant_id IN id_list AND", "").replace("p.participant_id IN id_list", "").strip() if where_clause else ""
                        # Clean up any "WHERE AND" or "AND AND" issues - do this multiple times to catch all cases
                        while "WHERE AND" in where_clause_filtered or "AND AND" in where_clause_filtered:
                            where_clause_filtered = where_clause_filtered.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                        # Remove leading AND
                        while where_clause_filtered.startswith("AND "):
                            where_clause_filtered = where_clause_filtered[4:].strip()
                        # Remove trailing AND
                        while where_clause_filtered.endswith(" AND"):
                            where_clause_filtered = where_clause_filtered[:-4].strip()
                        # Ensure it starts with WHERE if it has content (but only if it's not empty)
                        if where_clause_filtered and where_clause_filtered.strip() and where_clause_filtered != "WHERE" and where_clause_filtered != "AND":
                            if not where_clause_filtered.startswith("WHERE"):
                                where_clause_filtered = "WHERE " + where_clause_filtered
                        elif not where_clause_filtered or where_clause_filtered == "WHERE" or where_clause_filtered == "AND":
                            where_clause_filtered = ""
                        
                        # Create a WITH clause version of identifiers_condition for this query
                        # Extract the CASE statement from identifiers_condition and format it properly
                        if identifiers_condition.startswith(","):
                            # Remove leading comma and create proper WITH clause
                            case_statement = identifiers_condition[1:].strip()  # Remove comma and whitespace
                            # First compute id_list, then match participants
                            identifiers_with_clause = f"""
        WITH {case_statement}"""
                        else:
                            identifiers_with_clause = identifiers_condition
                        
                        # Handle depositions filter if present
                        if dep_param:
                            # Remove depositions filter from where_clause_filtered since we'll use required MATCH
                            other_filters = where_clause_filtered.replace(f"st.study_id {deposition_operator} ${dep_param}", "").replace(f"AND st.study_id {deposition_operator} ${dep_param}", "").replace(f"st.study_id {deposition_operator} ${dep_param} AND", "").strip() if where_clause_filtered else ""
                            # Clean up any double WHERE or AND issues
                            if other_filters:
                                other_filters = other_filters.replace("WHERE WHERE", "WHERE").replace("AND AND", "AND").replace("  ", " ").strip()
                                # Remove WHERE prefix
                                if other_filters.startswith("WHERE"):
                                    other_filters = other_filters[6:].strip()  # Remove "WHERE " prefix
                                # Remove leading AND
                                while other_filters.startswith("AND "):
                                    other_filters = other_filters[4:].strip()
                                # Remove trailing AND
                                while other_filters.endswith(" AND"):
                                    other_filters = other_filters[:-4].strip()
                                # Final check - if empty after cleanup, set to empty string
                                if not other_filters or other_filters == "AND":
                                    other_filters = ""
                            
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}"
                            if other_filters:
                                with_clause += f"\n        WHERE {other_filters}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}
        WITH participant_id, p, study_id,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                        else:
                            # No depositions filter - use OPTIONAL MATCH for studies
                            # Build WITH clause with optional WHERE
                            # Note: st comes from OPTIONAL MATCH, so we need to handle NULL case
                            # Remove any st.study_id conditions from where_clause_filtered since st might be NULL
                            where_clause_clean = where_clause_filtered
                            if where_clause_clean:
                                # Remove st.study_id conditions (depositions filter) - not applicable with OPTIONAL MATCH
                                where_clause_clean = where_clause_clean.replace("st.study_id =", "").replace("st.study_id IN", "").replace("st.study_id=", "").replace("st.study_idIN", "")
                                # Clean up any resulting "AND AND" or trailing/leading AND
                                where_clause_clean = where_clause_clean.replace("AND AND", "AND").strip()
                                while where_clause_clean.startswith("AND "):
                                    where_clause_clean = where_clause_clean[4:].strip()
                                while where_clause_clean.endswith(" AND"):
                                    where_clause_clean = where_clause_clean[:-4].strip()
                                if where_clause_clean == "WHERE" or where_clause_clean == "AND" or not where_clause_clean.strip():
                                    where_clause_clean = ""
                            
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}"
                            if where_clause_clean:
                                # Remove "WHERE " prefix if present
                                where_conditions = where_clause_clean.replace("WHERE ", "").strip()
                                # Remove any references to study_id (the variable) since it's being defined in this WITH clause
                                # and WHERE clause is evaluated after variable assignments, but we want to be safe
                                where_conditions = where_conditions.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                                # Clean up any resulting "AND AND" or trailing/leading AND
                                where_conditions = where_conditions.replace("AND AND", "AND").strip()
                                while where_conditions.startswith("AND "):
                                    where_conditions = where_conditions[4:].strip()
                                while where_conditions.endswith(" AND"):
                                    where_conditions = where_conditions[:-4].strip()
                                if where_conditions and where_conditions.strip() and where_conditions != "AND":
                                    with_clause += f"\n        WHERE {where_conditions}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        {with_clause}
        WITH participant_id, p, study_id,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                    else:
                        # No identifiers - handle depositions filter if present
                        if dep_param:
                            # Remove depositions filter from where_clause since it's already in the MATCH WHERE
                            dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                            other_filters = where_clause.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause else ""
                            # Clean up any double WHERE or AND issues - do this multiple times to catch all cases
                            while "WHERE AND" in other_filters or "AND AND" in other_filters:
                                other_filters = other_filters.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                            # Remove leading AND
                            while other_filters.startswith("AND "):
                                other_filters = other_filters[4:].strip()
                            # Remove trailing AND
                            while other_filters.endswith(" AND"):
                                other_filters = other_filters[:-4].strip()
                            # Remove WHERE prefix if present
                            if other_filters.startswith("WHERE"):
                                other_filters = other_filters[6:].strip()
                            # If it's just "WHERE" or empty or "AND", make it empty
                            if other_filters == "WHERE" or other_filters == "AND" or not other_filters.strip():
                                other_filters = ""
                            
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}"
                            if other_filters and other_filters.strip():
                                with_clause += f"\n        WHERE {other_filters}"
                            
                            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}
        WITH participant_id, p, study_id,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                        else:
                            # Handle depositions filter if present
                            if dep_param:
                                # Remove depositions filter from where_clause since we'll use required MATCH
                                other_filters = where_clause.replace(f"st.study_id {deposition_operator} ${dep_param}", "").replace(f"AND st.study_id {deposition_operator} ${dep_param}", "").replace(f"st.study_id {deposition_operator} ${dep_param} AND", "").strip() if where_clause else ""
                                # Clean up any double WHERE or AND issues
                                if other_filters:
                                    other_filters = other_filters.replace("WHERE WHERE", "WHERE").replace("AND AND", "AND").replace("  ", " ").strip()
                                    # Remove WHERE prefix
                                    if other_filters.startswith("WHERE"):
                                        other_filters = other_filters[6:].strip()  # Remove "WHERE " prefix
                                    # Remove leading AND
                                    while other_filters.startswith("AND "):
                                        other_filters = other_filters[4:].strip()
                                    # Remove trailing AND
                                    while other_filters.endswith(" AND"):
                                        other_filters = other_filters[:-4:].strip()
                                    # Final check - if empty after cleanup, set to empty string
                                    if not other_filters or other_filters == "AND":
                                        other_filters = ""
                                
                                # Build WITH clause with optional WHERE
                                with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}{identifiers_condition}"
                                if other_filters:
                                    with_clause += f"\n        WHERE {other_filters}"
                                
                                cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}
        WITH participant_id, p, study_id,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                            else:
                                # Build WITH clause with optional WHERE
                                # Note: st comes from OPTIONAL MATCH, so we need to handle NULL case
                                # Remove any st.study_id conditions from where_clause since st might be NULL
                                where_clause_clean = where_clause
                                if where_clause_clean:
                                    # Remove st.study_id conditions (depositions filter) - not applicable with OPTIONAL MATCH
                                    where_clause_clean = where_clause_clean.replace("st.study_id =", "").replace("st.study_id IN", "").replace("st.study_id=", "").replace("st.study_idIN", "")
                                    # Clean up any resulting "AND AND" or trailing/leading AND
                                    where_clause_clean = where_clause_clean.replace("AND AND", "AND").strip()
                                    while where_clause_clean.startswith("AND "):
                                        where_clause_clean = where_clause_clean[4:].strip()
                                    while where_clause_clean.endswith(" AND"):
                                        where_clause_clean = where_clause_clean[:-4].strip()
                                    if where_clause_clean == "WHERE" or where_clause_clean == "AND" or not where_clause_clean.strip():
                                        where_clause_clean = ""
                                
                                with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}{identifiers_condition}"
                                if where_clause_clean:
                                    # Remove "WHERE " prefix if present
                                    where_conditions = where_clause_clean.replace("WHERE ", "").strip()
                                    # Remove any references to study_id (the variable) since it's being defined in this WITH clause
                                    # and WHERE clause is evaluated after variable assignments, but we want to be safe
                                    where_conditions = where_conditions.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                                    # Clean up any resulting "AND AND" or trailing/leading AND
                                    where_conditions = where_conditions.replace("AND AND", "AND").strip()
                                    while where_conditions.startswith("AND "):
                                        where_conditions = where_conditions[4:].strip()
                                    while where_conditions.endswith(" AND"):
                                        where_conditions = where_conditions[:-4].strip()
                                    if where_conditions and where_conditions.strip() and where_conditions != "AND":
                                        with_clause += f"\n        WHERE {where_conditions}"
                                
                                cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        {with_clause}
        WITH participant_id, p, study_id,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                else:
                    # When identifiers are used, ensure we count unique participants
                    # The OPTIONAL MATCH for study can create duplicates if participant belongs to multiple studies
                    # So we need to deduplicate early by participant_id
                    if identifiers_condition:
                        # Build WHERE clause without identifiers condition (already applied)
                        where_clause_filtered = where_clause.replace("WHERE p.participant_id IN id_list", "").replace("AND p.participant_id IN id_list", "").replace("p.participant_id IN id_list AND", "").replace("p.participant_id IN id_list", "").strip() if where_clause else ""
                        # Clean up any "WHERE AND" or "AND AND" issues
                        while "WHERE AND" in where_clause_filtered or "AND AND" in where_clause_filtered:
                            where_clause_filtered = where_clause_filtered.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                        # Remove leading AND
                        while where_clause_filtered.startswith("AND "):
                            where_clause_filtered = where_clause_filtered[4:].strip()
                        # Remove trailing AND
                        while where_clause_filtered.endswith(" AND"):
                            where_clause_filtered = where_clause_filtered[:-4].strip()
                        # Ensure it starts with WHERE if it has content (but only if it's not empty)
                        if where_clause_filtered and where_clause_filtered.strip() and where_clause_filtered != "WHERE" and where_clause_filtered != "AND":
                            if not where_clause_filtered.startswith("WHERE"):
                                where_clause_filtered = "WHERE " + where_clause_filtered
                        elif not where_clause_filtered or where_clause_filtered == "WHERE" or where_clause_filtered == "AND":
                            where_clause_filtered = ""
                        
                        # Create a WITH clause version of identifiers_condition for this query
                        # Extract the CASE statement from identifiers_condition and format it properly
                        if identifiers_condition.startswith(","):
                            # Remove leading comma and create proper WITH clause
                            case_statement = identifiers_condition[1:].strip()  # Remove comma and whitespace
                            # First compute id_list, then match participants
                            identifiers_with_clause = f"""
        WITH {case_statement}"""
                        else:
                            identifiers_with_clause = identifiers_condition
                        
                        # Handle depositions filter if present
                        if dep_param:
                            dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                            # Remove depositions filter from where_clause_filtered since it's already in the MATCH WHERE
                            other_filters = where_clause_filtered.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause_filtered else ""
                            # Clean up any double WHERE or AND issues - do this multiple times to catch all cases
                            while "WHERE AND" in other_filters or "AND AND" in other_filters:
                                other_filters = other_filters.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                            # Remove leading AND
                            while other_filters.startswith("AND "):
                                other_filters = other_filters[4:].strip()
                            # Remove trailing AND
                            while other_filters.endswith(" AND"):
                                other_filters = other_filters[:-4].strip()
                            # Remove WHERE prefix if present
                            if other_filters.startswith("WHERE"):
                                other_filters = other_filters[6:].strip()
                            # If it's just "WHERE" or empty or "AND", make it empty
                            if other_filters == "WHERE" or other_filters == "AND" or not other_filters.strip():
                                other_filters = ""
                            
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT participant_id, p, st.study_id AS study_id{race_condition}"
                            if other_filters and other_filters.strip():
                                with_clause += f"\n        WHERE {other_filters}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                        else:
                            # Build WITH clause with optional WHERE
                            # Need to include st.study_id AS study_id after OPTIONAL MATCH
                            with_clause = f"WITH DISTINCT participant_id, p, st.study_id AS study_id{race_condition}"
                            if where_clause_filtered:
                                # Remove "WHERE " prefix if present
                                where_conditions = where_clause_filtered.replace("WHERE ", "").strip()
                                # Remove any references to study_id (the variable) since it's being defined in this WITH clause
                                where_conditions = where_conditions.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                                # Clean up any resulting "AND AND" or trailing/leading AND
                                where_conditions = where_conditions.replace("AND AND", "AND").strip()
                                while where_conditions.startswith("AND "):
                                    where_conditions = where_conditions[4:].strip()
                                while where_conditions.endswith(" AND"):
                                    where_conditions = where_conditions[:-4].strip()
                                if where_conditions and where_conditions.strip() and where_conditions != "AND":
                                    with_clause += f"\n        WHERE {where_conditions}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        // Use participant -> consent_group -> study relationship to match the main query's relationship path
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st IS NOT NULL"}
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                    else:
                        # No identifiers - handle depositions filter if present
                        if dep_param:
                            # Remove depositions filter from where_clause since it's already in the MATCH WHERE
                            dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                            other_filters = where_clause.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause else ""
                            # Clean up any double WHERE or AND issues - do this multiple times to catch all cases
                            while "WHERE AND" in other_filters or "AND AND" in other_filters:
                                other_filters = other_filters.replace("WHERE AND", "WHERE").replace("AND AND", "AND").replace("WHERE WHERE", "WHERE").strip()
                            # Remove leading AND
                            while other_filters.startswith("AND "):
                                other_filters = other_filters[4:].strip()
                            # Remove trailing AND
                            while other_filters.endswith(" AND"):
                                other_filters = other_filters[:-4].strip()
                            # Remove WHERE prefix if present
                            if other_filters.startswith("WHERE"):
                                other_filters = other_filters[6:].strip()
                            # If it's just "WHERE" or empty or "AND", make it empty
                            if other_filters == "WHERE" or other_filters == "AND" or not other_filters.strip():
                                other_filters = ""
                            
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}"
                            # Combine normal filters (if any) + race token filter (if any) into a single WHERE,
                            # evaluated while race_tokens/pr_tokens are in scope (same WITH clause).
                            other_filters_clean = ""
                            if other_filters and other_filters.strip():
                                # Remove any references to study_id (the variable) since it's being defined in this WITH clause
                                other_filters_clean = other_filters.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                                # Clean up any resulting "AND AND" or trailing/leading AND
                                other_filters_clean = other_filters_clean.replace("AND AND", "AND").strip()
                                while other_filters_clean.startswith("AND "):
                                    other_filters_clean = other_filters_clean[4:].strip()
                                while other_filters_clean.endswith(" AND"):
                                    other_filters_clean = other_filters_clean[:-4].strip()
                                if other_filters_clean == "AND" or not other_filters_clean.strip():
                                    other_filters_clean = ""
                            combined_with_where = combine_where_clauses(
                                f"WHERE {other_filters_clean}" if other_filters_clean else "",
                                race_filter_condition,
                            )
                            if combined_with_where:
                                with_clause += f"\n        {combined_with_where}"
                            
                            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
                        else:
                            # Build WITH clause with optional WHERE
                            # Note: st comes from OPTIONAL MATCH, so we need to handle NULL case
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p, st.study_id AS study_id{race_condition}{identifiers_condition}"
                            where_conditions_clean = ""
                            if where_clause:
                                # Remove "WHERE " prefix if present
                                where_conditions_clean = where_clause.replace("WHERE ", "").strip()
                                # Remove any references to study_id (the variable) since it's being defined in this WITH clause
                                where_conditions_clean = where_conditions_clean.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                                # Clean up any resulting "AND AND" or trailing/leading AND
                                where_conditions_clean = where_conditions_clean.replace("AND AND", "AND").strip()
                                while where_conditions_clean.startswith("AND "):
                                    where_conditions_clean = where_conditions_clean[4:].strip()
                                while where_conditions_clean.endswith(" AND"):
                                    where_conditions_clean = where_conditions_clean[:-4].strip()
                                if where_conditions_clean == "AND" or not where_conditions_clean.strip():
                                    where_conditions_clean = ""
                            combined_with_where = combine_where_clauses(
                                f"WHERE {where_conditions_clean}" if where_conditions_clean else "",
                                race_filter_condition,
                            )
                            if combined_with_where:
                                with_clause += f"\n        {combined_with_where}"
                            
                            cypher = f"""
        MATCH (p:participant)
        // Use participant -> consent_group -> study relationship to match the main query's relationship path
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st IS NOT NULL"}
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
            else:
                # No filters at all - but check for diagnosis search
                if race_filter_condition:
                    # Race-only filter (or race was popped out of `filters`) must still be applied.
                    # Use participant -> consent_group -> study relationship for traversal, apply race token filter in the WITH clause
                    # while race_tokens/pr_tokens are in scope.
                    cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p, st{race_condition}
        {race_where_clause}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                elif diagnosis_search_term:
                    # Apply diagnosis search filter
                    cypher = f"""
        MATCH (p:participant)
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p, collect(DISTINCT d) AS diagnosis_nodes
        WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0
        // Use participant -> consent_group -> study relationship to match the main query's relationship path
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st IS NOT NULL"}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                else:
                    # No filters at all - but check for depositions filter
                    if dep_param:
                        # Apply depositions filter
                        cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                    else:
                        # No filters at all - simple count
                        # Use participant -> consent_group -> study relationship
                        # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
                        cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
        
        logger.info(
            "Executing get_subjects_summary Cypher query",
            cypher=cypher if 'cypher' in locals() and cypher else "not defined",
            params=params
        )
        # Execute query with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher, params)
                records = []
                async for record in result:
                    records.append(dict(record))
                
                # Ensure result is fully consumed
                await result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if records or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_subjects_summary query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_subjects_summary query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error(
                        "Error in get_subjects_summary query after retries",
                        error=str(e),
                        cypher=cypher if 'cypher' in locals() and cypher else "not defined",
                        params=params if 'params' in locals() else {},
                        exc_info=True
                    )
                    raise
        
        if not records:
            return {"total_count": 0}
        
        summary = records[0]
        logger.debug("Completed subjects summary", total_count=summary.get("total_count", 0))
        
        return summary
    
    def _validate_filters(self, filters: Dict[str, Any], entity_type: str) -> None:
        """
        Validate that all filter fields are allowed.
        
        Args:
            filters: Dictionary of filters to validate
            entity_type: Type of entity for allowlist checking
            
        Raises:
            UnsupportedFieldError: If any field is not allowed
        """
        for field in filters.keys():
            # Skip special fields
            if field.startswith("_"):
                continue
                
            if not self.allowlist.is_field_allowed(entity_type, field):
                # Log the invalid field but don't include it in the error message
                logger.warning(
                    "Unsupported field in filter",
                    field=field,
                    entity_type=entity_type
                )
                raise UnsupportedFieldError(field, entity_type)
    
    def _record_to_subject(self, record: Dict[str, Any], base_url: Optional[str] = None) -> Subject:
        """
        Convert a database record to a Subject object with nested CCDI-DCC format.
        
        Args:
            record: Database record dictionary
            base_url: Base URL for generating server URLs in identifiers (optional)
            
        Returns:
            Subject object with nested CCDI-DCC structure
        """
        # Extract values from record
        participant_id = record.get("name")
        study_id = record.get("namespace")
        study_ids = record.get("depositions")  # This is now an array of study_ids
        race_value = record.get("race")
        sex_value_raw = record.get("sex")
        vital_status_raw = record.get("vital_status")
        age_at_vital_status = record.get("age_at_vital_status")
        associated_diagnoses_raw = record.get("associated_diagnoses")
        survival_records_raw = record.get("survival_records")
        diagnosis_nodes_raw = record.get("diagnosis_nodes")

        def _get_prop(obj: Any, key: str) -> Any:
            """Best-effort property access for neo4j Node-like objects / dicts.
            
            Handles date/time objects (ZONED_DATE_TIME, etc.) by converting them
            to ISO format strings to prevent serialization errors.
            """
            from app.core.serialization import convert_date_time_to_string
            
            if obj is None:
                return None
            
            value = None
            try:
                if hasattr(obj, "get"):
                    value = obj.get(key)
            except Exception:
                pass
            
            if value is None:
                try:
                    if isinstance(obj, dict):
                        value = obj.get(key)
                except Exception:
                    pass
            
            if value is None:
                try:
                    value = obj[key]  # type: ignore[index]
                except Exception:
                    return None
            
            # Convert date/time objects to strings
            return convert_date_time_to_string(value)

        # If the Cypher returned raw survival records, compute derived survival fields in Python.
        # This avoids complex Cypher reductions that have caused Memgraph instability/crashes.
        if vital_status_raw is None and survival_records_raw:
            survs = []
            for sr in (survival_records_raw or []):
                status = _get_prop(sr, "last_known_survival_status")
                if status is None:
                    continue
                age_raw = _get_prop(sr, "age_at_last_known_survival_status")
                age_int = None
                if age_raw is not None:
                    try:
                        age_int = int(age_raw)
                    except Exception:
                        age_int = None
                survs.append({"status": str(status), "age": age_int})

            if survs:
                dead_ages = [s["age"] for s in survs if s["status"] == "Dead" and s["age"] is not None]
                any_dead = len(dead_ages) > 0
                max_age = max([s["age"] for s in survs if s["age"] is not None], default=None)

                if any_dead:
                    vital_status_raw = "Dead"
                    age_at_vital_status = max(dead_ages) if dead_ages else None
                else:
                    # pick the status from a record with max_age if possible
                    if max_age is not None:
                        status_at_max = next((s["status"] for s in survs if s["age"] == max_age), None)
                        vital_status_raw = status_at_max or survs[0]["status"]
                        age_at_vital_status = max_age
                    else:
                        vital_status_raw = survs[0]["status"]
                        age_at_vital_status = None

        # If the Cypher returned diagnosis nodes, extract diagnosis strings in Python.
        if not associated_diagnoses_raw and diagnosis_nodes_raw:
            extracted = []
            for node in (diagnosis_nodes_raw or []):
                diag_val = _get_prop(node, "diagnosis")
                if diag_val is None:
                    continue
                if isinstance(diag_val, list):
                    extracted.extend([d for d in diag_val if d is not None and str(d).strip()])
                else:
                    if str(diag_val).strip():
                        extracted.append(diag_val)
            associated_diagnoses_raw = extracted
        
        # Apply field mapping to vital_status (e.g., "Not Reported" -> "Not reported")
        vital_status = map_field_value("vital_status", vital_status_raw) if vital_status_raw else vital_status_raw
        
        # Handle associated_diagnoses - it might be a string, list, or list of diagnosis nodes
        # The query now aggregates diagnosis nodes, so we get a list of diagnosis values
        associated_diagnoses = []
        if associated_diagnoses_raw:
            if isinstance(associated_diagnoses_raw, list):
                # Flatten the list - each element might be a string or another list
                for item in associated_diagnoses_raw:
                    if isinstance(item, list):
                        associated_diagnoses.extend(item)
                    elif isinstance(item, str) and item.strip():
                        associated_diagnoses.append(item)
                    elif item is not None:
                        # Handle diagnosis node objects (if they exist)
                        associated_diagnoses.append(str(item))
            elif isinstance(associated_diagnoses_raw, str):
                # If it's a string, treat it as a single diagnosis
                associated_diagnoses = [associated_diagnoses_raw] if associated_diagnoses_raw.strip() else []
        
        # Remove duplicates and empty values
        associated_diagnoses = list(dict.fromkeys([d for d in associated_diagnoses if d and str(d).strip()]))
        
        # Handle race - split by semicolon if present
        original_race_list = []
        if race_value:
            if isinstance(race_value, str):
                # Split by semicolon and clean up whitespace
                original_race_list = [race.strip() for race in race_value.split(';') if race.strip()]
            elif isinstance(race_value, list):
                # If it's already a list, process each item
                original_race_list = []
                for race_item in race_value:
                    if isinstance(race_item, str):
                        original_race_list.extend([r.strip() for r in race_item.split(';') if r.strip()])
                    else:
                        original_race_list.append(str(race_item))
            else:
                original_race_list = [str(race_value)]
        
        # Determine ethnicity based on original race values
        ethnicity_value = None
        if original_race_list:
            if any('Hispanic or Latino' in race for race in original_race_list):
                ethnicity_value = 'Hispanic or Latino'
            else:
                ethnicity_value = 'Not reported'

        # Remove 'Hispanic or Latino' from reported race values
        race_list = [r for r in original_race_list if r != 'Hispanic or Latino']
        
        # If race value was only "Hispanic or Latino", replace with "Not Reported"
        # If race value contains "Hispanic or Latino" and other values, keep current behavior (already removed)
        if not race_list and original_race_list:
            # Check if original only had "Hispanic or Latino" (exact match)
            if all(race.strip() == 'Hispanic or Latino' for race in original_race_list):
                race_list = ['Not Reported']
        
        # Normalize sex using config mappings
        sex_value = None
        if sex_value_raw is not None:
            sex_str = str(sex_value_raw).strip()
            
            # Use config mappings if available
            if self.settings and hasattr(self.settings, 'sex_value_mappings') and self.settings.sex_value_mappings:
                mappings = self.settings.sex_value_mappings
                # Try exact match first
                if sex_str in mappings:
                    sex_value = mappings[sex_str]
                # Try case-insensitive match
                else:
                    sex_lower = sex_str.lower()
                    for db_val, norm_val in mappings.items():
                        if db_val.lower() == sex_lower:
                            sex_value = norm_val
                            break
                    # If already normalized or not found, use default
                    if sex_value is None:
                        if sex_str in mappings.values():
                            sex_value = sex_str  # Already normalized
                        else:
                            # Fallback to 'U' if 'Not Reported' exists, otherwise first value
                            sex_value = mappings.get("Not Reported", "U") if "Not Reported" in mappings else (list(mappings.values())[0] if mappings else "U")
            else:
                # Fallback to simple logic if no config
                sex_lower = sex_str.lower()
                if sex_lower in {"female", "f"}:
                    sex_value = "F"
                elif sex_lower in {"male", "m"}:
                    sex_value = "M"
                else:
                    sex_value = "U"

        # Build nested subject structure
        # Use latest (last alphabetically) study_id for id.namespace.name (sorted study_ids list)
        sorted_study_ids = sorted([sid for sid in (study_ids if study_ids else []) if sid]) if study_ids else []
        primary_study_id = sorted_study_ids[-1] if sorted_study_ids else study_id  # Use latest (last) study_id
        
        subject_data = {
            "id": {
                "namespace": {
                    "organization": "CCDI-DCC",
                    "name": primary_study_id
                },
                "name": participant_id
            },
            "kind": "Participant",
            # Note: All "ancestors" fields are excluded from results but kept as optional placeholders in DTO models
            "metadata": {
                "sex": {"value": sex_value} if sex_value else None,
                # Race reporting rule: remove 'Hispanic or Latino'. If only that term was present,
                # replace with "Not Reported"; if race was entirely missing, keep it as None.
                "race": (
                    [{"value": race} for race in race_list]
                    if race_value is not None
                    else None
                ),
                "ethnicity": {"value": ethnicity_value} if ethnicity_value else None,
                "identifiers": sorted(
                    [
                        {
                            "value": {
                                "namespace": {
                                    "organization": "CCDI-DCC",
                                    "name": study_id
                                },
                                "name": participant_id,
                                "type": "Linked",
                                "server": build_identifier_server_url(
                                    base_url=base_url or "",
                                    entity_type="subject",
                                    organization="CCDI-DCC",
                                    study_id=study_id,
                                    name=participant_id
                                ) if base_url else None
                            }
                        }
                        for study_id in (study_ids if study_ids else [])
                        if study_id and participant_id  # Filter out None/null values
                    ],
                    key=lambda x: x["value"]["namespace"]["name"]  # Sort by namespace name (study_id)
                ) if participant_id and study_ids else None,
                "associated_diagnoses": sorted(
                    [
                        {
                            "value": diag,
                            "comment": None
                        }
                        for diag in associated_diagnoses
                    ],
                    key=lambda x: x["value"]  # Sort by diagnosis value
                ) if associated_diagnoses else None,
                # "unharmonized": None,  # Commented out for now, reserved for future use
                "vital_status": {"value": vital_status} if vital_status else None,
                # Convert -999 to null for age_at_vital_status in response (count endpoints keep -999 as-is)
                # Also convert 0 to null if vital_status exists (0 likely came from -999 when all ages were -999)
                "age_at_vital_status": (
                    {"value": int(age_at_vital_status)} 
                    if age_at_vital_status is not None 
                       and age_at_vital_status != -999 
                       and not (age_at_vital_status == 0 and vital_status is not None)
                    else None
                ),
                "depositions": sorted(
                    [
                        {"kind": "dbGaP", "value": study_id}
                        for study_id in (study_ids if study_ids else [])
                        if study_id  # Filter out None/null values
                    ],
                    key=lambda x: x["value"]  # Sort by value (study_id)
                ) if study_ids else []
            },
            "gateways": []
        }
        
        # Create a Subject object with the nested structure
        return Subject(**subject_data)
