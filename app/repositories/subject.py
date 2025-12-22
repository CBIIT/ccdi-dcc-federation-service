"""
Subject repository for the CCDI Federation Service.

This module provides data access operations for subjects
using Cypher queries to Memgraph.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.core.constants import Race
from app.core.field_mappings import map_field_value, reverse_map_field_value, is_database_only_value
from app.lib.field_allowlist import FieldAllowlist
from app.lib.url_builder import build_identifier_server_url
from app.models.dto import Subject
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)


class SubjectRepository:
    """Repository for subject data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings=None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings
        
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
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                # Parse || separator and create list
                if isinstance(identifiers_value, str) and "||" in identifiers_value:
                    # Split on || separator and clean whitespace
                    identifiers_list = [i.strip() for i in identifiers_value.split("||")]
                    identifiers_list = [i for i in identifiers_list if i]
                    identifiers_value = identifiers_list if identifiers_list else None
                
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
        
        # Handle depositions filter (study_id)
        # Support || separator for OR logic (e.g., "phs001 || phs002")
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            if depositions_value is not None and str(depositions_value).strip():
                depositions_str = str(depositions_value).strip()
                # Parse || separator
                if "||" in depositions_str:
                    depositions_list = [d.strip() for d in depositions_str.split("||")]
                    depositions_list = [d for d in depositions_list if d]
                    if not depositions_list:
                        depositions_list = None
                        depositions_value = None
                else:
                    depositions_list = [depositions_str]
                
                if depositions_list:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    # Filter by study_id - participants must belong to the specified study
                    if len(depositions_list) == 1:
                        params[dep_param] = depositions_list[0]
                        deposition_operator = "="
                        where_conditions.append("st.study_id = ${}".format(dep_param))
                    else:
                        params[dep_param] = depositions_list
                        deposition_operator = "IN"
                        where_conditions.append("st.study_id IN ${}".format(dep_param))
        
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
        
        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status", "ethnicity"}:
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
        
        # Add race filter to WHERE clause if it exists (after WITH defines race_tokens/pr_tokens)
        if race_filter_condition:
            if where_clause:
                where_clause = where_clause + " AND " + race_filter_condition
            else:
                where_clause = "WHERE " + race_filter_condition
        
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
                        derived_conditions.append(f"final_vital_status = ${param_name}")
                        # Apply reverse mapping for vital_status filter (e.g., "Not reported" -> "Not Reported")
                        value = reverse_map_field_value("vital_status", value) if value else value
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                    # Convert age to integer since it's stored as int in database
                    try:
                        value = int(value) if value is not None else value
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
                elif field == "ethnicity":
                    # Ethnicity filter will be applied in the WITH clause after calculation
                    ethnicity_param = param_name
                    derived_conditions.append(f"ethnicity_value = ${ethnicity_param}")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
            if filtered_derived:
                derived_where_clause = "WHERE " + " AND ".join(filtered_derived)
        
        # When depositions filter is present, we need required MATCH for studies
        # But we can't put MATCH after OPTIONAL MATCH, so we need to restructure
        if dep_param:
            # Remove depositions filter from where_clause since it's applied in the MATCH WHERE
            dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
            # Also remove identifiers condition if present (it's applied in MATCH WHERE when identifiers_condition exists)
            id_filter_str = "p.participant_id IN id_list"
            where_clause_no_dep = where_clause.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause else ""
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
            
            # Build WITH clause with optional WHERE
            with_clause = f"WITH p, s, d, c, st{race_condition}{identifiers_condition}"
            # Build WHERE conditions for WITH clause
            with_where_conditions = []
            # Always apply identifiers filter if present (id_list is created in WITH clause)
            if identifiers_condition:
                with_where_conditions.append("p.participant_id IN id_list")
            # Add other filters if any
            if other_filters:
                with_where_conditions.append(other_filters)
            # Apply WHERE clause if we have any conditions
            if with_where_conditions:
                with_clause += f"\n        WHERE {' AND '.join(with_where_conditions)}"
            
            # For the depositions path, we need to apply derived filters AFTER calculation (line 381)
            # So we keep derived_where_clause as is for later application
            
            # Required study match - put it before OPTIONAL MATCHes
            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        {with_clause}
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
        // Group by participant to aggregate study_ids and handle multiple records per participant
        WITH toString(p.participant_id) AS participant_id, p, d, st, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        WITH participant_id, 
             // Use head to get first participant node (they're all the same per participant_id)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT d) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT st.study_id) AS study_ids
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Aggregate all diagnoses from all diagnosis nodes
             // Extract diagnosis values from each node (d.diagnosis can be string or list)
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        WITH participant_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Use first study_id for namespace (for backward compatibility)
             head(study_ids) AS namespace,
             // All study_ids for depositions (filter out null values)
             [sid IN study_ids WHERE sid IS NOT NULL AND toString(sid) <> ''] AS study_ids_filtered
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
        SKIP $offset
        LIMIT $limit
        """.strip()
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
            # Add other filters from where_clause (excluding identifiers condition)
            if where_clause:
                # Remove "WHERE " prefix if present
                where_conditions_str = where_clause.replace("WHERE ", "").strip()
                # Remove identifiers condition if present (already added above)
                id_filter_str = "p.participant_id IN id_list"
                where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                # Clean up any resulting "AND AND" or trailing/leading AND
                where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                while where_conditions_str.startswith("AND "):
                    where_conditions_str = where_conditions_str[4:].strip()
                while where_conditions_str.endswith(" AND"):
                    where_conditions_str = where_conditions_str[:-4].strip()
                if where_conditions_str:
                    with_where_conditions.append(where_conditions_str)
            # Apply WHERE clause if we have any conditions
            if with_where_conditions:
                with_clause += f"\n        WHERE {' AND '.join(with_where_conditions)}"
            
            cypher = f"""
        MATCH (p:participant)
        {with_clause}
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, collect(s) AS survival_records
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Collect studies separately (no cartesian product)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, survival_records, diagnosis_nodes, collect(DISTINCT st.study_id) AS study_ids
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        WITH p, diagnosis_nodes, study_ids,
             // Keep only records with a status
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, diagnosis_nodes, study_ids, survs,
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
        WITH p, diagnosis_nodes, study_ids, survs,
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
        // Group by participant - everything is already aggregated per participant
        WITH toString(p.participant_id) AS participant_id, p, diagnosis_nodes, study_ids, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        WITH participant_id, 
             // Use head to get first participant node (they're all the same per participant_id)
             head(collect(DISTINCT p)) AS p,
             // diagnosis_nodes and study_ids are already collected, just take first
             head(collect(diagnosis_nodes)) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             // study_ids are already collected, just take first
             head(collect(study_ids)) AS study_ids
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Aggregate all diagnoses from all diagnosis nodes
             // Extract diagnosis values from each node (d.diagnosis can be string or list)
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids,
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        WITH participant_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Use first study_id for namespace (for backward compatibility)
             head(study_ids) AS namespace,
             // All study_ids for depositions (filter out null values)
             [sid IN study_ids WHERE sid IS NOT NULL AND toString(sid) <> ''] AS study_ids_filtered
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
        SKIP $offset
        LIMIT $limit
        """.strip()
        
        # Log the full query for debugging
        # Print the full query for debugging
        print("=" * 80)
        print("FULL CYPHER QUERY:")
        print("=" * 80)
        print(cypher if 'cypher' in locals() else "cypher not yet defined")
        print("=" * 80)
        print("PARAMS:", params)
        print("=" * 80)
        if 'where_clause' in locals():
            print("where_clause:", where_clause)
        if 'race_filter_condition' in locals():
            print("race_filter_condition:", race_filter_condition)
        if 'race_condition' in locals():
            print("race_condition:", race_condition)
        print("=" * 80)
        
        logger.info(
            "Executing get_subjects Cypher query",
            cypher=cypher if 'cypher' in locals() else "not defined",
            params=params,
            filters=filters
        )
        
        # Execute query with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher, params)
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
                    # Also print the query to console for immediate visibility
                    print("=" * 80)
                    print("ERROR: Full Cypher Query:")
                    print("=" * 80)
                    print(cypher if 'cypher' in locals() else "No query")
                    print("=" * 80)
                    print("Params:", params)
                    print("=" * 80)
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
                    // tokenize stored semicolon-separated race string
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
        
        # Query 1: Get total count of all unique participants matching filters
        # Total count should match summary endpoint - use same logic (no survival processing for total)
        if is_survival_field:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        else:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null field value
        null_check = f"{field_access} IS NULL" if not is_survival_field else f"final_{field} IS NULL"
        if is_survival_field:
            # For survival fields, missing logic depends on the field:
            # - vital_status: missing when final_vital_status IS NULL
            # - age_at_vital_status: missing when final_age_at_vital_status IS NULL (includes -999 cases)
            if field == "age_at_vital_status":
                missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WITH p.participant_id as participant_id, final_vital_status, final_age_at_vital_status
        WITH participant_id, 
             head(collect(final_vital_status)) as final_vital_status,
             head(collect(final_age_at_vital_status)) as final_age_at_vital_status
        WHERE final_age_at_vital_status IS NULL
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
            else:
                # For vital_status, missing means no vital_status
                missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WITH p.participant_id as participant_id, final_vital_status, final_age_at_vital_status
        WITH participant_id, head(collect(final_vital_status)) as final_vital_status
        WHERE final_vital_status IS NULL
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
        else:
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE {null_check}
        RETURN count(DISTINCT p.participant_id) as missing
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
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WITH p.participant_id as participant_id, {field_value_expr} as field_val
        WITH participant_id, head(collect(field_val)) as field_val
        WHERE field_val IS NOT NULL
        WITH participant_id, 
             CASE 
               WHEN field_val IS NULL THEN []
               ELSE [field_val]
             END as field_values
        UNWIND field_values as value
        WITH value, participant_id,
             toString(value) as normalized_value
        RETURN normalized_value as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE {not_null_check}
        WITH p, 
             CASE 
               WHEN {field_value_expr} IS NULL THEN []
               ELSE 
                 // Wrap in list for UNWIND - works for both strings and lists
                 [{field_value_expr}]
             END as field_values
        UNWIND field_values as value
        WITH value, p.participant_id as participant_id,
             CASE 
               WHEN '{field}' = 'sex' THEN{normalization_expr}
               ELSE toString(value)
             END as normalized_value
        RETURN normalized_value as value, count(DISTINCT participant_id) as count
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
        
        # Query 1: Get total count of all unique participants matching filters
        total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null race value
        missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WHERE p.race IS NULL
        RETURN count(DISTINCT p.participant_id) as missing
        """.strip()
        
        # Query 3: Create a single query that counts distinct participants for each valid race
        # Special handling: if race is only "Hispanic or Latino", count as "Not Reported"
        # Split race by semicolon, remove "Hispanic or Latino", then match against valid races
        params["valid_races"] = valid_races
        
        values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id as participant_id, p.race as race
        WHERE race IS NOT NULL
        WITH participant_id, race,
             // Split race by semicolon and trim each part
             [r IN SPLIT(race, ';') | trim(r)] as race_parts
        WITH participant_id, race, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, race, race_list_filtered, had_hispanic,
             CASE 
               // If race was only "Hispanic or Latino", replace with "Not Reported"
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               // Otherwise, use the filtered race values that are valid
               ELSE [r IN race_list_filtered WHERE r IN $valid_races]
             END as matching_races
        UNWIND matching_races as race_value
        RETURN race_value as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_race Cypher queries",
            race_count=len(valid_races),
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
        
        # Format results - ensure all valid races are included (even with 0 count)
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}
        
        # Build final counts list with all valid races
        counts = []
        for race_value in valid_races:
            counts.append({
                "value": race_value,
                "count": counts_by_value.get(race_value, 0)
            })
        
        # Sort by count descending (numeric), then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        logger.info(
            "Completed subject count by race",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
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
        
        # Query 1: Get total count of all unique participants matching filters
        total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null race (missing ethnicity)
        missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WHERE p.race IS NULL
        RETURN count(DISTINCT p.participant_id) as missing
        """.strip()
        
        # Query 3: Count by ethnicity (derived from race)
        values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id as participant_id, p.race as race
        WHERE race IS NOT NULL
        WITH participant_id, race,
             CASE 
               WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        RETURN ethnicity_value as value, count(DISTINCT participant_id) as count
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
        
        # Query 1: Get total count of all unique participants matching filters
        # Only include OPTIONAL MATCHes if we have filters that need them
        if where_clause or identifiers_condition:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        else:
            # No filters - simple count
            total_cypher = """
        MATCH (p:participant)
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with no diagnoses (missing)
        # Optimized to avoid unnecessary OPTIONAL MATCHes
        if where_clause or identifiers_condition:
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(DISTINCT p) as missing
        """.strip()
        else:
            # No filters - simple check for missing diagnoses
            missing_cypher = """
        MATCH (p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH DISTINCT p.participant_id as participant_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
        
        # Query 3: Count by diagnosis values
        # d.diagnosis is a STRING (not a list) - each diagnosis node has one diagnosis value
        # Multiple diagnosis nodes can link to one participant, so each contributes one value
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(p:participant)
        # Optimized: Start from diagnosis nodes, apply filters only if needed
        # Use exact query that worked in Memgraph when no filters
        if where_clause or (identifiers_condition and identifiers_condition.strip()):
            # Has filters - need to apply them but keep it efficient
            values_cypher = f"""
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        WITH DISTINCT p.participant_id as participant_id, d.diagnosis as diagnosis_value
        MATCH (p2:participant {{participant_id: participant_id}})
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p2)
        OPTIONAL MATCH (p2)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p2, s, c, st, participant_id, diagnosis_value{identifiers_condition}
        {where_clause}
        WHERE toString(diagnosis_value) <> ''
        RETURN toString(diagnosis_value) as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # No filters - use exact query that worked in Memgraph
            values_cypher = """
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        WITH DISTINCT p.participant_id as participant_id, d.diagnosis as diagnosis_value
        RETURN toString(diagnosis_value) as value, count(DISTINCT participant_id) as count
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
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                # Parse || separator and create list
                if isinstance(identifiers_value, str) and "||" in identifiers_value:
                    # Split on || separator and clean whitespace
                    identifiers_list = [i.strip() for i in identifiers_value.split("||")]
                    identifiers_list = [i for i in identifiers_list if i]
                    identifiers_value = identifiers_list if identifiers_list else None
                
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
        
        # Handle depositions filter (study_id)
        # Support || separator for OR logic (e.g., "phs001 || phs002")
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            if depositions_value is not None and str(depositions_value).strip():
                depositions_str = str(depositions_value).strip()
                # Parse || separator
                if "||" in depositions_str:
                    depositions_list = [d.strip() for d in depositions_str.split("||")]
                    depositions_list = [d for d in depositions_list if d]
                    if not depositions_list:
                        depositions_list = None
                        depositions_value = None
                else:
                    depositions_list = [depositions_str]
                
                if depositions_list:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    # Filter by study_id - participants must belong to the specified study
                    if len(depositions_list) == 1:
                        params[dep_param] = depositions_list[0]
                        deposition_operator = "="
                        where_conditions.append("st.study_id = ${}".format(dep_param))
                    else:
                        params[dep_param] = depositions_list
                        deposition_operator = "IN"
                        where_conditions.append("st.study_id IN ${}".format(dep_param))
        
        # Handle diagnosis search - will be applied after diagnosis collection
        diagnosis_search_term = None
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            params["diagnosis_search_term"] = diagnosis_search_term
            # Don't add to where_conditions yet - will be applied after diagnosis collection

        # Build diagnosis search fragment to be inserted before final count
        # This fragment assumes 'p' (participant node) and 'participant_id' are in scope
        diagnosis_search_fragment = ""
        if diagnosis_search_term:
            diagnosis_search_fragment = """
        // Apply diagnosis search filter
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(diag_search:diagnosis)
        WITH participant_id, collect(DISTINCT diag_search) AS diag_search_nodes
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
            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status", "ethnicity"}:
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
        
        # Add race filter to WHERE clause if it exists (after WITH defines race_tokens/pr_tokens)
        if race_filter_condition:
            if where_clause:
                where_clause = where_clause + " AND " + race_filter_condition
            else:
                where_clause = "WHERE " + race_filter_condition
        
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
                        derived_conditions.append(f"final_vital_status = ${param_name}")
                        # Apply reverse mapping for vital_status filter (e.g., "Not reported" -> "Not Reported")
                        value = reverse_map_field_value("vital_status", value) if value else value
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                    # Convert age to integer since it's stored as int in database
                    try:
                        value = int(value) if value is not None else value
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
                elif field == "ethnicity":
                    # Ethnicity filter will be applied in the WITH clause after calculation
                    ethnicity_param = param_name
                    derived_conditions.append(f"ethnicity_value = ${ethnicity_param}")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
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
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        {combined_where}
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH participant_id, p, d, c, st{race_condition},
             collect(s) AS survival_records
        WITH participant_id, p, d, c, st,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, d, c, st, survs,
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
        WITH participant_id, p, d, c, st,
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
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
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
        // Collect studies separately (no cartesian product)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH participant_id, p, survival_records, diagnosis_nodes, collect(DISTINCT st.study_id) AS study_ids
        {"WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        // Apply filters
        WITH participant_id, p, survival_records, diagnosis_nodes, study_ids{race_condition}
        {where_clause_filtered}
        WITH participant_id, p, diagnosis_nodes, study_ids,
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, diagnosis_nodes, study_ids, survs,
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
        WITH participant_id, p, survs, has_dead, max_dead_age, max_age,
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
        WITH participant_id,
             head(collect(final_vital_status)) AS final_vital_status,
             head(collect(final_age_at_vital_status)) AS final_age_at_vital_status,
             head(collect(ethnicity_value)) AS ethnicity_value
        {derived_where_clause}
        // Note: Cannot apply diagnosis search here as 'p' is no longer in scope after head(collect())
        // Diagnosis search would need to be applied earlier in the query if needed with survival filters
        RETURN count(DISTINCT participant_id) as total_count
        """.strip()
            else:
                # No identifiers condition - use original query
                # When depositions filter is present, use required MATCH for studies (before OPTIONAL MATCH)
                if dep_param:
                    cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH DISTINCT p.participant_id AS participant_id, p, s, d, c, st{race_condition}{identifiers_condition}
        {where_clause}
        WITH participant_id, p, d, c, st,
             collect(s) AS survival_records
        WITH participant_id, p, d, c, st,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, d, c, st, survs,
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
        WITH participant_id, p, d, c, st,
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
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                else:
                    # No depositions filter - use OPTIONAL MATCH approach like count endpoint
                    cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}
        {where_clause}{survival_processing}
        WITH p.participant_id as participant_id, final_vital_status, final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        WITH participant_id,
             head(collect(final_vital_status)) AS final_vital_status,
             head(collect(final_age_at_vital_status)) AS final_age_at_vital_status,
             head(collect(ethnicity_value)) AS ethnicity_value
        {derived_where_clause}
        // Note: Cannot apply diagnosis search here as 'p' is no longer in scope after head(collect())
        // Diagnosis search would need to be applied earlier in the query if needed with survival filters
        RETURN count(DISTINCT participant_id) as total_count
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
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}"
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
        WITH participant_id, p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                        else:
                            # No depositions filter - use OPTIONAL MATCH for studies
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}"
                            if where_clause_filtered:
                                # Remove "WHERE " prefix if present
                                where_conditions = where_clause_filtered.replace("WHERE ", "").strip()
                                if where_conditions:
                                    with_clause += f"\n        WHERE {where_conditions}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        {with_clause}
        WITH participant_id, p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
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
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}"
                            if other_filters and other_filters.strip():
                                with_clause += f"\n        WHERE {other_filters}"
                            
                            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}
        WITH participant_id, p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
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
                                with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}{identifiers_condition}"
                                if other_filters:
                                    with_clause += f"\n        WHERE {other_filters}"
                                
                                cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}
        WITH participant_id, p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                            else:
                                # Build WITH clause with optional WHERE
                                with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}{identifiers_condition}"
                                if where_clause:
                                    # Remove "WHERE " prefix if present
                                    where_conditions = where_clause.replace("WHERE ", "").strip()
                                    if where_conditions:
                                        with_clause += f"\n        WHERE {where_conditions}"
                                
                                cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        {with_clause}
        WITH participant_id, p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
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
                            with_clause = f"WITH DISTINCT participant_id, p{race_condition}"
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
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                        else:
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT participant_id, p{race_condition}"
                            if where_clause_filtered:
                                # Remove "WHERE " prefix if present
                                where_conditions = where_clause_filtered.replace("WHERE ", "").strip()
                                if where_conditions:
                                    with_clause += f"\n        WHERE {where_conditions}"
                            
                            cypher = f"""
        {identifiers_with_clause}
        MATCH (p:participant)
        WHERE p.participant_id IN id_list
        WITH DISTINCT p.participant_id AS participant_id, p
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
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
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}"
                            if other_filters and other_filters.strip():
                                with_clause += f"\n        WHERE {other_filters}"
                            
                            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                        else:
                            # Build WITH clause with optional WHERE
                            with_clause = f"WITH DISTINCT p.participant_id AS participant_id, p{race_condition}{identifiers_condition}"
                            if where_clause:
                                # Remove "WHERE " prefix if present
                                where_conditions = where_clause.replace("WHERE ", "").strip()
                                if where_conditions:
                                    with_clause += f"\n        WHERE {where_conditions}"
                            
                            cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        {with_clause}{diagnosis_search_fragment}
        WITH DISTINCT participant_id
        RETURN count(participant_id) as total_count
        """.strip()
            else:
                # No filters at all - but check for diagnosis search
                if diagnosis_search_term:
                    # Apply diagnosis search filter
                    cypher = f"""
        MATCH (p:participant)
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p, collect(DISTINCT d) AS diagnosis_nodes
        WHERE size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0
        RETURN count(DISTINCT p.participant_id) as total_count
        """.strip()
                else:
                    # No filters at all - simple count
                    cypher = """
        MATCH (p:participant)
        RETURN count(DISTINCT p.participant_id) as total_count
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
