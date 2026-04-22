"""
Summary methods for SubjectRepository.
"""
import asyncio
from typing import Any, Dict

from app.core.logging import get_logger
from app.core.field_mappings import reverse_map_field_value, is_database_only_value
from app.utils.cypher_builder import combine_where_clauses

logger = get_logger(__name__)


class SubjectSummary:
    """Mixin providing summary methods for SubjectRepository."""

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
                    # Apply reverse mapping: convert API values to database values for filtering
                    # e.g., "Not allowed to collect" (API) -> "Not Allowed to Collect" (DB)
                    db_race_list = []
                    for api_race in race_list:
                        db_race = reverse_map_field_value("race", api_race)
                        if db_race:
                            # reverse_map_field_value can return a single value or list
                            if isinstance(db_race, list):
                                db_race_list.extend(db_race)
                            else:
                                db_race_list.append(db_race)
                        else:
                            # No mapping found, use API value as-is (fallback)
                            db_race_list.append(api_race)

                    # Remove duplicates while preserving order
                    db_race_list = list(dict.fromkeys(db_race_list))

                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    # Use database values for filtering
                    params[race_param] = db_race_list

                    # Check if "Not Reported" is in the filter - if so, also match "Hispanic or Latino" only records
                    # Note: Check original API values, not DB values, since "Not Reported" doesn't have a mapping
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

        # Handle associated_diagnosis_categories filter
        diag_category_filter = None
        if "associated_diagnosis_categories" in filters:
            raw_cat = filters.pop("associated_diagnosis_categories")
            if isinstance(raw_cat, str) and raw_cat.strip():
                diag_category_filter = raw_cat.strip()
                params["diag_category_filter"] = diag_category_filter

        diag_category_fragment = ""
        if diag_category_filter:
            diag_category_fragment = """
        // Apply diagnosis category filter (semicolon-delimited tokens matched individually)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(diag_cat:diagnosis)
        WITH participant_id, study_id, collect(DISTINCT diag_cat) AS diag_cat_nodes
        WHERE size([dn IN diag_cat_nodes WHERE dn IS NOT NULL AND any(token IN split(toString(coalesce(dn.diagnosis_category, '')), ';') WHERE trim(token) = $diag_category_filter)]) > 0"""

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
                            END) AS max_age,
            // Check if all ages are -999 (for counting as missing)
            all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
            // Check if Dead age is -999
            any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p, st,
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
                        other_filters = where_clause_no_dep.strip()
                        if other_filters.startswith("WHERE "):
                            other_filters = other_filters[6:]  # Remove "WHERE " prefix (6 characters)
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
        WITH participant_id, p, st{race_condition},
             collect(DISTINCT s) AS survival_records,
             collect(DISTINCT d) AS diagnosis_nodes
        WITH participant_id, p, st, diagnosis_nodes,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, st, diagnosis_nodes, survs,
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
        WITH participant_id, p, st, diagnosis_nodes,
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
        WITH participant_id, st.study_id AS study_id, diagnosis_nodes
        WHERE toString(study_id) <> ''
        {"AND size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        WITH DISTINCT participant_id, study_id
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
        // Collect diagnoses only when diagnosis search is requested
        {"OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)\n        WITH participant_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes" if diagnosis_search_term else "WITH participant_id, p, survival_records, [] AS diagnosis_nodes"}
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
        WITH DISTINCT p.participant_id AS participant_id, p, st{race_condition}{identifiers_condition},
             collect(DISTINCT s) AS survival_records,
             collect(DISTINCT d) AS diagnosis_nodes
        {where_clause_clean_for_summary}
        WITH participant_id, p, st, diagnosis_nodes,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, st, diagnosis_nodes, survs,
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
        WITH participant_id, p, st, diagnosis_nodes,
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
        WITH participant_id, st.study_id AS study_id, diagnosis_nodes
        WHERE toString(study_id) <> ''
        {"AND size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0" if diagnosis_search_term else ""}
        WITH DISTINCT participant_id, study_id
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
        WITH p{race_condition}
        {combine_where_clauses(where_clause_clean_for_summary, race_filter_condition if race_filter_condition else "")}

        // Require study context via real match (no list comprehensions)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study)
        WITH DISTINCT p

        // Apply diagnosis search via streamed rows, then dedupe
        {"OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)\n"
         "        WITH p, d\n"
         "        WHERE d IS NOT NULL AND ANY(diag IN CASE WHEN valueType(d.diagnosis) = 'LIST' THEN d.diagnosis ELSE [d.diagnosis] END "
         "WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))\n"
         "        WITH DISTINCT p" if diagnosis_search_term else ""}
        // Collect survival once per participant (avoid repeating per study row)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, collect(DISTINCT s) AS survival_records
        WITH p,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
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
                    END) AS max_age,
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p,
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
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status,
             CASE
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value

        {derived_where_clause}

        // Join studies after derived filters to keep row mode and reduce work
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        WHERE toString(study_id) <> ''
        WITH DISTINCT participant_id, study_id
        RETURN count(*) AS total_count
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
        {derived_where_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {derived_where_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {derived_where_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {derived_where_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        // Use participant -> consent_group -> study relationship to match the main query's relationship path
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st IS NOT NULL"}
        {with_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {with_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {with_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {with_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {with_clause}{diagnosis_search_fragment}{diag_category_fragment}
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
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st IS NOT NULL"}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                elif diag_category_filter:
                    cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(diag_cat:diagnosis)
        WITH p, collect(DISTINCT diag_cat) AS diag_cat_nodes
        WHERE size([dn IN diag_cat_nodes WHERE dn IS NOT NULL AND any(token IN split(toString(coalesce(dn.diagnosis_category, '')), ';') WHERE trim(token) = $diag_category_filter)]) > 0
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

    async def get_subjects_summary_for_diagnosis_endpoint(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for subjects with diagnosis search.
        Optimized to filter by diagnosis FIRST before collecting survival records.

        Args:
            filters: Filters to apply (must include _diagnosis_search)

        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting subjects summary for diagnosis endpoint", filters=filters)
        # Avoid mutating the caller's dict
        filters = dict(filters or {})

        # Ensure diagnosis search is present and remove internal key from generic filter processing
        diagnosis_search_term = filters.pop("_diagnosis_search", None)
        if not diagnosis_search_term:
            logger.warning("get_subjects_summary_for_diagnosis_endpoint called without _diagnosis_search")
            return await self.get_subjects_summary(filters)

        # Build WHERE conditions and parameters (reuse logic from get_subjects_summary)
        where_conditions = []
        params = {"diagnosis_search_term": diagnosis_search_term}
        param_counter = 0

        # Handle race parameter normalization
        race_condition = ""
        race_filter_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []

                if race_list:
                    db_race_list = []
                    for api_race in race_list:
                        db_race = reverse_map_field_value("race", api_race)
                        if db_race:
                            if isinstance(db_race, list):
                                db_race_list.extend(db_race)
                            else:
                                db_race_list.append(db_race)
                        else:
                            db_race_list.append(api_race)

                    db_race_list = list(dict.fromkeys(db_race_list))
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = db_race_list

                    includes_not_reported = any(r.strip() == "Not Reported" for r in race_list)
                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""

                    if includes_not_reported:
                        race_filter_condition = """(reduce(found = false, tok IN race_tokens | found OR tok IN pr_tokens) OR
                        (size(pr_tokens) > 0 AND reduce(all_hispanic = true, pt IN pr_tokens | all_hispanic AND pt = 'Hispanic or Latino') AND 'Not Reported' IN race_tokens))"""
                    else:
                        race_filter_condition = "reduce(found = false, tok IN race_tokens | found OR (tok IN pr_tokens AND tok <> 'Hispanic or Latino'))"

        # Handle identifiers
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            identifiers_list = self._split_or_values(identifiers_value)
            if identifiers_list:
                identifiers_value = identifiers_list[0] if len(identifiers_list) == 1 else identifiers_list
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                    CASE
                      WHEN ${id_param} IS NULL THEN NULL
                      WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                      WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                      ELSE []
                    END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")

        # Handle depositions
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            depositions_list = self._split_or_values(depositions_value)
            if depositions_list:
                param_counter += 1
                dep_param = f"param_{param_counter}"
                if len(depositions_list) == 1:
                    params[dep_param] = depositions_list[0]
                    deposition_operator = "="
                else:
                    params[dep_param] = depositions_list
                    deposition_operator = "IN"

        # Map API sex values to database values (M -> Male, F -> Female, U -> Not Reported)
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]
        # Handle other filters
        field_name_mapping = {"sex": "sex_at_birth"}
        for field, value in filters.items():
            if field.startswith("_") or field in {"vital_status", "age_at_vital_status", "ethnicity"}:
                continue  # Derived fields handled separately

            db_field = field_name_mapping.get(field, field)
            param_counter += 1
            param_name = f"param_{param_counter}"
            condition = f"p.{db_field} IN ${param_name}" if isinstance(value, list) else f"p.{db_field} = ${param_name}"
            params[param_name] = value
            where_conditions.append(condition)

        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)
        # Optional hardening: normalize malformed WHERE fragments defensively
        if where_clause:
            where_clause = " ".join(where_clause.split())  # collapse duplicated whitespace
            where_clause = where_clause.replace("WHERE AND ", "WHERE ").replace("WHERE OR ", "WHERE ")
            if where_clause in {"WHERE", "WHERE AND", "WHERE OR"}:
                where_clause = ""
        # Handle derived fields
        derived_filters = {}
        derived_conditions = []
        if "vital_status" in filters:
            derived_filters["vital_status"] = filters["vital_status"]
        if "age_at_vital_status" in filters:
            derived_filters["age_at_vital_status"] = filters["age_at_vital_status"]

        needs_survival_processing = bool(derived_filters.get("vital_status") or derived_filters.get("age_at_vital_status"))

        for field, value in derived_filters.items():
            param_counter += 1
            param_name = f"derived_{field}_{param_counter}"
            if field == "vital_status":
                if is_database_only_value("vital_status", value):
                    derived_conditions.append("false")
                else:
                    derived_conditions.append(
                        f"(final_vital_status IS NOT NULL AND toLower(toString(final_vital_status)) = toLower(toString(${param_name})))"
                    )
                    value = reverse_map_field_value("vital_status", value) if value else value
            elif field == "age_at_vital_status":
                derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                try:
                    value = int(value) if value is not None else value
                except (ValueError, TypeError):
                    logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
            params[param_name] = value

        derived_where_clause = ""
        if derived_conditions:
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
            if filtered_derived:
                derived_where_clause = "WHERE " + " AND ".join(filtered_derived)

        # Clean where_clause for summary
        where_clause_clean_for_summary = where_clause
        if where_clause_clean_for_summary:
            where_clause_clean_for_summary = where_clause_clean_for_summary.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
            where_clause_clean_for_summary = where_clause_clean_for_summary.replace("AND AND", "AND").strip()
            while where_clause_clean_for_summary.startswith("AND "):
                where_clause_clean_for_summary = where_clause_clean_for_summary[4:].strip()
            while where_clause_clean_for_summary.endswith(" AND"):
                where_clause_clean_for_summary = where_clause_clean_for_summary[:-4].strip()
            if where_clause_clean_for_summary == "WHERE" or where_clause_clean_for_summary == "AND" or not where_clause_clean_for_summary.strip():
                where_clause_clean_for_summary = ""

        # Build optimized query: filter by diagnosis FIRST, then collect survival
        if dep_param:
            # Depositions path: filter by diagnosis FIRST
            cypher = f"""
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE st.study_id {deposition_operator} ${dep_param}
        WITH p, st{race_condition}{identifiers_condition}
        {combine_where_clauses(where_clause_clean_for_summary, race_filter_condition if race_filter_condition else "")}

        // Apply diagnosis search filter EARLY (before collecting survival records)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, st, d
        WHERE d IS NOT NULL AND ANY(diag IN CASE WHEN valueType(d.diagnosis) = 'LIST' THEN d.diagnosis ELSE [d.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
        WITH DISTINCT p, st

        // Now collect survival records only for filtered participants
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, st,
             collect(DISTINCT s) AS survival_records
        WITH p, st, survival_records,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, st, survs,
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
        WITH p, st,
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
             END AS final_age_at_vital_status
        {derived_where_clause}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        WHERE toString(study_id) <> ''
        WITH DISTINCT participant_id, study_id
        RETURN count(*) as total_count
        """.strip()
        else:
            # Non-depositions path: same optimized pattern
            cypher = f"""
        MATCH (p:participant)
        WITH p{race_condition}{identifiers_condition}
        {combine_where_clauses(where_clause_clean_for_summary, race_filter_condition if race_filter_condition else "")}

        // Require study context
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study)
        WITH DISTINCT p

        // Apply diagnosis search filter EARLY (before collecting survival records)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d
        WHERE d IS NOT NULL AND ANY(diag IN CASE WHEN valueType(d.diagnosis) = 'LIST' THEN d.diagnosis ELSE [d.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
        WITH DISTINCT p

        // Now collect survival records only for filtered participants
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, collect(DISTINCT s) AS survival_records
        WITH p, survival_records,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
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
        WITH p,
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
             END AS final_age_at_vital_status
        {derived_where_clause}

        // Join studies after derived filters
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        WHERE toString(study_id) <> ''
        WITH DISTINCT participant_id, study_id
        RETURN count(*) AS total_count
        """.strip()

        # Execute query
        logger.debug("Executing get_subjects_summary_for_diagnosis_endpoint Cypher query", cypher=cypher, params=params)

        # Strip comments for execution
        cypher_to_run = "\n".join([line for line in cypher.split("\n") if not line.strip().startswith("//")])

        try:
            result = await self.session.run(cypher_to_run, params)
            records = await result.data()
        except Exception as e:
            logger.error(
                "Error executing get_subjects_summary_for_diagnosis_endpoint query",
                error=str(e),
                error_type=type(e).__name__,
                exc_info=True
            )
            raise

        if not records:
            return {"total_count": 0}

        summary = records[0]
        logger.debug("Completed subjects summary for diagnosis endpoint", total_count=summary.get("total_count", 0))

        return summary
