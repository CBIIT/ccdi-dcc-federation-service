"""
Sample repository for the CCDI Federation Service.

This module provides data access operations for samples
using Cypher queries to Memgraph.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Sample
from app.models.errors import UnsupportedFieldError
from app.core.config import Settings
from app.core.constants import Race
from app.core.field_mappings import map_field_value, reverse_map_field_value

logger = get_logger(__name__)


class SampleRepository:
    """Repository for sample data operations."""
    
    @staticmethod
    def _reverse_map_library_selection_method_static(api_value):
        """Reverse map API value to database value for library_selection_method.
        
        Used for filtering - maps API values back to DB values.
        Uses centralized field mappings from config_data/field_mappings.json.
        """
        result = reverse_map_field_value("library_selection_method", api_value)
        # reverse_map_field_value can return a list, but for library_selection_method it should be a string
        return result if isinstance(result, str) else (result[0] if isinstance(result, list) and result else None)
    
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings: Optional[Settings] = None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings
    
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
        
        return f"CASE {' '.join(case_parts)} END"
        
    async def get_samples(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None
    ) -> List[Sample]:
        """
        Get paginated list of samples with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of Sample objects
            
        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching samples",
            filters=filters,
            offset=offset,
            limit=limit
        )
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
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
                where_conditions.append("sa.sample_id IN id_list")
        
        # Build filter conditions - these will be applied in the WITH clause after collecting diagnoses
        with_conditions = []
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            with_conditions.append("""diagnoses IS NOT NULL AND toLower(toString(diagnoses.diagnosis)) CONTAINS toLower($diagnosis_search_term)""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters - map to correct nodes based on field
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Map fields to their source nodes (after WITH clause, so we can reference them directly)
            if field == "disease_phase":
                # Filter on diagnosis with matching disease_phase
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
            elif field == "anatomical_sites":
                # anatomical_sites can be either a list or a string
                # If it's a list, we need to check if ANY of the values match (OR logic)
                # Normalize the value (trim whitespace) for consistent matching
                if isinstance(value, list):
                    # Multiple values - store as list for OR logic
                    params[param_name] = [v.strip() if isinstance(v, str) else v for v in value]
                    with_conditions.append(("anatomical_sites_list", param_name))
                elif isinstance(value, str):
                    value = value.strip()
                    params[param_name] = value
                    # Build filter condition for list (will try this first)
                    # Store both versions - will be handled in query execution
                    with_conditions.append(("anatomical_sites_list", param_name))
                    # Also store string version for fallback
                    with_conditions.append(("anatomical_sites_string", param_name))
            elif field == "library_selection_method":
                # Apply reverse mapping for filtering (API value -> DB value)
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                with_conditions.append(f"sf IS NOT NULL AND sf.library_selection = ${param_name}")
            elif field == "library_strategy":
                # Apply reverse mapping for filtering (API value -> DB value)
                # For "Other", we need to match both "Archer Fusion" (reverse mapped) and "Other" (direct match)
                db_value = reverse_map_field_value("library_strategy", value)
                if db_value is None:
                    # If no reverse mapping, use the value as-is (for values not in mapping)
                    params[param_name] = value
                    with_conditions.append(f"sf IS NOT NULL AND sf.library_strategy = ${param_name}")
                else:
                    # We have a reverse mapping - need to match both the mapped value and the original value
                    mapped_db_value = db_value if isinstance(db_value, str) else (db_value[0] if isinstance(db_value, list) and db_value else value)
                    # Match either the reverse-mapped value OR the original value (in case DB already has "Other")
                    param_counter += 1
                    param_name_original = f"param_{param_counter}"
                    params[param_name] = mapped_db_value
                    params[param_name_original] = value
                    with_conditions.append(f"sf IS NOT NULL AND (sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name_original})")
            elif field == "specimen_molecular_analyte_type":
                # Apply reverse mapping for filtering (API value -> DB value(s))
                # "RNA" can map to both "Transcriptomic" and "Viral RNA" in DB
                # Special handling: need to check if ANY sequencing_file matches, not just the first one
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values map to this API value - store as special condition
                    # Will be handled after collecting all sequencing_files
                    with_conditions.append(("specimen_molecular_analyte_type_list", reverse_mapped))
                elif reverse_mapped is None:
                    # Filter for null or "Not Reported"
                    with_conditions.append(f"(sf IS NULL OR sf.library_source_molecule IS NULL OR sf.library_source_molecule = 'Not Reported')")
                else:
                    params[param_name] = reverse_mapped
                    # Store as special condition - will be handled after collecting all sequencing_files
                    with_conditions.append(("specimen_molecular_analyte_type_single", param_name))
            elif field == "disease_phase":
                # Apply reverse mapping for filtering (API value -> DB value(s))
                # "Relapse" can map to both "Recurrent Disease" and "Relapse" in DB
                reverse_mapped = reverse_map_field_value("disease_phase", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values map to this API value - use OR condition
                    db_values_list = [f"'{v}'" for v in reverse_mapped]
                    db_values_str = ", ".join(db_values_list)
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase IN [{db_values_str}]")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
            elif field == "library_source_material":
                # Apply reverse mapping for filtering (API value -> DB value)
                # "Other" maps to null, so filter for null values
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                if reverse_mapped is None:
                    # Filter for null or "Other" values
                    with_conditions.append(f"(sf IS NULL OR sf.library_source_material IS NULL OR sf.library_source_material = 'Other')")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"sf IS NOT NULL AND sf.library_source_material = ${param_name}")
            elif field == "preservation_method":
                with_conditions.append(f"pf IS NOT NULL AND pf.fixation_embedding_method = ${param_name}")
            elif field == "tumor_classification":
                # Apply reverse mapping for filtering (API value -> DB value)
                # "non-malignant" maps to null, so filter for null values
                reverse_mapped = reverse_map_field_value("tumor_classification", value)
                if reverse_mapped is None:
                    # Filter for null or "non-malignant" values
                    with_conditions.append(f"(diagnoses IS NULL OR diagnoses.tumor_classification IS NULL OR diagnoses.tumor_classification = 'non-malignant')")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_classification = ${param_name}")
            elif field == "tumor_grade":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_grade = ${param_name}")
            elif field == "age_at_diagnosis":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.age_at_diagnosis = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "age_at_collection":
                with_conditions.append(f"sa.participant_age_at_collection = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "depositions":
                with_conditions.append(f"st IS NOT NULL AND st.study_id = ${param_name}")
            elif field == "diagnosis":
                # Check both diagnoses.diagnosis and diagnoses.diagnosis_comment (for "see diagnosis_comment" cases)
                # If diagnosis is "see diagnosis_comment", check the comment field instead
                diagnosis_condition = (
                    f"(diagnoses IS NOT NULL AND "
                    f"(diagnoses.diagnosis = ${param_name} OR "
                    f"(toLower(trim(toString(diagnoses.diagnosis))) = 'see diagnosis_comment' AND "
                    f"diagnoses.diagnosis_comment IS NOT NULL AND "
                    f"trim(toString(diagnoses.diagnosis_comment)) = ${param_name})))"
                )
                with_conditions.append(diagnosis_condition)
            else:
                # Default to sample node
                if isinstance(value, list):
                    with_conditions.append(f"sa.{field} IN ${param_name}")
                else:
                    with_conditions.append(f"sa.{field} = ${param_name}")
            
            if field not in ["disease_phase", "tumor_grade", "age_at_diagnosis", "age_at_collection", "diagnosis", "anatomical_sites"]:
                # For non-diagnosis fields, handle list values
                # Skip anatomical_sites as it's handled specially with tuples
                if isinstance(value, list):
                    # Only replace if the last condition is a string (not a tuple)
                    if with_conditions and isinstance(with_conditions[-1], str):
                        with_conditions[-1] = with_conditions[-1].replace(f"= ${param_name}", f"IN ${param_name}")
            
            # Only set param if not already set (age fields set it above)
            if param_name not in params:
                params[param_name] = value
        
        # Separate anatomical_sites and specimen_molecular_analyte_type conditions from regular conditions
        anatomical_sites_param = None
        anatomical_sites_list_condition = None
        anatomical_sites_string_condition = None
        specimen_molecular_analyte_type_list = None
        specimen_molecular_analyte_type_single_param = None
        regular_conditions = []
        
        for condition in with_conditions:
            if isinstance(condition, tuple) and condition[0] == "anatomical_sites_list":
                anatomical_sites_param = condition[1]
                # Check if the parameter value is a list (multiple values from || delimiter)
                param_value = params.get(condition[1])
                if isinstance(param_value, list):
                    # Multiple values - check if ANY of them match (OR logic)
                    # For each value in the list, check if it matches the anatomic_site (exact or in semicolon-separated list)
                    # Build OR conditions for each value
                    or_conditions = []
                    for idx, val in enumerate(param_value):
                        val_param = f"{condition[1]}_{idx}"
                        params[val_param] = val
                        or_conditions.append(f"""(
                            ${val_param} = sa.anatomic_site OR
                            reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                              CASE WHEN trim(tok) = trim(toString(${val_param})) THEN true ELSE found END
                            ) = true
                        )""")
                    anatomical_sites_list_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
                else:
                    # Single value - handle both exact match and semicolon-separated string cases
                    anatomical_sites_list_condition = f"""sa.anatomic_site IS NOT NULL AND (
                        ${condition[1]} = sa.anatomic_site OR
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${condition[1]})) THEN true ELSE found END
                        ) = true
                    )"""
            elif isinstance(condition, tuple) and condition[0] == "anatomical_sites_string":
                # Handle both exact match and semicolon-separated string cases
                # Check exact match (trimmed) OR if the value matches exactly one element after splitting by ';'
                anatomical_sites_string_condition = f"""sa.anatomic_site IS NOT NULL AND (
                    trim(toString(sa.anatomic_site)) = trim(toString(${condition[1]})) OR
                    reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                      CASE WHEN trim(tok) = trim(toString(${condition[1]})) THEN true ELSE found END
                    ) = true
                )"""
            elif isinstance(condition, tuple) and condition[0] == "specimen_molecular_analyte_type_list":
                # Store the list of DB values that map to the API value (e.g., ["Transcriptomic", "Viral RNA"] for "RNA")
                specimen_molecular_analyte_type_list = condition[1]
            elif isinstance(condition, tuple) and condition[0] == "specimen_molecular_analyte_type_single":
                # Store the parameter name for single value mapping
                specimen_molecular_analyte_type_single_param = condition[1]
            else:
                regular_conditions.append(condition)
        
        # Build WHERE clause conditions
        all_conditions = regular_conditions.copy()
        if anatomical_sites_list_condition:
            all_conditions.append(anatomical_sites_list_condition)
        # Add specimen_molecular_analyte_type condition if present (will be checked after collecting all sequencing_files)
        if specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param:
            all_conditions.append("has_matching_sf = true")
        
        # Separate cheap filters (can be applied before OPTIONAL MATCHes) from expensive ones
        # Cheap filters: sample_id, anatomic_site (sample node properties)
        # Expensive filters: st IS NOT NULL (requires OPTIONAL MATCHes), participant/diagnosis filters
        early_where_conditions = [
            "sa.sample_id IS NOT NULL",
            "toString(sa.sample_id) <> ''"
        ]
        late_where_conditions = [
            "st IS NOT NULL"  # Ensure sample has a path to a study (requires OPTIONAL MATCHes)
        ]
        
        # Extract anatomical_sites condition if present (can be applied early)
        anatomical_sites_early_condition = None
        if anatomical_sites_list_condition:
            anatomical_sites_early_condition = anatomical_sites_list_condition
            # Remove it from all_conditions since we'll apply it early
            all_conditions = [c for c in all_conditions if c != anatomical_sites_list_condition]
        
        # Add anatomical_sites to early conditions if present
        if anatomical_sites_early_condition:
            early_where_conditions.append(anatomical_sites_early_condition)
        
        # Build early WHERE clause (applied before OPTIONAL MATCHes)
        early_where_clause = "\n        WHERE " + " AND ".join(early_where_conditions) if early_where_conditions else ""
        
        # Preserve identifiers condition if it exists (it was added earlier)
        where_conditions = where_conditions if where_conditions else []
        # Add late conditions
        for late_cond in late_where_conditions:
            if late_cond not in where_conditions:
                where_conditions.append(late_cond)
        # Add filter conditions (excluding anatomical_sites which is already in early_where_clause)
        if all_conditions:
            where_conditions.extend(all_conditions)
        
        # Build late WHERE clause (applied after OPTIONAL MATCHes and WITH)
        where_clause = "\n        WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Determine which OPTIONAL MATCH clauses are needed based on filters
        # Always include participant for identifiers
        needs_participant = True  # Always needed for identifiers
        needs_participant_for_filters = any(
            field in filters for field in ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status"]
        ) or any("p." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        # Always fetch diagnosis, pathology_file, and sequencing_file nodes for metadata fields
        # These are needed for disease_phase, tumor_grade, tumor_classification, age_at_diagnosis,
        # preservation_method, library_selection_method, library_strategy, library_source_material
        needs_diagnosis = any(
            field in filters for field in ["disease_phase", "tumor_grade", "age_at_diagnosis", "diagnosis"]
        ) or any("d." in str(cond) or "diagnoses" in str(cond) for cond in all_conditions if isinstance(cond, str))
        # Always include diagnosis for metadata fields
        needs_diagnosis = True
        
        needs_pathology_file = any(
            field in filters for field in ["preservation_method"]
        ) or any("pf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        # Always include pathology_file for metadata fields
        needs_pathology_file = True
        
        needs_sequencing_file = any(
            field in filters for field in ["library_selection_method", "library_strategy", "library_source_material"]
        ) or any("sf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        # Always include sequencing_file for metadata fields
        needs_sequencing_file = True
        
        # Build OPTIONAL MATCH clauses
        optional_matches = []
        # Always include participant for identifiers
        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
        # Always include diagnosis, pathology_file, and sequencing_file for metadata
        optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
        optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
        # Always include study paths
        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
        
        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        
        # Build WITH clause - always include diagnosis, pathology_file, and sequencing_file for metadata
        with_vars = ["sa", "p"]  # Always include participant for identifiers
        # For specimen_molecular_analyte_type, collect ALL sequencing_files first to check if ANY match
        if specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param:
            with_collects = [
                "head(collect(DISTINCT d)) AS diagnoses",
                "head(collect(DISTINCT pf)) AS pf",
                "collect(DISTINCT sf) AS all_sfs"  # Collect all sequencing_files
            ]
        else:
            with_collects = [
                "head(collect(DISTINCT d)) AS diagnoses",
                "head(collect(DISTINCT pf)) AS pf",
                "head(collect(DISTINCT sf)) AS sf"
            ]
        with_vars.append("coalesce(st1, st2) AS st")
        
        with_clause = ", ".join(with_vars)
        if with_collects:
            with_clause += ",\n             " + ",\n             ".join(with_collects)
        
        # Add identifiers_condition to WITH clause if present
        if identifiers_condition:
            with_clause += identifiers_condition
        
        # For specimen_molecular_analyte_type, we need a second WITH clause to use all_sfs
        # (can't reference a variable in the same WITH clause where it's defined)
        second_with_clause = None
        if specimen_molecular_analyte_type_list:
            # Build list of DB values for IN clause
            db_values_str = ", ".join([f"'{v}'" for v in specimen_molecular_analyte_type_list])
            second_with_vars = ["sa", "p", "st", "diagnoses", "pf"]
            second_with_vars.append(f"size([sf IN all_sfs WHERE sf IS NOT NULL AND sf.library_source_molecule IN [{db_values_str}]]) > 0 AS has_matching_sf")
            second_with_vars.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
            second_with_clause = ", ".join(second_with_vars)
        elif specimen_molecular_analyte_type_single_param:
            # Single value mapping
            second_with_vars = ["sa", "p", "st", "diagnoses", "pf"]
            second_with_vars.append(f"size([sf IN all_sfs WHERE sf IS NOT NULL AND sf.library_source_molecule = ${specimen_molecular_analyte_type_single_param}]) > 0 AS has_matching_sf")
            second_with_vars.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
            second_with_clause = ", ".join(second_with_vars)
        
        # If identifiers are present, integrate WHERE clause into WITH clause
        # Otherwise, apply WHERE clause separately
        if identifiers_condition and where_clause:
            # Remove "WHERE " prefix and integrate into WITH clause
            where_conditions_str = where_clause.replace("WHERE ", "").strip()
            if where_conditions_str:
                with_clause += f"\n        WHERE {where_conditions_str}"
                where_clause = ""  # Clear it since it's now in WITH clause
        
        # Build RETURN clause - always include diagnosis, pathology_file, and sequencing_file for metadata
        return_vars = ["sa", "p", "st", "sf", "pf", "diagnoses"]
        
        return_clause = ", ".join(return_vars)
        
        # Build unified query
        # Only include samples that have a path to a study
        # Path 1: sample -> cell_line -> study
        # Path 2: sample -> participant -> consent_group -> study
        # Build the DISTINCT clause with all return variables
        distinct_vars = ", ".join(return_vars)
        
        # If we have a second WITH clause (for specimen_molecular_analyte_type), include it
        second_with_str = ""
        if second_with_clause:
            # Apply WHERE clause after second WITH if present
            second_with_str = f"WITH {second_with_clause}\n        {where_clause}\n        "
            where_clause = ""  # Clear it since it's now in second WITH
        elif where_clause:
            # Apply WHERE clause after first WITH if no second WITH
            where_clause = f"{where_clause}\n        "
        else:
            where_clause = ""
        
        cypher = f"""
        MATCH (sa:sample)
        {early_where_clause}
        {optional_matches_str}
        WITH {with_clause}
        {second_with_str}{where_clause}WITH DISTINCT {distinct_vars}
        RETURN {return_clause}
        ORDER BY sa.sample_id
        SKIP $offset
        LIMIT $limit
        """.strip()
        
        # Build a debug version of the query with parameter values substituted for easier debugging
        debug_cypher = cypher
        for param_name, param_value in params.items():
            # Replace parameter placeholders with actual values for debugging
            debug_cypher = debug_cypher.replace(f"${param_name}", repr(param_value))
        
        logger.info(
            "Executing get_samples Cypher query",
            cypher=cypher,
            debug_cypher=debug_cypher,
            params=params,
            with_clause=with_clause,
            where_clause=where_clause,
            return_clause=return_clause,
            identifiers_condition=identifiers_condition if 'identifiers_condition' in locals() else None
        )
        
        # Execute query with proper result consumption and retry logic
        # For anatomical_sites, try list version first, fallback to string if it fails
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
                    logger.debug(f"Retrying get_samples query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_samples query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    # Re-raise to be handled by outer try-except
                    raise
        
        try:
            logger.info(
                "Query executed successfully",
                records_count=len(records),
                cypher=cypher[:500] if cypher else None,
                sample_records=records[:3] if records else []  # Log first 3 records for debugging
            )
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(
                "Error executing get_samples query",
                error=str(e),
                error_type=type(e).__name__,
                filters=filters,
                cypher=cypher[:500] if cypher else None,
                exc_info=True
            )
            # If anatomical_sites filter is present and we got an IN error, try string version
            if anatomical_sites_param and anatomical_sites_string_condition and ("in expected a list" in error_msg or "in expected" in error_msg):
                logger.debug("List query failed for anatomical_sites, trying string version")
                # Rebuild query with string version
                all_conditions = regular_conditions.copy()
                all_conditions.append(anatomical_sites_string_condition)  # Use string version
                
                # Build WHERE clause conditions - combine sample_id check with filter conditions
                where_conditions = [
                    "sa.sample_id IS NOT NULL",
                    "toString(sa.sample_id) <> ''",
                    "st IS NOT NULL"  # Ensure sample has a path to a study
                ]
                if all_conditions:
                    where_conditions.extend(all_conditions)
                
                where_clause = "\n        WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                # Use same conditional logic as main query
                # Determine which OPTIONAL MATCH clauses are needed
                needs_participant = any("p." in str(cond) for cond in all_conditions if isinstance(cond, str))
                # Always include diagnosis, pathology_file, and sequencing_file for metadata fields
                optional_matches = []
                # Always include participant for identifiers
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                # Always include diagnosis, pathology_file, and sequencing_file for metadata
                optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                
                optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                
                # Always include diagnosis, pathology_file, and sequencing_file for metadata
                with_vars = ["sa", "p"]  # Always include participant for identifiers
                with_collects = [
                    "head(collect(DISTINCT d)) AS diagnoses",
                    "head(collect(DISTINCT pf)) AS pf",
                    "head(collect(DISTINCT sf)) AS sf"
                ]
                with_vars.append("coalesce(st1, st2) AS st")
                
                with_clause = ", ".join(with_vars)
                if with_collects:
                    with_clause += ",\n                     " + ",\n                     ".join(with_collects)
                # Add identifiers_condition to WITH clause if present
                if identifiers_condition:
                    with_clause += identifiers_condition
                
                # Always include diagnosis, pathology_file, and sequencing_file for metadata
                return_vars = ["sa", "p", "st", "sf", "pf", "diagnoses"]
                
                return_clause = ", ".join(return_vars)
                
                cypher = f"""
                MATCH (sa:sample)
                {optional_matches_str}
                WITH {with_clause}
                {where_clause}
                WITH DISTINCT {return_clause}
                RETURN {return_clause}
                ORDER BY sa.sample_id
                SKIP $offset
                LIMIT $limit
                """.strip()
                
                # Build debug version with parameter substitution
                debug_cypher_string = cypher
                for param_name, param_value in params.items():
                    debug_cypher_string = debug_cypher_string.replace(f"${param_name}", repr(param_value))
                
                logger.info(
                    "Retrying anatomical_sites query with string version",
                    cypher=cypher,
                    debug_cypher=debug_cypher_string,
                    params=params,
                    where_clause=where_clause
                )
                
                try:
                    result = await self.session.run(cypher, params)
                    async for record in result:
                        records.append(dict(record))
                    
                    logger.debug("Successfully executed anatomical_sites query as string")
                    logger.info(
                        "Query executed successfully (string version)",
                        records_count=len(records),
                        cypher=cypher,
                        debug_cypher=debug_cypher_string,
                        params=params
                    )
                except Exception as e2:
                    logger.error(
                        "Error executing get_samples Cypher query (both list and string failed for anatomical_sites)",
                        error=str(e2),
                        error_type=type(e2).__name__,
                        cypher=cypher[:500],
                        exc_info=True
                )
                raise
            else:
                # Different error, re-raise
                logger.error(
                    "Error executing get_samples Cypher query",
                    error=str(e),
                    error_type=type(e).__name__,
                    cypher=cypher[:500] if cypher else None,
                    exc_info=True
                )
                raise
        
        # Convert to Sample objects (this code runs after successful query execution)
        samples = []
        skipped_count = 0
        for idx, record in enumerate(records):
            try:
                # Extract nodes from record
                sa_node = record.get("sa")
                p_node = record.get("p")
                st_node = record.get("st")
                sf_node = record.get("sf")
                pf_node = record.get("pf")
                diagnoses_node = record.get("diagnoses")
                
                # Convert Memgraph/Neo4j Node objects to dictionaries
                # Node objects can be converted using dict() or by accessing .items()
                def node_to_dict(node):
                    """Convert a Node object to a dictionary."""
                    if node is None:
                        return {}
                    if isinstance(node, dict):
                        return node
                    # Try dict() conversion first (works for Neo4j/Memgraph Node objects)
                    try:
                        return dict(node)
                    except (TypeError, ValueError):
                        # Fall back to accessing properties
                        if hasattr(node, 'properties'):
                            return node.properties
                        elif hasattr(node, 'items'):
                            return dict(node.items())
                        else:
                            # Last resort: try to get all attributes
                            return {k: getattr(node, k) for k in dir(node) if not k.startswith('_')}
                
                sa = node_to_dict(sa_node)
                p = node_to_dict(p_node)
                st = node_to_dict(st_node)
                sf = node_to_dict(sf_node)
                pf = node_to_dict(pf_node)
                # diagnoses is now a single node (or None) - convert to dict
                diagnoses = node_to_dict(diagnoses_node) if diagnoses_node else None
                
                sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url=base_url)
                samples.append(sample)
            except Exception as e:
                skipped_count += 1
                logger.warning(
                    "Error converting record to sample, skipping",
                    error=str(e),
                    error_type=type(e).__name__,
                    record_idx=idx,
                    skipped_count=skipped_count,
                    record_keys=list(record.keys()) if record else [],
                    sa_node_type=type(sa_node).__name__ if 'sa_node' in locals() and sa_node else None
                )
                # Continue processing other records instead of failing completely
                continue
        
        if skipped_count > 0:
            logger.warning(
                "Some records were skipped during conversion",
                total_records=len(records),
                successful=len(samples),
                skipped=skipped_count
            )
        
        logger.info(
            "Found samples",
            count=len(samples),
            filters=filters,
            records_count=len(records) if 'records' in locals() else 0
        )
        
        # If no samples found, return empty list (not an error)
        if not samples:
            logger.info("No samples found matching criteria", filters=filters)
        
        return samples
    
    async def get_sample_by_identifier(
        self,
        organization: str,
        namespace: str,
        name: str,
        base_url: Optional[str] = None
    ) -> Optional[Sample]:
        """
        Get a specific sample by organization, namespace, and name.
        
        Args:
            organization: Organization identifier
            namespace: Namespace identifier
            name: Sample name/identifier
            
        Returns:
            Sample object or None if not found
        """
        logger.debug(
            "Fetching sample by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Build query to find sample by identifier using relationships
        # Samples can be connected to studies via:
        # 1. sample -> participant -> consent_group -> study
        # 2. sample -> cell_line -> study
        cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id = $sample_name
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH sa, p, coalesce(st1, st2) AS st
        WHERE st IS NOT NULL AND st.study_id = $namespace
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, p, st, sf, pf, collect(DISTINCT d) AS diagnoses
        RETURN sa, p, st, sf, pf, diagnoses
        LIMIT 1
        """
        
        params = {
            "sample_name": name,
            "namespace": namespace
        }
        
        logger.info(
            "Executing get_sample_by_identifier Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query with proper result consumption
        result = await self.session.run(cypher, params)
        records = []
        async for record in result:
            records.append(dict(record))
        
        if not records:
            logger.debug("Sample not found", organization=organization, namespace=namespace, name=name)
            return None
        
        # Convert to Sample object
        record = records[0]
        # Convert Neo4j Node objects to dictionaries
        sa_node = record.get("sa")
        p_node = record.get("p")
        st_node = record.get("st")
        sf_node = record.get("sf")
        pf_node = record.get("pf")
        diagnoses_nodes = record.get("diagnoses", [])
        
        # Convert nodes to dictionaries
        sa = dict(sa_node) if sa_node else {}
        p = dict(p_node) if p_node else {}
        st = dict(st_node) if st_node else {}
        sf = dict(sf_node) if sf_node else {}
        pf = dict(pf_node) if pf_node else {}
        diagnoses = [dict(d) if d else {} for d in diagnoses_nodes]
        
        # Handle diagnoses - take first one if it's a list
        diagnoses_dict = diagnoses[0] if diagnoses and isinstance(diagnoses, list) and len(diagnoses) > 0 else (diagnoses if isinstance(diagnoses, dict) else None)
        
        sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses_dict, base_url=base_url)
        
        logger.debug("Found sample", organization=organization, namespace=namespace, name=name)
        
        return sample
    
    async def count_samples_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count samples grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            List of dictionaries with value and count
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting samples by field",
            field=field,
            filters=filters
        )
        
        # Special handling for diagnosis field - use dedicated method with conversion logic
        if field == "diagnosis":
            return await self._count_samples_by_associated_diagnoses(filters)
        
        # Validate field is allowed for count operations
        # Only sample-specific metadata fields are allowed (participant fields are not supported for samples)
        sample_metadata_fields = {
            "disease_phase", "anatomical_sites", "library_selection_method", "library_strategy",
            "library_source_material", "preservation_method", "tumor_grade", "specimen_molecular_analyte_type",
            "tissue_type", "tumor_classification", "age_at_diagnosis", "age_at_collection",
            "tumor_tissue_morphology", "diagnosis"
        }
        allowed_fields = sample_metadata_fields
        if field not in allowed_fields:
            raise UnsupportedFieldError(
                field=field,
                entity_type="sample"
            )
        
        # Note: Participant fields (race, ethnicity, associated_diagnoses, sex, vital_status, age_at_vital_status)
        # are not supported for sample count endpoints - only sample metadata fields are allowed
        
        # Build WHERE conditions and parameters with relationships
        # Map field to correct node based on field type
        # Participant fields come from participant node
        # Sample metadata fields come from sample, sequencing_file, pathology_file, diagnosis, or study nodes
        participant_field_mapping = {
            "sex": "p.sex_at_birth",
            "race": "p.race",
            "ethnicity": "p.ethnicity",
            "vital_status": "p.vital_status",
            "age_at_vital_status": "p.age_at_vital_status",
            "associated_diagnoses": "d.diagnosis"  # Special case - from diagnosis nodes
        }
        
        # Sample metadata field mapping - maps to the actual node and property
        sample_metadata_field_mapping = {
            "disease_phase": ("d", "disease_phase"),  # From diagnosis node
            "anatomical_sites": ("sa", "anatomic_site"),  # From sample node
            "library_selection_method": ("sf", "library_selection"),  # From sequencing_file node
            "library_strategy": ("sf", "library_strategy"),  # From sequencing_file node
            "library_source_material": ("sf", "library_source_material"),  # From sequencing_file node
            "preservation_method": ("pf", "fixation_embedding_method"),  # From pathology_file node
            "tumor_grade": ("d", "tumor_grade"),  # From diagnosis node
            "specimen_molecular_analyte_type": ("sf", "library_source_molecule"),  # From sequencing_file node
            "tissue_type": ("sa", "tissue_type"),  # From sample node (if exists)
            "tumor_classification": ("d", "tumor_classification"),  # From diagnosis node
            "age_at_diagnosis": ("d", "age_at_diagnosis"),  # From diagnosis node
            "age_at_collection": ("sa", "participant_age_at_collection"),  # From sample node
            "tumor_tissue_morphology": ("sa", "tumor_tissue_morphology"),  # From sample node (if exists)
            "depositions": ("st", "study_id"),  # From study node
            "diagnosis": ("d", "diagnosis")  # From diagnosis node
        }
        
        # Determine if this is a participant field or sample metadata field
        is_participant_field = field in participant_field_mapping
        is_sample_metadata_field = field in sample_metadata_field_mapping
        
        if is_participant_field:
            node_field = participant_field_mapping[field]
        elif is_sample_metadata_field:
            node_alias, property_name = sample_metadata_field_mapping[field]
            node_field = f"{node_alias}.{property_name}"
        else:
            # Fallback (should not reach here due to validation above)
            node_field = f"p.{field}"
        
        # Build base WHERE conditions for filtering
        base_where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle race parameter normalization (same as subjects)
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
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    base_where_conditions.append(f"""ANY(tok IN ${race_param} WHERE tok IN [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)])""")
        
        # Handle identifiers parameter
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                if isinstance(identifiers_value, list):
                    base_where_conditions.append(f"sa.sample_id IN ${id_param}")
                else:
                    base_where_conditions.append(f"sa.sample_id = ${id_param}")
        
        # Handle depositions filter (study_id)
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            if depositions_value is not None and str(depositions_value).strip():
                param_counter += 1
                dep_param = f"param_{param_counter}"
                params[dep_param] = str(depositions_value).strip()
                base_where_conditions.append(f"st.study_id = ${dep_param}")
        
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
        
        # Handle anatomical_sites filter (sample field, not participant field)
        if "anatomical_sites" in filters:
            anatomical_sites_value = filters.pop("anatomical_sites")
            if anatomical_sites_value is not None:
                param_counter += 1
                param_name = f"param_{param_counter}"
                # Normalize the value (trim whitespace) for consistent matching
                if isinstance(anatomical_sites_value, list):
                    # Multiple values - check if ANY of them match (OR logic)
                    params[param_name] = [v.strip() if isinstance(v, str) else v for v in anatomical_sites_value]
                    # Build OR conditions for each value
                    or_conditions = []
                    for idx, val in enumerate(anatomical_sites_value):
                        val_param = f"{param_name}_{idx}"
                        params[val_param] = val.strip() if isinstance(val, str) else val
                        or_conditions.append(f"""(
                            ${val_param} = sa.anatomic_site OR 
                            reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                              CASE WHEN trim(tok) = trim(toString(${val_param})) THEN true ELSE found END
                            ) = true
                        )""")
                    anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
                elif isinstance(anatomical_sites_value, str):
                    anatomical_sites_value = anatomical_sites_value.strip()
                    params[param_name] = anatomical_sites_value
                    # anatomical_sites can be either a list, a string, or a semicolon-separated string
                    # Simplified condition: handle both list and string cases
                    # For list: use IN operator
                    # For string: split by ';', trim each element, and check if search value matches exactly one element
                    anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND (
                        ${param_name} = sa.anatomic_site OR 
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${param_name})) THEN true ELSE found END
                        ) = true
                    )"""
                else:
                    params[param_name] = anatomical_sites_value
                    anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND (
                        ${param_name} = sa.anatomic_site OR 
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${param_name})) THEN true ELSE found END
                        ) = true
                    )"""
                base_where_conditions.append(anatomical_sites_condition)
                logger.debug(
                    "Added anatomical_sites filter condition",
                    param_name=param_name,
                    param_value=anatomical_sites_value,
                    condition=anatomical_sites_condition
                )
        
        # Add regular filters (participant fields)
        for filter_field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Map filter field names to actual database fields
            # For sex filter, use sex_at_birth in the query
            db_field = "sex_at_birth" if filter_field == "sex" else filter_field
            
            if isinstance(value, list):
                base_where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                base_where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build base WHERE clause (for filtering)
        base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""
        
        # Standard field handling (sex, vital_status, age_at_vital_status)
        # For sex field, use simplified direct query with normalization
        if field == "sex":
            # Build sex normalization CASE statement from config
            sex_normalization = self._build_sex_normalization_case(field)
            
            # Build WHERE clause with field filter
            # Note: We add IS NOT NULL filter for the main query, but total/missing queries use base_where_clause
            field_where_conditions = base_where_conditions.copy() if base_where_conditions else []
            field_where_conditions.append(f"{node_field} IS NOT NULL")
            field_where_conditions.append("st IS NOT NULL")  # Ensure sample has a path to a study
            field_where_clause = "WHERE " + " AND ".join(field_where_conditions) if field_where_conditions else f"WHERE {node_field} IS NOT NULL AND st IS NOT NULL"
            
            if sex_normalization:
                # Use normalization CASE statement (it already includes CASE/END)
                # Group by sample_id first to get one sex value per sample (using head to pick first)
                # This ensures each sample is counted only once, even if it has multiple participants
                # Only include samples that have a path to a study
                # Path 1: sample -> cell_line -> study
                # Path 2: sample -> participant -> consent_group -> study
                cypher = f"""
                MATCH (sa:sample)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, 
                     head(collect(DISTINCT p.sex_at_birth)) as value
                WITH sample_id, value,
                     {sex_normalization} as normalized_value
                RETURN normalized_value AS sex_at_birth,
                       count(DISTINCT sample_id) AS sample_count
                ORDER BY sample_count DESC, sex_at_birth ASC
                """.strip()
            else:
                # Fallback without normalization
                # Group by sample_id first to get one sex value per sample
                # Only include samples that have a path to a study
                # Path 1: sample -> cell_line -> study
                # Path 2: sample -> participant -> consent_group -> study
                cypher = f"""
                MATCH (sa:sample)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, 
                     head(collect(DISTINCT p.sex_at_birth)) as value
                RETURN value AS sex_at_birth,
                       count(DISTINCT sample_id) AS sample_count
                ORDER BY sample_count DESC, sex_at_birth ASC
                """.strip()
        elif field == "anatomical_sites":
            # Special handling for anatomical_sites - it's an array field in sample node
            # Need to unwind the array and count each value
            # Build WHERE clause - always include anatomic_site check and study path check
            field_where_conditions = base_where_conditions.copy() if base_where_conditions else []
            field_where_conditions.append("sa.anatomic_site IS NOT NULL")
            field_where_conditions.append("st IS NOT NULL")  # Ensure sample has a path to a study
            field_where_clause = "WHERE " + " AND ".join(field_where_conditions)
            
            # Build query - handle both cases with and without base filters
            # Always include study paths to ensure samples have a path to a study
            # Path 1: sample -> cell_line -> study
            # Path 2: sample -> participant -> consent_group -> study
            # Check if we need to join with participant/study nodes (only if we have base filters that reference them)
            has_participant_filters = any(
                "p." in cond or "st." in cond or "d." in cond 
                for cond in base_where_conditions
            ) if base_where_conditions else False
            
            # Build queries for both list and string cases
            # We'll try list first, and if it fails, try string
            # Store both queries as a tuple for anatomical_sites
            if has_participant_filters:
                # Has base filters - need to join with participant/study nodes
                # Determine which joins are actually needed based on base filters
                needs_participant = any("p." in cond for cond in base_where_conditions)
                needs_study = any("st." in cond for cond in base_where_conditions)
                needs_diagnosis = any("d." in cond or "diagnoses" in cond for cond in base_where_conditions)
                
                optional_matches = []
                if needs_participant:
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                # Always include study paths
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                if needs_diagnosis:
                    optional_matches.append("OPTIONAL MATCH (sa)<-[:of_diagnosis]-(d:diagnosis)")
                
                optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                
                # Query 1: Assume it's a list (or could be semicolon-separated string)
                # Handle both list and semicolon-separated string cases
                cypher_list = f"""
                MATCH (sa:sample)
                {optional_matches_str}
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id,
                     CASE 
                       WHEN valueType(sites) = 'LIST' THEN sites
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH DISTINCT sample_id, trimmed_value
                RETURN trimmed_value as value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                
                # Query 2: Assume it's a string (could be semicolon-separated)
                # Split by semicolon if it contains ';', otherwise treat as single value
                cypher_string = f"""
                MATCH (sa:sample)
                {optional_matches_str}
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id, 
                     CASE 
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH DISTINCT sample_id, trimmed_value
                RETURN trimmed_value as value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
            else:
                # No base filters - simpler query but still need study paths
                # Query 1: Assume it's a list
                cypher_list = f"""
                MATCH (sa:sample)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id,
                     CASE 
                       WHEN valueType(sites) = 'LIST' THEN sites
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH DISTINCT sample_id, trimmed_value
                RETURN trimmed_value as value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                
                # Query 2: Assume it's a string
                cypher_string = f"""
                MATCH (sa:sample)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa.sample_id as sample_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id, 
                     CASE 
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH DISTINCT sample_id, trimmed_value
                RETURN trimmed_value as value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
            
            # Store both queries as a tuple for anatomical_sites (will be handled in execution)
            cypher = (cypher_list, cypher_string)
        else:
            # Other standard fields (vital_status, age_at_vital_status, or sample metadata fields)
            # Use similar pattern to sex: group by sample_id to get one value per sample
            field_where_conditions = base_where_conditions.copy() if base_where_conditions else []
            field_where_conditions.append(f"{node_field} IS NOT NULL")
            
            # Build combined WHERE clause - include study path check
            all_where_conditions = [
                "sa.sample_id IS NOT NULL",
                "toString(sa.sample_id) <> ''",
                "st IS NOT NULL"  # Ensure sample has a path to a study
            ]
            all_where_conditions.extend(field_where_conditions)
            combined_where_clause = "WHERE " + " AND ".join(all_where_conditions)
            
            # For sample metadata fields, we need to include OPTIONAL MATCH for related nodes
            if is_sample_metadata_field:
                node_alias, _ = sample_metadata_field_mapping[field]
                # Build query with necessary OPTIONAL MATCH clauses
                optional_matches = []
                
                # Check if base filters need participant or study nodes
                needs_participant = any("p." in cond for cond in base_where_conditions) if base_where_conditions else False
                needs_study = any("st." in cond for cond in base_where_conditions) if base_where_conditions else False
                
                # Always include the node needed for the field
                if node_alias == "sf":  # sequencing_file
                    optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                if node_alias == "pf":  # pathology_file
                    optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                if node_alias == "d":  # diagnosis
                    optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                if node_alias == "st":  # study
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                
                # Include participant if needed for base filters or for fields on sample node (for consistency with summary)
                if needs_participant or node_alias == "sa":
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                
                # Always include study paths to ensure samples have a path to a study
                # Path 1: sample -> cell_line -> study
                # Path 2: sample -> participant -> consent_group -> study
                if node_alias != "st":
                    # Only add if not already included above
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                    if not needs_study:
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                    else:
                        # If st was already included, we still need st2 for the coalesce
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                else:
                    # st was already included, but we need both paths for coalesce
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                
                optional_matches_str = "\n            ".join(optional_matches) if optional_matches else ""
                
                # Use coalesce for study if we have both paths
                study_var = "coalesce(st1, st2) AS st" if node_alias == "st" or (not needs_study and node_alias != "st") else "coalesce(st, st1) AS st" if node_alias != "st" and needs_study else "st"
                
                # Build WITH clause to include all needed variables
                with_vars = ["sa"]
                # Include the node for the field (sf, pf, d, or st)
                if node_alias == "sf":
                    with_vars.append("sf")
                elif node_alias == "pf":
                    with_vars.append("pf")
                elif node_alias == "d":
                    with_vars.append("d")
                # Include participant if needed (always include for consistency with summary)
                if needs_participant or node_alias == "sa":
                    with_vars.append("p")
                # Always include study (needed for st IS NOT NULL check in combined_where_clause)
                with_vars.append(study_var)
                
                with_clause = ", ".join(with_vars)
                
                # For specimen_molecular_analyte_type, use a different query structure:
                # 1. First find samples with valid study
                # 2. Then find sequencing_file associated with these samples
                # 3. Then categorize by specimen_molecular_analyte_type per sample
                if field == "specimen_molecular_analyte_type":
                    # Optimized query for specimen_molecular_analyte_type:
                    # 1. Start from sample (to match search query logic)
                    # 2. Collect all sequencing_files per sample
                    # 3. Check study path
                    # 4. Return sample_id and all molecule values for Python-side mapping and deduplication
                    # IMPORTANT: Must match search query logic - collect ALL sequencing_files per sample
                    # and check if ANY match, to avoid double-counting samples with multiple matching values
                    cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL 
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa.sample_id as sample_id, 
                     collect(DISTINCT sf.library_source_molecule) as molecule_values, 
                     coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                  AND size([val IN molecule_values WHERE val IS NOT NULL 
                            AND val <> '' 
                            AND val <> '-999'
                            AND val <> 'Not Reported']) > 0
                UNWIND [val IN molecule_values WHERE val IS NOT NULL 
                        AND val <> '' 
                        AND val <> '-999'
                        AND val <> 'Not Reported'] as molecule_value
                RETURN sample_id, molecule_value as value
                """.strip()
                elif node_alias == "sf" and not base_where_clause:
                    # Optimized query for other sequencing_file fields (library_selection_method, library_strategy, library_source_material):
                    # 1. Start from sequencing_file (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Group by field value and count distinct samples
                    # Performance improvements:
                    # - Start from sequencing_file instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Remove unnecessary participant match
                    # - Simplify redundant WHERE conditions (assume string fields)
                    cypher = f"""
                MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
                WHERE sa.sample_id IS NOT NULL 
                  AND sa.sample_id <> ''
                  AND {node_field} IS NOT NULL
                  AND {node_field} <> ''
                  AND {node_field} <> '-999'
                  AND {node_field} <> 'Not Reported'
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH DISTINCT sa.sample_id as sample_id, {node_field} as value, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                RETURN value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                elif node_alias == "pf" and not base_where_clause:
                    # Optimized query for pathology_file fields (preservation_method):
                    # 1. Start from pathology_file (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Group by field value and count distinct samples
                    # Performance improvements:
                    # - Start from pathology_file instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Remove unnecessary participant match
                    # - Simplify redundant WHERE conditions (assume string fields)
                    cypher = f"""
                MATCH (pf:pathology_file)-[:of_pathology_file]->(sa:sample)
                WHERE sa.sample_id IS NOT NULL 
                  AND sa.sample_id <> ''
                  AND {node_field} IS NOT NULL
                  AND {node_field} <> ''
                  AND {node_field} <> '-999'
                  AND {node_field} <> 'Not Reported'
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH DISTINCT sa.sample_id as sample_id, {node_field} as value, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                RETURN value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                elif node_alias == "d" and not base_where_clause:
                    # Optimized query for diagnosis fields (disease_phase, tumor_grade, etc.):
                    # 1. Start from diagnosis (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Use head() to get one diagnosis per sample, then group by field value
                    # Performance improvements:
                    # - Start from diagnosis instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Remove unnecessary participant match
                    # - Simplify redundant WHERE conditions (assume string fields)
                    cypher = f"""
                MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)
                WHERE sa.sample_id IS NOT NULL 
                  AND sa.sample_id <> ''
                  AND {node_field} IS NOT NULL
                  AND {node_field} <> ''
                  AND {node_field} <> '-999'
                  AND {node_field} <> 'Not Reported'
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH DISTINCT sa.sample_id as sample_id, head(collect(DISTINCT {node_field})) as value, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                RETURN value, count(DISTINCT sample_id) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                else:
                    # For other fields, use the standard query structure
                    additional_where = ""
                    cypher = f"""
                MATCH (sa:sample)
                {optional_matches_str}
                WITH {with_clause}
                {combined_where_clause}
                WITH DISTINCT sa, 
                     head(collect(DISTINCT {node_field})) as value
                WHERE value IS NOT NULL
                  AND toString(value) <> ''
                  AND trim(toString(value)) <> ''
                  AND toString(value) <> '-999'
                  AND trim(toString(value)) <> '-999'{additional_where}
                RETURN toString(value) as value, count(DISTINCT sa) AS count
                ORDER BY count DESC, value ASC
                """.strip()
            else:
                # Participant fields - use existing pattern but ensure study paths
                # Only include samples that have a path to a study
                # Path 1: sample -> cell_line -> study
                # Path 2: sample -> participant -> consent_group -> study
                cypher = f"""
                MATCH (sa:sample)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH DISTINCT sa, 
                     head(collect(DISTINCT {node_field})) as value
                WHERE value IS NOT NULL
                  AND toString(value) <> ''
                  AND trim(toString(value)) <> ''
                  AND toString(value) <> '-999'
                  AND trim(toString(value)) <> '-999'
                RETURN toString(value) as value, count(DISTINCT sa) AS count
                ORDER BY count DESC, value ASC
                """.strip()
        
        logger.info(
            "Executing count_samples_by_field Cypher query",
            cypher=cypher[:200] if isinstance(cypher, str) else "multiple queries",
            params=params,
            field=field
        )
        
        # Execute query with proper result consumption
        # For anatomical_sites, try list query first, fallback to string query if it fails
        records = []
        if field == "anatomical_sites" and isinstance(cypher, tuple):
            cypher_list, cypher_string = cypher
            try:
                # Try list query first
                result = await self.session.run(cypher_list, params)
                async for record in result:
                    records.append(dict(record))
                logger.debug("Successfully executed anatomical_sites query as list")
            except Exception as e:
                error_msg = str(e).lower()
                if "unwind" in error_msg and ("list" in error_msg or "string" in error_msg):
                    # It's a string, try the string query
                    logger.debug("List query failed, trying string query for anatomical_sites")
                    try:
                        result = await self.session.run(cypher_string, params)
                        async for record in result:
                            records.append(dict(record))
                        logger.debug("Successfully executed anatomical_sites query as string")
                    except Exception as e2:
                        logger.error(
                            "Error executing count_samples_by_field Cypher query (both list and string failed)",
                            error=str(e2),
                            error_type=type(e2).__name__,
                            field=field,
                            cypher=cypher_string[:500],
                            params=params,
                            exc_info=True
                        )
                        raise
                else:
                    # Different error, re-raise
                    logger.error(
                        "Error executing count_samples_by_field Cypher query",
                        error=str(e),
                        error_type=type(e).__name__,
                        field=field,
                        cypher=cypher_list[:500],
                        params=params,
                        exc_info=True
                    )
                    raise
        else:
            # Regular query execution
            try:
                result = await self.session.run(cypher, params)
                async for record in result:
                    records.append(dict(record))
            except Exception as e:
                logger.error(
                    "Error executing count_samples_by_field Cypher query",
                    error=str(e),
                    error_type=type(e).__name__,
                    field=field,
                    cypher=cypher[:500] if isinstance(cypher, str) else str(cypher)[:500],
                    params=params,
                    exc_info=True
                )
                raise
        
        logger.info(
            "Count query results",
            field=field,
            records_count=len(records),
            sample_records=records[:5] if records else [],
            query=cypher[:500] if field == "anatomical_sites" else None
        )
        
        # Format results
        counts = []
        
        # Special handling for specimen_molecular_analyte_type to avoid double-counting
        # Query returns sample_id and value (molecule_value) - need to map to API values
        # and count distinct samples per API value (not per DB value)
        if field == "specimen_molecular_analyte_type" and records and "sample_id" in records[0]:
            # Group by sample_id and collect all API values (after mapping)
            sample_to_api_values = {}
            for record in records:
                sample_id = record.get("sample_id")
                db_value = record.get("value")
                
                if not sample_id or not db_value:
                    continue
                
                # Map DB value to API value
                api_value = map_field_value(field, db_value)
                if api_value is None:
                    continue
                
                # Collect distinct API values per sample
                if sample_id not in sample_to_api_values:
                    sample_to_api_values[sample_id] = set()
                sample_to_api_values[sample_id].add(api_value)
            
            # Count samples per API value
            api_value_counts = {}
            for sample_id, api_values in sample_to_api_values.items():
                for api_value in api_values:
                    api_value_counts[api_value] = api_value_counts.get(api_value, 0) + 1
            
            # Convert to counts list
            for api_value, count in api_value_counts.items():
                counts.append({
                    "value": api_value,
                    "count": count
                })
            counts.sort(key=lambda x: (-x["count"], x["value"]))
        else:
            # Standard processing for other fields
            for record in records:
                # Handle both formats: sex returns sex_at_birth/sample_count, others return value/count
                if field == "sex" and "sex_at_birth" in record:
                    value = record.get("sex_at_birth")
                    # Filter out empty strings - skip them (they should be counted as missing)
                    if value == "" or (isinstance(value, str) and value.strip() == ""):
                        continue  # Skip empty values
                    counts.append({
                        "value": value,
                        "count": record.get("sample_count", 0)
                    })
                else:
                    value = record.get("value")
                    # Filter out empty strings and "-999" - skip them (they should be counted as missing)
                    if value == "" or (isinstance(value, str) and value.strip() == ""):
                        continue  # Skip empty values
                    # Filter out "-999" for age fields (sentinel value for missing data)
                    if str(value) == "-999" or (isinstance(value, str) and value.strip() == "-999"):
                        continue  # Skip "-999" values
                    
                    # Apply field mapping (DB value -> API value) using centralized mappings
                    mapped_value = map_field_value(field, value)
                    
                    # If mapping returns None, skip this value (it should be counted as missing)
                    # This handles null_mappings (e.g., "Not Reported" for specimen_molecular_analyte_type)
                    if mapped_value is None:
                        continue
                    
                    counts.append({
                        "value": mapped_value,
                        "count": record.get("count", 0)
                    })
            
            # Aggregate counts for fields where multiple DB values map to the same API value
            # (e.g., disease_phase: "Recurrent Disease" and "Relapse" both map to "Relapse")
            # Note: specimen_molecular_analyte_type is handled above, so this is for other fields
            aggregated_counts = {}
            for item in counts:
                val = item["value"]
                cnt = item["count"]
                if val in aggregated_counts:
                    aggregated_counts[val] += cnt
                else:
                    aggregated_counts[val] = cnt
            # Rebuild counts list and sort
            counts = [{"value": val, "count": cnt} for val, cnt in aggregated_counts.items()]
            counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        # Calculate total and missing counts
        # Total: count of all distinct samples matching filters
        # IMPORTANT: Total must match /sample/summary, which only counts samples with a path to a study
        if field == "sex":
            # Total: all samples with a path to a study (matching /sample/summary - 50211 when no filters)
            # This should match the total from /sample/summary endpoint
            # When no filters, count all samples that have a path to a study. When filters exist, they may reference participants,
            # so we need to handle that case - but for sex count, we want to count ALL samples with study as total
            if not base_where_clause:
                # No filters - count all samples with a path to a study (matches /sample/summary)
                # Use the same query structure as get_samples_summary - always include participant
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as total
                """.strip()
            else:
                # Has filters - need to apply them, but still count all samples that match
                # Filters that reference p (participant) will exclude samples without participants
                # This is expected behavior when filters are applied
                # Only include joins that are actually needed based on base filters
                needs_participant = any("p." in cond for cond in base_where_conditions)
                needs_study = any("st." in cond for cond in base_where_conditions)
                
                optional_matches = []
                if needs_participant:
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                if needs_study:
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                
                optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                
                # Need to include study paths and require st IS NOT NULL to match summary
                if not needs_study:
                    # Add study paths if not already included
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                    optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                    # Add st IS NOT NULL to WHERE clause
                    study_where = " AND coalesce(st1, st2) IS NOT NULL"
                    if base_where_clause:
                        total_where_clause = base_where_clause + study_where
                    else:
                        total_where_clause = "WHERE coalesce(st1, st2) IS NOT NULL"
                else:
                    # Study paths already included, just add st IS NOT NULL check
                    study_where = " AND st IS NOT NULL"
                    if base_where_clause:
                        total_where_clause = base_where_clause + study_where
                    else:
                        total_where_clause = "WHERE st IS NOT NULL"
                
                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                {total_where_clause}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
            
            # Missing: samples without participants OR samples with participants but sex_at_birth IS NULL
            # This includes:
            # 1. Samples without any participant relationship
            # 2. Samples with participants but NULL sex_at_birth
            if not base_where_clause:
                # No filters - count samples without participants or with NULL sex
                # IMPORTANT: Must match values query structure - only count samples WITH studies
                missing_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa, p
                WHERE p IS NULL OR p.sex_at_birth IS NULL
                RETURN count(DISTINCT sa) as missing
                """.strip()
            else:
                # Has filters - apply them, but also check for missing sex
                missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
                missing_where_conditions.append("(p IS NULL OR p.sex_at_birth IS NULL)")
                missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions) if missing_where_conditions else "WHERE (p IS NULL OR p.sex_at_birth IS NULL)"
                
                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause.startswith('WHERE ') else missing_where_clause}
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as missing
                """.strip()
            
            # Execute total and missing queries
            total_result = await self.session.run(total_cypher, params)
            total_records = []
            async for record in total_result:
                total_records.append(dict(record))
            total = total_records[0].get("total", 0) if total_records else 0
            
            missing_result = await self.session.run(missing_cypher, params)
            missing_records = []
            async for record in missing_result:
                missing_records.append(dict(record))
            missing = missing_records[0].get("missing", 0) if missing_records else 0
            
            # Verify: total should equal sum of values + missing
            values_sum = sum(item["count"] for item in counts)
            if total != values_sum + missing:
                logger.warning(
                    "Total count mismatch for sex field",
                    total=total,
                    values_sum=values_sum,
                    missing=missing,
                    difference=total - (values_sum + missing)
                )
        elif field == "anatomical_sites":
            # Total: all samples with a path to a study (matching /sample/summary - 50211 when no filters)
            if not base_where_clause:
                # No filters - count all samples with a path to a study (matches /sample/summary)
                # Use the same query structure as get_samples_summary - always include participant
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as total
                """.strip()
            else:
                # Has filters - need to include study paths and require st IS NOT NULL
                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else ''}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
            
            # Missing: samples with NULL or empty anatomical_sites, or all values are "Invalid value"
            # Handle both list and string cases without APOC
            # Build two queries: one for list, one for string
            if not base_where_clause:
                # No base filters - build queries for both list and string cases
                # IMPORTANT: Must match values query structure - only count samples WITH studies
                missing_cypher_list = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR size(sites) = 0
                   OR ALL(site IN sites WHERE site IS NULL OR toString(site) = '' OR toLower(trim(toString(site))) = 'invalid value')
                RETURN count(DISTINCT sa) as missing
                """.strip()
                
                missing_cypher_string = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR toString(sites) = ''
                   OR toLower(trim(toString(sites))) = 'invalid value'
                RETURN count(DISTINCT sa) as missing
                """.strip()
            else:
                # Has base filters - build queries for both list and string cases
                missing_cypher_list = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else 'WHERE st IS NOT NULL'}
                WITH DISTINCT sa, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR size(sites) = 0
                   OR ALL(site IN sites WHERE site IS NULL OR toString(site) = '' OR toLower(trim(toString(site))) = 'invalid value')
                RETURN count(DISTINCT sa) as missing
                """.strip()
                
                missing_cypher_string = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else 'WHERE st IS NOT NULL'}
                WITH DISTINCT sa, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR toString(sites) = ''
                   OR toLower(trim(toString(sites))) = 'invalid value'
                RETURN count(DISTINCT sa) as missing
                """.strip()
            
            # Store both queries as a tuple (will be handled in execution)
            missing_cypher = (missing_cypher_list, missing_cypher_string)
            
            # Execute total and missing queries
            total_result = await self.session.run(total_cypher, params)
            total_records = []
            async for record in total_result:
                total_records.append(dict(record))
            total = total_records[0].get("total", 0) if total_records else 0
            
            # For anatomical_sites, try list query first, fallback to string query if it fails
            missing = 0
            if isinstance(missing_cypher, tuple):
                missing_cypher_list, missing_cypher_string = missing_cypher
                try:
                    # Try list query first
                    missing_result = await self.session.run(missing_cypher_list, params)
                    missing_records = []
                    async for record in missing_result:
                        missing_records.append(dict(record))
                    missing = missing_records[0].get("missing", 0) if missing_records else 0
                    logger.debug("Successfully executed anatomical_sites missing query as list")
                except Exception as e:
                    error_msg = str(e).lower()
                    if "all" in error_msg and ("list" in error_msg or "string" in error_msg):
                        # It's a string, try the string query
                        logger.debug("List missing query failed, trying string query for anatomical_sites")
                        try:
                            missing_result = await self.session.run(missing_cypher_string, params)
                            missing_records = []
                            async for record in missing_result:
                                missing_records.append(dict(record))
                            missing = missing_records[0].get("missing", 0) if missing_records else 0
                            logger.debug("Successfully executed anatomical_sites missing query as string")
                        except Exception as e2:
                            logger.error(
                                "Error executing anatomical_sites missing query (both list and string failed)",
                                error=str(e2),
                                error_type=type(e2).__name__,
                                field=field,
                                exc_info=True
                            )
                            # Default to 0 if both fail
                            missing = 0
                    else:
                        logger.error(
                            "Error executing anatomical_sites missing query",
                            error=str(e),
                            error_type=type(e).__name__,
                            field=field,
                            exc_info=True
                        )
                        # Default to 0 on error
                        missing = 0
            else:
                # Regular missing query execution
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                missing = missing_records[0].get("missing", 0) if missing_records else 0
        else:
            # For other standard fields (vital_status, age_at_vital_status, or sample metadata fields)
            # Total: all samples with a path to a study (matching /sample/summary - 50211 when no filters)
            # For specimen_molecular_analyte_type, total should only count samples with sequencing_file nodes
            if field == "specimen_molecular_analyte_type" and not base_where_clause:
                # For specimen_molecular_analyte_type: count all samples matching to a study
                # This is the total - all samples with a study path (matching /sample/summary - 50211 when no filters)
                # Use the same query structure as other fields to ensure consistency
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as total
                """.strip()
            elif not base_where_clause:
                # No filters - count all samples with a path to a study (matches /sample/summary)
                # Use the same query structure as get_samples_summary - always include participant
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as total
                """.strip()
            else:
                # Has filters - need to apply them
                if is_sample_metadata_field:
                    # For specimen_molecular_analyte_type, use a different query structure:
                    # 1. First find samples with valid study
                    # 2. Then find sequencing_file associated with these samples
                    # 3. Count all samples (for total)
                    node_alias, _ = sample_metadata_field_mapping[field]
                    if field == "specimen_molecular_analyte_type":
                        # For specimen_molecular_analyte_type: count all samples matching to a study
                        # This is the total - all samples with a study path (matching /sample/summary - 50211 when no filters)
                        total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
                    else:
                        # For other sample metadata fields, use standard approach
                        optional_matches = []
                        if node_alias == "sf":
                            optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                        if node_alias == "pf":
                            optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                        if node_alias == "d":
                            optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                        if node_alias == "st":
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                        # Also include participant for filters that might reference it
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                        
                        # Always include study paths to ensure samples have a path to a study
                        # Check if study paths are already included
                        has_study_paths = any("st1" in match or "st2" in match for match in optional_matches)
                        if not has_study_paths:
                            # Add study paths
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                        
                        optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                        
                        # For fields on related nodes (sf, pf, d), we need to ensure the node exists
                        # and the sample has a path to a study
                        if node_alias in ["sf", "pf", "d"]:
                            # Require the related node to exist and sample to have a study path
                            study_where = " AND coalesce(st1, st2) IS NOT NULL"
                            node_where = f" AND {node_alias} IS NOT NULL"
                            if base_where_clause:
                                total_where_clause = base_where_clause + study_where + node_where
                            else:
                                total_where_clause = f"WHERE coalesce(st1, st2) IS NOT NULL{node_where}"
                        else:
                            # For fields on sample node or study node, just require study path
                            study_where = " AND coalesce(st1, st2) IS NOT NULL"
                            if base_where_clause:
                                total_where_clause = base_where_clause + study_where
                            else:
                                total_where_clause = "WHERE coalesce(st1, st2) IS NOT NULL"
                        
                        total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                {total_where_clause}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
                else:
                    # Participant fields - need to include study paths and require st IS NOT NULL
                    total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else ''}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
            
            # Missing: samples without the field value (NULL or missing relationship)
            if not base_where_clause:
                # No filters - count samples with NULL field
                if is_sample_metadata_field:
                    node_alias, _ = sample_metadata_field_mapping[field]
                    optional_matches = []
                    # Only add OPTIONAL MATCH if field is on a related node (not on sample node itself)
                    # If node_alias == "sa", no joins needed - field is directly on sample node
                    if node_alias == "sf":
                        optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                    if node_alias == "pf":
                        optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                    if node_alias == "d":
                        optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                    if node_alias == "st":
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                    # For fields on sample node (sa), we still need study paths to match summary
                    if node_alias == "sa":
                        # Add participant and study paths if not already included
                        if not any("p:participant" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                        if not any("st1:study" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                        if not any("st2:study" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                    
                    # For all related node fields (sf, pf, d, st), need to include study paths in missing query
                    # to match the values and total queries (only count samples WITH studies)
                    if node_alias in ["sf", "pf", "d", "st"]:
                        # Add study paths if not already included
                        if not any("st1" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                        if not any("st2" in match for match in optional_matches):
                            # Check if participant is already included
                            if not any("p:participant" in match for match in optional_matches):
                                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                    
                    # Build optional_matches_str AFTER adding study paths
                    optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                    
                    # For diagnosis fields (d.*), we need to check if ALL diagnoses have NULL/empty values
                    # For other fields, check if the field is NULL/empty
                    if node_alias == "d":
                        # Diagnosis fields: count as missing if sample has NO diagnoses with valid values
                        # i.e., all diagnoses have NULL/empty, OR no diagnoses exist
                        # IMPORTANT: Must match values query structure - only count samples WITH studies
                        # Need to ensure study paths are included in optional_matches
                        # Add study paths if not already included
                        if not any("st1:study" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                        if not any("st2:study" in match for match in optional_matches):
                            if not any("p:participant" in match for match in optional_matches):
                                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                        
                        optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                        
                        missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                WITH sa, d, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa, collect(DISTINCT {node_field}) as field_values
                WHERE size(field_values) = 0 
                   OR ALL(val IN field_values WHERE val IS NULL OR toString(val) = '' OR trim(toString(val)) = '' OR toString(val) = '-999' OR trim(toString(val)) = '-999')
                RETURN count(DISTINCT sa) as missing
                """.strip()
                    else:
                        # Non-diagnosis fields: check if field is NULL/empty or "-999"
                        optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                        
                        # Build WITH clause to include the node variable if needed
                        # For fields on sample node (sa), we still need study paths to match summary
                        if node_alias == "sa":
                            # Field is on sample node itself, but we need study paths for consistency with summary
                            with_clause = f"sa.sample_id as sample_id, p, coalesce(st1, st2) AS st, {node_field} as field_value"
                        else:
                            # Field is on a related node, need to include it in WITH clause
                            with_vars = ["sa.sample_id as sample_id"]
                            if node_alias == "sf":
                                with_vars.append("sf")
                            elif node_alias == "pf":
                                with_vars.append("pf")
                            elif node_alias == "d":
                                with_vars.append("d")
                            elif node_alias == "st":
                                with_vars.append("st")
                            with_vars.append(f"{node_field} as field_value")
                            
                            # For specimen_molecular_analyte_type, include study paths
                            if field == "specimen_molecular_analyte_type" and node_alias == "sf":
                                with_vars.append("coalesce(st1, st2) AS st")
                            
                            with_clause = ", ".join(with_vars)
                        
                        # For fields on sample node, also need to check st IS NOT NULL to match summary
                        # For specimen_molecular_analyte_type, use a different query structure:
                        # 1. First find samples with valid study
                        # 2. Then find sequencing_file associated with these samples
                        # 3. Count as missing if value is invalid/Not Reported (but NOT if sequencing_file is NULL, because those samples shouldn't be in the total)
                        if field == "specimen_molecular_analyte_type":
                            # Missing: samples with study path that either:
                            # 1. Don't have any sequencing_file, OR
                            # 2. Have sequencing_file(s) but all have null/invalid/Not Reported values
                            # Optimized: filter invalid values early, remove redundant toString/trim calls
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH DISTINCT sa, 
                     collect(DISTINCT sf.library_source_molecule) as molecule_values
                WHERE size([val IN molecule_values WHERE val IS NOT NULL 
                             AND val <> '' 
                             AND val <> '-999'
                             AND val <> 'Not Reported']) = 0
                RETURN count(DISTINCT sa) as missing
                """.strip()
                        elif node_alias == "sf" and not base_where_clause:
                            # Optimized missing count for other sequencing_file fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any sequencing_file, OR
                            # 2. Have sequencing_file(s) but all have null/invalid/Not Reported values
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH DISTINCT sa, 
                     collect(DISTINCT {node_field}) as field_values
                WHERE size([val IN field_values WHERE val IS NOT NULL 
                             AND val <> '' 
                             AND val <> '-999'
                             AND val <> 'Not Reported']) = 0
                RETURN count(DISTINCT sa) as missing
                """.strip()
                        elif node_alias == "pf" and not base_where_clause:
                            # Optimized missing count for pathology_file fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any pathology_file, OR
                            # 2. Have pathology_file(s) but all have null/invalid/Not Reported values
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
                WITH DISTINCT sa, 
                     collect(DISTINCT {node_field}) as field_values
                WHERE size([val IN field_values WHERE val IS NOT NULL 
                             AND val <> '' 
                             AND val <> '-999'
                             AND val <> 'Not Reported']) = 0
                RETURN count(DISTINCT sa) as missing
                """.strip()
                        elif node_alias == "d" and not base_where_clause:
                            # Optimized missing count for diagnosis fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any diagnosis, OR
                            # 2. Have diagnosis(es) but all have null/invalid/Not Reported values
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
                WITH DISTINCT sa, 
                     collect(DISTINCT {node_field}) as field_values
                WHERE size([val IN field_values WHERE val IS NOT NULL 
                             AND val <> '' 
                             AND val <> '-999'
                             AND val <> 'Not Reported']) = 0
                RETURN count(DISTINCT sa) as missing
                """.strip()
                        else:
                            # For other fields, build missing_where and missing_cypher
                            # IMPORTANT: All fields must check for study paths to match values and total queries
                            if node_alias == "sa" and "st" in with_clause:
                                # For fields on sample node, ensure st IS NOT NULL to match summary
                                missing_where = "st IS NOT NULL AND (field_value IS NULL OR toString(field_value) = '' OR trim(toString(field_value)) = '' OR toString(field_value) = '-999' OR trim(toString(field_value)) = '-999')"
                            else:
                                # For fields on related nodes, also need to check st IS NOT NULL
                                missing_where = "coalesce(st1, st2) IS NOT NULL AND (field_value IS NULL OR toString(field_value) = '' OR trim(toString(field_value)) = '' OR toString(field_value) = '-999' OR trim(toString(field_value)) = '-999')"
                            
                            # For fields on sample node (sa), ensure the query structure matches the total query
                            # Use the same pattern: WITH sa, p, coalesce(st1, st2) AS st, then filter by st IS NOT NULL
                            if node_alias == "sa":
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                WITH sa, p, coalesce(st1, st2) AS st, {node_field} as field_value
                WHERE st IS NOT NULL
                  AND (field_value IS NULL OR toString(field_value) = '' OR trim(toString(field_value)) = '' OR toString(field_value) = '-999' OR trim(toString(field_value)) = '-999')
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as missing
                """.strip()
                            else:
                                # For fields on related nodes, include study paths in WITH clause and check st IS NOT NULL
                                # Update with_clause to include study paths if not already included
                                # Check if study path is already in with_clause (from specimen_molecular_analyte_type special case)
                                if "coalesce(st1, st2) AS st" not in with_clause and "st AS" not in with_clause:
                                    # Add study path to with_clause
                                    with_clause_updated = f"{with_clause}, coalesce(st1, st2) AS st"
                                else:
                                    with_clause_updated = with_clause
                                
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                WITH {with_clause_updated}
                WHERE {missing_where}
                RETURN count(DISTINCT sample_id) as missing
                """.strip()
                else:
                    # Participant fields
                    missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                WITH DISTINCT sa, p, {node_field} as field_value
                WHERE p IS NULL 
                   OR field_value IS NULL 
                   OR toString(field_value) = '' 
                   OR trim(toString(field_value)) = ''
                   OR toString(field_value) = '-999'
                   OR trim(toString(field_value)) = '-999'
                RETURN count(DISTINCT sa) as missing
                """.strip()
            else:
                # Has filters - apply them, but also check for missing field
                if is_sample_metadata_field:
                    node_alias, _ = sample_metadata_field_mapping[field]
                    optional_matches = []
                    if node_alias == "sf":
                        optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                    if node_alias == "pf":
                        optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                    if node_alias == "d":
                        optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                    if node_alias == "st":
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                    optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                    
                    # For diagnosis fields (d.*), we need to check if ALL diagnoses have NULL/empty values
                    # For other fields, check if the field is NULL/empty
                    if node_alias == "d":
                        # Diagnosis fields: count as missing if sample has NO diagnoses with valid values
                        # i.e., all diagnoses have NULL/empty, OR no diagnoses exist
                        missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
                        missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions) if missing_where_conditions else ""
                        
                        missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                {missing_where_clause}
                WITH DISTINCT sa, collect(DISTINCT {node_field}) as field_values
                WHERE size(field_values) = 0 
                   OR ALL(val IN field_values WHERE val IS NULL OR toString(val) = '' OR trim(toString(val)) = '' OR toString(val) = '-999' OR trim(toString(val)) = '-999')
                RETURN count(DISTINCT sa) as missing
                """.strip()
                    else:
                        # Non-diagnosis fields: check if field is NULL/empty or "-999"
                        missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
                        missing_where_conditions.append(f"({node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '' OR toString({node_field}) = '-999' OR trim(toString({node_field})) = '-999')")
                        missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions) if missing_where_conditions else f"WHERE ({node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '')"
                        
                        # Build WITH clause to include the node variable if needed
                        # For missing count with filters, we need to include the node variable in WITH before applying WHERE
                        # Also need to ensure study paths are included
                        needs_participant = any("p." in cond for cond in base_where_conditions)
                        needs_study = any("st." in cond for cond in base_where_conditions)
                        
                        # Build WITH clause - always include sa, then add related nodes
                        with_vars = ["sa"]
                        if node_alias == "sf":
                            with_vars.append("sf")
                        elif node_alias == "pf":
                            with_vars.append("pf")
                        elif node_alias == "d":
                            with_vars.append("d")
                        # Include participant if needed for filters
                        if needs_participant:
                            with_vars.append("p")
                        # Include study - need to handle both paths
                        if node_alias == "st":
                            # st was already matched in optional_matches
                            with_vars.append("st")
                        else:
                            # Need to add study paths if not already included
                            if not needs_study:
                                # Add both study paths for coalesce
                                with_vars.append("coalesce(st1, st2) AS st")
                            else:
                                # st was already matched, but we need both paths for coalesce
                                with_vars.append("coalesce(st, st1) AS st")
                        
                        with_clause = f"WITH {', '.join(with_vars)}\n                " if with_vars else ""
                        
                        missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                {with_clause}{missing_where_clause}
                RETURN count(DISTINCT sa.sample_id) as missing
                """.strip()
                else:
                    # Participant fields
                    missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
                    # Count as missing: no participant OR NULL or empty string or "-999"
                    missing_where_conditions.append(f"(p IS NULL OR {node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '' OR toString({node_field}) = '-999' OR trim(toString({node_field})) = '-999')")
                    missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions) if missing_where_conditions else f"WHERE (p IS NULL OR {node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '' OR toString({node_field}) = '-999' OR trim(toString({node_field})) = '-999')"
                    
                    missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, coalesce(st1, st2) AS st
                WHERE st IS NOT NULL
                {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause.startswith('WHERE ') else missing_where_clause}
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as missing
                """.strip()
            
            # Execute total and missing queries
            total_result = await self.session.run(total_cypher, params)
            total_records = []
            async for record in total_result:
                total_records.append(dict(record))
            total = total_records[0].get("total", 0) if total_records else 0
            
            missing_result = await self.session.run(missing_cypher, params)
            missing_records = []
            async for record in missing_result:
                missing_records.append(dict(record))
            missing = missing_records[0].get("missing", 0) if missing_records else 0
        
        logger.debug(
            "Completed sample count by field",
            field=field,
            results_count=len(counts),
            total=total,
            missing=missing
        )
        
        return {
            "total": total,
            "missing": missing,
            "values": counts  # counts already has format [{"value": ..., "count": ...}]
        }
    
    async def _count_samples_by_race(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct samples by race.
        
        For race values like "Asian;White", the sample is counted
        for both "Asian" and "White".
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of race counts)
        """
        logger.debug("Counting samples by race with enum validation", filters=filters)
        
        # Get all valid race enum values
        valid_races = Race.values()
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                if isinstance(identifiers_value, list):
                    where_conditions.append(f"p.participant_id IN ${id_param}")
                else:
                    where_conditions.append(f"p.participant_id = ${id_param}")
        
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
            
            db_field = "sex_at_birth" if filter_field == "sex" else filter_field
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique samples matching filters
        if not where_clause:
            total_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            RETURN count(DISTINCT sa.sample_id) as total
            """.strip()
        else:
            total_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            {where_clause}
            RETURN count(DISTINCT sa.sample_id) as total
            """.strip()
        
        # Query 2: Get count of samples with null race value (missing)
        if not where_clause:
            missing_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            WITH DISTINCT sa, p
            WHERE p IS NULL OR p.race IS NULL
            RETURN count(DISTINCT sa) as missing
            """.strip()
        else:
            missing_where_conditions = where_conditions.copy()
            missing_where_conditions.append("(p IS NULL OR p.race IS NULL)")
            missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)
            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause.startswith('WHERE ') else missing_where_clause}
            WITH DISTINCT sa
            RETURN count(DISTINCT sa) as missing
            """.strip()
        
        # Query 3: Create a single query that counts distinct samples for each valid race
        params["valid_races"] = valid_races
        
        if not where_clause:
            values_cypher = f"""
            MATCH (sa:sample)-[:of_sample]->(p:participant)
            WHERE p.race IS NOT NULL
            WITH DISTINCT sa.sample_id as sample_id, 
                 head(collect(DISTINCT p.race)) as race
            WITH sample_id, race,
                 [r IN SPLIT(race, ';') | trim(r)] as race_parts
            WITH sample_id, race, race_parts,
                 [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
            WITH sample_id, race, race_list_filtered,
                 CASE 
                   WHEN size(race_list_filtered) = 0 THEN ['Not Reported']
                   ELSE [r IN race_list_filtered WHERE r IN $valid_races]
                 END as matching_races
            UNWIND matching_races as race_value
            RETURN race_value as value, count(DISTINCT sample_id) as count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            values_cypher = f"""
            MATCH (sa:sample)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            {where_clause}
            WITH DISTINCT sa.sample_id as sample_id, 
                 head(collect(DISTINCT p.race)) as race
            WHERE race IS NOT NULL
            WITH sample_id, race,
                 [r IN SPLIT(race, ';') | trim(r)] as race_parts
            WITH sample_id, race, race_parts,
                 [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
            WITH sample_id, race, race_list_filtered,
                 CASE 
                   WHEN size(race_list_filtered) = 0 THEN ['Not Reported']
                   ELSE [r IN race_list_filtered WHERE r IN $valid_races]
                 END as matching_races
            UNWIND matching_races as race_value
            RETURN race_value as value, count(DISTINCT sample_id) as count
            ORDER BY count DESC, value ASC
            """.strip()
        
        logger.info(
            "Executing count_samples_by_race Cypher queries",
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
                    logger.debug(f"Retrying count_samples_by_race query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_samples_by_race query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_samples_by_race query after retries", error=str(e), exc_info=True)
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
            "Completed sample count by race",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def _count_samples_by_ethnicity(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct samples by ethnicity (derived from race field).
        
        Ethnicity is determined from race:
        - If race contains 'Hispanic or Latino' → 'Hispanic or Latino'
        - Otherwise → 'Not reported'
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list with only 2 ethnicity options)
        """
        logger.debug("Counting samples by ethnicity (derived from race)", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                if isinstance(identifiers_value, list):
                    where_conditions.append(f"p.participant_id IN ${id_param}")
                else:
                    where_conditions.append(f"p.participant_id = ${id_param}")
        
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
            
            db_field = "sex_at_birth" if filter_field == "sex" else filter_field
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique samples matching filters
        if not where_clause:
            total_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            RETURN count(DISTINCT sa.sample_id) as total
            """.strip()
        else:
            total_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            {where_clause}
            RETURN count(DISTINCT sa.sample_id) as total
            """.strip()
        
        # Query 2: Get count of samples with null race (missing ethnicity)
        if not where_clause:
            missing_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            WITH DISTINCT sa, p
            WHERE p IS NULL OR p.race IS NULL
            RETURN count(DISTINCT sa) as missing
            """.strip()
        else:
            missing_where_conditions = where_conditions.copy()
            missing_where_conditions.append("(p IS NULL OR p.race IS NULL)")
            missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)
            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause.startswith('WHERE ') else missing_where_clause}
            WITH DISTINCT sa
            RETURN count(DISTINCT sa) as missing
            """.strip()
        
        # Query 3: Count by ethnicity (derived from race)
        if not where_clause:
            values_cypher = """
            MATCH (sa:sample)-[:of_sample]->(p:participant)
            WHERE p.race IS NOT NULL
            WITH DISTINCT sa.sample_id as sample_id, 
                 head(collect(DISTINCT p.race)) as race
            WITH sample_id, race,
                 CASE 
                   WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
                   ELSE 'Not reported'
                 END as ethnicity_value
            RETURN ethnicity_value as value, count(DISTINCT sample_id) as count
            ORDER BY value ASC
            """.strip()
        else:
            values_cypher = f"""
            MATCH (sa:sample)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            {where_clause}
            WITH DISTINCT sa.sample_id as sample_id, 
                 head(collect(DISTINCT p.race)) as race
            WHERE race IS NOT NULL
            WITH sample_id, race,
                 CASE 
                   WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
                   ELSE 'Not reported'
                 END as ethnicity_value
            RETURN ethnicity_value as value, count(DISTINCT sample_id) as count
            ORDER BY value ASC
            """.strip()
        
        logger.info(
            "Executing count_samples_by_ethnicity Cypher queries",
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
                    logger.debug(f"Retrying count_samples_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_samples_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_samples_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results
        counts = []
        for record in values_records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })
        
        logger.info(
            "Completed sample count by ethnicity",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def _count_samples_by_associated_diagnoses(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct samples by associated diagnoses.
        
        For samples with multiple diagnoses, the sample is counted
        for each diagnosis they have.
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of diagnosis counts)
        """
        logger.debug("Counting samples by associated diagnoses", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                if isinstance(identifiers_value, list):
                    where_conditions.append(f"p.participant_id IN ${id_param}")
                else:
                    where_conditions.append(f"p.participant_id = ${id_param}")
        
        # Handle diagnosis search - we skip this for diagnosis counting since we're counting by diagnosis
        if "_diagnosis_search" in filters:
            filters.pop("_diagnosis_search")  # Remove it to avoid circular filtering
        
        # Add regular filters (excluding associated_diagnoses since we're counting by it)
        for filter_field, value in filters.items():
            if filter_field == "associated_diagnoses":
                continue  # Skip diagnosis filter when counting by diagnosis
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            db_field = "sex_at_birth" if filter_field == "sex" else filter_field
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique samples matching filters
        # Use the same query structure as get_samples_summary - require study path
        if not where_clause:
            total_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            WITH DISTINCT sa
            RETURN count(DISTINCT sa) as total
            """.strip()
        else:
            total_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            WITH DISTINCT sa
            RETURN count(DISTINCT sa) as total
            """.strip()
        
        # Query 2: Get count of samples with no valid diagnoses (missing)
        # Only count samples with a study path (matching summary endpoint)
        # Missing = samples with no diagnoses OR all diagnoses are invalid
        if not where_clause:
            missing_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH DISTINCT sa, collect(d) as diagnoses
            WITH sa,
                 head([d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ]) as diagnosis_value
            WHERE diagnosis_value IS NULL
               OR toString(diagnosis_value) = ''
               OR trim(toString(diagnosis_value)) = ''
            RETURN count(DISTINCT sa) as missing
            """.strip()
        else:
            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH DISTINCT sa, collect(d) as diagnoses
            WITH sa,
                 head([d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ]) as diagnosis_value
            WHERE diagnosis_value IS NULL
               OR toString(diagnosis_value) = ''
               OR trim(toString(diagnosis_value)) = ''
            RETURN count(DISTINCT sa) as missing
            """.strip()
        
        # Query 3: Count by diagnosis values
        # d.diagnosis is a STRING (not a list) - each diagnosis node has one diagnosis value
        # Multiple diagnosis nodes can link to one sample, so each contributes one value
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(sa:sample)
        # If diagnosis is "see diagnosis_comment", use diagnosis_comment as the value
        # Filter out "see diagnosis_comment" if diagnosis_comment is NULL or empty
        if not where_clause:
            values_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH DISTINCT sa, collect(d) as diagnoses
            WITH sa, 
                 head([d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ]) as diagnosis_value
            WHERE diagnosis_value IS NOT NULL 
              AND toString(diagnosis_value) <> '' 
              AND trim(toString(diagnosis_value)) <> ''
            RETURN toString(diagnosis_value) as value, count(DISTINCT sa) as count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            values_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, p, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH DISTINCT sa, collect(d) as diagnoses
            WITH sa, 
                 head([d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ]) as diagnosis_value
            WHERE diagnosis_value IS NOT NULL 
              AND toString(diagnosis_value) <> '' 
              AND trim(toString(diagnosis_value)) <> ''
            RETURN toString(diagnosis_value) as value, count(DISTINCT sa) as count
            ORDER BY count DESC, value ASC
            """.strip()
        
        logger.info(
            "Executing count_samples_by_associated_diagnoses Cypher queries",
            params_count=len(params),
            values_query=values_cypher
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
                    logger.debug(f"Retrying count_samples_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_samples_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_samples_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results and filter out any "see diagnosis_comment" that might have slipped through
        counts = []
        for record in values_records:
            value = record.get("value")
            # Additional safety check: filter out "see diagnosis_comment" if it somehow appears
            if value and "see diagnosis_comment" in str(value).lower():
                logger.warning(
                    "Filtering out 'see diagnosis_comment' from results",
                    value=value,
                    count=record.get("count", 0)
                )
                continue
            counts.append({
                "value": value,
                "count": record.get("count", 0)
            })
        
        # Sort by count descending, then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        logger.info(
            "Completed sample count by associated diagnoses",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }
    
    async def get_samples_summary(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for samples.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting samples summary", filters=filters)
        
        # If no filters, use simple optimized query (matches the structure used in count queries)
        # Check if filters dict is empty or only contains None values
        has_real_filters = any(v is not None and v != "" for v in filters.values()) if filters else False
        
        if not has_real_filters:
            cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, p, coalesce(st1, st2) AS st
        WHERE st IS NOT NULL
        WITH DISTINCT sa
        RETURN count(DISTINCT sa) as total_count
        """.strip()
            
            result = await self.session.run(cypher, {})
            records = []
            async for record in result:
                records.append(dict(record))
            await result.consume()
            
            total_count = records[0].get("total_count", 0) if records else 0
            
            return {
                "counts": {
                    "total": total_count
                }
            }
        
        # Build filter conditions similar to get_samples
        params = {}
        param_counter = 0
        where_conditions = []
        
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
                where_conditions.append("sa.sample_id IN id_list")
        
        with_conditions = []
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            with_conditions.append("""diagnoses IS NOT NULL AND toLower(toString(diagnoses.diagnosis)) CONTAINS toLower($diagnosis_search_term)""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters - map to correct nodes based on field
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Map fields to their source nodes (same as get_samples)
            if field == "disease_phase":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
            elif field == "anatomical_sites":
                # anatomical_sites can be either a list or a string
                # Store both filter conditions - will be handled in query execution
                # We'll try list version first, fallback to string if it fails
                with_conditions.append(("anatomical_sites_list", param_name))
                with_conditions.append(("anatomical_sites_string", param_name))
            elif field == "library_selection_method":
                # Apply reverse mapping for filtering (API value -> DB value)
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                with_conditions.append(f"sf IS NOT NULL AND sf.library_selection = ${param_name}")
            elif field == "library_strategy":
                # Apply reverse mapping for filtering (API value -> DB value)
                # For "Other", we need to match both "Archer Fusion" (reverse mapped) and "Other" (direct match)
                db_value = reverse_map_field_value("library_strategy", value)
                if db_value is None:
                    # If no reverse mapping, use the value as-is (for values not in mapping)
                    params[param_name] = value
                    with_conditions.append(f"sf IS NOT NULL AND sf.library_strategy = ${param_name}")
                else:
                    # We have a reverse mapping - need to match both the mapped value and the original value
                    mapped_db_value = db_value if isinstance(db_value, str) else (db_value[0] if isinstance(db_value, list) and db_value else value)
                    # Match either the reverse-mapped value OR the original value (in case DB already has "Other")
                    param_counter += 1
                    param_name_original = f"param_{param_counter}"
                    params[param_name] = mapped_db_value
                    params[param_name_original] = value
                    with_conditions.append(f"sf IS NOT NULL AND (sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name_original})")
            elif field == "specimen_molecular_analyte_type":
                # Apply reverse mapping for filtering (API value -> DB value(s))
                # "RNA" can map to both "Transcriptomic" and "Viral RNA" in DB
                # Special handling: need to check if ANY sequencing_file matches, not just the first one
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values map to this API value - store as special condition
                    # Will be handled after collecting all sequencing_files
                    with_conditions.append(("specimen_molecular_analyte_type_list", reverse_mapped))
                elif reverse_mapped is None:
                    # Filter for null or "Not Reported"
                    with_conditions.append(f"(sf IS NULL OR sf.library_source_molecule IS NULL OR sf.library_source_molecule = 'Not Reported')")
                else:
                    params[param_name] = reverse_mapped
                    # Store as special condition - will be handled after collecting all sequencing_files
                    with_conditions.append(("specimen_molecular_analyte_type_single", param_name))
            elif field == "disease_phase":
                # Apply reverse mapping for filtering (API value -> DB value(s))
                # "Relapse" can map to both "Recurrent Disease" and "Relapse" in DB
                reverse_mapped = reverse_map_field_value("disease_phase", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values map to this API value - use OR condition
                    db_values_list = [f"'{v}'" for v in reverse_mapped]
                    db_values_str = ", ".join(db_values_list)
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase IN [{db_values_str}]")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
            elif field == "library_source_material":
                # Apply reverse mapping for filtering (API value -> DB value)
                # "Other" maps to null, so filter for null values
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                if reverse_mapped is None:
                    # Filter for null or "Other" values
                    with_conditions.append(f"(sf IS NULL OR sf.library_source_material IS NULL OR sf.library_source_material = 'Other')")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"sf IS NOT NULL AND sf.library_source_material = ${param_name}")
            elif field == "preservation_method":
                with_conditions.append(f"pf IS NOT NULL AND pf.fixation_embedding_method = ${param_name}")
            elif field == "tumor_classification":
                # Apply reverse mapping for filtering (API value -> DB value)
                # "non-malignant" maps to null, so filter for null values
                reverse_mapped = reverse_map_field_value("tumor_classification", value)
                if reverse_mapped is None:
                    # Filter for null or "non-malignant" values
                    with_conditions.append(f"(diagnoses IS NULL OR diagnoses.tumor_classification IS NULL OR diagnoses.tumor_classification = 'non-malignant')")
                else:
                    params[param_name] = reverse_mapped
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_classification = ${param_name}")
            elif field == "tumor_grade":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_grade = ${param_name}")
            elif field == "age_at_diagnosis":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.age_at_diagnosis = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "age_at_collection":
                with_conditions.append(f"sa.participant_age_at_collection = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "depositions":
                with_conditions.append(f"st IS NOT NULL AND st.study_id = ${param_name}")
            elif field == "diagnosis":
                # Check both diagnoses.diagnosis and diagnoses.diagnosis_comment (for "see diagnosis_comment" cases)
                # If diagnosis is "see diagnosis_comment", check the comment field instead
                diagnosis_condition = (
                    f"(diagnoses IS NOT NULL AND "
                    f"(diagnoses.diagnosis = ${param_name} OR "
                    f"(toLower(trim(toString(diagnoses.diagnosis))) = 'see diagnosis_comment' AND "
                    f"diagnoses.diagnosis_comment IS NOT NULL AND "
                    f"trim(toString(diagnoses.diagnosis_comment)) = ${param_name})))"
                )
                with_conditions.append(diagnosis_condition)
            else:
                # Default to sample node
                if isinstance(value, list):
                    with_conditions.append(f"sa.{field} IN ${param_name}")
                else:
                    with_conditions.append(f"sa.{field} = ${param_name}")
            
            if field not in ["disease_phase", "tumor_grade", "age_at_diagnosis", "age_at_collection", "diagnosis", "anatomical_sites"]:
                # For non-diagnosis fields, handle list values
                # Skip anatomical_sites as it's handled specially with tuples
                if isinstance(value, list):
                    # Only replace if the last condition is a string (not a tuple)
                    if with_conditions and isinstance(with_conditions[-1], str):
                        with_conditions[-1] = with_conditions[-1].replace(f"= ${param_name}", f"IN ${param_name}")
            
            # Only set param if not already set (age fields set it above)
            if param_name not in params:
                params[param_name] = value
        
        # Build WHERE clause - handle anatomical_sites and specimen_molecular_analyte_type filters specially
        anatomical_sites_param = None
        anatomical_sites_list_condition = None
        anatomical_sites_string_condition = None
        specimen_molecular_analyte_type_list = None
        specimen_molecular_analyte_type_single_param = None
        regular_conditions = []
        
        for condition in with_conditions:
            if isinstance(condition, tuple) and condition[0] == "anatomical_sites_list":
                anatomical_sites_param = condition[1]
                # Check if the parameter value is a list (multiple values from || delimiter)
                param_value = params.get(condition[1])
                if isinstance(param_value, list):
                    # Multiple values - check if ANY of them match (OR logic)
                    # Build OR conditions for each value
                    or_conditions = []
                    for idx, val in enumerate(param_value):
                        val_param = f"{condition[1]}_{idx}"
                        params[val_param] = val
                        or_conditions.append(f"""(
                            ${val_param} = sa.anatomic_site OR
                            reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                              CASE WHEN trim(tok) = trim(toString(${val_param})) THEN true ELSE found END
                            ) = true
                        )""")
                    anatomical_sites_list_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
                else:
                    # Single value - handle both exact match and semicolon-separated string cases
                    anatomical_sites_list_condition = f"""sa.anatomic_site IS NOT NULL AND (
                        ${condition[1]} = sa.anatomic_site OR
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${condition[1]})) THEN true ELSE found END
                        ) = true
                    )"""
            elif isinstance(condition, tuple) and condition[0] == "anatomical_sites_string":
                # String version: handle exact match and semicolon-separated strings
                anatomical_sites_string_condition = f"""sa.anatomic_site IS NOT NULL AND (
                    trim(toString(sa.anatomic_site)) = trim(toString(${condition[1]})) OR
                    reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                      CASE WHEN trim(tok) = trim(toString(${condition[1]})) THEN true ELSE found END
                    ) = true
                )"""
            elif isinstance(condition, tuple) and condition[0] == "specimen_molecular_analyte_type_list":
                # Store the list of DB values that map to the API value (e.g., ["Transcriptomic", "Viral RNA"] for "RNA")
                specimen_molecular_analyte_type_list = condition[1]
            elif isinstance(condition, tuple) and condition[0] == "specimen_molecular_analyte_type_single":
                # Store the parameter name for single value mapping
                specimen_molecular_analyte_type_single_param = condition[1]
            else:
                regular_conditions.append(condition)
        
        # Build WHERE clause - use list version for anatomical_sites if present
        all_conditions = regular_conditions.copy()
        if anatomical_sites_list_condition:
            # Use list version first (will try string version if it fails)
            all_conditions.append(anatomical_sites_list_condition)
        # Add specimen_molecular_analyte_type condition if present (will be checked after collecting all sequencing_files)
        if specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param:
            all_conditions.append("has_matching_sf = true")
        
        where_clause = ""
        if all_conditions:
            where_clause = "WHERE " + " AND ".join(all_conditions)
        
        # Determine which OPTIONAL MATCH clauses are needed based on filters
        needs_participant = any(
            field in filters for field in ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status"]
        ) or any("p." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_diagnosis = any(
            field in filters for field in ["disease_phase", "tumor_grade", "age_at_diagnosis", "diagnosis"]
        ) or any("d." in str(cond) or "diagnoses" in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_pathology_file = any(
            field in filters for field in ["preservation_method"]
        ) or any("pf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_sequencing_file = any(
            field in filters for field in ["library_selection_method", "library_strategy", "library_source_material", "specimen_molecular_analyte_type"]
        ) or any("sf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_study = any(
            field in filters for field in ["depositions"]
        ) or any("st." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        # Build OPTIONAL MATCH clauses
        optional_matches = []
        # Need participant if filtering by participant fields OR if we need study paths
        if needs_participant or needs_study:
            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
        if needs_diagnosis:
            optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        if needs_pathology_file:
            optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
        if needs_sequencing_file:
            optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
        
        # Check if where_clause references st (e.g., for depositions filter)
        needs_st_for_where = any("st." in str(cond) or "st IS NOT NULL" in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        # Study paths - ALWAYS include to ensure samples have a path to a study
        # Path 1: sample -> cell_line -> study
        # Path 2: sample -> participant -> consent_group -> study
        study_paths = []
        # Need participant for path2
        if not needs_participant:
            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
        study_paths.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
        study_paths.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
        
        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        study_paths_str = "\n        ".join(study_paths) if study_paths else ""
        
        # Build WITH clause - only include variables that were matched
        with_vars = ["sa"]
        with_collects = []
        if needs_participant or needs_study or needs_st_for_where:
            with_vars.append("p")
        if needs_diagnosis:
            with_vars.append("head(collect(DISTINCT d)) AS diagnoses")
        if needs_pathology_file:
            with_collects.append("head(collect(DISTINCT pf)) AS pf")
        # For specimen_molecular_analyte_type, collect ALL sequencing_files first to check if ANY match
        if needs_sequencing_file:
            if specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param:
                with_collects.append("collect(DISTINCT sf) AS all_sfs")  # Collect all sequencing_files
            else:
                with_collects.append("head(collect(DISTINCT sf)) AS sf")
        
        # Always include study (coalesce both paths)
        with_vars.append("coalesce(st1, st2) AS st")
        
        with_clause = ", ".join(with_vars)
        if with_collects:
            with_clause += ",\n             " + ",\n             ".join(with_collects)
        
        # Add identifiers_condition to WITH clause if present
        if identifiers_condition:
            with_clause += identifiers_condition
        
        # For specimen_molecular_analyte_type, we need a second WITH clause to use all_sfs
        # (can't reference a variable in the same WITH clause where it's defined)
        second_with_clause = None
        if specimen_molecular_analyte_type_list:
            # Build list of DB values for IN clause
            db_values_str = ", ".join([f"'{v}'" for v in specimen_molecular_analyte_type_list])
            # Build second_with_vars based on what's available from first WITH
            second_with_vars = ["sa", "st"]
            if needs_participant or needs_study or needs_st_for_where:
                second_with_vars.append("p")
            if needs_diagnosis:
                second_with_vars.append("diagnoses")
            if needs_pathology_file:
                second_with_vars.append("pf")
            second_with_vars.append(f"size([sf IN all_sfs WHERE sf IS NOT NULL AND sf.library_source_molecule IN [{db_values_str}]]) > 0 AS has_matching_sf")
            second_with_vars.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
            second_with_clause = ", ".join(second_with_vars)
        elif specimen_molecular_analyte_type_single_param:
            # Single value mapping
            # Build second_with_vars based on what's available from first WITH
            second_with_vars = ["sa", "st"]
            if needs_participant or needs_study or needs_st_for_where:
                second_with_vars.append("p")
            if needs_diagnosis:
                second_with_vars.append("diagnoses")
            if needs_pathology_file:
                second_with_vars.append("pf")
            second_with_vars.append(f"size([sf IN all_sfs WHERE sf IS NOT NULL AND sf.library_source_molecule = ${specimen_molecular_analyte_type_single_param}]) > 0 AS has_matching_sf")
            second_with_vars.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
            second_with_clause = ", ".join(second_with_vars)
        
        # Build WHERE clause - include filter conditions AND ensure sample has path to study
        # Separate conditions that need to be in first WITH (for identifiers) from those that need to be after second WITH (for has_matching_sf)
        first_where_conditions = ["st IS NOT NULL"]
        second_where_conditions = []
        
        # Parse where_clause to separate conditions with has_matching_sf from others
        if where_clause:
            # Extract conditions from where_clause (remove "WHERE " prefix)
            where_conditions_str = where_clause.replace("WHERE ", "").strip()
            if where_conditions_str:
                # Split by " AND " to get individual conditions
                conditions = [c.strip() for c in where_conditions_str.split(" AND ")]
                for condition in conditions:
                    if "has_matching_sf" in condition:
                        second_where_conditions.append(condition)
                    else:
                        first_where_conditions.append(condition)
        
        # If identifiers are present, integrate first WHERE clause into WITH clause
        # This ensures id_list is available when the WHERE condition is evaluated
        # But we can't include has_matching_sf here since it's not defined yet
        where_in_with = False
        if identifiers_condition:
            # Integrate first WHERE clause into WITH clause (without has_matching_sf)
            where_conditions_str = " AND ".join(first_where_conditions)
            if where_conditions_str:
                with_clause += f"\n        WHERE {where_conditions_str}"
                where_in_with = True
            first_where_clause = ""
        else:
            # No identifiers - apply first WHERE clause separately
            first_where_clause = "\n        WHERE " + " AND ".join(first_where_conditions) if first_where_conditions else ""
        
        # Build final WHERE clause (includes has_matching_sf if present)
        final_where_clause = "\n        WHERE " + " AND ".join(second_where_conditions) if second_where_conditions else ""
        
        # If we have a second WITH clause (for specimen_molecular_analyte_type), include it
        second_with_str = ""
        if second_with_clause:
            # Apply WHERE clause after second WITH if present
            # If where_in_with is True, first_where_clause is empty, so only use final_where_clause
            # Otherwise, combine both into a single WHERE clause
            if where_in_with:
                # First WHERE is already in WITH clause, only add final_where_clause (has_matching_sf)
                if final_where_clause.strip():
                    second_with_str = f"WITH {second_with_clause}\n        {final_where_clause.strip()}\n        "
                else:
                    second_with_str = f"WITH {second_with_clause}\n        "
            else:
                # Combine first_where_clause and final_where_clause into a single WHERE clause
                # Extract conditions from both (remove "WHERE " prefix and combine with AND)
                first_conditions = []
                if first_where_clause.strip():
                    first_where_str = first_where_clause.replace("WHERE", "").strip()
                    if first_where_str:
                        first_conditions.append(first_where_str)
                
                second_conditions = []
                if final_where_clause.strip():
                    second_where_str = final_where_clause.replace("WHERE", "").strip()
                    if second_where_str:
                        second_conditions.append(second_where_str)
                
                # Combine all conditions into a single WHERE clause
                all_combined_conditions = first_conditions + second_conditions
                if all_combined_conditions:
                    combined_where_str = " AND ".join(all_combined_conditions)
                    second_with_str = f"WITH {second_with_clause}\n        WHERE {combined_where_str}\n        "
                else:
                    second_with_str = f"WITH {second_with_clause}\n        "
            final_where_clause = ""  # Clear it since it's now in second_with_str
        elif first_where_clause or final_where_clause:
            # Apply WHERE clause after first WITH if no second WITH
            combined_where = (first_where_clause + final_where_clause).strip()
            final_where_clause = f"{combined_where}\n        " if combined_where else ""
        else:
            final_where_clause = ""
        
        # Build the query - ensure proper spacing
        if second_with_str:
            # second_with_str already includes the WITH and WHERE clauses
            cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        {optional_matches_str}
        // Try multiple paths to get study - collect all possible studies
        {study_paths_str}
        WITH {with_clause}
        {second_with_str}WITH DISTINCT sa.sample_id AS sample_id
        RETURN count(DISTINCT sample_id) as total_count
        """.strip()
        else:
            # No second WITH, use final_where_clause after first WITH
            cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        {optional_matches_str}
        // Try multiple paths to get study - collect all possible studies
        {study_paths_str}
        WITH {with_clause}
        {final_where_clause}WITH DISTINCT sa.sample_id AS sample_id
        RETURN count(DISTINCT sample_id) as total_count
        """.strip()
        
        logger.info(
            "Executing get_samples_summary Cypher query",
            cypher=cypher,
            params=params,
            identifiers_condition=identifiers_condition if identifiers_condition else None,
            where_conditions=where_conditions,
            final_where_conditions=final_where_conditions if 'final_where_conditions' in locals() else None,
            with_clause=with_clause
        )
        
        # Execute query with proper result consumption and retry logic
        # For anatomical_sites, try list version first, fallback to string if it fails
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
                    logger.debug(f"Retrying get_samples_summary query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_samples_summary query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    # Re-raise to be handled by outer try-except
                    raise
        
        try:
            logger.debug(
                "Query executed successfully",
                records_count=len(records),
                cypher=cypher[:200] if cypher else None
            )
            
            if not records:
                logger.warning("No records returned from summary query")
                return {"total_count": 0}
            
            summary = records[0]
            total_count = summary.get("total_count", 0)
            logger.info("Completed samples summary", total_count=total_count)
            
            return {"total_count": total_count}
        except Exception as e:
            error_msg = str(e).lower()
            # If anatomical_sites filter is present and we got an IN error, try string version
            if anatomical_sites_param and anatomical_sites_string_condition and ("in expected a list" in error_msg or "in expected" in error_msg):
                logger.debug("List query failed for anatomical_sites in summary, trying string version")
                # Rebuild query with string version
                all_conditions = regular_conditions.copy()
                all_conditions.append(anatomical_sites_string_condition)  # Use string version
                
                # Build WHERE clause - include filter conditions AND ensure sample has path to study
                final_where_conditions = ["st IS NOT NULL"]
                if all_conditions:
                    final_where_conditions.extend(all_conditions)
                where_clause = "WHERE " + " AND ".join(final_where_conditions)
                
                cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
                OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                // Try multiple paths to get study
                // Path 1: sample -> cell_line -> study
                // Path 2: sample -> participant -> consent_group -> study
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, p, 
                     coalesce(st1, st2) AS st,
                     head(collect(DISTINCT sf)) AS sf,
                     head(collect(DISTINCT pf)) AS pf,
                     head(collect(DISTINCT d)) AS diagnoses
                {where_clause}
                WITH DISTINCT sa.sample_id AS sample_id
                RETURN count(DISTINCT sample_id) as total_count
                """.strip()
                
                try:
                    result = await self.session.run(cypher, params)
                    async for record in result:
                        records.append(dict(record))
                    logger.debug("Successfully executed anatomical_sites summary query as string")
                    
                    if not records:
                        logger.warning("No records returned from summary query")
                        return {"total_count": 0}
                    
                    summary = records[0]
                    total_count = summary.get("total_count", 0)
                    logger.info("Completed samples summary (string version)", total_count=total_count)
                    
                    return {"total_count": total_count}
                except Exception as e2:
                    logger.error(
                        "Error executing get_samples_summary Cypher query (both list and string failed for anatomical_sites)",
                        error=str(e2),
                        error_type=type(e2).__name__,
                        cypher=cypher[:500],
                        exc_info=True
                    )
                    raise
            else:
                # Different error, re-raise
                logger.error(
                    "Error executing get_samples_summary Cypher query",
                    error=str(e),
                    error_type=type(e).__name__,
                    cypher=cypher[:500] if cypher else None,
                    params_keys=list(params.keys()) if params else [],
                    exc_info=True
                )
                raise
    
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
    
    def _record_to_sample(
        self, 
        sa: Dict[str, Any], 
        p: Dict[str, Any], 
        st: Dict[str, Any], 
        sf: Dict[str, Any], 
        pf: Dict[str, Any], 
        diagnoses: Optional[Dict[str, Any]],
        base_url: Optional[str] = None
    ) -> Sample:
        """
        Convert database records to a Sample object with proper field mappings.
        
        Args:
            sa: Sample node dictionary
            p: Participant node dictionary
            st: Study node dictionary
            sf: Sequencing file node dictionary
            pf: Pathology file node dictionary
            diagnoses: Single diagnosis node dictionary (or None)
            
        Returns:
            Sample object with proper structure
        """
        from app.models.dto import (
            Sample, SampleIdentifier, NamespaceIdentifier, SubjectId,
            SampleMetadata, DiagnosisField
        )
        
        # Build sample ID: namespace from study, name from sample_id
        # Handle case where sa might be empty or None
        if not sa:
            logger.warning("Sample node (sa) is empty or None, skipping record")
            raise ValueError("Sample node (sa) is required but was empty or None")
        
        # Try to get study_id from multiple sources
        study_id = ""
        if st and isinstance(st, dict):
            study_id = st.get("study_id", "")
        
        # If study_id is still empty, try to get it from the sample node itself
        if not study_id and sa and isinstance(sa, dict):
            study_id = sa.get("study_id", "")
        
        # If still empty, try to get it from participant
        if not study_id and p and isinstance(p, dict):
            study_id = p.get("study_id", "")
        
        sample_id = sa.get("sample_id", "") if isinstance(sa, dict) else ""
        
        # If sample_id is not available, try other possible fields
        if not sample_id:
            sample_id = sa.get("id", "") if isinstance(sa, dict) else ""
        if not sample_id:
            sample_id = sa.get("name", "") if isinstance(sa, dict) else ""
        
        # Validate required fields - both study_id and sample_id are required
        if not study_id or not study_id.strip():
            logger.warning(
                "Sample record missing study_id (namespace), skipping",
                sa_keys=list(sa.keys()) if isinstance(sa, dict) else [],
                st_keys=list(st.keys()) if st and isinstance(st, dict) else [],
                p_keys=list(p.keys()) if p and isinstance(p, dict) else [],
                sample_id=sample_id
            )
            raise ValueError(f"Sample record missing required study_id (namespace). Sample ID: {sample_id}")
        
        if not sample_id or not sample_id.strip():
            logger.warning(
                "Sample node missing sample_id field, skipping",
                sa_keys=list(sa.keys()) if isinstance(sa, dict) else [],
                study_id=study_id
            )
            raise ValueError(f"Sample record missing required sample_id. Study ID: {study_id}")
        
        # Create namespace and sample identifier
        # Pass as dictionary to avoid Pydantic validation issues
        sample_identifier = SampleIdentifier(
            namespace={
                "organization": "CCDI-DCC",
                "name": study_id.strip()
            },
            name=sample_id.strip()
        )
        
        # Build subject reference: name from participant, namespace from study
        subject = None
        if p and isinstance(p, dict) and st and isinstance(st, dict):
            participant_id = p.get("participant_id", "")
            if not participant_id:
                participant_id = p.get("id", "") if isinstance(p, dict) else ""
            if participant_id:
                subject_namespace = NamespaceIdentifier(
                    organization="CCDI-DCC",
                    name=study_id
                )
                subject = SubjectId(
                    namespace=subject_namespace,
                    name=participant_id
                )
        
        # Build metadata with proper field mappings
        def _null_if_invalid(value):
            """Replace 'Invalid value' with None, and -999 with None."""
            if value is None:
                return None
            if isinstance(value, (int, float)) and value == -999:
                return None
            if isinstance(value, str) and value.strip().lower() == "invalid value":
                return None
            # Handle arrays/lists - filter out "Invalid value" entries
            if isinstance(value, (list, tuple)):
                filtered = [v for v in value if v is not None and str(v).strip().lower() != "invalid value"]
                # Return None if all values were invalid, otherwise return the filtered list
                return filtered if filtered else None
            return value
        
        def _null_if_neg999(value):
            """Replace -999 with None, return value as-is (not converted to string)."""
            if value is None:
                return None
            if isinstance(value, (int, float)) and value == -999:
                return None
            return value  # Return as-is, not as string
        
        # Get depositions from study - format as objects with kind and value
        depositions = None
        if st and isinstance(st, dict) and st.get("study_id"):
            study_id = st.get("study_id")
            if study_id:
                depositions = [{"kind": "dbGaP", "value": study_id}]
        
        # Build diagnosis field
        # If empty data, return null; otherwise return {value: diagnosis, comment: diagnosis_comment}
        diagnosis_field = None
        if diagnoses and isinstance(diagnoses, dict):
            # diagnoses is now a single node (or None)
            diagnosis_value = diagnoses.get("diagnosis")
            diagnosis_comment = diagnoses.get("diagnosis_comment")
            
            # Check if diagnosis_value is empty/null/whitespace
            if diagnosis_value and str(diagnosis_value).strip():
                # Has diagnosis value - return object with value and comment
                diagnosis_field = DiagnosisField(
                    value=str(diagnosis_value).strip(),
                    comment=str(diagnosis_comment).strip() if diagnosis_comment and str(diagnosis_comment).strip() else None
                )
            # If diagnosis_value is empty/null/whitespace, diagnosis_field remains None (returns null)
        
        # Helper function to wrap value in ValueField if not None and not empty
        def _wrap_value(value):
            """Wrap value in ValueField if not None and not empty, otherwise return None."""
            if value is None:
                return None
            # Convert to string and check if it's empty or just whitespace
            str_value = str(value).strip()
            if not str_value:
                return None
            from app.models.dto import ValueField
            return ValueField(value=str_value)
        
        def _wrap_list_value(value_list):
            """Wrap list of values in list of ValueField objects if not None and not empty, otherwise return None."""
            if value_list is None or not isinstance(value_list, list) or len(value_list) == 0:
                return None
            from app.models.dto import ValueField
            # Filter out empty strings and create ValueField for each valid value
            wrapped = [ValueField(value=str(v).strip()) for v in value_list if v is not None and str(v).strip()]
            return wrapped if wrapped else None
        
        def _map_library_selection_method(db_value):
            """Map database value to API value for library_selection_method.
            
            Uses centralized field mappings from config_data/field_mappings.json.
            """
            return map_field_value("library_selection_method", db_value)
        
        def _reverse_map_library_selection_method(api_value):
            """Reverse map API value to database value for library_selection_method.
            
            Used for filtering - maps API values back to DB values.
            Uses centralized field mappings from config_data/field_mappings.json.
            """
            result = reverse_map_field_value("library_selection_method", api_value)
            # reverse_map_field_value can return a list, but for library_selection_method it should be a string
            return result if isinstance(result, str) else (result[0] if isinstance(result, list) and result else None)
        
        
        # Helper function to wrap integer value in IntegerValueField if not None
        def _wrap_integer_value(value):
            """Wrap integer value in IntegerValueField if not None, otherwise return None."""
            if value is None:
                return None
            # Convert to int, handling both int and float values
            try:
                int_value = int(float(value))  # Convert float to int (e.g., 10.0 -> 10)
                from app.models.dto import IntegerValueField
                return IntegerValueField(value=int_value)
            except (ValueError, TypeError):
                return None
        
        # Helper function to handle anatomical_sites (may be array or string)
        def _process_anatomical_sites(value):
            """Process anatomical_sites - handle arrays and strings, return list of all valid values."""
            if value is None:
                return None
            result = []
            # If it's an array/list, process each value
            if isinstance(value, (list, tuple)):
                for v in value:
                    if v is not None and str(v).strip() != "" and str(v).strip().lower() != "invalid value":
                        result.append(str(v).strip())
            # If it's a string, check if it's semicolon-separated or a single value
            elif isinstance(value, str):
                value_stripped = value.strip()
                if value_stripped and value_stripped.lower() != "invalid value":
                    # Check if it contains semicolons (semicolon-separated values)
                    if ';' in value_stripped:
                        # Split by semicolon and process each part
                        parts = value_stripped.split(';')
                        for part in parts:
                            part_stripped = part.strip()
                            if part_stripped and part_stripped.lower() != "invalid value":
                                result.append(part_stripped)
                    else:
                        # Single value
                        result.append(value_stripped)
            else:
                # For other types, convert to string and check
                value_str = str(value).strip() if value else ""
                if value_str and value_str.lower() != "invalid value":
                    result.append(value_str)
            # Return None if no valid values, otherwise return the list
            return result if result else None
        
        # Build identifiers - reference the subject (participant)
        identifiers = None
        # Ensure we have study_id from st if not already set
        if not study_id and st and isinstance(st, dict):
            study_id = st.get("study_id", "")
        
        if p and isinstance(p, dict) and study_id and sample_id:
            participant_id = p.get("participant_id", "")
            if not participant_id:
                participant_id = p.get("id", "") if isinstance(p, dict) else ""
            
            if participant_id and study_id and sample_id:
                from app.models.dto import IdentifierField, IdentifierValue
                
                # Build server URL - format: /api/v1/sample/CCDI-DCC/{study_id}/{sample_id}
                # Note: This format doesn't include entity type, matching user's example
                server_url = None
                if base_url:
                    server_url = f"{base_url}/api/v1/sample/CCDI-DCC/{study_id}/{sample_id}"
                
                identifier_value = IdentifierValue(
                    namespace={
                        "organization": "CCDI-DCC",
                        "name": study_id
                    },
                    name=sample_id,
                    type="Linked",
                    server=server_url
                )
                identifiers = [IdentifierField(value=identifier_value)]
            else:
                logger.debug(
                    "Cannot build identifier - missing participant_id, study_id, or sample_id",
                    has_participant_id=bool(participant_id),
                    has_study_id=bool(study_id),
                    has_sample_id=bool(sample_id),
                    p_keys=list(p.keys()) if isinstance(p, dict) else []
                )
        else:
            logger.debug(
                "Cannot build identifier - missing participant, study, or sample_id",
                has_p=bool(p),
                p_is_dict=isinstance(p, dict) if p else False,
                has_st=bool(st),
                study_id=study_id,
                sample_id=sample_id
            )
        
        # Build metadata with updated field mappings
        # disease_phase: d.disease_phase (from diagnoses, not sa)
        disease_phase_value = None
        if diagnoses and isinstance(diagnoses, dict):
            disease_phase_value = diagnoses.get("disease_phase")
        
        # anatomical_sites: sa.anatomic_site - "Invalid value" to be replaced with null (already handled in _process_anatomical_sites)
        anatomical_sites_value = sa.get("anatomic_site") if sa else None
        
        # library_selection_method: sf.library_selection
        library_selection_value = sf.get("library_selection") if sf else None
        
        # library_strategy: sf.library_strategy
        library_strategy_value = sf.get("library_strategy") if sf else None
        
        # library_source_material: sf.library_source_material
        library_source_material_value = sf.get("library_source_material") if sf else None
        
        # specimen_molecular_analyte_type: sf.library_source_molecule
        specimen_molecular_analyte_type_value = sf.get("library_source_molecule") if sf else None
        
        # preservation_method: pf.fixation_embedding_method
        preservation_method_value = pf.get("fixation_embedding_method") if pf else None
        
        # tumor_grade: d.tumor_grade
        tumor_grade_value = None
        if diagnoses and isinstance(diagnoses, dict):
            tumor_grade_value = diagnoses.get("tumor_grade")
        
        # age_at_diagnosis: d.age_at_diagnosis or null if -999
        age_at_diagnosis_value = None
        if diagnoses and isinstance(diagnoses, dict):
            age_at_diagnosis_value = diagnoses.get("age_at_diagnosis")
        
        # age_at_collection: sa.participant_age_at_collection or null if -999
        age_at_collection_value = sa.get("participant_age_at_collection") if sa else None
        
        # tumor_classification: d.tumor_classification
        tumor_classification_value = None
        if diagnoses and isinstance(diagnoses, dict):
            tumor_classification_value = diagnoses.get("tumor_classification")
        
        # Build metadata with field mappings applied
        metadata = SampleMetadata(
            disease_phase=_wrap_value(map_field_value("disease_phase", _null_if_invalid(disease_phase_value))),
            anatomical_sites=_wrap_list_value(_process_anatomical_sites(anatomical_sites_value)),
            library_selection_method=_wrap_value(_map_library_selection_method(_null_if_invalid(library_selection_value))),
            library_strategy=_wrap_value(map_field_value("library_strategy", _null_if_invalid(library_strategy_value))),
            library_source_material=_wrap_value(map_field_value("library_source_material", _null_if_invalid(library_source_material_value))),
            preservation_method=_wrap_value(_null_if_invalid(preservation_method_value)),
            tumor_grade=_wrap_value(_null_if_invalid(tumor_grade_value)),
            specimen_molecular_analyte_type=_wrap_value(map_field_value("specimen_molecular_analyte_type", _null_if_invalid(specimen_molecular_analyte_type_value))),
            tissue_type=None,  # Not in the provided mapping
            tumor_classification=_wrap_value(map_field_value("tumor_classification", _null_if_invalid(tumor_classification_value))),
            age_at_diagnosis=_wrap_integer_value(_null_if_neg999(age_at_diagnosis_value)),
            age_at_collection=_wrap_integer_value(_null_if_neg999(age_at_collection_value)),
            tumor_tissue_morphology=None,  # Not in the provided mapping
            depositions=depositions,
            diagnosis=diagnosis_field,
            identifiers=identifiers
        )
        
        # Create Sample object
        sample = Sample(
            id=sample_identifier,
            subject=subject,
            metadata=metadata
        )
        
        return sample
