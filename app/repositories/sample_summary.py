"""
Summary methods for SampleRepository.

This module contains methods for getting sample summary statistics.
"""

import asyncio
from typing import Dict, Any, Optional
from app.core.logging import get_logger
from app.core.field_mappings import (
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
    map_field_value
)
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)


class SampleSummary:
    """Mixin class providing summary methods for SampleRepository."""
    
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
        
        # IMPORTANT: Check routing BEFORE popping identifiers (identifiers is popped later for early filtering)
        # This ensures routing decisions include identifiers filter
        original_filters_keys = set(filters.keys())
        
        # PERFORMANCE OPTIMIZATION: Use reverse query for sequencing_file-only filters
        sequencing_file_filter_keys = {"library_selection_method", "library_strategy", "library_source_material", "specimen_molecular_analyte_type"}
        has_sf_filters = any(k in original_filters_keys for k in sequencing_file_filter_keys)
        # Check for other filters BEFORE identifiers is popped
        # identifiers, depositions, and anatomical_sites are sample-level filters that can be combined with sequencing_file filters
        has_other_filters = any(k not in sequencing_file_filter_keys for k in original_filters_keys)
        
        if has_sf_filters and not has_other_filters:
            logger.debug("Using optimized reverse query for summary with sequencing_file-only filters")
            return await self._get_samples_summary_reverse_query(filters)
        
        # OPTIMIZATION: Specialized summary query for diagnosis search-only filters
        has_diagnosis_search = "_diagnosis_search" in filters
        allowed_with_diagnosis_search = {"identifiers", "depositions", "_diagnosis_search"}
        diagnosis_search_only_summary = (
            has_diagnosis_search and
            all(k in allowed_with_diagnosis_search for k in original_filters_keys)
        )
        
        if diagnosis_search_only_summary:
            logger.debug("Using optimized summary query for diagnosis search-only filters")
            return await self._get_samples_summary_diagnosis_search(filters)
        
        # If no filters, use simple optimized query (matches the structure used in count queries)
        # Check if filters dict is empty or only contains None values
        has_real_filters = any(v is not None and v != "" for v in filters.values()) if filters else False
        
        if not has_real_filters:
            # Use multi-hop traversal for consistency with main query
            # Use sample_id + study_id as unique identifier (same sample_id can be in different studies)
            cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa,
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        WITH sa, sid
        WHERE sid IS NOT NULL
        WITH DISTINCT sa.sample_id AS sample_id, sid AS study_id
        RETURN count(*) as total_count
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
        
        # Track depositions early params separately to avoid parameter conflicts
        self._depositions_early_params = []
        
        # Handle identifiers parameter normalization
        # Support || separator for OR logic (e.g., "SAMP001 || SAMP002")
        # OPTIMIZATION: Apply identifiers filter EARLY in MATCH WHERE clause to reduce dataset before OPTIONAL MATCHes
        identifiers_condition = ""
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                # Parse || separator and create list
                if isinstance(identifiers_value, str) and "||" in identifiers_value:
                    identifiers_list = [i.strip() for i in identifiers_value.split("||")]
                    identifiers_list = [i for i in identifiers_list if i]
                    identifiers_value = identifiers_list if identifiers_list else None
                
                if identifiers_value:
                    param_counter += 1
                    id_param = f"param_{param_counter}"
                    params[id_param] = identifiers_value
                    
                    # Build early filter for MATCH WHERE clause (before OPTIONAL MATCHes)
                    # This significantly reduces the dataset before expensive OPTIONAL MATCH operations
                    if isinstance(identifiers_value, list):
                        # Multiple identifiers - use IN clause directly
                        identifiers_early_filter = f"sa.sample_id IN ${id_param}"
                    else:
                        # Single identifier - use = for better performance
                        identifiers_early_filter = f"sa.sample_id = ${id_param}"
                    
                    # Also keep identifiers_condition for WITH clause (needed for id_list normalization in some cases)
                    identifiers_condition = f""",
                    // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                    CASE
                      WHEN ${id_param} IS NULL THEN NULL
                      WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                      WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                      ELSE []
                    END AS id_list"""
                    # Don't add to where_conditions - it's now applied early in MATCH WHERE clause
        
        with_conditions = []
        
        # Handle diagnosis search - need to check if ANY diagnosis in the sample matches
        diagnosis_search_term = None
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            # OPTIMIZATION 4A: Pre-process search term to lowercase (done once in Python)
            # This removes toLower() calls from Cypher, improving performance
            diagnosis_search_term_lower = diagnosis_search_term.lower().strip()
            params["diagnosis_search_term"] = diagnosis_search_term  # Keep original for potential use
            params["diagnosis_search_term_lower"] = diagnosis_search_term_lower  # Pre-processed lowercase
            params["diagnosis_search_term_see_comment"] = "see diagnosis_comment"  # Pre-computed constant
            # This will be applied as a condition after collecting ALL diagnosis nodes
        
        # Add regular filters - map to correct nodes based on field
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Map fields to their source nodes (same as get_samples)
            if field == "anatomical_sites":
                # anatomical_sites can be either a list or a string
                # Store both filter conditions - will be handled in query execution
                # We'll try list version first, fallback to string if it fails
                with_conditions.append(("anatomical_sites_list", param_name))
                with_conditions.append(("anatomical_sites_string", param_name))
            elif field == "library_selection_method":
                # Check if value is a database-only value (e.g., "PolyA", "Not Applicable")
                if is_database_only_value("library_selection_method", value):
                    # This value is a database-only value and is not valid for filtering
                    with_conditions.append(("library_selection_method_invalid", "invalid"))
                else:
                    # Apply reverse mapping for filtering (API value -> DB value)
                    # Need to check if ANY sequencing_file matches, not just the first one
                    db_value = self._reverse_map_library_selection_method_static(value)
                    params[param_name] = db_value
                    with_conditions.append(("library_selection_method", param_name))
            elif field == "library_strategy":
                # Check if value is a database-only value (e.g., "Archer Fusion")
                if is_database_only_value("library_strategy", value):
                    # This value is a database-only value and is not valid for filtering
                    with_conditions.append(("library_strategy_invalid", "invalid"))
                else:
                    # Apply reverse mapping for filtering (API value -> DB value)
                    # For "Other", we need to match both "Archer Fusion" (reverse mapped) and "Other" (direct match)
                    # Need to check if ANY sequencing_file matches, not just the first one
                    db_value = reverse_map_field_value("library_strategy", value)
                    if db_value is None:
                        # If no reverse mapping, use the value as-is (for values not in mapping)
                        params[param_name] = value
                        with_conditions.append(("library_strategy", param_name))
                    else:
                        # We have a reverse mapping - need to match both the mapped value and the original value
                        mapped_db_value = db_value if isinstance(db_value, str) else (db_value[0] if isinstance(db_value, list) and db_value else value)
                        # Match either the reverse-mapped value OR the original value (in case DB already has "Other")
                        param_counter += 1
                        param_name_original = f"param_{param_counter}"
                        params[param_name] = mapped_db_value
                        params[param_name_original] = value
                        with_conditions.append(("library_strategy", param_name, param_name_original))
            elif field == "specimen_molecular_analyte_type":
                # Check if value is a database-only value (e.g., "Transcriptomic", "Genomic", "Viral RNA")
                # or in null_mappings (e.g., "Not Reported")
                if is_database_only_value("specimen_molecular_analyte_type", value) or is_null_mapped_value("specimen_molecular_analyte_type", value):
                    # This value is not valid for filtering
                    with_conditions.append(("specimen_molecular_analyte_type_invalid", "invalid"))
                else:
                    # Apply reverse mapping for filtering (API value -> DB value(s))
                    # "RNA" can map to both "Transcriptomic" and "Viral RNA" in DB
                    # Special handling: need to check if ANY sequencing_file matches, not just the first one
                    reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                    if isinstance(reverse_mapped, list):
                        # Multiple DB values map to this API value - store as special condition
                        # Will be handled after collecting all sequencing_files
                        with_conditions.append(("specimen_molecular_analyte_type_list", reverse_mapped))
                    else:
                        # If reverse_mapped is None, use the original value (no mapping needed)
                        params[param_name] = reverse_mapped if reverse_mapped else value
                        # Store as special condition - will be handled after collecting all sequencing_files
                        with_conditions.append(("specimen_molecular_analyte_type_single", param_name))
            elif field == "disease_phase":
                # Check if value is a database-only value (e.g., "Recurrent Disease")
                if is_database_only_value("disease_phase", value):
                    # This value is not valid for filtering
                    # Add an impossible condition to return empty results
                    with_conditions.append("false")
                elif is_null_mapped_value("disease_phase", value):
                    # "Not Reported" is a valid filter value - match database values case-sensitively
                    # The value is stored in DB as-is, so match it directly
                    params[param_name] = value
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
                else:
                    # Apply reverse mapping for filtering (API value -> DB value(s))
                    # "Relapse" can map to both "Recurrent Disease" and "Relapse" in DB
                    reverse_mapped = reverse_map_field_value("disease_phase", value)
                    if isinstance(reverse_mapped, list):
                        # Multiple DB values map to this API value - use IN clause with parameter for better query planning
                        params[param_name] = reverse_mapped
                        with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase IN ${param_name}")
                    else:
                        params[param_name] = reverse_mapped
                        with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
            elif field == "library_source_material":
                # Use helper function to validate library_source_material filter
                # Note: Returns None if invalid (null_mapped), but we don't need to return [] here
                # because the invalid case is handled via the tuple ("library_source_material_invalid", "invalid")
                # which is processed later in the query building logic
                self._validate_library_source_material_filter(value, param_name, params, with_conditions)
            elif field == "preservation_method":
                params[param_name] = value
                with_conditions.append(("preservation_method", param_name))
            elif field == "tissue_type":
                # Use helper function to validate tissue_type filter
                if self._validate_tissue_type_filter(value, param_name, params, with_conditions) is None:
                    # Return empty summary dict instead of empty list
                    return {"counts": {"total": 0}}
            elif field == "tumor_classification":
                # Check if value is in null_mappings (e.g., "non-malignant")
                if is_null_mapped_value("tumor_classification", value):
                    # This value is treated as NULL/missing and is not valid for filtering
                    with_conditions.append("false")
                else:
                    # Apply reverse mapping for filtering (API value -> DB value)
                    reverse_mapped = reverse_map_field_value("tumor_classification", value)
                    # If reverse_mapped is None, use the original value (no mapping needed)
                    params[param_name] = reverse_mapped if reverse_mapped else value
                    with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_classification = ${param_name}")
            elif field == "tumor_grade":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_grade = ${param_name}")
            elif field == "tumor_tissue_morphology":
                with_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_tissue_morphology = ${param_name}")
            elif field == "age_at_diagnosis":
                with_conditions.append(f"diagnoses IS NOT NULL AND toInteger(diagnoses.age_at_diagnosis) = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "age_at_collection":
                with_conditions.append(f"toInteger(sa.participant_age_at_collection) = ${param_name}")
                # Convert value to number for numeric comparison
                try:
                    params[param_name] = int(value) if value is not None else None
                except (ValueError, TypeError):
                    params[param_name] = value
            elif field == "depositions":
                # OPTIMIZATION: depositions filter will be applied early in MATCH WHERE clause
                # Store the parameter for early filtering (don't add to with_conditions)
                # Support || separator for multi-value OR logic
                if isinstance(value, str) and "||" in value:
                    dep_list = [d.strip() for d in value.split("||")]
                    dep_list = [d for d in dep_list if d]
                    if len(dep_list) > 1:
                        params[param_name] = dep_list
                    elif len(dep_list) == 1:
                        params[param_name] = dep_list[0]
                    else:
                        # Empty list after filtering, skip
                        continue
                else:
                    # Single value (no || separator)
                    params[param_name] = value
                # Store depositions param name for early filtering (will be added to early_where_conditions later)
                if not hasattr(self, '_depositions_early_params'):
                    self._depositions_early_params = []
                self._depositions_early_params.append(param_name)
                # Don't add to with_conditions - it's now applied early in MATCH WHERE clause
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
            
            if field not in ["disease_phase", "tumor_grade", "tumor_tissue_morphology", "tumor_classification", "age_at_diagnosis", "age_at_collection", "diagnosis", "anatomical_sites", "tissue_type"]:
                # For non-diagnosis fields, handle list values
                # Skip anatomical_sites as it's handled specially with tuples
                # Skip tissue_type as it's already handled with proper validation and IN clause if needed
                if isinstance(value, list):
                    # Only replace if the last condition is a string (not a tuple)
                    if with_conditions and isinstance(with_conditions[-1], str):
                        with_conditions[-1] = with_conditions[-1].replace(f"= ${param_name}", f"IN ${param_name}")
            
            # Only set param if not already set (age fields set it above)
            if param_name not in params:
                params[param_name] = value
        
        # Build WHERE clause - handle anatomical_sites, specimen_molecular_analyte_type, and sequencing_file field filters specially
        anatomical_sites_param = None
        anatomical_sites_list_condition = None
        anatomical_sites_string_condition = None
        specimen_molecular_analyte_type_list = None
        specimen_molecular_analyte_type_single_param = None
        library_selection_method_param = None
        library_strategy_param = None
        library_source_material_param = None
        preservation_method_param = None
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
            elif isinstance(condition, tuple) and condition[0] == "specimen_molecular_analyte_type_invalid":
                # Invalid value (database-only or null-mapped value) - set impossible condition
                specimen_molecular_analyte_type_single_param = "invalid"
            elif isinstance(condition, tuple) and condition[0] == "library_selection_method":
                # Store the parameter name - will be checked after collecting all sequencing_files
                library_selection_method_param = condition[1]
            elif isinstance(condition, tuple) and condition[0] == "library_selection_method_invalid":
                # Invalid value (database-only value) - set impossible condition
                library_selection_method_param = "invalid"
            elif isinstance(condition, tuple) and condition[0] == "library_strategy":
                # Store the parameter name(s) - will be checked after collecting all sequencing_files
                if len(condition) == 3:
                    # Has both mapped and original values
                    library_strategy_param = (condition[1], condition[2])
                else:
                    # Single value
                    library_strategy_param = condition[1]
            elif isinstance(condition, tuple) and condition[0] == "library_strategy_invalid":
                # Invalid value (database-only value) - set impossible condition
                library_strategy_param = "invalid"
            elif isinstance(condition, tuple) and condition[0] == "library_source_material":
                # Store the parameter name - will be checked after collecting all sequencing_files
                library_source_material_param = condition[1]
            elif isinstance(condition, tuple) and condition[0] == "library_source_material_invalid":
                # Invalid value (in null_mappings) - set impossible condition to return empty results
                library_source_material_param = "invalid"
            elif isinstance(condition, tuple) and condition[0] == "preservation_method":
                preservation_method_param = condition[1]
                # Don't add to regular_conditions - will be applied in OPTIONAL MATCH WHERE clause (early filter optimization)
            else:
                regular_conditions.append(condition)
        
        # PERFORMANCE FIX: Early return for invalid filter values
        # If any filter has an invalid value (e.g., "Other" for library_source_material),
        # return empty results immediately without hitting the database
        if (specimen_molecular_analyte_type_single_param == "invalid" or
            library_selection_method_param == "invalid" or
            library_strategy_param == "invalid" or
            library_source_material_param == "invalid"):
            logger.info(
                "Invalid filter value detected in summary query - returning empty results",
                filters=filters,
                specimen_molecular_analyte_type_invalid=specimen_molecular_analyte_type_single_param == "invalid",
                library_selection_method_invalid=library_selection_method_param == "invalid",
                library_strategy_invalid=library_strategy_param == "invalid",
                library_source_material_invalid=library_source_material_param == "invalid"
            )
            return {"counts": {"total": 0}}
        
        # Build early WHERE conditions (applied before OPTIONAL MATCHes)
        # Separate cheap filters (can be applied before OPTIONAL MATCHes) from expensive ones
        early_where_conditions = [
            "sa.sample_id IS NOT NULL",
            "toString(sa.sample_id) <> ''"
        ]
        
        # OPTIMIZATION: Add identifiers filter early (before OPTIONAL MATCHes)
        # This significantly reduces the dataset before expensive joins
        if identifiers_early_filter:
            early_where_conditions.append(identifiers_early_filter)
        
        # OPTIMIZATION: Add anatomical_sites filter early (sample property, can be filtered early)
        if anatomical_sites_list_condition:
            early_where_conditions.append(anatomical_sites_list_condition)
            # Remove from regular_conditions since it's now in early_where_conditions
            if anatomical_sites_list_condition in regular_conditions:
                regular_conditions.remove(anatomical_sites_list_condition)
        elif anatomical_sites_string_condition:
            early_where_conditions.append(anatomical_sites_string_condition)
            # Remove from regular_conditions since it's now in early_where_conditions
            if anatomical_sites_string_condition in regular_conditions:
                regular_conditions.remove(anatomical_sites_string_condition)
        
        # OPTIMIZATION: Add tissue_type filter early (sample property, can be filtered early)
        # Extract tissue_type condition from regular_conditions and move to early_where_conditions
        tissue_type_condition = None
        for condition in regular_conditions[:]:  # Use slice to avoid modification during iteration
            if isinstance(condition, str) and "sa.sample_tumor_status" in condition:
                tissue_type_condition = condition
                early_where_conditions.append(condition)
                regular_conditions.remove(condition)
                break
        
        # Build early WHERE clause from early_where_conditions
        early_where_clause = "\n        WHERE " + " AND ".join(early_where_conditions) if early_where_conditions else ""
        
        # Determine if we need to collect sequencing_files or diagnoses (needed for diagnosis_only_summary check)
        needs_sf_collection = (specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param or
                              library_selection_method_param is not None or
                              library_strategy_param is not None or
                              library_source_material_param is not None)
        # Also need to collect all diagnoses if disease_phase or other diagnosis filters are present
        # Note: "diagnosis" filter is handled via optimized query path when alone, but we still need
        # to set needs_diag_collection correctly for the standard query path fallback
        has_diagnosis_filters = any(
            field in filters for field in ["disease_phase", "tumor_grade", "tumor_tissue_morphology", "tumor_classification", "age_at_diagnosis", "diagnosis"]
        )
        needs_diag_collection = diagnosis_search_term is not None or has_diagnosis_filters
        
        # Depositions filter: must be applied AFTER we have st (study node). See get_samples() for same logic.
        depositions_study_filter_summary = ""
        if hasattr(self, '_depositions_early_params') and self._depositions_early_params:
            all_dep_values = []
            for dep_param_name in self._depositions_early_params:
                dep_value = params.get(dep_param_name)
                if dep_value:
                    if isinstance(dep_value, list):
                        all_dep_values.extend(dep_value)
                    else:
                        all_dep_values.append(dep_value)
            if all_dep_values:
                dep_early_param_name = self._get_next_param_name(params, param_counter)
                if len(all_dep_values) > 1:
                    params[dep_early_param_name] = all_dep_values
                    depositions_study_filter_summary = f" AND st.study_id IN ${dep_early_param_name}"
                else:
                    params[dep_early_param_name] = all_dep_values[0]
                    depositions_study_filter_summary = f" AND st.study_id = ${dep_early_param_name}"
        
        # OPTIMIZATION: Specialized query for diagnosis filter summary (exact match)
        # When ONLY diagnosis filter is present (or with identifiers/depositions), use optimized query structure
        has_diagnosis_filter_summary = "diagnosis" in filters
        allowed_with_diagnosis_summary = {"identifiers", "depositions", "diagnosis"}
        diagnosis_only_summary = (
            has_diagnosis_filter_summary and
            not needs_sf_collection and
            not needs_diag_collection and
            not anatomical_sites_list_condition and
            all(k in allowed_with_diagnosis_summary for k in filters.keys())
        )
        
        if diagnosis_only_summary:
            # Extract diagnosis filter parameter and value
            diagnosis_param_summary = None
            diagnosis_value_summary = filters.get("diagnosis")
            
            # Find the param name for diagnosis from with_conditions
            import re
            for cond in with_conditions:
                if isinstance(cond, str) and "diagnoses.diagnosis" in cond and "diagnoses.diagnosis_comment" in cond:
                    # Extract param name from condition
                    match = re.search(r'\$param_(\d+)', cond)
                    if match:
                        diagnosis_param_summary = f"param_{match.group(1)}"
                        break
            
            if diagnosis_param_summary and diagnosis_value_summary:
                # Build optimized summary query using same structure as main query
                depositions_sid_filter_summary = depositions_study_filter_summary.replace("st.study_id", "sid") if depositions_study_filter_summary else ""
                identifiers_where_summary = f" AND {identifiers_early_filter}" if identifiers_early_filter else ""
                
                cypher_summary_optimized = f"""MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''{identifiers_where_summary}

// collect study ids from both paths
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_list

OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list

// combine and drop nulls; unwind to ensure sample matches a study (one row per pair)
WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids
UNWIND combined_ids AS sid
MATCH (st:study {{study_id: sid}})
WHERE sid IS NOT NULL{depositions_sid_filter_summary}

// collect ALL diagnoses for the sample (diagnoses matched before counting)
OPTIONAL MATCH (sa)<-[:of_diagnosis]-(d:diagnosis)
WITH sa, st, collect(DISTINCT d) AS diagnoses

// require that SOME diagnosis matches the filter (case-sensitive exact match)
WHERE size([
      dx IN diagnoses
      WHERE dx IS NOT NULL AND (
        trim(toString(dx.diagnosis)) = trim(toString(${diagnosis_param_summary}))
        OR (
          toLower(trim(toString(dx.diagnosis))) = 'see diagnosis_comment'
          AND dx.diagnosis_comment IS NOT NULL
          AND trim(toString(dx.diagnosis_comment)) = trim(toString(${diagnosis_param_summary}))
        )
      )
    ]) > 0

// count distinct (sample_id, study_id) pairs
WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
RETURN count(*) AS total_count
""".strip()
                
                logger.info(
                    "Using optimized diagnosis filter query for summary",
                    pattern="diagnosis_filter_summary_optimized",
                    filters=list(filters.keys()),
                )
                
                try:
                    result = await self.session.run(cypher_summary_optimized, params)
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
                except Exception as e:
                    logger.warning(
                        "Diagnosis-optimized summary query failed, falling back to standard query",
                        error=str(e),
                        exc_info=True,
                    )
                    # Fall through to standard query
        
        # Early return if any condition is "false" (invalid filter value)
        # This prevents building and executing expensive queries that will return empty results
        if "false" in regular_conditions or "false" in early_where_conditions:
            logger.info(
                "Invalid filter value detected in summary query - returning empty results early",
                filters=filters,
                regular_conditions=regular_conditions,
                early_where_conditions=early_where_conditions
            )
            return {"counts": {"total": 0}}
        
        # Build early WHERE clause
        early_where_clause = "\n        WHERE " + " AND ".join(early_where_conditions) if early_where_conditions else ""
        
        # Build WHERE clause - use list version for anatomical_sites if present
        all_conditions = regular_conditions.copy()
        
        # Don't add identifier filter conditions to where_conditions - they're now applied early
        
        # When preservation_method filter is present, add pf IS NOT NULL to WHERE clause
        # (the filter value is applied in OPTIONAL MATCH WHERE clause via early filter optimization)
        if preservation_method_param:
            all_conditions.append("pf IS NOT NULL")
        
        if anatomical_sites_list_condition:
            # Use list version first (will try string version if it fails)
            all_conditions.append(anatomical_sites_list_condition)
        # Add sequencing_file field conditions if present (will be checked after collecting all sequencing_files)
        # needs_sf_collection is already defined earlier
        if needs_sf_collection:
            all_conditions.append("has_matching_sf = true")
        # Add diagnosis search condition if present (will be checked after collecting all diagnoses)
        # needs_diag_collection is already defined earlier
        if needs_diag_collection:
            if diagnosis_search_term is not None:
                all_conditions.append("has_matching_diagnosis = true")
            elif has_diagnosis_filters:
                # For diagnosis filters, check if ANY diagnosis matches
                # Find disease_phase condition and convert it to check all_diagnoses
                diagnosis_filter_conditions = []
                for condition in all_conditions[:]:
                    if isinstance(condition, str) and "diagnoses.disease_phase" in condition:
                        # Convert to check all_diagnoses collection
                        # Original: "diagnoses IS NOT NULL AND diagnoses.disease_phase = $param_X"
                        # New: "size([d IN all_diagnoses WHERE d IS NOT NULL AND d.disease_phase = $param_X]) > 0"
                        condition_part = condition.replace("diagnoses IS NOT NULL AND ", "").replace("diagnoses.", "d.")
                        diagnosis_filter_conditions.append(f"size([d IN all_diagnoses WHERE d IS NOT NULL AND {condition_part}]) > 0")
                        all_conditions.remove(condition)
                    elif isinstance(condition, str) and ("diagnoses.tumor_grade" in condition or "diagnoses.tumor_classification" in condition or "diagnoses.tumor_tissue_morphology" in condition or "diagnoses.age_at_diagnosis" in condition):
                        # Convert other diagnosis filters similarly
                        condition_part = condition.replace("diagnoses IS NOT NULL AND ", "").replace("diagnoses.", "d.")
                        diagnosis_filter_conditions.append(f"size([d IN all_diagnoses WHERE d IS NOT NULL AND {condition_part}]) > 0")
                        all_conditions.remove(condition)
                    elif isinstance(condition, str) and "diagnoses.diagnosis" in condition:
                        # Convert diagnosis filter to check all_diagnoses collection
                        # Original: "(diagnoses IS NOT NULL AND (diagnoses.diagnosis = $param_X OR (toLower(...) = 'see diagnosis_comment' AND diagnoses.diagnosis_comment = $param_X)))"
                        # New: "size([d IN all_diagnoses WHERE d IS NOT NULL AND (d.diagnosis = $param_X OR (toLower(...) = 'see diagnosis_comment' AND d.diagnosis_comment = $param_X))]) > 0"
                        # Extract the inner condition (everything after "(diagnoses IS NOT NULL AND ")
                        condition_stripped = condition.strip()
                        if condition_stripped.startswith("(diagnoses IS NOT NULL AND "):
                            # Remove the outer wrapper: "(diagnoses IS NOT NULL AND " and trailing ")"
                            inner_condition = condition_stripped[len("(diagnoses IS NOT NULL AND "):].rstrip(")")
                            # Replace diagnoses. with d. in the inner condition
                            inner_condition = inner_condition.replace("diagnoses.diagnosis", "d.diagnosis").replace("diagnoses.diagnosis_comment", "d.diagnosis_comment")
                            diagnosis_filter_conditions.append(f"size([d IN all_diagnoses WHERE d IS NOT NULL AND ({inner_condition})]) > 0")
                        else:
                            # Fallback: try to extract more flexibly
                            if "(diagnoses IS NOT NULL AND " in condition:
                                start_pos = condition.find("(diagnoses IS NOT NULL AND ") + len("(diagnoses IS NOT NULL AND ")
                                inner_condition = condition[start_pos:].rstrip(")")
                                inner_condition = inner_condition.replace("diagnoses.diagnosis", "d.diagnosis").replace("diagnoses.diagnosis_comment", "d.diagnosis_comment")
                                diagnosis_filter_conditions.append(f"size([d IN all_diagnoses WHERE d IS NOT NULL AND ({inner_condition})]) > 0")
                            else:
                                # Last resort: simple replacement
                                condition_part = condition.replace("diagnoses IS NOT NULL AND ", "").replace("diagnoses.", "d.")
                                diagnosis_filter_conditions.append(f"size([d IN all_diagnoses WHERE d IS NOT NULL AND {condition_part}]) > 0")
                        all_conditions.remove(condition)
                if diagnosis_filter_conditions:
                    all_conditions.extend(diagnosis_filter_conditions)
        
        where_clause = ""
        if all_conditions:
            where_clause = "WHERE " + " AND ".join(all_conditions)
        
        # Determine which OPTIONAL MATCH clauses are needed based on filters
        needs_participant = any(
            field in filters for field in ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status"]
        ) or any("p." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_diagnosis = any(
            field in filters for field in ["disease_phase", "tumor_grade", "tumor_tissue_morphology", "tumor_classification", "age_at_diagnosis", "diagnosis"]
        ) or any("d." in str(cond) or "diagnoses" in str(cond) or "has_matching_diagnosis" in str(cond) for cond in all_conditions if isinstance(cond, str)) or diagnosis_search_term is not None
        
        needs_pathology_file = any(
            field in filters for field in ["preservation_method"]
        ) or any("pf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_sequencing_file = any(
            field in filters for field in ["library_selection_method", "library_strategy", "library_source_material", "specimen_molecular_analyte_type"]
        ) or any("sf." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        needs_study = any(
            field in filters for field in ["depositions"]
        ) or any("st." in str(cond) for cond in all_conditions if isinstance(cond, str))
        
        # OPTIMIZATION: Apply preservation_method filter EARLY in OPTIONAL MATCH WHERE clause
        # This avoids collecting all pathology_files then filtering (10-20x faster)
        pf_optional_match_where_summary = None
        if preservation_method_param:
            pf_optional_match_where_summary = f"WHERE pf.fixation_embedding_method = ${preservation_method_param}"
        
        # Build OPTIONAL MATCH clauses
        optional_matches = []
        # Need participant if filtering by participant fields OR if we need study paths
        if needs_participant or needs_study:
            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
        if needs_diagnosis:
            optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        # Apply early filter to pathology_file OPTIONAL MATCH if optimization applies
        if needs_pathology_file:
            if pf_optional_match_where_summary:
                optional_matches.append(f"OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)\n        {pf_optional_match_where_summary}")
            else:
                optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
        if needs_sequencing_file:
            optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
        
        # OPTIMIZATION: Early filtering (same as get_samples)
        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        
        # Build WITH clause - only include variables that were matched
        with_vars = ["sa"]
        with_collects = []
        if needs_participant or needs_study:
            with_vars.append("p")
        # For diagnosis search, collect ALL diagnoses first to check if ANY match
        # Note: needs_diag_collection may be updated above if diagnosis filters are present
        if needs_diagnosis:
            if needs_diag_collection:
                with_collects.append("collect(DISTINCT d) AS all_diagnoses")  # Collect ALL for search or filters
            else:
                with_collects.append("head(collect(DISTINCT d)) AS diagnoses")
        if needs_pathology_file:
            with_collects.append("head(collect(DISTINCT pf)) AS pf")
        # For sequencing_file fields, collect ALL sequencing_files first to check if ANY match
        # needs_sf_collection is already defined earlier
        if needs_sequencing_file:
            if needs_sf_collection:
                with_collects.append("collect(DISTINCT sf) AS all_sfs")  # Collect all sequencing_files
            else:
                with_collects.append("head(collect(DISTINCT sf)) AS sf")
        
        # Always include study
        with_vars.append("st")
        
        with_clause = ", ".join(with_vars)
        if with_collects:
            with_clause += ",\n             " + ",\n             ".join(with_collects)
        
        # Add identifiers_condition to WITH clause if present
        if identifiers_condition:
            with_clause += identifiers_condition
        
        # For sequencing_file fields or diagnosis search, we need a second WITH clause to use all_sfs or all_diagnoses
        # (can't reference a variable in the same WITH clause where it's defined)
        second_with_clause = None
        if needs_sf_collection or needs_diag_collection:
            # Build second_with_vars based on what's available from first WITH
            second_with_vars = ["sa", "st"]
            if needs_participant or needs_study:
                second_with_vars.append("p")
            
            # Pass through id_list if identifiers filter is present
            if identifiers_condition:
                second_with_vars.append("id_list")
            
            # Handle diagnosis search - check if ANY diagnosis matches
            # IMPORTANT: Only use diagnosis search condition if diagnosis_search_term is actually present
            # needs_diag_collection can be True for other diagnosis filters (disease_phase, tumor_classification, etc.)
            # but those don't require the diagnosis search parameters
            if needs_diag_collection and diagnosis_search_term is not None:
                # OPTIMIZATION 4A + 4D: Simplified diagnosis search condition
                # - Pre-processed search term (toLower done in Python)
                # - Simplified list handling (avoid wrapper for single values)
                # Handle both d.diagnosis and d.diagnosis_comment (when diagnosis = "see diagnosis_comment")
                diagnosis_search_condition = f"""size([d IN all_diagnoses WHERE d IS NOT NULL AND (
                    (toLower(trim(toString(d.diagnosis))) <> $diagnosis_search_term_see_comment AND 
                     CASE 
                       WHEN valueType(d.diagnosis) = 'LIST' THEN 
                         ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS $diagnosis_search_term_lower)
                       ELSE 
                         toLower(toString(d.diagnosis)) CONTAINS $diagnosis_search_term_lower
                     END)
                    OR
                    (toLower(trim(toString(d.diagnosis))) = $diagnosis_search_term_see_comment AND 
                     d.diagnosis_comment IS NOT NULL AND 
                     toLower(toString(d.diagnosis_comment)) CONTAINS $diagnosis_search_term_lower)
                )]) > 0"""
                second_with_vars.append(f"{diagnosis_search_condition} AS has_matching_diagnosis")
                # Use head() to pick first matching diagnosis from the collection
                second_with_vars.append("head([d IN all_diagnoses WHERE d IS NOT NULL | d]) AS diagnoses")
            elif needs_diag_collection:
                # needs_diag_collection is True but no diagnosis search - just collect diagnoses
                # This happens when diagnosis filters (disease_phase, tumor_classification, etc.) are present
                # but no _diagnosis_search filter
                # CRITICAL: Pass through all_diagnoses so it's available for WHERE clause conditions
                # that check all_diagnoses collection (e.g., diagnosis filter conversion)
                second_with_vars.append("all_diagnoses")
                # Use head() with list comprehension to pick first non-null diagnosis from the collection
                # Note: collect(DISTINCT d) may include null if d is null from OPTIONAL MATCH,
                # so filter out nulls in the list comprehension
                second_with_vars.append("head([d IN all_diagnoses WHERE d IS NOT NULL | d]) AS diagnoses")
            elif needs_diagnosis:
                second_with_vars.append("diagnoses")
            
            if needs_pathology_file:
                second_with_vars.append("pf")
        
        # Build SF-related conditions only if needs_sf_collection is true
        if needs_sf_collection:
            
            # Build conditions to check if ANY sequencing_file matches
            sf_match_conditions = []
            
            # Check specimen_molecular_analyte_type
            if specimen_molecular_analyte_type_list:
                db_values_str = ", ".join([f"'{v}'" for v in specimen_molecular_analyte_type_list])
                sf_match_conditions.append(f"sf.library_source_molecule IN [{db_values_str}]")
            elif specimen_molecular_analyte_type_single_param:
                if specimen_molecular_analyte_type_single_param == "invalid":
                    # Invalid value - add impossible condition to return empty results
                    sf_match_conditions.append("false")
                else:
                    sf_match_conditions.append(f"sf.library_source_molecule = ${specimen_molecular_analyte_type_single_param}")
            
            # Check library_selection_method
            if library_selection_method_param is not None:
                if library_selection_method_param == "invalid":
                    # Invalid value - add impossible condition to return empty results
                    sf_match_conditions.append("false")
                else:
                    sf_match_conditions.append(f"sf.library_selection = ${library_selection_method_param}")

            # Check library_strategy
            if library_strategy_param is not None:
                if library_strategy_param == "invalid":
                    # Invalid value - add impossible condition to return empty results
                    sf_match_conditions.append("false")
                elif isinstance(library_strategy_param, tuple):
                    # Has both mapped and original values
                    sf_match_conditions.append(f"(sf.library_strategy = ${library_strategy_param[0]} OR sf.library_strategy = ${library_strategy_param[1]})")
                else:
                    # Single value
                    sf_match_conditions.append(f"sf.library_strategy = ${library_strategy_param}")
            
            # Check library_source_material
            if library_source_material_param is not None:
                if library_source_material_param == "invalid":
                    # Invalid value (in null_mappings) - add impossible condition to return empty results
                    sf_match_conditions.append("false")
                else:
                    # Check if param_value is a list (for IN clause) or single value (for = clause)
                    param_value = params.get(library_source_material_param)
                    if isinstance(param_value, list) and len(param_value) > 0:
                        # Use IN clause for list values
                        sf_match_conditions.append(f"sf.library_source_material IN ${library_source_material_param}")
                    else:
                        # Use = for single value
                        sf_match_conditions.append(f"sf.library_source_material = ${library_source_material_param}")
            
            # Combine all conditions with OR (if multiple fields) or use single condition
            if len(sf_match_conditions) == 1:
                has_matching_sf_expr = f"size([sf IN all_sfs WHERE sf IS NOT NULL AND {sf_match_conditions[0]}]) > 0"
            else:
                # Multiple conditions - combine with OR
                combined_condition = " OR ".join([f"({cond})" for cond in sf_match_conditions])
                has_matching_sf_expr = f"size([sf IN all_sfs WHERE sf IS NOT NULL AND ({combined_condition})]) > 0"
            
            # SF collection is active, add SF-related variables
            second_with_vars.append(f"{has_matching_sf_expr} AS has_matching_sf")
            second_with_vars.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
        
        # If we have diagnosis search or sf collection, finalize second_with_clause
        if needs_sf_collection or needs_diag_collection:
            # If we don't have SF collection but have diagnosis search/filters, we need all_diagnoses
            # But only if it's not already being used in a head() expression
            if needs_diag_collection and not needs_sf_collection:
                # No SF collection, but we have diagnosis filters
                # Check if we've already added a diagnoses variable that uses all_diagnoses
                # If so, we don't need to add all_diagnoses separately since it's already referenced
                has_diagnoses_from_all = any("all_diagnoses" in var for var in second_with_vars)
                # Only add all_diagnoses if it's needed for WHERE clause conditions and not already referenced
                # Note: all_diagnoses is already available from first WITH clause, so we don't need to add it here
                # unless it's needed for WHERE clause filtering (which is handled separately)
                # Also pass through sf variable if it exists
                if needs_sequencing_file:
                    second_with_vars.append("sf")
            # If we have SF collection but no diagnosis search, we still need diagnoses variable if it was collected
            elif needs_sf_collection and not needs_diag_collection:
                # No diagnosis search, but diagnoses might be needed for filters (e.g., disease_phase)
                if needs_diagnosis and "diagnoses" not in ", ".join(second_with_vars):
                    second_with_vars.append("diagnoses")
            second_with_clause = ", ".join(second_with_vars)
        
        # Build WHERE clause - include filter conditions
        # Separate conditions that need to be in first WITH (for identifiers) from those that need to be after second WITH (for has_matching_sf/has_matching_diagnosis)
        first_where_conditions = []
        second_where_conditions = []
        
        # Parse where_clause to separate conditions with has_matching_sf/has_matching_diagnosis from others
        if where_clause:
            # Extract conditions from where_clause (remove "WHERE " prefix)
            where_conditions_str = where_clause.replace("WHERE ", "").strip()
            if where_conditions_str:
                # Split by " AND " to get individual conditions
                # But be careful not to split on " AND " inside brackets/parentheses (e.g., in list comprehensions)
                conditions = []
                current_condition = ""
                paren_depth = 0
                bracket_depth = 0
                i = 0
                while i < len(where_conditions_str):
                    char = where_conditions_str[i]
                    if char == '(':
                        paren_depth += 1
                        current_condition += char
                    elif char == ')':
                        paren_depth -= 1
                        current_condition += char
                    elif char == '[':
                        bracket_depth += 1
                        current_condition += char
                    elif char == ']':
                        bracket_depth -= 1
                        current_condition += char
                    elif char == ' ' and i + 4 < len(where_conditions_str) and where_conditions_str[i:i+5] == " AND ":
                        # Check if we're at " AND " and not inside brackets/parentheses
                        if paren_depth == 0 and bracket_depth == 0:
                            if current_condition.strip():
                                conditions.append(current_condition.strip())
                            current_condition = ""
                            i += 4  # Skip " AND " (will be incremented by 1 at end of loop)
                        else:
                            # Inside brackets/parentheses - add the entire " AND " to current condition
                            current_condition += " AND "
                            i += 4  # Skip " AND " (will be incremented by 1 at end of loop)
                    else:
                        current_condition += char
                    i += 1
                # Add the last condition
                if current_condition.strip():
                    conditions.append(current_condition.strip())
                
                for condition in conditions:
                    if "has_matching_sf" in condition or "has_matching_diagnosis" in condition:
                        second_where_conditions.append(condition)
                    elif needs_diag_collection and ("all_diagnoses" in condition or "diagnoses" in condition):
                        # If there's a diagnosis search or diagnosis filters, all_diagnoses is created in first WITH clause
                        # Since needs_diag_collection is true, there will be a second_with_clause (created above)
                        # So conditions referencing all_diagnoses should be in second_where_conditions
                        if "all_diagnoses" in condition:
                            # This is already converted to check all_diagnoses collection, put in second_where_conditions
                            second_where_conditions.append(condition)
                        elif "diagnoses" in condition:
                            # Legacy condition using diagnoses (should have been converted above, but handle it)
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
        
        # Build the query - use multi-hop traversal for early filtering (same as get_samples)
        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        
        if second_with_str:
            # second_with_str already includes the WITH and WHERE clauses
            # Use sample_id + study_id as unique identifier (same sample_id can be in different studies)
            cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        {early_where_clause.replace('WHERE ', 'AND ') if early_where_clause else ''}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa,
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, sid
        WHERE sid IS NOT NULL
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter_summary}
        {optional_matches_str}
        WITH {with_clause}
        {second_with_str}WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
        else:
            # No second WITH, use final_where_clause after first WITH
            # Deduplicate by sample_id to ensure one row per sample (handles multiple study relationships)
            cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        {early_where_clause.replace('WHERE ', 'AND ') if early_where_clause else ''}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa,
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, sid
        WHERE sid IS NOT NULL
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter_summary}
        {optional_matches_str}
        WITH {with_clause}
        {final_where_clause}WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
        
        logger.info(
            "Executing get_samples_summary Cypher query",
            cypher=cypher,
            params=params,
            identifiers_condition=identifiers_condition if identifiers_condition else None,
            where_conditions=where_conditions,
            final_where_conditions=final_where_conditions if 'final_where_conditions' in locals() else None,
            with_clause=with_clause,
            depositions_study_filter_summary=depositions_study_filter_summary,
            _depositions_early_params=getattr(self, '_depositions_early_params', None)
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
                return {"counts": {"total": 0}}
            
            summary = records[0]
            total_count = summary.get("total_count", 0)
            logger.info("Completed samples summary", total_count=total_count)
            
            return {"counts": {"total": total_count}}
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
                WITH sa.sample_id AS sample_id, st.study_id AS study_id
                RETURN count(*) as total_count
                """.strip()
                
                try:
                    result = await self.session.run(cypher, params)
                    async for record in result:
                        records.append(dict(record))
                    logger.debug("Successfully executed anatomical_sites summary query as string")
                    
                    if not records:
                        logger.warning("No records returned from summary query")
                        return {"counts": {"total": 0}}
                    
                    summary = records[0]
                    total_count = summary.get("total_count", 0)
                    logger.info("Completed samples summary (string version)", total_count=total_count)
                    
                    return {"counts": {"total": total_count}}
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
    
    async def _get_samples_summary_reverse_query(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Optimized summary query for sequencing_file-only filters.
        Uses reverse query starting from sequencing_file.
        """
        params = {}
        where_conditions = []
        param_counter = 0
        
        # Build WHERE conditions - same as _get_samples_by_sequencing_file_filters
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if field == "library_source_material":
                if is_null_mapped_value("library_source_material", value):
                    return {"counts": {"total": 0}}
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                params[param_name] = reverse_mapped
                where_conditions.append(f"sf.library_source_material = ${param_name}")
                
            elif field == "library_strategy":
                if is_database_only_value("library_strategy", value):
                    return {"counts": {"total": 0}}
                reverse_mapped = reverse_map_field_value("library_strategy", value)
                if reverse_mapped and reverse_mapped != value:
                    param_counter += 1
                    param_name2 = f"param_{param_counter}"
                    params[param_name] = reverse_mapped if isinstance(reverse_mapped, str) else reverse_mapped[0]
                    params[param_name2] = value
                    where_conditions.append(f"(sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name2})")
                else:
                    params[param_name] = value
                    where_conditions.append(f"sf.library_strategy = ${param_name}")
                    
            elif field == "library_selection_method":
                if is_database_only_value("library_selection_method", value):
                    return {"counts": {"total": 0}}
                db_value = self._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                where_conditions.append(f"sf.library_selection = ${param_name}")
                
            elif field == "specimen_molecular_analyte_type":
                if is_database_only_value("specimen_molecular_analyte_type", value) or is_null_mapped_value("specimen_molecular_analyte_type", value):
                    return {"counts": {"total": 0}}
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    db_values_str = ", ".join([f"'{v}'" for v in reverse_mapped])
                    where_conditions.append(f"sf.library_source_molecule IN [{db_values_str}]")
                else:
                    params[param_name] = reverse_mapped
                    where_conditions.append(f"sf.library_source_molecule = ${param_name}")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
        
        # Optimized count query
        # PERFORMANCE: use multi-hop traversal for sample->study lookup and count unique (sample_id, study_id).
        cypher = f"""
        MATCH (sf:sequencing_file)
        WHERE {where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, sf, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, sf, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, sf, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        WITH sa, sf, sid
        WHERE sid IS NOT NULL
        WITH DISTINCT sa.sample_id AS sample_id, sid AS study_id
        RETURN count(*) as total_count
        """.strip()
        
        logger.info(
            "Executing optimized reverse summary query",
            cypher=cypher[:300],
            params=params
        )
        
        try:
            result = await self.session.run(cypher, params)
            record = await result.single()
            await result.consume()
            
            total_count = record["total_count"] if record else 0
            
            logger.debug("Reverse summary query executed successfully", total_count=total_count)
            
            # Return in the expected format for the service layer
            return {
                "counts": {
                    "total": total_count
                }
            }
            
        except Exception as e:
            logger.error("Error executing reverse summary query", error=str(e), exc_info=True)
            raise
    
