"""
Count methods for SampleRepository.

This module contains methods for counting samples by field values.
"""

import asyncio
from typing import Dict, Any, List
from app.core.logging import get_logger
from app.models.errors import UnsupportedFieldError
from app.core.diagnosis_category import HARMONIZED_DIAGNOSIS_CATEGORIES
from app.core.field_mappings import (
    map_field_value,
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
    build_invalid_value_filter,
    build_invalid_value_list_filter,
    build_invalid_value_all_clause,
    build_case_mapping_statement,
    get_mapped_db_values,
    load_sample_enum,
    load_sequencing_file_enum
)

logger = get_logger(__name__)

_HARMONIZED_PVS_SORTED: List[str] = sorted(HARMONIZED_DIAGNOSIS_CATEGORIES)
_HARMONIZED_PVS_LOWER: List[str] = [pv.lower() for pv in _HARMONIZED_PVS_SORTED]


class SampleCount:
    """Mixin class providing count methods for SampleRepository."""

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

        # Special handling for diagnosis_category — harmonized PV count
        if field == "diagnosis_category":
            return await self._count_samples_by_diagnosis_category(filters)

        # Validate field is allowed for count operations
        # Only sample-specific metadata fields are allowed (participant fields are not supported for samples)
        sample_metadata_fields = {
            "disease_phase", "anatomical_sites", "library_selection_method", "library_strategy",
            "library_source_material", "preservation_method", "tumor_grade", "specimen_molecular_analyte_type",
            "tissue_type", "tumor_classification", "age_at_diagnosis", "age_at_collection",
            "tumor_tissue_morphology", "diagnosis", "diagnosis_category"
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
            "tissue_type": ("sa", "sample_tumor_status"),  # From sample node (sample_tumor_status field)
            "tumor_classification": ("d", "tumor_classification"),  # From diagnosis node
            "age_at_diagnosis": ("d", "age_at_diagnosis"),  # From diagnosis node
            "age_at_collection": ("sa", "participant_age_at_collection"),  # From sample node
            "tumor_tissue_morphology": ("d", "tumor_tissue_morphology"),  # From diagnosis node
            "depositions": ("st", "study_id"),  # From study node
            "diagnosis": ("d", "diagnosis")  # From diagnosis node
        }
        
        # Determine if this is a participant field or sample metadata field
        is_participant_field = field in participant_field_mapping
        is_sample_metadata_field = field in sample_metadata_field_mapping
        
        # Flag to track if we're using a combined query (total + missing + values in one query)
        # Currently only used for library_source_material when no filters
        is_combined_query = False
        
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
        # Support || separator for OR logic (e.g., "SAMP001 || SAMP002")
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
                    if isinstance(identifiers_value, list):
                        base_where_conditions.append(f"sa.sample_id IN ${id_param}")
                    else:
                        base_where_conditions.append(f"sa.sample_id = ${id_param}")
        
        # Handle depositions filter (study_id)
        # Support || separator for OR logic (e.g., "phs001 || phs002")
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            if depositions_value is not None and str(depositions_value).strip():
                depositions_str = str(depositions_value).strip()
                # Parse || separator
                if "||" in depositions_str:
                    depositions_list = [d.strip() for d in depositions_str.split("||")]
                    depositions_list = [d for d in depositions_list if d]
                    if depositions_list:
                        param_counter += 1
                        dep_param = f"param_{param_counter}"
                        if len(depositions_list) == 1:
                            params[dep_param] = depositions_list[0]
                            base_where_conditions.append(f"st.study_id = ${dep_param}")
                        else:
                            params[dep_param] = depositions_list
                            base_where_conditions.append(f"st.study_id IN ${dep_param}")
                else:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = depositions_str
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
        
        # Handle diagnosis field filters (disease_phase, tumor_grade, tumor_classification, age_at_diagnosis, tumor_tissue_morphology)
        # These need to reference diagnoses node, not participant node
        diagnosis_filter_fields = ["disease_phase", "tumor_grade", "tumor_classification", "age_at_diagnosis", "tumor_tissue_morphology", "diagnosis"]
        for filter_field in list(filters.keys()):
            if filter_field in diagnosis_filter_fields:
                value = filters.pop(filter_field)
                param_counter += 1
                param_name = f"param_{param_counter}"
                
                if filter_field == "disease_phase":
                    if is_database_only_value("disease_phase", value) or is_null_mapped_value("disease_phase", value):
                        base_where_conditions.append("false")
                    else:
                        reverse_mapped = reverse_map_field_value("disease_phase", value)
                        if isinstance(reverse_mapped, list):
                            params[param_name] = reverse_mapped
                            base_where_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase IN ${param_name}")
                        else:
                            params[param_name] = reverse_mapped
                            base_where_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.disease_phase = ${param_name}")
                elif filter_field == "tumor_classification":
                    if is_null_mapped_value("tumor_classification", value):
                        base_where_conditions.append("false")
                    else:
                        reverse_mapped = reverse_map_field_value("tumor_classification", value)
                        params[param_name] = reverse_mapped
                        base_where_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_classification = ${param_name}")
                elif filter_field == "tumor_grade":
                    params[param_name] = value
                    base_where_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_grade = ${param_name}")
                elif filter_field == "tumor_tissue_morphology":
                    params[param_name] = value
                    base_where_conditions.append(f"diagnoses IS NOT NULL AND diagnoses.tumor_tissue_morphology = ${param_name}")
                elif filter_field == "age_at_diagnosis":
                    try:
                        params[param_name] = int(value) if value is not None else None
                    except (ValueError, TypeError):
                        params[param_name] = value
                    base_where_conditions.append(f"diagnoses IS NOT NULL AND toInteger(diagnoses.age_at_diagnosis) = ${param_name}")
                elif filter_field == "diagnosis":
                    params[param_name] = value
                    base_where_conditions.append(f"""(diagnoses IS NOT NULL AND 
                        (diagnoses.diagnosis = ${param_name} OR 
                        (toLower(trim(toString(diagnoses.diagnosis))) = 'see diagnosis_comment' AND 
                        diagnoses.diagnosis_comment IS NOT NULL AND 
                        trim(toString(diagnoses.diagnosis_comment)) = ${param_name})))""")
        
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
        
        # Standard field handling
        if field == "anatomical_sites":
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
                WITH sample_id, study_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH sample_id, study_id, trimmed_value
                RETURN trimmed_value as value, count(*) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                
                # Query 2: Assume it's a string (could be semicolon-separated)
                # Split by semicolon if it contains ';', otherwise treat as single value
                cypher_string = f"""
                MATCH (sa:sample)
                {optional_matches_str}
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH sa.sample_id as sample_id, st.study_id as study_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id, study_id, 
                     CASE 
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, study_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH sample_id, study_id, trimmed_value
                RETURN trimmed_value as value, count(*) AS count
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
                WITH sa.sample_id as sample_id, st.study_id as study_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id, study_id,
                     CASE 
                       WHEN valueType(sites) = 'LIST' THEN sites
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, study_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH sample_id, study_id, trimmed_value
                RETURN trimmed_value as value, count(*) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                
                # Query 2: Assume it's a string
                cypher_string = f"""
                MATCH (sa:sample)
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, coalesce(st1, st2) AS st
                {field_where_clause}
                WITH sa.sample_id as sample_id, st.study_id as study_id, sa.anatomic_site as sites
                WHERE sites IS NOT NULL
                WITH sample_id, study_id, 
                     CASE 
                       WHEN toString(sites) CONTAINS ';' THEN SPLIT(toString(sites), ';')
                       ELSE [toString(sites)]
                     END AS site_values
                UNWIND site_values AS site_value
                WITH sample_id, study_id, trim(toString(site_value)) AS trimmed_value
                WHERE trimmed_value IS NOT NULL 
                  AND trimmed_value <> ''
                  AND toLower(trimmed_value) <> 'invalid value'
                WITH sample_id, study_id, trimmed_value
                RETURN trimmed_value as value, count(*) AS count
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
                    # 1. Start from sequencing_file (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Map DB values to API values in Cypher using CASE statement
                    # 4. Group by API value and count distinct samples directly in Cypher
                    # Performance improvements:
                    # - Start from sequencing_file instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Do mapping and counting in Cypher (eliminates Python-side processing overhead)
                    # - Returns aggregated results directly (much fewer rows)
                    # Optimized: Start from sample (like other sequencing_file fields), collect distinct values per sample first
                    # Then map the collected values (much fewer rows to process in CASE statement)
                    # Profile showed CASE evaluation on 1.1M rows was 30% of time - this reduces that significantly
                    # Strategy: Collect distinct molecule values per sample → Map → Deduplicate → Aggregate
                    # Note: IN clause uses mapped DB values from field_mappings.json, CASE statement built dynamically
                    mapped_db_values = get_mapped_db_values(field)
                    if not mapped_db_values:
                        # No mappings configured, return empty results
                        cypher = "RETURN '' as value, 0 AS count LIMIT 0"
                    else:
                        # Build IN clause from mapped DB values (escape single quotes by doubling them)
                        db_values_str = ", ".join([f"'{val.replace(chr(39), chr(39) + chr(39))}'" for val in mapped_db_values])
                        # Build CASE statement dynamically from mappings
                        case_statement = build_case_mapping_statement(field, "molecule_value")
                        if not case_statement:
                            # No mappings, return empty results
                            cypher = "RETURN '' as value, 0 AS count LIMIT 0"
                        else:
                            # COMBINED QUERY: Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Performance: Collect → Filter → Map → Calculate total/missing/values
                            # Use parameterized query for better index usage
                            params["mapped_db_values"] = mapped_db_values
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT sf.library_source_molecule) as molecule_values
                WITH sample_id, study_id, molecule_values,
                     // Filter to only mapped DB values using parameterized query
                     [val IN molecule_values WHERE val IS NOT NULL AND val IN $mapped_db_values] as mapped_db_values_list,
                     // For MISSING: Check if no valid values (NULL/empty/-999/null_mappings)
                     CASE WHEN size([val IN molecule_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                // IMPORTANT: Calculate BEFORE unwinding to get correct totals
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, mapped_db_values_list: mapped_db_values_list}}) as all_samples
                // Now unwind mapped values and map to API values
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.mapped_db_values_list as mapped_db_values_list,
                     total, missing,
                     CASE WHEN size(sample_data.mapped_db_values_list) = 0 THEN [null] ELSE sample_data.mapped_db_values_list END as values_to_unwind
                UNWIND values_to_unwind as molecule_value
                WITH sample_id, study_id, molecule_value, total, missing
                WHERE molecule_value IS NOT NULL  // Filter out the null placeholder rows
                WITH sample_id, study_id,
                     {case_statement} as api_value,
                     total, missing
                WHERE api_value IS NOT NULL
                // Deduplicate by (sample_id, study_id, api_value) before counting
                WITH DISTINCT sample_id, study_id, api_value, total, missing
                WITH api_value as value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                elif node_alias == "sf" and not base_where_clause:
                    # Special handling for library_source_material: Combined query (total + missing + values)
                    # For other sequencing_file fields: Use separate queries
                    if field == "library_source_material":
                        # COMBINED QUERY: Returns total, missing, and values in one pass
                        # This avoids running 3 separate queries and processes samples once
                        # IMPORTANT: Missing check should NOT include enum validation - only check for NULL/empty/-999/null_mappings
                        # Values check SHOULD include enum validation to only count valid enum values
                        invalid_list_filter = build_invalid_value_list_filter(field)
                        # Load enum values to filter FOR valid values in the values query
                        enum_values = load_sequencing_file_enum("library_source_material")
                        if enum_values:
                            # Validate enum values don't contain dangerous characters
                            # This is a security safeguard even though enum values come from JSON files
                            dangerous_chars = ['"', "'", '\\', '`', '{', '}', '[', ']']
                            for enum_val in enum_values:
                                if any(char in str(enum_val) for char in dangerous_chars):
                                    logger.error(
                                        "Invalid enum value contains dangerous characters",
                                        field=field,
                                        enum_value=enum_val,
                                        dangerous_chars=[char for char in dangerous_chars if char in str(enum_val)]
                                    )
                                    raise ValueError(f"Enum value '{enum_val}' contains dangerous characters")
                            
                            # Use parameterized query instead of string interpolation for security
                            # Pass enum_values as a parameter to prevent Cypher injection
                            params["enum_values"] = enum_values
                            # Separate logic:
                            # - valid_values: Filter for enum values AND exclude null_mappings (for counting values)
                            # - is_missing: Only check if no valid values exist (NULL/empty/-999/null_mappings), WITHOUT enum check
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid enum values AND exclude null_mappings
                     // Use parameterized query ($enum_values) instead of string interpolation for security
                     [val IN field_values WHERE val IS NOT NULL 
                      AND {invalid_list_filter}
                      AND val IN $enum_values] as valid_values,
                     // For MISSING: Only check if no valid values (NULL/empty/-999/null_mappings), WITHOUT enum check
                     // This matches the original missing query logic
                     // IMPORTANT: null_mappings like "Other" are excluded by invalid_list_filter (val <> 'Other')
                     // So samples with only "Other" will have empty filtered list → size = 0 → counted as missing ✓
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                // IMPORTANT: The original missing query filters samples WHERE size(...) = 0
                // Our combined query calculates is_missing per sample, then sums
                // These should be equivalent, but we need to ensure we're grouping correctly
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id, 
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                        else:
                            # Fallback to original approach if enum not available
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     [val IN field_values WHERE val IS NOT NULL 
                      AND {invalid_list_filter}] as valid_values,
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id, 
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                        # Mark this as a combined query so we can handle it differently
                        is_combined_query = True
                    else:
                        # Check if this field should use combined query for better performance
                        if field == "library_strategy":
                            # COMBINED QUERY for library_strategy:
                            # Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Requirements:
                            # - Count samples by unique (sample_id + study_id) pairs
                            # - Total: all samples with study paths
                            # - Missing: samples without valid values (no sequencing_file or all invalid)
                            # - Values: samples with valid values, grouped by strategy
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999)
                     [val IN field_values WHERE val IS NOT NULL 
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id, 
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                        else:
                            # COMBINED QUERY for library_selection_method:
                            # Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Requirements:
                            # - Count samples by unique (sample_id + study_id) pairs
                            # - Total: all samples with study paths
                            # - Missing: samples without valid values (no sequencing_file or all invalid)
                            # - Values: samples with valid values, grouped by selection method
                            # Constraint: Total ≤ Values_sum + Missing
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999, not null_mappings)
                     [val IN field_values WHERE val IS NOT NULL 
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id, 
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                elif node_alias == "pf" and not base_where_clause:
                    # COMBINED QUERY for pathology_file fields (preservation_method):
                    # Returns total, missing, and values in one pass
                    # This avoids running 3 separate queries and processes samples once
                    # Requirements:
                    # - Count samples by unique (sample_id + study_id) pairs
                    # - Total: all samples with study paths
                    # - Missing: samples without valid values (no pathology_file or all invalid)
                    # - Values: samples with valid values, grouped by method
                    invalid_list_filter = build_invalid_value_list_filter(field)
                    cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999)
                     [val IN field_values WHERE val IS NOT NULL 
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                     AND {invalid_list_filter}]) = 0 
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id, 
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                    # Mark this as a combined query so we can handle it differently
                    is_combined_query = True
                elif node_alias == "d" and not base_where_clause:
                    # Optimized query for diagnosis fields (disease_phase, tumor_grade, etc.):
                    # 1. Start from diagnosis (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Use head() to get one diagnosis per sample, then group by field value
                    # Performance improvements:
                    # - Start from diagnosis instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Simplify redundant WHERE conditions (assume string fields)
                    invalid_filter = build_invalid_value_filter(node_field, field)
                    cypher = f"""
                MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                  AND {invalid_filter}
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, d, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, d, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, d, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                WITH sa, d, toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id
                WITH sample_id, study_id,
                     head(collect(DISTINCT {node_field})) as value
                WITH DISTINCT sample_id, study_id, value
                WHERE value IS NOT NULL
                  AND toString(value) <> ''
                  AND trim(toString(value)) <> ''
                  AND toString(value) <> '-999'
                  AND trim(toString(value)) <> '-999'
                RETURN toString(value) as value, count(*) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                elif node_alias == "sa" and not base_where_clause:
                    # Optimized query for sample node fields (tissue_type, age_at_collection):
                    # 1. Filter invalid values early
                    # 2. Group by field value and count distinct samples
                    # Performance improvements:
                    # - Consistent query structure with total and missing queries
                    cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                  AND {node_field} IS NOT NULL
                  AND toString({node_field}) <> ''
                  AND trim(toString({node_field})) <> ''
                  AND toString({node_field}) <> '-999'
                  AND trim(toString({node_field})) <> '-999'
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                WITH sa.sample_id as sample_id, st.study_id as study_id, {node_field} as value
                RETURN toString(value) as value, count(*) AS count
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
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                WITH sa, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
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
        
        # Special handling for combined query (library_source_material)
        # Combined query returns: value, count, total, missing
        # Extract total and missing from first row, then process values
        # IMPORTANT: If all samples have no valid values, records will be empty
        # In that case, we need to run a separate query to get total and missing
        total = 0
        missing = 0
        if is_combined_query:
            if records:
                # Extract total and missing from first row (they're the same in all rows)
                total = records[0].get("total", 0)
                missing = records[0].get("missing", 0)
                logger.debug(
                    "Combined query results",
                    field=field,
                    total=total,
                    missing=missing,
                    records_count=len(records)
                )
                # Process values (skip total/missing columns)
                for record in records:
                    value = record.get("value")
                    count = record.get("count", 0)
                    
                    if not value or count == 0:
                        continue
                    
                    # For specimen_molecular_analyte_type, the value is already mapped in Cypher (API value)
                    # For other fields, apply field mapping (DB value -> API value)
                    if field == "specimen_molecular_analyte_type":
                        mapped_value = value  # Already mapped in Cypher CASE statement
                    else:
                        mapped_value = map_field_value(field, value)
                    
                    # If mapping returns None, skip this value (it should be counted as missing)
                    # Also explicitly check if the original value is in null_mappings as a safeguard
                    if mapped_value is None:
                        continue
                    
                    # Additional safeguard: explicitly filter out null_mapped values
                    # This ensures values like "Other" for library_source_material are excluded
                    if is_null_mapped_value(field, value):
                        continue
                    
                    counts.append({
                        "value": mapped_value,
                        "count": count
                    })
                
                # Aggregate counts for fields where multiple DB values map to the same API value
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
            else:
                # If records is empty, all samples have no valid values
                # We need to get total and missing from a separate query
                # This happens when ALL samples have is_missing=1 and no valid enum values
                logger.warning(
                    "Combined query returned empty results - all samples may be missing",
                    field=field
                )
                # Run a simple query to get total and missing
                # Use the same logic as the combined query but without UNWIND
                # Determine the node and field based on field name
                invalid_list_filter = build_invalid_value_list_filter(field)
                # For combined queries, we know the field structure:
                # - library_source_material, library_strategy, library_selection_method, specimen_molecular_analyte_type: sequencing_file (sf)
                # - preservation_method: pathology_file (pf)
                if field in ["library_source_material", "library_strategy", "library_selection_method", "specimen_molecular_analyte_type"]:
                    node_alias = "sf"
                    relationship = "-[:of_sequencing_file]->"
                    node_type = "sequencing_file"
                elif field == "preservation_method":
                    node_alias = "pf"
                    relationship = "-[:of_pathology_file]->"
                    node_type = "pathology_file"
                else:
                    # Fallback - should not happen for combined queries
                    node_alias = "sf"
                    relationship = "-[:of_sequencing_file]->"
                    node_type = "sequencing_file"
                
                # Determine the correct node_field based on field name
                if field == "specimen_molecular_analyte_type":
                    node_field = "sf.library_source_molecule"
                elif field in ["library_source_material", "library_strategy", "library_selection_method"]:
                    node_field = f"{node_alias}.{sample_metadata_field_mapping[field][1]}"
                elif field == "preservation_method":
                    node_field = f"{node_alias}.{sample_metadata_field_mapping[field][1]}"
                else:
                    # Fallback
                    node_field = f"{node_alias}.{sample_metadata_field_mapping.get(field, ('', field))[1]}"
                
                fallback_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH ({node_alias}:{node_type}){relationship}(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH count(*) as total,
                     sum(CASE WHEN size([val IN field_values WHERE val IS NOT NULL 
                                         AND {invalid_list_filter}]) = 0 
                              THEN 1 ELSE 0 END) as missing
                RETURN total, missing
                """.strip()
                try:
                    fallback_result = await self.session.run(fallback_cypher, params)
                    fallback_records = []
                    async for record in fallback_result:
                        fallback_records.append(dict(record))
                    if fallback_records:
                        total = fallback_records[0].get("total", 0)
                        missing = fallback_records[0].get("missing", 0)
                except Exception as e:
                    logger.error(
                        "Error executing fallback query for combined query",
                        error=str(e),
                        field=field,
                        exc_info=True
                    )
        
        # Special handling for specimen_molecular_analyte_type
        # Query now returns aggregated results (value, count) with mapping done in Cypher
        # This eliminates Python-side processing overhead and reduces returned rows significantly
        elif field == "specimen_molecular_analyte_type":
            # Query already returns value and count, just process like other fields
            # But need to handle deduplication: one sample can have multiple DB values that map to same API value
            # The Cypher query handles this with DISTINCT sample_id per api_value
            for record in records:
                value = record.get("value")
                count = record.get("count", 0)
                
                if not value or count == 0:
                    continue
                
                counts.append({
                    "value": value,
                    "count": count
                })
            counts.sort(key=lambda x: (-x["count"], x["value"]))
        else:
            # Standard processing for other fields
            for record in records:
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
        # Skip if we already have total/missing from combined query
        if is_combined_query:
            # Total and missing already extracted from combined query above
            pass
        elif field == "anatomical_sites":
            # Total: all samples with a path to a study (matching /sample/summary - 50211 when no filters)
            if not base_where_clause:
                # No filters - count all samples with a path to a study (matches /sample/summary)
                # Use the same query structure as get_samples_summary - always include participant
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id
                RETURN count(*) as total
                """.strip()
            else:
                # Has filters - need to include study paths and require st IS NOT NULL
                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                WITH sa, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
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
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR size(sites) = 0
                   OR ALL(site IN sites WHERE site IS NULL OR toString(site) = '' OR toLower(trim(toString(site))) = 'invalid value')
                RETURN count(*) as missing
                """.strip()
                
                missing_cypher_string = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id, sa.anatomic_site as sites
                WHERE sites IS NULL 
                   OR toString(sites) = ''
                   OR toLower(trim(toString(sites))) = 'invalid value'
                RETURN count(*) as missing
                """.strip()
            else:
                # Has base filters - build queries for both list and string cases
                missing_cypher_list = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id, p, sa.anatomic_site as sites
                {base_where_clause.replace('WHERE ', 'WHERE ') if base_where_clause else ''}
                WHERE sites IS NULL 
                   OR size(sites) = 0
                   OR ALL(site IN sites WHERE site IS NULL OR toString(site) = '' OR toLower(trim(toString(site))) = 'invalid value')
                RETURN count(*) as missing
                """.strip()
                
                missing_cypher_string = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id, p, sa.anatomic_site as sites
                {base_where_clause.replace('WHERE ', 'WHERE ') if base_where_clause else ''}
                WHERE sites IS NULL 
                   OR toString(sites) = ''
                   OR toLower(trim(toString(sites))) = 'invalid value'
                RETURN count(*) as missing
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
                # Use toString() for consistency with other queries
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
                RETURN count(*) as total
                """.strip()
            elif not base_where_clause:
                # No filters - count all samples with a path to a study (matches /sample/summary)
                # Use the same query structure as get_samples_summary - always include participant
                # Use toString() for consistency with other queries
                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
                RETURN count(*) as total
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
                        # Use toString() for consistency with other queries
                        total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
                RETURN count(*) as total
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
                        
                        # For fields on related nodes (sf, pf, d), we need to ensure the sample has a path to a study
                        # For diagnosis fields (d), total should count ALL samples with study paths, not just samples with diagnoses
                        if node_alias == "d":
                            # Diagnosis fields: count ALL samples with study paths (including those without diagnoses)
                            if base_where_clause:
                                # Has filters - need to apply them
                                # Build optional matches for filters that need them
                                filter_optional_matches = []
                                if any("p." in cond for cond in base_where_conditions):
                                    filter_optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                                if any("d." in cond or "diagnoses" in cond for cond in base_where_conditions):
                                    filter_optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                                filter_optional_matches_str = "\n                ".join(filter_optional_matches) if filter_optional_matches else ""
                                
                                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                {filter_optional_matches_str}
                WITH sa, p, d
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else ''}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
                            else:
                                # No filters - count ALL samples with study paths (matches values query structure)
                                # Use toString() for consistency with other queries
                                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
                RETURN count(*) as total
                """.strip()
                        elif node_alias in ["sf", "pf"]:
                            # For other related nodes (sf, pf), count ALL samples with study paths
                            # (not just samples with the node, to match missing query which includes samples without the node)
                            # Requirements:
                            # - Count ALL samples with path to a study
                            # - Use (sample_id + study_id) as unique identifier
                            # Skip total query for combined query fields (handled by combined query)
                            if field in ["preservation_method", "library_source_material", "library_strategy", "library_selection_method"] and not base_where_clause:
                                # Total count is already included in combined query
                                total_cypher = None
                            elif base_where_clause:
                                # Has filters - need to apply them
                                # Build optional matches for filters that need them
                                filter_optional_matches = []
                                if any("p." in cond for cond in base_where_conditions):
                                    filter_optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                                if any(f"{node_alias}." in cond for cond in base_where_conditions):
                                    if node_alias == "sf":
                                        filter_optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                                    elif node_alias == "pf":
                                        filter_optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                                filter_optional_matches_str = "\n                ".join(filter_optional_matches) if filter_optional_matches else ""
                                
                                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                {filter_optional_matches_str}
                WITH sa, p, {node_alias}, sa.sample_id as sample_id, st.study_id as study_id
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else ''}
                WITH DISTINCT sample_id, study_id
                RETURN count(*) as total
                """.strip()
                            else:
                                # No filters - count ALL samples with study paths (matches values/missing query structure)
                                # Count by unique (sample_id + study_id) pairs
                                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id
                RETURN count(*) as total
                """.strip()
                        else:
                            # For fields on sample node or study node, just require study path
                            if base_where_clause:
                                # Has filters - need to apply them
                                # Build optional matches for filters that need them
                                filter_optional_matches = []
                                if any("p." in cond for cond in base_where_conditions):
                                    filter_optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                                filter_optional_matches_str = "\n                ".join(filter_optional_matches) if filter_optional_matches else ""
                                
                                total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                {filter_optional_matches_str}
                WITH sa, p
                {base_where_clause.replace('WHERE ', 'AND ') if base_where_clause else ''}
                RETURN count(DISTINCT sa.sample_id) as total
                """.strip()
                            else:
                                # No filters - use multi-hop traversal (consistent with values query)
                                total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id
                RETURN count(*) as total
                """.strip()
                else:
                    # Participant fields - need to include study paths and require st IS NOT NULL
                    total_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                WITH sa, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
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
                        
                        # Build invalid value conditions based on null_mappings for this field
                        invalid_all_clause = build_invalid_value_all_clause(field)
                        missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                WITH sa, d, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
                WITH DISTINCT sa, collect(DISTINCT {node_field}) as all_values
                WITH sa,
                     [val IN all_values WHERE val IS NOT NULL] as non_null_values
                WHERE size(non_null_values) = 0 
                   OR ALL(val IN non_null_values WHERE {invalid_all_clause})
                RETURN count(DISTINCT sa) as missing
                """.strip()
                    else:
                        # Non-diagnosis fields: check if field is NULL/empty or "-999"
                        optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                        
                        # Build WITH clause to include the node variable if needed
                        # For fields on sample node (sa), we still need study paths to match summary
                        if node_alias == "sa":
                            # Field is on sample node itself, but we need study paths for consistency with summary
                            # Note: This with_clause is not used for node_alias == "sa" (separate query is built)
                            with_clause = f"sa.sample_id as sample_id, p, {node_field} as field_value"
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
                            invalid_list_filter = build_invalid_value_list_filter("specimen_molecular_analyte_type")
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT sf.library_source_molecule) as molecule_values
                WHERE size([val IN molecule_values WHERE val IS NOT NULL 
                             AND {invalid_list_filter}]) = 0
                RETURN count(*) as missing
                """.strip()
                        elif node_alias == "sf" and not base_where_clause:
                            # Skip missing query for combined query fields (handled by combined query)
                            if field in ["library_source_material", "library_strategy", "library_selection_method"]:
                                # Missing count is already included in combined query
                                missing_cypher = None
                            else:
                                # Optimized missing count for other sequencing_file fields
                                # Missing: samples with study path that either:
                                # 1. Don't have any sequencing_file, OR
                                # 2. Have sequencing_file(s) but all have null/invalid values (based on null_mappings)
                                # Performance: Collect only field values (strings), not nodes - this is more efficient
                                # OPTIMIZATION: Collect DISTINCT field values per (sample_id, study_id) pair
                                # Then filter invalid values in WHERE clause - avoids scanning all sequencing_file records
                                invalid_list_filter = build_invalid_value_list_filter(field)
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WHERE size([val IN field_values WHERE val IS NOT NULL 
                             AND {invalid_list_filter}]) = 0
                RETURN count(*) as missing
                """.strip()
                        elif node_alias == "pf" and not base_where_clause:
                            # Skip missing query for preservation_method (handled by combined query)
                            if field == "preservation_method":
                                # Missing count is already included in combined query
                                missing_cypher = None
                            else:
                                # Optimized missing count for other pathology_file fields
                                # Missing: samples with study path that either:
                                # 1. Don't have any pathology_file, OR
                                # 2. Have pathology_file(s) but all have null/invalid values (based on null_mappings)
                                # IMPORTANT: When OPTIONAL MATCH doesn't find a pathology_file, pf is NULL
                                # collect(DISTINCT pf.fixation_embedding_method) on NULL returns [null]
                                # So samples without pathology_file will have field_values = [null] and size([val IN [null] WHERE val IS NOT NULL ...]) = 0, counted as missing ✅
                                # Counts unique (sample_id + study_id) pairs (consistent with values query)
                                invalid_list_filter = build_invalid_value_list_filter(field)
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WHERE size([val IN field_values WHERE val IS NOT NULL 
                             AND {invalid_list_filter}]) = 0
                RETURN count(*) as missing
                """.strip()
                        elif node_alias == "d" and not base_where_clause:
                            # Optimized missing count for diagnosis fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any diagnosis, OR
                            # 2. Have diagnosis(es) but all have null/invalid values (based on null_mappings)
                            # Note: When OPTIONAL MATCH doesn't find a diagnosis, d is NULL
                            # collect(DISTINCT d.disease_phase) on NULL returns empty list []
                            # So samples without diagnoses will have field_values = [] and size([]) = 0, counted as missing ✅
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as all_values
                WITH sample_id, study_id,
                     [val IN all_values WHERE val IS NOT NULL] as non_null_values
                WHERE size(non_null_values) = 0 
                   OR size([val IN non_null_values WHERE {invalid_list_filter}]) = 0
                RETURN count(*) as missing
                """.strip()
                        else:
                            # For other fields, build missing_where and missing_cypher
                            # IMPORTANT: All fields must check for study paths to match values and total queries
                            # Note: Study path check is now done via pattern comprehension in the query itself
                            missing_where = "(field_value IS NULL OR toString(field_value) = '' OR trim(toString(field_value)) = '' OR toString(field_value) = '-999' OR trim(toString(field_value)) = '-999')"
                            
                            # For fields on sample node (sa), ensure the query structure matches the total query
                            if node_alias == "sa":
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                  AND ({node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '' OR toString({node_field}) = '-999' OR trim(toString({node_field})) = '-999')
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id
                RETURN count(*) as missing
                """.strip()
                            else:
                                # For fields on related nodes, include pattern comprehension for study paths
                                # Add pattern comprehension to with_clause
                                with_clause_updated = f"{with_clause}, size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1, size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2"
                                
                                # Update missing_where to include study path check
                                missing_where_updated = f"has_study1 > 0 OR has_study2 > 0 AND {missing_where}"
                                
                                missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                WITH {with_clause_updated}
                WHERE {missing_where_updated}
                RETURN count(DISTINCT sample_id) as missing
                """.strip()
                else:
                    # Participant fields
                    missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
                WITH sa, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
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
                WITH sa, d, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
                {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause else ''}
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
                        # Add pattern comprehension for study paths
                        with_vars.append("size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1")
                        with_vars.append("size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2")
                        
                        with_clause = f"WITH {', '.join(with_vars)}\n                " if with_vars else ""
                        
                        # Add study path check to WHERE clause
                        study_where = "has_study1 > 0 OR has_study2 > 0"
                        if missing_where_clause:
                            missing_where_clause_updated = f"WHERE {study_where} AND {missing_where_clause.replace('WHERE ', '')}"
                        else:
                            missing_where_clause_updated = f"WHERE {study_where}"
                        
                        missing_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
                {optional_matches_str}
                {with_clause}{missing_where_clause_updated}
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
                WITH sa, p,
                     size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                     size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
                WHERE has_study1 > 0 OR has_study2 > 0
                {missing_where_clause.replace('WHERE ', 'AND ') if missing_where_clause.startswith('WHERE ') else missing_where_clause}
                WITH DISTINCT sa
                RETURN count(DISTINCT sa) as missing
                """.strip()
            
            # Execute total and missing queries (skip if using combined query)
            if total_cypher is not None:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                total = total_records[0].get("total", 0) if total_records else 0
            # else: total already extracted from combined query
            
            if missing_cypher is not None:
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                missing = missing_records[0].get("missing", 0) if missing_records else 0
            # else: missing already extracted from combined query
        
        # Verify: total should equal sum of values + missing
        # IMPORTANT: Skip adjustment for combined queries (library_source_material, library_strategy, preservation_method)
        # because missing count comes directly from the database query and is correct
        # IMPORTANT: For fields where samples can have multiple values (e.g., library_strategy),
        # the formula Total = Values sum + Missing doesn't hold because:
        # - Total = unique (sample_id, study_id) pairs
        # - Values sum = sum of all (sample_id, study_id, value) combinations
        # - Missing = unique (sample_id, study_id) pairs with no valid values
        # So we keep the original missing count from the database query
        if not is_combined_query:
            values_sum = sum(item["count"] for item in counts)
            if total != values_sum + missing:
                # Fields where samples can have multiple values per (sample_id, study_id) pair
                multi_value_fields = {"library_strategy", "library_selection_method", "anatomical_sites"}
                if field in multi_value_fields:
                    # For multi-value fields, Total can be < Values sum + Missing
                    # This is expected when samples have multiple values
                    logger.debug(
                        "Total count relationship for multi-value field",
                        field=field,
                        total=total,
                        values_sum=values_sum,
                        missing=missing,
                        difference=total - (values_sum + missing),
                        note="For multi-value fields, Total = unique samples, Values sum = sum of (sample, value) combinations"
                    )
                else:
                    # For single-value fields, log warning but don't adjust
                    logger.warning(
                        "Total count mismatch for field",
                        field=field,
                        total=total,
                        values_sum=values_sum,
                        missing=missing,
                        difference=total - (values_sum + missing),
                        values_count=len(counts),
                        note="Keeping original missing count from database query"
                    )
                # Do NOT adjust missing count - keep the original from database query
                # The missing count is correct and should not be modified
        else:
            # For combined queries, just log the verification without adjusting
            values_sum = sum(item["count"] for item in counts)
            if total != values_sum + missing:
                logger.warning(
                    "Total count mismatch for combined query (should not happen)",
                    field=field,
                    total=total,
                    values_sum=values_sum,
                    missing=missing,
                    difference=total - (values_sum + missing),
                    values_count=len(counts),
                    note="Missing count comes from database query and should be correct"
                )
        
        logger.debug(
            "Completed sample count by field",
            field=field,
            results_count=len(counts),
            total=total,
            missing=missing,
            values_sum=sum(item["count"] for item in counts)
        )
        
        # Per SAMPLE_ENDPOINT_RULES rule 2: counts are by (sample_id, study_id) per value.
        # One (sample_id, study_id) can contribute to multiple value buckets, so
        # sum(value counts) + missing may be greater than total. This is expected.
        return {
            "total": total,
            "missing": missing,
            "values": counts  # counts already has format [{"value": ..., "count": ...}]
        }
    
    # REMOVED: _count_samples_by_race and _count_samples_by_ethnicity methods
    # These are not needed because sex, race, and ethnicity are not valid sample count fields.
    # The validation at line 1787-1791 rejects these fields before these methods can be called.
    
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
        # Use multi-hop traversal for study paths
        if not where_clause:
            total_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
            RETURN count(*) as total
            """.strip()
        else:
            total_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, d, st
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            WITH DISTINCT sa
            RETURN count(DISTINCT sa) as total
            """.strip()
        
        # Query 2: Get count of samples with no valid diagnoses (missing)
        # Only count samples with a study path (matching summary endpoint)
        # Missing = samples with no diagnoses OR ALL diagnoses are invalid
        # IMPORTANT: Check ALL diagnoses, not just the first one
        # Use multi-hop traversal for study paths
        if not where_clause:
            missing_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(d) as diagnoses
            WITH sample_id, study_id,
                 [d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ] as diagnosis_values
            WITH sample_id, study_id,
                 [val IN diagnosis_values WHERE val IS NOT NULL 
                  AND toString(val) <> '' 
                  AND trim(toString(val)) <> ''] as valid_values
            WHERE size(valid_values) = 0
            RETURN count(*) as missing
            """.strip()
        else:
            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, d, st
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(d) as diagnoses
            WITH sample_id, study_id,
                 [d IN diagnoses WHERE d IS NOT NULL | 
                   CASE 
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' 
                          AND d.diagnosis_comment IS NOT NULL 
                          AND trim(toString(d.diagnosis_comment)) <> ''
                     THEN d.diagnosis_comment
                     WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                     THEN null
                     ELSE d.diagnosis
                   END
                 ] as diagnosis_values
            WITH sample_id, study_id,
                 [val IN diagnosis_values WHERE val IS NOT NULL 
                  AND toString(val) <> '' 
                  AND trim(toString(val)) <> ''] as valid_values
            WHERE size(valid_values) = 0
            RETURN count(*) as missing
            """.strip()
        
        # Query 3: Count by diagnosis values
        # Check ALL diagnoses of each sample (not just first)
        # d.diagnosis can be a STRING or LIST - handle both
        # Multiple diagnosis nodes can link to one sample, so each contributes values
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(sa:sample)
        # If diagnosis is "see diagnosis_comment", use diagnosis_comment as the value
        # Filter out "see diagnosis_comment" if diagnosis_comment is NULL or empty
        # Use multi-hop traversal for study paths
        if not where_clause:
            values_cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(d) as diagnoses
            // UNWIND all diagnoses to count each one
            UNWIND diagnoses AS diag_node
            WITH sample_id, study_id, diag_node
            WHERE diag_node IS NOT NULL
            WITH sample_id, study_id,
                 CASE 
                   WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment' 
                        AND diag_node.diagnosis_comment IS NOT NULL 
                        AND trim(toString(diag_node.diagnosis_comment)) <> ''
                   THEN diag_node.diagnosis_comment
                   WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment'
                   THEN null
                   ELSE diag_node.diagnosis
                 END AS diagnosis_value
            WHERE diagnosis_value IS NOT NULL 
              AND toString(diagnosis_value) <> '' 
              AND trim(toString(diagnosis_value)) <> ''
            WITH DISTINCT sample_id, study_id, toString(diagnosis_value) as value
            RETURN value, count(*) as count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            values_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, p, d, st
            {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(d) as diagnoses
            // UNWIND all diagnoses to count each one
            UNWIND diagnoses AS diag_node
            WITH sample_id, study_id, diag_node
            WHERE diag_node IS NOT NULL
            WITH sample_id, study_id,
                 CASE 
                   WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment' 
                        AND diag_node.diagnosis_comment IS NOT NULL 
                        AND trim(toString(diag_node.diagnosis_comment)) <> ''
                   THEN diag_node.diagnosis_comment
                   WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment'
                   THEN null
                   ELSE diag_node.diagnosis
                 END AS diagnosis_value
            WHERE diagnosis_value IS NOT NULL 
              AND toString(diagnosis_value) <> '' 
              AND trim(toString(diagnosis_value)) <> ''
            WITH DISTINCT sample_id, study_id, toString(diagnosis_value) as value
            RETURN value, count(*) as count
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

    async def _count_samples_by_diagnosis_category(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct (sample, study) combinations by harmonized diagnosis_category.

        Graph path: (d:diagnosis)-[:of_diagnosis]->(sa:sample),
        then (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)
                   -[:of_consent_group]->(st:study)
        OR         (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st:study)
        """
        logger.debug("Counting samples by diagnosis_category", filters=filters)

        params: Dict[str, Any] = {
            "harmonized_pvs": _HARMONIZED_PVS_SORTED,
            "harmonized_pvs_lower": _HARMONIZED_PVS_LOWER,
        }

        total_cypher = """
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa, [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids
UNWIND study_ids AS study_id
RETURN count(*) AS total
""".strip()

        missing_cypher = """
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa, [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids
UNWIND study_ids AS study_id
OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
WITH sa.sample_id AS sample_id, study_id, collect(d) AS diagnoses
WHERE size([
    d IN diagnoses WHERE d IS NOT NULL
    AND d.diagnosis_category IS NOT NULL
    AND toString(d.diagnosis_category) <> ''
    AND any(tok IN split(toString(d.diagnosis_category), ';')
            WHERE toLower(trim(tok)) IN $harmonized_pvs_lower)
]) = 0
RETURN count(*) AS missing
""".strip()

        values_cypher = """
MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)
WHERE d.diagnosis_category IS NOT NULL AND toString(d.diagnosis_category) <> ''
WITH sa, d
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, d, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, d, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa.sample_id AS sample_id,
     [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids,
     [tok IN split(toString(d.diagnosis_category), ';') WHERE trim(tok) <> ''] AS tokens
UNWIND study_ids AS study_id
UNWIND tokens AS raw_token
WITH sample_id, study_id, trim(raw_token) AS token
WITH sample_id, study_id, token,
     [pv IN $harmonized_pvs WHERE toLower(pv) = toLower(token)][0] AS matched_pv
WHERE matched_pv IS NOT NULL
WITH DISTINCT sample_id, study_id, matched_pv
RETURN matched_pv AS value, count(*) AS count
ORDER BY count DESC, value ASC
""".strip()

        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records: List[Dict[str, Any]] = []

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

                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break

                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning("Error in count_samples_by_diagnosis_category, retrying", error=str(e))
                else:
                    logger.error("Error in count_samples_by_diagnosis_category after retries",
                                 error=str(e), exc_info=True)
                    raise

        counts = [
            {"value": r.get("value"), "count": r.get("count", 0)}
            for r in values_records
        ]

        logger.info(
            "Completed sample count by diagnosis_category",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

