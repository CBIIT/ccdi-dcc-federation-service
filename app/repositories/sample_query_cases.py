"""
Query case implementations for SampleRepository.

This module contains the restructured query paths:
- Case 1: Sample-only filters
- Case 2: Sample + Study filters only
- Case 3: Has diagnosis/sequencing_file/pathology_file filters
"""

from typing import List, Dict, Any, Optional, Tuple, Union
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.models.dto import Sample
from app.core.field_mappings import (
    load_sample_enum,
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
    load_sequencing_file_enum,
)
from app.repositories.subject_diagnosis_cypher import (
    add_diagnosis_search_params,
    diagnosis_category_contains_predicate,
    diagnosis_category_exact_token_predicate,
    diagnosis_search_predicate,
)
from app.repositories.sample_helpers import SD_CAT_MARKER

logger = get_logger(__name__)

# Projection for diagnosis nodes in collect() results.
# Returns only the 7 properties used by _record_to_sample(), avoiding transferring
# full node objects (which carry all properties) across the driver connection.
_DIAG_PROJ = (
    "{diagnosis: d.diagnosis, diagnosis_comment: d.diagnosis_comment, "
    "diagnosis_category: d.diagnosis_category, disease_phase: d.disease_phase, "
    "tumor_grade: d.tumor_grade, age_at_diagnosis: d.age_at_diagnosis, "
    "tumor_classification: d.tumor_classification}"
)


class SampleQueryCases:
    """Mixin class providing query case implementations for SampleRepository."""
    
    async def _get_samples_case1_sample_only(
        self,
        sample_filters: Dict[str, Any],
        offset: int,
        limit: int,
        base_url: Optional[str],
        return_total: bool
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Case 1: Sample-only filters query path.
        
        Query structure:
        1. MATCH samples with sample filters
        2. ORDER BY sample_id
        3. SKIP/LIMIT (early pagination at sample level)
        4. OPTIONAL MATCH studies for paginated samples
        5. OPTIONAL MATCH other nodes (diagnosis, sequencing_file, pathology_file) - pick 1 random each
        6. OPTIONAL MATCH participant (after pagination)
        """
        # Build sample filter conditions
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        sample_where_conditions = ["sa.sample_id IS NOT NULL", "trim(toString(sa.sample_id)) <> ''"]
        
        # Process sample filters
        identifiers_early_filter = None
        if "identifiers" in sample_filters:
            identifiers_value = sample_filters["identifiers"]
            if identifiers_value is not None and str(identifiers_value).strip():
                if isinstance(identifiers_value, str) and "||" in identifiers_value:
                    identifiers_list = [i.strip() for i in identifiers_value.split("||")]
                    identifiers_list = [i for i in identifiers_list if i]
                    identifiers_value = identifiers_list if identifiers_list else None
                
                if identifiers_value:
                    param_counter += 1
                    id_param = f"param_{param_counter}"
                    params[id_param] = identifiers_value
                    if isinstance(identifiers_value, list):
                        identifiers_early_filter = f"sa.sample_id IN ${id_param}"
                    else:
                        identifiers_early_filter = f"sa.sample_id = ${id_param}"
                    # Add identifiers filter to WHERE conditions
                    sample_where_conditions.append(identifiers_early_filter)
        
        # Process tissue_type filter
        if "tissue_type" in sample_filters:
            tissue_value = sample_filters["tissue_type"]
            param_counter += 1
            tissue_param = f"param_{param_counter}"
            # Use helper function to validate
            with_conditions_temp = []
            if self._validate_tissue_type_filter(tissue_value, tissue_param, params, with_conditions_temp) is None:
                # Invalid tissue_type - return empty results
                logger.info("Case 1: Invalid tissue_type filter, returning empty results", tissue_type=tissue_value)
                if return_total:
                    return ([], 0)
                return []
            # Add the condition from with_conditions_temp to sample_where_conditions
            if with_conditions_temp:
                sample_where_conditions.append(with_conditions_temp[0])
        
        # Process anatomical_sites filter
        if "anatomical_sites" in sample_filters:
            anatomical_sites_value = sample_filters["anatomical_sites"]
            param_counter += 1
            anatomical_sites_param = f"param_{param_counter}"
            
            # Handle list or string (API layer handles || splitting)
            if isinstance(anatomical_sites_value, list):
                # Multiple values - build OR conditions
                or_conditions = []
                for idx, val in enumerate(anatomical_sites_value):
                    val_param = f"{anatomical_sites_param}_{idx}"
                    params[val_param] = val.strip() if isinstance(val, str) else val
                    or_conditions.append(f"""(
                        ${val_param} = sa.anatomic_site OR
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${val_param})) THEN true ELSE found END
                        ) = true
                    )""")
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
            else:
                # Single value - handle exact match and semicolon-separated string
                params[anatomical_sites_param] = anatomical_sites_value.strip() if isinstance(anatomical_sites_value, str) else anatomical_sites_value
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND (
                    ${anatomical_sites_param} = sa.anatomic_site OR
                    reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                      CASE WHEN trim(tok) = trim(toString(${anatomical_sites_param})) THEN true ELSE found END
                    ) = true
                )"""
            sample_where_conditions.append(anatomical_sites_condition)
        
        # Process age_at_collection filter
        if "age_at_collection" in sample_filters:
            age_value = sample_filters["age_at_collection"]
            param_counter += 1
            age_param = f"param_{param_counter}"
            try:
                params[age_param] = int(age_value) if age_value is not None else None
            except (ValueError, TypeError):
                params[age_param] = age_value
            sample_where_conditions.append(f"toInteger(sa.participant_age_at_collection) = ${age_param}")
        
        # Build WHERE clause for sample filters
        sample_where_str = " AND ".join(sample_where_conditions)
        
        # Build count query if return_total
        total_count = None
        if return_total:
            cypher_count = f"""
            MATCH (sa:sample)
            WHERE {sample_where_str}
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, collect(DISTINCT st2.study_id) AS st2_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, st2_list + collect(DISTINCT st1.study_id) AS combined
            UNWIND combined AS study_id
            WITH DISTINCT sa.sample_id AS sample_id, study_id
            WHERE study_id IS NOT NULL
            RETURN count(*) as total_count
            """.strip()
            
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count = recs[0].get("total_count", 0) if recs else 0
            except Exception as e:
                logger.warning("Case 1 count query failed", error=str(e), exc_info=True)
                total_count = 0
        
        # Build query: paginate at (sample_id, study_id) pair level to match count query
        cypher = f"""
        MATCH (sa:sample)
        WHERE {sample_where_str}
        // Collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, collect(DISTINCT st2.study_id) AS st2_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, st2_list_raw, collect(DISTINCT st1.study_id) AS st1_list_raw
        WITH sa,
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, sid
        WHERE sid IS NOT NULL
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa, st
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        
        // OPTIONAL MATCH other nodes - pick 1 random each
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        
        WITH sa, st,
             [d IN collect(DISTINCT d) WHERE d IS NOT NULL | {_DIAG_PROJ}] AS diagnoses,
             head(collect(DISTINCT pf)) AS pf,
             head(collect(DISTINCT sf)) AS sf
        
        // After pagination: OPTIONAL MATCH participant
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, sf, pf, diagnoses
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.debug("Case 1 query")
        
        result = await self.session.run(cypher, params)
        records = []
        async for record in result:
            records.append(dict(record))
        await result.consume()
        
        samples = []
        for record in records:
            try:
                sa = dict(record["sa"]) if record.get("sa") else None
                p = dict(record["p"]) if record.get("p") else None
                st = dict(record["st"]) if record.get("st") else None
                sf = dict(record["sf"]) if record.get("sf") else None
                pf = dict(record["pf"]) if record.get("pf") else None
                diagnoses = [dict(d) for d in record["diagnoses"] if d is not None] if record.get("diagnoses") else None
                if sa:
                    sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                    if sample_obj:
                        samples.append(sample_obj)
            except Exception as e:
                logger.warning("Error converting sample record in Case 1: %s", e, exc_info=True)
                continue
        
        if return_total:
            return (samples, total_count if total_count is not None else len(samples))
        return samples

    async def _get_samples_case2_sample_study(
        self,
        filters: Dict[str, Any],
        offset: int,
        limit: int,
        base_url: Optional[str],
        return_total: bool
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Case 2: Sample + Study filters only query path.
        
        Query structure:
        1. MATCH samples with sample filters
        2. OPTIONAL MATCH studies with study filters (depositions)
        3. Filter: must have at least one study
        4. ORDER BY (sample_id, study_id)
        5. SKIP/LIMIT (early pagination at sample-study pair level)
        6. OPTIONAL MATCH other nodes (diagnosis, sequencing_file, pathology_file) - pick 1 random each
        7. OPTIONAL MATCH participant (after pagination)
        """
        # Delegate to existing early pagination logic
        result = await self._get_samples_early_pagination_with_filters(filters, offset, limit, base_url, return_total)
        if result is None:
            # Early pagination path doesn't support these filters, fall through to standard query
            return None
        # Return empty list if no results, otherwise return the result (which may be a tuple if return_total)
        if isinstance(result, tuple):
            return result
        return result if result else []

    async def _get_samples_case3_with_node_filters(
        self,
        filters: Dict[str, Any],
        categorized: Dict[str, Dict[str, Any]],
        offset: int,
        limit: int,
        base_url: Optional[str],
        return_total: bool
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Case 3: Has diagnosis/sequencing_file/pathology_file filters query path.
        
        Query structure:
        1. MATCH samples with sample filters
        2. OPTIONAL MATCH studies with study filters (depositions)
        3. OPTIONAL MATCH diagnosis with diagnosis filters (WHERE clause in OPTIONAL MATCH)
        4. OPTIONAL MATCH sequencing_file with sequencing_file filters (WHERE clause in OPTIONAL MATCH)
        5. OPTIONAL MATCH pathology_file with pathology_file filters (WHERE clause in OPTIONAL MATCH)
        6. Collect all matching nodes per type
        7. Filter: at least one diagnosis matches (if diagnosis filters present)
        8. Filter: at least one sequencing_file matches (if sequencing_file filters present)
        9. Filter: at least one pathology_file matches (if pathology_file filters present)
        10. ORDER BY (sample_id, study_id)
        11. SKIP/LIMIT (early pagination at sample-study pair level)
        12. Pick 1 matching node per type (head() from filtered collections)
        13. OPTIONAL MATCH participant (after pagination)
        """
        # Build parameters and filter conditions
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        
        # Step 1: Build sample filter conditions
        sample_where_conditions = ["sa.sample_id IS NOT NULL", "trim(toString(sa.sample_id)) <> ''"]
        
        # Process identifiers filter
        identifiers_early_filter = None
        if "identifiers" in categorized["sample"]:
            identifiers_value = categorized["sample"]["identifiers"]
            if identifiers_value is not None and str(identifiers_value).strip():
                if isinstance(identifiers_value, str) and "||" in identifiers_value:
                    identifiers_list = [i.strip() for i in identifiers_value.split("||")]
                    identifiers_list = [i for i in identifiers_list if i]
                    identifiers_value = identifiers_list if identifiers_list else None
                
                if identifiers_value:
                    param_counter += 1
                    id_param = f"param_{param_counter}"
                    params[id_param] = identifiers_value
                    if isinstance(identifiers_value, list):
                        identifiers_early_filter = f"sa.sample_id IN ${id_param}"
                    else:
                        identifiers_early_filter = f"sa.sample_id = ${id_param}"
                    sample_where_conditions.append(identifiers_early_filter)
        
        # Process tissue_type filter
        if "tissue_type" in categorized["sample"]:
            tissue_value = categorized["sample"]["tissue_type"]
            param_counter += 1
            tissue_param = f"param_{param_counter}"
            with_conditions_temp = []
            if self._validate_tissue_type_filter(tissue_value, tissue_param, params, with_conditions_temp) is None:
                logger.info("Case 3: Invalid tissue_type filter, returning empty results", tissue_type=tissue_value)
                if return_total:
                    return ([], 0)
                return []
            if with_conditions_temp:
                sample_where_conditions.append(with_conditions_temp[0])
        
        # Process anatomical_sites filter
        if "anatomical_sites" in categorized["sample"]:
            anatomical_sites_value = categorized["sample"]["anatomical_sites"]
            param_counter += 1
            anatomical_sites_param = f"param_{param_counter}"
            
            if isinstance(anatomical_sites_value, list):
                or_conditions = []
                for idx, val in enumerate(anatomical_sites_value):
                    val_param = f"{anatomical_sites_param}_{idx}"
                    params[val_param] = val.strip() if isinstance(val, str) else val
                    or_conditions.append(f"""(
                        ${val_param} = sa.anatomic_site OR
                        reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                          CASE WHEN trim(tok) = trim(toString(${val_param})) THEN true ELSE found END
                        ) = true
                    )""")
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
            else:
                params[anatomical_sites_param] = anatomical_sites_value.strip() if isinstance(anatomical_sites_value, str) else anatomical_sites_value
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND (
                    ${anatomical_sites_param} = sa.anatomic_site OR
                    reduce(found = false, tok IN SPLIT(toString(sa.anatomic_site), ';') | 
                      CASE WHEN trim(tok) = trim(toString(${anatomical_sites_param})) THEN true ELSE found END
                    ) = true
                )"""
            sample_where_conditions.append(anatomical_sites_condition)
        
        # Process age_at_collection filter
        if "age_at_collection" in categorized["sample"]:
            age_value = categorized["sample"]["age_at_collection"]
            param_counter += 1
            age_param = f"param_{param_counter}"
            try:
                params[age_param] = int(age_value) if age_value is not None else None
            except (ValueError, TypeError):
                params[age_param] = age_value
            sample_where_conditions.append(f"toInteger(sa.participant_age_at_collection) = ${age_param}")
        
        sample_where_str = " AND ".join(sample_where_conditions)
        
        # Step 2: Build study filter (depositions)
        depositions_study_filter = ""
        if "depositions" in categorized["study"]:
            dep_value = categorized["study"]["depositions"]
            if dep_value is not None and str(dep_value).strip():
                if isinstance(dep_value, str) and "||" in dep_value:
                    dep_list = [d.strip() for d in dep_value.split("||") if d.strip()]
                    if dep_list:
                        param_counter += 1
                        dep_param = f"param_{param_counter}"
                        params[dep_param] = dep_list if len(dep_list) > 1 else dep_list[0]
                        depositions_study_filter = f" AND st.study_id IN ${dep_param}" if len(dep_list) > 1 else f" AND st.study_id = ${dep_param}"
                else:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = dep_value
                    depositions_study_filter = f" AND st.study_id = ${dep_param}"
        
        # Step 3: Build diagnosis filters for OPTIONAL MATCH WHERE clause
        has_diagnosis_filters = bool(
            set(categorized["diagnosis"].keys()) - {SD_CAT_MARKER}
        )
        diagnosis_search_term = categorized["diagnosis"].get("_diagnosis_search")
        needs_diagnosis_search = diagnosis_search_term is not None
        use_substring_diagnosis_category = bool(
            categorized["diagnosis"].get(SD_CAT_MARKER)
        )
        
        # Handle diagnosis search - needs special collection filter
        diagnosis_search_filter = None
        disease_phase_collection_filter = None
        combined_diagnosis_condition = None
        if needs_diagnosis_search:
            add_diagnosis_search_params(params, diagnosis_search_term)
            diagnosis_search_filter = diagnosis_search_predicate("d")
        
        if has_diagnosis_filters:
            diagnosis_conditions = []
            
            for field, value in categorized["diagnosis"].items():
                if field == "_diagnosis_search":
                    # Already handled above
                    continue
                if field == SD_CAT_MARKER:
                    continue

                param_counter += 1
                param_name = f"param_{param_counter}"
                
                if field == "disease_phase":
                    if is_null_mapped_value("disease_phase", value):
                        params[param_name] = value
                        if needs_diagnosis_search:
                            # For diagnosis search, combine with collection filter
                            disease_phase_collection_filter = f"d.disease_phase = ${param_name}"
                        else:
                            diagnosis_conditions.append(f"d.disease_phase = ${param_name}")
                    else:
                        reverse_mapped = reverse_map_field_value("disease_phase", value)
                        if isinstance(reverse_mapped, list):
                            params[param_name] = reverse_mapped
                            if needs_diagnosis_search:
                                # For diagnosis search, combine with collection filter
                                disease_phase_collection_filter = f"d.disease_phase IN ${param_name}"
                            else:
                                diagnosis_conditions.append(f"d.disease_phase IN ${param_name}")
                        else:
                            params[param_name] = reverse_mapped
                            if needs_diagnosis_search:
                                # For diagnosis search, combine with collection filter
                                disease_phase_collection_filter = f"d.disease_phase = ${param_name}"
                            else:
                                diagnosis_conditions.append(f"d.disease_phase = ${param_name}")
                elif field == "tumor_classification":
                    if is_null_mapped_value("tumor_classification", value):
                        diagnosis_conditions.append("false")
                    else:
                        reverse_mapped = reverse_map_field_value("tumor_classification", value)
                        params[param_name] = reverse_mapped if reverse_mapped else value
                        diagnosis_conditions.append(f"d.tumor_classification = ${param_name}")
                elif field == "tumor_grade":
                    params[param_name] = value
                    diagnosis_conditions.append(f"d.tumor_grade = ${param_name}")
                elif field == "tumor_tissue_morphology":
                    params[param_name] = value
                    diagnosis_conditions.append(f"d.tumor_tissue_morphology = ${param_name}")
                elif field == "age_at_diagnosis":
                    try:
                        params[param_name] = int(value) if value is not None else None
                    except (ValueError, TypeError):
                        params[param_name] = value
                    diagnosis_conditions.append(f"toInteger(d.age_at_diagnosis) = ${param_name}")
                elif field == "diagnosis":
                    params[param_name] = value
                    diagnosis_conditions.append(f"""(d.diagnosis = ${param_name} OR
                        (toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment' AND
                         d.diagnosis_comment IS NOT NULL AND
                         trim(toString(d.diagnosis_comment)) = ${param_name}))""")
                elif field == "diagnosis_category":
                    if use_substring_diagnosis_category and isinstance(value, str) and value.strip():
                        params["diag_category_contains_term"] = value.strip()
                        diagnosis_conditions.append(diagnosis_category_contains_predicate("d"))
                    else:
                        params["diag_category_filter"] = value
                        diagnosis_conditions.append(diagnosis_category_exact_token_predicate("d"))
            
            if diagnosis_conditions:
                combined_diagnosis_condition = " AND ".join([f"({cond})" for cond in diagnosis_conditions])

        # Step 3.5: Build pre-UNWIND diagnosis block.
        # Moves diagnosis filter before study collection so UNWIND only expands samples that
        # already have a matching diagnosis, avoiding the full samples × studies cross product.
        pre_unwind_diagnosis_block = None
        if has_diagnosis_filters and not needs_diagnosis_search and combined_diagnosis_condition:
            pre_unwind_diagnosis_block = (
                f"MATCH (d:diagnosis)-[:of_diagnosis]->(sa)\n"
                f"        WHERE {combined_diagnosis_condition}\n"
                f"        WITH sa, collect(DISTINCT d) AS all_diagnoses"
            )
        elif needs_diagnosis_search:
            parts = [f"({diagnosis_search_filter})"]
            if disease_phase_collection_filter:
                parts.append(f"({disease_phase_collection_filter})")
            if combined_diagnosis_condition:
                parts.append(f"({combined_diagnosis_condition})")
            _pre_combined = "d IS NOT NULL AND " + " AND ".join(parts)
            pre_unwind_diagnosis_block = (
                f"OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)\n"
                f"        WITH sa, [d IN collect(DISTINCT d) WHERE {_pre_combined}] AS all_diagnoses\n"
                f"        WHERE size(all_diagnoses) > 0"
            )

        # Step 4: Build sequencing_file filters for OPTIONAL MATCH WHERE clause
        sf_optional_match_where = None
        combined_sf_condition = None
        has_sf_filters = len(categorized["sequencing_file"]) > 0
        if has_sf_filters:
            sf_conditions = []
            
            for field, value in categorized["sequencing_file"].items():
                param_counter += 1
                param_name = f"param_{param_counter}"
                
                if field == "library_selection_method":
                    # Check if value is a database-only value
                    if is_database_only_value("library_selection_method", value):
                        logger.info("Case 3: Invalid library_selection_method value (database-only), returning empty results", library_selection_method=value)
                        if return_total:
                            return ([], 0)
                        return []
                    # Use reverse mapping helper if available
                    reverse_mapped = self._reverse_map_library_selection_method_static(value)
                    if reverse_mapped:
                        params[param_name] = reverse_mapped
                        sf_conditions.append(f"sf.library_selection = ${param_name}")
                    else:
                        params[param_name] = value
                        sf_conditions.append(f"sf.library_selection = ${param_name}")
                elif field == "library_strategy":
                    # Check if value is a database-only value (e.g., "Archer Fusion")
                    if is_database_only_value("library_strategy", value):
                        logger.info("Case 3: Invalid library_strategy value (database-only), returning empty results", library_strategy=value)
                        if return_total:
                            return ([], 0)
                        return []
                    # Handle reverse mapping (e.g., "Other" -> "Archer Fusion")
                    reverse_mapped = reverse_map_field_value("library_strategy", value)
                    if reverse_mapped and reverse_mapped != value:
                        # Has mapping - need to match both the mapped value and the original value
                        param_counter += 1
                        param_name2 = f"param_{param_counter}"
                        params[param_name] = reverse_mapped if isinstance(reverse_mapped, str) else reverse_mapped[0]
                        params[param_name2] = value
                        sf_conditions.append(f"(sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name2})")
                    else:
                        # No mapping or mapping is same as value - use value directly
                        params[param_name] = value
                        sf_conditions.append(f"sf.library_strategy = ${param_name}")
                elif field == "library_source_material":
                    # Check if value is in null_mappings (e.g., "Other")
                    if is_null_mapped_value("library_source_material", value):
                        logger.info("Case 3: Invalid library_source_material filter (null-mapped), returning empty results", library_source_material=value)
                        if return_total:
                            return ([], 0)
                        return []
                    # Apply reverse mapping for the filter value to get DB value
                    reverse_mapped = reverse_map_field_value("library_source_material", value)
                    if isinstance(reverse_mapped, list):
                        params[param_name] = reverse_mapped
                        sf_conditions.append(f"sf.library_source_material IN ${param_name}")
                    else:
                        params[param_name] = reverse_mapped if reverse_mapped else value
                        sf_conditions.append(f"sf.library_source_material = ${param_name}")
                elif field == "specimen_molecular_analyte_type":
                    # Map API value to DB value(s)
                    reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                    if isinstance(reverse_mapped, list):
                        db_values_str = ", ".join([f"'{v}'" for v in reverse_mapped])
                        sf_conditions.append(f"sf.library_source_molecule IN [{db_values_str}]")
                    else:
                        params[param_name] = reverse_mapped if reverse_mapped else value
                        sf_conditions.append(f"sf.library_source_molecule = ${param_name}")
            
            if sf_conditions:
                combined_sf_condition = " AND ".join([f"({cond})" for cond in sf_conditions])
                sf_optional_match_where = f"WHERE sf IS NOT NULL AND ({combined_sf_condition})"
        
        # Step 5: Build pathology_file filters for OPTIONAL MATCH WHERE clause
        pf_optional_match_where = None
        has_pf_filters = len(categorized["pathology_file"]) > 0
        if has_pf_filters:
            if "preservation_method" in categorized["pathology_file"]:
                preservation_value = categorized["pathology_file"]["preservation_method"]
                param_counter += 1
                pf_param = f"param_{param_counter}"
                params[pf_param] = preservation_value
                pf_optional_match_where = f"WHERE pf.fixation_embedding_method = ${pf_param}"

        # Step 5.5: pre-UNWIND sf block — when combined_sf_condition is set, matching sf nodes
        # are collected per sample before study collection, avoiding the full samples × studies
        # cross product. combined_sf_condition is the raw WHERE condition; MATCH guarantees sf
        # is not null, so no null-check prefix is needed.

        # Build post-UNWIND OPTIONAL MATCH clauses.
        # When pre_unwind_diagnosis_block is set, diagnosis is handled before study collection;
        # when combined_sf_condition is set, sf is handled before study collection.
        optional_matches = []

        if pre_unwind_diagnosis_block is None:
            # No pre-UNWIND block: add d OPTIONAL MATCH here for data enrichment
            optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")

        if pf_optional_match_where:
            optional_matches.append(f"OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)\n        {pf_optional_match_where}")
        else:
            optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")

        if combined_sf_condition is None:
            if sf_optional_match_where:
                optional_matches.append(f"OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)\n        {sf_optional_match_where}")
            else:
                optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")

        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        
        # Build WITH clause for post-UNWIND collection.
        with_collects = ["st"]
        if pre_unwind_diagnosis_block is not None:
            # all_diagnoses was collected pre-UNWIND and carried through; reference directly.
            with_collects.append("all_diagnoses")
        elif has_diagnosis_filters or needs_diagnosis_search:
            if needs_diagnosis_search:
                combined_parts = [f"({diagnosis_search_filter})"]
                if disease_phase_collection_filter:
                    combined_parts.append(f"({disease_phase_collection_filter})")
                if combined_diagnosis_condition:
                    combined_parts.append(f"({combined_diagnosis_condition})")
                combined_filter = "d IS NOT NULL AND " + " AND ".join(combined_parts)
                with_collects.append(f"[d IN collect(DISTINCT d) WHERE {combined_filter}] AS all_diagnoses")
            else:
                with_collects.append("collect(DISTINCT d) AS all_diagnoses")
        else:
            with_collects.append(f"[d IN collect(DISTINCT d) WHERE d IS NOT NULL | {_DIAG_PROJ}] AS diagnoses")

        if has_pf_filters:
            with_collects.append("collect(DISTINCT pf) AS all_pfs")
        else:
            with_collects.append("head(collect(DISTINCT pf)) AS pf")

        if combined_sf_condition is not None:
            # all_sf was collected pre-UNWIND and carried through; reference directly.
            with_collects.append("all_sf")
        elif has_sf_filters:
            with_collects.append("collect(DISTINCT sf) AS all_sfs")
        else:
            with_collects.append("head(collect(DISTINCT sf)) AS sf")

        with_clause = f"WITH sa, {', '.join(with_collects)}"
        
        # Build WHERE clause to filter for required matches.
        # When pre_unwind_diagnosis_block is set, the diagnosis size check is enforced pre-UNWIND.
        # When combined_sf_condition is set, the sf size check is enforced pre-UNWIND.
        where_conditions = []
        if pre_unwind_diagnosis_block is None and (has_diagnosis_filters or needs_diagnosis_search):
            where_conditions.append("size([d IN all_diagnoses WHERE d IS NOT NULL]) > 0")
        if has_pf_filters:
            where_conditions.append("size([pf IN all_pfs WHERE pf IS NOT NULL]) > 0")
        if combined_sf_condition is None and has_sf_filters:
            where_conditions.append("size([sf IN all_sfs WHERE sf IS NOT NULL]) > 0")

        where_clause = f"\n        WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Build count query if return_total
        total_count = None
        if return_total:
            has_pre_unwind = (pre_unwind_diagnosis_block is not None or combined_sf_condition is not None)
            if has_pre_unwind:
                # Optimized count: filter samples pre-UNWIND using all active pre-UNWIND blocks.
                # After WITH DISTINCT sa the pre-UNWIND vars are dropped; standard study collection follows.
                count_pre_unwind_blocks = []
                count_pre_unwind_vars: list[str] = []
                if pre_unwind_diagnosis_block is not None:
                    count_pre_unwind_blocks.append(pre_unwind_diagnosis_block)
                    count_pre_unwind_vars.append("all_diagnoses")
                if combined_sf_condition is not None:
                    carry = (", ".join(count_pre_unwind_vars) + ", ") if count_pre_unwind_vars else ""
                    count_pre_unwind_blocks.append(
                        f"MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)\n"
                        f"            WHERE {combined_sf_condition}\n"
                        f"            WITH sa, {carry}collect(DISTINCT sf) AS all_sf"
                    )
                count_pre_unwind_str = "\n            ".join(count_pre_unwind_blocks)

                count_with_collects = ["st"]
                if has_pf_filters:
                    count_with_collects.append("collect(DISTINCT pf) AS all_pfs")
                else:
                    count_with_collects.append("head(collect(DISTINCT pf)) AS pf")
                if combined_sf_condition is None and has_sf_filters:
                    count_with_collects.append("collect(DISTINCT sf) AS all_sfs")
                elif combined_sf_condition is None:
                    count_with_collects.append("head(collect(DISTINCT sf)) AS sf")
                count_with_clause = f"WITH sa, {', '.join(count_with_collects)}"

                count_where_conditions = []
                if has_pf_filters:
                    count_where_conditions.append("size([pf IN all_pfs WHERE pf IS NOT NULL]) > 0")
                if combined_sf_condition is None and has_sf_filters:
                    count_where_conditions.append("size([sf IN all_sfs WHERE sf IS NOT NULL]) > 0")
                count_where_clause = f"\n            WHERE {' AND '.join(count_where_conditions)}" if count_where_conditions else ""

                cypher_count = f"""
            MATCH (sa:sample)
            WHERE {sample_where_str}
            {count_pre_unwind_str}
            WITH DISTINCT sa
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid{depositions_study_filter}
            {optional_matches_str}
            {count_with_clause}{count_where_clause}
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) AS total_count
            """.strip()
            else:
                cypher_count = f"""
            MATCH (sa:sample)
            WHERE {sample_where_str}
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid{depositions_study_filter}
            {optional_matches_str}
            {with_clause}{where_clause}
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) AS total_count
            """.strip()
            
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count = recs[0].get("total_count", 0) if recs else 0
                logger.info(
                    "Case 3 count query result",
                    total_count=total_count,
                    filters=filters
                )
            except Exception as e:
                logger.warning("Case 3 count query failed", error=str(e), exc_info=True)
                total_count = 0
        
        # Build main query
        # After collecting and filtering, pick 1 node per type and paginate
        pick_clause_parts = []
        if has_diagnosis_filters or needs_diagnosis_search:
            pick_clause_parts.append(f"[d IN all_diagnoses WHERE d IS NOT NULL | {_DIAG_PROJ}] AS diagnoses")
        else:
            pick_clause_parts.append("diagnoses")
        
        if has_pf_filters:
            pick_clause_parts.append("head([pf IN all_pfs WHERE pf IS NOT NULL | pf]) AS pf")
        else:
            pick_clause_parts.append("pf")
        
        if combined_sf_condition is not None:
            pick_clause_parts.append("head([sf IN all_sf WHERE sf IS NOT NULL | sf]) AS sf")
        elif has_sf_filters:
            pick_clause_parts.append("head([sf IN all_sfs WHERE sf IS NOT NULL | sf]) AS sf")
        else:
            pick_clause_parts.append("sf")

        pick_clause = ", ".join(pick_clause_parts)

        # Build pre-UNWIND block sequence (diagnosis first, then sf) and the study collection
        # string that threads all pre-UNWIND vars through each WITH clause.
        pre_unwind_blocks: list[str] = []
        pre_unwind_vars: list[str] = []

        if pre_unwind_diagnosis_block is not None:
            pre_unwind_blocks.append(pre_unwind_diagnosis_block)
            pre_unwind_vars.append("all_diagnoses")

        if combined_sf_condition is not None:
            carry = (", ".join(pre_unwind_vars) + ", ") if pre_unwind_vars else ""
            pre_unwind_blocks.append(
                f"MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)\n"
                f"        WHERE {combined_sf_condition}\n"
                f"        WITH sa, {carry}collect(DISTINCT sf) AS all_sf"
            )
            pre_unwind_vars.append("all_sf")

        pre_unwind_block_str = "\n        " + "\n        ".join(pre_unwind_blocks) if pre_unwind_blocks else ""
        _carry = ", ".join(pre_unwind_vars) + ", " if pre_unwind_vars else ""
        study_collection_str = (
            "OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)\n"
            f"        WITH sa, {_carry}collect(DISTINCT st1.study_id) AS st1_list\n"
            "        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)\n"
            f"        WITH sa, {_carry}st1_list, collect(DISTINCT st2.study_id) AS st2_list\n"
            f"        WITH sa, {_carry}(st2_list + st1_list) AS combined"
        )

        cypher = f"""
        MATCH (sa:sample)
        WHERE {sample_where_str}{pre_unwind_block_str}
        {study_collection_str}
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        {optional_matches_str}
        {with_clause}{where_clause}
        WITH sa, st, {pick_clause}
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        // After pagination: OPTIONAL MATCH participant
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, sf, pf, diagnoses
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.debug("Case 3 query")
        
        result = await self.session.run(cypher, params)
        records = []
        async for record in result:
            records.append(dict(record))
        await result.consume()
        
        samples = []
        for record in records:
            try:
                sa = dict(record["sa"]) if record.get("sa") else None
                p = dict(record["p"]) if record.get("p") else None
                st = dict(record["st"]) if record.get("st") else None
                sf = dict(record["sf"]) if record.get("sf") else None
                pf = dict(record["pf"]) if record.get("pf") else None
                diagnoses = [dict(d) for d in record["diagnoses"] if d is not None] if record.get("diagnoses") else None
                if sa:
                    sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                    if sample_obj:
                        samples.append(sample_obj)
            except Exception as e:
                logger.warning("Error converting sample record in Case 3: %s", e, exc_info=True)
                continue
        
        if return_total:
            return (samples, total_count if total_count is not None else len(samples))
        return samples
