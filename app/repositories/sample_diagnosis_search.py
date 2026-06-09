"""
Sample repository diagnosis search methods.

This module contains methods for experimental diagnosis search functionality.
These methods are provided as a mixin class that can be inherited by SampleRepository.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from app.core.field_mappings import (
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value
)
from app.core.logging import get_logger
from app.models.dto import Sample
from app.repositories.sample_converters import node_to_dict
from app.repositories.subject_diagnosis_cypher import (
    diagnosis_category_contains_predicate,
    add_diagnosis_search_params,
    diagnosis_search_predicate,
)

logger = get_logger(__name__)


class SampleDiagnosisSearch:
    """Mixin class providing diagnosis search methods for SampleRepository."""
    
    async def _get_samples_by_diagnosis_search(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Internal reverse-query helper for diagnosis-centric sample filters.

        The live `/sample-diagnosis` list endpoint currently delegates through
        `get_samples()` / Case 3. This helper remains available for targeted
        diagnosis-search flows and for the matching summary implementation.
        """
        params = {"offset": offset, "limit": limit}
        param_counter = 0

        diagnosis_search_term = filters.get("_diagnosis_search")

        # Handle disease_phase filter if present - CRITICAL: Apply early to avoid cartesian product explosion
        # This prevents collecting all diagnoses then filtering, which creates (samples × studies × diagnoses) rows
        disease_phase_filter = ""
        if "disease_phase" in filters:
            disease_phase_value = filters["disease_phase"]
            # Validate disease_phase value
            if is_database_only_value("disease_phase", disease_phase_value) or is_null_mapped_value("disease_phase", disease_phase_value):
                logger.info("Invalid disease_phase value - returning empty results", value=disease_phase_value)
                # Check if this is the summary method (no return_total parameter)
                if "summary" in str(self.__class__.__name__) or not hasattr(self, '_get_samples_by_diagnosis_search'):
                    return {"counts": {"total": 0}}
                return [] if not return_total else ([], 0)
            
            # Apply reverse mapping for filtering (API value -> DB value(s))
            reverse_mapped = reverse_map_field_value("disease_phase", disease_phase_value)
            if isinstance(reverse_mapped, list):
                # Multiple DB values map to this API value - use IN clause
                param_counter += 1
                dp_param = f"param_{param_counter}"
                params[dp_param] = reverse_mapped
                disease_phase_filter = f" AND d.disease_phase IN ${dp_param}"
            else:
                param_counter += 1
                dp_param = f"param_{param_counter}"
                params[dp_param] = reverse_mapped
                disease_phase_filter = f" AND d.disease_phase = ${dp_param}"
        
        # Handle other diagnosis field filters (tumor_grade, tumor_classification, etc.)
        additional_diagnosis_filters = []
        
        # Handle tumor_grade filter
        if "tumor_grade" in filters:
            tumor_grade_value = filters["tumor_grade"]
            param_counter += 1
            tg_param = f"param_{param_counter}"
            params[tg_param] = tumor_grade_value
            additional_diagnosis_filters.append(f"d.tumor_grade = ${tg_param}")
        
        # Handle tumor_classification filter
        if "tumor_classification" in filters:
            tumor_classification_value = filters["tumor_classification"]
            if is_null_mapped_value("tumor_classification", tumor_classification_value):
                logger.info("Invalid tumor_classification value - returning empty results", value=tumor_classification_value)
                return [] if not return_total else ([], 0)
            reverse_mapped = reverse_map_field_value("tumor_classification", tumor_classification_value)
            param_counter += 1
            tc_param = f"param_{param_counter}"
            params[tc_param] = reverse_mapped if reverse_mapped else tumor_classification_value
            additional_diagnosis_filters.append(f"d.tumor_classification = ${tc_param}")
        
        # Handle tumor_tissue_morphology filter
        if "tumor_tissue_morphology" in filters:
            tumor_tissue_morphology_value = filters["tumor_tissue_morphology"]
            param_counter += 1
            ttm_param = f"param_{param_counter}"
            params[ttm_param] = tumor_tissue_morphology_value
            additional_diagnosis_filters.append(f"d.tumor_tissue_morphology = ${ttm_param}")
        
        # Handle age_at_diagnosis filter
        if "age_at_diagnosis" in filters:
            age_at_diagnosis_value = filters["age_at_diagnosis"]
            try:
                age_int = int(age_at_diagnosis_value) if age_at_diagnosis_value is not None else None
            except (ValueError, TypeError):
                age_int = age_at_diagnosis_value
            param_counter += 1
            aad_param = f"param_{param_counter}"
            params[aad_param] = age_int
            additional_diagnosis_filters.append(f"toInteger(d.age_at_diagnosis) = ${aad_param}")

        # Handle diagnosis_category filter (AND on same diagnosis node as search).
        # Same semantics as GET /subject-diagnosis associated_diagnosis_categories: substring on full field.
        if "diagnosis_category" in filters:
            diag_cat_value = filters["diagnosis_category"]
            if isinstance(diag_cat_value, str) and (dc_stripped := diag_cat_value.strip()):
                params["diag_category_contains_term"] = dc_stripped
                additional_diagnosis_filters.append(diagnosis_category_contains_predicate("dx"))

        if diagnosis_search_term:
            add_diagnosis_search_params(params, diagnosis_search_term)
            diagnosis_search_filter_condition = diagnosis_search_predicate("dx")
        elif disease_phase_filter or additional_diagnosis_filters:
            diagnosis_search_filter_condition = "true"
        else:
            return [] if not return_total else ([], 0)

        # Handle identifiers filter if present
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters["identifiers"]
            if identifiers_value:
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
        
        # Handle depositions filter
        depositions_study_filter = ""
        if "depositions" in filters:
            dep_value = filters["depositions"]
            if isinstance(dep_value, str) and "||" in dep_value:
                dep_list = [d.strip() for d in dep_value.split("||") if d.strip()]
                dep_list = [d for d in dep_list if d]
                if len(dep_list) > 1:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = dep_list
                    depositions_study_filter = f" AND st.study_id IN ${dep_param}"
                elif len(dep_list) == 1:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = dep_list[0]
                    depositions_study_filter = f" AND st.study_id = ${dep_param}"
            else:
                param_counter += 1
                dep_param = f"param_{param_counter}"
                params[dep_param] = dep_value
                depositions_study_filter = f" AND st.study_id = ${dep_param}"
        
        # Build complete diagnosis filter WHERE clause with proper parentheses
        # CRITICAL: Parenthesize correctly to ensure AND binds with the entire OR expression
        disease_phase_filter_condition = disease_phase_filter.replace("d.disease_phase", "dx.disease_phase") if disease_phase_filter else ""
        
        diagnosis_where_clause = f"""(
            {diagnosis_search_filter_condition}
        )"""
        if disease_phase_filter_condition:
            # disease_phase_filter_condition already contains " AND dx.disease_phase..."
            diagnosis_where_clause += disease_phase_filter_condition
        
        # Add additional diagnosis field filters (convert 'd.' to 'dx.' for OPTIONAL MATCH)
        if additional_diagnosis_filters:
            # Convert variable name from 'd.' to 'dx.' since we use 'dx' in OPTIONAL MATCH
            additional_filters_dx = [f.replace("d.", "dx.") for f in additional_diagnosis_filters]
            diagnosis_where_clause += " AND " + " AND ".join(additional_filters_dx)

        # Convert depositions_study_filter to depositions_sid_filter for use in UNWIND clause
        depositions_sid_filter = depositions_study_filter.replace("st.study_id", "sid") if depositions_study_filter else ""
        # Build WHERE clause for depositions filter (remove " AND " prefix since it will be in WHERE clause)
        depositions_where_clause = f"WHERE {depositions_sid_filter.replace(' AND ', '')}" if depositions_sid_filter else ""
        
        # When return_total: run lightweight count first, then list query
        total_count_diag = None
        if return_total:
            sa_where_parts = ["sa.sample_id IS NOT NULL"]
            if identifiers_early_filter:
                sa_where_parts.append(identifiers_early_filter)
            sa_where_clause = " AND ".join(sa_where_parts)
            
            cypher_count = f"""
        MATCH (sa:sample)
        WHERE {sa_where_clause}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids
        UNWIND combined_ids AS sid
        MATCH (st:study {{study_id: sid}})
        {depositions_where_clause}
        
        // collect ALL diagnoses that match search + phase (per sample-study pair)
        // Note: We collect ALL matching diagnoses per (sample_id, study_id) pair, not just one
        OPTIONAL MATCH (sa)<-[:of_diagnosis]-(dx:diagnosis)
        WHERE {diagnosis_where_clause}
        WITH sa, st, collect(DISTINCT dx) AS diagnoses
        WHERE size(diagnoses) > 0
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count_diag = recs[0].get("total_count", 0) if recs else 0
            except Exception as e_count:
                logger.error("Error in diagnosis search reverse count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build optimized query with TRUE EARLY PAGINATION
        # Key optimization: Start from samples, then find studies, then OPTIONAL MATCH filtered diagnoses
        # This avoids row multiplication from starting with diagnoses
        sa_where_parts = ["sa.sample_id IS NOT NULL", "trim(toString(sa.sample_id)) <> ''"]
        if identifiers_early_filter:
            sa_where_parts.append(identifiers_early_filter)
        sa_where_clause = " AND ".join(sa_where_parts)
        
        cypher = f"""
        MATCH (sa:sample)
        WHERE {sa_where_clause}
        
        // collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        
        // combine and drop nulls; unwind to ensure sample matches a study (one row per pair)
        WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids
        UNWIND combined_ids AS sid
        MATCH (st:study {{study_id: sid}})
        {depositions_where_clause}
        
        // collect ALL diagnoses that match search + phase (per sample-study pair)
        // CRITICAL OPTIMIZATION: Filter diagnoses in OPTIONAL MATCH WHERE clause to avoid collecting all then filtering
        // This prevents cartesian product explosion: (samples × studies × all_diagnoses)
        // 
        // IMPORTANT: We collect ALL matching diagnoses per (sample_id, study_id) pair, not just one.
        // For example, if searching for "val1":
        //   - Diagnosis A with value = "val1" (exact match) → collected
        //   - Diagnosis B with value contains "val1" (partial match) → collected
        //   - Diagnosis C with value = "val1" (exact match) → collected
        // All matching diagnoses are returned in the diagnoses list.
        OPTIONAL MATCH (sa)<-[:of_diagnosis]-(dx:diagnosis)
        WHERE {diagnosis_where_clause}
        WITH sa, st, collect(DISTINCT dx) AS diagnoses
        
        // require that at least one diagnosis matches (size > 0)
        WHERE size(diagnoses) > 0
        
        // apply ordering and early pagination here (page operates on filtered sample-study pairs)
        WITH sa, st, diagnoses
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        
        // avoid cross-product by aggregating each optional block separately
        // Note: For participant/pathology_file/sequencing_file, we collect only ONE per sample (using head)
        // This is different from diagnoses, where we collect ALL matching diagnoses per (sample_id, study_id) pair
        OPTIONAL MATCH (sa)-[:of_sample]->(p0:participant)
        WITH sa, st, diagnoses, head(collect(DISTINCT p0)) AS p
        
        OPTIONAL MATCH (pf0:pathology_file)-[:of_pathology_file]->(sa)
        WITH sa, st, diagnoses, p, head(collect(DISTINCT pf0)) AS pf
        
        OPTIONAL MATCH (sf0:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, st, diagnoses, p, pf, head(collect(DISTINCT sf0)) AS sf
        
        RETURN sa, p, st, pf, sf, diagnoses
        """.strip()
        
        logger.info(
            "Executing optimized reverse query for diagnosis search",
            cypher=cypher[:300],
            params={k: v for k, v in params.items() if k != "diagnosis_search_term_lower"}
        )
        
        # Execute query
        try:
            result = await self.session.run(cypher, params)
            records = []
            async for record in result:
                records.append(dict(record))
            await result.consume()
            
            logger.info(
                "Diagnosis search reverse query executed successfully",
                records_count=len(records)
            )
            
            # Convert records to Sample objects
            samples = []
            for record in records:
                try:
                    # Extract nodes from record
                    sa_node = record.get("sa")
                    p_node = record.get("p")
                    st_node = record.get("st")
                    sf_node = record.get("sf")
                    pf_node = record.get("pf")
                    diagnoses_node = record.get("diagnoses")
                    
                    # Convert Memgraph/Neo4j Node objects to dictionaries
                    sa = node_to_dict(sa_node)
                    p = node_to_dict(p_node)
                    st = node_to_dict(st_node)
                    sf = node_to_dict(sf_node)
                    pf = node_to_dict(pf_node)
                    
                    # Handle diagnoses: it's a list from collect(DISTINCT dx), keep ALL matched diagnoses
                    if diagnoses_node:
                        if isinstance(diagnoses_node, list):
                            diagnoses_list = [node_to_dict(d) for d in diagnoses_node if d is not None]
                        else:
                            diagnoses_list = [node_to_dict(diagnoses_node)]
                    else:
                        diagnoses_list = []

                    if sa:
                        sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses_list or None, base_url=base_url)
                        if sample:
                            samples.append(sample)
                except Exception as e:
                    logger.error("Error converting record to sample", error=str(e), record=str(record)[:200])
                    continue
            
            if return_total and total_count_diag is not None:
                return (samples, total_count_diag)
            return samples
            
        except Exception as e:
            logger.error("Error executing diagnosis search reverse query", error=str(e), exc_info=True)
            raise
    
    async def _get_samples_summary_diagnosis_search(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Optimized summary query for diagnosis search-only filters.
        
        Uses the same optimized structure as _get_samples_by_diagnosis_search
        but returns only the count.
        """
        params = {}
        param_counter = 0

        diagnosis_search_term = filters.get("_diagnosis_search")

        # Handle disease_phase filter if present - CRITICAL: Apply early to avoid cartesian product explosion
        # This prevents collecting all diagnoses then filtering, which creates (samples × studies × diagnoses) rows
        disease_phase_filter_condition = ""
        disease_phase_param_name = None
        if "disease_phase" in filters:
            disease_phase_value = filters["disease_phase"]
            # Validate disease_phase value
            if is_database_only_value("disease_phase", disease_phase_value) or is_null_mapped_value("disease_phase", disease_phase_value):
                logger.info("Invalid disease_phase value - returning empty results", value=disease_phase_value)
                return {"counts": {"total": 0}}
            
            # Apply reverse mapping for filtering (API value -> DB value(s))
            reverse_mapped = reverse_map_field_value("disease_phase", disease_phase_value)
            if isinstance(reverse_mapped, list):
                # Multiple DB values map to this API value - use IN clause
                param_counter += 1
                dp_param = f"param_{param_counter}"
                params[dp_param] = reverse_mapped
                disease_phase_param_name = dp_param
                disease_phase_filter_condition = f" AND dx.disease_phase IN ${dp_param}"
            else:
                param_counter += 1
                dp_param = f"param_{param_counter}"
                params[dp_param] = reverse_mapped
                disease_phase_param_name = dp_param
                disease_phase_filter_condition = f" AND dx.disease_phase = ${dp_param}"
             
        # Handle other diagnosis field filters (tumor_grade, tumor_classification, etc.)
        additional_diagnosis_filters = []
        
        # Handle tumor_grade filter
        if "tumor_grade" in filters:
            tumor_grade_value = filters["tumor_grade"]
            param_counter += 1
            tg_param = f"param_{param_counter}"
            params[tg_param] = tumor_grade_value
            additional_diagnosis_filters.append(f"dx.tumor_grade = ${tg_param}")
        
        # Handle tumor_classification filter
        if "tumor_classification" in filters:
            tumor_classification_value = filters["tumor_classification"]
            if is_null_mapped_value("tumor_classification", tumor_classification_value):
                logger.info("Invalid tumor_classification value - returning empty results", value=tumor_classification_value)
                return {"counts": {"total": 0}}
            reverse_mapped = reverse_map_field_value("tumor_classification", tumor_classification_value)
            param_counter += 1
            tc_param = f"param_{param_counter}"
            params[tc_param] = reverse_mapped if reverse_mapped else tumor_classification_value
            additional_diagnosis_filters.append(f"dx.tumor_classification = ${tc_param}")
        
        # Handle tumor_tissue_morphology filter
        if "tumor_tissue_morphology" in filters:
            tumor_tissue_morphology_value = filters["tumor_tissue_morphology"]
            param_counter += 1
            ttm_param = f"param_{param_counter}"
            params[ttm_param] = tumor_tissue_morphology_value
            additional_diagnosis_filters.append(f"dx.tumor_tissue_morphology = ${ttm_param}")
        
        # Handle age_at_diagnosis filter
        if "age_at_diagnosis" in filters:
            age_at_diagnosis_value = filters["age_at_diagnosis"]
            try:
                age_int = int(age_at_diagnosis_value) if age_at_diagnosis_value is not None else None
            except (ValueError, TypeError):
                age_int = age_at_diagnosis_value
            param_counter += 1
            aad_param = f"param_{param_counter}"
            params[aad_param] = age_int
            additional_diagnosis_filters.append(f"toInteger(dx.age_at_diagnosis) = ${aad_param}")

        # Handle diagnosis_category filter (AND on same diagnosis node as search).
        # Same semantics as GET /subject-diagnosis associated_diagnosis_categories: substring on full field.
        if "diagnosis_category" in filters:
            diag_cat_value = filters["diagnosis_category"]
            if isinstance(diag_cat_value, str) and (dc_stripped := diag_cat_value.strip()):
                params["diag_category_contains_term"] = dc_stripped
                additional_diagnosis_filters.append(diagnosis_category_contains_predicate("dx"))

        if diagnosis_search_term:
            add_diagnosis_search_params(params, diagnosis_search_term)
            diagnosis_search_filter_condition = diagnosis_search_predicate("dx")
        elif disease_phase_filter_condition or additional_diagnosis_filters:
            diagnosis_search_filter_condition = "true"
        else:
            return {"counts": {"total": 0}}

        # Handle identifiers filter if present
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters["identifiers"]
            if identifiers_value:
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
        
        # Handle depositions filter
        depositions_study_filter = ""
        depositions_sid_filter_summary = ""
        if "depositions" in filters:
            dep_value = filters["depositions"]
            if isinstance(dep_value, str) and "||" in dep_value:
                dep_list = [d.strip() for d in dep_value.split("||") if d.strip()]
                dep_list = [d for d in dep_list if d]
                if len(dep_list) > 1:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = dep_list
                    depositions_study_filter = f" AND st.study_id IN ${dep_param}"
                    depositions_sid_filter_summary = f" AND sid IN ${dep_param}"
                elif len(dep_list) == 1:
                    param_counter += 1
                    dep_param = f"param_{param_counter}"
                    params[dep_param] = dep_list[0]
                    depositions_study_filter = f" AND st.study_id = ${dep_param}"
                    depositions_sid_filter_summary = f" AND sid = ${dep_param}"
            else:
                param_counter += 1
                dep_param = f"param_{param_counter}"
                params[dep_param] = dep_value
                depositions_study_filter = f" AND st.study_id = ${dep_param}"
                depositions_sid_filter_summary = f" AND sid = ${dep_param}"
        
        # Build WHERE clause for depositions filter in summary query
        depositions_where_clause_summary = f"WHERE {depositions_sid_filter_summary.replace(' AND ', '')}" if depositions_sid_filter_summary else ""
        
        # Build optimized summary query
        sa_where_parts = ["sa.sample_id IS NOT NULL", "trim(toString(sa.sample_id)) <> ''"]
        if identifiers_early_filter:
            sa_where_parts.append(identifiers_early_filter)
        sa_where_clause = " AND ".join(sa_where_parts)
        
        # Build complete diagnosis filter WHERE clause with proper parentheses
        # CRITICAL: Parenthesize correctly to ensure AND binds with the entire OR expression
        # Structure: WHERE ((search_condition) OR (comment_condition)) AND disease_phase_filter
        diagnosis_where_clause_summary = f"""(
            {diagnosis_search_filter_condition}
        )"""

        if disease_phase_filter_condition:
            # disease_phase_filter_condition already contains " AND dx.disease_phase..."
            diagnosis_where_clause_summary += disease_phase_filter_condition
             
        # Add additional diagnosis field filters
        if additional_diagnosis_filters:
            diagnosis_where_clause_summary += " AND " + " AND ".join(additional_diagnosis_filters)
        
        cypher_summary = f"""
        MATCH (sa:sample)
        WHERE {sa_where_clause}
        
        // collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        
        // combine and drop nulls; unwind to ensure sample matches a study (one row per pair)
        WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids
        UNWIND combined_ids AS sid
        MATCH (st:study {{study_id: sid}})
        {depositions_where_clause_summary}
        
        // collect ALL diagnoses that match search + phase (per sample-study pair)
        // CRITICAL OPTIMIZATION: Filter in OPTIONAL MATCH WHERE clause to avoid cartesian product
        // 
        // IMPORTANT: We collect ALL matching diagnoses per (sample_id, study_id) pair, not just one.
        // The WHERE clause uses CONTAINS (case-insensitive) which matches both exact and partial matches.
        // All matching diagnoses are collected and counted.
        OPTIONAL MATCH (sa)<-[:of_diagnosis]-(dx:diagnosis)
        WHERE {diagnosis_where_clause_summary}
        WITH sa, st, collect(DISTINCT dx) AS diagnoses
        
        // require that at least one diagnosis matches
        WHERE size(diagnoses) > 0
        
        // Count distinct (sample_id, study_id) pairs
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) AS total_count
        """.strip()
        
        logger.info(
            "Executing optimized summary query for diagnosis search",
            cypher=cypher_summary[:300],
            params={k: v for k, v in params.items() if k != "diagnosis_search_term_lower"}
        )
        
        try:
            result = await self.session.run(cypher_summary, params)
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
            logger.error("Error executing diagnosis search summary query", error=str(e), exc_info=True)
            # Return 0 on error (consistent with other summary methods)
            return {"counts": {"total": 0}}
