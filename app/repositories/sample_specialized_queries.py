"""
Sample repository specialized query methods.

This module contains reverse query methods for performance optimization.
These methods start from related nodes (sequencing_file, pathology_file) and find samples.
These methods are provided as a mixin class that can be inherited by SampleRepository.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from app.core.logging import get_logger
from app.models.dto import Sample
from app.core.field_mappings import (
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
    load_sample_enum,
    load_sequencing_file_enum
)
from app.repositories.sample_converters import node_to_dict
from app.repositories.sample_validators import SampleValidators

logger = get_logger(__name__)


class SampleSpecializedQueries(SampleValidators):
    """Mixin class providing specialized reverse query methods for SampleRepository."""
    
    async def _get_samples_by_sequencing_file_filters(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Optimized query for sequencing_file-only filters.
        
        Uses REVERSE query approach:
        1. Match sequencing_files with the filter (uses index - FAST)
        2. Find samples related to those files
        3. Do other relationship traversals
        
        This is 10-100x faster than the standard approach of collecting all files first.
        """
        params = {"offset": offset, "limit": limit}
        where_conditions = []
        param_counter = 0
        
        # Build WHERE conditions for sequencing_file properties
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if field == "library_source_material":
                # Check if invalid value
                if is_null_mapped_value("library_source_material", value):
                    logger.info("Invalid library_source_material value - returning empty results", value=value)
                    return []
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                params[param_name] = reverse_mapped
                where_conditions.append(f"sf.library_source_material = ${param_name}")
                
            elif field == "library_strategy":
                # Check if invalid value
                if is_database_only_value("library_strategy", value):
                    logger.info("Invalid library_strategy value - returning empty results", value=value)
                    return []
                # Handle reverse mapping
                reverse_mapped = reverse_map_field_value("library_strategy", value)
                if reverse_mapped and reverse_mapped != value:
                    # Has mapping - need to match both
                    param_counter += 1
                    param_name2 = f"param_{param_counter}"
                    params[param_name] = reverse_mapped if isinstance(reverse_mapped, str) else reverse_mapped[0]
                    params[param_name2] = value
                    where_conditions.append(f"(sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name2})")
                else:
                    params[param_name] = value
                    where_conditions.append(f"sf.library_strategy = ${param_name}")
                    
            elif field == "library_selection_method":
                # Check if invalid value
                if is_database_only_value("library_selection_method", value):
                    logger.info("Invalid library_selection_method value - returning empty results", value=value)
                    return []
                db_value = self._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                where_conditions.append(f"sf.library_selection = ${param_name}")
                
            elif field == "specimen_molecular_analyte_type":
                # Check if invalid value
                if is_database_only_value("specimen_molecular_analyte_type", value) or is_null_mapped_value("specimen_molecular_analyte_type", value):
                    logger.info("Invalid specimen_molecular_analyte_type value - returning empty results", value=value)
                    return []
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values (e.g., "RNA" -> ["Transcriptomic", "Viral RNA"])
                    db_values_str = ", ".join([f"'{v}'" for v in reverse_mapped])
                    where_conditions.append(f"sf.library_source_molecule IN [{db_values_str}]")
                else:
                    params[param_name] = reverse_mapped
                    where_conditions.append(f"sf.library_source_molecule = ${param_name}")
        
        # Build WHERE clause
        where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
        
        # When return_total: run lightweight count first, then list query
        total_count_sf = None
        if return_total:
            cypher_count = f"""
        MATCH (sf:sequencing_file)
        WHERE {where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count_sf = recs[0].get("total_count", 0) if recs else 0
            except Exception as e_count:
                logger.error("Error in sequencing_file reverse count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build optimized reverse query
        # Key optimization: Start from sequencing_file (uses index), then find samples
        # Note: For large result sets (10k+ samples), study path traversal is inherently expensive
        # Further optimization would require database-level changes (e.g., indexed has_study property)
        # This avoids the expensive traversal through cell_line/participant/consent_group.
        # IMPORTANT: We also deduplicate matching sequencing_files per (sample, study) before pagination
        # to prevent row explosion when multiple sf match the filter for the same sample.
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
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st, collect(DISTINCT sf) AS matching_sfs
        WITH sa, st, head(matching_sfs) AS sf
        ORDER BY toString(sa.sample_id)
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        WITH sa, p, st,
             head(collect(DISTINCT d)) AS diagnoses,
             head(collect(DISTINCT pf)) AS pf,
             sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.info(
            "Executing optimized reverse query",
            cypher=cypher[:300],
            params=params
        )
        
        # Execute query
        try:
            result = await self.session.run(cypher, params)
            records = []
            async for record in result:
                records.append(dict(record))
            await result.consume()
            
            logger.info(
                "Reverse query executed successfully",
                records_count=len(records)
            )
            
            # Convert records to Sample objects
            samples = []
            for record in records:
                try:
                    sa_node = record.get("sa")
                    p_node = record.get("p")
                    st_node = record.get("st")
                    sf_node = record.get("sf")
                    pf_node = record.get("pf")
                    diagnoses_node = record.get("diagnoses")
                    
                    # Convert nodes to dictionaries using utility function
                    sa = node_to_dict(sa_node)
                    p = node_to_dict(p_node)
                    st = node_to_dict(st_node)
                    sf = node_to_dict(sf_node)
                    pf = node_to_dict(pf_node)
                    diagnoses = node_to_dict(diagnoses_node) if diagnoses_node else None
                    
                    sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url=base_url)
                    samples.append(sample)
                except Exception as e:
                    logger.error("Error converting record to sample", error=str(e), record=str(record)[:200])
                    continue
            
            if return_total and total_count_sf is not None:
                return (samples, total_count_sf)
            return samples
            
        except Exception as e:
            logger.error("Error executing reverse query", error=str(e), exc_info=True)
            raise
    
    async def _get_samples_by_pathology_file_filters(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Query for pathology_file-only filters.
        
        Uses standard query approach starting from sample nodes:
        1. Match samples
        2. OPTIONAL MATCH pathology_file with filter
        3. Filter to only samples that have matching pathology_file
        4. Resolve studies and paginate
        5. Collect other relationships
        """
        params = {"offset": offset, "limit": limit}
        where_conditions = []
        param_counter = 0
        
        # Build WHERE conditions for pathology_file properties
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if field == "preservation_method":
                params[param_name] = value
                where_conditions.append(f"pf.fixation_embedding_method = ${param_name}")
        
        # Build WHERE clause for pathology_file filter
        pf_where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
        
        # When return_total: run lightweight count first, then list query
        total_count_pf = None
        if return_total:
            cypher_count = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        WITH sa, pf
        WHERE pf IS NOT NULL AND ({pf_where_clause})
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count_pf = recs[0].get("total_count", 0) if recs else 0
            except Exception as e_count:
                logger.error("Error in pathology_file count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build query starting from sample nodes
        cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        WITH sa, pf
        WHERE pf IS NOT NULL AND ({pf_where_clause})
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, pf, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, pf, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, pf, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st, pf, toString(sa.sample_id) AS sample_id, toString(st.study_id) AS study_id
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, pf, sample_id, study_id
        ORDER BY sample_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, p, st,
             head(collect(DISTINCT d)) AS diagnoses,
             head(collect(DISTINCT pf)) AS pf,
             head(collect(DISTINCT sf)) AS sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.info(
            "Executing query for pathology_file filters",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        try:
            result = await self.session.run(cypher, params)
            records = []
            async for record in result:
                records.append(dict(record))
            await result.consume()
            
            logger.info(
                "Pathology_file reverse query executed successfully",
                records_count=len(records)
            )
            
            # Convert records to Sample objects
            samples = []
            for record in records:
                try:
                    sa_node = record.get("sa")
                    p_node = record.get("p")
                    st_node = record.get("st")
                    sf_node = record.get("sf")
                    pf_node = record.get("pf")
                    diagnoses_node = record.get("diagnoses")
                    
                    # Convert nodes to dictionaries using utility function
                    sa = node_to_dict(sa_node)
                    p = node_to_dict(p_node)
                    st = node_to_dict(st_node)
                    sf = node_to_dict(sf_node)
                    pf = node_to_dict(pf_node)
                    diagnoses = node_to_dict(diagnoses_node) if diagnoses_node else None
                    
                    sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url=base_url)
                    samples.append(sample)
                except Exception as e:
                    logger.error("Error converting record to sample", error=str(e), record=str(record)[:200])
                    continue
            
            if return_total and total_count_pf is not None:
                return (samples, total_count_pf)
            return samples
            
        except Exception as e:
            logger.error("Error executing pathology_file reverse query", error=str(e), exc_info=True)
            raise
    
    async def _get_samples_by_combined_filters(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Optimized query for combined sequencing_file + pathology_file filters.
        
        Uses REVERSE query approach:
        1. Match sequencing_files with filter (uses index - FAST)
        2. Match pathology_files with filter (uses index - FAST)
        3. Find samples that have BOTH (via join on sample)
        4. Do other relationship traversals
        
        This is 10-100x faster than matching all samples first.
        """
        params = {"offset": offset, "limit": limit}
        sf_where_conditions = []
        pf_where_conditions = []
        param_counter = 0
        
        # Build WHERE conditions for sequencing_file properties
        sequencing_file_filter_keys = {"library_selection_method", "library_strategy", "library_source_material", "specimen_molecular_analyte_type"}
        for field, value in filters.items():
            if field not in sequencing_file_filter_keys:
                continue
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if field == "library_source_material":
                if is_null_mapped_value("library_source_material", value):
                    logger.info("Invalid library_source_material value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                # If reverse_mapped is None, use the original value (no mapping needed)
                params[param_name] = reverse_mapped if reverse_mapped else value
                sf_where_conditions.append(f"sf.library_source_material = ${param_name}")
            elif field == "library_strategy":
                if is_database_only_value("library_strategy", value):
                    logger.info("Invalid library_strategy value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                reverse_mapped = reverse_map_field_value("library_strategy", value)
                if reverse_mapped and reverse_mapped != value:
                    param_counter += 1
                    param_name2 = f"param_{param_counter}"
                    params[param_name] = reverse_mapped if isinstance(reverse_mapped, str) else reverse_mapped[0]
                    params[param_name2] = value
                    sf_where_conditions.append(f"(sf.library_strategy = ${param_name} OR sf.library_strategy = ${param_name2})")
                else:
                    params[param_name] = value
                    sf_where_conditions.append(f"sf.library_strategy = ${param_name}")
            elif field == "library_selection_method":
                if is_database_only_value("library_selection_method", value):
                    logger.info("Invalid library_selection_method value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                db_value = self._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                sf_where_conditions.append(f"sf.library_selection = ${param_name}")
            elif field == "specimen_molecular_analyte_type":
                if is_database_only_value("specimen_molecular_analyte_type", value) or is_null_mapped_value("specimen_molecular_analyte_type", value):
                    logger.info("Invalid specimen_molecular_analyte_type value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    db_values_str = ", ".join([f"'{v}'" for v in reverse_mapped])
                    sf_where_conditions.append(f"sf.library_source_molecule IN [{db_values_str}]")
                else:
                    # If reverse_mapped is None, use the original value (no mapping needed)
                    params[param_name] = reverse_mapped if reverse_mapped else value
                    sf_where_conditions.append(f"sf.library_source_molecule = ${param_name}")
        
        # Build WHERE conditions for pathology_file properties
        if "preservation_method" in filters:
            param_counter += 1
            param_name = f"param_{param_counter}"
            params[param_name] = filters["preservation_method"]
            pf_where_conditions.append(f"pf.fixation_embedding_method = ${param_name}")
        
        # Build WHERE conditions for sample node properties (can be applied after reverse query)
        sa_where_conditions = []
        if "tissue_type" in filters:
            param_counter += 1
            param_name = f"param_{param_counter}"
            tissue_value = filters["tissue_type"]
            valid_values = load_sample_enum("sample_tumor_status")
            if valid_values:
                if isinstance(tissue_value, list):
                    invalid_values = [v for v in tissue_value if v not in valid_values]
                    if invalid_values:
                        return [] if not return_total else ([], 0)
                    params[param_name] = tissue_value
                    sa_where_conditions.append(f"sa.sample_tumor_status IN ${param_name}")
                else:
                    if tissue_value not in valid_values:
                        return [] if not return_total else ([], 0)
                    params[param_name] = [tissue_value]
                    sa_where_conditions.append(f"sa.sample_tumor_status IN ${param_name}")
        
        # Build WHERE clauses
        sf_where_clause = " AND ".join(sf_where_conditions) if sf_where_conditions else "TRUE"
        pf_where_clause = " AND ".join(pf_where_conditions) if pf_where_conditions else "TRUE"
        sa_where_clause = " AND ".join(sa_where_conditions) if sa_where_conditions else ""
        
        # When return_total: run lightweight count first, then list query
        total_count_combined = None
        if return_total:
            cypher_count = f"""
        MATCH (sf:sequencing_file)
        WHERE {sf_where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        MATCH (pf:pathology_file)
        WHERE {pf_where_clause}
        MATCH (pf)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                result_count = await self.session.run(cypher_count, params)
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count_combined = recs[0].get("total_count", 0) if recs else 0
            except Exception as e_count:
                logger.error("Error in combined reverse count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build optimized combined reverse query
        # Key optimization: Start from BOTH sequencing_file AND pathology_file with filters, then find samples
        # This ensures we only process samples that have BOTH matching files
        # Sample node filters (like tissue_type) are applied on the sample node after reverse matching
        cypher = f"""
        MATCH (sf:sequencing_file)
        WHERE {sf_where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''{f" AND {sa_where_clause}" if sa_where_clause else ""}
        MATCH (pf:pathology_file)
        WHERE {pf_where_clause}
        MATCH (pf)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, sf, pf, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, sf, pf, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, sf, pf, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st, collect(DISTINCT sf) AS matching_sfs, collect(DISTINCT pf) AS matching_pfs
        WITH sa, st, head(matching_sfs) AS sf, head(matching_pfs) AS pf
        ORDER BY toString(sa.sample_id)
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        WITH sa, p, st,
             head(collect(DISTINCT d)) AS diagnoses,
             sf,
             pf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.info(
            "Executing optimized combined reverse query",
            cypher=cypher[:300],
            params=params
        )
        
        # Execute query
        try:
            result = await self.session.run(cypher, params)
            records = []
            async for record in result:
                records.append(dict(record))
            await result.consume()
            
            logger.info(
                "Combined reverse query executed successfully",
                records_count=len(records)
            )
            
            # Convert records to Sample objects
            samples = []
            for record in records:
                try:
                    sa_node = record.get("sa")
                    p_node = record.get("p")
                    st_node = record.get("st")
                    sf_node = record.get("sf")
                    pf_node = record.get("pf")
                    diagnoses_node = record.get("diagnoses")
                    
                    # Convert nodes to dictionaries using utility function
                    sa = node_to_dict(sa_node)
                    p = node_to_dict(p_node)
                    st = node_to_dict(st_node)
                    sf = node_to_dict(sf_node)
                    pf = node_to_dict(pf_node)
                    diagnoses = node_to_dict(diagnoses_node) if diagnoses_node else None
                    
                    sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url=base_url)
                    samples.append(sample)
                except Exception as e:
                    logger.error("Error converting record to sample", error=str(e), record=str(record)[:200])
                    continue
            
            if return_total and total_count_combined is not None:
                return (samples, total_count_combined)
            return samples
            
        except Exception as e:
            logger.error("Error executing combined reverse query", error=str(e), exc_info=True)
            raise
    
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
                # If reverse_mapped is None, use the original value (no mapping needed)
                params[param_name] = reverse_mapped if reverse_mapped else value
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
            
            logger.info("Reverse summary query executed successfully", total_count=total_count)
            
            # Return in the expected format for the service layer
            return {
                "counts": {
                    "total": total_count
                }
            }
            
        except Exception as e:
            logger.error("Error executing reverse summary query", error=str(e), exc_info=True)
            raise
