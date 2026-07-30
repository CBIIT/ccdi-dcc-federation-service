"""
Sample repository for the CCDI Federation Service.

This module provides data access operations for samples
using Cypher queries to Memgraph.
"""

from typing import List, Dict, Any, Optional, Tuple, Union
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.db.memgraph import run_count_query_with_retry
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Sample
from app.models.errors import UnsupportedFieldError
from app.core.config import Settings
from app.core.field_mappings import (
    reverse_map_field_value,
    is_null_mapped_value,
    is_database_only_value,
    # Re-exported for unit-test patch points and mixin call sites
    map_field_value,
    build_invalid_value_filter,
    build_invalid_value_list_filter,
    build_invalid_value_all_clause,
    build_case_mapping_statement,
    get_mapped_db_values,
    load_sequencing_file_enum,
    load_sample_enum,
    get_null_mappings,
)
from app.repositories.sample_converters import SampleConverters
from app.repositories.sample_diagnosis_search import SampleDiagnosisSearch
from app.repositories.sample_query_cases import SampleQueryCases
from app.utils.cypher_builder import anatomic_site_member_predicate
from app.repositories.sample_helpers import SampleHelpers, SD_CAT_MARKER
from app.repositories.sample_count import SampleCount
from app.repositories.sample_summary import SampleSummary

logger = get_logger(__name__)


class SampleRepository(SampleDiagnosisSearch, SampleQueryCases, SampleHelpers, SampleCount, SampleSummary, SampleConverters):
    """Repository for sample data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings: Optional[Settings] = None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings
    
    async def _get_samples_early_pagination_with_filters(
        self,
        filters: Dict[str, Any],
        offset: int,
        limit: int,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Optional[Union[List[Sample], Tuple[List[Sample], int]]]:
        """
        Get samples using early-pagination flow when only identifiers, depositions, anatomical_sites, and/or tissue_type are present.
        Flow: (1) MATCH sa + identifiers/anatomical_sites/tissue_type (2) study resolution + depositions (3) OPTIONAL MATCH p
              (4) ORDER BY SKIP LIMIT [early pagination] (5) OPTIONAL MATCH d, pf, sf (6) RETURN.
        Returns list of Sample objects, or (list, total_count) if return_total=True, or None if filters cannot be handled by this path.
        """
        params: Dict[str, Any] = {"offset": offset, "limit": limit}
        early_where_parts = [
            "sa.sample_id IS NOT NULL",
        ]
        depositions_filter = ""
        
        # Parse identifiers (read only; do not mutate filters)
        identifiers_value = filters.get("identifiers")
        if identifiers_value is not None and (not isinstance(identifiers_value, str) or identifiers_value.strip()):
            if isinstance(identifiers_value, str) and "||" in identifiers_value:
                identifiers_list = [i.strip() for i in identifiers_value.split("||") if i.strip()]
                identifiers_value = identifiers_list if identifiers_list else None
            if identifiers_value:
                params["_id_param"] = identifiers_value
                if isinstance(identifiers_value, list):
                    early_where_parts.append("sa.sample_id IN $_id_param")
                else:
                    early_where_parts.append("sa.sample_id = $_id_param")
        
        # Parse depositions (read only)
        dep_value = filters.get("depositions")
        if dep_value is not None and str(dep_value).strip():
            if isinstance(dep_value, str) and "||" in dep_value:
                dep_list = [d.strip() for d in dep_value.split("||") if d.strip()]
                if dep_list:
                    params["_dep_param"] = dep_list if len(dep_list) > 1 else dep_list[0]
                    depositions_filter = " AND st.study_id IN $_dep_param" if len(dep_list) > 1 else " AND st.study_id = $_dep_param"
            else:
                params["_dep_param"] = dep_value
                depositions_filter = " AND st.study_id = $_dep_param"
        
        # Parse anatomical_sites (sample property, can be filtered early)
        anatomical_sites_value = filters.get("anatomical_sites")
        if anatomical_sites_value is not None:
            if isinstance(anatomical_sites_value, list):
                # Multiple values - build OR conditions
                or_conditions = []
                for idx, val in enumerate(anatomical_sites_value):
                    val_param = f"_anatomical_sites_{idx}"
                    params[val_param] = val.strip() if isinstance(val, str) else val
                    or_conditions.append(
                        f"({anatomic_site_member_predicate('sa', '$' + val_param)})"
                    )
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND ({' OR '.join(or_conditions)})"""
            else:
                # Single value - handle exact match and semicolon-separated string
                params["_anatomical_sites_param"] = anatomical_sites_value.strip() if isinstance(anatomical_sites_value, str) else anatomical_sites_value
                anatomical_sites_condition = f"""sa.anatomic_site IS NOT NULL AND (
                    {anatomic_site_member_predicate('sa', '$_anatomical_sites_param')}
                )"""
            early_where_parts.append(anatomical_sites_condition)
        
        # Parse tissue_type (sample property, can be filtered early)
        tissue_type_value = filters.get("tissue_type")
        if tissue_type_value is not None:
            # Use helper function to validate and build condition
            with_conditions_temp = []
            tissue_param = "_tissue_type_param"
            if self._validate_tissue_type_filter(tissue_type_value, tissue_param, params, with_conditions_temp) is None:
                # Invalid tissue_type - return empty results
                logger.info("Early pagination: Invalid tissue_type filter, returning empty results", tissue_type=tissue_type_value)
                if return_total:
                    return ([], 0)
                return []
            # Add the condition from with_conditions_temp to early_where_parts
            if with_conditions_temp:
                early_where_parts.append(with_conditions_temp[0])
        
        # Parse tumor_classification (sample property, can be filtered early)
        tumor_classification_value = filters.get("tumor_classification")
        if tumor_classification_value is not None:
            if is_null_mapped_value("tumor_classification", tumor_classification_value) or is_database_only_value("tumor_classification", tumor_classification_value):
                logger.info("Early pagination: Invalid tumor_classification filter, returning empty results", value=tumor_classification_value)
                if return_total:
                    return ([], 0)
                return []
            reverse_mapped = reverse_map_field_value("tumor_classification", tumor_classification_value)
            params["_tumor_classification_param"] = reverse_mapped if reverse_mapped else tumor_classification_value
            if isinstance(params["_tumor_classification_param"], list):
                early_where_parts.append("sa.tumor_spatial_extent IN $_tumor_classification_param")
            else:
                early_where_parts.append("sa.tumor_spatial_extent = $_tumor_classification_param")

        # This path handles identifiers, depositions, anatomical_sites, tissue_type, and tumor_classification (sample properties)
        # No other keys allowed (would need to fall through to standard query)
        allowed = {"identifiers", "depositions", "anatomical_sites", "tissue_type", "tumor_classification"}
        if set(filters.keys()) - allowed:
            return None
        
        early_where_clause = " AND ".join(early_where_parts)
        
        # OPTIMIZATION: For depositions-only queries, start from study to enable early pagination
        # Check if only depositions filter (early_where_parts only has base conditions)
        has_only_depositions = depositions_filter and len(early_where_parts) == 1  # Only base conditions
        
        # Build count query if return_total
        total_count = None
        if return_total:
            if has_only_depositions:
                # Depositions-only: start from study for better performance
                study_filter_clause = depositions_filter.replace(" AND ", "")
                cypher_count = f"""
            MATCH (st:study)
            WHERE {study_filter_clause}
            // Path 1: via cell_line - collect samples
            OPTIONAL MATCH (st)<-[:of_cell_line]-(:cell_line)<-[:of_sample]-(sa1:sample)
            WHERE sa1.sample_id IS NOT NULL
            WITH st, collect(DISTINCT sa1) AS sa1_list
            // Path 2: via participant -> consent_group - collect samples
            OPTIONAL MATCH (st)<-[:of_consent_group]-(:consent_group)<-[:of_participant]-(:participant)<-[:of_sample]-(sa2:sample)
            WHERE sa2.sample_id IS NOT NULL
            WITH st, sa1_list, collect(DISTINCT sa2) AS sa2_list
            // Combine both paths and unwind
            WITH st, [sa IN (sa1_list + sa2_list) WHERE sa IS NOT NULL] AS sa_list
            UNWIND sa_list AS sa
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) as total_count
            """.strip()
            else:
                # Has other filters - use standard query structure
                study_filter_clause_st1 = ""
                study_filter_clause_st2 = ""
                if depositions_filter:
                    # Replace "st.study_id" with "st1.study_id" and "st2.study_id" for the OPTIONAL MATCH clauses
                    base_filter = depositions_filter.replace(" AND ", "")
                    study_filter_clause_st1 = base_filter.replace("st.study_id", "st1.study_id")
                    study_filter_clause_st2 = base_filter.replace("st.study_id", "st2.study_id")
                
                cypher_count = f"""
            MATCH (sa:sample)
            WHERE {early_where_clause}
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            {f"WHERE {study_filter_clause_st1}" if study_filter_clause_st1 else ""}
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            {f"WHERE {study_filter_clause_st2}" if study_filter_clause_st2 else ""}
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, [sid IN (st2_list + st1_list) WHERE sid IS NOT NULL] AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) as total_count
            """.strip()
            
            try:
                total_count = await run_count_query_with_retry(self.session, cypher_count, params)
            except Exception as e:
                logger.warning("Early pagination count query failed", error=str(e), exc_info=True)
                total_count = None
        
        if has_only_depositions:
            # Depositions-only: start from study for better performance
            study_filter_clause = depositions_filter.replace(" AND ", "")
            cypher = f"""
        MATCH (st:study)
        WHERE {study_filter_clause}
        // Path 1: via cell_line - collect samples
        OPTIONAL MATCH (st)<-[:of_cell_line]-(:cell_line)<-[:of_sample]-(sa1:sample)
        WHERE sa1.sample_id IS NOT NULL
        WITH st, collect(DISTINCT sa1) AS sa1_list
        // Path 2: via participant -> consent_group - collect samples
        OPTIONAL MATCH (st)<-[:of_consent_group]-(:consent_group)<-[:of_participant]-(:participant)<-[:of_sample]-(sa2:sample)
        WHERE sa2.sample_id IS NOT NULL
        WITH st, sa1_list, collect(DISTINCT sa2) AS sa2_list
        // Combine both paths and unwind
        WITH st, [sa IN (sa1_list + sa2_list) WHERE sa IS NOT NULL] AS sa_list
        UNWIND sa_list AS sa
        WITH DISTINCT sa, st
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        // After pagination: OPTIONAL MATCH participant (only for paginated samples - much faster)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, p, st, head(collect(DISTINCT d)) AS diagnoses, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        else:
            # Has other filters (identifiers, anatomical_sites, tissue_type) - use standard query structure
            study_filter_clause_st1 = ""
            study_filter_clause_st2 = ""
            if depositions_filter:
                # Replace "st.study_id" with "st1.study_id" and "st2.study_id" for the OPTIONAL MATCH clauses
                base_filter = depositions_filter.replace(" AND ", "")
                study_filter_clause_st1 = base_filter.replace("st.study_id", "st1.study_id")
                study_filter_clause_st2 = base_filter.replace("st.study_id", "st2.study_id")
            
            cypher = f"""
        MATCH (sa:sample)
        WHERE {early_where_clause}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        {f"WHERE {study_filter_clause_st1}" if study_filter_clause_st1 else ""}
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        {f"WHERE {study_filter_clause_st2}" if study_filter_clause_st2 else ""}
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, [sid IN (st2_list + st1_list) WHERE sid IS NOT NULL] AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        // After pagination: OPTIONAL MATCH participant (only for paginated samples - much faster)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, p, st, head(collect(DISTINCT d)) AS diagnoses, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.info(
            "Executing early pagination with filters (identifiers/depositions/anatomical_sites/tissue_type)",
            pattern="early_pagination_with_filters",
            offset=offset,
            limit=limit,
            filters=list(filters.keys()),
            return_total=return_total,
        )
        
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
                diagnoses = dict(record["diagnoses"]) if record.get("diagnoses") else None
                if sa:
                    sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                    if sample_obj:
                        samples.append(sample_obj)
            except Exception as e:
                logger.warning("Error converting sample record in early-pagination path: %s", e, exc_info=True)
                continue
        
        # When return_total but the count query failed: return a bare list so
        # SampleService can fall back to summary instead of reporting total=0
        # while samples still has real rows.
        if return_total and total_count is not None:
            return (samples, total_count)
        return samples

    async def get_samples(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False
    ) -> Union[List[Sample], Tuple[List[Sample], int]]:
        """
        Get paginated list of samples with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            base_url: Optional base URL for sample links
            return_total: If True, also run a count query and return (samples, total_count).
                          Uses same filter state as list query to avoid duplicate get_samples_summary call.
        
        Returns:
            List of Sample objects, or (List of Sample objects, total_count) when return_total=True
            
        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching samples",
            filters=filters,
            offset=offset,
            limit=limit,
            return_total=return_total
        )
        
        # Handle no-filters case first (Case 0)
        if not filters or len(filters) == 0:
            logger.debug("Using Case 0: No filters query path")
            # No specialized case applies to empty filters; falls through the
            # categorization checks below (all False) into Case 3, which handles it.
        
        # Categorize filters by node type
        categorized = self._categorize_filters(filters)
        has_sample_filters = len(categorized["sample"]) > 0
        has_study_filters = len(categorized["study"]) > 0
        has_diagnosis_filters = bool(set(categorized["diagnosis"].keys()) - {SD_CAT_MARKER})
        has_sf_filters = len(categorized["sequencing_file"]) > 0
        has_pf_filters = len(categorized["pathology_file"]) > 0
        
        # Determine query path based on filter combination
        # Case 1: Sample-only filters (no other node filters)
        if has_sample_filters and not has_study_filters and not has_diagnosis_filters and not has_sf_filters and not has_pf_filters:
            logger.debug("Using Case 1: Sample-only filters query path")
            return await self._get_samples_case1_sample_only(
                categorized["sample"], offset, limit, base_url, return_total
            )
        
        # Case 2: Sample + Study filters only (no diagnosis/sequencing_file/pathology_file filters)
        if (has_sample_filters or has_study_filters) and not has_diagnosis_filters and not has_sf_filters and not has_pf_filters:
            logger.debug("Using Case 2: Sample + Study filters only query path")
            # Combine sample and study filters
            combined_filters = {**categorized["sample"], **categorized["study"]}
            result_case2 = await self._get_samples_case2_sample_study(
                combined_filters, offset, limit, base_url, return_total
            )
            # Case 2 may return None when early-pagination path cannot handle the specific filter set.
            # Fall through to Case 3 instead of returning None to callers (Case 3 always handles it).
            if result_case2 is not None:
                return result_case2
        
        # Optimization: Pathology_file-only filters (no other node filters) - use specialized method
        if has_pf_filters and not has_diagnosis_filters and not has_sf_filters and not has_sample_filters and not has_study_filters:
            logger.debug("Using optimized pathology_file-only filters query path")
            return await self._get_samples_by_pathology_file_filters(
                categorized["pathology_file"], offset, limit, base_url, return_total
            )

        # Optimization: Sequencing_file-only filters (no other node filters).
        # Fields such as specimen_molecular_analyte_type live on sequencing_file, so start
        # from the indexed sequencing_file side instead of scanning all samples (Case 3).
        if has_sf_filters and not has_diagnosis_filters and not has_pf_filters and not has_sample_filters and not has_study_filters:
            logger.debug("Using optimized sequencing_file-only filters query path")
            return await self._get_samples_by_sequencing_file_filters(
                categorized["sequencing_file"], offset, limit, base_url, return_total
            )

        # Case 3: Has diagnosis/sequencing_file/pathology_file filters
        # Apply filters before pagination, then paginate at sample-study pair level
        logger.info(
            "Using Case 3: Has other node filters query path",
            filters=filters,
            sample_filters=list(categorized.get("sample", {}).keys()),
            pathology_file_filters=list(categorized.get("pathology_file", {}).keys()),
            diagnosis_filters=list(categorized.get("diagnosis", {}).keys()),
            sequencing_file_filters=list(categorized.get("sequencing_file", {}).keys()),
            study_filters=list(categorized.get("study", {}).keys())
        )
        return await self._get_samples_case3_with_node_filters(
            filters, categorized, offset, limit, base_url, return_total
        )

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
                # Check if invalid value (null-mapped or DB-only like "Other")
                if is_null_mapped_value("library_source_material", value) or is_database_only_value(
                    "library_source_material", value
                ):
                    logger.info("Invalid library_source_material value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                reverse_mapped = reverse_map_field_value("library_source_material", value)
                if isinstance(reverse_mapped, list):
                    params[param_name] = reverse_mapped
                    where_conditions.append(f"sf.library_source_material IN ${param_name}")
                else:
                    params[param_name] = reverse_mapped if reverse_mapped else value
                    where_conditions.append(f"sf.library_source_material = ${param_name}")

            elif field == "library_strategy":
                # Check if invalid value
                if is_database_only_value("library_strategy", value):
                    logger.info("Invalid library_strategy value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
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
                    return [] if not return_total else ([], 0)
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
                params[param_name] = db_value
                where_conditions.append(f"sf.library_selection = ${param_name}")

            elif field == "specimen_molecular_analyte_type":
                # Check if invalid value
                if is_database_only_value("specimen_molecular_analyte_type", value) or is_null_mapped_value("specimen_molecular_analyte_type", value):
                    logger.info("Invalid specimen_molecular_analyte_type value - returning empty results", value=value)
                    return [] if not return_total else ([], 0)
                reverse_mapped = reverse_map_field_value("specimen_molecular_analyte_type", value)
                if isinstance(reverse_mapped, list):
                    # Multiple DB values (e.g., "RNA" -> ["Transcriptomic", "Viral RNA"])
                    params[param_name] = reverse_mapped
                    where_conditions.append(f"sf.library_source_molecule IN ${param_name}")
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
        WHERE sa.sample_id IS NOT NULL
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        WITH sa, sid
        WHERE sid IS NOT NULL
        WITH DISTINCT sa.sample_id AS sample_id, sid AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                total_count_sf = await run_count_query_with_retry(self.session, cypher_count, params)
            except Exception as e_count:
                logger.error("Error in sequencing_file reverse count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build optimized reverse query with EARLY PAGINATION
        # Key optimization: Start from sequencing_file (uses index), then find samples
        # PERFORMANCE OPTIMIZATION: Paginate at (sample_id, study_id) pair level to match count query
        # Flow: (1) Match sf + sa (2) Collect study IDs (3) UNWIND studies (4) ORDER BY + SKIP/LIMIT [pagination at pair level]
        #       (5) Rematch sf for paginated (sa, st) pairs (6) Deduplicate sf per (sa, st) (7) OPTIONAL MATCH other relationships
        # IMPORTANT: Pagination at (sa, st) pair level ensures consistency with count query
        cypher = f"""
        MATCH (sf:sequencing_file)
        WHERE {where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL
        // Collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, sf, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, sf, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa, sf, 
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, sf, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, sf, sid
        WHERE sid IS NOT NULL
        WITH DISTINCT sa, sid
        ORDER BY toString(sa.sample_id), toString(sid)
        SKIP $offset
        LIMIT $limit
        // Resolve study + rematch sf only for the paginated page
        MATCH (st:study)
        WHERE st.study_id = sid
        OPTIONAL MATCH (sf_rematched:sequencing_file)-[:of_sequencing_file]->(sa)
        WHERE {where_clause.replace("sf.", "sf_rematched.")}
        WITH sa, st, collect(DISTINCT sf_rematched) AS matching_sfs
        WITH sa, st, head(matching_sfs) AS sf
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
            "Executing optimized reverse query with early pagination",
            pattern="reverse_query_sequencing_file_early_pagination",
            cypher=cypher[:300],
            params=params,
            offset=offset,
            limit=limit
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
                    
                    # Convert nodes to dictionaries
                    sa = dict(sa_node) if sa_node else {}
                    p = dict(p_node) if p_node else {}
                    st = dict(st_node) if st_node else {}
                    sf = dict(sf_node) if sf_node else {}
                    pf = dict(pf_node) if pf_node else {}
                    diagnoses = dict(diagnoses_node) if diagnoses_node else {}
                    
                    sample = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url=base_url)
                    samples.append(sample)
                except Exception as e:
                    logger.error("Error converting record to sample", error=str(e), record=str(record)[:200])
                    continue
            
            # When return_total but count failed: return a bare list so SampleService
            # and get_samples_for_diagnosis_endpoint can fall back to summary instead
            # of treating len(samples) as the full matching total (page-size undercount).
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
        Optimized query for pathology_file-only filters.
        
        Uses REVERSE query approach:
        1. Match pathology_files with the filter (uses index - FAST)
        2. Find samples related to those files
        3. Do other relationship traversals
        
        This is 10-100x faster than the standard approach of collecting all files first.
        """
        params = {"offset": offset, "limit": limit}
        where_conditions = []
        param_counter = 0
        
        # Build WHERE conditions for pathology_file properties
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if field == "preservation_method":
                # Reject DB-only spellings (e.g. Cytospin Slide, Other); "Unknown" is
                # null-mapped for responses but is a valid API filter, so only
                # is_database_only_value gates this -- not is_null_mapped_value.
                if is_database_only_value("preservation_method", value):
                    logger.info("Invalid preservation_method value (database-only), returning empty results", value=value)
                    return ([], 0) if return_total else []
                reverse_mapped = reverse_map_field_value("preservation_method", value)
                params[param_name] = reverse_mapped if reverse_mapped else value
                if isinstance(params[param_name], list):
                    where_conditions.append(f"pf.fixation_embedding_method IN ${param_name}")
                else:
                    where_conditions.append(f"pf.fixation_embedding_method = ${param_name}")

        # Build WHERE clause for pathology_file filter
        pf_where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
        
        # When return_total: run lightweight count first, then list query
        total_count_pf = None
        if return_total:
            cypher_count = f"""
        MATCH (pf:pathology_file)
        WHERE {pf_where_clause}
        MATCH (pf)-[:of_pathology_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL
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
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
            try:
                total_count_pf = await run_count_query_with_retry(self.session, cypher_count, params)
            except Exception as e_count:
                logger.error("Error in pathology_file reverse count query", error=str(e_count), exc_info=True)
                # Fall through to list query without total_count
        
        # Build optimized reverse query with EARLY PAGINATION
        # Key optimization: Start from pathology_file (uses index), then find samples
        # PERFORMANCE OPTIMIZATION: Paginate samples BEFORE collecting study relationships
        # This significantly reduces memory usage and improves performance for large datasets
        # Flow: (1) Match pf + sa (2) Get distinct samples (3) ORDER BY + SKIP/LIMIT [early pagination]
        #       (4) Collect study IDs (5) UNWIND studies (6) Deduplicate pf per (sa, st)
        #       (7) OPTIONAL MATCH other relationships
        # IMPORTANT: Early pagination - apply SKIP/LIMIT BEFORE unwinding studies
        # to avoid expensive operations on large datasets
        cypher = f"""
        MATCH (pf:pathology_file)
        WHERE {pf_where_clause}
        MATCH (pf)-[:of_pathology_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL
        // Collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, pf, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, pf, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa, pf, 
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, pf, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, pf, sid
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH DISTINCT sa, st
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        WHERE {pf_where_clause}
        WITH sa, st, collect(DISTINCT pf) AS matching_pfs
        WITH sa, st, head(matching_pfs) AS pf
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        WITH sa, p, st,
             head(collect(DISTINCT d)) AS diagnoses,
             pf,
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
                records_count=len(records),
                offset=offset,
                limit=limit,
                params=params,
                sample_ids=[dict(r.get("sa", {})) if r.get("sa") else {} for r in records[:5]]
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
                    
                    # Convert nodes to dictionaries
                    sa = dict(sa_node) if sa_node else {}
                    p = dict(p_node) if p_node else {}
                    st = dict(st_node) if st_node else {}
                    sf = dict(sf_node) if sf_node else {}
                    pf = dict(pf_node) if pf_node else {}
                    diagnoses = dict(diagnoses_node) if diagnoses_node else {}
                    
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
          AND sa.sample_id IS NOT NULL
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid AND st.study_id = $namespace
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
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

    async def get_samples_for_diagnosis_endpoint(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
    ) -> Tuple[List[Sample], int]:
        """
        Dedicated data+total path for /sample-diagnosis endpoint.
        Keeps /sample endpoint behavior untouched.
        """
        result = await self.get_samples(
            filters=filters,
            offset=offset,
            limit=limit,
            base_url=base_url,
            return_total=True,
        )

        if isinstance(result, tuple):
            samples, total_count = result
            return samples, int(total_count or 0)

        # Defensive fallback: repository returned a bare list (count unavailable).
        # Prefer summary over silent total=0 when the page has rows.
        samples = result if isinstance(result, list) else []
        if not samples:
            return [], 0
        try:
            summary = await self.get_samples_summary(filters)
            counts = summary.get("counts") or {}
            # Use `is None` — a legitimate total=0 must not fall through to len(samples).
            raw_total = counts.get("total")
            total = int(raw_total) if raw_total is not None else len(samples)
            return samples, total
        except Exception as e:
            logger.warning(
                "sample-diagnosis: summary fallback after missing total failed",
                error=str(e),
            )
            return samples, len(samples)