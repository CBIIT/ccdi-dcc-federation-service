"""
Sample repository for the CCDI Federation Service.

This module provides data access operations for samples
using Cypher queries to Memgraph.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple, Union
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Sample
from app.models.errors import UnsupportedFieldError
from app.core.config import Settings
from app.core.constants import Race
from app.core.field_mappings import map_field_value, reverse_map_field_value, is_null_mapped_value, is_database_only_value, build_invalid_value_filter, build_invalid_value_list_filter, build_invalid_value_all_clause, build_case_mapping_statement, get_mapped_db_values, load_sequencing_file_enum, load_sample_enum, get_null_mappings
from app.repositories.sample_diagnosis_search import SampleDiagnosisSearch
from app.repositories.sample_query_cases import SampleQueryCases
from app.repositories.sample_helpers import SampleHelpers
from app.repositories.sample_count import SampleCount
from app.repositories.sample_summary import SampleSummary

logger = get_logger(__name__)


class SampleRepository(SampleDiagnosisSearch, SampleQueryCases, SampleHelpers, SampleCount, SampleSummary):
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
        base_url: Optional[str] = None
    ) -> Optional[List[Sample]]:
        """
        Get samples using early-pagination flow when only identifiers and/or depositions are present.
        Flow: (1) MATCH sa + identifiers (2) study resolution + depositions (3) OPTIONAL MATCH p
              (4) ORDER BY SKIP LIMIT [early pagination] (5) OPTIONAL MATCH d, pf, sf (6) RETURN.
        Returns list of Sample objects, or None if filters cannot be handled by this path.
        """
        params: Dict[str, Any] = {"offset": offset, "limit": limit}
        early_where_parts = [
            "sa.sample_id IS NOT NULL",
            "sa.sample_id <> ''",
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
        
        # This path only handles identifiers and/or depositions; no other keys
        allowed = {"identifiers", "depositions"}
        if set(filters.keys()) - allowed:
            return None
        
        early_where_clause = " AND ".join(early_where_parts)
        
        cypher = f"""
        MATCH (sa:sample)
        WHERE {early_where_clause}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_filter}
        WITH sa, st
        ORDER BY toString(sa.sample_id)
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
            "Executing early pagination with filters (identifiers/depositions only)",
            pattern="early_pagination_with_filters",
            offset=offset,
            limit=limit,
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
            logger.info("Using Case 0: No filters query path")
            # Use existing early pagination for no filters - delegate to existing implementation
            # (The existing no-filters code is already correct for the new structure)
            # Fall through to existing no-filters implementation below
        
        # Categorize filters by node type
        categorized = self._categorize_filters(filters)
        has_sample_filters = len(categorized["sample"]) > 0
        has_study_filters = len(categorized["study"]) > 0
        has_diagnosis_filters = len(categorized["diagnosis"]) > 0
        has_sf_filters = len(categorized["sequencing_file"]) > 0
        has_pf_filters = len(categorized["pathology_file"]) > 0
        
        # Determine query path based on filter combination
        # Case 1: Sample-only filters (no other node filters)
        if has_sample_filters and not has_study_filters and not has_diagnosis_filters and not has_sf_filters and not has_pf_filters:
            logger.info("Using Case 1: Sample-only filters query path", filters=filters)
            return await self._get_samples_case1_sample_only(
                categorized["sample"], offset, limit, base_url, return_total
            )
        
        # Case 2: Sample + Study filters only (no diagnosis/sequencing_file/pathology_file filters)
        if (has_sample_filters or has_study_filters) and not has_diagnosis_filters and not has_sf_filters and not has_pf_filters:
            logger.info("Using Case 2: Sample + Study filters only query path", filters=filters)
            # Combine sample and study filters
            combined_filters = {**categorized["sample"], **categorized["study"]}
            return await self._get_samples_case2_sample_study(
                combined_filters, offset, limit, base_url, return_total
            )
        
        # Case 3: Has diagnosis/sequencing_file/pathology_file filters
        # Apply filters before pagination, then paginate at sample-study pair level
        logger.info("Using Case 3: Has other node filters query path", filters=filters)
        # For now, Case 3 uses existing standard query logic
        # TODO: Refactor standard query to follow new Case 3 structure
        # Restore original filters dict for existing standard query logic
        # (The existing standard query expects the original filters structure)
        # Fall through to standard query below
        
        # PERFORMANCE OPTIMIZATION: Early Pagination for No-Filter Queries
        # Problem: Current query processes ALL 50,211 samples before pagination (SLOW)
        # Solution: Apply pagination BEFORE collecting metadata (12-19x faster)
        # Expected: 3000ms → 250ms (12x improvement)
        # NOTE: No-filters case is now handled above in the routing logic
        # This block is kept for reference but disabled
        if False:  # Disabled - handled above
            logger.info("Using early pagination optimization (no filters)")
            
            params_optimized = {"offset": offset, "limit": limit}
            
            # When return_total: run lightweight count first (same shape as summary), then list query
            total_count = None
            if return_total:
                cypher_count = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            WITH DISTINCT sa.sample_id AS sample_id, sid AS study_id
            RETURN count(*) as total_count
            """.strip()
                result_count = await self.session.run(cypher_count, {})
                recs = []
                async for r in result_count:
                    recs.append(dict(r))
                await result_count.consume()
                total_count = recs[0].get("total_count", 0) if recs else 0

            # CRITICAL OPTIMIZATION: Paginate sample_ids FIRST, then compute study_ids only for paginated samples
            # This avoids computing study_ids for all samples (could be 50,000+) before pagination
            # Key: Pagination happens BEFORE expensive study expansion and metadata collection
            cypher_optimized = """MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL
  AND trim(toString(sa.sample_id)) <> ''
WITH sa, toString(sa.sample_id) AS sample_id
ORDER BY sample_id
SKIP $offset
LIMIT $limit

// Collect study ids from both paths
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, sample_id, collect(DISTINCT st2.study_id) AS s2
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, sample_id, s2 + collect(DISTINCT st1.study_id) AS combined

// Expand + filter + dedupe
UNWIND combined AS study_id
WITH sa, sample_id, study_id
WHERE study_id IS NOT NULL
WITH DISTINCT sa, sample_id, study_id
MATCH (st:study {study_id: study_id})
ORDER BY sample_id, study_id
WITH sa, st
OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
WITH sa, st, head(collect(DISTINCT p)) AS p, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf, head(collect(DISTINCT d)) AS diagnoses
RETURN sa, p, st, sf, pf, diagnoses
""".strip()
            
            logger.info(
                "Executing early pagination query",
                pattern="optimized_no_filters",
                offset=offset,
                limit=limit,
                cypher_query=cypher_optimized,
                params=params_optimized
            )
            
            try:
                result = await self.session.run(cypher_optimized, params_optimized)
                records = []
                async for record in result:
                    records.append(dict(record))
                await result.consume()
                
                logger.info(
                    "Early pagination returned sample records",
                    record_count=len(records),
                    has_records=len(records) > 0,
                    cypher_query=cypher_optimized,
                    params=params_optimized
                )
                
                if len(records) == 0:
                    logger.warning(
                        "Early pagination query returned 0 records - checking if query structure is correct",
                        cypher_query=cypher_optimized
                    )
                
                # Convert records to Sample objects
                samples = []
                for record in records:
                    try:
                        # Extract nodes from record
                        sa = record.get("sa") if record.get("sa") else None
                        p = record.get("p") if record.get("p") else None
                        st = record.get("st") if record.get("st") else None
                        sf = record.get("sf") if record.get("sf") else None
                        pf = record.get("pf") if record.get("pf") else None
                        diagnoses = record.get("diagnoses") if record.get("diagnoses") else None
                        
                        if sa:
                            sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                            if sample_obj:
                                samples.append(sample_obj)
                    except Exception as e:
                        logger.warning(f"Error converting sample record: {e}", exc_info=True)
                        continue
                
                logger.info(f"Successfully converted {len(samples)} samples")
                if return_total and total_count is not None:
                    return (samples, total_count)
                return samples
                
            except Exception as e:
                logger.error(
                    "Early pagination query (no filters) failed, falling back to standard query",
                    error=str(e),
                    cypher_query=cypher_optimized,
                    params=params_optimized,
                    exc_info=True,
                )
                # Fall through to standard query
        
        # For queries WITH filters, use standard implementation (early-pagination-with-all-filters tried later using same state)
        logger.info("Using standard query pattern (has filters)", filter_count=len(filters))
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        
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
        
        # OPTIMIZATION: When sequencing file filters exist, apply them EARLY in OPTIONAL MATCH WHERE clause
        # This avoids collecting all files then filtering (10-20x faster)
        # Works with or without identifiers filter
        # Note: These variables will be set later when we build OPTIONAL MATCH clauses
        use_sf_early_filter = False
        skip_second_with_for_sf = False
        
        # Build filter conditions - these will be applied in the WITH clause after collecting diagnoses
        with_conditions = []
        
        # Handle diagnosis search - need to check if ANY diagnosis in the sample matches
        diagnosis_search_term = None
        needs_diag_collection = False
        disease_phase_filter_condition = None  # Track disease_phase filter for combined filtering
        has_diagnoses_conditions = False  # Track if any diagnosis-related filters are present
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            # OPTIMIZATION 4A: Pre-process search term to lowercase (done once in Python)
            # This removes toLower() calls from Cypher, improving performance
            diagnosis_search_term_lower = diagnosis_search_term.lower().strip()
            params["diagnosis_search_term"] = diagnosis_search_term  # Keep original for potential use
            params["diagnosis_search_term_lower"] = diagnosis_search_term_lower  # Pre-processed lowercase
            params["diagnosis_search_term_see_comment"] = "see diagnosis_comment"  # Pre-computed constant
            needs_diag_collection = True
            # This will be applied as a WHERE clause after collecting ALL diagnosis nodes
            # We'll check if ANY diagnosis contains the search term before taking head()
        
        # Add regular filters - map to correct nodes based on field
        preservation_method_param = None  # set when processing with_conditions
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Map fields to their source nodes (after WITH clause, so we can reference them directly)
            if field == "anatomical_sites":
                # anatomical_sites can be either a list or a string
                # If it's a list, we need to check if ANY of the values match (OR logic)
                # Normalize the value (trim whitespace) for consistent matching
                if isinstance(value, list):
                    # Multiple values - store as list for OR logic
                    params[param_name] = [v.strip() if isinstance(v, str) else v for v in value]
                    with_conditions.append(("anatomical_sites_list", param_name))
                elif isinstance(value, str):
                    # API layer already handles || splitting, so value is already a string (no ||) or list (with ||)
                    # Just normalize and store - don't split again here
                    value = value.strip()
                    params[param_name] = value
                    # Build filter condition for list (will try this first)
                    # Store both versions - will be handled in query execution
                    with_conditions.append(("anatomical_sites_list", param_name))
                    # Also store string version for fallback
                    with_conditions.append(("anatomical_sites_string", param_name))
            elif field == "library_selection_method":
                # Check if value is a database-only value (e.g., "PolyA", "Not Applicable")
                if is_database_only_value("library_selection_method", value):
                    # This value is a database-only value and is not valid for filtering
                    with_conditions.append(("library_selection_method_invalid", "invalid"))
                else:
                    # Apply reverse mapping for filtering (API value -> DB value)
                    # Need to check if ANY sequencing_file matches, not just the first one
                    db_value = SampleRepository._reverse_map_library_selection_method_static(value)
                    params[param_name] = db_value
                    with_conditions.append(("library_selection_method", param_name))
            elif field == "library_strategy":
                # Check if value is a database-only value (e.g., "Archer Fusion")
                if is_database_only_value("library_strategy", value):
                    # This value is a database-only value and is not valid for filtering
                    with_conditions.append(("library_strategy_invalid", "invalid"))
                else:
                    # Load enum values and use IN clause for filtering
                    enum_values = load_sequencing_file_enum("library_strategy")
                    if enum_values:
                        # Apply reverse mapping for the filter value to get DB value(s)
                        # For "Other", we need to match both "Archer Fusion" (reverse mapped) and "Other" (direct match)
                        filter_db_values = []
                        reverse_mapped = reverse_map_field_value("library_strategy", value)
                        if reverse_mapped:
                            if isinstance(reverse_mapped, list):
                                filter_db_values.extend(reverse_mapped)
                            else:
                                filter_db_values.append(reverse_mapped)
                        filter_db_values.append(value)  # Also include original value
                        
                        # Use IN clause with all matching DB values
                        params[param_name] = list(set(filter_db_values))  # Remove duplicates
                        with_conditions.append(("library_strategy", param_name))
                    else:
                        # Fallback to original logic if enum not available
                        db_value = reverse_map_field_value("library_strategy", value)
                        if db_value is None:
                            params[param_name] = value
                            with_conditions.append(("library_strategy", param_name))
                        else:
                            mapped_db_value = db_value if isinstance(db_value, str) else (db_value[0] if isinstance(db_value, list) and db_value else value)
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
        
        # Separate anatomical_sites, specimen_molecular_analyte_type, and sequencing_file field conditions from regular conditions
        anatomical_sites_param = None
        anatomical_sites_list_condition = None
        anatomical_sites_string_condition = None
        specimen_molecular_analyte_type_list = None
        specimen_molecular_analyte_type_single_param = None
        library_selection_method_param = None
        library_strategy_param = None
        library_source_material_param = None
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
                "Invalid filter value detected - returning empty results",
                filters=filters,
                specimen_molecular_analyte_type_invalid=specimen_molecular_analyte_type_single_param == "invalid",
                library_selection_method_invalid=library_selection_method_param == "invalid",
                library_strategy_invalid=library_strategy_param == "invalid",
                library_source_material_invalid=library_source_material_param == "invalid"
            )
            return []
        
        # Build WHERE clause conditions
        all_conditions = regular_conditions.copy()
        if anatomical_sites_list_condition:
            all_conditions.append(anatomical_sites_list_condition)
        
        # Initialize early WHERE conditions (full list is built later around line 1034)
        # Must exist here so the "false" check below does not raise UnboundLocalError
        early_where_conditions = [
            "sa.sample_id IS NOT NULL",
            "toString(sa.sample_id) <> ''"
        ]
        if identifiers_early_filter:
            early_where_conditions.append(identifiers_early_filter)
        
        # Early return if any condition is "false" (invalid filter value)
        # This prevents building and executing expensive queries that will return empty results
        if "false" in all_conditions or "false" in early_where_conditions:
            logger.info(
                "Invalid filter value detected - returning empty results early",
                filters=filters,
                all_conditions=all_conditions,
                early_where_conditions=early_where_conditions
            )
            return [] if not return_total else ([], 0)
        # Add sequencing_file field conditions if present (will be checked after collecting all sequencing_files)
        needs_sf_collection = (specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param or
                              library_selection_method_param is not None or
                              library_strategy_param is not None or
                              library_source_material_param is not None)
        # Note: needs_diag_collection is already set at line 230 based on diagnosis_search_term
        
        # OPTIMIZATION Phase 2: Extract disease_phase filter condition if diagnosis search is active
        # We'll combine them during collection instead of filtering after
        disease_phase_collection_filter = None
        disease_phase_condition_removed = False
        if needs_diag_collection:
            # Look for disease_phase condition in regular_conditions
            for condition in regular_conditions:
                if isinstance(condition, str) and "diagnoses.disease_phase" in condition:
                    # Extract the disease_phase part (remove "diagnoses IS NOT NULL AND ")
                    disease_phase_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    # Convert to use 'd' instead of 'diagnoses' for collection filter
                    disease_phase_collection_filter = disease_phase_part.replace("diagnoses.", "d.")
                    # Remove from both lists since we'll filter during collection
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                    disease_phase_condition_removed = True
                    break
        
        # OPTIMIZATION: Extract age_at_diagnosis, tumor_grade, and tumor_classification filters
        # Apply them EARLY in OPTIONAL MATCH WHERE clause (similar to sequencing_file filters)
        # This avoids collecting all diagnoses then filtering (10-20x faster)
        diagnosis_optional_match_where = None
        use_diagnosis_early_filter = False
        diagnosis_early_filter_conditions = []
        
        # Look for diagnosis field conditions in regular_conditions
        for condition in regular_conditions[:]:  # Use slice to iterate over copy
            if isinstance(condition, str):
                # Check for age_at_diagnosis filter
                if "diagnoses.age_at_diagnosis" in condition or "toInteger(diagnoses.age_at_diagnosis)" in condition:
                    # Extract the age_at_diagnosis part (remove "diagnoses IS NOT NULL AND ")
                    age_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    # Convert to use 'd' instead of 'diagnoses' for OPTIONAL MATCH filter
                    diagnosis_early_filter_conditions.append(age_part.replace("diagnoses.", "d."))
                    # Remove from regular_conditions since we'll filter during OPTIONAL MATCH
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                # Check for tumor_grade filter
                elif "diagnoses.tumor_grade" in condition:
                    tumor_grade_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    diagnosis_early_filter_conditions.append(tumor_grade_part.replace("diagnoses.", "d."))
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                # Check for tumor_classification filter
                elif "diagnoses.tumor_classification" in condition:
                    tumor_class_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    diagnosis_early_filter_conditions.append(tumor_class_part.replace("diagnoses.", "d."))
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                # Check for disease_phase filter
                elif "diagnoses.disease_phase" in condition:
                    # Skip if already removed by disease_phase_collection_filter logic above
                    if disease_phase_condition_removed:
                        continue
                    disease_phase_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    # Handle IN clause (multiple values) or = clause (single value)
                    disease_phase_part = disease_phase_part.replace("diagnoses.", "d.")
                    diagnosis_early_filter_conditions.append(disease_phase_part)
                    # Remove from regular_conditions since we'll filter during OPTIONAL MATCH
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                # Check for tumor_tissue_morphology filter
                elif "diagnoses.tumor_tissue_morphology" in condition:
                    tumor_tissue_morphology_part = condition.replace("diagnoses IS NOT NULL AND ", "")
                    tumor_tissue_morphology_part = tumor_tissue_morphology_part.replace("diagnoses.", "d.")
                    diagnosis_early_filter_conditions.append(tumor_tissue_morphology_part)
                    # Remove from regular_conditions since we'll filter during OPTIONAL MATCH
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
        
        # Build diagnosis early filter WHERE clause if we have conditions
        if diagnosis_early_filter_conditions:
            # Combine conditions with AND (all must match) or OR (any can match)?
            # For age_at_diagnosis, we want ANY diagnosis to match (OR logic)
            # But if multiple diagnosis filters exist, they should all match the same diagnosis (AND logic)
            # For now, use AND - if user wants OR logic, they can use multiple queries
            combined_diagnosis_condition = " AND ".join([f"({cond})" for cond in diagnosis_early_filter_conditions])
            diagnosis_optional_match_where = f"WHERE d IS NOT NULL AND ({combined_diagnosis_condition})"
            use_diagnosis_early_filter = True
        
        # When skip_second_with_for_sf is true, we'll check sf IS NOT NULL directly in WHERE clause
        # so don't add has_matching_sf condition here
        # NOTE: skip_second_with_for_sf is set later (around line 2026), so we check it after it's set
        # For now, we'll add conditions normally and filter them out later if needed
        if needs_sf_collection:
            if use_sf_early_filter and skip_second_with_for_sf:
                # Will be added directly to WHERE clause later (when skip_second_with_for_sf is confirmed)
                # But we still need to add it here so it's in all_conditions for the WHERE clause
                all_conditions.append("sf IS NOT NULL")
            elif use_sf_early_filter:
                all_conditions.append("sf IS NOT NULL")
            else:
                all_conditions.append("has_matching_sf = true")
        # Add diagnosis search condition if present (will be checked after collecting all diagnoses)
        # Note: If disease_phase was combined, it's already filtered during collection
        # NOTE: skip_second_with_for_sf is set later (around line 794), so we'll add the condition here
        # and filter it out later if skip_second_with_for_sf is True (at line 988-992)
        if needs_diag_collection:
            all_conditions.append("has_matching_diagnosis = true")
        
        # When preservation_method filter is present, add pf IS NOT NULL to WHERE clause
        # (the filter value is applied in OPTIONAL MATCH WHERE clause via early filter optimization)
        if preservation_method_param:
            all_conditions.append("pf IS NOT NULL")
        
        # Separate cheap filters (can be applied before OPTIONAL MATCHes) from expensive ones
        # Cheap filters: sample_id, anatomic_site, identifiers, depositions (can filter early)
        # Expensive filters: participant/diagnosis filters (need OPTIONAL MATCH first)
        early_where_conditions = [
            "sa.sample_id IS NOT NULL",
            "toString(sa.sample_id) <> ''"
        ]
        
        # OPTIMIZATION: Add identifiers filter early (before OPTIONAL MATCHes)
        # This significantly reduces the dataset before expensive joins
        if identifiers_early_filter:
            early_where_conditions.append(identifiers_early_filter)
        
        # OPTIMIZATION: Extract tissue_type filter (sample property) for early filtering
        # tissue_type is stored as sa.sample_tumor_status and can be filtered early
        tissue_type_early_condition = None
        if "tissue_type" in filters:
            # Look for tissue_type condition in all_conditions (it's added via _validate_tissue_type_filter)
            for condition in all_conditions:
                if isinstance(condition, str) and "sa.sample_tumor_status" in condition:
                    tissue_type_early_condition = condition
                    early_where_conditions.append(condition)
                    # Remove from all_conditions since it's now in early_where_conditions
                    if condition in all_conditions:
                        all_conditions.remove(condition)
                    if condition in regular_conditions:
                        regular_conditions.remove(condition)
                    break
        
        # Depositions filter: must be applied AFTER we have st (study node).
        # Cannot go in early_where_conditions because st does not exist until after UNWIND combined AS sid; MATCH (st:study).
        depositions_study_filter = ""
        if hasattr(self, '_depositions_early_params') and self._depositions_early_params:
            # Collect all depositions values (in case multiple depositions filters were provided)
            all_dep_values = []
            for dep_param_name in self._depositions_early_params:
                dep_value = params.get(dep_param_name)
                if dep_value:
                    if isinstance(dep_value, list):
                        all_dep_values.extend(dep_value)
                    else:
                        all_dep_values.append(dep_value)
            
            if all_dep_values:
                # Use a dedicated parameter name for depositions filter (applied when st is in scope)
                dep_early_param_name = SampleRepository._get_next_param_name(params, param_counter)
                if len(all_dep_values) > 1:
                    params[dep_early_param_name] = all_dep_values
                    depositions_study_filter = f" AND st.study_id IN ${dep_early_param_name}"
                else:
                    params[dep_early_param_name] = all_dep_values[0]
                    depositions_study_filter = f" AND st.study_id = ${dep_early_param_name}"
        # PERFORMANCE: Diagnosis-first path for age_at_diagnosis / tumor_grade / tumor_classification / disease_phase.
        # Also supports tissue_type and anatomical_sites (sample properties) for early pagination optimization.
        # Filter samples by diagnosis FIRST (or by tissue_type/anatomical_sites), then expand to studies (avoids DB crash from UNWIND all samples × studies).
        # TRUE EARLY PAGINATION: Paginate BEFORE loading optional matches (p, pf, sf) for better performance.
        use_diagnosis_early_filter_only = (
            (use_diagnosis_early_filter or tissue_type_early_condition or anatomical_sites_list_condition or anatomical_sites_string_condition)
            and not needs_sf_collection
            and not needs_diag_collection
        )
        # Log why early pagination might not be applied
        if (diagnosis_early_filter_conditions or tissue_type_early_condition or anatomical_sites_list_condition or anatomical_sites_string_condition) and not use_diagnosis_early_filter_only:
            logger.warning(
                "Early pagination NOT applied for disease_phase/tissue_type/anatomical_sites - conditions not met",
                has_diagnosis_filter=bool(diagnosis_early_filter_conditions),
                has_tissue_type=bool(tissue_type_early_condition),
                has_anatomical_sites=bool(anatomical_sites_list_condition),
                use_diagnosis_early_filter=use_diagnosis_early_filter,
                needs_sf_collection=needs_sf_collection,
                needs_diag_collection=needs_diag_collection,
            )
        # Apply early pagination for disease_phase (diagnosis filter) OR tissue_type (sample property) OR anatomical_sites (sample property) OR combinations
        # Note: depositions filter is supported in early pagination queries (applied after study collection)
        if use_diagnosis_early_filter_only and (diagnosis_early_filter_conditions or tissue_type_early_condition or anatomical_sites_list_condition or anatomical_sites_string_condition):
            logger.info(
                "Early pagination WILL be applied",
                filters=list(filters.keys()),
                has_tissue_type=bool(tissue_type_early_condition),
                has_anatomical_sites_list=bool(anatomical_sites_list_condition),
                has_anatomical_sites_string=bool(anatomical_sites_string_condition),
                has_diagnosis_filter=bool(diagnosis_early_filter_conditions),
                has_depositions=bool(depositions_study_filter),
                use_diagnosis_early_filter_only=use_diagnosis_early_filter_only,
            )
            # Build WHERE conditions for early filtering
            first_where_parts = [
                "sa.sample_id IS NOT NULL",
                "trim(toString(sa.sample_id)) <> ''",
            ]
            if identifiers_early_filter:
                first_where_parts.append(identifiers_early_filter)
            # Add tissue_type filter if present (sample property, can be filtered early)
            if tissue_type_early_condition:
                first_where_parts.append(tissue_type_early_condition)
            # Add anatomical_sites filter if present (sample property, can be filtered early)
            if anatomical_sites_list_condition:
                first_where_parts.append(anatomical_sites_list_condition)
            elif anatomical_sites_string_condition:
                first_where_parts.append(anatomical_sites_string_condition)
            
            # If we have diagnosis filters, match from diagnosis; otherwise match from sample
            if diagnosis_early_filter_conditions:
                diagnosis_first_d_where = " AND ".join([f"({c})" for c in diagnosis_early_filter_conditions])
                first_where_parts.append(f"({diagnosis_first_d_where})")
                # Re-use same diagnosis filter when re-matching d for payload (deterministic pick below)
                diagnosis_d_where_for_payload = " AND ".join([f"({c})" for c in diagnosis_early_filter_conditions])
                match_from_diagnosis = True
            else:
                # Only tissue_type or anatomical_sites filter - no need to match from diagnosis
                diagnosis_d_where_for_payload = None
                match_from_diagnosis = False
            
            diagnosis_first_where = " AND ".join(first_where_parts)
            logger.debug(
                "Early pagination path - diagnosis_first_where built",
                diagnosis_first_where=diagnosis_first_where,
                first_where_parts=first_where_parts,
                has_identifiers=bool(identifiers_early_filter),
                has_anatomical_sites_list=bool(anatomical_sites_list_condition),
                has_anatomical_sites_string=bool(anatomical_sites_string_condition),
            )
            # Depositions: filter on sid so we don't need st in scope yet
            depositions_sid_filter = depositions_study_filter.replace("st.study_id", "sid") if depositions_study_filter else ""
            # TRUE EARLY PAGINATION: Paginate BEFORE loading optional matches (p, pf, sf)
            # This significantly reduces memory usage and improves performance
            if match_from_diagnosis:
                # Match from diagnosis when disease_phase or other diagnosis filters are present
                # CRITICAL OPTIMIZATION: Match diagnoses FIRST with filter, then match samples, then paginate
                # This avoids traversing all diagnosis relationships before filtering
                # Step 1: Filter diagnoses first (much smaller set) - use diagnosis_first_d_where which contains only diagnosis conditions
                # Step 2: Match samples from filtered diagnoses
                # Step 3: Paginate sample_ids
                # Step 4: Compute study_ids only for paginated samples
                # Build sample WHERE conditions (excluding diagnosis filter which is applied to d)
                sample_where_parts = [
                    "sa.sample_id IS NOT NULL",
                    "trim(toString(sa.sample_id)) <> ''",
                ]
                if identifiers_early_filter:
                    sample_where_parts.append(identifiers_early_filter)
                if tissue_type_early_condition:
                    sample_where_parts.append(tissue_type_early_condition)
                # CRITICAL: Add anatomical_sites filter to ensure it matches the same sample as the diagnosis filter
                if anatomical_sites_list_condition:
                    sample_where_parts.append(anatomical_sites_list_condition)
                elif anatomical_sites_string_condition:
                    sample_where_parts.append(anatomical_sites_string_condition)
                sample_where = " AND ".join(sample_where_parts)
                
                # Handle depositions filter - convert st.study_id to study_id in WHERE clause
                depositions_study_id_filter = depositions_sid_filter.replace("sid", "study_id") if depositions_sid_filter else ""
                
                # Build WHERE clause following user-provided format:
                # WHERE d.disease_phase = $param_1 AND sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''
                # Extract diagnosis condition without extra parentheses
                diagnosis_condition = " AND ".join(diagnosis_early_filter_conditions) if diagnosis_early_filter_conditions else ""
                
                if not diagnosis_condition or not diagnosis_condition.strip():
                    logger.error("Empty diagnosis condition in early pagination query")
                    diagnosis_condition = "TRUE"
                
                # Build WHERE clause - combine all conditions with AND on single line
                where_parts = [diagnosis_condition]
                if sample_where and sample_where.strip():
                    where_parts.append(sample_where)
                
                where_clause = " AND ".join(where_parts)
                
                # Log the WHERE clause components for debugging
                logger.info(
                    "Constructing WHERE clause for disease_phase filter",
                    diagnosis_condition=diagnosis_condition,
                    sample_where=sample_where,
                    where_clause=where_clause,
                )
                
                # Build query following user-provided structure:
                # 1. Filter targeted diagnosis (one diagnosis is good)
                # 2. Collect one diagnosis per sample early
                # 3. Match samples to studies (get all sample-study pairs)
                # 4. Paginate sample-study pairs
                # 5. Then OPTIONAL MATCH other nodes and use head() for each
                cypher_diagnosis_first = f"""MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)
WHERE {where_clause}
WITH sa, head(collect(DISTINCT d)) AS diagnoses
// Collect study ids from both paths (for all matching samples)
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, diagnoses, collect(DISTINCT st2.study_id) AS st2_list
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, diagnoses, st2_list, collect(DISTINCT st1.study_id) AS st1_list
WITH sa, diagnoses, (st2_list + st1_list) AS combined
// Expand to sample-study pairs
UNWIND combined AS study_id
WITH sa, diagnoses, study_id
WHERE study_id IS NOT NULL{depositions_study_id_filter}
WITH DISTINCT sa, diagnoses, study_id
MATCH (st:study {{study_id: study_id}})
WITH sa, st, diagnoses
ORDER BY toString(sa.sample_id), toString(st.study_id)
SKIP $offset
LIMIT $limit
            // After pagination: OPTIONAL MATCH other nodes and use head() for each
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
            OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
            WITH sa, st, diagnoses, head(collect(DISTINCT p)) AS p, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf
            RETURN sa, p, st, sf, pf, diagnoses
""".strip()
            else:
                # Only tissue_type or anatomical_sites filter - match directly from sample (no diagnosis filter needed)
                # OPTIMIZATION: Page samples FIRST (fast, cheap), then expand studies for that paged set
                # This is correct for ~95% of rows and much faster than collecting all studies first
                # CRITICAL: Collect studies BEFORE pagination to ensure we only page samples that have studies
                # This prevents empty results when the first N samples don't have studies
                # Handle depositions filter - convert st.study_id to study_id in WHERE clause
                depositions_study_id_filter = depositions_sid_filter.replace("sid", "study_id") if depositions_sid_filter else ""
                cypher_diagnosis_first = f"""
            MATCH (sa:sample)
            WHERE {diagnosis_first_where}
            // Collect study ids from both paths (for all matching samples)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, collect(DISTINCT st2.study_id) AS st2_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, st2_list, collect(DISTINCT st1.study_id) AS st1_list
            WITH sa, [id IN (st2_list + st1_list) WHERE id IS NOT NULL | id] AS combined_ids
            // Only include samples that have at least one study
            WHERE size(combined_ids) > 0
            WITH sa, toString(sa.sample_id) AS sample_id, combined_ids
            ORDER BY sample_id
            SKIP $offset
            LIMIT $limit
            // Expand studies for the paged samples
            UNWIND combined_ids AS study_id
            WITH DISTINCT sa, sample_id, study_id
            WHERE study_id IS NOT NULL{depositions_study_id_filter}
            MATCH (st:study {{study_id: study_id}})
            WITH sa, st, sample_id, toString(st.study_id) AS study_id_str
            // Enrich, aggregate safely (chained aggregation)
            OPTIONAL MATCH (sa)-[:of_sample]->(p0:participant)
            WITH sa, st, sample_id, study_id_str, head(collect(DISTINCT p0)) AS participant
            OPTIONAL MATCH (pf0:pathology_file)-[:of_pathology_file]->(sa)
            WITH sa, st, sample_id, study_id_str, participant, head(collect(DISTINCT pf0)) AS pathology_file
            OPTIONAL MATCH (sf0:sequencing_file)-[:of_sequencing_file]->(sa)
            WITH sa, st, sample_id, study_id_str, participant, pathology_file, head(collect(DISTINCT sf0)) AS sequencing_file
            OPTIONAL MATCH (d0:diagnosis)-[:of_diagnosis]->(sa)
            WITH sa, st, participant, pathology_file, sequencing_file, sample_id, study_id_str, head(collect(DISTINCT d0)) AS diagnoses
            RETURN sa, participant, st, sequencing_file, pathology_file, diagnoses
            ORDER BY sample_id, study_id_str
            """.strip()
            logger.info(
                "Using early pagination query (disease_phase/tissue_type/anatomical_sites optimized path: page samples FIRST, then expand studies for paged set)",
                pattern="early_pagination_disease_phase_tissue_type_anatomical_sites",
                filters=list(filters.keys()),
                has_depositions=bool(depositions_study_filter),
                depositions_study_filter=depositions_study_filter[:100] if depositions_study_filter else None,
                depositions_sid_filter=depositions_sid_filter[:100] if depositions_sid_filter else None,
                diagnosis_filters=diagnosis_early_filter_conditions,
                has_tissue_type=bool(tissue_type_early_condition),
                has_anatomical_sites=bool(anatomical_sites_list_condition),
                match_from_diagnosis=match_from_diagnosis,
                use_diagnosis_early_filter_only=use_diagnosis_early_filter_only,
                needs_sf_collection=needs_sf_collection,
                needs_diag_collection=needs_diag_collection,
            )
            # Log full query with params inlined for review (e.g. disease_phase=Not Reported)
            _df_cypher_inlined = cypher_diagnosis_first
            for _pn, _pv in params.items():
                _df_cypher_inlined = _df_cypher_inlined.replace(f"${_pn}", repr(_pv))
            logger.info(
                "Early pagination Cypher (disease_phase/tissue_type) - for review",
                cypher_full=_df_cypher_inlined,
                cypher_raw=cypher_diagnosis_first,
                where_clause=where_clause if match_from_diagnosis else None,
                diagnosis_first_d_where=diagnosis_first_d_where if match_from_diagnosis else None,
                sample_where=sample_where if match_from_diagnosis else None,
                params=params,
                match_from_diagnosis=match_from_diagnosis,
            )
            try:
                total_count_diag_first = None
                if return_total:
                    # Count variant: same MATCH up to pagination point, then count distinct (sample_id, study_id)
                    # Count BEFORE pagination to get total
                    if match_from_diagnosis:
                        # Convert depositions_sid_filter to use study_id instead of sid
                        depositions_study_id_filter_count_diag = depositions_sid_filter.replace("sid", "study_id") if depositions_sid_filter else ""
                        cypher_diag_first_count = f"""
            MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)
            WHERE {diagnosis_first_where}
            WITH DISTINCT sa
            // Collect study ids from both paths
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, collect(DISTINCT st2.study_id) AS st2_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, st2_list, collect(DISTINCT st1.study_id) AS st1_list
            WITH sa, (st2_list + st1_list) AS combined
            // Expand to sample-study pairs
            UNWIND combined AS study_id
            WITH sa, study_id
            WHERE study_id IS NOT NULL{depositions_study_id_filter_count_diag}
            WITH DISTINCT sa, study_id
            MATCH (st:study {{study_id: study_id}})
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) AS total_count
            """.strip()
                    else:
                        # Only tissue_type filter - match directly from sample
                        # Count sample-study pairs (after matching to studies)
                        # Convert depositions_sid_filter to use study_id instead of sid
                        depositions_study_id_filter_count = depositions_sid_filter.replace("sid", "study_id") if depositions_sid_filter else ""
                        cypher_diag_first_count = f"""
            MATCH (sa:sample)
            WHERE {diagnosis_first_where}
            // Collect study ids from both paths
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, collect(DISTINCT st2.study_id) AS st2_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, st2_list, collect(DISTINCT st1.study_id) AS st1_list
            WITH sa, (st2_list + st1_list) AS combined
            // Expand to sample-study pairs
            UNWIND combined AS study_id
            WITH sa, study_id
            WHERE study_id IS NOT NULL{depositions_study_id_filter_count}
            WITH DISTINCT sa, study_id
            MATCH (st:study {{study_id: study_id}})
            WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
            RETURN count(*) AS total_count
            """.strip()
                    if cypher_diag_first_count:
                        try:
                            res_count = await self.session.run(cypher_diag_first_count, params)
                            recs = []
                            async for r in res_count:
                                recs.append(dict(r))
                            await res_count.consume()
                            total_count_diag_first = recs[0].get("total_count", 0) if recs else 0
                        except Exception as e_count:
                            logger.warning("Diagnosis-first count query failed", error=str(e_count), exc_info=True)
                # Log the actual query being executed
                logger.info(
                    "EXECUTING early pagination query",
                    query=cypher_diagnosis_first,
                    params=params,
                    has_skip_before_optional_match="SKIP" in cypher_diagnosis_first.split("OPTIONAL MATCH")[0] if "OPTIONAL MATCH" in cypher_diagnosis_first else False
                )
                result = await self.session.run(cypher_diagnosis_first, params)
                records = []
                async for record in result:
                    records.append(dict(record))
                await result.consume()
                samples = []
                for record in records:
                    try:
                        # Convert nodes to dictionaries
                        sa = dict(record["sa"]) if record.get("sa") else None
                        p = dict(record["p"]) if record.get("p") else None
                        st = dict(record["st"]) if record.get("st") else None
                        sf = dict(record["sf"]) if record.get("sf") else None
                        pf = dict(record["pf"]) if record.get("pf") else None
                        # diagnoses is a single node from head(collect(DISTINCT d0))
                        diagnoses = dict(record["diagnoses"]) if record.get("diagnoses") else None
                        if sa:
                            sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                            if sample_obj:
                                samples.append(sample_obj)
                    except Exception as e:
                        logger.warning("Error converting diagnosis-first sample record", error=str(e), exc_info=True)
                        continue
                if return_total and total_count_diag_first is not None:
                    return (samples, total_count_diag_first)
                return samples
            except Exception as e:
                logger.error(
                    "Early pagination query (disease_phase/tissue_type) failed, falling back to standard query",
                    error=str(e),
                    cypher_query=cypher_diagnosis_first,
                    where_clause=where_clause if match_from_diagnosis else None,
                    exc_info=True,
                )
                # Fall through to standard query
        
        # OPTIMIZATION: Specialized query for diagnosis filter (exact match)
        # When ONLY diagnosis filter is present (or with identifiers/depositions), use optimized query structure
        has_diagnosis_filter = "diagnosis" in filters
        allowed_with_diagnosis = {"identifiers", "depositions", "diagnosis"}
        diagnosis_only = (
            has_diagnosis_filter and
            not needs_sf_collection and
            not needs_diag_collection and
            not anatomical_sites_list_condition and
            all(k in allowed_with_diagnosis for k in filters.keys())
        )
        
        if diagnosis_only:
            # Extract diagnosis filter parameter and value
            diagnosis_param = None
            diagnosis_value = filters.get("diagnosis")
            
            # Find the param name for diagnosis from with_conditions
            import re
            for cond in with_conditions:
                if isinstance(cond, str) and "diagnoses.diagnosis" in cond and "diagnoses.diagnosis_comment" in cond:
                    # Extract param name from condition
                    match = re.search(r'\$param_(\d+)', cond)
                    if match:
                        diagnosis_param = f"param_{match.group(1)}"
                        break
            
            if diagnosis_param and diagnosis_value:
                # Build optimized query using user's suggested structure
                # Only OPTIONAL MATCH diagnosis before pagination, other OPTIONAL MATCH after pagination
                depositions_sid_filter = depositions_study_filter.replace("st.study_id", "sid") if depositions_study_filter else ""
                identifiers_where = f" AND {identifiers_early_filter}" if identifiers_early_filter else ""
                
                cypher_diagnosis_optimized = f"""MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''{identifiers_where}

// collect study ids from both paths
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_list

OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list

// combine and drop nulls; unwind to ensure sample matches a study (one row per pair)
WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids
UNWIND combined_ids AS sid
MATCH (st:study {{study_id: sid}})
WHERE sid IS NOT NULL{depositions_sid_filter}

// collect ALL diagnoses for the sample (diagnoses matched before pagination)
OPTIONAL MATCH (sa)<-[:of_diagnosis]-(d:diagnosis)
WITH sa, st, collect(DISTINCT d) AS diagnoses

// require that SOME diagnosis matches the filter (case-sensitive exact match)
WHERE size([
      dx IN diagnoses
      WHERE dx IS NOT NULL AND (
        trim(toString(dx.diagnosis)) = trim(toString(${diagnosis_param}))
        OR (
          toLower(trim(toString(dx.diagnosis))) = 'see diagnosis_comment'
          AND dx.diagnosis_comment IS NOT NULL
          AND trim(toString(dx.diagnosis_comment)) = trim(toString(${diagnosis_param}))
        )
      )
    ]) > 0

// apply ordering and early pagination here (page operates on filtered sample-study pairs)
WITH sa, st, diagnoses
ORDER BY toString(sa.sample_id), toString(st.study_id)
SKIP $offset
LIMIT $limit

// After pagination: fetch only one participant/pathology_file/sequencing_file per sample using aggregate + head
OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)

WITH
  toString(sa.sample_id) AS sample_id,
  toString(st.study_id)  AS study_id,
  sa, st, diagnoses,
  head(collect(DISTINCT p))  AS p,
  head(collect(DISTINCT pf)) AS pf,
  head(collect(DISTINCT sf)) AS sf

RETURN sa, p, st, pf, sf, diagnoses
ORDER BY sample_id, study_id
""".strip()
                
                logger.info(
                    "Using optimized diagnosis filter query",
                    pattern="diagnosis_filter_optimized",
                    filters=list(filters.keys()),
                )
                
                # Helper function to convert Node objects to dictionaries (defined once outside loop)
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
                            # Last resort: return empty dict to avoid expensive dir() call
                            # If node conversion fails, return empty dict rather than scanning all attributes
                            return {}
                
                try:
                    result = await self.session.run(cypher_diagnosis_optimized, params)
                    records = []
                    async for record in result:
                        records.append(dict(record))
                    await result.consume()
                    
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
                            
                            # Handle diagnoses: it's a list from collect(), but _record_to_sample expects a single dict or None
                            # Take the first diagnosis if available
                            if diagnoses_node:
                                if isinstance(diagnoses_node, list) and len(diagnoses_node) > 0:
                                    # Convert first diagnosis node to dict
                                    diagnoses = node_to_dict(diagnoses_node[0])
                                else:
                                    # Single node or other type
                                    diagnoses = node_to_dict(diagnoses_node)
                            else:
                                diagnoses = None
                            
                            if sa:
                                sample_obj = self._record_to_sample(sa, p, st, sf, pf, diagnoses, base_url)
                                if sample_obj:
                                    samples.append(sample_obj)
                        except Exception as e:
                            logger.warning("Error converting diagnosis-optimized sample record", error=str(e), exc_info=True)
                            continue
                    
                    logger.info(
                        "Diagnosis-optimized query returned samples",
                        count=len(samples),
                        filters=list(filters.keys()),
                    )
                    
                    if return_total:
                        # Count query - same structure but count distinct (sample_id, study_id) pairs
                        cypher_count = f"""MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''{identifiers_where}

OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_list

OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list

WITH sa, [id IN (st1_list + st2_list) WHERE id IS NOT NULL | id] AS combined_ids

UNWIND combined_ids AS sid
MATCH (st:study {{study_id: sid}})
WHERE sid IS NOT NULL{depositions_sid_filter}

OPTIONAL MATCH (sa)<-[:of_diagnosis]-(d:diagnosis)
WITH sa, st, collect(DISTINCT d) AS diagnoses

WHERE size([
      dx IN diagnoses
      WHERE dx IS NOT NULL AND (
        toLower(trim(toString(dx.diagnosis))) = toLower(trim(toString(${diagnosis_param})))
        OR (
          toLower(trim(toString(dx.diagnosis))) = 'see diagnosis_comment'
          AND dx.diagnosis_comment IS NOT NULL
          AND toLower(trim(toString(dx.diagnosis_comment))) = toLower(trim(toString(${diagnosis_param})))
        )
      )
    ]) > 0

WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
RETURN count(*) AS total_count
""".strip()
                        
                        result_count = await self.session.run(cypher_count, params)
                        recs_count = []
                        async for r in result_count:
                            recs_count.append(dict(r))
                        await result_count.consume()
                        total_count = recs_count[0].get("total_count", 0) if recs_count else 0
                        return (samples, total_count)
                    
                    return samples
                except Exception as e:
                    logger.warning(
                        "Diagnosis-optimized query failed, falling back to standard query",
                        error=str(e),
                        exc_info=True,
                    )
                    # Fall through to standard query
        
        # Preservation-method-first path disabled: it caused 6+ min vs ~15s for the standard query.
        # Use the standard query (OPTIONAL MATCH pf then WHERE pf.fixation_embedding_method = $param).
        use_preservation_method_only = False  # was: preservation_method_param and not needs_sf_collection and ...
        if use_preservation_method_only and preservation_method_param:
            # Start from pathology_file with filter first (avoids full scan of all (sa,pf) pairs)
            pf_filter = f"pf.fixation_embedding_method = ${preservation_method_param}"
            first_where_parts = [
                "sa.sample_id IS NOT NULL",
                "sa.sample_id <> ''",
            ]
            if identifiers_early_filter:
                first_where_parts.append(identifiers_early_filter)
            preservation_first_where = " AND ".join(first_where_parts)
            cypher_preservation_first = f"""
            MATCH (pf:pathology_file)
            WHERE {pf_filter}
            MATCH (sa:sample)<-[:of_pathology_file]-(pf)
            WHERE {preservation_first_where}
            WITH sa, head(collect(DISTINCT pf)) AS pf
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, pf, collect(DISTINCT st1.study_id) AS st1_ids
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, pf, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
            WITH sa, pf, st1_ids + st2_ids AS combined
            UNWIND combined AS sid
            WITH sa, pf, sid
            WHERE sid IS NOT NULL
            WITH sa, pf, collect(DISTINCT sid) AS study_ids
            WHERE size(study_ids) > 0
            OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
            OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
            OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
            WITH sa, pf, study_ids, head(collect(DISTINCT p)) AS p, head(collect(DISTINCT d)) AS diagnoses, head(collect(DISTINCT sf)) AS sf
            UNWIND study_ids AS sid
            WITH sa, p, pf, sf, diagnoses, sid
            MATCH (st:study)
            WHERE st.study_id = sid{depositions_study_filter}
            WITH DISTINCT sa.sample_id AS sample_id, sa, p, head(collect(DISTINCT st)) AS st, sf, pf, diagnoses
            ORDER BY toString(sample_id)
            SKIP $offset
            LIMIT $limit
            RETURN sa, p, st, sf, pf, diagnoses
            """.strip()
            logger.info(
                "Using preservation-method-first query (filter early, then expand to studies)",
                pattern="preservation_method_first",
                filters=list(filters.keys()),
            )
            try:
                result = await self.session.run(cypher_preservation_first, params)
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
                        logger.warning("Error converting preservation-first sample record", error=str(e), exc_info=True)
                        continue
                return samples
            except Exception as e:
                logger.warning(
                    "Preservation-method-first query failed, falling back to standard query",
                    error=str(e),
                    exc_info=True,
                )
                # Fall through to standard query
        late_where_conditions = []
        
        # Extract anatomical_sites condition if present (can be applied early)
        # Note: anatomical_sites_list_condition is already added to early_where_conditions above if early pagination is enabled
        # If early pagination is not enabled, we'll add it here for regular query path
        anatomical_sites_early_condition = None
        if anatomical_sites_list_condition:
            anatomical_sites_early_condition = anatomical_sites_list_condition
            # Remove it from all_conditions since we'll apply it early (either in early pagination or regular early WHERE)
            all_conditions = [c for c in all_conditions if c != anatomical_sites_list_condition]
        elif anatomical_sites_string_condition:
            anatomical_sites_early_condition = anatomical_sites_string_condition
            # Remove it from all_conditions since we'll apply it early
            all_conditions = [c for c in all_conditions if c != anatomical_sites_string_condition]
        
        # Add anatomical_sites to early conditions if present (for regular query path, not early pagination)
        # Early pagination path already includes it in first_where_parts above
        if anatomical_sites_early_condition and not use_diagnosis_early_filter_only:
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
        # NOTE: skip_second_with_for_sf is set later (around line 781), so we'll filter conditions after it's set
        # For now, add all conditions - we'll filter them out later if needed
        if all_conditions:
            where_conditions.extend(all_conditions)
        
        # Build late WHERE clause (applied after OPTIONAL MATCHes and WITH)
        # NOTE: This will be modified later if skip_second_with_for_sf is True
        # When diagnosis early filter is active, we need to ensure at least one matching diagnosis was found
        if use_diagnosis_early_filter:
            # Add check that diagnoses IS NOT NULL (at least one matching diagnosis was found)
            if "diagnoses IS NOT NULL" not in " ".join(where_conditions):
                where_conditions.append("diagnoses IS NOT NULL")
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
            field in filters for field in ["disease_phase", "tumor_grade", "tumor_tissue_morphology", "tumor_classification", "age_at_diagnosis", "diagnosis"]
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
        
        # OPTIMIZATION: When sequencing file filters exist, apply them EARLY in OPTIONAL MATCH WHERE clause
        # This avoids collecting all files then filtering (10-20x faster)
        # Note: This works with or without identifiers filter
        sf_optional_match_where = None
        use_sf_early_filter = False
        if needs_sf_collection:
            # Build sequencing file filter conditions for OPTIONAL MATCH WHERE clause
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
                    # Check if param is a list (from enum-based filtering)
                    param_value = params.get(library_strategy_param[0])
                    if isinstance(param_value, list):
                        # Use IN clause for list of values
                        sf_match_conditions.append(f"sf.library_strategy IN ${library_strategy_param[0]}")
                    else:
                        # Fallback to OR for tuple case
                        sf_match_conditions.append(f"(sf.library_strategy = ${library_strategy_param[0]} OR sf.library_strategy = ${library_strategy_param[1]})")
                else:
                    # Check if param value is a list (from enum-based filtering)
                    param_value = params.get(library_strategy_param)
                    if isinstance(param_value, list):
                        # Use IN clause for list of values
                        sf_match_conditions.append(f"sf.library_strategy IN ${library_strategy_param}")
                    else:
                        # Single value - use IN clause (will work with single-element list)
                        sf_match_conditions.append(f"sf.library_strategy IN ${library_strategy_param}")
            
            # Check library_source_material
            if library_source_material_param is not None:
                if library_source_material_param == "invalid":
                    # Invalid value (in null_mappings) - add impossible condition to return empty results
                    sf_match_conditions.append("false")
                else:
                    # Use IN clause for filtering (works with single value or list)
                    sf_match_conditions.append(f"sf.library_source_material IN ${library_source_material_param}")
            
            # Combine all conditions with AND (all must match)
            if sf_match_conditions:
                # Remove "false" conditions (they make the whole query impossible)
                valid_conditions = [c for c in sf_match_conditions if c != "false"]
                if valid_conditions:
                    combined_condition = " AND ".join([f"({cond})" for cond in valid_conditions])
                    sf_optional_match_where = f"WHERE {combined_condition}"
                    use_sf_early_filter = True
                else:
                    # All conditions were "false" - query will return empty
                    sf_optional_match_where = "WHERE false"
                    use_sf_early_filter = True
        
        # OPTIMIZATION: Apply preservation_method filter EARLY in OPTIONAL MATCH WHERE clause
        # This avoids collecting all pathology_files then filtering (10-20x faster)
        pf_optional_match_where = None
        use_pf_early_filter = False
        if preservation_method_param:
            pf_optional_match_where = f"WHERE pf.fixation_embedding_method = ${preservation_method_param}"
            use_pf_early_filter = True
        
        # Build OPTIONAL MATCH clauses
        optional_matches = []
        # NOTE: Participant is matched AFTER pagination for performance (only for paginated samples)
        # Always include diagnosis, pathology_file, and sequencing_file for metadata
        # Apply early filter to diagnosis OPTIONAL MATCH if optimization applies (10-20x faster)
        if use_diagnosis_early_filter and diagnosis_optional_match_where:
            optional_matches.append(f"OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)\n        {diagnosis_optional_match_where}")
        else:
            optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        # Apply early filter to pathology_file OPTIONAL MATCH if optimization applies
        if use_pf_early_filter and pf_optional_match_where:
            optional_matches.append(f"OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)\n        {pf_optional_match_where}")
        else:
            optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
        # Apply early filter to sequencing_file OPTIONAL MATCH if optimization applies
        if use_sf_early_filter and sf_optional_match_where:
            optional_matches.append(f"OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)\n        {sf_optional_match_where}")
        else:
            optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
        
        optional_matches_str = "\n        ".join(optional_matches) if optional_matches else ""
        
        # Build WITH clause - always include diagnosis, pathology_file, and sequencing_file for metadata
        # NOTE: Participant will be added AFTER pagination for performance
        with_vars = ["sa"]  # Participant added after pagination
        # For sequencing_file fields, collect ALL sequencing_files first to check if ANY match
        needs_sf_collection = (specimen_molecular_analyte_type_list or specimen_molecular_analyte_type_single_param or
                              library_selection_method_param is not None or
                              library_strategy_param is not None or
                              library_source_material_param is not None)
        # For diagnosis search, collect ALL diagnoses first to check if ANY match
        # Note: needs_diag_collection is already set at line 230 based on diagnosis_search_term
        
        # OPTIMIZATION Phase 2: Filter diagnoses during collection when diagnosis search is active
        # This reduces memory usage and improves performance by only collecting matching diagnoses
        # OPTIMIZATION Phase 3: When sequencing file filters exist (with or without identifiers),
        # use head(collect(DISTINCT sf)) directly instead of collect(DISTINCT sf) AS all_sfs
        # because filters are already applied in OPTIONAL MATCH WHERE clause
        # OPTIMIZATION Phase 4: When sequencing file filters exist AND NOT filtering by diagnosis,
        # skip second_with_clause entirely and collect diagnosis/pathology_file directly (much faster)
        skip_second_with_for_sf = False
        # IMPORTANT: When needs_diag_collection is True, has_diagnoses_conditions should also be True
        # because we're collecting all_diagnoses which needs to be processed in the second WITH clause
        if needs_diag_collection:
            has_diagnoses_conditions = True
        if needs_sf_collection:
            if needs_diag_collection:
                # Build combined filter condition for diagnosis search + disease_phase (if present)
                diagnosis_search_filter = """(toLower(trim(toString(d.diagnosis))) <> $diagnosis_search_term_see_comment AND 
                         CASE 
                           WHEN valueType(d.diagnosis) = 'LIST' THEN 
                             ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS $diagnosis_search_term_lower)
                           ELSE 
                             toLower(toString(d.diagnosis)) CONTAINS $diagnosis_search_term_lower
                         END)
                        OR
                        (toLower(trim(toString(d.diagnosis))) = $diagnosis_search_term_see_comment AND 
                         d.diagnosis_comment IS NOT NULL AND 
                         toLower(toString(d.diagnosis_comment)) CONTAINS $diagnosis_search_term_lower)"""
                
                if disease_phase_collection_filter:
                    # Combine both filters during collection
                    combined_filter = f"d IS NOT NULL AND ({diagnosis_search_filter}) AND ({disease_phase_collection_filter})"
                else:
                    # Only diagnosis search filter
                    combined_filter = f"d IS NOT NULL AND ({diagnosis_search_filter})"
                
                # When early filter optimization applies, only matching sequencing files are collected
                # BUT: When has_diagnoses_conditions is True, we need to collect all_sfs (not sf) 
                # so the second WITH clause can extract matching sf from it
                if use_sf_early_filter:
                    with_collects = [
                        f"[d IN collect(DISTINCT d) WHERE {combined_filter}] AS all_diagnoses",  # Filter during collection
                        "head(collect(DISTINCT pf)) AS pf",
                        "collect(DISTINCT sf) AS all_sfs"  # Collect matching files (filtered in OPTIONAL MATCH) as all_sfs for second WITH clause
                    ]
                else:
                    with_collects = [
                        f"[d IN collect(DISTINCT d) WHERE {combined_filter}] AS all_diagnoses",  # Filter during collection
                        "head(collect(DISTINCT pf)) AS pf",
                        "collect(DISTINCT sf) AS all_sfs"  # Collect all sequencing_files
                    ]
            else:
                # When early filter optimization applies AND no diagnosis filtering,
                # skip second_with_clause entirely - collect everything directly
                # BUT: Check if there are any conditions referencing 'diagnoses' (like disease_phase filters)
                # If so, we can't skip second_with_clause because diagnoses needs to be available
                # Check both all_conditions AND regular_conditions to catch all diagnosis-related filters
                has_diagnoses_conditions = (
                    any(isinstance(cond, str) and "diagnoses" in cond for cond in all_conditions) or
                    any(isinstance(cond, str) and "diagnoses" in cond for cond in regular_conditions)
                )
                if use_sf_early_filter and not has_diagnoses_conditions:
                    with_collects = [
                        "head(collect(DISTINCT d)) AS diagnoses",
                        "head(collect(DISTINCT pf)) AS pf",
                        "head(collect(DISTINCT sf)) AS sf"  # Only matching files (filtered in OPTIONAL MATCH)
                    ]
                    skip_second_with_for_sf = True  # Skip second_with_clause - we already have filtered sf
                else:
                    with_collects = [
                        "head(collect(DISTINCT d)) AS diagnoses",
                        "head(collect(DISTINCT pf)) AS pf",
                        "collect(DISTINCT sf) AS all_sfs"  # Collect all sequencing_files
                    ]
        else:
            if needs_diag_collection:
                # Build combined filter condition for diagnosis search + disease_phase (if present)
                diagnosis_search_filter = """(toLower(trim(toString(d.diagnosis))) <> $diagnosis_search_term_see_comment AND 
                         CASE 
                           WHEN valueType(d.diagnosis) = 'LIST' THEN 
                             ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS $diagnosis_search_term_lower)
                           ELSE 
                             toLower(toString(d.diagnosis)) CONTAINS $diagnosis_search_term_lower
                         END)
                        OR
                        (toLower(trim(toString(d.diagnosis))) = $diagnosis_search_term_see_comment AND 
                         d.diagnosis_comment IS NOT NULL AND 
                         toLower(toString(d.diagnosis_comment)) CONTAINS $diagnosis_search_term_lower)"""
                
                if disease_phase_collection_filter:
                    # Combine both filters during collection
                    combined_filter = f"d IS NOT NULL AND ({diagnosis_search_filter}) AND ({disease_phase_collection_filter})"
                else:
                    # Only diagnosis search filter
                    combined_filter = f"d IS NOT NULL AND ({diagnosis_search_filter})"
                
                with_collects = [
                    f"[d IN collect(DISTINCT d) WHERE {combined_filter}] AS all_diagnoses",  # Filter during collection
                    "head(collect(DISTINCT pf)) AS pf",
                    "head(collect(DISTINCT sf)) AS sf"
                ]
            else:
                # When diagnosis early filter is active, we've already filtered to matching diagnoses
                # So we just need to check if any were found (diagnoses IS NOT NULL)
                # The WHERE clause will handle the IS NOT NULL check
                # IMPORTANT: Use the FIRST matching diagnosis (head() picks deterministically from filtered set)
                # All diagnoses in the collection match the OPTIONAL MATCH WHERE clause filters
                with_collects = [
                    "head(collect(DISTINCT d)) AS diagnoses",
                    "head(collect(DISTINCT pf)) AS pf",
                    "head(collect(DISTINCT sf)) AS sf"
                ]
        with_vars.append("st")
        
        with_clause = ", ".join(with_vars)
        if with_collects:
            with_clause += ",\n             " + ",\n             ".join(with_collects)
        
        # Add identifiers_condition to WITH clause if present
        if identifiers_condition:
            with_clause += identifiers_condition
        
        # For sequencing_file fields or diagnosis search, we need a second WITH clause
        # (can't reference a variable in the same WITH clause where it's defined)
        # OPTIMIZATION Phase 3 & 4: When early filter optimization applies AND no diagnosis filtering,
        # skip second_with_clause entirely (much faster)
        second_with_clause = None
        if skip_second_with_for_sf:
            # Skip second_with_clause - everything is already collected and filtered
            second_with_clause = None
        elif needs_sf_collection or needs_diag_collection:
            second_with_vars = ["sa", "st"]  # Participant added after pagination
            
            # Pass through id_list if identifiers filter is present
            if identifiers_condition:
                second_with_vars.append("id_list")
            
            # Handle diagnosis search - check if ANY diagnosis matches
            if needs_diag_collection:
                # OPTIMIZATION Phase 2: Diagnoses are already filtered during collection
                # all_diagnoses now only contains matching diagnoses (search + disease_phase if present)
                # So we just need to check if any were found
                second_with_vars.append("size(all_diagnoses) > 0 AS has_matching_diagnosis")
                second_with_vars.append("head(all_diagnoses) AS diagnoses")
            else:
                second_with_vars.append("diagnoses")
            
            second_with_vars.append("pf")
        
        # Build conditions to check if ANY sequencing_file matches (only if needed)
        # Skip this when early filter optimization applies (filters already applied in OPTIONAL MATCH)
        # Also skip if skip_second_with_for_sf is True (no second_with_clause needed)
        if not skip_second_with_for_sf:
            if needs_sf_collection and not use_sf_early_filter:
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
                        # Check if param_value is a list (for IN clause) or single value (for = clause)
                        param_value = params.get(library_strategy_param)
                        if isinstance(param_value, list) and len(param_value) > 0:
                            # Use IN clause for list values (from enum-based filtering)
                            sf_match_conditions.append(f"sf.library_strategy IN ${library_strategy_param}")
                        else:
                            # Use = for single value
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
                            # Use IN clause for list values (from enum-based filtering)
                            sf_match_conditions.append(f"sf.library_source_material IN ${library_source_material_param}")
                        else:
                            # Use = for single value
                            sf_match_conditions.append(f"sf.library_source_material = ${library_source_material_param}")
                
                # Combine all conditions with OR (if multiple fields) or use single condition
                if len(sf_match_conditions) == 1:
                    has_matching_sf_expr = f"size([sf IN all_sfs WHERE sf IS NOT NULL AND {sf_match_conditions[0]}]) > 0"
                    # Return the MATCHING sequencing file, not just the first one
                    sf_return_expr = f"head([sf IN all_sfs WHERE sf IS NOT NULL AND {sf_match_conditions[0]} | sf])"
                else:
                    # Multiple conditions - combine with OR
                    combined_condition = " OR ".join([f"({cond})" for cond in sf_match_conditions])
                    has_matching_sf_expr = f"size([sf IN all_sfs WHERE sf IS NOT NULL AND ({combined_condition})]) > 0"
                    # Return the MATCHING sequencing file, not just the first one
                    sf_return_expr = f"head([sf IN all_sfs WHERE sf IS NOT NULL AND ({combined_condition}) | sf])"
                
                second_with_vars.append(f"{has_matching_sf_expr} AS has_matching_sf")
                second_with_vars.append(f"{sf_return_expr} AS sf")
            elif needs_sf_collection and use_sf_early_filter:
                # Early filter optimization applies - sequencing files already filtered in OPTIONAL MATCH
                # BUT: When has_diagnoses_conditions is True, we collected 'all_sfs' (not 'sf')
                # So we need to extract from 'all_sfs' in that case
                if has_diagnoses_conditions:
                    # We collected 'all_sfs', which already contains only matching files (filtered in OPTIONAL MATCH WHERE)
                    # So we just need to check if any exist and get the first one
                    has_matching_sf_expr = "size([sf IN all_sfs WHERE sf IS NOT NULL]) > 0"
                    sf_return_expr = "head([sf IN all_sfs WHERE sf IS NOT NULL | sf])"
                    
                    second_with_vars.append(f"{has_matching_sf_expr} AS has_matching_sf")
                    second_with_vars.append(f"{sf_return_expr} AS sf")
                else:
                    # No diagnosis conditions - we collected 'sf' directly, just check if it's not null
                    second_with_vars.append("sf IS NOT NULL AS has_matching_sf")
                    second_with_vars.append("sf")
            elif needs_diag_collection:
                # Only diagnosis search, no sequencing file collection
                second_with_vars.append("sf")
            
            # Finalize second_with_clause if we have any second WITH vars
            if needs_sf_collection or needs_diag_collection:
                second_with_clause = ", ".join(second_with_vars)
        
        # If identifiers are present, integrate WHERE clause into WITH clause
        # BUT only if there's no second_with_clause (for search/sequencing_file filters)
        # If there's a second_with_clause, we need to apply WHERE clause after it
        # IMPORTANT: When skip_second_with_for_sf is True, filter out conditions that require second_with_clause
        # before adding to with_clause
        if identifiers_condition and where_clause and not second_with_clause:
            # Remove "WHERE " prefix and extract conditions
            where_conditions_str = where_clause.replace("WHERE ", "").strip()
            if where_conditions_str:
                # Filter out conditions that require second_with_clause when skip_second_with_for_sf is True
                if skip_second_with_for_sf:
                    # Split conditions and filter out ones that require second_with_clause
                    conditions_list = [c.strip() for c in where_conditions_str.split(" AND ") if c.strip()]
                    filtered_conditions = [
                        c for c in conditions_list 
                        if "has_matching_diagnosis" not in c 
                        and "has_matching_sf" not in c
                        and "diagnoses" not in c
                        and c.strip() != "sf IS NOT NULL"  # Will be added separately later
                        and not (c.strip().endswith("= true") and ("has_matching" in c or "diagnoses" in c))  # Remove any = true conditions for second_with_clause variables
                    ]
                    if filtered_conditions:
                        where_conditions_str = " AND ".join(filtered_conditions)
                    else:
                        where_conditions_str = ""  # All conditions filtered out
                    # IMPORTANT: Also preserve remaining conditions for later use (line 988)
                    # Store filtered conditions in a way that can be used later
                    # For now, we'll rebuild where_clause at line 988 if needed
                
                if where_conditions_str:
                    # IMPORTANT: When skip_second_with_for_sf is True, we need to also add 'sf IS NOT NULL'
                    # to the WHERE clause in with_clause to avoid having two WHERE clauses
                    if skip_second_with_for_sf:
                        sf_available = use_sf_early_filter and needs_sf_collection
                        if sf_available:
                            # Add 'sf IS NOT NULL' to the WHERE clause in with_clause
                            with_clause += f"\n        WHERE {where_conditions_str} AND sf IS NOT NULL"
                        else:
                            # sf not available, just add filtered conditions
                            with_clause += f"\n        WHERE {where_conditions_str}"
                        # Clear where_clause since everything is now in with_clause
                        where_clause = ""
                    else:
                        # Normal case: just add filtered conditions to with_clause
                        with_clause += f"\n        WHERE {where_conditions_str}"
                        where_clause = ""  # Clear it since it's now in WITH clause
                else:
                    # No filtered conditions, but we might still need 'sf IS NOT NULL'
                    if skip_second_with_for_sf:
                        sf_available = use_sf_early_filter and needs_sf_collection
                        if sf_available:
                            # Add 'sf IS NOT NULL' to with_clause
                            with_clause += f"\n        WHERE sf IS NOT NULL"
                        # Clear where_clause
                        where_clause = ""
                    else:
                        # No conditions to add, clear where_clause
                        where_clause = ""
        
        # Build RETURN clause - always include diagnosis, pathology_file, and sequencing_file for metadata
        # NOTE: Participant will be added after pagination
        return_vars = ["sa", "st", "sf", "pf", "diagnoses"]
        
        return_clause = ", ".join(return_vars)
        
        # Build unified query
        # Only include samples that have a path to a study
        # Build the DISTINCT clause with all return variables
        distinct_vars = ", ".join(return_vars)
        
        # If we have a second WITH clause (for specimen_molecular_analyte_type), include it
        # OPTIMIZATION Phase 4: When skip_second_with_for_sf is true, apply WHERE clause directly after first WITH
        second_with_str = ""
        if skip_second_with_for_sf:
            # IMPORTANT: When skip_second_with_for_sf is True, we need to ensure 'sf IS NOT NULL' is checked
            # If identifiers_condition exists, it's already in with_clause (from lines 2233-2255)
            # If identifiers_condition doesn't exist, we need to add it to with_clause here
            if use_sf_early_filter and needs_sf_collection:
                # Check if 'sf IS NOT NULL' is already in with_clause (from identifiers path)
                if "sf IS NOT NULL" not in with_clause:
                    # Add 'sf IS NOT NULL' to with_clause if not already present
                    if "WHERE" in with_clause:
                        # Append to existing WHERE clause
                        with_clause += " AND sf IS NOT NULL"
                    else:
                        # Add WHERE clause with sf IS NOT NULL
                        with_clause += "\n        WHERE sf IS NOT NULL"
            # Clear where_clause since conditions are now in with_clause
            where_clause = ""
        elif second_with_clause:
            # Apply WHERE clause after second WITH if present
            # Ensure proper spacing - where_clause should end with newline if non-empty
            if where_clause:
                # Ensure where_clause ends with newline and proper spacing
                where_clause_clean = where_clause.strip()
                if where_clause_clean:
                    second_with_str = f"WITH {second_with_clause}\n        {where_clause_clean}\n        "
                else:
                    second_with_str = f"WITH {second_with_clause}\n        "
            else:
                second_with_str = f"WITH {second_with_clause}\n        "
            where_clause = ""  # Clear it since it's now in second WITH
        elif where_clause:
            # Apply WHERE clause after first WITH if no second WITH
            where_clause = f"{where_clause}\n        "
        else:
            where_clause = ""
        
        # DEBUG: Log critical parts before building query to debug "trueWHERE" error
        if skip_second_with_for_sf:
            logger.warning(
                "DEBUG: Building query with skip_second_with_for_sf=True",
                skip_second_with_for_sf=skip_second_with_for_sf,
                second_with_str=second_with_str,
                second_with_str_repr=repr(second_with_str),
                where_clause=where_clause,
                where_clause_repr=repr(where_clause),
                where_clause_len=len(where_clause) if where_clause else 0,
                with_clause=with_clause[:200] if len(with_clause) > 200 else with_clause
            )
        
        # Early-pagination path: same filters (any params), simpler shape.
        # When early_pagination_where is "true" (depositions/identifiers only): TRUE early pagination = ORDER BY SKIP LIMIT before loading p,d,pf,sf.
        # Otherwise: pagination after OPTIONAL MATCH + WHERE + aggregate.
        # Skip when diagnosis search is active: that filter is applied during collection (all_diagnoses), not as a simple WHERE on d.
        if not needs_diag_collection:
            early_where_str = " AND ".join(early_where_conditions) if early_where_conditions else "sa.sample_id IS NOT NULL AND sa.sample_id <> ''"
            early_pagination_where_parts = []
            for c in (where_conditions or []):
                if not isinstance(c, str):
                    continue
                if "all_sfs" in c or "all_diagnoses" in c:
                    continue
                s = c.replace("diagnoses.", "d.").replace("diagnoses IS NOT NULL", "d IS NOT NULL")
                if "has_matching_sf = true" in s:
                    s = "sf IS NOT NULL"
                elif "has_matching_diagnosis = true" in s:
                    s = "d IS NOT NULL"
                early_pagination_where_parts.append(s)
            early_pagination_where = " AND ".join(early_pagination_where_parts) if early_pagination_where_parts else "true"
            # True early pagination: only when no post-optional-match filters (depositions/identifiers only)
            use_true_early_pagination = early_pagination_where == "true"
            # When return_total: run lightweight count (same filters, no collect) then list query
            total_count_early = None
            if return_total:
                if use_true_early_pagination:
                    cypher_early_count = f"""
        MATCH (sa:sample)
        WHERE {early_where_str}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                else:
                    cypher_early_count = f"""
        MATCH (sa:sample)
        WHERE {early_where_str}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        {optional_matches_str}
        WHERE {early_pagination_where}
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) as total_count
        """.strip()
                try:
                    result_count = await self.session.run(cypher_early_count, params)
                    recs = []
                    async for r in result_count:
                        recs.append(dict(r))
                    await result_count.consume()
                    total_count_early = recs[0].get("total_count", 0) if recs else 0
                except Exception as e_count:
                    logger.warning("Early-pagination count query failed, total will come from summary if requested", error=str(e_count))
            if use_true_early_pagination:
                # Paginate BEFORE loading p, d, pf, sf (only fetch metadata for the page)
                cypher_early = f"""
        MATCH (sa:sample)
        WHERE {early_where_str}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        WITH sa, st
        ORDER BY toString(sa.sample_id)
        SKIP $offset
        LIMIT $limit
        {optional_matches_str}
        // After pagination: OPTIONAL MATCH participant (only for paginated samples - much faster)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, head(collect(DISTINCT d)) AS diagnoses, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
            else:
                cypher_early = f"""
        MATCH (sa:sample)
        WHERE {early_where_str}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        {optional_matches_str}
        WHERE {early_pagination_where}
        WITH sa, st, head(collect(DISTINCT d)) AS diagnoses, head(collect(DISTINCT pf)) AS pf, head(collect(DISTINCT sf)) AS sf
        ORDER BY toString(sa.sample_id)
        SKIP $offset
        LIMIT $limit
        // After pagination: OPTIONAL MATCH participant (only for paginated samples - much faster)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, diagnoses, pf, sf
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
            try:
                result_early = await self.session.run(cypher_early, params)
                records_early = []
                async for rec in result_early:
                    records_early.append(dict(rec))
                await result_early.consume()
                samples_early = []
                for record in records_early:
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
                                samples_early.append(sample_obj)
                    except Exception as e:
                        logger.warning("Error converting sample record in early-pagination path: %s", e, exc_info=True)
                        continue
                logger.info(
                    "Using early-pagination-with-filters query",
                    filter_count=len(filters),
                    returned=len(samples_early),
                    has_depositions=bool(depositions_study_filter),
                    filters=list(filters.keys()),
                )
                if return_total and total_count_early is not None:
                    return (samples_early, total_count_early)
                return samples_early
            except Exception as e_early:
                logger.warning(
                    "Early-pagination-with-filters path failed, using standard query",
                    error=str(e_early),
                    filters=dict(filters),
                )

        cypher = f"""
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        {early_where_clause.replace('WHERE ', 'AND ') if early_where_clause else ''}
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid{depositions_study_filter}
        {optional_matches_str}
        WITH {with_clause}
        {second_with_str}{where_clause}WITH DISTINCT {distinct_vars}
        // Deduplicate by sample_id to ensure one row per sample (handles multiple study relationships)
        // Use head() to pick one study per sample (consistent with count query which groups by sample_id)
        // IMPORTANT: When grouping by sample, preserve the diagnosis that matches all filters
        // Collect diagnosis per sample (not per sample-study pair) to ensure consistency
        WITH DISTINCT sa.sample_id as sample_id, sa, head(collect(DISTINCT st)) as st, sf, pf, head(collect(DISTINCT diagnoses)) as diagnoses
        ORDER BY toString(sample_id)
        SKIP $offset
        LIMIT $limit
        // After pagination: OPTIONAL MATCH participant (only for paginated samples - much faster)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, sf, pf, diagnoses
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        # Log the standard query being executed (for debugging)
        logger.info(
            "EXECUTING standard query (NO early pagination)",
            query=cypher[:500] + "..." if len(cypher) > 500 else cypher,
            query_length=len(cypher),
            has_skip_after_optional_match="SKIP" in cypher.split("OPTIONAL MATCH")[-1] if "OPTIONAL MATCH" in cypher else True,
            filters=list(filters.keys()),
            early_where_clause=early_where_clause[:200] if early_where_clause else None,
        )

        # Log full query for debugging (truncated if too long)
        logger.info(
            "Standard query full Cypher",
            cypher_full=cypher,
            params=params,
        )
        
        # When return_total: run count variant of standard query (same filters, count distinct sample_id+study_id)
        # IMPORTANT: Count query must count ALL sample-study pairs BEFORE head() aggregation
        # The list query uses head(collect(DISTINCT st)) to pick one study per sample for display,
        # but the count should include ALL matching sample-study pairs
        total_count_std = None
        if return_total:
            # Build count query by counting BEFORE the head() aggregation
            # The count should happen right after WHERE clause filters are applied, before head() picks one study
            _head_pattern = "        WITH DISTINCT sa.sample_id as sample_id, sa, head(collect(DISTINCT st)) as st, sf, pf, diagnoses"
            if _head_pattern in cypher:
                # Split at the head() aggregation - count all pairs before head()
                # At this point, we have "WITH DISTINCT {distinct_vars}" which includes sa and st
                # Count all distinct sample-study pairs directly
                _before_head = cypher.split(_head_pattern)[0]
                # Count all sample-study pairs (st is available from distinct_vars)
                cypher_count_std = _before_head + """
        // Count all sample-study pairs (before head() aggregation)
        WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id
        RETURN count(*) AS total_count
                """.strip()
            else:
                # Fallback: use simple replacement (may not work correctly with head())
                _tail = "        ORDER BY toString(sample_id)\n        SKIP $offset\n        LIMIT $limit\n        RETURN sa, p, st, sf, pf, diagnoses"
                _count_tail = "        WITH DISTINCT sample_id, st.study_id AS study_id\n        RETURN count(*) as total_count"
                cypher_count_std = cypher.replace(_tail, _count_tail) if _tail in cypher else None
            
            if cypher_count_std:
                try:
                    logger.debug("Standard query count Cypher", cypher_count=cypher_count_std[:500] if len(cypher_count_std) > 500 else cypher_count_std)
                    result_count_std = await self.session.run(cypher_count_std, params)
                    recs_std = []
                    async for r in result_count_std:
                        recs_std.append(dict(r))
                    await result_count_std.consume()
                    total_count_std = recs_std[0].get("total_count", 0) if recs_std else 0
                except Exception as e_count_std:
                    logger.warning("Standard path count query failed, total will come from summary if requested", error=str(e_count_std), exc_info=True)
        
        # DEBUG: Log the exact query section that might cause "trueWHERE" error
        if skip_second_with_for_sf:
            # Extract the problematic section
            query_section = f"{second_with_str}{where_clause}\n        WITH DISTINCT"
            logger.warning(
                "DEBUG: Query section around WHERE clause",
                query_section=query_section,
                query_section_repr=repr(query_section),
                has_truewhere="trueWHERE" in query_section.replace(" ", "").replace("\n", "")
            )
        
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
                # Build study path first using multi-hop traversal
                optional_matches = []
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                optional_matches.append("WITH sa, collect(DISTINCT st1.study_id) AS st1_list")
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")
                optional_matches.append("WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list")
                optional_matches.append("WITH sa, (st2_list + st1_list) AS combined")
                optional_matches.append("UNWIND combined AS sid")
                optional_matches.append("MATCH (st:study)")
                optional_matches.append("WHERE st.study_id = sid")
                # Always include participant for identifiers
                optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                # Always include diagnosis, pathology_file, and sequencing_file for metadata
                optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                
                optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""
                
                # Always include diagnosis, pathology_file, and sequencing_file for metadata
                with_vars = ["sa", "p", "st"]  # Always include participant and study
                with_collects = [
                    "head(collect(DISTINCT d)) AS diagnoses",
                    "head(collect(DISTINCT pf)) AS pf",
                    "head(collect(DISTINCT sf)) AS sf"
                ]
                
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
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                {optional_matches_str}
                WITH {with_clause}
                {where_clause.replace('WHERE ', 'AND ') if where_clause else ''}
                WITH DISTINCT {return_clause}
                RETURN {return_clause}
                ORDER BY toString(sa.sample_id)
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
                            # Last resort: return empty dict to avoid expensive dir() call
                            # If node conversion fails, return empty dict rather than scanning all attributes
                            return {}
                
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
        
        if return_total and total_count_std is not None:
            return (samples, total_count_std)
        return samples
    
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
                # If reverse_mapped is None, use the original value (no mapping needed)
                params[param_name] = reverse_mapped if reverse_mapped else value
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
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
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
        WITH sa, sid
        WHERE sid IS NOT NULL
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
        
        # Build optimized reverse query with EARLY PAGINATION
        # Key optimization: Start from sequencing_file (uses index), then find samples
        # PERFORMANCE OPTIMIZATION: Paginate samples BEFORE collecting study relationships
        # This significantly reduces memory usage and improves performance for large datasets
        # Flow: (1) Match sf + sa (2) Get distinct samples (3) ORDER BY + SKIP/LIMIT [early pagination]
        #       (4) Rematch sf for paginated samples (5) Collect study IDs (6) UNWIND studies
        #       (7) Deduplicate sf per (sa, st) (8) OPTIONAL MATCH other relationships
        # IMPORTANT: Early pagination - apply SKIP/LIMIT BEFORE collecting study relationships
        # to avoid expensive operations on large datasets
        cypher = f"""
        MATCH (sf:sequencing_file)
        WHERE {where_clause}
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
        WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
        WITH DISTINCT sa, sf
        ORDER BY toString(sa.sample_id)
        SKIP $offset
        LIMIT $limit
        // Collect study ids from both paths (after pagination)
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
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st, collect(DISTINCT sf) AS matching_sfs
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
                logger.error("Error in pathology_file reverse count query", error=str(e_count), exc_info=True)
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
                params[param_name] = reverse_mapped
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
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
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
                    params[param_name] = reverse_mapped
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
        WITH sa, sf, pf, collect(DISTINCT st1.study_id) AS st1_list_raw
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, sf, pf, st1_list_raw, collect(DISTINCT st2.study_id) AS st2_list_raw
        WITH sa, sf, pf,
             [x IN st1_list_raw WHERE x IS NOT NULL] AS st1_list,
             [x IN st2_list_raw WHERE x IS NOT NULL] AS st2_list
        WITH sa, sf, pf, (st2_list + st1_list) AS combined
        WHERE size(combined) > 0
        UNWIND combined AS sid
        WITH sa, sf, pf, sid
        WHERE sid IS NOT NULL
        MATCH (st:study)
        WHERE st.study_id = sid
        WITH sa, st, collect(DISTINCT sf) AS matching_sfs, collect(DISTINCT pf) AS matching_pfs
        WITH sa, st, head(matching_sfs) AS sf, head(matching_pfs) AS pf
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, sf, pf
        ORDER BY toString(sa.sample_id), toString(st.study_id)
        SKIP $offset
        LIMIT $limit
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
            
            if return_total and total_count_combined is not None:
                return (samples, total_count_combined)
            return samples
            
        except Exception as e:
            logger.error("Error executing combined reverse query", error=str(e), exc_info=True)
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
          AND sa.sample_id <> ''
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
                db_value = SampleRepository._reverse_map_library_selection_method_static(value)
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
            participant_id = str(p.get("participant_id", ""))
            if not participant_id or participant_id == "":
                participant_id = str(p.get("id", "")) if isinstance(p, dict) else ""
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
            participant_id = str(p.get("participant_id", ""))
            if not participant_id or participant_id == "":
                participant_id = str(p.get("id", "")) if isinstance(p, dict) else ""

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
        
        # tissue_type: sa.sample_tumor_status (mapped from sample_tumor_status field)
        tissue_type_value = sa.get("sample_tumor_status") if sa else None
        
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
            tissue_type=_wrap_value(_null_if_invalid(tissue_type_value)),
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
