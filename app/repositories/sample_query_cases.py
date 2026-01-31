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
from app.core.field_mappings import load_sample_enum

logger = get_logger(__name__)


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
        
        # Build query: paginate at sample level first
        cypher = f"""
        MATCH (sa:sample)
        WHERE {sample_where_str}
        WITH sa, toString(sa.sample_id) AS sample_id
        ORDER BY sample_id
        SKIP $offset
        LIMIT $limit
        
        // Collect study ids from both paths
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, sample_id, collect(DISTINCT st2.study_id) AS s2
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, sample_id, s2 + collect(DISTINCT st1.study_id) AS combined
        
        // Expand studies
        UNWIND combined AS study_id
        WITH sa, sample_id, study_id
        WHERE study_id IS NOT NULL
        MATCH (st:study {{study_id: study_id}})
        
        // OPTIONAL MATCH other nodes - pick 1 random each
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
        OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
        
        WITH sa, st, sample_id, study_id,
             head(collect(DISTINCT d)) AS diagnoses,
             head(collect(DISTINCT pf)) AS pf,
             head(collect(DISTINCT sf)) AS sf
        
        ORDER BY sample_id, study_id
        
        // After pagination: OPTIONAL MATCH participant
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        WITH sa, p, st, sf, pf, diagnoses
        RETURN sa, p, st, sf, pf, diagnoses
        """.strip()
        
        logger.info("Case 1 query", cypher=cypher, params=params)
        
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
        # TODO: Implement Case 2
        # For now, delegate to existing early pagination logic
        return await self._get_samples_early_pagination_with_filters(filters, offset, limit, base_url) or []

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
        6. Filter: must match all required node types
        7. Collect all matching nodes per type
        8. Filter: at least one diagnosis matches (if diagnosis filters present)
        9. Filter: at least one sequencing_file matches (if sequencing_file filters present)
        10. Filter: at least one pathology_file matches (if pathology_file filters present)
        11. ORDER BY (sample_id, study_id)
        12. SKIP/LIMIT (early pagination at sample-study pair level)
        13. Pick 1 matching node per type (head() from filtered collections)
        14. OPTIONAL MATCH participant (after pagination)
        """
        # Case 3: Apply node filters before pagination
        # For now, return None to fall through to existing standard query
        # TODO: Implement Case 3 with new structure:
        # - Apply filters in OPTIONAL MATCH WHERE clauses
        # - Collect all matching nodes per type
        # - Filter for required matches
        # - Paginate at sample-study pair level
        # - Pick 1 matching node per type
        # - Add participant after pagination
        return None
