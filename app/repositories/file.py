"""
File repository for the CCDI Federation Service.

This module provides data access operations for sequencing files
using Cypher queries to Memgraph.
"""

import asyncio
import re
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.db.memgraph import run_count_query_with_retry
from app.core.constants import FileType, load_file_enum
from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY
from app.lib.field_allowlist import FieldAllowlist, EntityType
from app.models.dto import File
from app.models.errors import UnsupportedFieldError, InvalidParametersError

logger = get_logger(__name__)

# Unharmonized filter field names are interpolated into Cypher, so they must be
# strict identifiers. This is the primary guard against Cypher property-name injection.
_UNHARMONIZED_FIELD_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Sentinel returned by _build_count_query when the filter is provably zero (e.g., invalid
# file_type enum value). Callers check identity (`is _ZERO_COUNT_SENTINEL`) before issuing a
# DB round-trip, preserving the pre-refactor behaviour of short-circuiting in Python.
_ZERO_COUNT_SENTINEL = "RETURN 0 AS total_count"

_SEQUENCING_FILE_DEFAULT = next(
    c for c in FILE_NODE_REGISTRY if c.node_label == "sequencing_file"
)


class FileRepository:
    """Repository for sequencing file data operations."""
    
    def __init__(
        self,
        session: AsyncSession,
        allowlist: FieldAllowlist,
        config: FileNodeConfig | None = None,
    ):
        """Initialize repository with database session, field allowlist, and node config."""
        self.session = session
        self.allowlist = allowlist
        # Default to sequencing_file for backwards compatibility
        self.config = config if config is not None else _SEQUENCING_FILE_DEFAULT

    def _validate_unharmonized_file_field(self, db_field_name: str, original_key: str) -> None:
        """
        Validate an unharmonized filter field name before it is interpolated into Cypher.

        Raises InvalidParametersError for a malformed name (defense-in-depth against
        Cypher injection) and UnsupportedFieldError for a well-formed but non-allowlisted
        name. `file` declares no unharmonized fields in metadata_fields.json, so this
        default-denies every unharmonized filter on the file endpoints.
        """
        if not _UNHARMONIZED_FIELD_NAME_RE.match(db_field_name):
            logger.warning("Malformed unharmonized filter field name rejected", entity="file")
            raise InvalidParametersError([original_key])
        if not self.allowlist.is_unharmonized_field_allowed(EntityType.FILE, db_field_name):
            logger.warning("Unsupported unharmonized filter field rejected", entity="file")
            raise UnsupportedFieldError(original_key, "file")

    async def get_files(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20
    ) -> List[File]:
        """
        Get paginated list of sequencing files with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of File objects
            
        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching sequencing files",
            filters=filters,
            offset=offset,
            limit=limit
        )
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        
        # Handle depositions filter separately - it filters by study_id
        # Parse || separator for OR logic (e.g., "phs002517 || phs002790")
        # Make a copy to avoid modifying the original filters dict
        filters_copy = filters.copy()
        depositions_value = filters_copy.pop("depositions", None)
        depositions_list = None
        depositions_param_name = None  # Will store parameter name for CALL+UNION queries
        if depositions_value is not None:
            # Split on || separator and clean whitespace
            depositions_list = [d.strip() for d in depositions_value.split("||")]
            # Filter out empty strings
            depositions_list = [d for d in depositions_list if d]
            if not depositions_list:
                depositions_list = None
                depositions_value = None
        
        # Handle checksums filter separately - supports || separator for OR logic
        # Note: checksums in filters_copy is mapped to "md5sum" field by get_file_filters()
        checksums_value = filters_copy.pop("md5sum", None)
        checksums_list = None
        if checksums_value is not None:
            # Split on || separator and clean whitespace
            checksums_list = [c.strip() for c in checksums_value.split("||")]
            # Filter out empty strings
            checksums_list = [c for c in checksums_list if c]
            if not checksums_list:
                checksums_list = None
        
        # Validate type filter - must match enum value exactly (case-sensitive)
        # Note: filter key is "file_type" because get_file_filters() maps "type" -> "file_type"
        # After validation, use case-insensitive matching in the query to handle database case variations
        type_filter_param = None
        if "file_type" in filters_copy:
            type_value = filters_copy.pop("file_type")  # Remove from filters_copy to handle separately
            # Check if the type value exactly matches an enum value (case-sensitive)
            if type_value not in FileType.values():
                # Type doesn't match any enum value, return empty results
                logger.info(
                    "Type filter value does not match any enum value (case-sensitive) - returning empty results",
                    type_value=type_value,
                    valid_values=FileType.values()[:5]  # Log first 5 for reference
                )
                return []
            # Use case-insensitive matching in the query (toLower for both sides)
            # This allows matching database values regardless of case, while still validating input case-sensitively
            param_counter += 1
            type_filter_param = f"param_{param_counter}"
            params[type_filter_param] = type_value
            logger.debug(
                "Type filter validated successfully, will use case-insensitive matching in query",
                type_value=type_value
            )
        
        # Add regular filters
        for field, value in filters_copy.items():
            # Handle unharmonized fields (e.g., metadata.unharmonized.file_name)
            if field.startswith("metadata.unharmonized."):
                # Extract the actual database field name
                # e.g., "metadata.unharmonized.file_name" -> "file_name"
                db_field_name = field.replace("metadata.unharmonized.", "")
                self._validate_unharmonized_file_field(db_field_name, field)

                param_counter += 1
                param_name = f"param_{param_counter}"

                if isinstance(value, list):
                    where_conditions.append(f"sf.{db_field_name} IN ${param_name}")
                else:
                    where_conditions.append(f"sf.{db_field_name} = ${param_name}")
                params[param_name] = value
                continue
            
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            # Convert file_size to number if it's a string that can be converted
            if field == "file_size" and isinstance(value, str):
                try:
                    # Strip quotes if present (e.g., '70925' -> 70925)
                    cleaned_value = value.strip().strip("'\"")
                    value = int(cleaned_value)
                except (ValueError, TypeError):
                    # If conversion fails, keep as string (might be a different format)
                    pass
            
            if isinstance(value, list):
                where_conditions.append(f"sf.{field} IN ${param_name}")
            else:
                where_conditions.append(f"sf.{field} = ${param_name}")
            params[param_name] = value
        
        # Add case-insensitive type filter if present
        if type_filter_param:
            where_conditions.append(f"toLower(sf.file_type) = toLower(${type_filter_param})")
        
        # Add checksums filter if present (supports || separator for OR logic)
        if checksums_list is not None:
            param_counter += 1
            checksums_param_name = f"param_{param_counter}"
            if len(checksums_list) == 1:
                # Single checksum: check both md5sum and checksum_value fields
                where_conditions.append(f"(sf.md5sum = ${checksums_param_name} OR sf.checksum_value = ${checksums_param_name})")
                params[checksums_param_name] = checksums_list[0]
            else:
                # Multiple checksums: check if either field is IN the list
                where_conditions.append(f"(sf.md5sum IN ${checksums_param_name} OR sf.checksum_value IN ${checksums_param_name})")
                params[checksums_param_name] = checksums_list
        
        # Build final query
        # Only include sequencing_files that have a path to a study
        # Path 1: sequencing_file -> sample -> participant -> consent_group -> study
        # Path 2: sequencing_file -> sample -> cell_line -> study
        
        # Combine all WHERE conditions
        all_where_conditions = []
        
        # Add study filter (must have a study)
        all_where_conditions.append("st IS NOT NULL")
        
        # Add depositions filter (filter by study_id)
        # Support || separator for OR logic (e.g., "phs002517 || phs002790")
        depositions_param_name = None
        if depositions_list is not None:
            param_counter += 1
            param_name = f"param_{param_counter}"
            depositions_param_name = param_name  # Store for CALL+UNION queries
            if len(depositions_list) == 1:
                all_where_conditions.append(f"st.study_id = ${param_name}")
                params[param_name] = depositions_list[0]
            else:
                all_where_conditions.append(f"st.study_id IN ${param_name}")
                params[param_name] = depositions_list
        
        # Add file field filters (applied to sf)
        if where_conditions:
            all_where_conditions.extend(where_conditions)
        
        # PERFORMANCE OPTIMIZATION: Split WHERE conditions into:
        # 1. File property filters (apply BEFORE any traversals)
        # 2. Study filters (apply AFTER study path is established)
        file_where_conditions = []  # Filters on sf.* properties
        study_where_conditions = []  # Filters on study properties
        
        for cond in all_where_conditions:
            if "st." in cond or cond == "st IS NOT NULL":
                study_where_conditions.append(cond)
            else:
                file_where_conditions.append(cond)
        
        # Build WHERE clauses
        file_where_clause = "WHERE " + " AND ".join(file_where_conditions) if file_where_conditions else ""
        study_where_clause = "WHERE " + " AND ".join(study_where_conditions) if study_where_conditions else ""
        
        # Detect if we have depositions filter for CALL+UNION optimization
        has_depositions_filter = depositions_list is not None
        
        # CONDITIONAL OPTIMIZATION: Use different query patterns based on filters
        # Pattern 1: File filters + Depositions = CALL+UNION (applies study filter during traversal)
        # Pattern 2: File filters only = Optimized (filter files first, then traverse)
        # Pattern 3: No file filters = Simple (single traversal, less overhead)
        
        if file_where_conditions and has_depositions_filter:
            # PATTERN 1: OPTIMIZED QUERY (for type + depositions queries)
            # Use multi-hop traversal: sample -> participant -> consent_group -> study (preferred)
            # or sample -> cell_line -> study (fallback)
            # Use stored depositions parameter name
            deposition_param = depositions_param_name
            deposition_operator = "=" if len(depositions_list) == 1 else "IN"
            
            cypher = f"""
            // Step 1: Match files and apply file property filters FIRST
            MATCH (sf:{self.config.node_label})
            {file_where_clause}
            // Step 2: Find study path using multi-hop traversal (with WITH clauses to prevent cartesian products)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            // study path 2 — via participant → consent → study (preferred path)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st2:study)
            WHERE st2.study_id {deposition_operator} ${deposition_param}
            WITH sf, sa, collect(DISTINCT st2) AS st2_list
            // study path 1 — via cell_line (fallback)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WHERE st1.study_id {deposition_operator} ${deposition_param}
            WITH sf, sa, st2_list, collect(DISTINCT st1) AS st1_list
            WITH sf, sa, coalesce(st2_list[0], st1_list[0]) AS st
            // Step 3: Filter out nulls
            WHERE st IS NOT NULL
            // Step 4: Paginate on the SCALAR guid only (avoid sorting full node records in memory)
            WITH DISTINCT sf.guid AS file_guid
            ORDER BY file_guid
            SKIP $offset
            LIMIT $limit
            // Step 5: Re-fetch the full node for just this page via the guid index (point lookups)
            MATCH (sf:{self.config.node_label})
            WHERE sf.guid = file_guid
            // Step 6: Collect samples for final results only
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa2:sample)
            WITH file_guid, sf, collect(DISTINCT sa2) as samples
            // Step 7: Get study for response (using multi-hop traversal)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa3:sample)
            OPTIONAL MATCH (sa3)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st3:study)
            WHERE st3.study_id {deposition_operator} ${deposition_param}
            WITH file_guid, sf, samples, collect(DISTINCT st3) AS st3_list
            OPTIONAL MATCH (sa3)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st4:study)
            WHERE st4.study_id {deposition_operator} ${deposition_param}
            WITH file_guid, sf, samples, st3_list, collect(DISTINCT st4) AS st4_list
            WITH file_guid, sf, samples, head(collect(DISTINCT coalesce(st3_list[0], st4_list[0]))) as study
            // Step 8: Restore deterministic page order (only ~per_page rows here)
            ORDER BY file_guid
            RETURN sf, samples, study as st
            """.strip()
        elif file_where_conditions:
            # OPTIMIZED QUERY (for queries with file filters like type=BAM)
            # Use multi-hop traversal: sample -> participant -> consent_group -> study (preferred)
            # or sample -> cell_line -> study (fallback)
            # Apply file filters FIRST, then traverse to study using multi-hop
            cypher = f"""
            // Step 1: Match files and apply file property filters FIRST.
            // Enforce the study-path invariant (same as the count query's WHERE st IS NOT NULL)
            // WITHOUT materialising full nodes: an EXISTS semi-join keeps pagination on the scalar
            // guid. Files with no path to any study are excluded, matching count_for_pagination.
            // (file_where_clause is non-empty in this branch, so appending AND (...) is valid.)
            MATCH (sf:{self.config.node_label})
            {file_where_clause}
            AND (EXISTS {{ MATCH (sf)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) }}
              OR EXISTS {{ MATCH (sf)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) }})
            // Step 2: Paginate on the SCALAR guid only (avoid sorting full node records in memory)
            WITH DISTINCT sf.guid AS file_guid
            ORDER BY file_guid
            SKIP $offset
            LIMIT $limit
            // Step 3: Re-fetch the full node for just this page via the guid index (point lookups)
            MATCH (sf:{self.config.node_label})
            WHERE sf.guid = file_guid
            // Step 5: Collect samples only for paginated files (much faster!)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa3:sample)
            WITH file_guid, sf, collect(DISTINCT sa3) AS samples
            // Step 6: Get study for response (only for the returned files, using multi-hop traversal)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa4:sample)
            OPTIONAL MATCH (sa4)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st4:study)
            WITH file_guid, sf, samples, collect(DISTINCT st4) AS st4_list
            OPTIONAL MATCH (sa4)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st5:study)
            WITH file_guid, sf, samples, st4_list, collect(DISTINCT st5) AS st5_list
            WITH file_guid, sf, samples, head(collect(DISTINCT coalesce(st4_list[0], st5_list[0]))) AS st
            // Step 7: Restore deterministic page order (only ~per_page rows here)
            ORDER BY file_guid
            RETURN sf, samples, st
            """.strip()
        elif has_depositions_filter:
            # PATTERN 2b: CRITICAL FIX - Match files directly without collecting into lists
            # LOGIC: File endpoint - each file (sf) must be connected to both study (st) and sample (sa)
            # Paginate by unique sf.guid only (files are unique, not file-study pairs)
            # Start from studies (2-10 nodes) instead of files (1M+ nodes)
            # Traverse: study <- consent_group <- participant <- sample <- sequencing_file
            # or: study <- cell_line <- sample <- sequencing_file
            deposition_param = depositions_param_name
            deposition_operator = "=" if len(depositions_list) == 1 else "IN"
            
            # CRITICAL: Match files that are connected to study via sample, paginate by unique sf.guid
            # This avoids collecting millions of files into memory
            # Note: This uses path 1 only to avoid memory issues. Path 2 files are excluded.
            # For complete results with both paths, consider using separate queries or cursor pagination.
            cypher = f"""
            // Step 1: Start from study nodes (only a few studies to process!)
            MATCH (st:study)
            WHERE st.study_id {deposition_operator} ${deposition_param}
            // Step 2: Collect this deposition's file guids via BOTH study paths so the list
            // matches count_for_pagination Pattern 2b (which counts both). Collecting the
            // SCALAR guids (not full nodes) keeps memory bounded — the set is capped by the
            // deposition and both traversals start from the few study nodes.
            // path 2 — via participant → consent_group (preferred)
            OPTIONAL MATCH (st)<-[:of_consent_group]-(:consent_group)<-[:of_participant]-(:participant)<-[:of_sample]-(:sample)<-[:{self.config.rel_name}]-(sf1:{self.config.node_label})
            WITH st, collect(DISTINCT sf1.guid) AS consent_guids
            // path 1 — via cell_line (fallback)
            OPTIONAL MATCH (st)<-[:of_cell_line]-(:cell_line)<-[:of_sample]-(:sample)<-[:{self.config.rel_name}]-(sf2:{self.config.node_label})
            WITH st.study_id AS sid, (consent_guids + collect(DISTINCT sf2.guid)) AS guids
            UNWIND guids AS file_guid
            WITH file_guid, sid
            WHERE file_guid IS NOT NULL
            // Step 3: Paginate on the unique file guid (one row per file, not per file-study
            // pair — pairing on study_id would duplicate a file shared across filtered studies
            // and shrink the page below per_page). Carry min(study_id) as a representative
            // (already deposition-filtered) study for the indexed point-lookup in Step 4b.
            WITH file_guid, min(sid) AS study_id
            ORDER BY file_guid
            SKIP $offset
            LIMIT $limit
            // Step 4: Re-fetch file + its deposition study for just this page (guid & study_id are indexed)
            MATCH (sf:{self.config.node_label})
            WHERE sf.guid = file_guid
            // Step 4b: Re-resolve the representative deposition study for the response
            MATCH (st:study)
            WHERE st.study_id = study_id
            // Step 5: Collect ALL samples for each paginated file (file can have multiple samples)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            WITH file_guid, sf, st, collect(DISTINCT sa) AS samples
            // Step 6: Restore deterministic page order
            ORDER BY file_guid
            RETURN sf, samples, st
            """.strip()
        else:
            # PATTERN 3: SIMPLE QUERY (no filters at all - basic pagination only)
            # Use multi-hop traversal: sample -> participant -> consent_group -> study (preferred)
            # or sample -> cell_line -> study (fallback)
            cypher = f"""
            // Step 1: Restrict to study-reachable files, then order + paginate on the SCALAR
            // guid only. The EXISTS study guard MUST be applied BEFORE pagination so the
            // paginated set matches count_for_pagination (_build_count_query Pattern 3),
            // which also counts only study-reachable files; without it, the ~187K files with
            // no study path are paginated + returned with an empty namespace while `all` and
            // the Link `last` under-report. Same guard as the file-filter branch above.
            // EXISTS is a scalar semi-join — it does not materialise full nodes, so sorting
            // scalar guids keeps memory bounded on large file labels (millions of nodes) while
            // preserving deterministic page/per_page ordering. Full nodes are re-fetched for
            // the page via the guid index.
            MATCH (file_page:{self.config.node_label})
            WHERE file_page.guid IS NOT NULL
              AND (EXISTS {{ (file_page)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) }}
                OR EXISTS {{ (file_page)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) }})
            WITH file_page.guid AS file_guid
            ORDER BY file_guid
            SKIP $offset
            LIMIT $limit
            // Step 2: Re-fetch the full node for just this page (guid is indexed -> point lookups)
            MATCH (sf:{self.config.node_label})
            WHERE sf.guid = file_guid
            // Step 3: Collect samples only for the paginated files
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            WITH file_guid, sf, collect(DISTINCT sa) AS samples
            // Step 4: Get study for response (multi-hop traversal, only for the page)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa2:sample)
            // study path 2 — via participant → consent → study (preferred path)
            OPTIONAL MATCH (sa2)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st2:study)
            WITH file_guid, sf, samples, collect(DISTINCT st2) AS st2_list
            // study path 1 — via cell_line (fallback)
            OPTIONAL MATCH (sa2)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH file_guid, sf, samples, st2_list, collect(DISTINCT st1) AS st1_list
            WITH file_guid, sf, samples, head(collect(DISTINCT coalesce(st2_list[0], st1_list[0]))) as study
            // Step 5: Restore deterministic page order (only ~per_page rows remain here)
            ORDER BY file_guid
            RETURN sf, samples, study as st
            """.strip()
        
        logger.info(
            "Executing get_files Cypher query",
            cypher=cypher,
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
                    logger.debug(f"Retrying get_files query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_files query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in get_files query after retries", error=str(e), exc_info=True)
                    raise
        
        # Convert to File objects
        files = []
        for record in records:
            file_data = record.get("sf", {})
            samples_data = record.get("samples", [])
            study_data = record.get("st", {})
            files.append(self._record_to_file(file_data, samples_data, study_data))
        
        logger.debug(
            "Found sequencing files",
            count=len(files),
            filters=filters
        )
        
        return files
    
    async def get_file_by_identifier(
        self,
        organization: str,
        namespace: str,
        name: str
    ) -> Optional[File]:
        """
        Get a specific sequencing file by organization, namespace, and name.
        
        Args:
            organization: Organization identifier (must be "CCDI-DCC")
            namespace: Namespace identifier (study_id)
            name: File identifier (file id)
            
        Returns:
            File object or None if not found
        """
        logger.debug(
            "Fetching sequencing file by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Build query to find sequencing_file by identifier
        # Use sf.guid field
        # Only include sequencing_files that have a path to a study
        # Path 1: sequencing_file -> sample -> participant -> consent_group -> study
        # Path 2: sequencing_file -> sample -> cell_line -> study
        cypher = f"""
        MATCH (sf:{self.config.node_label})
        WHERE sf.guid = $name
        MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH sf, sa, coalesce(st1, st2) AS st
        WHERE st IS NOT NULL AND st.study_id = $namespace
        WITH DISTINCT sf, collect(DISTINCT sa) AS samples, st
        RETURN sf, samples, st
        LIMIT 1
        """
        
        params = {
            "name": name,
            "namespace": namespace
        }
        
        logger.info(
            "Executing get_file_by_identifier Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query with proper result consumption
        result = await self.session.run(cypher, params)
        records = []
        async for record in result:
            records.append(dict(record))
        
        if not records:
            logger.debug("Sequencing file not found", name=name, namespace=namespace)
            return None
        
        record = records[0]
        file_data = record.get("sf", {})
        samples_data = record.get("samples", [])
        study_data = record.get("st", {})
        file = self._record_to_file(file_data, samples_data, study_data)
        
        logger.debug("Found sequencing file", name=name, namespace=namespace, file_data=getattr(file, 'id', str(file)[:50]))
        
        return file
    
    async def count_files_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count sequencing files grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting sequencing files by field",
            field=field,
            filters=filters
        )
        
        # Validate field is allowed for count operations - only "type" and "depositions" are supported
        allowed_fields = {"type", "depositions"}
        if field not in allowed_fields:
            # Log the invalid field but don't include it in the error message
            logger.warning(
                "Unsupported field for file count",
                field=field,
                entity_type="file"
            )
            raise UnsupportedFieldError(field, "file")
        
        # Special handling for depositions - aggregate by study_id
        if field == "depositions":
            return await self._count_files_by_depositions(filters)
        
        # Map API field names to database field names
        field_mapping = {
            "type": "file_type"
        }
        db_field = field_mapping.get(field, field)
        
        # Validate type filter - must match enum value exactly (case-sensitive)
        # Note: filter key is "file_type" because get_file_filters() maps "type" -> "file_type"
        # After validation, use case-insensitive matching in the query to handle database case variations
        type_filter_param = None
        filters_copy = filters.copy()
        params = {}  # Initialize params dict
        param_counter = 0
        
        if "file_type" in filters_copy:
            type_value = filters_copy.pop("file_type")  # Remove from filters_copy to handle separately
            # Check if the type value exactly matches an enum value (case-sensitive)
            if type_value not in FileType.values():
                # Type doesn't match any enum value, return empty results
                logger.debug(
                    "Type filter value does not match any enum value (case-sensitive)",
                    type_value=type_value,
                    valid_values=FileType.values()[:5]  # Log first 5 for reference
                )
                return {
                    "total": 0,
                    "missing": 0,
                    "values": []
                }
            # Use case-insensitive matching in the query (toLower for both sides)
            param_counter += 1
            type_filter_param = f"param_{param_counter}"
            params[type_filter_param] = type_value
        
        # Build WHERE conditions and parameters for filters (excluding field-specific conditions)
        base_where_conditions = []
        
        # Add regular filters
        for filter_field, value in filters_copy.items():
            # Handle unharmonized fields (e.g., metadata.unharmonized.file_name)
            if filter_field.startswith("metadata.unharmonized."):
                # Extract the actual database field name
                db_field_name = filter_field.replace("metadata.unharmonized.", "")
                self._validate_unharmonized_file_field(db_field_name, filter_field)

                param_counter += 1
                param_name = f"param_{param_counter}"

                if isinstance(value, list):
                    base_where_conditions.append(f"sf.{db_field_name} IN ${param_name}")
                else:
                    base_where_conditions.append(f"sf.{db_field_name} = ${param_name}")
                params[param_name] = value
                continue

            param_counter += 1
            param_name = f"param_{param_counter}"

            if isinstance(value, list):
                base_where_conditions.append(f"sf.{filter_field} IN ${param_name}")
            else:
                base_where_conditions.append(f"sf.{filter_field} = ${param_name}")
            params[param_name] = value

        # Add case-insensitive type filter if present
        if type_filter_param:
            base_where_conditions.append(f"toLower(sf.file_type) = toLower(${type_filter_param})")
        
        # OPTIMIZATION: When counting by type, filter to enum values using IN clause
        # BUT: Only apply this filter to the VALUES query, not total/missing queries
        # This ensures total counts match between type and depositions endpoints
        # (both should count ALL files with valid study paths)
        type_enum_param = None
        type_enum_filter = None
        if field == "type":
            enum_values = FileType.values() if FileType.values() else load_file_enum()
            if enum_values:
                # Create case-insensitive IN filter for all enum values
                # Use toLower() for case-insensitive matching with parameter
                param_counter += 1
                type_enum_param = f"param_{param_counter}"
                # Store lowercase enum values for case-insensitive matching
                enum_values_lower = [v.lower() for v in enum_values]
                params[type_enum_param] = enum_values_lower
                # Build case-insensitive IN condition using parameter
                # NOTE: This will be added ONLY to values query, not total/missing
                type_enum_filter = f"toLower(sf.{db_field}) IN ${type_enum_param}"
                logger.debug(
                    "Using enum-based IN filter for type count (values query only)",
                    enum_count=len(enum_values),
                    db_field=db_field
                )
        
        # Build base WHERE clause for file filters (applied before traversals)
        # NOTE: type_enum_filter is NOT included here - it's only for values query
        base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""
        
        # OPTIMIZATION STRATEGY:
        # Apply file filters FIRST (before traversals) to reduce dataset size
        # Use reverse traversal when no file filters exist (start from studies)
        # This significantly reduces the number of nodes processed
        
        # Check if we have file filters (excluding study path requirements)
        has_file_filters = len(base_where_conditions) > 0
        
        if has_file_filters:
            # OPTIMIZED PATTERN: Apply file filters FIRST (including IN clause for enum types)
            # Then traverse to study - this processes fewer files before traversals
            # The IN clause filters files to only valid enum types, significantly reducing dataset
            
            # Query 1: Total count (with file filters applied early)
            total_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {base_where_clause}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            RETURN count(sf) as total
            """.strip()

            # Query 2: Missing count (with file filters applied early)
            # For type count: Missing includes NULL file_type OR file_type not in enum list
            missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
            if type_enum_filter:
                # For type count: Missing = NULL OR not in enum list
                # Use NOT (IN enum) to catch files with invalid enum values
                missing_where_conditions.append(f"(sf.{db_field} IS NULL OR NOT ({type_enum_filter}))")
            else:
                # For other fields: Missing = NULL
                missing_where_conditions.append(f"sf.{db_field} IS NULL")
            missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)

            missing_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {missing_where_clause}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            RETURN count(sf) as missing
            """.strip()

            # Query 3: Values with counts (with file filters applied early)
            # For type count: Also apply enum IN filter to only group valid enum types
            field_where_conditions_filtered = base_where_conditions.copy() if base_where_conditions else []
            field_where_conditions_filtered.append(f"sf.{db_field} IS NOT NULL")
            # Add enum filter for type count (only for values query)
            if type_enum_filter:
                field_where_conditions_filtered.append(type_enum_filter)
            field_where_clause_filtered = "WHERE " + " AND ".join(field_where_conditions_filtered) if field_where_conditions_filtered else ""

            values_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {field_where_clause_filtered}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            WITH DISTINCT sf, sf.{db_field} as field_val
            WHERE field_val IS NOT NULL AND toString(field_val) <> '' AND toString(field_val) <> 'null'
            RETURN toString(field_val) as value, count(sf) as count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            # SIMPLE PATTERN: No file filters - use original pattern (already optimized)
            # Query 1: Total count
            total_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            RETURN count(sf) as total
            """.strip()

            # Query 2: Missing count
            # For type count: Missing includes NULL file_type OR file_type not in enum list
            if type_enum_filter:
                # For type count: Missing = NULL OR not in enum list
                missing_where_additional = f" AND (sf.{db_field} IS NULL OR NOT ({type_enum_filter}))"
            else:
                # For other fields: Missing = NULL
                missing_where_additional = f" AND sf.{db_field} IS NULL"

            missing_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL{missing_where_additional}
            RETURN count(sf) as missing
            """.strip()

            # Query 3: Values with counts
            # For type count: Also apply enum IN filter to only group valid enum types
            values_where_parts = [f"sf.{db_field} IS NOT NULL"]
            if type_enum_filter:
                values_where_parts.append(type_enum_filter)
            values_where_additional = " AND " + " AND ".join(values_where_parts) if values_where_parts else ""

            values_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL{values_where_additional}
            WITH DISTINCT sf, sf.{db_field} as field_val
            WHERE field_val IS NOT NULL AND toString(field_val) <> '' AND toString(field_val) <> 'null'
            RETURN toString(field_val) as value, count(sf) as count
            ORDER BY count DESC, value ASC
            """.strip()
        
        logger.info(
            "Executing count_files_by_field Cypher queries (optimized - early filtering)",
            field=field,
            db_field=db_field,
            has_file_filters=has_file_filters
        )
        
        # Execute 3 queries with retry logic (simplified queries for better performance)
        max_retries = 2
        retry_count = 0
        total = 0
        missing = 0
        values_records = []
        
        while retry_count <= max_retries:
            try:
                # Execute total query
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total = total_records[0].get("total", 0) if total_records else 0
                
                # Execute missing query
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing = missing_records[0].get("missing", 0) if missing_records else 0
                
                # Execute values query
                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if (total > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_files_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_files_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_files_by_field query after retries", error=str(e), exc_info=True)
                    raise
        
        # Format results
        # For "type" field, map file types to enum values and count non-matching as missing
        if field == "type":
            # Map file types to enum values and aggregate counts
            enum_counts: Dict[str, int] = {}
            non_matching_count = 0
            
            for record in values_records:
                raw_value = record.get("value")
                count = record.get("count", 0)
                
                # Map to enum value (case-insensitive)
                mapped_value = self._map_file_type_to_enum(raw_value)
                
                if mapped_value:
                    # Add to enum counts
                    enum_counts[mapped_value] = enum_counts.get(mapped_value, 0) + count
                else:
                    # Count as missing (non-matching type)
                    non_matching_count += count
            
            # Format results with enum values
            counts = [
                {"value": enum_value, "count": count}
                for enum_value, count in sorted(enum_counts.items(), key=lambda x: (-x[1], x[0]))
            ]
            
            # Add non-matching types to missing count
            missing = missing + non_matching_count
        else:
            # For other fields, use results as-is
            counts = []
            for record in values_records:
                counts.append({
                    "value": record.get("value"),
                    "count": record.get("count", 0)
                })
        
        logger.debug(
            "Completed sequencing file count by field",
            field=field,
            total=total,
            missing=missing,
            values_count=len(counts)
        )
        
        return {
            "total": total,
            "missing": missing,
            "values": counts
        }
    
    async def _count_files_by_depositions(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count sequencing files grouped by study_id (depositions).
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values
        """
        logger.debug("Counting sequencing files by depositions (study_id)", filters=filters)
        
        # Build WHERE conditions and parameters for filters
        base_where_conditions = []
        params = {}
        param_counter = 0
        
        # Add regular filters
        for filter_field, value in filters.items():
            # Handle unharmonized fields (e.g., metadata.unharmonized.file_name)
            if filter_field.startswith("metadata.unharmonized."):
                # Extract the actual database field name
                db_field_name = filter_field.replace("metadata.unharmonized.", "")
                self._validate_unharmonized_file_field(db_field_name, filter_field)

                param_counter += 1
                param_name = f"param_{param_counter}"

                if isinstance(value, list):
                    base_where_conditions.append(f"sf.{db_field_name} IN ${param_name}")
                else:
                    base_where_conditions.append(f"sf.{db_field_name} = ${param_name}")
                params[param_name] = value
                continue

            param_counter += 1
            param_name = f"param_{param_counter}"

            if isinstance(value, list):
                base_where_conditions.append(f"sf.{filter_field} IN ${param_name}")
            else:
                base_where_conditions.append(f"sf.{filter_field} = ${param_name}")
            params[param_name] = value

        # Build base WHERE clause for file filters
        base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""
        
        # OPTIMIZATION: Apply file filters FIRST (before traversals) to reduce dataset size
        # Check if we have file filters
        has_file_filters = len(base_where_conditions) > 0
        
        if has_file_filters:
            # OPTIMIZED PATTERN: Apply file filters FIRST, then traverse to study
            # Query 1: Total count (with file filters applied early)
            total_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {base_where_clause}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            RETURN count(sf) as total
            """.strip()

            # Query 2: Missing count (files with null study_id)
            missing_where_conditions = base_where_conditions.copy() if base_where_conditions else []
            missing_where_conditions.append("st.study_id IS NULL")
            missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)

            missing_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {base_where_clause}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            {missing_where_clause}
            RETURN count(sf) as missing
            """.strip()

            # Query 3: Values with counts - group by study_id (with file filters applied early)
            values_cypher = f"""
            MATCH (sf:{self.config.node_label})
            {base_where_clause}
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL AND st.study_id IS NOT NULL
            WITH DISTINCT sf, st.study_id as study_id_val
            WHERE study_id_val IS NOT NULL AND toString(study_id_val) <> '' AND toString(study_id_val) <> 'null'
            RETURN toString(study_id_val) as value, count(sf) as count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            # SIMPLE PATTERN: No file filters - use original pattern
            # Query 1: Total count
            total_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL
            RETURN count(sf) as total
            """.strip()

            # Query 2: Missing count (files with null study_id)
            missing_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH DISTINCT sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL AND st.study_id IS NULL
            RETURN count(sf) as missing
            """.strip()

            # Query 3: Values with counts - group by study_id
            values_cypher = f"""
            MATCH (sf:{self.config.node_label})-[:{self.config.rel_name}]->(sa:sample)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sf, coalesce(st1, st2) AS st
            WHERE st IS NOT NULL AND st.study_id IS NOT NULL
            WITH DISTINCT sf, st.study_id as study_id_val
            WHERE study_id_val IS NOT NULL AND toString(study_id_val) <> '' AND toString(study_id_val) <> 'null'
            RETURN toString(study_id_val) as value, count(sf) as count
            ORDER BY count DESC, value ASC
            """.strip()
        
        logger.info(
            "Executing count_files_by_depositions Cypher queries (optimized - early filtering)",
            depositions_count=True,
            has_file_filters=has_file_filters
        )
        
        # Execute total query
        total_result = await self.session.run(total_cypher, params)
        total_records = []
        async for record in total_result:
            total_records.append(dict(record))
        total = total_records[0].get("total", 0) if total_records else 0
        
        # Execute missing query
        missing_result = await self.session.run(missing_cypher, params)
        missing_records = []
        async for record in missing_result:
            missing_records.append(dict(record))
        missing = missing_records[0].get("missing", 0) if missing_records else 0
        
        # Execute values query
        values_result = await self.session.run(values_cypher, params)
        values_records = []
        async for record in values_result:
            values_records.append(dict(record))
        
        # Format results
        counts = []
        for record in values_records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })
        
        logger.debug(
            "Completed sequencing file count by depositions",
            total=total,
            missing=missing,
            values_count=len(counts)
        )
        
        return {
            "total": total,
            "missing": missing,
            "values": counts
        }
    
    async def _build_count_query(
        self,
        filters: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Build (cypher, params) for the study-reachable file count with the same
        filter logic as get_files_summary. Used by count_for_pagination and
        get_files_summary. Counts sequencing_file that reach a study via
        participant→consent→study or cell_line→study.

        The no-filter case (Pattern 3) uses streaming EXISTS + count(sf): the prior
        collect + count(DISTINCT sf) form materialized a hash set of all ~1.5M matched
        sf nodes and OOMed the snapshot (>7.65 GiB). The filtered cases (Patterns 1, 2,
        2b) keep the collect form — their filters bound the candidate set, so they do
        not OOM and the collect form is faster there than EXISTS.

        Returns a sentinel query ("RETURN 0 AS total_count", {}) when a filter
        value is provably empty (e.g., an invalid file_type enum value), so callers
        always receive a (str, dict) tuple and never need to special-case None.
        """
        # async def for consistent await-able interface with callers;
        # this method is currently pure computation with no I/O.
        # Build WHERE conditions and parameters
        where_conditions = []
        params: Dict[str, Any] = {}
        param_counter = 0

        # Handle depositions filter separately - it filters by study_id
        # Parse || separator for OR logic (e.g., "phs002517 || phs002790")
        # Make a copy to avoid modifying the original filters dict
        filters_copy = filters.copy()
        depositions_value = filters_copy.pop("depositions", None)
        depositions_list = None
        if depositions_value is not None:
            # Split on || separator and clean whitespace
            depositions_list = [d.strip() for d in depositions_value.split("||")]
            # Filter out empty strings
            depositions_list = [d for d in depositions_list if d]
            if not depositions_list:
                depositions_list = None
                depositions_value = None

        # Handle checksums filter separately - supports || separator for OR logic
        # Note: checksums in filters_copy is mapped to "md5sum" field by get_file_filters()
        checksums_value = filters_copy.pop("md5sum", None)
        checksums_list = None
        if checksums_value is not None:
            # Split on || separator and clean whitespace
            checksums_list = [c.strip() for c in checksums_value.split("||")]
            # Filter out empty strings
            checksums_list = [c for c in checksums_list if c]
            if not checksums_list:
                checksums_list = None

        # Validate type filter - must match enum value exactly (case-sensitive)
        # Note: filter key is "file_type" because get_file_filters() maps "type" -> "file_type"
        # After validation, use case-insensitive matching in the query to handle database case variations
        type_filter_param = None
        if "file_type" in filters_copy:
            type_value = filters_copy.pop("file_type")  # Remove from filters_copy to handle separately
            # Check if the type value exactly matches an enum value (case-sensitive)
            if type_value not in FileType.values():
                # Type doesn't match any enum value — return a sentinel zero-count query
                logger.info(
                    "Type filter value does not match any enum value (case-sensitive) - returning zero-count sentinel query",
                    type_value=type_value,
                    valid_values=FileType.values()[:5]  # Log first 5 for reference
                )
                return _ZERO_COUNT_SENTINEL, {}
            # Use case-insensitive matching in the query (toLower for both sides)
            param_counter += 1
            type_filter_param = f"param_{param_counter}"
            params[type_filter_param] = type_value
            logger.debug(
                "Type filter validated successfully for summary, will use case-insensitive matching in query",
                type_value=type_value
            )

        # Add regular filters
        for field, value in filters_copy.items():
            # Handle unharmonized fields (e.g., metadata.unharmonized.file_name)
            if field.startswith("metadata.unharmonized."):
                # Extract the actual database field name
                # e.g., "metadata.unharmonized.file_name" -> "file_name"
                db_field_name = field.replace("metadata.unharmonized.", "")
                self._validate_unharmonized_file_field(db_field_name, field)

                param_counter += 1
                param_name = f"param_{param_counter}"

                if isinstance(value, list):
                    where_conditions.append(f"sf.{db_field_name} IN ${param_name}")
                else:
                    where_conditions.append(f"sf.{db_field_name} = ${param_name}")
                params[param_name] = value
                continue

            param_counter += 1
            param_name = f"param_{param_counter}"

            # Convert file_size to number if it's a string that can be converted
            if field == "file_size" and isinstance(value, str):
                try:
                    # Strip quotes if present (e.g., '70925' -> 70925)
                    cleaned_value = value.strip().strip("'\"")
                    value = int(cleaned_value)
                except (ValueError, TypeError):
                    # If conversion fails, keep as string (might be a different format)
                    pass

            if isinstance(value, list):
                where_conditions.append(f"sf.{field} IN ${param_name}")
            else:
                where_conditions.append(f"sf.{field} = ${param_name}")
            params[param_name] = value

        # Add case-insensitive type filter if present
        if type_filter_param:
            where_conditions.append(f"toLower(sf.file_type) = toLower(${type_filter_param})")

        # Add checksums filter if present (supports || separator for OR logic)
        if checksums_list is not None:
            param_counter += 1
            checksums_param_name = f"param_{param_counter}"
            if len(checksums_list) == 1:
                # Single checksum: check both md5sum and checksum_value fields
                where_conditions.append(f"(sf.md5sum = ${checksums_param_name} OR sf.checksum_value = ${checksums_param_name})")
                params[checksums_param_name] = checksums_list[0]
            else:
                # Multiple checksums: check if either field is IN the list
                where_conditions.append(f"(sf.md5sum IN ${checksums_param_name} OR sf.checksum_value IN ${checksums_param_name})")
                params[checksums_param_name] = checksums_list

        # Build final query - OPTIMIZATION: Same as get_files, filter files FIRST
        # PERFORMANCE OPTIMIZATION: Split WHERE conditions into:
        # 1. File property filters (apply BEFORE any traversals)
        # 2. Study filters (apply AFTER study path is established)
        file_where_conditions = []  # Filters on sf.* properties
        study_where_conditions = []  # Filters on study properties

        if where_conditions:
            file_where_conditions = list(where_conditions)

        study_where_conditions.append("st IS NOT NULL")

        # Add depositions filter (filter by study_id)
        # Support || separator for OR logic (e.g., "phs002517 || phs002790")
        depositions_param_name = None
        if depositions_list is not None:
            param_counter += 1
            param_name = f"param_{param_counter}"
            depositions_param_name = param_name  # Store for CALL+UNION queries
            if len(depositions_list) == 1:
                study_where_conditions.append(f"st.study_id = ${param_name}")
                params[param_name] = depositions_list[0]
            else:
                study_where_conditions.append(f"st.study_id IN ${param_name}")
                params[param_name] = depositions_list

        # Build WHERE clauses
        file_where_clause = "WHERE " + " AND ".join(file_where_conditions) if file_where_conditions else ""
        study_where_clause = "WHERE " + " AND ".join(study_where_conditions)

        # Detect if we have depositions filter for CALL+UNION optimization
        has_depositions_filter = depositions_list is not None

        # CONDITIONAL OPTIMIZATION: Match get_files pattern (same 4 patterns, using multi-hop traversal)
        if file_where_conditions and has_depositions_filter:
            # PATTERN 1: OPTIMIZED SUMMARY (with file filters + depositions)
            # Use multi-hop traversal: sample -> participant -> consent_group -> study (preferred)
            # or sample -> cell_line -> study (fallback)
            # NOTE: this collect form is kept (not rewritten to streaming EXISTS like the
            # no-filter Pattern 3) because the file filters cap the candidate sf set — it
            # does not OOM here and is ~3x faster than EXISTS on selective filters.
            deposition_param = depositions_param_name
            deposition_operator = "=" if len(depositions_list) == 1 else "IN"

            cypher = f"""
            MATCH (sf:{self.config.node_label})
            {file_where_clause}
            // Use multi-hop traversal (with WITH clauses to prevent cartesian products)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            // study path 2 — via participant → consent → study (preferred path)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st2:study)
            WHERE st2.study_id {deposition_operator} ${deposition_param}
            WITH sf, sa, collect(DISTINCT st2) AS st2_list
            // study path 1 — via cell_line (fallback)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WHERE st1.study_id {deposition_operator} ${deposition_param}
            WITH sf, sa, st2_list, collect(DISTINCT st1) AS st1_list
            WITH sf, coalesce(st2_list[0], st1_list[0]) AS st
            WHERE st IS NOT NULL
            RETURN count(DISTINCT sf) as total_count
            """.strip()
        elif file_where_conditions:
            # PATTERN 2: OPTIMIZED SUMMARY (with file filters only)
            # Use multi-hop traversal: sample -> participant -> consent_group -> study (preferred)
            # or sample -> cell_line -> study (fallback)
            # NOTE: collect form kept (not streaming EXISTS) for the same reason as
            # Pattern 1 — the file filters bound the sf set, so it does not OOM and is
            # faster than EXISTS on selective filters.
            cypher = f"""
            MATCH (sf:{self.config.node_label})
            {file_where_clause}
            // Use multi-hop traversal (with WITH clauses to prevent cartesian products)
            OPTIONAL MATCH (sf)-[:{self.config.rel_name}]->(sa:sample)
            // study path 2 — via participant → consent → study (preferred path)
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)
                       -[:of_participant]->(:consent_group)
                       -[:of_consent_group]->(st2:study)
            WITH sf, sa, collect(DISTINCT st2) AS st2_list
            // study path 1 — via cell_line (fallback)
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sf, sa, st2_list, collect(DISTINCT st1) AS st1_list
            WITH sf, coalesce(st2_list[0], st1_list[0]) AS st
            {study_where_clause}
            RETURN count(DISTINCT sf) as total_count
            """.strip()
        elif has_depositions_filter:
            # PATTERN 2b: OPTIMIZED REVERSE TRAVERSAL SUMMARY (depositions only)
            # Use multi-hop traversal: Start from studies (2-10 nodes) instead of files (1M+ nodes)
            # Traverse: study <- consent_group <- participant <- sample <- sequencing_file
            # or: study <- cell_line <- sample <- sequencing_file
            deposition_param = depositions_param_name
            deposition_operator = "=" if len(depositions_list) == 1 else "IN"

            # Stream the files from both study paths and let count(DISTINCT) dedupe them.
            # Do NOT collect the file set into a list first: a single deposition can reach
            # ~758K files, and materialising them needed ~768MB. Streaming needs <=128MB and
            # is ~2x faster. Collecting the scalar guids instead of the nodes is *worse*, not
            # better (~1280MB): a node in a list is a lightweight handle, whereas the guids
            # materialise as strings and count(DISTINCT <string>) hashes strings rather than
            # node ids. The scalar-guid form in get_files Pattern 2b earns its keep by letting
            # pagination skip re-fetching nodes -- a count materialises no rows, so it gains
            # nothing from it.
            # UNION ALL, not UNION: count(DISTINCT sf) already dedupes, so UNION would dedupe
            # twice (measured 2.59s/256MB vs 1.81s/128MB).
            cypher = f"""
            // Start from study nodes (only a few studies)
            MATCH (st:study)
            WHERE st.study_id {deposition_operator} ${deposition_param}
            CALL {{
              // path 2 - via participant -> consent_group (preferred)
              WITH st
              MATCH (st)<-[:of_consent_group]-(:consent_group)<-[:of_participant]-(:participant)<-[:of_sample]-(:sample)<-[:{self.config.rel_name}]-(sf:{self.config.node_label})
              RETURN sf
              UNION ALL
              // path 1 - via cell_line (fallback)
              WITH st
              MATCH (st)<-[:of_cell_line]-(:cell_line)<-[:of_sample]-(:sample)<-[:{self.config.rel_name}]-(sf:{self.config.node_label})
              RETURN sf
            }}
            RETURN count(DISTINCT sf) as total_count
            """.strip()
        else:
            # PATTERN 3: no filters at all (the hot /file path).
            # Stream every sf and count those with any study path via EXISTS. The prior
            # collect + count(DISTINCT sf) form materialized a hash set of all ~1.5M
            # matched sf nodes and OOMed on the local snapshot (>7.65 GiB). EXISTS
            # short-circuits per-file and needs no DISTINCT (a sequencing_file maps to at
            # most one sample), so peak memory is ~O(1) instead of O(matched files).
            cypher = f"""
            MATCH (sf:{self.config.node_label})
            WHERE EXISTS {{ (sf)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) }}
               OR EXISTS {{ (sf)-[:{self.config.rel_name}]->(:sample)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) }}
            RETURN count(sf) AS total_count
            """.strip()

        return cypher, params

    async def count_for_pagination(self, filters: Dict[str, Any]) -> int:
        """
        Return the study-reachable file count for these filters.
        Used by FileService to split pagination across node types.

        Raises database exceptions directly — caller is responsible for handling.
        This is intentional: partial failures propagate rather than silently returning 0.
        """
        cypher, params = await self._build_count_query(filters)
        if cypher is _ZERO_COUNT_SENTINEL:
            return 0
        try:
            # Retry transient failures (timeout/memory/connection) before propagating,
            # so a blip on the heavy count query doesn't abort the whole /file response.
            return await run_count_query_with_retry(self.session, cypher, params)
        except Exception:
            logger.error(
                "Database error in count_for_pagination",
                node_label=self.config.node_label,
                filters=filters,
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
    
    def _map_file_type_to_enum(self, file_type: Any) -> Optional[str]:
        """
        Map a file type value to a FileType enum value (case-insensitive matching).
        
        Args:
            file_type: The file type value from the database
            
        Returns:
            The matching enum value if found, None otherwise
        """
        if file_type is None:
            return None
        
        file_type_str = str(file_type).strip()
        if not file_type_str:
            return None
        
        # Case-insensitive matching against enum values
        file_type_lower = file_type_str.lower()
        for enum_value in FileType.values():
            if enum_value.lower() == file_type_lower:
                return enum_value
        
        # No match found
        return None
    
    def _record_to_file(self, record: Dict[str, Any], samples: List[Any] = None, study: Dict[str, Any] = None) -> File:
        """
        Convert a database record to a File object with proper structure.
        
        Args:
            record: Database record dictionary (sequencing_file node)
            samples: List of sample nodes associated with this file
            study: Study node dictionary
            
        Returns:
            File object with id, samples, and metadata structure
        """
        from app.repositories.sample_converters import node_to_dict
        
        sf = node_to_dict(record)
        study_dict = node_to_dict(study) if study else {}
        
        # Get file identifier - use only guid field
        file_id = sf.get("guid") or ""
        study_id = study_dict.get("study_id", "")
        
        # Build file identifier
        file_identifier = {
            "namespace": {
                "organization": "CCDI-DCC",
                "name": study_id
            },
            "name": file_id
        }
        
        # Build samples array
        samples_list = []
        if samples:
            for sample_node in samples:
                sa = node_to_dict(sample_node)
                sample_id = sa.get("sample_id", "")
                if sample_id and study_id:
                    samples_list.append({
                        "namespace": {
                            "organization": "CCDI-DCC",
                            "name": study_id
                        },
                        "name": sample_id
                    })
        
        # Build metadata
        # Format metadata fields as objects with value property
        def format_metadata_value(value):
            """Format metadata value as object with value property or null."""
            if value is None:
                return None
            return {"value": value}
        
        # Format checksums with nested md5 structure
        def format_checksums(md5_value):
            """Format checksums as nested structure with md5."""
            if md5_value is None:
                return None
            return {
                "value": {
                    "md5": md5_value
                }
            }
        
        # Get depositions from study - format as objects with kind and value
        depositions = None
        if study_dict and study_dict.get("study_id"):
            study_id_value = study_dict.get("study_id")
            if study_id_value:
                depositions = [{"kind": "dbGaP", "value": study_id_value}]
        
        # Map file type to enum value (case-insensitive)
        raw_file_type = sf.get("file_type") or sf.get("type")
        mapped_file_type = self._map_file_type_to_enum(raw_file_type)
        
        # Build unharmonized section with file_name
        unharmonized = {}
        file_name_value = sf.get("file_name")
        if file_name_value is not None:
            unharmonized["file_name"] = format_metadata_value(file_name_value)

        # Merge any per-type additional unharmonized fields declared in config
        for api_field, db_property in self.config.unharmonized_fields.items():
            value = sf.get(db_property)
            if value is not None:
                unharmonized[api_field] = format_metadata_value(value)
        
        metadata = {
            "size": format_metadata_value(sf.get("file_size") or sf.get("size")),
            "type": format_metadata_value(mapped_file_type),  # Use mapped enum value or None
            "checksums": format_checksums(sf.get("md5sum")),
            "description": format_metadata_value(sf.get("file_description")),
            "depositions": depositions,
            "unharmonized": unharmonized if unharmonized else None
        }
        
        # Build file structure
        file_data = {
            "id": file_identifier,
            "samples": samples_list,
            "metadata": metadata
        }
        
        return File(**file_data)
