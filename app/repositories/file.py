"""
File repository for the CCDI Federation Service.

This module provides data access operations for sequencing files
using Cypher queries to Memgraph.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.core.constants import FileType
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import File
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)


class FileRepository:
    """Repository for sequencing file data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        
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
        # Make a copy to avoid modifying the original filters dict
        filters_copy = filters.copy()
        depositions_value = filters_copy.pop("depositions", None)
        
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
        
        # Build final query
        # Only include sequencing_files that have a path to a study
        # Path 1: sequencing_file -> sample -> participant -> consent_group -> study
        # Path 2: sequencing_file -> sample -> cell_line -> study
        
        # Combine all WHERE conditions
        all_where_conditions = []
        
        # Add study filter (must have a study)
        all_where_conditions.append("st IS NOT NULL")
        
        # Add depositions filter (filter by study_id)
        if depositions_value is not None:
            param_counter += 1
            param_name = f"param_{param_counter}"
            all_where_conditions.append(f"st.study_id = ${param_name}")
            params[param_name] = depositions_value
        
        # Add file field filters (applied to sf)
        if where_conditions:
            all_where_conditions.extend(where_conditions)
        
        where_clause = "WHERE " + " AND ".join(all_where_conditions) if all_where_conditions else ""
        
        # Optimized query: Apply pagination early, then collect samples only for returned files
        # This avoids collecting samples for all files before pagination - KEY PERFORMANCE OPTIMIZATION
        cypher = f"""
        // Step 1: Find all sequencing_files with their study relationships (for filtering)
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {where_clause}
        // Step 2: Apply pagination early (before collecting samples) - this is the key optimization
        // This means we only process the files we need, not all files
        WITH sf
        ORDER BY sf.id
        SKIP $offset
        LIMIT $limit
        // Step 3: Now collect samples only for the paginated files (much faster!)
        OPTIONAL MATCH (sf)-[:of_sequencing_file]->(sa2:sample)
        OPTIONAL MATCH (sa2)-[:of_sample]->(p2:participant)
        OPTIONAL MATCH (p2)-[:of_participant]->(c2:consent_group)-[:of_consent_group]->(st3:study)
        OPTIONAL MATCH (sa2)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st4:study)
        WITH sf, collect(DISTINCT sa2) AS samples, head(collect(DISTINCT coalesce(st3, st4))) AS st
        RETURN sf, samples, st
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
        # Use sf.id field
        # Only include sequencing_files that have a path to a study
        # Path 1: sequencing_file -> sample -> participant -> consent_group -> study
        # Path 2: sequencing_file -> sample -> cell_line -> study
        cypher = """
        MATCH (sf:sequencing_file)
        WHERE sf.id = $name
        MATCH (sf)-[:of_sequencing_file]->(sa:sample)
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
        
        # Build base WHERE clause (for filtering)
        base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""
        
        # Build WHERE clause for values query (includes field IS NOT NULL)
        field_where_conditions = base_where_conditions.copy() if base_where_conditions else []
        field_where_conditions.append("st IS NOT NULL")
        field_where_conditions.append(f"sf.{db_field} IS NOT NULL")
        field_where_clause = "WHERE " + " AND ".join(field_where_conditions) if field_where_conditions else ""
        
        # Build WHERE clause for total and missing queries (includes study path check)
        all_where_conditions = base_where_conditions.copy() if base_where_conditions else []
        all_where_conditions.append("st IS NOT NULL")
        all_where_clause = "WHERE " + " AND ".join(all_where_conditions) if all_where_conditions else "WHERE st IS NOT NULL"
        
        # Query for total count
        total_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {all_where_clause}
        RETURN count(DISTINCT sf) as total
        """.strip()
        
        # Query for missing count (files with null field value)
        missing_where_conditions = all_where_conditions.copy() if all_where_conditions else []
        missing_where_conditions.append(f"sf.{db_field} IS NULL")
        missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)
        
        missing_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {missing_where_clause}
        RETURN count(DISTINCT sf) as missing
        """.strip()
        
        # Query for values with counts
        # Handle both list and string values without using APOC functions
        # For file fields like file_type, the value is typically a string
        # We wrap single values in a list for UNWIND, and use lists directly
        # In Memgraph, filter invalid values in the CASE statement instead of using WHERE after UNWIND
        values_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH sf, coalesce(st1, st2) AS st
        {field_where_clause}
        WITH DISTINCT sf, sf.{db_field} as field_val
        WITH sf, field_val,
             CASE 
               WHEN field_val IS NULL OR toString(field_val) = '' OR toString(field_val) = 'null' THEN []
               ELSE [field_val]
             END as field_values
        UNWIND field_values as value
        WITH value, sf
        WHERE value IS NOT NULL
        RETURN toString(value) as value, count(DISTINCT sf) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_files_by_field Cypher queries",
            field=field,
            db_field=db_field,
            values_cypher=values_cypher
        )
        
        # Execute queries with retry logic
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
        
        # Build WHERE clause for total and missing queries (includes study path check)
        all_where_conditions = base_where_conditions.copy() if base_where_conditions else []
        all_where_conditions.append("st IS NOT NULL")
        all_where_clause = "WHERE " + " AND ".join(all_where_conditions) if all_where_conditions else "WHERE st IS NOT NULL"
        
        # Query for total count
        total_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {all_where_clause}
        RETURN count(DISTINCT sf) as total
        """.strip()
        
        # Query for missing count (files with null study_id)
        missing_where_conditions = all_where_conditions.copy() if all_where_conditions else []
        missing_where_conditions.append("st.study_id IS NULL")
        missing_where_clause = "WHERE " + " AND ".join(missing_where_conditions)
        
        missing_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {missing_where_clause}
        RETURN count(DISTINCT sf) as missing
        """.strip()
        
        # Query for values with counts - group by study_id
        field_where_conditions = base_where_conditions.copy() if base_where_conditions else []
        field_where_conditions.append("st IS NOT NULL")
        field_where_conditions.append("st.study_id IS NOT NULL")
        field_where_clause = "WHERE " + " AND ".join(field_where_conditions) if field_where_conditions else ""
        
        values_cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH sf, coalesce(st1, st2) AS st
        {field_where_clause}
        WITH DISTINCT sf, st.study_id as study_id_val
        WITH sf, study_id_val,
             CASE 
               WHEN study_id_val IS NULL OR toString(study_id_val) = '' OR toString(study_id_val) = 'null' THEN []
               ELSE [study_id_val]
             END as field_values
        UNWIND field_values as value
        WITH value, sf
        WHERE value IS NOT NULL
        RETURN toString(value) as value, count(DISTINCT sf) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_files_by_depositions Cypher queries",
            values_cypher=values_cypher
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
    
    async def get_files_summary(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for sequencing files.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting sequencing files summary", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle depositions filter separately - it filters by study_id
        # Make a copy to avoid modifying the original filters dict
        filters_copy = filters.copy()
        depositions_value = filters_copy.pop("depositions", None)
        
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
                    "Type filter value does not match any enum value (case-sensitive) - returning empty summary",
                    type_value=type_value,
                    valid_values=FileType.values()[:5]  # Log first 5 for reference
                )
                return {"total_count": 0}
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
        
        # Build final query
        # Only include sequencing_files that have a path to a study
        # Path 1: sequencing_file -> sample -> participant -> consent_group -> study
        # Path 2: sequencing_file -> sample -> cell_line -> study
        all_where_conditions = ["st IS NOT NULL"]
        if where_conditions:
            all_where_conditions.extend(where_conditions)
        
        # Add depositions filter (filter by study_id)
        if depositions_value is not None:
            param_counter += 1
            param_name = f"param_{param_counter}"
            all_where_conditions.append(f"st.study_id = ${param_name}")
            params[param_name] = depositions_value
        
        where_clause = "WHERE " + " AND ".join(all_where_conditions)
        
        # Optimized summary query: Use DISTINCT earlier to reduce work
        cypher = f"""
        MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa:sample)
        OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st2:study)
        WITH DISTINCT sf, coalesce(st1, st2) AS st
        {where_clause}
        RETURN count(sf) as total_count
        """.strip()
        
        logger.info(
            "Executing get_files_summary Cypher query",
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
                
                # If we got results, break out of retry loop
                if records:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_files_summary query (attempt {retry_count + 1})")
                else:
                    break
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_files_summary query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in get_files_summary query after retries", error=str(e), exc_info=True)
                    raise
        
        if not records:
            logger.debug("No records returned from get_files_summary query")
            return {"total_count": 0}
        
        summary = records[0]
        logger.debug("Completed sequencing files summary", total_count=summary.get("total_count", 0))
        
        return summary
    
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
        # Convert node objects to dictionaries
        def node_to_dict(node):
            """Convert a Node object to a dictionary."""
            if node is None:
                return {}
            if isinstance(node, dict):
                return node
            try:
                return dict(node)
            except (TypeError, ValueError):
                if hasattr(node, 'properties'):
                    return node.properties
                elif hasattr(node, 'items'):
                    return dict(node.items())
                else:
                    return {k: getattr(node, k) for k in dir(node) if not k.startswith('_')}
        
        sf = node_to_dict(record)
        study_dict = node_to_dict(study) if study else {}
        
        # Get file identifier - use only id field
        file_id = sf.get("id") or ""
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
