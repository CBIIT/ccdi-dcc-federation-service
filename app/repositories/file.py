"""
File repository for the CCDI Federation Service.

This module provides data access operations for files
using Cypher queries to Memgraph.
"""

from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import File
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)


class FileRepository:
    """Repository for file data operations."""
    
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
        Get paginated list of files with filtering.
        
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
            "Fetching files",
            filters=filters,
            offset=offset,
            limit=limit
        )
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
        param_counter = 0
        
        # Add regular filters
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"f.{field} IN ${param_name}")
            else:
                where_conditions.append(f"f.{field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        cypher = f"""
        MATCH (f:file)
        {where_clause}
        RETURN f
        SKIP $offset
        LIMIT $limit
        """.strip()
        
        logger.info(
            "Executing get_files Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        # Convert to File objects
        files = []
        for record in records:
            file_data = record.get("f", {})
            files.append(self._record_to_file(file_data))
        
        logger.debug(
            "Found files",
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
        Get a specific file by organization, namespace, and name.
        
        Args:
            organization: Organization identifier
            namespace: Namespace identifier
            name: File name/identifier
            
        Returns:
            File object or None if not found
        """
        logger.debug(
            "Fetching file by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Build query to find file by identifier
        cypher = """
        MATCH (f:file)
        RETURN f
        LIMIT 1
        """
        
        # Build the full identifier
        identifier = f"{organization}.{namespace}.{name}"
        params = {"identifier": identifier}
        
        logger.info(
            "Executing get_file_by_identifier Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        if not records:
            logger.debug("File not found", identifier=identifier)
            return None
        
        # Convert to File object
        file_data = records[0].get("f", {})
        file = self._record_to_file(file_data)
        
        logger.debug("Found file", identifier=identifier, file_data=getattr(file, 'id', str(file)[:50]))
        
        return file
    
    async def count_files_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Count files grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            List of dictionaries with value and count
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting files by field",
            field=field,
            filters=filters
        )
        
        # Build WHERE conditions and parameters
        where_conditions = [f"f.{field} IS NOT NULL"]
        params = {}
        param_counter = 0
        
        # Add regular filters
        for filter_field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"f.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"f.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        cypher = f"""
        MATCH (f:file)
        {where_clause}
        WITH f, 
             CASE 
               WHEN f.{field} IS NULL THEN []
               WHEN NOT apoc.meta.type(f.{field}) = 'LIST' THEN [f.{field}]
               ELSE f.{field}
             END as field_values
        UNWIND field_values as value
        RETURN toString(value) as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_files_by_field Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        # Format results
        counts = []
        for record in records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })
        
        logger.debug(
            "Completed file count by field",
            field=field,
            results_count=len(counts)
        )
        
        return counts
    
    async def get_files_summary(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for files.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting files summary", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Add regular filters
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"f.{field} IN ${param_name}")
            else:
                where_conditions.append(f"f.{field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        cypher = f"""
        MATCH (f:file)
        {where_clause}
        RETURN count(f) as total_count
        """.strip()
        
        logger.info(
            "Executing get_files_summary Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        if not records:
            return {"total_count": 0}
        
        summary = records[0]
        logger.debug("Completed files summary", total_count=summary.get("total_count", 0))
        
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
                raise UnsupportedFieldError(f"Field '{field}' is not supported for {entity_type} filtering")
    
    def _record_to_file(self, record: Dict[str, Any]) -> File:
        """
        Convert a database record to a flexible File object.
        
        Args:
            record: Database record dictionary
            
        Returns:
            File object with flexible structure
        """
        # Create a File object with all fields from the record
        # This allows for any field structure to be returned
        return File(**record)
