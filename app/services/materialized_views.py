"""
Materialized Views Service for File Count Queries.

This service manages pre-computed counts stored as nodes in the graph database,
providing near-instant responses for count queries.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from neo4j import AsyncSession

from app.core.logging import get_logger

logger = get_logger(__name__)


class MaterializedViewService:
    """Service for managing materialized views of file counts."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_file_count_by_type(
        self,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get file counts by type from materialized view.
        
        Args:
            filters: Optional filters (currently not supported in materialized views)
            
        Returns:
            Dictionary with total, missing, and values, or None if view doesn't exist
        """
        # For now, materialized views don't support filters
        # If filters are provided, fall back to live query
        if filters:
            return None  # Signal to use live query
        
        cypher = """
        MATCH (stats:FileCountStats {type: "by_type"})
        MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)
        WITH stats, count.value as value, count.count as count_val
        WITH stats, value, count_val
        WITH stats, collect(DISTINCT {value: value, count: count_val}) as counts
        RETURN 
            stats.total as total,
            stats.missing as missing,
            stats.last_updated as last_updated,
            counts as values
        """
        
        result = await self.session.run(cypher)
        record = await result.single()
        
        if not record:
            return None  # No materialized view exists
        
        # Sort values by count DESC, value ASC
        values = sorted(
            record["values"],
            key=lambda x: (-x["count"], x["value"])
        )
        
        return {
            "total": record["total"],
            "missing": record["missing"],
            "values": values,
            "last_updated": record["last_updated"]
        }
    
    async def get_file_count_by_depositions(
        self,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get file counts by depositions from materialized view.
        
        Args:
            filters: Optional filters (currently not supported in materialized views)
            
        Returns:
            Dictionary with total, missing, and values, or None if view doesn't exist
        """
        if filters:
            return None  # Signal to use live query
        
        cypher = """
        MATCH (stats:FileCountStats {type: "by_depositions"})
        MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)
        WITH stats, count.value as value, count.count as count_val
        WITH stats, value, count_val
        WITH stats, collect(DISTINCT {value: value, count: count_val}) as counts
        RETURN 
            stats.total as total,
            stats.missing as missing,
            stats.last_updated as last_updated,
            counts as values
        """
        
        result = await self.session.run(cypher)
        record = await result.single()
        
        if not record:
            return None  # No materialized view exists
        
        # Sort values by count DESC, value ASC
        values = sorted(
            record["values"],
            key=lambda x: (-x["count"], x["value"])
        )
        
        return {
            "total": record["total"],
            "missing": record["missing"],
            "values": values,
            "last_updated": record["last_updated"]
        }
    
    async def refresh_file_count_by_type(self) -> Dict[str, Any]:
        """
        Refresh materialized view for file counts by type.
        
        This runs the actual count queries and stores results in materialized view nodes.
        
        Returns:
            Dictionary with refresh statistics
        """
        logger.info("Refreshing materialized view: file count by type")
        
        # Step 1: Calculate counts using live queries
        # Import here to avoid circular dependencies
        from app.repositories.file import FileRepository
        from app.lib.field_allowlist import get_field_allowlist
        from app.config_data.file_node_registry import FILE_NODE_REGISTRY

        allowlist = get_field_allowlist()
        raw_results = []
        for cfg in FILE_NODE_REGISTRY:
            repo = FileRepository(self.session, allowlist, cfg)
            raw_results.append(await repo.count_files_by_field("type", {}))

        combined_values: dict = {}
        for r in raw_results:
            for item in r.get("values", []):
                v = item.get("value")
                if v is not None and v != "" and v != "null":
                    combined_values[v] = combined_values.get(v, 0) + item.get("count", 0)
        counts_result = {
            "total": sum(r.get("total", 0) for r in raw_results),
            "missing": sum(r.get("missing", 0) for r in raw_results),
            "values": [{"value": v, "count": c} for v, c in sorted(combined_values.items(), key=lambda x: (-x[1], x[0]))],
        }

        # Step 2: Delete old materialized view
        delete_cypher = """
        MATCH (stats:FileCountStats {type: "by_type"})
        OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)
        DETACH DELETE stats, count
        """
        await self.session.run(delete_cypher)
        
        # Step 3: Create new stats node
        create_stats_cypher = """
        CREATE (stats:FileCountStats {
            type: "by_type",
            total: $total,
            missing: $missing,
            last_updated: timestamp(),
            version: 1
        })
        RETURN stats
        """
        
        await self.session.run(
            create_stats_cypher,
            {
                "total": counts_result["total"],
                "missing": counts_result["missing"]
            }
        )
        
        # Step 4: Create count nodes
        if counts_result["values"]:
            # Batch create count nodes
            create_counts_cypher = """
            MATCH (stats:FileCountStats {type: "by_type"})
            UNWIND $counts AS count_data
            CREATE (count:FileCount {
                field: "file_type",
                value: count_data.value,
                count: count_data.count,
                stats_type: "by_type"
            })
            CREATE (count)-[:BELONGS_TO]->(stats)
            RETURN count
            """
            
            await self.session.run(
                create_counts_cypher,
                {"counts": counts_result["values"]}
            )
        
        logger.info(
            "Materialized view refreshed: file count by type",
            total=counts_result["total"],
            missing=counts_result["missing"],
            values_count=len(counts_result["values"])
        )
        
        return {
            "type": "by_type",
            "total": counts_result["total"],
            "missing": counts_result["missing"],
            "values_count": len(counts_result["values"]),
            "last_updated": datetime.now().isoformat()
        }
    
    async def refresh_file_count_by_depositions(self) -> Dict[str, Any]:
        """
        Refresh materialized view for file counts by depositions.
        
        Returns:
            Dictionary with refresh statistics
        """
        logger.info("Refreshing materialized view: file count by depositions")
        
        # Step 1: Calculate counts using live queries
        from app.repositories.file import FileRepository
        from app.lib.field_allowlist import get_field_allowlist
        from app.config_data.file_node_registry import FILE_NODE_REGISTRY

        allowlist = get_field_allowlist()
        raw_results = []
        for cfg in FILE_NODE_REGISTRY:
            repo = FileRepository(self.session, allowlist, cfg)
            raw_results.append(await repo.count_files_by_field("depositions", {}))

        combined_values: dict = {}
        for r in raw_results:
            for item in r.get("values", []):
                v = item.get("value")
                if v is not None and v != "" and v != "null":
                    combined_values[v] = combined_values.get(v, 0) + item.get("count", 0)
        counts_result = {
            "total": sum(r.get("total", 0) for r in raw_results),
            "missing": sum(r.get("missing", 0) for r in raw_results),
            "values": [{"value": v, "count": c} for v, c in sorted(combined_values.items(), key=lambda x: (-x[1], x[0]))],
        }

        # Step 2: Delete old materialized view
        delete_cypher = """
        MATCH (stats:FileCountStats {type: "by_depositions"})
        OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)
        DETACH DELETE stats, count
        """
        await self.session.run(delete_cypher)
        
        # Step 3: Create new stats node
        create_stats_cypher = """
        CREATE (stats:FileCountStats {
            type: "by_depositions",
            total: $total,
            missing: $missing,
            last_updated: timestamp(),
            version: 1
        })
        RETURN stats
        """
        
        await self.session.run(
            create_stats_cypher,
            {
                "total": counts_result["total"],
                "missing": counts_result["missing"]
            }
        )
        
        # Step 4: Create count nodes
        if counts_result["values"]:
            create_counts_cypher = """
            MATCH (stats:FileCountStats {type: "by_depositions"})
            UNWIND $counts AS count_data
            CREATE (count:FileCount {
                field: "study_id",
                value: count_data.value,
                count: count_data.count,
                stats_type: "by_depositions"
            })
            CREATE (count)-[:BELONGS_TO]->(stats)
            RETURN count
            """
            
            await self.session.run(
                create_counts_cypher,
                {"counts": counts_result["values"]}
            )
        
        logger.info(
            "Materialized view refreshed: file count by depositions",
            total=counts_result["total"],
            missing=counts_result["missing"],
            values_count=len(counts_result["values"])
        )
        
        return {
            "type": "by_depositions",
            "total": counts_result["total"],
            "missing": counts_result["missing"],
            "values_count": len(counts_result["values"]),
            "last_updated": datetime.now().isoformat()
        }
    
    async def refresh_all(self) -> Dict[str, Any]:
        """
        Refresh all materialized views.
        
        Returns:
            Dictionary with refresh statistics for all views
        """
        results = {}
        
        try:
            results["by_type"] = await self.refresh_file_count_by_type()
        except Exception as e:
            logger.error("Error refreshing by_type materialized view", error=str(e), exc_info=True)
            results["by_type"] = {"error": str(e)}
        
        try:
            results["by_depositions"] = await self.refresh_file_count_by_depositions()
        except Exception as e:
            logger.error("Error refreshing by_depositions materialized view", error=str(e), exc_info=True)
            results["by_depositions"] = {"error": str(e)}
        
        return results
    
    async def get_view_age(self, view_type: str) -> Optional[int]:
        """
        Get age of materialized view in seconds.
        
        Args:
            view_type: "by_type" or "by_depositions"
            
        Returns:
            Age in seconds, or None if view doesn't exist
        """
        cypher = """
        MATCH (stats:FileCountStats {type: $view_type})
        RETURN timestamp() - stats.last_updated as age_seconds
        """
        
        result = await self.session.run(cypher, {"view_type": view_type})
        record = await result.single()
        
        if record:
            return record["age_seconds"]
        return None

