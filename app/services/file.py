"""
File service for the CCDI Federation Service.

This module provides business logic for sequencing file operations,
including caching, validation, and coordination between
repositories and API endpoints.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from neo4j import AsyncSession

from app.config_data.file_node_registry import FILE_NODE_REGISTRY
from app.core.config import Settings
from app.core.logging import get_logger
from app.core.cache import CacheService
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import File, FileResponse, CountResponse, SummaryResponse
from app.models.errors import NotFoundError, ValidationError
from app.repositories.file import FileRepository
from app.services.materialized_views import MaterializedViewService
from app.db.memgraph import DatabaseConnectionError

logger = get_logger(__name__)


class FileService:
    """Service for sequencing file business logic."""
    
    def __init__(
        self,
        session: AsyncSession,
        allowlist: FieldAllowlist,
        settings: Settings,
        cache_service: Optional[CacheService] = None
    ):
        """Initialize service with dependencies."""
        self.materialized_view_service = MaterializedViewService(session)
        self.settings = settings
        self.cache_service = cache_service
        # Build one repository per registered file node type
        self._repos = [
            FileRepository(session, allowlist, cfg)
            for cfg in FILE_NODE_REGISTRY
        ]
        
    async def get_files(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20
    ) -> tuple[List[File], int]:
        """
        Get paginated list of files across all registered node types.

        Returns:
            Tuple of (files, total_count).
        """
        logger.debug(
            "Getting sequencing files",
            filters=filters,
            offset=offset,
            limit=limit
        )

        # Validate pagination limits
        if limit > self.settings.pagination.max_page_size:
            limit = self.settings.pagination.max_page_size

        try:
            # Sequential per repo: all repos share one session; concurrent gather
            # would cause "read() called while another coroutine is already waiting".
            counts = []
            for repo in self._repos:
                counts.append(await repo.count_for_pagination(filters))
            total_count = sum(counts)

            # Offset-split: determine which repos contribute to this page
            contributions: list[tuple] = []  # (repo, local_offset, local_limit)
            remaining_offset = offset
            remaining_limit = limit

            for repo, count in zip(self._repos, counts):
                if remaining_offset >= count:
                    remaining_offset -= count
                    continue
                local_limit = min(remaining_limit, count - remaining_offset)
                contributions.append((repo, remaining_offset, local_limit))
                remaining_offset = 0
                remaining_limit -= local_limit
                if remaining_limit == 0:
                    break

            if not contributions:
                return [], total_count

            files = []
            for repo, lo, ll in contributions:
                batch = await repo.get_files(filters, lo, ll)
                files.extend(batch)

            logger.info(
                "Retrieved sequencing files",
                count=len(files),
                total=total_count,
                offset=offset,
                limit=limit
            )
            return files, total_count

        except DatabaseConnectionError as e:
            logger.error(
                "Database connection error while fetching files",
                error=str(e),
                error_type=type(e).__name__,
                filters=filters,
                is_database_connection_error=True,
            )
            return [], 0
    
    async def get_file_by_identifier(
        self,
        organization: str,
        namespace: str,
        name: str
    ) -> File:
        """
        Get a specific sequencing file by organization, namespace, and name.
        
        Args:
            organization: Organization identifier (must be "CCDI-DCC")
            namespace: Namespace identifier (study_id)
            name: File identifier (file_id)
            
        Returns:
            File object
            
        Raises:
            NotFoundError: If sequencing file is not found
        """
        logger.debug(
            "Getting sequencing file by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Validate parameters
        self._validate_identifier_params(organization, namespace, name)

        result = None
        for repo in self._repos:
            match = await repo.get_file_by_identifier(organization, namespace, name)
            if match is not None:
                result = match
                break

        if not result:
            raise NotFoundError("Files")

        logger.info(
            "Retrieved sequencing file by identifier",
            organization=organization,
            namespace=namespace,
            name=name,
        )
        return result
    
    async def count_files_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> CountResponse:
        """
        Count sequencing files grouped by a specific field value.
        
        Uses materialized views when available (no filters), otherwise falls back
        to live queries from the repository.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            CountResponse with field counts
        """
        logger.debug(
            "Counting sequencing files by field",
            field=field,
            filters=filters
        )
        
        # Check cache first
        cache_key = None
        if self.cache_service:
            cache_key = self._build_cache_key("file_count", field, filters)
            cached_result = await self.cache_service.get(cache_key)
            if cached_result:
                logger.debug("Returning cached sequencing file count", field=field)
                return CountResponse(**cached_result)
        
        # Sequential per repo: shared session cannot handle concurrent queries.
        # Each call is individually timeout-bounded by the remaining budget.
        query_timeout = self.settings.query_timeout or 60
        start_time = time.time()
        try:
            results = []
            for repo in self._repos:
                remaining = query_timeout - (time.time() - start_time)
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                results.append(
                    await asyncio.wait_for(
                        repo.count_files_by_field(field, filters),
                        timeout=remaining,
                    )
                )
        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            logger.error(
                "Query timeout exceeded",
                field=field,
                timeout=query_timeout,
                elapsed_time=elapsed,
                filters=filters
            )
            raise ValidationError(
                "Query execution exceeded the allowed timeout. "
                "The request may be too complex or the database is under heavy load."
            )

        total = sum(r.get("total", 0) for r in results)
        missing = sum(r.get("missing", 0) for r in results)

        combined: Dict[str, int] = {}
        for r in results:
            for item in r.get("values", []):
                v = item.get("value")
                if v is not None and v != "" and v != "null":
                    combined[v] = combined.get(v, 0) + item.get("count", 0)

        values = [
            {"value": v, "count": c}
            for v, c in sorted(combined.items(), key=lambda x: (-x[1], x[0]))
        ]

        # Build response
        response = CountResponse(
            total=total,
            missing=missing,
            values=values,
        )
        
        # Cache result
        if self.cache_service and cache_key:
            await self.cache_service.set(
                cache_key,
                response.model_dump(),
                ttl=self.settings.cache.count_ttl
            )
        
        logger.info(
            "Completed sequencing file count by field",
            field=field,
            total=response.total,
            missing=response.missing,
            values_count=len(response.values)
        )
        
        return response
    
    async def get_files_summary(
        self,
        filters: Dict[str, Any]
    ) -> SummaryResponse:
        """
        Get summary statistics for sequencing files.
        
        Args:
            filters: Filters to apply
            
        Returns:
            SummaryResponse with summary statistics
        """
        logger.debug("Getting sequencing files summary", filters=filters)
        
        # Check cache first
        cache_key = None
        if self.cache_service:
            cache_key = self._build_cache_key("file_summary", None, filters)
            cached_result = await self.cache_service.get(cache_key)
            if cached_result:
                logger.debug("Returning cached sequencing files summary")
                return SummaryResponse(**cached_result)
        
        # Sequential per repo: shared session cannot handle concurrent queries.
        try:
            counts = []
            for repo in self._repos:
                counts.append(await repo.count_for_pagination(filters))
        except DatabaseConnectionError as e:
            logger.error(
                "Database connection error while fetching files summary",
                error=str(e),
                error_type=type(e).__name__,
                filters=filters,
                is_database_connection_error=True,
            )
            from app.models.dto import SummaryCounts
            return SummaryResponse(counts=SummaryCounts(total=0))

        from app.models.dto import SummaryCounts
        response = SummaryResponse(
            counts=SummaryCounts(total=sum(counts))
        )
        
        # Cache result
        if self.cache_service and cache_key:
            await self.cache_service.set(
                cache_key,
                response.model_dump(),
                ttl=self.settings.cache.summary_ttl
            )
        
        logger.info(
            "Completed sequencing files summary",
            total_count=response.counts.total
        )
        
        return response
    
    def _validate_identifier_params(self, organization: str, namespace: str, name: str) -> None:
        """
        Validate identifier parameters.
        
        Args:
            organization: Organization identifier (must be "CCDI-DCC")
            namespace: Namespace identifier (study_id)
            name: File identifier (file_id)
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not organization or not organization.strip():
            raise ValidationError("Organization identifier cannot be empty")
        
        # Organization must be "CCDI-DCC"
        if organization.strip() != "CCDI-DCC":
            raise ValidationError(f"Invalid organization: {organization}. Only 'CCDI-DCC' is supported.")
        
        if not namespace or not namespace.strip():
            raise ValidationError("Namespace identifier cannot be empty")
        
        if not name or not name.strip():
            raise ValidationError("File identifier cannot be empty")
        
        # Check for invalid characters
        # For organization and namespace, check for path separators and spaces
        for param_name, param_value in [("organization", organization), ("namespace", namespace)]:
            if any(char in param_value for char in ["/", "\\", " "]):
                raise ValidationError(f"Invalid characters in {param_name}: {param_value}")
        
        # For name (file_id), only restrict path separators (allow dots, underscores, hyphens for file names)
        if any(char in name for char in ["/", "\\"]):
            raise ValidationError(f"Invalid characters in name: {name}")
    
    def _build_cache_key(
        self,
        operation: str,
        field: Optional[str],
        filters: Dict[str, Any]
    ) -> str:
        """
        Build cache key for caching results.
        
        Args:
            operation: Type of operation (count, summary, etc.)
            field: Field name for count operations
            filters: Applied filters
            
        Returns:
            Cache key string
        """
        # Sort filters for consistent cache keys
        filter_items = sorted(filters.items()) if filters else []
        filter_str = "|".join([f"{k}:{v}" for k, v in filter_items])
        
        if field:
            return f"{operation}:{field}:{filter_str}"
        else:
            return f"{operation}:{filter_str}"
