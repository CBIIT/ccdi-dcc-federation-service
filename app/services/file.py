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

from app.core.config import Settings
from app.core.logging import get_logger
from app.core.cache import CacheService
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import File, FileResponse, CountResponse, SummaryResponse
from app.models.errors import NotFoundError, ValidationError
from app.repositories.file import FileRepository

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
        self.repository = FileRepository(session, allowlist)
        self.settings = settings
        self.cache_service = cache_service
        
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
        """
        logger.debug(
            "Getting sequencing files",
            filters=filters,
            offset=offset,
            limit=limit
        )
        
        # Validate pagination limits
        if limit > self.settings.pagination.max_per_page:
            limit = self.settings.pagination.max_per_page
            logger.debug(
                "Limiting page size",
                requested=limit,
                max_allowed=self.settings.pagination.max_per_page
            )
        
        # Get data from repository
        files = await self.repository.get_files(filters, offset, limit)
        
        logger.info(
            "Retrieved sequencing files",
            count=len(files),
            offset=offset,
            limit=limit
        )
        
        return files
    
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
        
        # Get from repository
        file = await self.repository.get_file_by_identifier(organization, namespace, name)
        
        if not file:
            raise NotFoundError("Files")
        
        logger.info(
            "Retrieved sequencing file by identifier",
            organization=organization,
            namespace=namespace,
            name=name,
            file_data=getattr(file, 'id', str(file)[:50])  # Flexible logging
        )
        
        return file
    
    async def count_files_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> CountResponse:
        """
        Count sequencing files grouped by a specific field value.
        
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
        
        # Get counts from repository with timeout handling
        query_timeout = self.settings.query_timeout or 60  # Default to 60 seconds
        start_time = time.time()
        try:
            result = await asyncio.wait_for(
                self.repository.count_files_by_field(field, filters),
                timeout=query_timeout
            )
            elapsed_time = time.time() - start_time
            if elapsed_time > 10:  # Log warning if query takes more than 10 seconds
                logger.warning(
                    "Slow query detected",
                    field=field,
                    elapsed_time=elapsed_time,
                    timeout=query_timeout
                )
        except asyncio.TimeoutError:
            elapsed_time = time.time() - start_time
            logger.error(
                "Query timeout exceeded",
                field=field,
                timeout=query_timeout,
                elapsed_time=elapsed_time,
                filters=filters
            )
            raise ValidationError(
                "Query execution exceeded the allowed timeout. "
                "The request may be too complex or the database is under heavy load."
            )
        
        # Build response
        response = CountResponse(
            total=result.get("total", 0),
            missing=result.get("missing", 0),
            values=result.get("values", [])
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
        
        # Get summary from repository
        summary_data = await self.repository.get_files_summary(filters)
        
        # Transform repository format to response format
        from app.models.dto import SummaryCounts
        response = SummaryResponse(
            counts=SummaryCounts(total=summary_data.get("total_count", 0))
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
