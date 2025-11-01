"""
Subject service for the CCDI Federation Service.

This module provides business logic for subject operations,
including caching, validation, and coordination between
repositories and API endpoints.
"""

from typing import List, Dict, Any, Optional
from neo4j import AsyncSession

from app.core.config import Settings
from app.core.logging import get_logger
from app.core.cache import CacheService
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Subject, SubjectResponse, CountResponse, SummaryResponse
from app.models.errors import NotFoundError, ValidationError
from app.repositories.subject import SubjectRepository

logger = get_logger(__name__)


class SubjectService:
    """Service for subject business logic."""
    
    def __init__(
        self,
        session: AsyncSession,
        allowlist: FieldAllowlist,
        settings: Settings,
        cache_service: Optional[CacheService] = None
    ):
        """Initialize service with dependencies."""
        self.repository = SubjectRepository(session, allowlist, settings)
        self.settings = settings
        self.cache_service = cache_service
        
    async def get_subjects(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20
    ) -> List[Subject]:
        """
        Get paginated list of subjects with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of Subject objects
        """
        logger.debug(
            "Getting subjects",
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
        subjects = await self.repository.get_subjects(filters, offset, limit)
        
        logger.info(
            "Retrieved subjects",
            count=len(subjects),
            offset=offset,
            limit=limit
        )
        
        return subjects
    
    async def get_subject_by_identifier(
        self,
        org: str,
        ns: str,
        name: str
    ) -> Subject:
        """
        Get a specific subject by organization, namespace, and name.
        
        Args:
            org: Organization identifier
            ns: Namespace identifier  
            name: Subject name/identifier
            
        Returns:
            Subject object
            
        Raises:
            NotFoundError: If subject is not found
        """
        logger.debug(
            "Getting subject by identifier",
            org=org,
            ns=ns,
            name=name
        )
        
        # Validate parameters
        self._validate_identifier_params(org, ns, name)
        
        # Get from repository
        subject = await self.repository.get_subject_by_identifier(org, ns, name)
        
        if not subject:
            raise NotFoundError(f"Subject not found: {org}.{ns}.{name}")
        
        logger.info(
            "Retrieved subject by identifier",
            org=org,
            ns=ns,
            name=name,
            subject_data=getattr(subject, 'id', str(subject)[:50])  # Flexible logging
        )
        
        return subject
    
    async def count_subjects_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> CountResponse:
        """
        Count subjects grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            CountResponse with field counts
        """
        logger.debug(
            "Counting subjects by field",
            field=field,
            filters=filters
        )
        
        # Check cache first
        cache_key = None
        if self.cache_service:
            cache_key = self._build_cache_key("subject_count", field, filters)
            cached_result = await self.cache_service.get(cache_key)
            if cached_result:
                logger.debug("Returning cached subject count", field=field)
                return CountResponse(**cached_result)
        
        # Get counts from repository
        result = await self.repository.count_subjects_by_field(field, filters)
        
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
                response.dict(),
                ttl=self.settings.cache.count_ttl
            )
        
        logger.info(
            "Completed subject count by field",
            field=field,
            total=response.total,
            missing=response.missing,
            values_count=len(response.values)
        )
        
        return response
    
    async def get_subjects_summary(
        self,
        filters: Dict[str, Any]
    ) -> SummaryResponse:
        """
        Get summary statistics for subjects.
        
        Args:
            filters: Filters to apply
            
        Returns:
            SummaryResponse with summary statistics
        """
        logger.debug(
            "Getting subjects summary",
            filters=filters,
            race_filter_type=type(filters.get("race")).__name__ if "race" in filters else None,
            race_filter_value=filters.get("race")
        )
        
        # Check cache first
        cache_key = None
        if self.cache_service:
            cache_key = self._build_cache_key("subject_summary", None, filters)
            logger.debug("Cache key", cache_key=cache_key, filters=filters)
            cached_result = await self.cache_service.get(cache_key)
            if cached_result:
                logger.debug(
                    "Returning cached subjects summary",
                    cached_total=cached_result.get("counts", {}).get("total") if isinstance(cached_result, dict) and "counts" in cached_result else cached_result.get("total_count")
                )
                # Handle both old and new cache formats
                if "counts" in cached_result:
                    return SummaryResponse(**cached_result)
                else:
                    # Transform old format to new format
                    from app.models.dto import SummaryCounts
                    return SummaryResponse(
                        counts=SummaryCounts(total=cached_result.get("total_count", 0))
                    )
        
        # Get summary from repository
        logger.debug("Calling repository.get_subjects_summary", filters=filters)
        summary_data = await self.repository.get_subjects_summary(filters)
        logger.debug("Repository returned", total_count=summary_data.get("total_count"))
        
        # Transform repository format to response format
        from app.models.dto import SummaryResponse, SummaryCounts
        response = SummaryResponse(
            counts=SummaryCounts(total=summary_data.get("total_count", 0))
        )
        
        # Cache result
        if self.cache_service and cache_key:
            logger.debug("Caching summary result", cache_key=cache_key, total=response.counts.total)
            await self.cache_service.set(
                cache_key,
                response.dict(),
                ttl=self.settings.cache.summary_ttl
            )
        
        logger.info(
            "Completed subjects summary",
            total_count=response.counts.total
        )
        
        return response
    
    def _validate_identifier_params(self, org: str, ns: str, name: str) -> None:
        """
        Validate identifier parameters.
        
        Args:
            org: Organization identifier
            ns: Namespace identifier
            name: Subject name
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not org or not org.strip():
            raise ValidationError("Organization identifier cannot be empty")
        
        if not ns or not ns.strip():
            raise ValidationError("Namespace identifier cannot be empty")
        
        if not name or not name.strip():
            raise ValidationError("Subject name cannot be empty")
        
        # Check for invalid characters
        for param_name, param_value in [("org", org), ("ns", ns), ("name", name)]:
            if any(char in param_value for char in [".", "/", "\\", " "]):
                raise ValidationError(f"Invalid characters in {param_name}: {param_value}")
    
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
        # Normalize list values to strings for consistent cache keys
        normalized_filters = {}
        for k, v in filters.items():
            if isinstance(v, list):
                # Sort list and join with comma for consistent cache key
                sorted_items = sorted([str(item).strip() for item in v if item])
                normalized_filters[k] = ",".join(sorted_items) if sorted_items else ""
            else:
                normalized_filters[k] = str(v) if v is not None else ""
        
        filter_items = sorted(normalized_filters.items()) if normalized_filters else []
        filter_str = "|".join([f"{k}:{v}" for k, v in filter_items])
        
        if field:
            return f"{operation}:{field}:{filter_str}"
        else:
            return f"{operation}:{filter_str}"
