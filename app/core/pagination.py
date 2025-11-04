"""
Pagination utilities for the CCDI Federation Service.

This module provides utilities for handling pagination and Link headers
according to the OpenAPI specification.
"""

from typing import Dict, Optional
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from fastapi import Request
from pydantic import BaseModel

from app.core.config import get_settings


class PaginationParams(BaseModel):
    """Pagination parameters model."""
    
    page: int = 1
    per_page: int = 100
    
    def __post_init__(self):
        """Validate pagination parameters."""
        settings = get_settings()
        
        if self.page < 1:
            raise ValueError("Page must be >= 1")
        
        if self.per_page < 1:
            raise ValueError("per_page must be >= 1")
            
        if self.per_page > settings.max_page_size:
            raise ValueError(f"per_page cannot exceed {settings.max_page_size}")
    
    @property
    def offset(self) -> int:
        """Calculate offset for database queries."""
        return (self.page - 1) * self.per_page
    
    @property
    def limit(self) -> int:
        """Get limit for database queries."""
        return self.per_page


class PaginationInfo(BaseModel):
    """Pagination information for responses."""
    
    page: int
    per_page: int
    total_pages: Optional[int] = None
    total_items: Optional[int] = None
    has_next: Optional[bool] = None
    has_prev: Optional[bool] = None


def calculate_pagination_info(
    page: int, per_page: int, total_items: int
) -> PaginationInfo:
    """Calculate pagination information."""
    total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 0
    
    return PaginationInfo(
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total_items=total_items,
        has_next=page < total_pages,
        has_prev=page > 1
    )


def build_link_header(
    request: Request,
    pagination: PaginationInfo,
    extra_params: Optional[Dict[str, str]] = None
) -> str:
    """
    Build Link header for pagination according to RFC 5988.
    
    Args:
        request: FastAPI request object
        pagination: Pagination information
        extra_params: Additional query parameters to preserve
        
    Returns:
        Link header string
    """
    base_url = str(request.url).split('?')[0]
    query_params = dict(request.query_params)
    
    # Add any extra parameters
    if extra_params:
        query_params.update(extra_params)
    
    # Remove page parameter as we'll set it explicitly
    query_params.pop('page', None)
    
    links = []
    
    # First page (required)
    first_params = {**query_params, 'page': 1, 'per_page': pagination.per_page}
    first_url = f"{base_url}?{urlencode(first_params)}"
    links.append(f'<{first_url}>; rel="first"')
    
    # Last page (only if we have total_pages)
    if pagination.total_pages is not None:
        last_params = {**query_params, 'page': pagination.total_pages, 'per_page': pagination.per_page}
        last_url = f"{base_url}?{urlencode(last_params)}"
        links.append(f'<{last_url}>; rel="last"')
    
    # Previous page (optional)
    if pagination.has_prev:
        prev_params = {**query_params, 'page': pagination.page - 1, 'per_page': pagination.per_page}
        prev_url = f"{base_url}?{urlencode(prev_params)}"
        links.append(f'<{prev_url}>; rel="prev"')
    
    # Next page (optional)
    if pagination.has_next:
        next_params = {**query_params, 'page': pagination.page + 1, 'per_page': pagination.per_page}
        next_url = f"{base_url}?{urlencode(next_params)}"
        links.append(f'<{next_url}>; rel="next"')
    
    return ', '.join(links)


def parse_pagination_params(
    page: Optional[int] = None, 
    per_page: Optional[int] = None
) -> PaginationParams:
    """
    Parse and validate pagination parameters.
    
    Args:
        page: Page number (1-based)
        per_page: Items per page
        
    Returns:
        Validated pagination parameters
        
    Raises:
        ValueError: If parameters are invalid
    """
    settings = get_settings()
    
    # Set defaults
    if page is None:
        page = 1
    if per_page is None:
        per_page = settings.default_page_size
    
    # Validate
    if page < 1:
        raise ValueError("Page must be >= 1")
    
    if per_page < 1:
        raise ValueError("per_page must be >= 1")
        
    if per_page > settings.max_page_size:
        raise ValueError(f"per_page cannot exceed {settings.max_page_size}")
    
    return PaginationParams(page=page, per_page=per_page)
