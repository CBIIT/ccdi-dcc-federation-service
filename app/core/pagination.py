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
    per_page: int = 50
    
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
    Build Link header for pagination according to RFC 5988 and OpenAPI spec.
    
    Requirements:
    - `first` and `last` links are REQUIRED (last can be same as first if single page)
    - `prev` and `next` links are OPTIONAL (only when multiple pages exist and appropriate)
    
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
    
    # Calculate total_pages if not provided but total_items is available
    total_pages = pagination.total_pages
    if total_pages is None and pagination.total_items is not None and pagination.total_items > 0:
        total_pages = (pagination.total_items + pagination.per_page - 1) // pagination.per_page
    
    # Determine if there are multiple pages
    # Multiple pages exist if: total_pages > 1, or has_next=True, or has_prev=True (implies page > 1)
    has_multiple_pages = (
        (total_pages is not None and total_pages > 1) or
        (pagination.has_next is True) or
        (pagination.has_prev is True)
    )
    
    # First page (required)
    first_params = {**query_params, 'page': 1, 'per_page': pagination.per_page}
    first_url = f"{base_url}?{urlencode(first_params)}"
    links.append(f'<{first_url}>; rel="first"')
    
    # Last page (required - must always be present)
    # If we don't know total_pages, use current page (or first page) as fallback
    if total_pages is not None:
        last_page = total_pages
    elif pagination.has_next is False:
        # If has_next is False, we're on the last page
        last_page = pagination.page
    else:
        # Fallback: assume single page (same as first)
        last_page = 1
    
    last_params = {**query_params, 'page': last_page, 'per_page': pagination.per_page}
    last_url = f"{base_url}?{urlencode(last_params)}"
    links.append(f'<{last_url}>; rel="last"')
    
    # Previous page (optional - only when multiple pages exist and not on first page)
    if has_multiple_pages and pagination.has_prev:
        prev_params = {**query_params, 'page': pagination.page - 1, 'per_page': pagination.per_page}
        prev_url = f"{base_url}?{urlencode(prev_params)}"
        links.append(f'<{prev_url}>; rel="prev"')
    
    # Next page (optional - only when multiple pages exist and not on last page)
    if has_multiple_pages and pagination.has_next:
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
