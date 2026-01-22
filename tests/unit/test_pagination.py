"""
Unit tests for pagination utilities.
"""

import pytest
from app.core.pagination import (
    PaginationParams,
    PaginationInfo,
    calculate_pagination_info,
    build_link_header
)


@pytest.mark.unit
def test_pagination_params_valid():
    """Test creating valid PaginationParams."""
    params = PaginationParams(page=1, per_page=10)
    
    assert params.page == 1
    assert params.per_page == 10
    assert params.offset == 0


@pytest.mark.unit
def test_pagination_params_offset_calculation():
    """Test pagination offset calculation."""
    params = PaginationParams(page=3, per_page=20)
    
    assert params.offset == 40  # (3-1) * 20

