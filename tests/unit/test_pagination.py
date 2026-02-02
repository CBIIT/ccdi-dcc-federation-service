"""
Unit tests for pagination utilities.
"""

import pytest
from unittest.mock import Mock, patch
from app.core.pagination import (
    PaginationParams,
    PaginationInfo,
    calculate_pagination_info,
    build_link_header,
    parse_pagination_params,
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


@pytest.mark.unit
def test_calculate_pagination_info():
    """Test pagination info calculation."""
    info = calculate_pagination_info(page=2, per_page=10, total_items=35)
    assert info.total_pages == 4
    assert info.has_next is True
    assert info.has_prev is True


@pytest.mark.unit
def test_build_link_header_with_total_pages():
    """Test Link header generation with total_pages."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=2&per_page=10"
    request.query_params = {"page": "2", "per_page": "10", "sex": "F"}
    pagination = PaginationInfo(page=2, per_page=10, total_pages=5, has_next=True, has_prev=True)

    header = build_link_header(request, pagination)

    assert 'rel="first"' in header
    assert 'rel="last"' in header
    assert 'rel="prev"' in header
    assert 'rel="next"' in header
    assert "sex=F" in header


@pytest.mark.unit
def test_build_link_header_infers_last_page_from_total_items():
    """Test Link header infers last page from total_items."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=1"
    request.query_params = {"page": "1"}
    pagination = PaginationInfo(page=1, per_page=10, total_items=15, has_next=True, has_prev=False)

    header = build_link_header(request, pagination)

    assert 'rel="last"' in header
    assert "page=2" in header  # last page should be 2


@pytest.mark.unit
def test_build_link_header_single_page_no_next_prev():
    """Test Link header when only one page exists."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject"
    request.query_params = {}
    pagination = PaginationInfo(page=1, per_page=10, total_pages=None, has_next=False, has_prev=False)

    header = build_link_header(request, pagination)

    assert 'rel="first"' in header
    assert 'rel="last"' in header
    assert 'rel="next"' not in header
    assert 'rel="prev"' not in header


@pytest.mark.unit
def test_build_link_header_with_extra_params():
    """Test Link header includes extra_params."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=1"
    request.query_params = {"page": "1"}
    pagination = PaginationInfo(page=1, per_page=10, total_pages=2, has_next=True, has_prev=False)

    header = build_link_header(request, pagination, extra_params={"race": "White"})
    assert "race=White" in header


@pytest.mark.unit
def test_parse_pagination_params_defaults_and_validation():
    """Test parse_pagination_params defaults and validation."""
    mock_settings = Mock()
    mock_settings.default_page_size = 25
    mock_settings.max_page_size = 100

    with patch("app.core.pagination.get_settings", return_value=mock_settings):
        params = parse_pagination_params()
        assert params.page == 1
        assert params.per_page == 25

        with pytest.raises(ValueError):
            parse_pagination_params(page=0, per_page=10)

        with pytest.raises(ValueError):
            parse_pagination_params(page=1, per_page=0)

        with pytest.raises(ValueError):
            parse_pagination_params(page=1, per_page=101)


@pytest.mark.unit
def test_pagination_params_post_init_validation():
    """Test PaginationParams.__post_init__ validation (called manually)."""
    from app.core.pagination import PaginationParams
    
    mock_settings = Mock()
    mock_settings.max_page_size = 100
    
    with patch('app.core.pagination.get_settings', return_value=mock_settings):
        # Create instance
        params = PaginationParams(page=1, per_page=10)
        
        # Test __post_init__ validation by calling it manually
        # This tests lines 25-34 in pagination.py
        params.__post_init__()  # Should not raise for valid values
        
        # Test invalid page
        params.page = 0
        with pytest.raises(ValueError, match="Page must be >= 1"):
            params.__post_init__()
        
        # Test invalid per_page
        params.page = 1
        params.per_page = 0
        with pytest.raises(ValueError, match="per_page must be >= 1"):
            params.__post_init__()
        
        # Test per_page exceeds max
        params.per_page = 101
        with pytest.raises(ValueError, match="per_page cannot exceed"):
            params.__post_init__()


@pytest.mark.unit
def test_build_link_header_last_page_fallback():
    """Test Link header last page fallback when total_pages is None and has_next is True."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=1"
    request.query_params = {"page": "1"}
    # has_next=True but total_pages=None - should fallback to page 1
    pagination = PaginationInfo(page=1, per_page=10, total_pages=None, total_items=None, has_next=True, has_prev=False)
    
    header = build_link_header(request, pagination)
    
    assert 'rel="last"' in header
    assert "page=1" in header  # Should fallback to first page


@pytest.mark.unit
def test_build_link_header_last_page_from_has_next_false():
    """Test Link header uses current page as last when has_next is False."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=3"
    request.query_params = {"page": "3"}
    pagination = PaginationInfo(page=3, per_page=10, total_pages=None, total_items=None, has_next=False, has_prev=True)
    
    header = build_link_header(request, pagination)
    
    assert 'rel="last"' in header
    assert "page=3" in header  # Should use current page


@pytest.mark.unit
def test_build_link_header_multiple_pages_detection():
    """Test Link header detects multiple pages from has_next/has_prev."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=2"
    request.query_params = {"page": "2"}
    # Multiple pages detected from has_prev=True even without total_pages
    pagination = PaginationInfo(page=2, per_page=10, total_pages=None, total_items=None, has_next=True, has_prev=True)
    
    header = build_link_header(request, pagination)
    
    assert 'rel="prev"' in header
    assert 'rel="next"' in header


@pytest.mark.unit
def test_calculate_pagination_info_zero_total_items():
    """Test pagination info calculation with zero total items."""
    info = calculate_pagination_info(page=1, per_page=10, total_items=0)
    assert info.total_pages == 0
    assert info.has_next is False
    assert info.has_prev is False


@pytest.mark.unit
def test_calculate_pagination_info_exact_multiple():
    """Test pagination info when total_items is exact multiple of per_page."""
    info = calculate_pagination_info(page=1, per_page=10, total_items=30)
    assert info.total_pages == 3
    assert info.has_next is True


@pytest.mark.unit
def test_build_link_header_preserves_query_params():
    """Test Link header preserves all query parameters except page."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?page=2&sex=M&race=White"
    request.query_params = {"page": "2", "sex": "M", "race": "White"}
    pagination = PaginationInfo(page=2, per_page=10, total_pages=5, has_next=True, has_prev=True)
    
    header = build_link_header(request, pagination)
    
    assert "sex=M" in header
    assert "race=White" in header
    assert "page=2" not in header or header.count("page=") == 4  # Should appear in all links but not from query_params


@pytest.mark.unit
def test_build_link_header_with_extra_params_overrides():
    """Test Link header extra_params override query_params."""
    request = Mock()
    request.url = "http://example.org/api/v1/subject?race=Black"
    request.query_params = {"race": "Black"}
    pagination = PaginationInfo(page=1, per_page=10, total_pages=2, has_next=True, has_prev=False)
    
    header = build_link_header(request, pagination, extra_params={"race": "White"})
    
    assert "race=White" in header
    assert "race=Black" not in header

