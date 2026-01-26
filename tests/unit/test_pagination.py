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

    def test_pagination_params_post_init_validation(self):
        """Test PaginationParams.__post_init__ validation (called manually)."""
        from app.core.pagination import PaginationParams
        from unittest.mock import patch
        
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

