"""
Unit tests for subject API endpoints.

Tests subject listing, retrieval, counting, and summary endpoints.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status, Path
from neo4j import AsyncSession

from app.api.v1.endpoints.subjects import (
    list_subjects,
    get_subject,
    count_subjects_by_field,
    get_subjects_summary,
    prepare_subjects_for_response,
    router as subjects_router
)
from app.models.dto import Subject, SubjectResponse, CountResponse, SummaryResponse
from app.models.errors import ErrorKind, InvalidParametersError, NotFoundError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestPrepareSubjectsForResponse:
    """Test cases for prepare_subjects_for_response helper function."""

    def test_prepare_subjects_excludes_gateways(self):
        """Test that prepare_subjects_for_response excludes gateways."""
        # Create a proper mock subject that has model_dump
        class MockSubject:
            def model_dump(self, exclude=None, exclude_none=False, exclude_unset=False):
                result = {
                    "id": {"name": "subject1"},
                    "kind": "Participant",
                    "metadata": {}
                }
                # If gateways is in exclude, don't include it
                if exclude and "gateways" in exclude:
                    pass  # Don't add gateways
                else:
                    result["gateways"] = {"some": "data"}
                return result
        
        subjects = [MockSubject()]
        result = prepare_subjects_for_response(subjects)
        
        assert len(result) == 1
        assert "gateways" not in result[0]
        assert result[0]["id"]["name"] == "subject1"


@pytest.mark.unit
class TestSubjectEndpoints:
    """Test cases for subject API endpoints."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        return settings

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock()
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.url.path = "/subject"
        # Create a dict-like object that supports both dict() conversion and .keys()
        # Must inherit from dict to support dict() conversion
        class QueryParams(dict):
            def __init__(self, params):
                super().__init__(params)
                self._params = params
            
            def keys(self):
                return self._params.keys()
        
        request.query_params = QueryParams({"page": "1", "per_page": "20"})
        return request

    @pytest.fixture
    def mock_response(self):
        """Create a mock response."""
        response = Mock(spec=Response)
        response.headers = {}
        return response

    @pytest.fixture
    def mock_pagination(self):
        """Create mock pagination params."""
        from app.core.pagination import PaginationParams
        return PaginationParams(page=1, per_page=20)

    async def test_list_subjects_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_subjects returns subjects successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        class MockSubject:
            def model_dump(self, exclude=None, exclude_none=False, exclude_unset=False):
                return {
                    "id": {"name": "subject1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "kind": "Participant",
                    "metadata": {}
                }
        
        mock_subjects = [MockSubject()]
        mock_summary = SummaryResponse(counts=SummaryCounts(total=100))
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(return_value=mock_subjects)
            mock_service.get_subjects_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    result = await list_subjects(
                        request=mock_request,
                        response=mock_response,
                        filters={},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 1
        # Check summary structure - it's a dict with "counts" key
        assert "summary" in result.model_dump()
        assert result.summary["counts"]["all"] == 100

    async def test_list_subjects_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test list_subjects handles database connection errors."""
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await list_subjects(
                            request=mock_request,
                            response=mock_response,
                            filters={},
                            pagination=mock_pagination,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_subject_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_subject returns a single subject."""
        class MockSubject:
            def model_dump(self, exclude=None, exclude_none=False, exclude_unset=False):
                return {
                    "id": {"name": "subject1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "kind": "Participant",
                    "metadata": {}
                }
        
        mock_subject = MockSubject()
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subject_by_identifier = AsyncMock(return_value=mock_subject)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    result = await get_subject(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="subject1",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, dict)
        assert result["id"]["name"] == "subject1"

    async def test_get_subject_not_found(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_subject returns empty result when subject not found."""
        # The endpoint returns an empty SubjectResponse when not found, not a 404
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subject_by_identifier = AsyncMock(return_value=None)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    result = await get_subject(
                        organization="CCDI-DCC",
                        namespace="phs002431",
                        name="nonexistent",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        # Should return a SubjectResponse with empty data
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0

    async def test_count_subjects_by_field_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test count_subjects_by_field returns count successfully."""
        from app.models.dto import CountResponse
        
        # The service returns a CountResponse object with total, missing, and values attributes
        # CountResponse has: total, missing, values (list of dicts with value and count)
        mock_count_response = CountResponse(
            total=1000,
            missing=0,
            values=[{"value": "M", "count": 500}, {"value": "F", "count": 500}]
        )
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.count_subjects_by_field = AsyncMock(return_value=mock_count_response)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    result = await count_subjects_by_field(
                        field="sex",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, CountResponse)
        assert result.total == 1000
        assert len(result.values) == 2
        # CountResult is a Pydantic model, access as object attributes
        assert result.values[0].value == "M"
        assert result.values[0].count == 500

    async def test_get_subjects_summary_success(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_subjects_summary returns summary successfully."""
        from app.models.dto import SummaryResponse, SummaryCounts
        
        # Mock request with no query parameters (summary endpoint doesn't accept parameters)
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        mock_summary = SummaryResponse(counts=SummaryCounts(total=500))
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects_summary = AsyncMock(return_value=mock_summary)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    result = await get_subjects_summary(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
        
        assert isinstance(result, SummaryResponse)
        assert result.counts.total == 500
        # Verify service was called with empty filters dict
        mock_service.get_subjects_summary.assert_called_once_with({})

    async def test_get_subjects_summary_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request
    ):
        """Test get_subjects_summary handles database connection errors."""
        # Mock request with no query parameters (summary endpoint doesn't accept parameters)
        mock_query_params = Mock()
        mock_query_params.keys = Mock(return_value=[])
        mock_query_params.__bool__ = Mock(return_value=False)
        mock_request.query_params = mock_query_params
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects_summary = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with patch('app.api.v1.endpoints.subjects.check_rate_limit', return_value=None):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_subjects_summary(
                            request=mock_request,
                            session=mock_session,
                            settings=mock_settings,
                            allowlist=mock_allowlist,
                            _rate_limit=None
                        )
                    
                    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
                    # Verify service was called with empty filters dict
                    mock_service.get_subjects_summary.assert_called_once_with({})

