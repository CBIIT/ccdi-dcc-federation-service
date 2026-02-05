"""
Enhanced unit tests for subject API endpoints.

Tests additional edge cases, complex scenarios, and error handling paths
that are not covered by existing tests.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.subjects import (
    get_subject,
    get_subjects_summary,
    count_subjects_by_field,
    list_subjects
)
from app.models.dto import Subject, SubjectResponse, CountResponse, SummaryResponse, SummaryCounts
from app.models.errors import ErrorKind, InvalidParametersError, InvalidRouteError
from app.db.memgraph import DatabaseConnectionError


@pytest.mark.unit
class TestSubjectEndpointsEnhanced:
    """Enhanced test cases for subject endpoints."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.method = "GET"
        request.url.path = "/api/v1/subject/by/sex/count"
        request.url.scheme = "http"
        request.url.netloc = "localhost:8000"
        request.query_params = {}
        return request

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        return settings

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock()
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    async def test_get_subject_field_name_as_participant_id(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject treats field names (like 'sex') as regular participant IDs."""
        from app.services.subject import SubjectService
        
        mock_request.query_params = {}
        
        # Mock service - field names are now treated as participant IDs, not filters
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_subject = Mock(spec=Subject)
            mock_subject.model_dump = Mock(return_value={"id": {"name": "sex"}})
            mock_service.get_subject_by_identifier = AsyncMock(return_value=mock_subject)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                result = await get_subject(
                    organization="CCDI-DCC",
                    namespace="phs002431",
                    name="sex",  # Treated as participant ID, not field filter
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                # Should return subject dict (single participant ID lookup)
                assert isinstance(result, dict)
                assert "id" in result
                # Verify it was called as participant ID lookup, not filter
                mock_service.get_subject_by_identifier.assert_called_once()

    async def test_get_subject_participant_id_search_single(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject with single participant ID search."""
        from app.services.subject import SubjectService
        
        mock_request.query_params = {}
        
        # Mock service
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_subject = Mock(spec=Subject)
            mock_subject.model_dump = Mock(return_value={"id": {"name": "TEST-001"}})
            mock_service.get_subject_by_identifier = AsyncMock(return_value=mock_subject)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                result = await get_subject(
                    organization="CCDI-DCC",
                    namespace="phs002431",
                    name="TEST-001",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                # Should return dict for single participant ID
                assert isinstance(result, dict)
                assert "id" in result

    async def test_get_subject_participant_id_search_multiple_rejected(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject rejects multiple participant IDs (comma-separated)."""
        mock_request.query_params = {}
        
        # The endpoint should raise InvalidParametersError for comma-separated IDs
        with pytest.raises(HTTPException) as exc_info:
            await get_subject(
                organization="CCDI-DCC",
                namespace="phs002431",
                name="TEST-001,TEST-002",  # Multiple IDs - should be rejected
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        error_detail = exc_info.value.detail.get("errors", [])[0] if isinstance(exc_info.value.detail, dict) else None
        if error_detail:
            assert error_detail.get("kind") == "InvalidParameters"
            reason_lower = error_detail.get("reason", "").lower()
            assert ("single participant id" in reason_lower or "multiple ids" in reason_lower or "not supported" in reason_lower)

    async def test_get_subject_participant_id_search_no_namespace(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject with participant ID search without namespace."""
        from app.services.subject import SubjectService
        
        mock_request.query_params = {}
        
        # Mock service - now always uses get_subject_by_identifier (even without namespace)
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_subject = Mock(spec=Subject)
            mock_subject.model_dump = Mock(return_value={"id": {"name": "TEST-001"}})
            mock_service.get_subject_by_identifier = AsyncMock(return_value=mock_subject)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                result = await get_subject(
                    organization="CCDI-DCC",
                    namespace="",  # Empty namespace - still uses get_subject_by_identifier
                    name="TEST-001",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                # Should return dict for single participant ID
                assert isinstance(result, dict)
                assert "id" in result
                # Verify it was called as participant ID lookup
                mock_service.get_subject_by_identifier.assert_called_once()

    @pytest.mark.skip(reason="Complex mocking required for get_subject_by_identifier with namespace")
    async def test_get_subject_participant_id_search_no_matches(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject with participant ID search that finds no matches (skipped - complex mocking)."""
        pass

    async def test_get_subject_typo_detection(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject detects typo in route."""
        mock_request.url.path = "/api/v1/subject/b1y/sex/count"
        mock_request.query_params = {}
        
        # The typo detection logic checks if name == "count" and organization looks like "by"
        # InvalidRouteError is converted to HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await get_subject(
                organization="b1y",  # Looks like "by" typo
                namespace="sex",  # Valid field name
                name="count",  # This triggers typo detection
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        # InvalidRouteError is converted to HTTPException with 404 status
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_subject_invalid_organization(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject with invalid organization."""
        mock_request.query_params = {}
        
        # InvalidParametersError is converted to HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await get_subject(
                organization="INVALID",
                namespace="phs002431",
                name="TEST-001",
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_get_subject_empty_organization_defaults(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subject with empty organization defaults to CCDI-DCC."""
        from app.services.subject import SubjectService
        
        mock_request.query_params = {}
        
        # Mock service
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_subject = Mock(spec=Subject)
            mock_subject.model_dump = Mock(return_value={"id": {"name": "TEST-001"}})
            mock_service.get_subject_by_identifier = AsyncMock(return_value=mock_subject)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                result = await get_subject(
                    organization="",
                    namespace="phs002431",
                    name="TEST-001",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                # Should succeed with default organization
                assert isinstance(result, dict)

    async def test_get_subjects_summary_rejects_parameters(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary rejects any query parameters."""
        # Mock request with query parameters - need to support len() check
        class QueryParamsWithSex(dict):
            def __init__(self):
                super().__init__({"sex": "M"})
            
            def keys(self):
                return ["sex"]
            
            def __len__(self):
                return 1
        
        mock_request.query_params = QueryParamsWithSex()
        
        # The endpoint should reject any parameters and raise InvalidParametersError
        with pytest.raises(HTTPException) as exc_info:
            await get_subjects_summary(
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        error_detail = exc_info.value.detail.get("errors", [])[0] if isinstance(exc_info.value.detail, dict) else None
        if error_detail:
            assert error_detail.get("kind") == "InvalidParameters"
            assert "does not accept any query parameters" in error_detail.get("reason", "")

    async def test_get_subjects_summary_no_parameters(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary with no parameters returns summary."""
        from app.services.subject import SubjectService
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
        # Mock service
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_summary = AsyncMock(return_value=SummaryResponse(counts=SummaryCounts(total=100)))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                result = await get_subjects_summary(
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None
                )
                
                assert isinstance(result, SummaryResponse)
                assert result.counts.total == 100
                # Verify service was called with empty filters dict
                mock_service.get_subjects_summary.assert_called_once_with({})

    async def test_get_subjects_summary_database_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary handles database connection errors."""
        from app.services.subject import SubjectService
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
        # Mock service to raise database error
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_summary = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
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

    async def test_get_subjects_summary_connection_error_in_generic_handler(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary handles connection errors in generic exception handler."""
        from app.services.subject import SubjectService
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
        # Mock service to raise a generic exception with connection-related message
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_summary = AsyncMock(
                side_effect=Exception("Database connection timeout")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
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

    async def test_get_subjects_summary_generic_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary handles generic errors (non-connection)."""
        from app.services.subject import SubjectService
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
        # Mock service to raise a generic exception without connection-related message
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_summary = AsyncMock(
                side_effect=ValueError("Invalid data")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
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

    async def test_get_subjects_summary_error_with_to_http_exception(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test get_subjects_summary handles errors with to_http_exception method."""
        from app.services.subject import SubjectService
        from fastapi import HTTPException as FastAPIHTTPException
        
        # Mock request with no query parameters - need to support len() check
        class EmptyQueryParams(dict):
            def __init__(self):
                super().__init__()
            
            def keys(self):
                return []
            
            def __len__(self):
                return 0
        
        mock_request.query_params = EmptyQueryParams()
        
        # Create a custom exception with to_http_exception method
        class CustomError(Exception):
            def to_http_exception(self):
                return FastAPIHTTPException(status_code=400, detail="Custom error")
        
        # Mock service to raise custom error
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_summary = AsyncMock(
                side_effect=CustomError("Custom error occurred")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await get_subjects_summary(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == 400
                assert exc_info.value.detail == "Custom error"

    async def test_count_subjects_by_field_database_error(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_subjects_by_field handles database connection errors."""
        from app.services.subject import SubjectService
        
        # Mock service to raise database error
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.count_subjects_by_field = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_subjects_by_field(
                        request=mock_request,
                        field="sex",
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                # Should return 404
                # Note: field_name and filters bugs were fixed (previously would have caused NameError)
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
                error_detail = exc_info.value.detail["errors"][0]
                assert error_detail["kind"] == ErrorKind.NOT_FOUND

    async def test_count_subjects_by_field_connection_error_detection(self, mock_request, mock_session, mock_settings, mock_allowlist):
        """Test count_subjects_by_field detects connection-related errors."""
        from app.services.subject import SubjectService
        
        # Mock service to raise connection-related error (not DatabaseConnectionError)
        connection_error = Exception("Database connection timeout")
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.count_subjects_by_field = AsyncMock(
                side_effect=connection_error
            )
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.subjects.get_cache_service', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await count_subjects_by_field(
                        request=mock_request,
                        field="sex",
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None
                    )
                
                # Should detect connection error and return 404
                # Note: field_name and filters bugs were fixed (previously would have caused NameError)
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
                error_detail = exc_info.value.detail["errors"][0]
                assert error_detail["kind"] == ErrorKind.NOT_FOUND

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

    async def test_list_subjects_invalid_filter_values(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects returns empty result when invalid filter values are provided."""
        filters = {"_invalid_ethnicity": True}
        
        result = await list_subjects(
            request=mock_request,
            response=mock_response,
            filters=filters,
            pagination=mock_pagination,
            session=mock_session,
            settings=mock_settings,
            allowlist=mock_allowlist,
            _rate_limit=None
        )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0
        assert result.summary["counts"]["current"] == 0

    async def test_list_subjects_invalid_sex_filter(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects returns empty result when invalid sex filter is provided."""
        filters = {"_invalid_sex": True}
        
        result = await list_subjects(
            request=mock_request,
            response=mock_response,
            filters=filters,
            pagination=mock_pagination,
            session=mock_session,
            settings=mock_settings,
            allowlist=mock_allowlist,
            _rate_limit=None
        )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0

    async def test_list_subjects_invalid_race_filter(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects returns empty result when invalid race filter is provided."""
        filters = {"_invalid_race": True}
        
        result = await list_subjects(
            request=mock_request,
            response=mock_response,
            filters=filters,
            pagination=mock_pagination,
            session=mock_session,
            settings=mock_settings,
            allowlist=mock_allowlist,
            _rate_limit=None
        )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0

    async def test_list_subjects_invalid_vital_status_filter(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects returns empty result when invalid vital_status filter is provided."""
        filters = {"_invalid_vital_status": True}
        
        result = await list_subjects(
            request=mock_request,
            response=mock_response,
            filters=filters,
            pagination=mock_pagination,
            session=mock_session,
            settings=mock_settings,
            allowlist=mock_allowlist,
            _rate_limit=None
        )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0

    async def test_list_subjects_invalid_age_at_vital_status_filter(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects returns empty result when invalid age_at_vital_status filter is provided."""
        filters = {"_invalid_age_at_vital_status": True}
        
        result = await list_subjects(
            request=mock_request,
            response=mock_response,
            filters=filters,
            pagination=mock_pagination,
            session=mock_session,
            settings=mock_settings,
            allowlist=mock_allowlist,
            _rate_limit=None
        )
        
        assert isinstance(result, SubjectResponse)
        assert len(result.data) == 0
        assert result.summary["counts"]["all"] == 0

    async def test_list_subjects_unknown_parameters(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects raises InvalidParametersError when unknown parameters are detected."""
        filters = {"_unknown_parameters": ["invalid_param"]}
        
        with pytest.raises(HTTPException) as exc_info:
            await list_subjects(
                request=mock_request,
                response=mock_response,
                filters=filters,
                pagination=mock_pagination,
                session=mock_session,
                settings=mock_settings,
                allowlist=mock_allowlist,
                _rate_limit=None
            )
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_list_subjects_http_exception_re_raise(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects re-raises HTTPException."""
        from app.services.subject import SubjectService
        
        http_exception = HTTPException(status_code=400, detail="Test error")
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=http_exception)
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
                    
                    assert exc_info.value.status_code == 400
                    assert exc_info.value.detail == "Test error"

    async def test_list_subjects_generic_connection_error(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects handles generic connection errors in exception handler."""
        from app.services.subject import SubjectService
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=Exception("Database connection timeout"))
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

    async def test_list_subjects_generic_non_connection_error(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects handles generic non-connection errors."""
        from app.services.subject import SubjectService
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=ValueError("Invalid data"))
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

    async def test_list_subjects_error_with_to_http_exception(self, mock_request, mock_session, mock_settings, mock_allowlist, mock_response, mock_pagination):
        """Test list_subjects handles errors with to_http_exception method."""
        from app.services.subject import SubjectService
        from fastapi import HTTPException as FastAPIHTTPException
        
        class CustomError(Exception):
            def to_http_exception(self):
                return FastAPIHTTPException(status_code=422, detail="Custom validation error")
        
        with patch('app.api.v1.endpoints.subjects.SubjectService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_subjects = AsyncMock(side_effect=CustomError("Custom error"))
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
                    
                    assert exc_info.value.status_code == 422
                    assert exc_info.value.detail == "Custom validation error"


@pytest.mark.unit
class TestPaginationUtils:
    """Test cases for pagination utilities."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.url = Mock()
        request.url.path = "/api/v1/subject"
        request.url.scheme = "http"
        request.url.netloc = "localhost:8000"
        request.query_params = {"page": "1", "per_page": "20"}
        return request

    def test_pagination_params_validation(self):
        """Test PaginationParams validation."""
        from app.core.pagination import PaginationParams
        
        # Pydantic BaseModel doesn't use __post_init__, so validation may not work as expected
        # Test the basic functionality
        params = PaginationParams(page=1, per_page=20)
        assert params.page == 1
        assert params.per_page == 20
        assert params.offset == 0
        assert params.limit == 20

    def test_pagination_params_offset_calculation(self):
        """Test PaginationParams offset calculation."""
        from app.core.pagination import PaginationParams
        
        params = PaginationParams(page=3, per_page=20)
        assert params.offset == 40  # (3-1) * 20

    def test_pagination_params_limit(self):
        """Test PaginationParams limit property."""
        from app.core.pagination import PaginationParams
        
        params = PaginationParams(page=1, per_page=50)
        assert params.limit == 50

    def test_calculate_pagination_info(self):
        """Test calculate_pagination_info function."""
        from app.core.pagination import calculate_pagination_info
        
        result = calculate_pagination_info(page=1, per_page=20, total_items=100)
        
        assert result.page == 1
        assert result.per_page == 20
        assert result.total_items == 100
        assert result.total_pages == 5
        assert result.has_next is True
        assert result.has_prev is False

    def test_calculate_pagination_info_last_page(self):
        """Test calculate_pagination_info on last page."""
        from app.core.pagination import calculate_pagination_info
        
        result = calculate_pagination_info(page=5, per_page=20, total_items=100)
        
        assert result.page == 5
        assert result.has_next is False
        assert result.has_prev is True

    def test_calculate_pagination_info_empty(self):
        """Test calculate_pagination_info with no items."""
        from app.core.pagination import calculate_pagination_info
        
        result = calculate_pagination_info(page=1, per_page=20, total_items=0)
        
        assert result.total_pages == 0
        assert result.has_next is False
        assert result.has_prev is False

    def test_build_link_header(self, mock_request):
        """Test build_link_header function."""
        from app.core.pagination import build_link_header, PaginationInfo
        
        pagination = PaginationInfo(
            page=2,
            per_page=20,
            total_pages=5,
            total_items=100,
            has_next=True,
            has_prev=True
        )
        
        link_header = build_link_header(mock_request, pagination)
        
        assert 'rel="first"' in link_header
        assert 'rel="last"' in link_header
        assert 'rel="prev"' in link_header
        assert 'rel="next"' in link_header

    def test_build_link_header_single_page(self, mock_request):
        """Test build_link_header with single page."""
        from app.core.pagination import build_link_header, PaginationInfo
        
        pagination = PaginationInfo(
            page=1,
            per_page=20,
            total_pages=1,
            total_items=10,
            has_next=False,
            has_prev=False
        )
        
        link_header = build_link_header(mock_request, pagination)
        
        assert 'rel="first"' in link_header
        assert 'rel="last"' in link_header
        assert 'rel="prev"' not in link_header
        assert 'rel="next"' not in link_header

    def test_parse_pagination_params(self):
        """Test parse_pagination_params function."""
        from app.core.pagination import parse_pagination_params
        
        with patch('app.core.pagination.get_settings') as mock_get_settings:
            mock_settings = Mock()
            mock_settings.default_page_size = 20
            mock_settings.max_page_size = 1000
            mock_get_settings.return_value = mock_settings
            
            result = parse_pagination_params(page=2, per_page=30)
            
            assert result.page == 2
            assert result.per_page == 30

    def test_parse_pagination_params_defaults(self):
        """Test parse_pagination_params with defaults."""
        from app.core.pagination import parse_pagination_params
        
        with patch('app.core.pagination.get_settings') as mock_get_settings:
            mock_settings = Mock()
            mock_settings.default_page_size = 20
            mock_settings.max_page_size = 1000
            mock_get_settings.return_value = mock_settings
            
            result = parse_pagination_params()
            
            assert result.page == 1
            assert result.per_page == 20

    def test_parse_pagination_params_invalid_page(self):
        """Test parse_pagination_params with invalid page."""
        from app.core.pagination import parse_pagination_params
        
        with patch('app.core.pagination.get_settings') as mock_get_settings:
            mock_settings = Mock()
            mock_settings.default_page_size = 20
            mock_settings.max_page_size = 1000
            mock_get_settings.return_value = mock_settings
            
            with pytest.raises(ValueError, match="Page must be >= 1"):
                parse_pagination_params(page=0, per_page=20)

    def test_parse_pagination_params_invalid_per_page(self):
        """Test parse_pagination_params with invalid per_page."""
        from app.core.pagination import parse_pagination_params
        
        with patch('app.core.pagination.get_settings') as mock_get_settings:
            mock_settings = Mock()
            mock_settings.default_page_size = 20
            mock_settings.max_page_size = 1000
            mock_get_settings.return_value = mock_settings
            
            with pytest.raises(ValueError, match="per_page must be >= 1"):
                parse_pagination_params(page=1, per_page=0)
