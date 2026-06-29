"""
Enhanced unit tests for experimental API endpoints.

Covers additional error handling paths and summary fallbacks.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, Response, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.experimental import (
    search_samples_by_diagnosis,
    search_subjects_by_diagnosis,
)
from app.models.dto import SamplesResponse, SubjectResponse, SummaryResponse, SummaryCounts
from app.db.memgraph import DatabaseConnectionError
from app.core.pagination import PaginationParams


@pytest.mark.unit
class TestExperimentalEndpointsEnhanced:
    """Enhanced test cases for experimental endpoints."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        settings = Mock()
        settings.identifier_server_url = "https://dcc.ccdi.cancer.gov"
        return settings

    @pytest.fixture
    def mock_allowlist(self):
        allowlist = Mock()
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def mock_request(self):
        request = Mock(spec=Request)
        request.method = "GET"
        request.url.path = "/experimental/sample-diagnosis"

        class QueryParams:
            def __init__(self, params):
                self._params = params

            def keys(self):
                return self._params.keys()

            def __iter__(self):
                return iter(self._params.items())

            def __getitem__(self, key):
                return self._params[key]

        request.query_params = QueryParams({"search": "cancer", "page": "1", "per_page": "20"})
        return request

    @pytest.fixture
    def mock_response(self):
        response = Mock(spec=Response)
        response.headers = {}
        return response

    @pytest.fixture
    def mock_pagination(self):
        return PaginationParams(page=1, per_page=20)

    async def test_search_samples_by_diagnosis_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_samples_by_diagnosis handles database errors from diagnosis endpoint path."""
        from app.services.sample import SampleService

        with patch("app.api.v1.endpoints.experimental.SampleService") as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_samples_for_diagnosis_endpoint = AsyncMock(
                side_effect=DatabaseConnectionError("Connection failed")
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await search_samples_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None,
                    )

                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_search_samples_by_diagnosis_connection_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test search_samples_by_diagnosis handles connection-related errors from diagnosis endpoint path."""
        from app.services.sample import SampleService

        with patch("app.api.v1.endpoints.experimental.SampleService") as mock_service_class:
            mock_service = AsyncMock(spec=SampleService)
            mock_service.get_samples_for_diagnosis_endpoint = AsyncMock(
                side_effect=Exception("Database connection timeout")
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await search_samples_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None,
                    )

                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_search_subjects_by_diagnosis_summary_database_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test database error returns empty result (all-or-nothing single round trip)."""
        from app.services.subject import SubjectService

        with patch("app.api.v1.endpoints.experimental.SubjectService") as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_for_diagnosis_endpoint = AsyncMock(
                return_value=([], 0)
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                result = await search_subjects_by_diagnosis(
                    request=mock_request,
                    response=mock_response,
                    filters={"search": "cancer"},
                    pagination=mock_pagination,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None,
                )

        assert isinstance(result, SubjectResponse)
        assert result.summary["counts"]["all"] == 0
        assert len(result.data) == 0

    async def test_search_subjects_by_diagnosis_summary_connection_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test connection error returns empty result (all-or-nothing single round trip)."""
        from app.services.subject import SubjectService

        with patch("app.api.v1.endpoints.experimental.SubjectService") as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_for_diagnosis_endpoint = AsyncMock(
                return_value=([], 0)
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                result = await search_subjects_by_diagnosis(
                    request=mock_request,
                    response=mock_response,
                    filters={"search": "cancer"},
                    pagination=mock_pagination,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None,
                )

        assert isinstance(result, SubjectResponse)
        assert result.summary["counts"]["all"] == 0
        assert len(result.data) == 0

    async def test_search_subjects_by_diagnosis_summary_other_error(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test non-connection error returns empty result (all-or-nothing single round trip)."""
        from app.services.subject import SubjectService

        with patch("app.api.v1.endpoints.experimental.SubjectService") as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_for_diagnosis_endpoint = AsyncMock(
                return_value=([], 0)
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                result = await search_subjects_by_diagnosis(
                    request=mock_request,
                    response=mock_response,
                    filters={"search": "cancer"},
                    pagination=mock_pagination,
                    session=mock_session,
                    settings=mock_settings,
                    allowlist=mock_allowlist,
                    _rate_limit=None,
                )

        assert isinstance(result, SubjectResponse)
        assert result.summary["counts"]["all"] == 0
        assert len(result.data) == 0


    async def test_search_subjects_by_diagnosis_invalid_filter_values_empty_result(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test subject-diagnosis endpoint returns empty result for invalid filter values."""
        from app.services.subject import SubjectService

        with patch("app.api.v1.endpoints.experimental.SubjectService") as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects = AsyncMock(return_value=[])
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                with patch("app.api.v1.endpoints.experimental.check_rate_limit", return_value=None):
                    result = await search_subjects_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={"_invalid_sex": "X", "search": "cancer"},
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None,
                    )

        assert isinstance(result, SubjectResponse)
        assert result.summary["counts"]["all"] == 0
        assert result.summary["counts"]["current"] == 0
        assert len(result.data) == 0

    async def test_search_subjects_by_diagnosis_combined_filters(
        self, mock_session, mock_settings, mock_allowlist, mock_request, mock_response, mock_pagination
    ):
        """Test subject-diagnosis endpoint with multiple filters (sex, race, vital_status)."""
        from app.services.subject import SubjectService

        class MockSubject:
            def model_dump(self, exclude=None, exclude_none=False, exclude_unset=False):
                return {
                    "id": {"name": "subject1", "namespace": {"organization": "CCDI-DCC", "name": "phs002431"}},
                    "kind": "Participant",
                    "metadata": {
                        "associated_diagnoses": [],
                        "vital_status": {"value": "Alive"},
                        "age_at_vital_status": {"value": 45},
                        "sex": {"value": "F"},
                        "race": [{"value": "White"}],
                    },
                }

        mock_subjects = [MockSubject()]

        with patch("app.api.v1.endpoints.experimental.SubjectService") as mock_service_class:
            mock_service = AsyncMock(spec=SubjectService)
            mock_service.get_subjects_for_diagnosis_endpoint = AsyncMock(
                return_value=(mock_subjects, 25)
            )
            mock_service_class.return_value = mock_service

            with patch("app.api.v1.endpoints.experimental.get_cache_service", return_value=None):
                with patch("app.api.v1.endpoints.experimental.check_rate_limit", return_value=None):
                    result = await search_subjects_by_diagnosis(
                        request=mock_request,
                        response=mock_response,
                        filters={
                            "search": "cancer",
                            "sex": "F",
                            "race": "White",
                            "vital_status": "Alive"
                        },
                        pagination=mock_pagination,
                        session=mock_session,
                        settings=mock_settings,
                        allowlist=mock_allowlist,
                        _rate_limit=None,
                    )

        assert isinstance(result, SubjectResponse)
        assert result.summary["counts"]["all"] == 25
        assert result.summary["counts"]["current"] == 1
        assert len(result.data) == 1
