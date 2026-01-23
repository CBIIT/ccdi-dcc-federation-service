"""
Enhanced unit tests for FastAPI dependencies.

Focuses on edge cases and missing branches in deps.py.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import Request, HTTPException

from app.api.v1.deps import (
    get_database_session,
    get_pagination_params,
    get_sample_filters,
    get_file_filters,
    get_subject_diagnosis_filters,
    get_sample_diagnosis_filters,
)


@pytest.mark.unit
class TestDatabaseSessionDependency:
    """Tests for get_database_session generator."""

    async def test_get_database_session_yields(self):
        mock_session = AsyncMock()

        async def fake_get_session():
            yield mock_session

        with patch("app.api.v1.deps.get_session", side_effect=fake_get_session):
            gen = get_database_session()
            session = await gen.__anext__()
            await gen.aclose()
            assert session is mock_session


@pytest.mark.unit
class TestPaginationParams:
    """Additional tests for pagination params error handling."""

    def test_pagination_params_invalid_raises_http(self):
        with patch("app.api.v1.deps.parse_pagination_params", side_effect=ValueError("bad")):
            with pytest.raises(HTTPException):
                get_pagination_params(page=0, per_page=0)


@pytest.mark.unit
class TestSampleFiltersEnhanced:
    """Additional tests for sample filters."""

    @pytest.fixture
    def mock_request(self):
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.items = Mock(return_value=[])
        return request

    def test_anatomical_sites_singular_rejected(self, mock_request):
        mock_request.query_params.items = Mock(return_value=[("anatomical_site", "value")])
        with pytest.raises(Exception):
            get_sample_filters(anatomical_sites=None, request=mock_request)

    def test_anatomical_sites_list(self, mock_request):
        result = get_sample_filters(anatomical_sites="Lung||Heart", request=mock_request)
        assert isinstance(result["anatomical_sites"], list)
        assert "Lung" in result["anatomical_sites"]


@pytest.mark.unit
class TestFileFiltersEnhanced:
    """Additional tests for file filters and unharmonized mapping."""

    @pytest.fixture
    def mock_request(self):
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.items = Mock(return_value=[("metadata.unharmonized.file_name", "test.fastq")])
        return request

    def test_file_filters_maps_type_alias(self, mock_request):
        result = get_file_filters(type="BAM", request=mock_request)
        assert result["file_type"] == "BAM"

    def test_file_filters_unharmonized(self, mock_request):
        result = get_file_filters(request=mock_request)
        assert result["metadata.unharmonized.file_name"] == "test.fastq"


@pytest.mark.unit
class TestDiagnosisFiltersEnhanced:
    """Additional tests for diagnosis filter helpers."""

    @pytest.fixture
    def mock_request(self):
        request = Mock(spec=Request)
        request.query_params = Mock()
        request.query_params.keys = Mock(return_value=[])
        request.query_params.items = Mock(return_value=[])
        return request

    def test_subject_diagnosis_filters_adds_search(self, mock_request):
        result = get_subject_diagnosis_filters(search="neuro", request=mock_request)
        assert result["_diagnosis_search"] == "neuro"

    def test_sample_diagnosis_filters_adds_search(self, mock_request):
        result = get_sample_diagnosis_filters(search="cancer", request=mock_request)
        assert result["_diagnosis_search"] == "cancer"

