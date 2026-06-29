"""
Unit tests for API endpoint handlers.

Tests FastAPI route handlers including request/response handling,
error handling, and data transformation.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock, mock_open
from fastapi import Request, HTTPException, status
from fastapi.testclient import TestClient

from app.api.v1.endpoints.root import api_root, router as root_router
from app.api.v1.endpoints.info import api_info, router as info_router
from app.api.v1.endpoints.metadata import (
    get_subject_metadata_fields,
    get_sample_metadata_fields,
    get_file_metadata_fields,
    load_metadata_fields,
    convert_to_response,
    router as metadata_router
)
from app.api.v1.endpoints.organizations import (
    get_organizations,
    get_organization_by_name,
    router as organizations_router
)
from app.models.dto import MetadataFieldsInfoResponse, Organization
from app.models.errors import ErrorKind


@pytest.mark.unit
class TestRootEndpoint:
    """Test cases for root API endpoint."""

    def test_api_root_success(self, tmp_path):
        """Test api_root returns API configuration."""
        # Create mock info.json file
        info_data = {
            "api": {
                "title": "CCDI Federation API",
                "api_version": "1.0.0",
                "description": "Test API",
                "endpoints": {
                    "/subjects": "GET /subjects"
                }
            }
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        with patch('app.api.v1.endpoints.root.DATA_PATH', info_file):
            result = api_root()
            
            assert result["title"] == "CCDI Federation API"
            assert result["version"] == "1.0.0"
            assert result["description"] == "Test API"
            assert "endpoints" in result

    def test_api_root_file_not_found(self):
        """Test api_root handles missing file."""
        with patch('app.api.v1.endpoints.root.DATA_PATH', Path("/nonexistent/file.json")):
            with pytest.raises(HTTPException) as exc_info:
                api_root()
            
            assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
            detail = exc_info.value.detail
            assert isinstance(detail, dict)
            assert "errors" in detail
            # ErrorKind.NOT_FOUND is a string constant
            assert detail["errors"][0]["kind"] == ErrorKind.NOT_FOUND

    def test_api_root_invalid_json(self, tmp_path):
        """Test api_root handles invalid JSON."""
        info_file = tmp_path / "info.json"
        info_file.write_text("invalid json content")
        
        with patch('app.api.v1.endpoints.root.DATA_PATH', info_file):
            with pytest.raises(HTTPException) as exc_info:
                api_root()
            
            assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.unit
class TestInfoEndpoint:
    """Test cases for info API endpoint."""

    def test_api_info_success(self, tmp_path):
        """Test api_info returns filtered API information."""
        info_data = {
            "server": {"name": "Test Server"},
            "api": {
                "api_version": "1.0.0",
                "documentation_url": "https://example.com/docs",
                "title": "Should be filtered out"
            },
            "data": {
                "version": {"version": "1.0", "about_url": "https://example.com"},
                "last_updated": "2024-01-01",
                "wiki_url": "https://example.com/wiki"
            },
            "organizations": []  # Should be filtered out
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        with patch('app.api.v1.endpoints.info.DATA_PATH', info_file):
            result = api_info()
            
            assert result["server"]["name"] == "Test Server"
            assert result["api"]["api_version"] == "1.0.0"
            assert result["api"]["documentation_url"] == "https://example.com/docs"
            assert "title" not in result["api"]  # Should be filtered
            assert "organizations" not in result  # Should be filtered
            assert "data" in result

    def test_api_info_file_not_found(self):
        """Test api_info handles missing file."""
        with patch('app.api.v1.endpoints.info.DATA_PATH', Path("/nonexistent/file.json")):
            with pytest.raises(HTTPException) as exc_info:
                api_info()
            
            assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_api_info_invalid_json(self, tmp_path):
        """Test api_info handles invalid JSON."""
        info_file = tmp_path / "info.json"
        info_file.write_text("invalid json")
        
        with patch('app.api.v1.endpoints.info.DATA_PATH', info_file):
            with pytest.raises(HTTPException) as exc_info:
                api_info()
            
            assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.unit
class TestMetadataEndpoints:
    """Test cases for metadata API endpoints."""

    def test_load_metadata_fields_success(self, tmp_path):
        """Test load_metadata_fields loads JSON successfully."""
        metadata_data = {
            "subjects": {
                "fields": [
                    {
                        "path": "id.name",
                        "harmonized": True,
                        "wiki_url": "https://example.com",
                        "standard": {
                            "name": "CCDI",
                            "url": "https://example.com/ccdi"
                        }
                    }
                ]
            }
        }
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text(json.dumps(metadata_data))
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            result = load_metadata_fields()
            
            assert "subjects" in result
            assert len(result["subjects"]["fields"]) == 1

    def test_load_metadata_fields_file_not_found(self):
        """Test load_metadata_fields handles missing file."""
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', Path("/nonexistent/file.json")):
            with pytest.raises(HTTPException) as exc_info:
                load_metadata_fields()
            
            assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_convert_to_response(self):
        """Test convert_to_response converts JSON to response model."""
        data = {
            "fields": [
                {
                    "path": "id.name",
                    "harmonized": True,
                    "wiki_url": "https://example.com",
                    "standard": {
                        "name": "CCDI",
                        "url": "https://example.com/ccdi"
                    }
                },
                {
                    "path": "sex",
                    "harmonized": False,
                    "wiki_url": "",
                    "standard": {}
                }
            ]
        }
        
        result = convert_to_response(data)
        
        assert isinstance(result, MetadataFieldsInfoResponse)
        assert len(result.fields) == 2
        assert result.fields[0].path == "id.name"
        assert result.fields[0].harmonized is True
        assert result.fields[0].wiki_url == "https://example.com"
        assert result.fields[0].standard.name == "CCDI"
        assert result.fields[1].harmonized is False
        assert result.fields[1].wiki_url is None

    def test_convert_to_response_omitted_wiki_url_is_null(self):
        """Missing wiki_url in JSON becomes null (None), not empty string."""
        data = {
            "fields": [
                {
                    "path": "id.only",
                    "harmonized": True,
                    "standard": {"name": "X", "url": "https://x"},
                }
            ]
        }
        result = convert_to_response(data)
        assert len(result.fields) == 1
        assert result.fields[0].wiki_url is None

    async def test_get_subject_metadata_fields_success(self, tmp_path):
        """Test get_subject_metadata_fields returns subject fields."""
        metadata_data = {
            "subjects": {
                "fields": [
                    {
                        "path": "id.name",
                        "harmonized": True,
                        "wiki_url": "",
                        "standard": {}
                    }
                ]
            }
        }
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text(json.dumps(metadata_data))
        
        mock_request = Mock(spec=Request)
        mock_request.url.path = "/metadata/fields/subject"
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            result = await get_subject_metadata_fields(mock_request)
            
            assert isinstance(result, MetadataFieldsInfoResponse)
            assert len(result.fields) == 1
            assert result.fields[0].path == "id.name"
            assert result.fields[0].wiki_url is None

    async def test_get_subject_metadata_fields_not_found(self, tmp_path):
        """Test get_subject_metadata_fields handles missing type."""
        metadata_data = {
            "samples": {"fields": []}
        }
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text(json.dumps(metadata_data))
        
        mock_request = Mock(spec=Request)
        mock_request.url.path = "/metadata/fields/subject"
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            result = await get_subject_metadata_fields(mock_request)
            
            assert isinstance(result, MetadataFieldsInfoResponse)
            assert len(result.fields) == 0

    async def test_get_sample_metadata_fields_success(self, tmp_path):
        """Test get_sample_metadata_fields returns sample fields."""
        metadata_data = {
            "samples": {
                "fields": [
                    {
                        "path": "id.name",
                        "harmonized": True,
                        "wiki_url": "",
                        "standard": {}
                    }
                ]
            }
        }
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text(json.dumps(metadata_data))
        
        mock_request = Mock(spec=Request)
        mock_request.url.path = "/metadata/fields/sample"
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            result = await get_sample_metadata_fields(mock_request)
            
            assert isinstance(result, MetadataFieldsInfoResponse)
            assert len(result.fields) == 1
            assert result.fields[0].path == "id.name"
            assert result.fields[0].wiki_url is None

    async def test_get_file_metadata_fields_success(self, tmp_path):
        """Test get_file_metadata_fields returns file fields."""
        metadata_data = {
            "file": {
                "fields": [
                    {
                        "path": "id.name",
                        "harmonized": True,
                        "wiki_url": "",
                        "standard": {}
                    }
                ]
            }
        }
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text(json.dumps(metadata_data))
        
        mock_request = Mock(spec=Request)
        mock_request.url.path = "/metadata/fields/file"
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            result = await get_file_metadata_fields(mock_request)
            
            assert isinstance(result, MetadataFieldsInfoResponse)
            assert len(result.fields) == 1
            assert result.fields[0].path == "id.name"
            assert result.fields[0].wiki_url is None

    async def test_get_metadata_fields_error_handling(self, tmp_path):
        """Test metadata endpoints handle errors gracefully."""
        metadata_file = tmp_path / "metadata_fields.json"
        metadata_file.write_text("invalid json")
        
        mock_request = Mock(spec=Request)
        mock_request.url.path = "/metadata/fields/subject"
        
        with patch('app.api.v1.endpoints.metadata.DATA_PATH', metadata_file):
            # Should return empty fields on error
            result = await get_subject_metadata_fields(mock_request)
            assert isinstance(result, MetadataFieldsInfoResponse)
            assert len(result.fields) == 0


@pytest.mark.unit
class TestOrganizationsEndpoints:
    """Test cases for organizations API endpoints."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = AsyncMock()
        return session

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        return settings

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        return request

    async def test_get_organizations_success(self, tmp_path, mock_session, mock_settings, mock_request):
        """Test get_organizations returns organizations with institutions."""
        # Create mock info.json
        info_data = {
            "organizations": [
                {
                    "identifier": "CCDI-DCC",
                    "name": "CCDI Data Coordinating Center"
                }
            ]
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        # Mock database result
        async def async_gen():
            yield {"institution": "Test Institution"}
            yield {"institution": "Another Institution"}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.api.v1.endpoints.organizations.DATA_PATH', info_file):
            with patch('app.api.v1.endpoints.organizations.check_rate_limit', return_value=None):
                result = await get_organizations(
                    mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].identifier == "CCDI-DCC"
        assert "institution" in result[0].metadata
        assert len(result[0].metadata["institution"]) == 2

    async def test_get_organizations_file_not_found(self, mock_session, mock_settings, mock_request):
        """Test get_organizations handles missing file."""
        with patch('app.api.v1.endpoints.organizations.DATA_PATH', Path("/nonexistent/file.json")):
            with patch('app.api.v1.endpoints.organizations.check_rate_limit', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await get_organizations(
                        mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_organization_by_name_success(self, tmp_path, mock_session, mock_settings, mock_request):
        """Test get_organization_by_name returns organization by identifier."""
        info_data = {
            "organizations": [
                {
                    "identifier": "CCDI-DCC",
                    "name": "CCDI Data Coordinating Center"
                }
            ]
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        # Mock database result
        async def async_gen():
            yield {"institution": "Test Institution"}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.api.v1.endpoints.organizations.DATA_PATH', info_file):
            with patch('app.api.v1.endpoints.organizations.check_rate_limit', return_value=None):
                result = await get_organization_by_name(
                    name="CCDI-DCC",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert isinstance(result, Organization)
        assert result.identifier == "CCDI-DCC"
        assert result.name == "CCDI Data Coordinating Center"

    async def test_get_organization_by_name_not_found(self, tmp_path, mock_session, mock_settings, mock_request):
        """Test get_organization_by_name handles not found."""
        info_data = {
            "organizations": [
                {
                    "identifier": "CCDI-DCC",
                    "name": "CCDI Data Coordinating Center"
                }
            ]
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        with patch('app.api.v1.endpoints.organizations.DATA_PATH', info_file):
            with patch('app.api.v1.endpoints.organizations.check_rate_limit', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await get_organization_by_name(
                        name="NONEXISTENT",
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_organization_by_name_case_insensitive(self, tmp_path, mock_session, mock_settings, mock_request):
        """Test get_organization_by_name is case-insensitive."""
        info_data = {
            "organizations": [
                {
                    "identifier": "CCDI-DCC",
                    "name": "CCDI Data Coordinating Center"
                }
            ]
        }
        info_file = tmp_path / "info.json"
        info_file.write_text(json.dumps(info_data))
        
        # Mock database result
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.api.v1.endpoints.organizations.DATA_PATH', info_file):
            with patch('app.api.v1.endpoints.organizations.check_rate_limit', return_value=None):
                result = await get_organization_by_name(
                    name="ccdi-dcc",  # lowercase
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert result.identifier == "CCDI-DCC"

