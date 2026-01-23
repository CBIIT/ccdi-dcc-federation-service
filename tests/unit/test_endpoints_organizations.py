"""
Unit tests for organizations API endpoints.

Covers list and detail endpoints, including retry behavior and error handling.
"""

import json
import pytest
from unittest.mock import AsyncMock, Mock, patch, mock_open
from fastapi import Request, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.organizations import (
    get_organizations,
    get_organization_by_name,
)
from app.models.dto import Organization
from app.models.errors import ErrorKind


@pytest.mark.unit
class TestOrganizationsEndpoints:
    """Test cases for organization endpoints."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        return Mock()

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.url.path = "/api/v1/organization"
        return request

    def _mock_result(self, records):
        """Create a mock async result that yields records."""
        async def async_gen():
            for record in records:
                yield record

        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        return mock_result

    async def test_get_organizations_success(self, mock_request, mock_session, mock_settings):
        """Test get_organizations returns organizations with institutions."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}},
                {"identifier": "TEST", "name": "Test Org"}
            ]
        }
        mock_session.run = AsyncMock(return_value=self._mock_result([
            {"institution": "Hospital A"},
            {"institution": "Hospital B"}
        ]))

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
            result = await get_organizations(
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                _rate_limit=None
            )

        assert isinstance(result, list)
        assert isinstance(result[0], Organization)
        assert result[0].metadata["institution"][0]["value"] == "Hospital A"

    async def test_get_organizations_retry_on_empty_then_success(self, mock_request, mock_session, mock_settings):
        """Test get_organizations retries when no records are returned."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}}
            ]
        }
        empty_result = self._mock_result([])
        second_result = self._mock_result([{"institution": "Hospital A"}])
        mock_session.run = AsyncMock(side_effect=[empty_result, second_result])

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.asyncio.sleep", new=AsyncMock()):
            with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
                result = await get_organizations(
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )

        assert mock_session.run.call_count == 2
        assert result[0].metadata["institution"][0]["value"] == "Hospital A"

    async def test_get_organizations_retry_on_exception_then_success(self, mock_request, mock_session, mock_settings):
        """Test get_organizations retries after an exception."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}}
            ]
        }
        second_result = self._mock_result([{"institution": "Hospital A"}])
        mock_session.run = AsyncMock(side_effect=[Exception("boom"), second_result])

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.asyncio.sleep", new=AsyncMock()):
            with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
                result = await get_organizations(
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )

        assert mock_session.run.call_count == 2
        assert result[0].metadata["institution"][0]["value"] == "Hospital A"

    async def test_get_organizations_file_not_found(self, mock_request, mock_session, mock_settings):
        """Test get_organizations handles missing data file."""
        mock_path = Mock()
        mock_path.open = Mock(side_effect=FileNotFoundError)
        with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
            with pytest.raises(HTTPException) as exc_info:
                await get_organizations(
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )

        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert exc_info.value.detail["errors"][0]["kind"] == ErrorKind.NOT_FOUND

    async def test_get_organizations_invalid_json(self, mock_request, mock_session, mock_settings):
        """Test get_organizations handles invalid JSON."""
        mock_path = Mock()
        mock_path.open = mock_open(read_data="invalid")
        with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
            with patch("app.api.v1.endpoints.organizations.json.load", side_effect=json.JSONDecodeError("msg", "doc", 0)):
                with pytest.raises(HTTPException) as exc_info:
                    await get_organizations(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        _rate_limit=None
                    )

        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert exc_info.value.detail["errors"][0]["kind"] == ErrorKind.NOT_FOUND

    async def test_get_organization_by_name_success(self, mock_request, mock_session, mock_settings):
        """Test get_organization_by_name returns matching organization."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}}
            ]
        }
        mock_session.run = AsyncMock(return_value=self._mock_result([
            {"institution": "Hospital A"}
        ]))

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
            result = await get_organization_by_name(
                name="ccdi-dcc",
                request=mock_request,
                session=mock_session,
                settings=mock_settings,
                _rate_limit=None
            )

        assert isinstance(result, Organization)
        assert result.identifier == "CCDI-DCC"
        assert result.metadata["institution"][0]["value"] == "Hospital A"

    async def test_get_organization_by_name_not_found(self, mock_request, mock_session, mock_settings):
        """Test get_organization_by_name returns 404 when not found."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}}
            ]
        }

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
            with pytest.raises(HTTPException) as exc_info:
                await get_organization_by_name(
                    name="missing",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )

        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert exc_info.value.detail["errors"][0]["kind"] == ErrorKind.NOT_FOUND
        mock_session.run.assert_not_called()

    async def test_get_organization_by_name_retry_on_exception_then_success(self, mock_request, mock_session, mock_settings):
        """Test get_organization_by_name retries after exception."""
        data = {
            "organizations": [
                {"identifier": "CCDI-DCC", "name": "CCDI Federation Service", "metadata": {}}
            ]
        }
        second_result = self._mock_result([{"institution": "Hospital A"}])
        mock_session.run = AsyncMock(side_effect=[Exception("boom"), second_result])

        mock_path = Mock()
        mock_path.open = mock_open(read_data=json.dumps(data))
        with patch("app.api.v1.endpoints.organizations.asyncio.sleep", new=AsyncMock()):
            with patch("app.api.v1.endpoints.organizations.DATA_PATH", mock_path):
                result = await get_organization_by_name(
                    name="CCDI-DCC",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )

        assert mock_session.run.call_count == 2
        assert result.metadata["institution"][0]["value"] == "Hospital A"

