"""
Unit tests for namespace API endpoints.

Tests namespace listing and detail retrieval endpoints.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, HTTPException, status
from neo4j import AsyncSession

from app.api.v1.endpoints.namespaces import (
    list_namespaces,
    get_namespace,
    NamespaceService,
    router as namespaces_router
)
from app.models.dto import Namespace, NamespaceIdentifier
from app.models.errors import ErrorKind, InvalidParametersError


@pytest.mark.unit
class TestNamespaceService:
    """Test cases for NamespaceService class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        return settings

    @pytest.fixture
    def service(self, mock_session, mock_settings):
        """Create a NamespaceService instance."""
        return NamespaceService(mock_session, mock_settings)

    def test_initialization(self, service, mock_session, mock_settings):
        """Test service initialization."""
        assert service.session is mock_session
        assert service.settings is mock_settings

    async def test_get_namespaces_success(self, service, mock_session):
        """Test get_namespaces returns list of namespaces."""
        # Mock database result
        async def async_gen():
            yield {
                "study_id": "phs002431",
                "study_description": "Test Study",
                "study_acronym": "TS",
                "study_name": "Test Study Name",
                "study_dd": "phs002431",
                "grant_ids": ["R01CA123456"]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_namespaces()
        
        assert isinstance(result, list)
        assert len(result) > 0
        assert isinstance(result[0], Namespace)
        assert result[0].id.name == "phs002431"
        assert result[0].id.organization == "CCDI-DCC"

    async def test_get_namespaces_empty(self, service, mock_session):
        """Test get_namespaces returns empty list when no namespaces exist."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_namespaces()
        
        assert isinstance(result, list)
        assert len(result) == 0

    async def test_get_namespace_detail_success(self, service, mock_session):
        """Test get_namespace_detail returns namespace details."""
        async def async_gen():
            yield {
                "study_id": "phs002431",
                "study_description": "Test Study",
                "study_acronym": "TS",
                "study_name": "Test Study Name",
                "study_dd": "phs002431",
                "grant_ids": ["R01CA123456"]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_namespace_detail("CCDI-DCC", "phs002431")
        
        assert result is not None
        assert isinstance(result, Namespace)
        assert result.id.name == "phs002431"
        assert result.id.organization == "CCDI-DCC"
        assert result.description == "Test Study"

    async def test_get_namespace_detail_not_found(self, service, mock_session):
        """Test get_namespace_detail returns None when namespace not found."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_namespace_detail("CCDI-DCC", "nonexistent")
        
        assert result is None

    async def test_get_namespaces_retry_logic(self, service, mock_session):
        """Test get_namespaces retries on transient errors."""
        # First call fails, second succeeds
        async def async_gen_success():
            yield {
                "study_id": "phs002431",
                "study_description": "Test",
                "study_acronym": "",
                "study_name": "",
                "study_dd": "phs002431",
                "grant_ids": []
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen_success())
        mock_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(
            side_effect=[
                Exception("Connection timeout"),
                mock_result
            ]
        )
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            result = await service.get_namespaces()
        
        assert isinstance(result, list)
        # Should have retried
        assert mock_session.run.call_count == 2


@pytest.mark.unit
class TestNamespaceEndpoints:
    """Test cases for namespace API endpoints."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        return settings

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.url.path = "/namespace"
        return request

    async def test_list_namespaces_success(self, mock_session, mock_settings, mock_request):
        """Test list_namespaces endpoint returns namespaces."""
        # Mock NamespaceService
        mock_namespaces = [
            Namespace(
                id=NamespaceIdentifier(organization="CCDI-DCC", name="phs002431"),
                description="Test Study",
                contact_email="test@example.com",
                metadata=None
            )
        ]
        
        with patch('app.api.v1.endpoints.namespaces.NamespaceService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_namespaces = AsyncMock(return_value=mock_namespaces)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.namespaces.check_rate_limit', return_value=None):
                result = await list_namespaces(
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].id.name == "phs002431"

    async def test_list_namespaces_error_handling(self, mock_session, mock_settings, mock_request):
        """Test list_namespaces handles errors gracefully."""
        with patch('app.api.v1.endpoints.namespaces.NamespaceService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_namespaces = AsyncMock(side_effect=Exception("Database error"))
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.namespaces.check_rate_limit', return_value=None):
                with pytest.raises(HTTPException) as exc_info:
                    await list_namespaces(
                        request=mock_request,
                        session=mock_session,
                        settings=mock_settings,
                        _rate_limit=None
                    )
                
                assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    async def test_get_namespace_success(self, mock_session, mock_settings, mock_request):
        """Test get_namespace endpoint returns namespace details."""
        mock_namespace = Namespace(
            id=NamespaceIdentifier(organization="CCDI-DCC", name="phs002431"),
            description="Test Study",
            contact_email="test@example.com",
            metadata=None
        )
        
        with patch('app.api.v1.endpoints.namespaces.NamespaceService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_namespace_detail = AsyncMock(return_value=mock_namespace)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.namespaces.check_rate_limit', return_value=None):
                result = await get_namespace(
                    organization="CCDI-DCC",
                    namespace="phs002431",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert isinstance(result, Namespace)
        assert result.id.name == "phs002431"

    async def test_get_namespace_invalid_organization(self, mock_session, mock_settings, mock_request):
        """Test get_namespace rejects invalid organization."""
        with patch('app.api.v1.endpoints.namespaces.check_rate_limit', return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await get_namespace(
                    organization="INVALID",
                    namespace="phs002431",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
            
            # Should raise HTTPException with 400 status (InvalidParameters)
            assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    async def test_get_namespace_not_found(self, mock_session, mock_settings, mock_request):
        """Test get_namespace returns namespace with null metadata when not found."""
        with patch('app.api.v1.endpoints.namespaces.NamespaceService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_namespace_detail = AsyncMock(return_value=None)
            mock_service_class.return_value = mock_service
            
            with patch('app.api.v1.endpoints.namespaces.check_rate_limit', return_value=None):
                result = await get_namespace(
                    organization="CCDI-DCC",
                    namespace="nonexistent",
                    request=mock_request,
                    session=mock_session,
                    settings=mock_settings,
                    _rate_limit=None
                )
        
        assert isinstance(result, Namespace)
        assert result.metadata is None  # Should be null when not found
        assert result.id.name == "nonexistent"

