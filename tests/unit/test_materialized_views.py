"""
Unit tests for MaterializedViewService.

Tests materialized view operations including getting counts,
refreshing views, and view age calculations.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
from neo4j import AsyncSession

from app.services.materialized_views import MaterializedViewService


@pytest.mark.unit
class TestMaterializedViewService:
    """Test cases for MaterializedViewService class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create a MaterializedViewService instance."""
        return MaterializedViewService(mock_session)

    def test_initialization(self, service, mock_session):
        """Test service initialization."""
        assert service.session is mock_session

    async def test_get_file_count_by_type_success(self, service, mock_session):
        """Test get_file_count_by_type returns counts from materialized view."""
        # Mock database result
        mock_record = {
            "total": 1000,
            "missing": 50,
            "last_updated": 1234567890,
            "values": [
                {"value": "FASTQ", "count": 500},
                {"value": "BAM", "count": 300},
                {"value": "VCF", "count": 150}
            ]
        }
        
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_file_count_by_type(filters=None)
        
        assert result is not None
        assert result["total"] == 1000
        assert result["missing"] == 50
        assert len(result["values"]) == 3
        # Values should be sorted by count DESC, value ASC
        assert result["values"][0]["value"] == "FASTQ"
        assert result["values"][0]["count"] == 500
        assert result["values"][1]["value"] == "BAM"
        assert result["values"][2]["value"] == "VCF"

    async def test_get_file_count_by_type_no_view(self, service, mock_session):
        """Test get_file_count_by_type returns None when view doesn't exist."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_file_count_by_type(filters=None)
        
        assert result is None

    async def test_get_file_count_by_type_with_filters(self, service):
        """Test get_file_count_by_type returns None when filters are provided."""
        result = await service.get_file_count_by_type(filters={"type": "FASTQ"})
        
        assert result is None  # Should signal to use live query

    async def test_get_file_count_by_depositions_success(self, service, mock_session):
        """Test get_file_count_by_depositions returns counts from materialized view."""
        mock_record = {
            "total": 2000,
            "missing": 100,
            "last_updated": 1234567890,
            "values": [
                {"value": "phs002431", "count": 1000},
                {"value": "phs002432", "count": 900}
            ]
        }
        
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_file_count_by_depositions(filters=None)
        
        assert result is not None
        assert result["total"] == 2000
        assert result["missing"] == 100
        assert len(result["values"]) == 2
        # Values should be sorted by count DESC
        assert result["values"][0]["value"] == "phs002431"
        assert result["values"][0]["count"] == 1000

    async def test_get_file_count_by_depositions_no_view(self, service, mock_session):
        """Test get_file_count_by_depositions returns None when view doesn't exist."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.get_file_count_by_depositions(filters=None)
        
        assert result is None

    async def test_get_file_count_by_depositions_with_filters(self, service):
        """Test get_file_count_by_depositions returns None when filters are provided."""
        result = await service.get_file_count_by_depositions(filters={"depositions": "phs002431"})
        
        assert result is None  # Should signal to use live query

    async def test_get_view_age_success(self, service, mock_session):
        """Test get_view_age returns age in seconds."""
        mock_record = {"age_seconds": 3600}  # 1 hour
        
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        age = await service.get_view_age("by_type")
        
        assert age == 3600
        # Verify query was called with correct parameters
        call_args = mock_session.run.call_args
        # Parameters can be passed as second positional arg or as keyword arg
        if len(call_args[0]) > 1:
            params = call_args[0][1] if isinstance(call_args[0][1], dict) else {}
        elif call_args[1] and "parameters" in call_args[1]:
            params = call_args[1]["parameters"]
        else:
            params = call_args[1] if call_args[1] else {}
        assert params.get("view_type") == "by_type"

    async def test_get_view_age_not_found(self, service, mock_session):
        """Test get_view_age returns None when view doesn't exist."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        age = await service.get_view_age("by_type")
        
        assert age is None

    @patch('app.repositories.file.FileRepository')
    @patch('app.lib.field_allowlist.get_field_allowlist')
    async def test_refresh_file_count_by_type_success(
        self, mock_get_allowlist, mock_file_repo_class, service, mock_session
    ):
        """Test refresh_file_count_by_type refreshes the materialized view."""
        # Mock allowlist
        mock_allowlist = Mock()
        mock_get_allowlist.return_value = mock_allowlist
        
        # Mock repository
        mock_repo = Mock()
        mock_repo.count_files_by_field = AsyncMock(return_value={
            "total": 1000,
            "missing": 50,
            "values": [
                {"value": "FASTQ", "count": 500},
                {"value": "BAM", "count": 300}
            ]
        })
        mock_file_repo_class.return_value = mock_repo
        
        # Mock database operations
        mock_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.refresh_file_count_by_type()

        assert result["type"] == "by_type"
        assert result["total"] == 2000   # 1000 × 2 registry entries
        assert result["missing"] == 100  # 50 × 2 registry entries
        assert result["values_count"] == 2
        assert "last_updated" in result
        # Verify delete, create stats, and create counts were called
        assert mock_session.run.call_count >= 3

    @patch('app.repositories.file.FileRepository')
    @patch('app.lib.field_allowlist.get_field_allowlist')
    async def test_refresh_file_count_by_depositions_success(
        self, mock_get_allowlist, mock_file_repo_class, service, mock_session
    ):
        """Test refresh_file_count_by_depositions refreshes the materialized view."""
        # Mock allowlist
        mock_allowlist = Mock()
        mock_get_allowlist.return_value = mock_allowlist
        
        # Mock repository
        mock_repo = Mock()
        mock_repo.count_files_by_field = AsyncMock(return_value={
            "total": 2000,
            "missing": 100,
            "values": [
                {"value": "phs002431", "count": 1000}
            ]
        })
        mock_file_repo_class.return_value = mock_repo
        
        # Mock database operations
        mock_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.refresh_file_count_by_depositions()

        assert result["type"] == "by_depositions"
        assert result["total"] == 4000   # 2000 × 2 registry entries
        assert result["missing"] == 200  # 100 × 2 registry entries
        assert result["values_count"] == 1
        assert "last_updated" in result

    @patch('app.repositories.file.FileRepository')
    @patch('app.lib.field_allowlist.get_field_allowlist')
    async def test_refresh_file_count_by_type_empty_values(
        self, mock_get_allowlist, mock_file_repo_class, service, mock_session
    ):
        """Test refresh_file_count_by_type handles empty values."""
        # Mock allowlist
        mock_allowlist = Mock()
        mock_get_allowlist.return_value = mock_allowlist
        
        # Mock repository with empty values
        mock_repo = Mock()
        mock_repo.count_files_by_field = AsyncMock(return_value={
            "total": 0,
            "missing": 0,
            "values": []
        })
        mock_file_repo_class.return_value = mock_repo
        
        # Mock database operations
        mock_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.refresh_file_count_by_type()
        
        assert result["total"] == 0
        assert result["values_count"] == 0
        # Should not create count nodes when values are empty
        # But should still create stats node
        assert mock_session.run.call_count >= 2

    @patch('app.repositories.file.FileRepository')
    @patch('app.lib.field_allowlist.get_field_allowlist')
    async def test_refresh_all_success(
        self, mock_get_allowlist, mock_file_repo_class, service, mock_session
    ):
        """Test refresh_all refreshes all materialized views."""
        # Mock allowlist
        mock_allowlist = Mock()
        mock_get_allowlist.return_value = mock_allowlist
        
        # Mock repository
        mock_repo = Mock()
        mock_repo.count_files_by_field = AsyncMock(return_value={
            "total": 1000,
            "missing": 50,
            "values": [{"value": "FASTQ", "count": 500}]
        })
        mock_file_repo_class.return_value = mock_repo
        
        # Mock database operations
        mock_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.refresh_all()
        
        assert "by_type" in result
        assert "by_depositions" in result
        assert result["by_type"]["type"] == "by_type"
        assert result["by_depositions"]["type"] == "by_depositions"

    @patch('app.repositories.file.FileRepository')
    @patch('app.lib.field_allowlist.get_field_allowlist')
    async def test_refresh_all_with_error(
        self, mock_get_allowlist, mock_file_repo_class, service, mock_session
    ):
        """Test refresh_all handles errors gracefully."""
        # Mock allowlist
        mock_allowlist = Mock()
        mock_get_allowlist.return_value = mock_allowlist
        
        # Mock repository to raise error for by_type
        mock_repo = Mock()
        deps_result = {"total": 2000, "missing": 100, "values": [{"value": "phs002431", "count": 1000}]}
        mock_repo.count_files_by_field = AsyncMock(side_effect=[
            Exception("Database error"),  # by_type, first registry entry → whole by_type fails
            deps_result,                  # by_depositions, first registry entry
            deps_result,                  # by_depositions, second registry entry
        ])
        mock_file_repo_class.return_value = mock_repo
        
        # Mock database operations
        mock_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await service.refresh_all()
        
        assert "by_type" in result
        assert "by_depositions" in result
        assert "error" in result["by_type"]
        assert result["by_depositions"]["type"] == "by_depositions"  # Should succeed

