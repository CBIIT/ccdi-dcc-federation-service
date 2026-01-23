"""
Enhanced integration tests for FileRepository.

Tests additional edge cases, error scenarios, and complex queries.
"""

import pytest

from app.repositories.file import FileRepository
from app.models.errors import UnsupportedFieldError


@pytest.mark.integration
class TestFileRepositoryEnhanced:
    """Enhanced integration tests for FileRepository."""
    
    async def test_get_files_with_size_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by size."""
        files = await file_repository.get_files(
            filters={"size": "1000000"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_checksums_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by checksums."""
        files = await file_repository.get_files(
            filters={"checksums": "abc123def456"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_description_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by description."""
        files = await file_repository.get_files(
            filters={"description": "test"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_multiple_filters(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files with multiple filters."""
        files = await file_repository.get_files(
            filters={
                "type": "BAM",
                "depositions": "phs002431"
            },
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_file_by_identifier(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting a file by identifier."""
        file = await file_repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            file_id="FILE-001"
        )
        
        if file:
            assert hasattr(file, 'id')
            assert file.id.name == "FILE-001"
    
    async def test_get_file_by_identifier_not_found(
        self, file_repository: FileRepository
    ):
        """Test getting a non-existent file."""
        file = await file_repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            file_id="NONEXISTENT"
        )
        
        assert file is None
    
    async def test_get_files_summary(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files summary."""
        summary = await file_repository.get_files_summary(filters={})
        
        assert summary is not None
        assert hasattr(summary, 'counts')
        assert summary.counts.total >= 0
    
    async def test_get_files_summary_with_filters(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files summary with filters."""
        summary = await file_repository.get_files_summary(
            filters={"type": "BAM"}
        )
        
        assert summary is not None
        assert summary.counts.total >= 0
    
    async def test_count_files_by_field_type(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test counting files by type field."""
        result = await file_repository.count_files_by_field("type", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_files_by_field_depositions(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test counting files by depositions field."""
        result = await file_repository.count_files_by_field("depositions", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_files_by_field_with_filters(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test counting files by field with additional filters."""
        result = await file_repository.count_files_by_field(
            "type",
            {"depositions": "phs002431"}
        )
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
    
    async def test_get_files_pagination(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test pagination works correctly."""
        page1 = await file_repository.get_files(
            filters={},
            offset=0,
            limit=1
        )
        
        page2 = await file_repository.get_files(
            filters={},
            offset=1,
            limit=1
        )
        
        assert isinstance(page1, list)
        assert isinstance(page2, list)
        assert len(page1) <= 1
        assert len(page2) <= 1
    
    async def test_get_files_with_depositions_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by depositions."""
        files = await file_repository.get_files(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_invalid_field_raises_error(
        self, file_repository: FileRepository
    ):
        """Test that invalid fields raise UnsupportedFieldError."""
        with pytest.raises(UnsupportedFieldError):
            await file_repository.get_files(
                filters={"invalid_field": "value"},
                offset=0,
                limit=10
            )
    
    async def test_get_files_summary_empty_database(
        self, file_repository: FileRepository
    ):
        """Test getting files summary from empty database."""
        summary = await file_repository.get_files_summary(filters={})
        
        assert summary is not None
        assert summary.counts.total == 0
    
    async def test_count_files_by_field_empty_database(
        self, file_repository: FileRepository
    ):
        """Test counting files by field in empty database."""
        result = await file_repository.count_files_by_field("type", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert result["total"] == 0
        assert result["missing"] == 0
        assert isinstance(result["values"], list)

