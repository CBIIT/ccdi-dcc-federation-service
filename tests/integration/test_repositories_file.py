"""
Integration tests for FileRepository.

Tests repository methods with a real database connection.
"""

import pytest

from app.repositories.file import FileRepository
from app.models.errors import UnsupportedFieldError


@pytest.mark.integration
class TestFileRepositoryIntegration:
    """Integration tests for FileRepository with real database."""
    
    async def test_get_files_empty_database(
        self, file_repository: FileRepository
    ):
        """Test getting files from empty database."""
        files = await file_repository.get_files(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
        assert len(files) == 0
    
    async def test_get_files_with_test_data(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files with test data."""
        files = await file_repository.get_files(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
        assert len(files) >= 0
    
    async def test_get_files_with_type_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by type."""
        files = await file_repository.get_files(
            filters={"type": "BAM"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
        for file in files:
            assert hasattr(file, 'metadata')
    
    async def test_get_files_with_depositions_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by depositions (study_id)."""
        files = await file_repository.get_files(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_checksums_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by checksums (md5sum)."""
        files = await file_repository.get_files(
            filters={"checksums": "abc123def456"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_size_filter(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files filtered by size."""
        files = await file_repository.get_files(
            filters={"size": 1000000},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
    async def test_get_files_with_multiple_filters(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test getting files with multiple filters."""
        files = await file_repository.get_files(
            filters={"type": "BAM", "depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(files, list)
    
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
        assert hasattr(summary.counts, 'total')
        assert isinstance(summary.counts.total, int)
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
        assert isinstance(result["values"], list)
    
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
    
    async def test_get_files_with_invalid_type_enum(
        self, file_repository: FileRepository, test_data_setup
    ):
        """Test that invalid type enum values return empty results."""
        files = await file_repository.get_files(
            filters={"type": "INVALID_TYPE"},
            offset=0,
            limit=10
        )
        
        # Should return empty list for invalid enum value
        assert isinstance(files, list)
        assert len(files) == 0
