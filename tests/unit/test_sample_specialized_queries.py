"""
Unit tests for specialized query methods in SampleRepository.

Tests reverse query methods for sequencing_file, pathology_file, and combined filters.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings


@pytest.mark.unit
class TestSpecializedQueries:
    """Test specialized reverse query methods."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock(spec=Settings)
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.sample_count_fields = []
        return settings
    
    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create repository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)
    
    async def test_get_samples_by_sequencing_file_filters_library_source_material(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with library_source_material."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_material": "DNA"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_null_mapped_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="DNA"):
                filters = {"library_source_material": "DNA"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                
                assert mock_session.run.called
                assert isinstance(result, list)
    
    async def test_get_samples_by_sequencing_file_filters_invalid_library_source_material(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters returns empty for invalid library_source_material."""
        with patch('app.repositories.sample.is_null_mapped_value', return_value=True):
            filters = {"library_source_material": "Invalid"}
            result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
            assert result == []
            assert not mock_session.run.called
    
    async def test_get_samples_by_sequencing_file_filters_library_strategy(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with library_strategy."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                
                assert mock_session.run.called
                assert isinstance(result, list)
    
    async def test_get_samples_by_sequencing_file_filters_invalid_library_strategy(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters returns empty for invalid library_strategy."""
        with patch('app.repositories.sample.is_database_only_value', return_value=True):
            filters = {"library_strategy": "Invalid"}
            result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
            assert result == []
            assert not mock_session.run.called
    
    async def test_get_samples_by_sequencing_file_filters_library_selection_method(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with library_selection_method."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_selection": "PCR"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch.object(repository, '_reverse_map_library_selection_method_static', return_value="PCR"):
                filters = {"library_selection_method": "PCR"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                
                assert mock_session.run.called
                assert isinstance(result, list)
    
    async def test_get_samples_by_sequencing_file_filters_invalid_library_selection_method(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters returns empty for invalid library_selection_method."""
        with patch('app.repositories.sample.is_database_only_value', return_value=True):
            filters = {"library_selection_method": "Invalid"}
            result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
            assert result == []
            assert not mock_session.run.called
    
    async def test_get_samples_by_sequencing_file_filters_specimen_molecular_analyte_type(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with specimen_molecular_analyte_type."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_molecule": "DNA"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.is_null_mapped_value', return_value=False):
                with patch('app.repositories.sample.reverse_map_field_value', return_value="DNA"):
                    filters = {"specimen_molecular_analyte_type": "DNA"}
                    result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                    
                    assert mock_session.run.called
                    assert isinstance(result, list)
    
    async def test_get_samples_by_sequencing_file_filters_invalid_specimen_molecular_analyte_type(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters returns empty for invalid specimen_molecular_analyte_type."""
        with patch('app.repositories.sample.is_database_only_value', return_value=True):
            filters = {"specimen_molecular_analyte_type": "Invalid"}
            result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
            assert result == []
            assert not mock_session.run.called
    
    async def test_get_samples_by_sequencing_file_filters_specimen_molecular_analyte_type_list(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with specimen_molecular_analyte_type returning list."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_molecule": "RNA"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.is_null_mapped_value', return_value=False):
                with patch('app.repositories.sample.reverse_map_field_value', return_value=["Transcriptomic", "Viral RNA"]):
                    filters = {"specimen_molecular_analyte_type": "RNA"}
                    result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                    
                    assert mock_session.run.called
                    # Get the query to verify IN clause
                    call_args = mock_session.run.call_args
                    query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
                    assert 'IN [' in query
    
    async def test_get_samples_by_sequencing_file_filters_return_total(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with return_total=True."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        # Mock count query result - need to support async iteration and dict() conversion
        async def count_async_gen():
            yield {"total_count": 50}
        
        mock_count_result = AsyncMock()
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())
        mock_count_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20, return_total=True)
                
                assert mock_session.run.call_count == 2
                assert isinstance(result, tuple)
                assert len(result) == 2
    
    async def test_get_samples_by_sequencing_file_filters_error_handling(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters error handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                with pytest.raises(Exception):
                    await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
    
    async def test_get_samples_by_pathology_file_filters_preservation_method(self, repository, mock_session):
        """Test _get_samples_by_pathology_file_filters with preservation_method."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"preservation_method": "FFPE"}
        result = await repository._get_samples_by_pathology_file_filters(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
    
    async def test_get_samples_by_pathology_file_filters_tumor_grade(self, repository, mock_session):
        """Test _get_samples_by_pathology_file_filters with tumor_grade."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"tumor_grade": "G1"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"tumor_grade": "G1"}
        result = await repository._get_samples_by_pathology_file_filters(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
    
    async def test_get_samples_by_pathology_file_filters_return_total(self, repository, mock_session):
        """Test _get_samples_by_pathology_file_filters with return_total=True."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        # Mock count query result - need to support async iteration and dict() conversion
        async def count_async_gen():
            yield {"total_count": 30}
        
        mock_count_result = AsyncMock()
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())
        mock_count_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        filters = {"preservation_method": "FFPE"}
        result = await repository._get_samples_by_pathology_file_filters(filters, offset=0, limit=20, return_total=True)
        
        assert mock_session.run.call_count == 2
        assert isinstance(result, tuple)
    
    async def test_get_samples_by_combined_filters(self, repository, mock_session):
        """Test _get_samples_by_combined_filters with both sequencing_file and pathology_file filters."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS", "preservation_method": "FFPE"}
                result = await repository._get_samples_by_combined_filters(filters, offset=0, limit=20)
                
                assert mock_session.run.called
                assert isinstance(result, list)
    
    async def test_get_samples_by_combined_filters_return_total(self, repository, mock_session):
        """Test _get_samples_by_combined_filters with return_total=True."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        # Mock count query result - need to support async iteration and dict() conversion
        async def count_async_gen():
            yield {"total_count": 20}
        
        mock_count_result = AsyncMock()
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())
        mock_count_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS", "preservation_method": "FFPE"}
                result = await repository._get_samples_by_combined_filters(filters, offset=0, limit=20, return_total=True)
                
                assert mock_session.run.call_count == 2
                assert isinstance(result, tuple)
    
    async def test_get_samples_summary_reverse_query(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query."""
        mock_result = AsyncMock()
        mock_record = Mock()
        mock_record.__getitem__ = Mock(return_value=75)
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"library_strategy": "WXS"}
        result = await repository._get_samples_summary_reverse_query(filters)
        
        assert mock_session.run.called
        assert "total" in result.get("counts", {})
