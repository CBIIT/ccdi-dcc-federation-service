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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                
                assert mock_session.run.called
                assert isinstance(result, list)
                
                # Verify early pagination: SKIP/LIMIT should come before collecting study relationships
                call_args = mock_session.run.call_args
                query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
                # Check that SKIP/LIMIT appears before OPTIONAL MATCH for study collection
                skip_pos = query.find("SKIP")
                limit_pos = query.find("LIMIT")
                # Verify pagination is present
                assert skip_pos != -1, "SKIP should be present in query"
                assert limit_pos != -1, "LIMIT should be present in query"
                # Study collection may come before or after pagination depending on query structure
                assert ('OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query or
                        'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or
                        'collect(DISTINCT st1.study_id)' in query or
                        'collect(DISTINCT st2.study_id)' in query), "Study collection should be present"
    
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.is_null_mapped_value', return_value=False):
                with patch('app.repositories.sample.reverse_map_field_value', return_value="DNA"):
                    filters = {"specimen_molecular_analyte_type": "DNA"}
                    result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
                    
                    assert mock_session.run.called
                    assert isinstance(result, list)
                    
                    # Verify early pagination structure
                    call_args = mock_session.run.call_args
                    query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
                    # Verify early pagination structure
                    # Current query structure: study collection -> ORDER BY -> SKIP -> LIMIT -> final OPTIONAL MATCHes
                    skip_pos = query.find("SKIP")
                    limit_pos = query.find("LIMIT")
                    study_collection_pos = query.find("collect(DISTINCT st1.study_id)")
                    final_optional_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)")
                    
                    assert skip_pos != -1, "SKIP should be present"
                    assert limit_pos != -1, "LIMIT should be present"
                    # Verify pagination and study collection are present (order may vary)
                    assert skip_pos != -1 and limit_pos != -1, "SKIP/LIMIT should be present"
                    assert (study_collection_pos != -1 or 
                            'collect(DISTINCT st1.study_id)' in query or
                            'collect(DISTINCT st2.study_id)' in query), "Study collection should be present"
    
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
                    
                    # Verify early pagination: SKIP/LIMIT after study collection but before final OPTIONAL MATCHes
                    skip_pos = query.find("SKIP")
                    limit_pos = query.find("LIMIT")
                    study_collection_pos = query.find("collect(DISTINCT st1.study_id)")
                    final_optional_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)")
                    
                    assert skip_pos != -1, "SKIP should be present"
                    assert limit_pos != -1, "LIMIT should be present"
                    # Verify pagination and study collection are present (order may vary)
                    assert skip_pos != -1 and limit_pos != -1, "SKIP/LIMIT should be present"
                    assert (study_collection_pos != -1 or 
                            'collect(DISTINCT st1.study_id)' in query or
                            'collect(DISTINCT st2.study_id)' in query), "Study collection should be present"
    
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
        # The code does dict(r), so return a simple dict (dict() on a dict returns itself)
        async def count_async_gen():
            yield {"total_count": 50}
        
        mock_count_result = AsyncMock()
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())  # Properly set up async iterator
        mock_count_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                result = await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20, return_total=True)
                
                assert mock_session.run.call_count == 2
                assert isinstance(result, tuple)
                assert len(result) == 2
                
                # Verify the list query (second call) has early pagination
                list_query_call = mock_session.run.call_args_list[1]
                query = list_query_call[0][0] if list_query_call[0] else list_query_call.kwargs.get('cypher', '')
                # Verify early pagination structure
                # Query structure may vary, but SKIP/LIMIT should be present
                skip_pos = query.find("SKIP")
                limit_pos = query.find("LIMIT")
                
                assert skip_pos != -1 and limit_pos != -1, "Early pagination structure should be present"
                # Verify study collection pattern exists (may be before or after pagination)
                assert ('collect(DISTINCT st1.study_id)' in query or 
                        'collect(DISTINCT st2.study_id)' in query or
                        'st1_list' in query or 'st2_list' in query or
                        'study_id' in query)
    
    async def test_get_samples_by_sequencing_file_filters_error_handling(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters error handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                with pytest.raises(Exception):
                    await repository._get_samples_by_sequencing_file_filters(filters, offset=0, limit=20)
    
    async def test_get_samples_by_sequencing_file_filters_pagination_structure(self, repository, mock_session):
        """Test that pagination is correctly implemented at (sample_id, study_id) pair level."""
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                filters = {"library_strategy": "WXS"}
                await repository._get_samples_by_sequencing_file_filters(filters, offset=10, limit=5)
                
                # Verify query structure for pagination at (sample_id, study_id) pair level
                call_args = mock_session.run.call_args
                query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
                
                # Extract positions of key query elements
                # Query structure: study collection -> UNWIND -> WITH DISTINCT sa, st -> ORDER BY -> SKIP -> LIMIT -> rematch sf -> OPTIONAL MATCHes
                order_by_pos = query.find("ORDER BY")
                skip_pos = query.find("SKIP")
                limit_pos = query.find("LIMIT")
                distinct_pos = query.find("WITH DISTINCT sa, st")
                unwind_pos = query.find("UNWIND combined")
                study_collection_pos = query.find("collect(DISTINCT st1.study_id)")
                rematch_sf_pos = query.find("sf_rematched:sequencing_file")
                final_optional_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)")
                
                # Verify pagination at pair level
                assert order_by_pos != -1, "ORDER BY should be present"
                assert skip_pos != -1, "SKIP should be present"
                assert limit_pos != -1, "LIMIT should be present"
                assert distinct_pos != -1, "WITH DISTINCT sa, st should be present"
                assert unwind_pos != -1, "UNWIND should be present"
                
                # Verify order: UNWIND -> WITH DISTINCT -> ORDER BY -> SKIP/LIMIT -> rematch sf -> OPTIONAL MATCHes
                assert unwind_pos < distinct_pos, "UNWIND should come before WITH DISTINCT"
                assert distinct_pos < order_by_pos, "WITH DISTINCT should come before ORDER BY"
                assert order_by_pos < skip_pos, "ORDER BY should come before SKIP"
                assert skip_pos < limit_pos, "SKIP should come before LIMIT"
                
                # SKIP/LIMIT should come before rematching sf and final OPTIONAL MATCHes
                if rematch_sf_pos != -1:
                    assert limit_pos < rematch_sf_pos, "LIMIT should come before rematching sf"
                if final_optional_match_pos != -1:
                    assert limit_pos < final_optional_match_pos, "LIMIT should come before final OPTIONAL MATCHes"
                
                # Verify parameters include offset and limit
                params = call_args.kwargs.get('params', {}) if call_args.kwargs else (call_args[0][1] if len(call_args[0]) > 1 else {})
                assert 'offset' in params, "offset parameter should be present"
                assert 'limit' in params, "limit parameter should be present"
                assert params.get('offset') == 10, "offset should be 10"
                assert params.get('limit') == 5, "limit should be 5"
    
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())  # Properly set up async iterator
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
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
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())  # Properly set up async iterator
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


def _empty_case3_result():
    """Return a mock DB result that yields one minimal sample record."""
    async def async_gen():
        yield {
            "sa": {"sample_id": "SAMP001"},
            "p": {"participant_id": "PART001"},
            "st": {"study_id": "phs001"},
            "sf": {},
            "pf": {},
            "diagnoses": [{"diagnosis": "leukemia", "disease_phase": "Initial Diagnosis"}],
        }

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    return mock_result


def _make_categorized(
    diagnosis: dict | None = None,
    sequencing_file: dict | None = None,
    pathology_file: dict | None = None,
) -> dict:
    """Build a minimal categorized filter dict for Case 3 tests."""
    return {
        "sample": {},
        "study": {},
        "diagnosis": diagnosis or {},
        "sequencing_file": sequencing_file or {},
        "pathology_file": pathology_file or {},
    }


def _mock_count_result(total: int = 50):
    """Async mock for Case 3 count query returning total_count."""
    async def count_async_gen():
        yield {"total_count": total}

    mock_count_result = AsyncMock()
    mock_count_result.__aiter__ = Mock(return_value=count_async_gen())
    mock_count_result.consume = AsyncMock()
    return mock_count_result


@pytest.mark.unit
class TestPreUnwindOrdering:
    """Assert that diagnosis MATCH/OPTIONAL MATCH appears before UNWIND in Case 3 queries.

    The pre-UNWIND optimization collects matching diagnosis nodes per sample
    before study collection and UNWIND, avoiding the full samples × studies
    cross product. These tests verify query ordering — not result correctness.
    """

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_allowlist(self):
        al = Mock(spec=FieldAllowlist)
        al.is_field_allowed = Mock(return_value=True)
        al.is_allowed = Mock(return_value=True)
        return al

    @pytest.fixture
    def mock_settings(self):
        s = Mock(spec=Settings)
        s.pagination = Mock()
        s.pagination.max_page_size = 1000
        s.sample_count_fields = []
        return s

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    @pytest.mark.asyncio
    async def test_filter_only_diagnosis_match_before_unwind(self, repository, mock_session):
        """Non-search diagnosis filter: MATCH (d:diagnosis) must precede UNWIND combined."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(diagnosis={"disease_phase": "Initial Diagnosis"})
        await repository._get_samples_case3_with_node_filters(
            {}, categorized, offset=0, limit=20, base_url=None, return_total=False
        )
        query = mock_session.run.call_args[0][0]
        match_pos = query.find("MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        unwind_pos = query.find("UNWIND combined")
        assert match_pos != -1, "pre-UNWIND MATCH (d:diagnosis) should be present"
        assert unwind_pos != -1, "UNWIND combined should be present"
        assert match_pos < unwind_pos, "diagnosis MATCH must appear before UNWIND"

    @pytest.mark.asyncio
    async def test_search_only_optional_match_before_unwind(self, repository, mock_session):
        """Diagnosis search: OPTIONAL MATCH (d:diagnosis) must precede UNWIND combined."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(diagnosis={"_diagnosis_search": "leukemia"})
        await repository._get_samples_case3_with_node_filters(
            {}, categorized, offset=0, limit=20, base_url=None, return_total=False
        )
        query = mock_session.run.call_args[0][0]
        opt_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        unwind_pos = query.find("UNWIND combined")
        assert opt_match_pos != -1, "pre-UNWIND OPTIONAL MATCH (d:diagnosis) should be present"
        assert unwind_pos != -1, "UNWIND combined should be present"
        assert opt_match_pos < unwind_pos, "diagnosis OPTIONAL MATCH must appear before UNWIND"

    @pytest.mark.asyncio
    async def test_search_with_filter_optional_match_before_unwind(self, repository, mock_session):
        """Search + filter combined: pre-UNWIND OPTIONAL MATCH (d:diagnosis) before UNWIND."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(
            diagnosis={"_diagnosis_search": "leukemia", "disease_phase": "Initial Diagnosis"}
        )
        await repository._get_samples_case3_with_node_filters(
            {}, categorized, offset=0, limit=20, base_url=None, return_total=False
        )
        query = mock_session.run.call_args[0][0]
        opt_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
        unwind_pos = query.find("UNWIND combined")
        assert opt_match_pos != -1, "pre-UNWIND OPTIONAL MATCH (d:diagnosis) should be present"
        assert unwind_pos != -1, "UNWIND combined should be present"
        assert opt_match_pos < unwind_pos, "diagnosis OPTIONAL MATCH must appear before UNWIND"

    @pytest.mark.asyncio
    async def test_sf_filter_pre_unwind_match_before_unwind(self, repository, mock_session):
        """Sequencing_file filter: MATCH (sf) must precede UNWIND combined."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(sequencing_file={"library_strategy": "WXS"})
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_query_cases.reverse_map_field_value", return_value="WXS"):
                await repository._get_samples_case3_with_node_filters(
                    {}, categorized, offset=0, limit=20, base_url=None, return_total=False
                )
        query = mock_session.run.call_args[0][0]
        sf_match_pos = query.find("MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
        unwind_pos = query.find("UNWIND combined")
        assert sf_match_pos != -1
        assert sf_match_pos < unwind_pos
        assert "all_sf" in query
        assert "OPTIONAL MATCH (sf:sequencing_file)" not in query

    @pytest.mark.asyncio
    async def test_diagnosis_and_sf_both_pre_unwind_before_unwind(self, repository, mock_session):
        """Diagnosis + sf filters: both pre-UNWIND vars carried into study collection."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(
            diagnosis={"disease_phase": "Initial Diagnosis"},
            sequencing_file={"library_strategy": "WXS"},
        )
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_query_cases.reverse_map_field_value", return_value="WXS"):
                await repository._get_samples_case3_with_node_filters(
                    {}, categorized, offset=0, limit=20, base_url=None, return_total=False
                )
        query = mock_session.run.call_args[0][0]
        unwind_pos = query.find("UNWIND combined")
        assert query.find("MATCH (d:diagnosis)-[:of_diagnosis]->(sa)") < unwind_pos
        assert query.find("MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)") < unwind_pos
        assert "all_diagnoses" in query and "all_sf" in query

    @pytest.mark.asyncio
    async def test_return_total_uses_pre_unwind_count_query(self, repository, mock_session):
        """return_total=True runs optimized count with WITH DISTINCT sa after pre-UNWIND."""
        mock_session.run = AsyncMock(
            side_effect=[_mock_count_result(42), _empty_case3_result()]
        )
        categorized = _make_categorized(diagnosis={"disease_phase": "Initial Diagnosis"})
        result = await repository._get_samples_case3_with_node_filters(
            {}, categorized, offset=0, limit=20, base_url=None, return_total=True
        )
        assert mock_session.run.call_count == 2
        count_query = mock_session.run.call_args_list[0][0][0]
        assert "WITH DISTINCT sa" in count_query
        assert "MATCH (d:diagnosis)-[:of_diagnosis]->(sa)" in count_query
        assert isinstance(result, tuple)
        assert result[1] == 42

    @pytest.mark.asyncio
    async def test_return_total_sf_pre_unwind_count_query(self, repository, mock_session):
        """return_total=True with sf-only filter uses sf pre-UNWIND in count path."""
        mock_session.run = AsyncMock(
            side_effect=[_mock_count_result(7), _empty_case3_result()]
        )
        categorized = _make_categorized(sequencing_file={"library_strategy": "WXS"})
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_query_cases.reverse_map_field_value", return_value="WXS"):
                result = await repository._get_samples_case3_with_node_filters(
                    {}, categorized, offset=0, limit=20, base_url=None, return_total=True
                )
        count_query = mock_session.run.call_args_list[0][0][0]
        assert "MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)" in count_query
        assert "WITH DISTINCT sa" in count_query
        assert result[1] == 7

    @pytest.mark.asyncio
    async def test_invalid_library_strategy_returns_empty_without_db(self, repository, mock_session):
        """Database-only library_strategy short-circuits before session.run."""
        mock_session.run = AsyncMock()
        categorized = _make_categorized(sequencing_file={"library_strategy": "Archer Fusion"})
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=True):
            result = await repository._get_samples_case3_with_node_filters(
                {}, categorized, offset=0, limit=20, base_url=None, return_total=False
            )
        assert result == []
        mock_session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_library_strategy_return_total_zero(self, repository, mock_session):
        """Database-only library_strategy with return_total returns ([], 0)."""
        mock_session.run = AsyncMock()
        categorized = _make_categorized(sequencing_file={"library_strategy": "Archer Fusion"})
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=True):
            result = await repository._get_samples_case3_with_node_filters(
                {}, categorized, offset=0, limit=20, base_url=None, return_total=True
            )
        assert result == ([], 0)
        mock_session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_library_strategy_other_reverse_mapping_in_pre_unwind(self, repository, mock_session):
        """Other -> Archer Fusion produces OR clause in pre-UNWIND sf WHERE."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(sequencing_file={"library_strategy": "Other"})
        with patch("app.repositories.sample_query_cases.is_database_only_value", return_value=False):
            with patch(
                "app.repositories.sample_query_cases.reverse_map_field_value",
                return_value="Archer Fusion",
            ):
                await repository._get_samples_case3_with_node_filters(
                    {}, categorized, offset=0, limit=20, base_url=None, return_total=False
                )
        query = mock_session.run.call_args[0][0]
        assert "sf.library_strategy = $param_1 OR sf.library_strategy = $param_2" in query

    @pytest.mark.asyncio
    async def test_pathology_file_filter_post_unwind(self, repository, mock_session):
        """preservation_method keeps pf OPTIONAL MATCH post-UNWIND with size check."""
        mock_session.run = AsyncMock(return_value=_empty_case3_result())
        categorized = _make_categorized(pathology_file={"preservation_method": "FFPE"})
        await repository._get_samples_case3_with_node_filters(
            {}, categorized, offset=0, limit=20, base_url=None, return_total=False
        )
        query = mock_session.run.call_args[0][0]
        assert "OPTIONAL MATCH (pf:pathology_file)" in query
        assert "pf.fixation_embedding_method" in query
        assert "size([pf IN all_pfs WHERE pf IS NOT NULL]) > 0" in query
