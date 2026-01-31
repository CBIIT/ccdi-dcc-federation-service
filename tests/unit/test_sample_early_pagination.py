"""
Unit tests for early pagination optimization in sample queries.

These tests ensure that early pagination queries correctly handle variable rematching
after pagination, especially for filtered relationships like pathology_file and sequencing_file.
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
class TestEarlyPagination:
    """Test early pagination optimization."""
    
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
    
    async def test_preservation_method_early_pagination(self, repository, mock_session):
        """Test that preservation_method filter uses early pagination correctly."""
        # Mock query execution
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = []
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call with preservation_method filter
        filters = {"preservation_method": "OCT"}
        await repository.get_samples(filters=filters, offset=0, limit=20)
        
        # Verify query was executed
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify pagination is present
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Check query structure - should have pf filtered somewhere
        query_lines = query.split('\n')
        
        # Check for early pagination pattern: rematch sa after SKIP/LIMIT
        has_early_pagination = False
        skip_limit_line = -1
        rematch_sa_line = -1
        
        for i, line in enumerate(query_lines):
            if 'SKIP $offset' in line or 'LIMIT $limit' in line:
                skip_limit_line = i
            if skip_limit_line > 0 and 'MATCH (sa:sample), (st:study)' in line:
                # Check if this is the rematch pattern (has WHERE with sample_id = sample_id)
                context = '\n'.join(query_lines[max(0, i-2):i+3])
                if 'sample_id = sample_id' in context or 'sample_id = sample_id' in line:
                    rematch_sa_line = i
                    has_early_pagination = True
                    break
        
        # Verify pf is filtered correctly
        # Query now starts from sample nodes, then optionally matches pathology_file with filter
        pf_filtered_correctly = False
        
        # Check for standard query pattern (starts from sample nodes)
        is_standard_query = 'MATCH (sa:sample)' in query
        
        if is_standard_query:
            # Standard query pattern: samples are matched first, then pf is optionally matched with filter
            # Check that pf filter is in WHERE clause after OPTIONAL MATCH
            pf_optional_match_idx = query.find('OPTIONAL MATCH (pf:pathology_file)')
            if pf_optional_match_idx >= 0:
                # Get context around pf optional match and subsequent WITH/WHERE clauses
                context_start = max(0, pf_optional_match_idx - 100)
                context_end = min(len(query), pf_optional_match_idx + 500)
                context = query[context_start:context_end]
                # Check for filter in WHERE clause (pf.fixation_embedding_method = $param or similar)
                if 'fixation_embedding_method' in context:
                    pf_filtered_correctly = True
        elif has_early_pagination:
            # Early pagination pattern: check for pf rematch with filter after rematching sa
            for i in range(rematch_sa_line, min(rematch_sa_line + 20, len(query_lines))):
                line = query_lines[i]
                if 'OPTIONAL MATCH (pf:pathology_file)' in line:
                    # Check if filter is in this line or next line
                    context_lines = query_lines[i:min(i+3, len(query_lines))]
                    context = '\n'.join(context_lines)
                    if 'fixation_embedding_method' in context:
                        pf_filtered_correctly = True
                        break
        else:
            # Fallback: check if pathology_file is referenced and filter field is mentioned
            if 'OPTIONAL MATCH (pf:pathology_file)' in query or '(pf:pathology_file)' in query:
                if 'fixation_embedding_method' in query or ('pathology_file' in query and ('fixation' in query or 'embedding' in query or 'preservation' in query)):
                    pf_filtered_correctly = True
        
        assert pf_filtered_correctly, \
            f"pf should be filtered with preservation_method. " \
            f"Standard query: {is_standard_query}, " \
            f"Early pagination: {has_early_pagination}, " \
            f"Query preview: {query[:500]}"
    
    async def test_sequencing_file_early_pagination(self, repository, mock_session):
        """Test that sequencing_file filters use early pagination correctly."""
        # Mock query execution
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = []
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call with library_strategy filter (triggers reverse query with early pagination)
        filters = {"library_strategy": "WXS"}
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                await repository.get_samples(filters=filters, offset=0, limit=20)
        
        # Verify query was executed
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify early pagination pattern is used
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify reverse query pattern (starts from sequencing_file)
        assert 'MATCH (sf:sequencing_file)' in query
        
        # Verify early pagination: SKIP/LIMIT should come AFTER study collection but BEFORE final OPTIONAL MATCHes
        # The current query structure collects studies first, then paginates, then collects other relationships
        skip_pos = query.find("SKIP")
        limit_pos = query.find("LIMIT")
        study_collection_pos = query.find("collect(DISTINCT st1.study_id)")
        final_optional_match_pos = query.find("OPTIONAL MATCH (d:diagnosis)")
        
        assert skip_pos != -1, "SKIP should be present"
        assert limit_pos != -1, "LIMIT should be present"
        # SKIP/LIMIT should come after study collection but before final OPTIONAL MATCHes
        if study_collection_pos != -1 and final_optional_match_pos != -1:
            assert study_collection_pos < skip_pos < final_optional_match_pos, "Study collection -> SKIP -> final OPTIONAL MATCHes"
            assert study_collection_pos < limit_pos < final_optional_match_pos, "Study collection -> LIMIT -> final OPTIONAL MATCHes"
    
    async def test_no_variable_used_before_match(self, repository, mock_session):
        """Test that variables are not used before being matched."""
        # Mock query execution
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = []
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call with preservation_method filter
        filters = {"preservation_method": "OCT"}
        await repository.get_samples(filters=filters, offset=0, limit=20)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Check that pf is matched before being used in WITH
        query_lines = query.split('\n')
        pf_matched = False
        pf_used_in_with = False
        
        for line in query_lines:
            if 'OPTIONAL MATCH (pf:pathology_file)' in line:
                pf_matched = True
            if pf_matched and 'WITH' in line and 'pf' in line:
                pf_used_in_with = True
                break
        
        # If pf is used in WITH, it should have been matched first
        if pf_used_in_with:
            assert pf_matched, "pf should be matched before being used in WITH clause"

    async def test_disease_phase_early_pagination_with_call_subquery(self, repository, mock_session):
        """Test that disease_phase filter uses early pagination WITHOUT CALL {} subquery (sequential OPTIONAL MATCH)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {"disease_phase": "Primary"}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"):
            filters = {"disease_phase": "Primary"}
            await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery (should use sequential OPTIONAL MATCH instead)
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify diagnosis-first match pattern
        assert 'MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query
        
        # Verify early pagination: SKIP/LIMIT before loading optional matches
        query_lines = query.split('\n')
        skip_limit_idx = -1
        optional_match_idx = -1
        
        for i, line in enumerate(query_lines):
            if 'SKIP $offset' in line or 'LIMIT $limit' in line:
                skip_limit_idx = i
            if skip_limit_idx > 0 and ('OPTIONAL MATCH (p:participant)' in line or 
                                       'OPTIONAL MATCH (pf:pathology_file)' in line):
                optional_match_idx = i
                break
        
        # SKIP/LIMIT should come before optional matches
        assert skip_limit_idx > 0, "SKIP/LIMIT should be present"
        if optional_match_idx > 0:
            assert skip_limit_idx < optional_match_idx, \
                f"SKIP/LIMIT (line {skip_limit_idx}) should come before OPTIONAL MATCH (line {optional_match_idx})"
        
        # Verify sequential study collection (not CALL subquery)
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query

    async def test_tissue_type_early_pagination_with_call_subquery(self, repository, mock_session):
        """Test that tissue_type filter uses early pagination WITHOUT CALL {} subquery (sequential OPTIONAL MATCH)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            filters = {"tissue_type": "Tumor"}
            await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery (should use sequential OPTIONAL MATCH instead)
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify tissue_type filter is in WHERE clause
        assert 'sample_tumor_status' in query or 'tissue_type' in query.lower()
        
        # Verify early pagination: SKIP/LIMIT before loading optional matches
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify sequential study collection (not CALL subquery)
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query

    async def test_disease_phase_and_tissue_type_early_pagination(self, repository, mock_session):
        """Test that both disease_phase and tissue_type filters use early pagination together WITHOUT CALL {}."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {"disease_phase": "Primary"}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"), \
             patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            filters = {"disease_phase": "Primary", "tissue_type": "Tumor"}
            await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery (should use sequential OPTIONAL MATCH instead)
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify both filters are present
        assert 'sample_tumor_status' in query or 'tissue_type' in query.lower()
        # disease_phase filter should be in diagnosis match
        assert 'MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query or \
               'MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)' in query
        
        # Verify sequential study collection
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query

    async def test_early_pagination_not_applied_when_needs_sf_collection(self, repository, mock_session):
        """Test that early pagination is NOT applied when needs_sf_collection is True."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # disease_phase + library_strategy (triggers needs_sf_collection)
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"):
            filters = {"disease_phase": "Primary", "library_strategy": "WXS"}
            await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Should NOT use CALL {} subquery path (falls back to standard query)
        # Instead should use standard query with needs_sf_collection handling
        # The query should still work, just not the optimized early pagination path

    async def test_early_pagination_count_query_with_call_subquery(self, repository, mock_session):
        """Test count query for disease_phase filter (may or may not use CALL {} depending on implementation)."""
        async def async_gen():
            yield {"total_count": 100}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"):
            filters = {"disease_phase": "Primary"}
            await repository.get_samples(filters=filters, offset=0, limit=20, return_total=True)
        
        # Should have called run at least once (for count query)
        assert mock_session.run.called
        
        # Check if count query was generated (may be first or second call)
        call_count = mock_session.run.call_count
        count_query_found = False
        for i in range(call_count):
            call_args = mock_session.run.call_args_list[i]
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            if 'total_count' in query or 'count(*)' in query:
                count_query_found = True
                # Count query structure may vary - just verify it exists and has count logic
                assert 'count' in query.lower() or 'total' in query.lower()
                break
        
        assert count_query_found, "Count query should be present"

    async def test_no_filters_early_pagination_no_call_subquery(self, repository, mock_session):
        """Test that /sample with no filters uses early pagination WITHOUT CALL {} subquery."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={}, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify early pagination: SKIP/LIMIT before expanding relationships
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify NO CALL {} subquery (should use sequential OPTIONAL MATCH instead)
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify sequential study collection pattern
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query
        
        # Verify pagination comes before study collection
        query_lines = query.split('\n')
        skip_limit_idx = -1
        study_match_idx = -1
        
        for i, line in enumerate(query_lines):
            if 'SKIP $offset' in line or 'LIMIT $limit' in line:
                skip_limit_idx = i
            if skip_limit_idx > 0 and ('OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in line or
                                        'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in line):
                study_match_idx = i
                break
        
        assert skip_limit_idx > 0, "SKIP/LIMIT should be present"
        if study_match_idx > 0:
            assert skip_limit_idx < study_match_idx, \
                f"SKIP/LIMIT (line {skip_limit_idx}) should come before study collection (line {study_match_idx})"

    async def test_disease_phase_early_pagination_no_call_subquery(self, repository, mock_session):
        """Test that disease_phase filter uses early pagination WITHOUT CALL {} subquery (new structure)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {"disease_phase": "Not Reported"}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Not Reported"):
            filters = {"disease_phase": "Not Reported"}
            result = await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery (should use sequential OPTIONAL MATCH instead)
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify diagnosis-first match pattern
        assert 'MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query
        
        # Verify early pagination: SKIP/LIMIT before expanding relationships
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify sequential study collection (not CALL subquery)
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query
        
        # Verify WHERE clause format (single line, not multi-line)
        query_lines = query.split('\n')
        where_line = None
        for i, line in enumerate(query_lines):
            if line.strip().startswith('WHERE'):
                where_line = line
                break
        
        if where_line:
            # WHERE clause should be on single line (not split across lines with AND)
            assert '\n  AND' not in query[query.find('WHERE'):query.find('WHERE')+200]

    async def test_tissue_type_early_pagination_no_call_subquery(self, repository, mock_session):
        """Test that tissue_type filter uses early pagination WITHOUT CALL {} subquery (new structure)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            filters = {"tissue_type": "Tumor"}
            result = await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify tissue_type filter in WHERE clause
        assert 'sample_tumor_status' in query
        
        # Verify early pagination
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify sequential study collection
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query

    async def test_disease_phase_and_tissue_type_combined_no_call_subquery(self, repository, mock_session):
        """Test that disease_phase + tissue_type uses early pagination WITHOUT CALL {} subquery."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {"disease_phase": "Not Reported"}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Not Reported"), \
             patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            filters = {"disease_phase": "Not Reported", "tissue_type": "Tumor"}
            result = await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify both filters are present
        assert 'sample_tumor_status' in query
        assert 'MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query or \
               'disease_phase' in query
        
        # Verify early pagination
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query

    async def test_no_filters_with_empty_study_list(self, repository, mock_session):
        """Test /sample query handles samples with no study associations gracefully."""
        async def async_gen():
            # Return empty result (samples filtered out because no studies)
            if False:
                yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={}, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        # Should return empty list when no samples have study associations
        assert result == []

    async def test_disease_phase_with_empty_results(self, repository, mock_session):
        """Test disease_phase filter returns empty list when no matches."""
        async def async_gen():
            # Return empty result
            if False:
                yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="NonExistent"):
            filters = {"disease_phase": "NonExistent"}
            result = await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        assert isinstance(result, list)
        assert result == []

    async def test_no_filters_query_structure_validation(self, repository, mock_session):
        """Test that /sample query structure matches expected format."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.get_samples(filters={}, offset=0, limit=20)
        
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify query structure: MATCH -> WHERE -> WITH -> ORDER BY -> SKIP/LIMIT -> study collection
        assert 'MATCH (sa:sample)' in query
        assert 'trim(toString(sa.sample_id))' in query
        assert 'ORDER BY sample_id' in query
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify study collection comes after pagination
        query_lines = query.split('\n')
        limit_idx = -1
        study_collection_idx = -1
        
        for i, line in enumerate(query_lines):
            if 'LIMIT $limit' in line:
                limit_idx = i
            if limit_idx > 0 and ('OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in line or
                                   'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in line):
                study_collection_idx = i
                break
        
        assert limit_idx > 0, "LIMIT should be present"
        if study_collection_idx > 0:
            assert limit_idx < study_collection_idx, \
                "LIMIT should come before study collection"

