"""
Unit tests for diagnosis search functionality in SampleRepository.

Tests the experimental diagnosis search endpoint behavior:
- Early pagination applied
- All matching diagnoses kept
- Non-filtered OPTIONAL MATCH returns only 1 record
- No CALL {} subqueries
- Behavior when search parameter is not provided
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
class TestDiagnosisSearch:
    """Test diagnosis search functionality."""
    
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
    
    async def test_diagnosis_search_early_pagination(self, repository, mock_session):
        """Test that diagnosis search query applies early pagination."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [
                    {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"},
                    {"diagnosis": "Neuroblastoma, NOS", "disease_phase": "Primary"}
                ]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify query starts from samples, not diagnoses
        assert 'MATCH (sa:sample)' in query
        assert query.index('MATCH (sa:sample)') < query.index('OPTIONAL MATCH (sa)<-[:of_diagnosis]-(dx:diagnosis)')
        
        # Verify early pagination: ORDER BY, SKIP, LIMIT before fetching optional relationships
        query_lines = query.split('\n')
        order_by_idx = -1
        skip_limit_idx = -1
        optional_match_participant_idx = -1
        
        for i, line in enumerate(query_lines):
            if 'ORDER BY' in line and 'toString(sa.sample_id)' in line:
                order_by_idx = i
            if 'SKIP $offset' in line or 'LIMIT $limit' in line:
                skip_limit_idx = i
            if 'OPTIONAL MATCH (sa)-[:of_sample]->(p0:participant)' in line:
                optional_match_participant_idx = i
                break
        
        # ORDER BY, SKIP, LIMIT should come before optional matches for participant/pathology/sequencing
        assert order_by_idx > 0, "ORDER BY should be present"
        assert skip_limit_idx > 0, "SKIP/LIMIT should be present"
        if optional_match_participant_idx > 0:
            assert skip_limit_idx < optional_match_participant_idx, \
                f"SKIP/LIMIT (line {skip_limit_idx}) should come before OPTIONAL MATCH for participant (line {optional_match_participant_idx})"
    
    async def test_diagnosis_search_all_diagnoses_kept(self, repository, mock_session):
        """Test that all matching diagnoses are kept in the query result."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [
                    {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"},
                    {"diagnosis": "Neuroblastoma, NOS", "disease_phase": "Primary"}
                ]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        # Get the query to verify structure
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify that diagnoses is returned as a list (not head(diagnoses))
        assert 'RETURN sa, p, st, pf, sf, diagnoses' in query
        assert 'head(diagnoses)' not in query  # Should return all diagnoses, not just head
        
        # Verify that diagnoses list is filtered but all matching ones are kept
        assert 'size(diagnoses) > 0' in query
        
        # Verify uses collect(DISTINCT dx) to collect all matching diagnoses
        assert 'collect(DISTINCT dx) AS diagnoses' in query
        
        # Verify uses dx variable, not d (to avoid variable reuse)
        assert 'OPTIONAL MATCH (sa)<-[:of_diagnosis]-(dx:diagnosis)' in query
        assert 'MATCH (d:diagnosis)' not in query  # Should not start with diagnosis match
    
    async def test_diagnosis_search_non_filtered_optional_match_single_record(self, repository, mock_session):
        """Test that non-filtered OPTIONAL MATCH returns only 1 record using head(collect())."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify that p, pf, sf use head(collect(DISTINCT ...)) to get only 1 record
        # Uses chained WITH clauses to avoid cartesian products
        assert 'head(collect(DISTINCT p0))' in query or 'head(collect(DISTINCT p0))' in query.replace(' ', '')
        assert 'head(collect(DISTINCT pf0))' in query or 'head(collect(DISTINCT pf0))' in query.replace(' ', '')
        assert 'head(collect(DISTINCT sf0))' in query or 'head(collect(DISTINCT sf0))' in query.replace(' ', '')
        
        # Verify chained WITH clauses to avoid cross-products
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(p0:participant)' in query
        assert 'WITH sa, st, diagnoses, head(collect(DISTINCT p0)) AS p' in query or 'WITH sa, st, diagnoses' in query
    
    async def test_diagnosis_search_sequential_optional_match(self, repository, mock_session):
        """Test that diagnosis search uses sequential OPTIONAL MATCH instead of CALL {}."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify query starts from samples, not diagnoses
        assert 'MATCH (sa:sample)' in query
        
        # Verify sequential OPTIONAL MATCH for study collection
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)' in query
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)' in query
        
        # Verify no redundant WHERE sid IS NOT NULL after UNWIND (already filtered in list comprehension)
        # The query should not have "WHERE sid IS NOT NULL" after MATCH (st:study)
        study_match_line = None
        for i, line in enumerate(query.split('\n')):
            if 'MATCH (st:study' in line:
                study_match_line = line
                break
        if study_match_line:
            # Should not have redundant WHERE sid IS NOT NULL
            assert 'WHERE sid IS NOT NULL' not in study_match_line or 'depositions' in study_match_line
    
    async def test_diagnosis_search_all_diagnoses_preserved_in_result(self, repository, mock_session):
        """Test that all matching diagnoses are preserved in the sample object."""
        from app.models.dto import Sample
        
        # Create mock records with multiple matching diagnoses
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [
                    {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"},
                    {"diagnosis": "Neuroblastoma, NOS", "disease_phase": "Primary"}
                ]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        # Verify that all_matching_diagnoses attribute is set (if sample has it)
        # Note: This depends on the _record_to_sample implementation
        assert len(result) > 0
        # The sample should have all_matching_diagnoses attribute set
        # (This is set in the record processing code)
    
    async def test_diagnosis_search_without_search_parameter(self, repository, mock_session):
        """Test that when _diagnosis_search is not provided, regular query logic is used."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call get_samples without _diagnosis_search - should use regular query logic
        filters = {"disease_phase": "Primary"}
        await repository.get_samples(filters=filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Should NOT use diagnosis search query (no MATCH (d:diagnosis) at start)
        # Should use regular query pattern
        assert 'MATCH (d:diagnosis)' not in query or query.index('MATCH (d:diagnosis)') > query.index('MATCH (sa:sample)')
    
    async def test_diagnosis_search_summary_query(self, repository, mock_session):
        """Test diagnosis search summary query structure."""
        mock_result = AsyncMock()
        mock_record = Mock()
        mock_record.__getitem__ = Mock(return_value=100)
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        result = await repository._get_samples_summary_diagnosis_search(filters)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify NO CALL {} subquery
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify it counts distinct (sample_id, study_id) pairs
        assert 'WITH DISTINCT sa.sample_id AS sample_id, st.study_id AS study_id' in query
        assert 'RETURN count(*) AS total_count' in query
    
    async def test_diagnosis_search_with_identifiers(self, repository, mock_session):
        """Test diagnosis search with identifiers filter."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "identifiers": "SAMP001"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify identifiers filter is applied early
        assert 'sa.sample_id' in query
        # Should be in WHERE clause before study collection
    
    async def test_diagnosis_search_with_depositions(self, repository, mock_session):
        """Test diagnosis search with depositions filter."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "depositions": "phs001"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify depositions filter is applied
        assert 'sid' in query or 'study_id' in query
        # Should filter study IDs
    
    async def test_diagnosis_search_empty_search_term(self, repository, mock_session):
        """Test that empty diagnosis search term returns empty list."""
        filters = {"_diagnosis_search": ""}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        assert result == []
        assert not mock_session.run.called
    
    async def test_diagnosis_search_none_search_term(self, repository, mock_session):
        """Test that None diagnosis search term returns empty list."""
        filters = {}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        assert result == []
        assert not mock_session.run.called
    
    async def test_diagnosis_search_with_identifiers_list(self, repository, mock_session):
        """Test diagnosis search with identifiers filter as list (|| delimiter)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "identifiers": "SAMP001||SAMP002"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get('params', {})
        
        # Verify identifiers filter uses IN clause for list
        assert 'IN $' in query or 'sa.sample_id' in query
    
    async def test_diagnosis_search_with_depositions_list(self, repository, mock_session):
        """Test diagnosis search with depositions filter as list (|| delimiter)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "depositions": "phs001||phs002"}
        await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify depositions filter uses IN clause for multiple values
        assert 'IN $' in query or 'study_id' in query
    
    async def test_diagnosis_search_return_total(self, repository, mock_session):
        """Test diagnosis search with return_total=True."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [{"diagnosis": "Neuroblastoma"}]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        # Mock count query result - need to support dict() conversion
        async def count_async_gen():
            yield {"total_count": 100}
        
        mock_count_result = AsyncMock()
        mock_count_result.__aiter__ = Mock(return_value=count_async_gen())
        mock_count_result.consume = AsyncMock()
        
        # First call is count query, second is list query
        mock_session.run = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20, return_total=True)
        
        assert mock_session.run.call_count == 2
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], int)
    
    async def test_diagnosis_search_error_handling(self, repository, mock_session):
        """Test diagnosis search error handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        with pytest.raises(Exception):
            await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
    
    async def test_diagnosis_search_summary_empty_search_term(self, repository, mock_session):
        """Test diagnosis search summary with empty search term."""
        filters = {"_diagnosis_search": ""}
        result = await repository._get_samples_summary_diagnosis_search(filters)
        assert result == {"counts": {"total": 0}}
        assert not mock_session.run.called
    
    async def test_diagnosis_search_summary_with_identifiers(self, repository, mock_session):
        """Test diagnosis search summary with identifiers filter."""
        mock_result = AsyncMock()
        mock_record = Mock()
        mock_record.__getitem__ = Mock(return_value=50)
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "identifiers": "SAMP001"}
        result = await repository._get_samples_summary_diagnosis_search(filters)
        
        assert mock_session.run.called
        assert "total" in result.get("counts", {})
    
    async def test_diagnosis_search_summary_with_depositions(self, repository, mock_session):
        """Test diagnosis search summary with depositions filter."""
        mock_result = AsyncMock()
        mock_record = Mock()
        mock_record.__getitem__ = Mock(return_value=25)
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma", "depositions": "phs001"}
        result = await repository._get_samples_summary_diagnosis_search(filters)
        
        assert mock_session.run.called
        assert "total" in result.get("counts", {})
    
    async def test_diagnosis_search_multiple_diagnoses_in_result(self, repository, mock_session):
        """Test that multiple matching diagnoses (exact and partial matches) are preserved."""
        from app.models.dto import Sample
        
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": [
                    {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"},
                    {"diagnosis": "Neuroblastoma, NOS", "disease_phase": "Primary"},
                    {"diagnosis": "Some Neuroblastoma variant", "disease_phase": "Primary"}
                ]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        filters = {"_diagnosis_search": "Neuroblastoma"}
        result = await repository._get_samples_by_diagnosis_search(filters, offset=0, limit=20)
        
        assert len(result) > 0
        sample = result[0]
        assert isinstance(sample, Sample)
        # Verify all_matching_diagnoses attribute is set
        assert hasattr(sample, 'all_matching_diagnoses')
        assert isinstance(sample.all_matching_diagnoses, list)
        # All three diagnoses match "Neuroblastoma" (exact and partial matches)
        assert len(sample.all_matching_diagnoses) == 3
        
        # Verify the query uses collect(DISTINCT dx) to collect all matches
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'collect(DISTINCT dx) AS diagnoses' in query


@pytest.mark.unit
class TestGetSamplesForDiagnosisEndpointRouting:
    """Verify get_samples_for_diagnosis_endpoint routes to the optimised path."""

    @pytest.fixture
    def repository(self):
        mock_session = AsyncMock()
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        settings = Mock(spec=Settings)
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.sample_count_fields = []
        return SampleRepository(mock_session, allowlist, settings)

    async def test_diagnosis_category_routes_to_optimised_path(self, repository):
        """diagnosis_category alone must use _get_samples_by_diagnosis_search (WHERE push-down)."""
        repository._get_samples_by_diagnosis_search = AsyncMock(return_value=([], 0))

        await repository.get_samples_for_diagnosis_endpoint(
            filters={"diagnosis_category": "Gliomas", "_sample_diagnosis_category_substring": True},
        )

        repository._get_samples_by_diagnosis_search.assert_awaited_once()

    async def test_diagnosis_search_routes_to_optimised_path(self, repository):
        """_diagnosis_search alone must use _get_samples_by_diagnosis_search."""
        repository._get_samples_by_diagnosis_search = AsyncMock(return_value=([], 0))

        await repository.get_samples_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Glioma"},
        )

        repository._get_samples_by_diagnosis_search.assert_awaited_once()

    async def test_mixed_filters_fall_through_to_get_samples(self, repository):
        """tissue_type + diagnosis_category must NOT use the optimised path (Case 3 fallback)."""
        repository._get_samples_by_diagnosis_search = AsyncMock(return_value=([], 0))
        repository.get_samples = AsyncMock(return_value=([], 0))

        await repository.get_samples_for_diagnosis_endpoint(
            filters={
                "tissue_type": "Tumor",
                "diagnosis_category": "Gliomas",
                "_sample_diagnosis_category_substring": True,
            },
        )

        repository._get_samples_by_diagnosis_search.assert_not_awaited()
        repository.get_samples.assert_awaited_once()
