"""
Unit tests for Case 1: Sample-only filters query path.

Tests the new restructured query path for sample-only filters:
- Early pagination at sample level
- Then expand studies
- Then OPTIONAL MATCH other nodes
- Participant matched after pagination
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
class TestCase1SampleOnly:
    """Test Case 1: Sample-only filters query path."""
    
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
    
    async def test_case1_tissue_type_filter(self, repository, mock_session):
        """Test Case 1 with tissue_type filter."""
        # Mock query result
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs002431"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"preservation_method": "FFPE"},
                "diagnoses": {"diagnosis": "Neuroblastoma"}
            }
        
        mock_result = AsyncMock()
        # Properly set up async iterator - __aiter__ returns the async generator
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call get_samples with tissue_type filter only (Case 1)
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor"},
            offset=0,
            limit=20
        )
        
        # Verify Case 1 was called
        assert mock_session.run.called
        
        # Check that the query contains early pagination (SKIP/LIMIT before study expansion)
        call_args = mock_session.run.call_args
        cypher_query = call_args[0][0] if call_args[0] else ""
        
        # Verify query structure: MATCH -> WHERE -> WITH -> ORDER BY -> SKIP/LIMIT -> OPTIONAL MATCH studies
        assert "MATCH (sa:sample)" in cypher_query
        assert "SKIP $offset" in cypher_query
        assert "LIMIT $limit" in cypher_query
        
        # Verify SKIP/LIMIT comes before study expansion
        skip_pos = cypher_query.find("SKIP $offset")
        limit_pos = cypher_query.find("LIMIT $limit")
        study_match_pos = cypher_query.find("OPTIONAL MATCH (sa)-[:of_sample]->(:participant)")
        
        assert skip_pos < study_match_pos, "SKIP should come before study expansion"
        assert limit_pos < study_match_pos, "LIMIT should come before study expansion"
        
        # Verify participant is matched after pagination
        participant_match_pos = cypher_query.find("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
        assert participant_match_pos > limit_pos, "Participant should be matched after pagination"
        
        assert isinstance(result, list)
    
    async def test_case1_anatomical_sites_filter(self, repository, mock_session):
        """Test Case 1 with anatomical_sites filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        # Properly set up async iterator - __aiter__ returns the async generator
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call get_samples with anatomical_sites filter only (Case 1)
        result = await repository.get_samples(
            filters={"anatomical_sites": "C72.9 : Central nervous system"},
            offset=0,
            limit=20
        )
        
        assert mock_session.run.called
        
        # Verify query contains anatomical_sites filter condition
        call_args = mock_session.run.call_args
        cypher_query = call_args[0][0] if call_args[0] else ""
        params = call_args[0][1] if len(call_args[0]) > 1 else {}
        
        assert "anatomic_site" in cypher_query or "anatomical_sites" in cypher_query.lower()
        assert isinstance(result, list)
    
    async def test_case1_identifiers_filter(self, repository, mock_session):
        """Test Case 1 with identifiers filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        # Properly set up async iterator - __aiter__ returns the async generator
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call get_samples with identifiers filter only (Case 1)
        result = await repository.get_samples(
            filters={"identifiers": "SAMP001"},
            offset=0,
            limit=20
        )
        
        assert mock_session.run.called
        
        # Verify query contains identifiers filter
        call_args = mock_session.run.call_args
        cypher_query = call_args[0][0] if call_args[0] else ""
        params = call_args[0][1] if len(call_args[0]) > 1 else {}
        
        assert "sample_id" in cypher_query
        assert isinstance(result, list)
    
    async def test_case1_multiple_sample_filters(self, repository, mock_session):
        """Test Case 1 with multiple sample filters."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        # Properly set up async iterator - __aiter__ returns the async generator
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call get_samples with multiple sample filters (Case 1)
        result = await repository.get_samples(
            filters={
                "tissue_type": "Tumor",
                "anatomical_sites": "C72.9 : Central nervous system",
                "identifiers": "SAMP001"
            },
            offset=0,
            limit=20
        )
        
        assert mock_session.run.called
        assert isinstance(result, list)
    
    async def test_case1_invalid_tissue_type(self, repository, mock_session):
        """Test Case 1 returns empty list for invalid tissue_type."""
        # Invalid tissue_type should return empty results immediately
        result = await repository.get_samples(
            filters={"tissue_type": "InvalidValue"},
            offset=0,
            limit=20
        )
        
        # Should return empty list without running query
        assert result == []
        # Verify query was not run (since validation failed)
        assert not mock_session.run.called
    
    async def test_case1_with_return_total(self, repository, mock_session):
        """Test Case 1 with return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs002431"},
                "sf": None,
                "pf": None,
                "diagnoses": None
            }
        
        async def async_gen_count():
            yield {"total_count": 100}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        # First call is count query, second is list query
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should return tuple (samples, total_count)
        assert isinstance(result, tuple)
        samples, total_count = result
        assert isinstance(samples, list)
        assert isinstance(total_count, int)
        assert total_count == 100
        
        # Verify both queries were run
        assert mock_session.run.call_count == 2
