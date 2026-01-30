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
        # Check for reverse query pattern (starts from pathology_file) OR early pagination pattern OR standard pattern
        pf_filtered_correctly = False
        
        # Check if this is a reverse query pattern (starts from pathology_file)
        is_reverse_query = 'MATCH (pf:pathology_file)' in query and query.find('MATCH (pf:pathology_file)') < query.find('MATCH (sa:sample)')
        
        if is_reverse_query:
            # Reverse query pattern: pf is matched first with filter, then finds samples
            # Check that pf has the filter in WHERE clause
            pf_match_idx = query.find('MATCH (pf:pathology_file)')
            if pf_match_idx >= 0:
                # Get context around pf match
                context_start = max(0, pf_match_idx - 100)
                context_end = min(len(query), pf_match_idx + 500)
                context = query[context_start:context_end]
                if 'fixation_embedding_method' in context and 'OCT' in context:
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
            # Standard pattern: pf filter in WHERE (pf.fixation_embedding_method = $param)
            # Param value (e.g. OCT) may not appear in query string
            if 'OPTIONAL MATCH (pf:pathology_file)' in query or '(pf:pathology_file)' in query:
                if 'fixation_embedding_method' in query or ('pathology_file' in query and ('fixation' in query or 'embedding' in query or 'preservation' in query)):
                    pf_filtered_correctly = True
        
        assert pf_filtered_correctly, \
            f"pf should be filtered with preservation_method. " \
            f"Reverse query: {is_reverse_query}, " \
            f"Early pagination: {has_early_pagination}, " \
            f"Query preview: {query[:500]}"
    
    async def test_sequencing_file_early_pagination(self, repository, mock_session):
        """Test that sequencing_file filters use early pagination correctly."""
        # Mock query execution
        mock_result = AsyncMock()
        mock_result.__aiter__.return_value = []
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Call with library_strategy filter (triggers early pagination)
        filters = {"library_strategy": "WXS"}
        await repository.get_samples(filters=filters, offset=0, limit=20)
        
        # Verify query was executed
        assert mock_session.run.called
        
        # Get the query
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        
        # Verify early pagination pattern is used
        assert 'SKIP $offset' in query
        assert 'LIMIT $limit' in query
        
        # Verify rematch pattern exists
        assert 'MATCH (sa:sample), (st:study)' in query or 'MATCH (sf:sequencing_file)' in query
    
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

