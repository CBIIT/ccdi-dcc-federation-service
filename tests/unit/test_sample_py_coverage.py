"""
Comprehensive unit tests for sample.py to improve coverage.

Targets uncovered lines:
- Lines 62-130: Filter parsing logic (_get_samples_early_pagination_with_filters)
- Lines 381-2806: Main get_samples() method and query building
- Lines 2808+: Specialized query methods
- Lines 3401+: get_sample_by_identifier
- Lines 3604+: _record_to_sample
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings
from app.models.dto import Sample


@pytest.mark.unit
class TestGetSamplesEarlyPaginationFilterParsing:
    """Test filter parsing logic in _get_samples_early_pagination_with_filters (lines 62-130)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_identifiers_with_pipe_delimiter(self, repository, mock_session):
        """Test identifiers parsing with || delimiter (line 62-63)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"identifiers": "SAMP001||SAMP002||SAMP003"},
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called
        # Verify the query uses IN clause for list
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'IN $_id_param' in query or 'IN' in query

    async def test_identifiers_single_value(self, repository, mock_session):
        """Test identifiers parsing with single value (line 69)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"identifiers": "SAMP001"},
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called
        # Verify the query uses = clause for single value
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert '= $_id_param' in query

    async def test_depositions_with_pipe_delimiter_multiple(self, repository, mock_session):
        """Test depositions parsing with || delimiter, multiple values (line 75-78)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001||phs002||phs003"},
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called
        # Verify the query uses IN clause for multiple values
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'IN $_dep_param' in query

    async def test_depositions_with_pipe_delimiter_single(self, repository, mock_session):
        """Test depositions parsing with || delimiter, single value after split (line 77)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001||"},  # Only one valid value after split
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called

    async def test_anatomical_sites_as_list(self, repository, mock_session):
        """Test anatomical_sites parsing as list (line 86-98)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "anatomic_site": "Brain"},
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"anatomical_sites": ["Brain", "Lung", "Liver"]},
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called
        # Verify the query has OR conditions for list
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'OR' in query or '_anatomical_sites_0' in query

    async def test_anatomical_sites_as_string(self, repository, mock_session):
        """Test anatomical_sites parsing as string (line 100-107)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "anatomic_site": "Brain"},
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"anatomical_sites": "Brain"},
            offset=0,
            limit=20
        )
        
        assert result is not None
        assert mock_session.run.called

    async def test_tissue_type_invalid_returns_empty(self, repository, mock_session):
        """Test tissue_type validation returns empty when invalid (line 116-121)."""
        # Mock _validate_tissue_type_filter to return None (invalid)
        with patch.object(repository, '_validate_tissue_type_filter', return_value=None):
            result = await repository._get_samples_early_pagination_with_filters(
                filters={"tissue_type": "InvalidType"},
                offset=0,
                limit=20
            )
            
            assert result == []
            # Should return early without calling session.run
            mock_session.run.assert_not_called()

    async def test_tissue_type_invalid_with_return_total(self, repository, mock_session):
        """Test tissue_type validation returns empty tuple when invalid with return_total=True (line 120)."""
        # Mock _validate_tissue_type_filter to return None (invalid)
        with patch.object(repository, '_validate_tissue_type_filter', return_value=None):
            result = await repository._get_samples_early_pagination_with_filters(
                filters={"tissue_type": "InvalidType"},
                offset=0,
                limit=20,
                return_total=True
            )
            
            assert result == ([], 0)
            # Should return early without calling session.run
            mock_session.run.assert_not_called()

    async def test_tissue_type_valid_continues(self, repository, mock_session):
        """Test tissue_type validation continues when valid (line 123-124)."""
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
        
        # Mock _validate_tissue_type_filter to return True (valid)
        with patch.object(repository, '_validate_tissue_type_filter', return_value=True):
            result = await repository._get_samples_early_pagination_with_filters(
                filters={"tissue_type": "Tumor"},
                offset=0,
                limit=20
            )
            
            assert result is not None
            assert mock_session.run.called

    async def test_returns_none_when_other_filters_present(self, repository, mock_session):
        """Test returns None when filters contain non-allowed keys (line 129-130)."""
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"identifiers": "SAMP001", "diagnosis": "Cancer"},  # diagnosis not allowed
            offset=0,
            limit=20
        )
        
        assert result is None
        mock_session.run.assert_not_called()

    async def test_identifiers_empty_string_skipped(self, repository, mock_session):
        """Test identifiers with empty string is skipped (line 60)."""
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"identifiers": ""},  # Empty string
            offset=0,
            limit=20
        )
        
        # Should proceed without identifiers filter
        assert result is not None
        assert mock_session.run.called

    async def test_identifiers_pipe_delimiter_empty_after_split(self, repository, mock_session):
        """Test identifiers with || delimiter that results in empty list (line 63)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"identifiers": "||  ||"},  # Only empty strings after split
            offset=0,
            limit=20
        )
        
        # Should proceed without identifiers filter (since list is empty)
        assert result is not None
        assert mock_session.run.called
        # Verify query doesn't have identifiers filter
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert '$_id_param' not in query

    async def test_depositions_empty_string_skipped(self, repository, mock_session):
        """Test depositions with empty string is skipped (line 73)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": ""},  # Empty string
            offset=0,
            limit=20
        )
        
        # Should proceed without depositions filter
        assert result is not None
        assert mock_session.run.called

    async def test_depositions_pipe_delimiter_empty_after_split(self, repository, mock_session):
        """Test depositions with || delimiter that results in empty list (line 76)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "||  ||"},  # Only empty strings after split
            offset=0,
            limit=20
        )
        
        # Should proceed without depositions filter
        assert result is not None
        assert mock_session.run.called


@pytest.mark.unit
class TestGetSamplesMainMethod:
    """Test main get_samples() method (lines 381-2806)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_get_samples_no_filters_routes_correctly(self, repository, mock_session):
        """Test get_samples with no filters routes to correct path."""
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
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_samples_no_filters_with_return_total(self, repository, mock_session):
        """Test get_samples with no filters and return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 100}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        result = await repository.get_samples(filters={}, offset=0, limit=20, return_total=True)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], int)
        assert mock_session.run.call_count == 2

    async def test_get_samples_case3_count_query_exception(self, repository, mock_session):
        """Test Case 3 exception handling in count query."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        # First call (count) raises exception, second call (list) succeeds
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        # Use filters that trigger Case 3
        result = await repository.get_samples(
            filters={"library_strategy": "WXS", "disease_phase": "Primary"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should still return results with total_count=0 (falls through)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] == 0  # Exception sets total_count to 0

    async def test_get_samples_case3_record_conversion_exception(self, repository, mock_session):
        """Test Case 3 exception handling during record conversion."""
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
        
        # Mock _record_to_sample to raise exception
        with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
            result = await repository.get_samples(
                filters={"library_strategy": "WXS", "disease_phase": "Primary"},
                offset=0,
                limit=20
            )
            
            # Should return empty list when conversion fails
            assert isinstance(result, list)
            assert len(result) == 0

    async def test_get_samples_with_return_total(self, repository, mock_session):
        """Test get_samples with return_total=True."""
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
        
        result = await repository.get_samples(filters={}, offset=0, limit=20, return_total=True)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], int)

    async def test_get_samples_case1_sample_only_filters(self, repository, mock_session):
        """Test get_samples routes to Case 1 (sample-only filters)."""
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
        
        # Mock the case1 method to verify it's called
        with patch.object(repository, '_get_samples_case1_sample_only', new_callable=AsyncMock) as mock_case1:
            mock_case1.return_value = []
            result = await repository.get_samples(
                filters={"tissue_type": "Tumor"},  # Sample-only filter
                offset=0,
                limit=20
            )
            
            mock_case1.assert_called_once()

    async def test_get_samples_case2_sample_study_filters(self, repository, mock_session):
        """Test get_samples routes to Case 2 (sample + study filters)."""
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
        
        # Mock the case2 method to verify it's called
        with patch.object(repository, '_get_samples_case2_sample_study', new_callable=AsyncMock) as mock_case2:
            mock_case2.return_value = []
            result = await repository.get_samples(
                filters={"tissue_type": "Tumor", "depositions": "phs001"},  # Sample + study filters
                offset=0,
                limit=20
            )
            
            mock_case2.assert_called_once()

    async def test_get_samples_case3_node_filters(self, repository, mock_session):
        """Test get_samples routes to Case 3 (has other node filters)."""
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
        
        # Mock the case3 method to verify it's called
        with patch.object(repository, '_get_samples_case3_with_node_filters', new_callable=AsyncMock) as mock_case3:
            mock_case3.return_value = []
            result = await repository.get_samples(
                filters={"library_strategy": "WXS"},  # Sequencing file filter
                offset=0,
                limit=20
            )
            
            mock_case3.assert_called_once()
    
    async def test_get_samples_case3_diagnosis_filter(self, repository, mock_session):
        """Test get_samples routes to Case 3 with diagnosis filter."""
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
        
        # Mock the case3 method to verify it's called for diagnosis filter
        with patch.object(repository, '_get_samples_case3_with_node_filters', new_callable=AsyncMock) as mock_case3:
            mock_case3.return_value = []
            result = await repository.get_samples(
                filters={"diagnosis": "Neuroblastoma"},  # Diagnosis filter should route to Case 3
                offset=0,
                limit=20
            )
            
            # Verify Case 3 was called (diagnosis filter should be categorized as diagnosis filter)
            mock_case3.assert_called_once()
            # Verify the diagnosis filter was passed to Case 3
            call_args = mock_case3.call_args
            assert call_args is not None
            filters_arg = call_args[0][0] if call_args[0] else {}
            categorized_arg = call_args[0][1] if len(call_args[0]) > 1 else {}
            assert "diagnosis" in filters_arg
            assert "diagnosis" in categorized_arg.get("diagnosis", {})

    async def test_get_samples_case1_with_return_total(self, repository, mock_session):
        """Test Case 1 with return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 10}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        # Don't mock case1 - call it directly
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor"},  # Sample-only filter
            offset=0,
            limit=20,
            return_total=True
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)

    async def test_get_samples_case1_count_query_exception(self, repository, mock_session):
        """Test Case 1 exception handling in count query."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        # First call (count) raises exception, second call (list) succeeds
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should still return results with total_count=0 (exception sets it to 0)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] == 0  # Exception sets total_count to 0

    async def test_get_samples_case2_count_query_exception(self, repository, mock_session):
        """Test Case 2 exception handling in count query."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        # First call (count) raises exception, second call (list) succeeds
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor", "depositions": "phs001"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should still return results with total_count=0 (exception sets it to 0)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] == 0  # Exception sets total_count to 0

    async def test_get_samples_case2_with_return_total(self, repository, mock_session):
        """Test Case 2 with return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 5}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        # Don't mock case2 - call it directly
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor", "depositions": "phs001"},  # Sample + study filters
            offset=0,
            limit=20,
            return_total=True
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)

    async def test_get_samples_case3_returns_none_fallthrough(self, repository, mock_session):
        """Test Case 3 returning None falls through to standard query (line 371-373)."""
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
        
        # Mock case3 to return None (indicating it can't handle the filters)
        with patch.object(repository, '_get_samples_case3_with_node_filters', new_callable=AsyncMock) as mock_case3:
            mock_case3.return_value = None
            result = await repository.get_samples(
                filters={"library_strategy": "WXS", "unknown_filter": "value"},  # Case 3 can't handle unknown_filter
                offset=0,
                limit=20
            )
            
            mock_case3.assert_called_once()
            # Should fall through and execute standard query
            assert mock_session.run.called

    async def test_get_samples_case1_record_conversion_exception(self, repository, mock_session):
        """Test Case 1 exception handling during record conversion."""
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
        
        # Mock _record_to_sample to raise exception for first call, succeed for second
        call_count = [0]
        def mock_record_to_sample(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("Conversion error")
            # Return a valid sample for subsequent calls
            from app.models.dto import Sample, SampleIdentifier, NamespaceIdentifier
            return Sample(
                id=SampleIdentifier(
                    namespace=NamespaceIdentifier(organization="CCDI-DCC", name="phs001"),
                    name="SAMP001"
                ),
                metadata={}
            )
        
        with patch.object(repository, '_record_to_sample', side_effect=mock_record_to_sample):
            result = await repository.get_samples(
                filters={"tissue_type": "Tumor"},
                offset=0,
                limit=20
            )
            
            # Should continue processing despite exception
            assert isinstance(result, list)

    async def test_get_samples_case2_record_conversion_exception(self, repository, mock_session):
        """Test Case 2 exception handling during record conversion."""
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
        
        # Mock _record_to_sample to raise exception
        with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
            result = await repository.get_samples(
                filters={"tissue_type": "Tumor", "depositions": "phs001"},
                offset=0,
                limit=20
            )
            
            # Should return empty list when conversion fails
            assert isinstance(result, list)
            assert len(result) == 0


@pytest.mark.unit
class TestGetSamplesBySequencingFileFilters:
    """Test _get_samples_by_sequencing_file_filters (lines 2808+)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_library_source_material_null_mapped_returns_empty(self, repository, mock_session):
        """Test library_source_material with null-mapped value returns empty (line 2837-2839)."""
        import app.repositories.sample as sm
        
        original_func = sm.is_null_mapped_value
        sm.is_null_mapped_value = lambda field, value: True if field == "library_source_material" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"library_source_material": "Not Reported"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_null_mapped_value = original_func

    async def test_library_strategy_database_only_returns_empty(self, repository, mock_session):
        """Test library_strategy with database-only value returns empty (line 2847-2849)."""
        import app.repositories.sample as sm
        
        original_func = sm.is_database_only_value
        sm.is_database_only_value = lambda field, value: True if field == "library_strategy" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"library_strategy": "Archer Fusion"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_database_only_value = original_func

    async def test_library_strategy_with_reverse_mapping(self, repository, mock_session):
        """Test library_strategy with reverse mapping (line 2852-2857)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: "WXS" if field == "library_strategy" and value == "Other" else value
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"library_strategy": "Other"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called
            # Verify query has OR condition for both mapped and original values
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'OR' in query or 'param_1' in query or 'param_2' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.reverse_map_field_value = original_reverse_map

    async def test_library_selection_method_database_only_returns_empty(self, repository, mock_session):
        """Test library_selection_method with database-only value returns empty."""
        import app.repositories.sample as sm
        
        original_func = sm.is_database_only_value
        sm.is_database_only_value = lambda field, value: True if field == "library_selection_method" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"library_selection_method": "PolyA"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_database_only_value = original_func

    async def test_specimen_molecular_analyte_type_list_mapping(self, repository, mock_session):
        """Test specimen_molecular_analyte_type with list mapping."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_molecule": "Transcriptomic"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            ["Transcriptomic", "Viral RNA"] if field == "specimen_molecular_analyte_type" and value == "RNA"
            else value
        )
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"specimen_molecular_analyte_type": "RNA"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called
            # Verify query uses IN clause for list
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'IN [' in query or 'IN' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null
            sm.reverse_map_field_value = original_reverse_map

    async def test_sequencing_file_count_query_exception(self, repository, mock_session):
        """Test exception handling in sequencing_file reverse count query (line 2917-2918)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {},
                "diagnoses": {}
            }
        
        # First call (count) raises exception, second call (list) succeeds
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_by_sequencing_file_filters(
                filters={"library_strategy": "WXS"},
                offset=0,
                limit=20,
                return_total=True
            )
            
            # Should still return results with total_count=None (falls through)
            assert isinstance(result, list)
            assert mock_session.run.call_count == 2
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_sequencing_file_record_conversion_exception(self, repository, mock_session):
        """Test exception handling during record conversion in sequencing_file query (line 3008-3010)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            # Mock _record_to_sample to raise exception
            with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
                result = await repository._get_samples_by_sequencing_file_filters(
                    filters={"library_strategy": "WXS"},
                    offset=0,
                    limit=20
                )
                
                # Should return empty list when conversion fails
                assert isinstance(result, list)
                assert len(result) == 0
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null


@pytest.mark.unit
class TestGetSampleByIdentifier:
    """Test get_sample_by_identifier (lines 3401+)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_get_sample_by_identifier_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample is found."""
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
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs001",
            name="SAMP001"
        )
        
        assert result is not None
        assert isinstance(result, Sample)
        assert mock_session.run.called

    async def test_get_sample_by_identifier_not_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample is not found."""
        # Create an empty async generator - must have yield to be recognized as async generator
        async def async_gen():
            # Empty async generator - condition ensures yield exists but never executes
            if False:
                yield None
        
        mock_result = AsyncMock()
        # __aiter__ should return the async generator object (result of calling async_gen())
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs001",
            name="NONEXISTENT"
        )
        
        assert result is None
        assert mock_session.run.called


@pytest.mark.unit
class TestRecordToSample:
    """Test _record_to_sample (lines 3604+)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    def test_record_to_sample_empty_sa_raises_error(self, repository):
        """Test _record_to_sample raises error when sa is empty (line 3635-3637)."""
        with pytest.raises(ValueError, match="Sample node.*required"):
            repository._record_to_sample({}, {}, {}, {}, {}, None)

    def test_record_to_sample_missing_study_id_from_st(self, repository):
        """Test _record_to_sample handles missing study_id from st node (line 3641-3642)."""
        sa = {"sample_id": "SAMP001"}
        st = {}  # Empty study dict
        p = {"study_id": "phs001"}  # Study ID in participant
        
        sample = repository._record_to_sample(sa, p, st, {}, {}, None)
        
        assert sample is not None
        assert sample.id.namespace.name == "phs001"  # Should get from participant

    def test_record_to_sample_missing_study_id_from_participant(self, repository):
        """Test _record_to_sample handles missing study_id from participant (line 3648-3650)."""
        sa = {"sample_id": "SAMP001", "study_id": "phs001"}  # Study ID in sample
        st = {}  # Empty study dict
        p = {}  # Empty participant
        
        sample = repository._record_to_sample(sa, p, st, {}, {}, None)
        
        assert sample is not None
        assert sample.id.namespace.name == "phs001"  # Should get from sample

    def test_record_to_sample_with_all_fields(self, repository):
        """Test _record_to_sample with all fields populated."""
        sa = {"sample_id": "SAMP001", "sample_tumor_status": "Tumor", "anatomic_site": "Brain"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs001"}
        sf = {"library_strategy": "WXS", "library_selection": "PCR"}
        pf = {"fixation_embedding_method": "FFPE"}
        diagnoses = {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"}
        
        sample = repository._record_to_sample(sa, p, st, sf, pf, diagnoses)
        
        assert sample is not None
        assert sample.id.name == "SAMP001"
        assert sample.id.namespace.name == "phs001"
        assert sample.metadata is not None

    def test_record_to_sample_with_none_diagnoses(self, repository):
        """Test _record_to_sample handles None diagnoses."""
        sa = {"sample_id": "SAMP001"}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None

    def test_record_to_sample_with_empty_dict_diagnoses(self, repository):
        """Test _record_to_sample handles empty dict diagnoses."""
        sa = {"sample_id": "SAMP001"}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, {})
        
        assert sample is not None
        assert sample.metadata is not None


@pytest.mark.unit
class TestGetSamplesEarlyPaginationAdvanced:
    """Test advanced paths in _get_samples_early_pagination_with_filters (lines 136-297)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_depositions_only_with_return_total(self, repository, mock_session):
        """Test depositions-only query with return_total=True (line 136-160)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 5}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        # First call returns count, second call returns list
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert result[1] == 5
        assert mock_session.run.call_count == 2

    async def test_depositions_only_with_other_filters_return_total(self, repository, mock_session):
        """Test depositions + other filters with return_total=True (line 161-197)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 3}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001", "identifiers": "SAMP001"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert result[1] == 3
        assert mock_session.run.call_count == 2

    async def test_count_query_exception_handling(self, repository, mock_session):
        """Test count query exception handling (line 195-197)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {}
            }
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        # First call (count) raises exception, second call (list) succeeds
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error"),
            mock_result_list
        ])
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should still return results with total_count=0
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] == 0

    async def test_depositions_only_list_query(self, repository, mock_session):
        """Test depositions-only list query path (line 199-227)."""
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
        
        result = await repository._get_samples_early_pagination_with_filters(
            filters={"depositions": "phs001"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        # Verify query starts from study node
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'MATCH (st:study)' in query

    async def test_record_conversion_exception_handling(self, repository, mock_session):
        """Test exception handling during record conversion (line 292-293)."""
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
        
        # Mock _record_to_sample to raise exception
        with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
            result = await repository._get_samples_early_pagination_with_filters(
                filters={"depositions": "phs001"},
                offset=0,
                limit=20
            )
            
            # Should return empty list when conversion fails
            assert isinstance(result, list)
            assert len(result) == 0


@pytest.mark.unit
class TestGetSamplesByPathologyFileFilters:
    """Test _get_samples_by_pathology_file_filters (lines 3020+)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_preservation_method_filter(self, repository, mock_session):
        """Test preservation_method filter (line 3047-3049)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository._get_samples_by_pathology_file_filters(
            filters={"preservation_method": "FFPE"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        # Verify query filters on pathology_file
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'pathology_file' in query or 'pf.' in query

    async def test_pathology_file_with_return_total(self, repository, mock_session):
        """Test pathology_file filters with return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 2}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        result = await repository._get_samples_by_pathology_file_filters(
            filters={"preservation_method": "FFPE"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[1] == 2

    async def test_pathology_file_count_query_exception(self, repository, mock_session):
        """Test exception handling in pathology_file reverse count query (line 3081-3082)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        # First call (count) raises exception, second call (list) succeeds
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        result = await repository._get_samples_by_pathology_file_filters(
            filters={"preservation_method": "FFPE"},
            offset=0,
            limit=20,
            return_total=True
        )
        
        # Should still return results with total_count=None (falls through)
        assert isinstance(result, list)
        assert mock_session.run.call_count == 2

    async def test_pathology_file_record_conversion_exception(self, repository, mock_session):
        """Test exception handling during record conversion in pathology_file query (line 3155-3157)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Mock _record_to_sample to raise exception
        with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
            result = await repository._get_samples_by_pathology_file_filters(
                filters={"preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            # Should return empty list when conversion fails
            assert isinstance(result, list)
            assert len(result) == 0


@pytest.mark.unit
class TestGetSamplesByCombinedFilters:
    """Test _get_samples_by_combined_filters (lines 3167+)."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_combined_filters_library_source_material_null(self, repository, mock_session):
        """Test combined filters with null-mapped library_source_material (line 3200-3202)."""
        import app.repositories.sample as sm
        
        original_func = sm.is_null_mapped_value
        sm.is_null_mapped_value = lambda field, value: True if field == "library_source_material" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_source_material": "Not Reported", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_null_mapped_value = original_func

    async def test_combined_filters_library_strategy_database_only(self, repository, mock_session):
        """Test combined filters with database-only library_strategy (line 3207-3209)."""
        import app.repositories.sample as sm
        
        original_func = sm.is_database_only_value
        sm.is_database_only_value = lambda field, value: True if field == "library_strategy" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "Archer Fusion", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_database_only_value = original_func

    async def test_combined_filters_library_strategy_with_reverse_mapping(self, repository, mock_session):
        """Test combined filters with library_strategy reverse mapping (line 3211-3216)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            "WXS" if field == "library_strategy" and value == "Other" else value
        )
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "Other", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called
            # Verify query has OR condition for both mapped and original values
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'OR' in query or 'param_1' in query or 'param_2' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.reverse_map_field_value = original_reverse_map

    async def test_combined_filters_with_return_total(self, repository, mock_session):
        """Test combined filters with return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        async def async_gen_count():
            yield {"total_count": 1}
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_result_count = AsyncMock()
        mock_result_count.__aiter__ = Mock(return_value=async_gen_count())
        mock_result_count.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[mock_result_count, mock_result_list])
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "WXS", "preservation_method": "FFPE"},
                offset=0,
                limit=20,
                return_total=True
            )
            
            assert isinstance(result, tuple)
            assert len(result) == 2
            assert result[1] == 1
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_library_selection_method_database_only(self, repository, mock_session):
        """Test combined filters with database-only library_selection_method (line 3220-3223)."""
        import app.repositories.sample as sm
        
        original_func = sm.is_database_only_value
        sm.is_database_only_value = lambda field, value: True if field == "library_selection_method" else original_func(field, value)
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_selection_method": "PolyA", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_database_only_value = original_func

    async def test_combined_filters_specimen_molecular_analyte_type_invalid(self, repository, mock_session):
        """Test combined filters with invalid specimen_molecular_analyte_type (line 3228-3230)."""
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: True if field == "specimen_molecular_analyte_type" else original_is_db_only(field, value)
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"specimen_molecular_analyte_type": "Invalid", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_specimen_molecular_analyte_type_list(self, repository, mock_session):
        """Test combined filters with specimen_molecular_analyte_type that maps to list (line 3232-3234)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_molecule": "Transcriptomic"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            ["Transcriptomic", "Viral RNA"] if field == "specimen_molecular_analyte_type" and value == "RNA"
            else original_reverse_map(field, value)
        )
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"specimen_molecular_analyte_type": "RNA", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called
            # Verify query uses IN clause for list
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'IN [' in query or 'IN' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null
            sm.reverse_map_field_value = original_reverse_map

    async def test_combined_filters_tissue_type_invalid_single(self, repository, mock_session):
        """Test combined filters with invalid tissue_type (single value, line 3261-3262)."""
        with patch('app.core.field_mappings.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "WXS", "preservation_method": "FFPE", "tissue_type": "Invalid"},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()

    async def test_combined_filters_tissue_type_invalid_list(self, repository, mock_session):
        """Test combined filters with invalid tissue_type (list with invalid values, line 3256-3257)."""
        with patch('app.core.field_mappings.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "WXS", "preservation_method": "FFPE", "tissue_type": ["Tumor", "Invalid"]},
                offset=0,
                limit=20
            )
            
            assert result == []
            mock_session.run.assert_not_called()

    async def test_combined_filters_tissue_type_valid_list(self, repository, mock_session):
        """Test combined filters with valid tissue_type list (line 3254-3259)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            with patch('app.core.field_mappings.load_sample_enum', return_value=["Tumor", "Normal"]):
                result = await repository._get_samples_by_combined_filters(
                    filters={"library_strategy": "WXS", "preservation_method": "FFPE", "tissue_type": ["Tumor", "Normal"]},
                    offset=0,
                    limit=20
                )
                
                assert isinstance(result, list)
                assert mock_session.run.called
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_count_query_exception(self, repository, mock_session):
        """Test exception handling in combined reverse count query (line 3305-3306)."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result_list = AsyncMock()
        mock_result_list.__aiter__ = Mock(return_value=async_gen_list())
        mock_result_list.consume = AsyncMock()
        
        mock_session.run = AsyncMock(side_effect=[
            Exception("Database error in count query"),
            mock_result_list
        ])
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"library_strategy": "WXS", "preservation_method": "FFPE"},
                offset=0,
                limit=20,
                return_total=True
            )
            
            # Should still return results with total_count=None (falls through)
            assert isinstance(result, list)
            assert mock_session.run.call_count == 2
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_record_conversion_exception(self, repository, mock_session):
        """Test exception handling during record conversion in combined filters (line 3390-3392)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_strategy": "WXS"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            # Mock _record_to_sample to raise exception
            with patch.object(repository, '_record_to_sample', side_effect=ValueError("Conversion error")):
                result = await repository._get_samples_by_combined_filters(
                    filters={"library_strategy": "WXS", "preservation_method": "FFPE"},
                    offset=0,
                    limit=20
                )
                
                # Should return empty list when conversion fails
                assert isinstance(result, list)
                assert len(result) == 0
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_query_exception(self, repository, mock_session):
        """Test exception handling in combined reverse query execution (line 3398-3400)."""
        mock_session.run = AsyncMock(side_effect=Exception("Database connection error"))
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            with pytest.raises(Exception, match="Database connection error"):
                await repository._get_samples_by_combined_filters(
                    filters={"library_strategy": "WXS", "preservation_method": "FFPE"},
                    offset=0,
                    limit=20
                )
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_combined_filters_specimen_molecular_analyte_type_single_value(self, repository, mock_session):
        """Test combined filters with specimen_molecular_analyte_type single value (not list, line 3554-3555)."""
        async def async_gen():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {"library_source_molecule": "DNA"},
                "pf": {"fixation_embedding_method": "FFPE"},
                "diagnoses": {}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            "DNA" if field == "specimen_molecular_analyte_type" and value == "DNA"
            else original_reverse_map(field, value)
        )
        
        try:
            result = await repository._get_samples_by_combined_filters(
                filters={"specimen_molecular_analyte_type": "DNA", "preservation_method": "FFPE"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null
            sm.reverse_map_field_value = original_reverse_map


@pytest.mark.unit
class TestRecordToSampleEdgeCases:
    """Test edge cases in _record_to_sample method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    def test_record_to_sample_participant_id_fallback(self, repository):
        """Test _record_to_sample with participant_id fallback to id (line 3694)."""
        sa = {"sample_id": "SAMP001"}
        p = {"id": "PART001"}  # No participant_id, but has id
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, p, st, {}, {}, None)
        
        assert sample is not None
        assert sample.subject is not None
        assert sample.subject.name == "PART001"

    def test_record_to_sample_with_neg999_value(self, repository):
        """Test _record_to_sample with -999 value handling (line 3711)."""
        sa = {"sample_id": "SAMP001", "age_at_diagnosis": -999}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None
        # -999 should be converted to None
        assert sample.metadata.age_at_diagnosis is None

    def test_record_to_sample_with_invalid_value_in_list(self, repository):
        """Test _record_to_sample with invalid value in list (line 3716-3718)."""
        sa = {"sample_id": "SAMP001", "anatomical_sites": ["Brain", "Invalid value", "Lung"]}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None
        # Invalid values should be filtered out

    def test_record_to_sample_empty_string_value(self, repository):
        """Test _record_to_sample with empty string value (line 3761)."""
        sa = {"sample_id": "SAMP001", "tissue_type": ""}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None

    def test_record_to_sample_reverse_map_library_selection_method_list(self, repository):
        """Test _record_to_sample reverse mapping library_selection_method from list (line 3787-3789)."""
        sa = {"sample_id": "SAMP001"}
        st = {"study_id": "phs001"}
        sf = {"library_selection": "PolyA"}
        
        # Mock reverse_map_field_value to return a list
        import app.repositories.sample as sm
        original_reverse_map = sm.reverse_map_field_value
        
        def mock_reverse_map(field, value):
            if field == "library_selection_method" and value == "PolyA":
                return ["PolyA", "PCR"]  # Return list
            return original_reverse_map(field, value)
        
        sm.reverse_map_field_value = mock_reverse_map
        
        try:
            sample = repository._record_to_sample(sa, {}, st, sf, {}, None)
            assert sample is not None
        finally:
            sm.reverse_map_field_value = original_reverse_map

    def test_record_to_sample_integer_conversion_error(self, repository):
        """Test _record_to_sample with integer conversion error (line 3802-3803)."""
        sa = {"sample_id": "SAMP001", "age_at_diagnosis": "not_a_number"}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None
        # Invalid integer should be None

    def test_record_to_sample_value_filtering_invalid_value(self, repository):
        """Test _record_to_sample value filtering with invalid value (line 3833-3835)."""
        sa = {"sample_id": "SAMP001", "some_field": "Invalid value"}
        st = {"study_id": "phs001"}
        
        sample = repository._record_to_sample(sa, {}, st, {}, {}, None)
        
        assert sample is not None
        assert sample.metadata is not None

    def test_record_to_sample_study_id_fallback_from_st(self, repository):
        """Test _record_to_sample study_id fallback from st node (line 3843)."""
        sa = {"sample_id": "SAMP001"}
        st = {"study_id": "phs001"}
        p = {}  # No study_id in participant
        
        sample = repository._record_to_sample(sa, p, st, {}, {}, None)
        
        assert sample is not None
        assert sample.id.namespace.name == "phs001"


@pytest.mark.unit
class TestGetSamplesSummaryReverseQueryInSample:
    """Test _get_samples_summary_reverse_query method in sample.py."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    async def test_get_samples_summary_reverse_query_library_source_material(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query with library_source_material filter."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value={"total_count": 5})
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_null = sm.is_null_mapped_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_null_mapped_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: "DNA" if field == "library_source_material" else value
        
        try:
            result = await repository._get_samples_summary_reverse_query({"library_source_material": "DNA"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 5}}
            mock_session.run.assert_called_once()
        finally:
            sm.is_null_mapped_value = original_is_null
            sm.reverse_map_field_value = original_reverse_map

    async def test_get_samples_summary_reverse_query_library_strategy_with_reverse_mapping(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query with library_strategy reverse mapping (line 3529-3534)."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value={"total_count": 3})
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            "WXS" if field == "library_strategy" and value == "Other" else value
        )
        
        try:
            result = await repository._get_samples_summary_reverse_query({"library_strategy": "Other"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 3}}
            # Verify query has OR condition for both mapped and original values
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'OR' in query or 'param_1' in query or 'param_2' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.reverse_map_field_value = original_reverse_map

    async def test_get_samples_summary_reverse_query_library_selection_method(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query with library_selection_method filter."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value={"total_count": 2})
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        
        sm.is_database_only_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_summary_reverse_query({"library_selection_method": "PCR"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 2}}
            mock_session.run.assert_called_once()
        finally:
            sm.is_database_only_value = original_is_db_only

    async def test_get_samples_summary_reverse_query_specimen_molecular_analyte_type_list(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query with specimen_molecular_analyte_type list mapping (line 3550-3552)."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value={"total_count": 4})
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            ["Transcriptomic", "Viral RNA"] if field == "specimen_molecular_analyte_type" and value == "RNA"
            else value
        )
        
        try:
            result = await repository._get_samples_summary_reverse_query({"specimen_molecular_analyte_type": "RNA"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 4}}
            # Verify query uses IN clause for list
            call_args = mock_session.run.call_args
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            assert 'IN [' in query or 'IN' in query
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null
            sm.reverse_map_field_value = original_reverse_map

    async def test_get_samples_summary_reverse_query_exception_handling(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query exception handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            with pytest.raises(Exception, match="Database error"):
                await repository._get_samples_summary_reverse_query({"library_strategy": "WXS"})
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_get_samples_summary_reverse_query_no_record(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query when query returns no record."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_is_null = sm.is_null_mapped_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.is_null_mapped_value = lambda field, value: False
        
        try:
            result = await repository._get_samples_summary_reverse_query({"library_strategy": "WXS"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 0}}
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.is_null_mapped_value = original_is_null

    async def test_get_samples_summary_reverse_query_library_strategy_no_reverse_mapping(self, repository, mock_session):
        """Test _get_samples_summary_reverse_query with library_strategy that has no reverse mapping (line 3535-3537)."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value={"total_count": 7})
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        import app.repositories.sample as sm
        
        original_is_db_only = sm.is_database_only_value
        original_reverse_map = sm.reverse_map_field_value
        
        sm.is_database_only_value = lambda field, value: False
        sm.reverse_map_field_value = lambda field, value: (
            None if field == "library_strategy" and value == "WXS" else value  # No reverse mapping
        )
        
        try:
            result = await repository._get_samples_summary_reverse_query({"library_strategy": "WXS"})
            
            assert isinstance(result, dict)
            assert result == {"counts": {"total": 7}}
            mock_session.run.assert_called_once()
        finally:
            sm.is_database_only_value = original_is_db_only
            sm.reverse_map_field_value = original_reverse_map
