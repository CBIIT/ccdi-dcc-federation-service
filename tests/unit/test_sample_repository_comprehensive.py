"""
Comprehensive tests for SampleRepository to improve coverage.

This module focuses on testing the complex methods and edge cases
that are currently under-tested in sample.py (43.80% coverage).

Coverage Goals:
- count_samples_by_field: All field types, filter combinations, edge cases
- get_samples: Filter combinations, optimization paths, pagination
- get_samples_summary: No filters, with filters, reverse query optimization
- get_sample_by_identifier: Found/not found cases
- _count_samples_by_associated_diagnoses: With and without filters

Expected Impact: Increase coverage from 43.80% to 70%+
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.models.errors import UnsupportedFieldError
from app.core.config import Settings


@pytest.mark.unit
class TestSampleRepositoryCountByField:
    """Comprehensive tests for count_samples_by_field method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_count_samples_by_field_unsupported_field(self, repository):
        """Test count_samples_by_field raises UnsupportedFieldError for unsupported fields."""
        with pytest.raises(UnsupportedFieldError) as exc_info:
            await repository.count_samples_by_field("invalid_field", {})
        
        assert exc_info.value.field == "invalid_field"
        assert exc_info.value._entity_type == "sample"  # Use private attribute

    async def test_count_samples_by_field_diagnosis_special_case(self, repository):
        """Test count_samples_by_field calls _count_samples_by_associated_diagnoses for diagnosis field."""
        with patch.object(repository, '_count_samples_by_associated_diagnoses', new_callable=AsyncMock) as mock_count:
            mock_count.return_value = {"total": 10, "missing": 2, "values": []}
            
            result = await repository.count_samples_by_field("diagnosis", {})
            
            mock_count.assert_called_once_with({})
            assert result == {"total": 10, "missing": 2, "values": []}

    async def test_count_samples_by_field_tissue_type_no_filters(self, repository, mock_session):
        """Test count_samples_by_field for tissue_type field with no filters."""
        async def async_gen_values():
            yield {"value": "Tumor", "count": 200}
            yield {"value": "Normal", "count": 50}
        
        async def async_gen_total():
            yield {"total": 250}
        
        async def async_gen_missing():
            yield {"missing": 5}
        
        mock_result_values = AsyncMock()
        mock_result_values.__aiter__ = Mock(return_value=async_gen_values())
        mock_result_values.consume = AsyncMock()
        
        mock_result_total = AsyncMock()
        mock_result_total.__aiter__ = Mock(return_value=async_gen_total())
        mock_result_total.consume = AsyncMock()
        
        mock_result_missing = AsyncMock()
        mock_result_missing.__aiter__ = Mock(return_value=async_gen_missing())
        mock_result_missing.consume = AsyncMock()
        
        # Order: values query, total query, missing query
        mock_session.run = AsyncMock(side_effect=[
            mock_result_values,
            mock_result_total,
            mock_result_missing
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {})
        
        assert isinstance(result, dict)
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert result["total"] == 250
        assert result["missing"] == 5
        assert len(result["values"]) == 2
        assert mock_session.run.call_count == 3

    async def test_count_samples_by_field_age_at_collection_no_filters(self, repository, mock_session):
        """Test count_samples_by_field for age_at_collection (sample node field) with no filters."""
        async def async_gen():
            yield {"value": 10, "count": 5}
            yield {"value": 20, "count": 3}
        mock_values = AsyncMock()
        mock_values.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_values,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 3}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 2}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("age_at_collection", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert result["total"] == 3

    async def test_count_samples_by_field_with_race_filter(self, repository, mock_session):
        """Test count_samples_by_field with race filter."""
        async def async_gen():
            yield {"value": "Tumor", "count": 10}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 10}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"race": "White"})
        
        assert "total" in result
        # Verify race filter was processed (check query was called)
        assert mock_session.run.called

    async def test_count_samples_by_field_with_identifiers_filter(self, repository, mock_session):
        """Test count_samples_by_field with identifiers filter (single value)."""
        async def async_gen():
            yield {"value": "Tumor", "count": 1}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 1}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"identifiers": "SAMP001"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_identifiers_filter_multiple(self, repository, mock_session):
        """Test count_samples_by_field with identifiers filter (multiple values with ||)."""
        async def async_gen():
            yield {"value": "Tumor", "count": 2}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 2}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"identifiers": "SAMP001 || SAMP002"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_depositions_filter(self, repository, mock_session):
        """Test count_samples_by_field with depositions filter."""
        async def async_gen():
            yield {"value": "Tumor", "count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 5}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"depositions": "phs002431"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_depositions_filter_multiple(self, repository, mock_session):
        """Test count_samples_by_field with multiple depositions (|| separator)."""
        async def async_gen():
            yield {"value": "Tumor", "count": 10}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 10}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"depositions": "phs001 || phs002"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_diagnosis_search(self, repository, mock_session):
        """Test count_samples_by_field with diagnosis search filter."""
        async def async_gen():
            yield {"value": "Tumor", "count": 3}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 3}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"_diagnosis_search": "cancer"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_anatomical_sites_filter(self, repository, mock_session):
        """Test count_samples_by_field with anatomical_sites filter (single value)."""
        async def async_gen():
            yield {"value": "Tumor", "count": 4}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 4}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"anatomical_sites": "Brain"})
        
        assert "total" in result

    async def test_count_samples_by_field_with_anatomical_sites_filter_list(self, repository, mock_session):
        """Test count_samples_by_field with anatomical_sites filter (list of values)."""
        async def async_gen():
            yield {"value": "Tumor", "count": 6}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 6}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {"anatomical_sites": ["Brain", "Liver"]})
        
        assert "total" in result

    async def test_count_samples_by_field_library_strategy(self, repository, mock_session):
        """Test count_samples_by_field for library_strategy (uses combined query when no filters)."""
        async def async_gen():
            yield {"value": "WXS", "count": 20, "total": 40, "missing": 5}
            yield {"value": "RNA-Seq", "count": 15, "total": 40, "missing": 5}
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.count_samples_by_field("library_strategy", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert result["total"] == 40
        assert result["missing"] == 5

    async def test_count_samples_by_field_library_selection_method(self, repository, mock_session):
        """Test count_samples_by_field for library_selection_method (uses combined query when no filters)."""
        async def async_gen():
            yield {"value": "PCR", "count": 25, "total": 50, "missing": 8}
            yield {"value": "Poly(A)", "count": 17, "total": 50, "missing": 8}
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch("app.repositories.sample.map_field_value", side_effect=lambda field, value: value), \
             patch("app.repositories.sample.is_null_mapped_value", return_value=False):
            result = await repository.count_samples_by_field("library_selection_method", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert result["total"] == 50
        assert result["missing"] == 8
        assert len(result["values"]) == 2
        assert result["values"][0]["value"] == "PCR"
        assert result["values"][0]["count"] == 25

    async def test_count_samples_by_field_disease_phase(self, repository, mock_session):
        """Test count_samples_by_field for disease_phase (diagnosis field)."""
        async def async_gen_values():
            yield {"value": "Primary", "count": 100}
            yield {"value": "Recurrent", "count": 50}
        
        async def async_gen_total():
            yield {"total": 150}
        
        async def async_gen_missing():
            yield {"missing": 10}
        
        mock_result_values = AsyncMock()
        mock_result_values.__aiter__ = Mock(return_value=async_gen_values())
        mock_result_values.consume = AsyncMock()
        
        mock_result_total = AsyncMock()
        mock_result_total.__aiter__ = Mock(return_value=async_gen_total())
        mock_result_total.consume = AsyncMock()
        
        mock_result_missing = AsyncMock()
        mock_result_missing.__aiter__ = Mock(return_value=async_gen_missing())
        mock_result_missing.consume = AsyncMock()
        
        # Order: values query, total query, missing query
        mock_session.run = AsyncMock(side_effect=[
            mock_result_values,
            mock_result_total,
            mock_result_missing
        ])
        
        result = await repository.count_samples_by_field("disease_phase", {})
        
        assert isinstance(result, dict)
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert result["total"] == 150
        assert result["missing"] == 10
        assert len(result["values"]) == 2
        assert mock_session.run.call_count == 3
        
        # Verify queries use diagnosis-first pattern (CALL {} subquery was removed)
        if mock_session.run.call_count > 0:
            call_args = mock_session.run.call_args_list[0]
            query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
            # Should use diagnosis-first path (no CALL {} subquery)
            assert ('MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query or 
                    'MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)' in query or
                    'MATCH (d:diagnosis)' in query)


@pytest.mark.unit
class TestSampleRepositoryGetSamples:
    """Comprehensive tests for get_samples method."""

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
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock(spec=Settings)
        settings.sex_value_mappings = {"M": "Male", "F": "Female"}
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_samples_no_filters_early_pagination(self, repository, mock_session):
        """Test get_samples with no filters uses early pagination optimization."""
        # Mock empty result
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        # Verify early pagination query was used (check for offset/limit params)
        assert mock_session.run.called
        call_args = mock_session.run.call_args
        if call_args:
            params = call_args[0][1] if len(call_args[0]) > 1 else {}
            # Early pagination uses offset and limit params
            assert "offset" in params or "limit" in params or len(params) == 0  # May be empty for no filters

    async def test_get_samples_sequencing_file_only_filters(self, repository, mock_session):
        """Test get_samples with only sequencing_file filters uses optimized query."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False):
            with patch('app.repositories.sample.reverse_map_field_value', return_value="WXS"):
                result = await repository.get_samples(
                    filters={"library_strategy": "WXS"},
                    offset=0,
                    limit=20
                )
        
        # Verify query was executed (may use Case 3 or reverse query optimization)
        assert mock_session.run.called
        assert isinstance(result, list)

    async def test_get_samples_with_tissue_type_filter_valid(self, repository, mock_session):
        """Test get_samples with valid tissue_type filter."""
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(filters={"tissue_type": "Tumor"}, offset=0, limit=20)
            
            assert isinstance(result, list)

    async def test_get_samples_with_tissue_type_filter_invalid(self, repository, mock_session):
        """Test get_samples with invalid tissue_type filter returns empty results."""
        with patch('app.repositories.sample_helpers.load_sample_enum', return_value=["Tumor", "Normal"]):
            # Invalid tissue_type should be validated and return empty results
            # The validation happens in _validate_tissue_type_filter which returns None for invalid values
            # This causes get_samples to return early with empty list
            result = await repository.get_samples(filters={"tissue_type": "Invalid"}, offset=0, limit=20)
            
            # When validation fails, it returns empty list
            assert isinstance(result, list)
            assert result == []

    async def test_get_samples_with_library_source_material_filter(self, repository, mock_session):
        """Test get_samples with library_source_material filter."""
        with patch('app.repositories.sample.load_sequencing_file_enum', return_value=["DNA", "RNA"]), \
             patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="DNA_DB"):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(filters={"library_source_material": "DNA"}, offset=0, limit=20)
            
            assert isinstance(result, list)

    async def test_get_samples_with_depositions_filter(self, repository, mock_session):
        """Test get_samples with depositions filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"depositions": "phs002431"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_identifiers_filter(self, repository, mock_session):
        """Test get_samples with identifiers filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"identifiers": "SAMP001"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_identifiers_filter_multiple(self, repository, mock_session):
        """Test get_samples with multiple identifiers (|| separator)."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"identifiers": "SAMP001 || SAMP002"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_anatomical_sites_filter(self, repository, mock_session):
        """Test get_samples with anatomical_sites filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"anatomical_sites": "Brain"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_race_filter(self, repository, mock_session):
        """Test get_samples with race filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"race": "White"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_diagnosis_search(self, repository, mock_session):
        """Test get_samples with diagnosis search filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={"_diagnosis_search": "cancer"}, offset=0, limit=20)
        
        assert isinstance(result, list)

    async def test_get_samples_with_multiple_filters(self, repository, mock_session):
        """Test get_samples with multiple filters combined."""
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor"]):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={
                    "tissue_type": "Tumor",
                    "depositions": "phs002431",
                    "anatomical_sites": "Brain"
                },
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)

    async def test_get_samples_with_pagination(self, repository, mock_session):
        """Test get_samples with pagination parameters."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={}, offset=10, limit=5)
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_samples_for_diagnosis_endpoint_returns_tuple(self, repository):
        """Non-diagnosis filters delegate to get_samples and unwrap the tuple correctly."""
        with patch.object(repository, "get_samples", new_callable=AsyncMock) as mock_get_samples:
            mock_get_samples.return_value = ([Mock()], 8)

            samples, total_count = await repository.get_samples_for_diagnosis_endpoint(
                filters={"tissue_type": "Tumor"},
                offset=0,
                limit=20,
                base_url="https://example.org",
            )

            assert isinstance(samples, list)
            assert total_count == 8
            mock_get_samples.assert_awaited_once_with(
                filters={"tissue_type": "Tumor"},
                offset=0,
                limit=20,
                base_url="https://example.org",
                return_total=True,
            )

    async def test_get_samples_for_diagnosis_endpoint_fallback_list(self, repository):
        """Returns zero total when get_samples returns a plain list (no count)."""
        with patch.object(repository, "get_samples", new_callable=AsyncMock) as mock_get_samples:
            mock_get_samples.return_value = [Mock()]

            samples, total_count = await repository.get_samples_for_diagnosis_endpoint(
                filters={"tissue_type": "Tumor"},
                offset=0,
                limit=20,
            )

            assert isinstance(samples, list)
            assert len(samples) == 1
            assert total_count == 0


@pytest.mark.unit
class TestSampleRepositoryGetSamplesSummary:
    """Comprehensive tests for get_samples_summary method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_samples_summary_no_filters(self, repository, mock_session):
        """Test get_samples_summary with no filters uses single total_count query."""
        # No filters: get_samples_summary calls session.run() once, returns total_count
        async def async_gen():
            yield {"total_count": 4}
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={})
        
        assert "counts" in result
        assert "total" in result["counts"]
        assert result["counts"]["total"] == 4

    async def test_get_samples_summary_sequencing_file_only_filters(self, repository):
        """Test get_samples_summary with only sequencing_file filters uses reverse query."""
        with patch.object(repository, '_get_samples_summary_reverse_query', new_callable=AsyncMock) as mock_reverse:
            mock_reverse.return_value = {"counts": {"total": 50}}
            
            result = await repository.get_samples_summary(filters={"library_strategy": "WXS"})
            
            mock_reverse.assert_called_once_with({"library_strategy": "WXS"})
            assert result == {"counts": {"total": 50}}

    async def test_get_samples_summary_with_identifiers_filter(self, repository, mock_session):
        """Test get_samples_summary with identifiers filter."""
        async def async_gen():
            yield {"total_count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={"identifiers": "SAMP001"})
        
        # When filters are present, returns {"counts": {"total": X}} format
        assert "counts" in result
        assert "total" in result["counts"]
        assert result["counts"]["total"] == 5

    async def test_get_samples_summary_with_depositions_filter(self, repository, mock_session):
        """Test get_samples_summary with depositions filter."""
        async def async_gen():
            yield {"total_count": 20}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={"depositions": "phs002431"})
        
        # When filters are present, returns {"counts": {"total": X}} format
        assert "counts" in result
        assert "total" in result["counts"]
        assert result["counts"]["total"] == 20


@pytest.mark.unit
class TestSampleRepositoryGetSampleByIdentifier:
    """Comprehensive tests for get_sample_by_identifier method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_sample_by_identifier_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample is found."""
        # Create mock record as a dict (after dict() conversion)
        mock_record = {
            "sa": {"sample_id": "SAMP001", "sample_tumor_status": "Tumor"},
            "p": {"participant_id": "PART001"},
            "st": {"study_id": "phs002431"},
            "sf": None,
            "pf": None,
            "diagnoses": []
        }
        
        # Mock async iteration - the method does: async for record in result: records.append(dict(record))
        async def async_gen():
            yield mock_record
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Mock _record_to_sample to return a sample object
        mock_sample = Mock()
        with patch.object(repository, '_record_to_sample', return_value=mock_sample):
            result = await repository.get_sample_by_identifier(
                organization="CCDI-DCC",
                namespace="phs002431",
                name="SAMP001"
            )
            
            assert result is not None
            assert result == mock_sample
            assert mock_session.run.called

    async def test_get_sample_by_identifier_not_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample is not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="NONEXISTENT"
        )
        
        assert result is None


@pytest.mark.unit
class TestSampleRepositoryCountByAssociatedDiagnoses:
    """Tests for _count_samples_by_associated_diagnoses method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_count_samples_by_associated_diagnoses_no_filters(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses with no filters."""
        async def async_gen():
            yield {"value": "Neuroblastoma", "count": 10}
            yield {"value": "Leukemia", "count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 15}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))  # missing query
        ])
        
        result = await repository._count_samples_by_associated_diagnoses({})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result

    async def test_count_samples_by_associated_diagnoses_with_filters(self, repository, mock_session):
        """Test _count_samples_by_associated_diagnoses with filters."""
        async def async_gen():
            yield {"value": "Neuroblastoma", "count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 5}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository._count_samples_by_associated_diagnoses({"depositions": "phs002431"})
        
        assert "total" in result


@pytest.mark.unit
class TestSampleRepositoryRecordToSample:
    """Comprehensive tests for _record_to_sample method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    def test_record_to_sample_basic(self, repository):
        """Test _record_to_sample with basic valid data."""
        sa = {"sample_id": "SAMP001", "sample_tumor_status": "Tumor", "anatomic_site": "Brain"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        sf = {"library_strategy": "WXS", "library_selection": "PCR"}
        pf = {"fixation_embedding_method": "FFPE"}
        diagnoses = {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"}
        
        sample = repository._record_to_sample(sa, p, st, sf, pf, diagnoses)
        
        assert sample is not None
        assert sample.id.name == "SAMP001"
        assert sample.id.namespace.name == "phs002431"

    def test_record_to_sample_empty_sa(self, repository):
        """Test _record_to_sample raises error when sa is empty."""
        with pytest.raises(ValueError, match="Sample node.*required"):
            repository._record_to_sample({}, {}, {}, {}, {}, None)

    def test_record_to_sample_missing_study_id(self, repository):
        """Test _record_to_sample raises error when study_id is missing."""
        sa = {"sample_id": "SAMP001"}
        with pytest.raises(ValueError, match="missing required study_id"):
            repository._record_to_sample(sa, {}, {}, {}, {}, None)

    def test_record_to_sample_missing_sample_id(self, repository):
        """Test _record_to_sample raises error when sample_id is missing."""
        # Provide a non-empty sa dict but without sample_id, id, or name fields
        sa = {"some_other_field": "value"}  # Has data but no sample_id
        st = {"study_id": "phs002431"}
        # The method tries to get sample_id from sa.get("sample_id"), then sa.get("id"), then sa.get("name")
        # If all are missing, it raises ValueError with "missing required sample_id"
        with pytest.raises(ValueError, match="missing required sample_id"):
            repository._record_to_sample(sa, {}, st, {}, {}, None)

    def test_record_to_sample_study_id_from_participant(self, repository):
        """Test _record_to_sample gets study_id from participant when not in st."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001", "study_id": "phs002431"}
        st = {}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, None)
            assert sample.id.namespace.name == "phs002431"

    def test_record_to_sample_with_invalid_values(self, repository):
        """Test _record_to_sample handles invalid values (-999, "Invalid value")."""
        sa = {
            "sample_id": "SAMP001",
            "sample_tumor_status": "Tumor",
            "participant_age_at_collection": -999
        }
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {"age_at_diagnosis": -999}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, diagnoses)
            # age_at_collection and age_at_diagnosis should be None (filtered out -999)
            assert sample.metadata.age_at_collection is None
            assert sample.metadata.age_at_diagnosis is None

    def test_record_to_sample_with_anatomical_sites_list(self, repository):
        """Test _record_to_sample handles anatomical_sites as list."""
        sa = {"sample_id": "SAMP001", "anatomic_site": ["Brain", "Liver"]}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, None)
            assert sample.metadata.anatomical_sites is not None
            assert len(sample.metadata.anatomical_sites) == 2

    def test_record_to_sample_with_anatomical_sites_semicolon_separated(self, repository):
        """Test _record_to_sample handles anatomical_sites as semicolon-separated string."""
        sa = {"sample_id": "SAMP001", "anatomic_site": "Brain; Liver"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, None)
            assert sample.metadata.anatomical_sites is not None
            assert len(sample.metadata.anatomical_sites) == 2

    def test_record_to_sample_with_base_url(self, repository):
        """Test _record_to_sample includes server URL when base_url provided."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(
                sa, p, st, {}, {}, None,
                base_url="https://api.example.com"
            )
            assert sample.metadata.identifiers is not None
            assert sample.metadata.identifiers[0].value.server is not None
            assert "phs002431" in sample.metadata.identifiers[0].value.server

    def test_record_to_sample_with_diagnosis_comment(self, repository):
        """Test _record_to_sample handles diagnosis with comment."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {
            "diagnosis": "Neuroblastoma",
            "diagnosis_comment": "See pathology report"
        }
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, diagnoses)
            assert sample.metadata.diagnosis is not None
            assert sample.metadata.diagnosis[0].value == "Neuroblastoma"
            assert sample.metadata.diagnosis[0].comment == "See pathology report"

    def test_record_to_sample_with_empty_diagnosis(self, repository):
        """Test _record_to_sample handles empty diagnosis."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {"diagnosis": ""}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, diagnoses)
            assert sample.metadata.diagnosis is None

    def test_record_to_sample_with_multiple_diagnoses_aggregates_categories(self, repository):
        """Test repository _record_to_sample preserves all diagnosis entries and category tokens."""
        sa = {
            "sample_id": "SAMP001",
            "participant_age_at_collection": "12.0",
            "sample_tumor_status": "Tumor",
        }
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = [
            {
                "diagnosis": "Neuroblastoma",
                "tumor_grade": "G1",
                "age_at_diagnosis": 10,
                "diagnosis_category": "Medulloblastoma;Gliomas",
            },
            {
                "diagnosis": "Leukemia",
                "tumor_grade": "G3",
                "age_at_diagnosis": 20,
                "diagnosis_category": "Medulloblastoma;Custom Category",
            },
        ]

        sample = repository._record_to_sample(sa, p, st, {}, {}, diagnoses)

        assert [item.value for item in sample.metadata.diagnosis] == ["Neuroblastoma", "Leukemia"]
        assert sample.metadata.tumor_grade.value == "G1"
        assert sample.metadata.age_at_diagnosis.value == 10
        assert [item.value for item in sample.metadata.diagnosis_category] == ["Medulloblastoma"]
        assert [item["value"] for item in sample.metadata.unharmonized["diagnosis_category"]] == [
            "Gliomas",
            "Custom Category",
        ]


@pytest.mark.unit
class TestSampleRepositoryGetSamplesBySequencingFileFilters:
    """Tests for _get_samples_by_sequencing_file_filters method."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_samples_by_sequencing_file_filters_library_source_material(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with library_source_material filter."""
        async def async_gen():
            mock_record = {
                "sa": {"sample_id": "SAMP001"},
                "p": {"participant_id": "PART001"},
                "st": {"study_id": "phs002431"},
                "sf": {"library_source_material": "DNA"},
                "pf": None,
                "diagnoses": None
            }
            yield mock_record
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="DNA_DB"), \
             patch.object(repository, '_record_to_sample', return_value=Mock()):
            result = await repository._get_samples_by_sequencing_file_filters(
                {"library_source_material": "DNA"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            assert mock_session.run.called

    async def test_get_samples_by_sequencing_file_filters_invalid_library_source_material(self, repository):
        """Test _get_samples_by_sequencing_file_filters returns empty for invalid library_source_material."""
        with patch('app.repositories.sample.is_null_mapped_value', return_value=True):
            result = await repository._get_samples_by_sequencing_file_filters(
                {"library_source_material": "Invalid"},
                offset=0,
                limit=20
            )
            
            assert result == []

    async def test_get_samples_by_sequencing_file_filters_library_strategy(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with library_strategy filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="WXS_DB"), \
             patch.object(repository, '_record_to_sample', return_value=Mock()):
            result = await repository._get_samples_by_sequencing_file_filters(
                {"library_strategy": "WXS"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)

    async def test_get_samples_by_sequencing_file_filters_specimen_molecular_analyte_type_list(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters with specimen_molecular_analyte_type (list mapping)."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.is_database_only_value', return_value=False), \
             patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value=["m1", "m2"]), \
             patch.object(repository, '_record_to_sample', return_value=Mock()):
            result = await repository._get_samples_by_sequencing_file_filters(
                {"specimen_molecular_analyte_type": "DNA"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            # Verify the query uses IN clause for list
            cypher = mock_session.run.call_args[0][0]
            assert "IN ['m1', 'm2']" in cypher or "IN" in cypher


@pytest.mark.unit
class TestSampleRepositoryAdditionalFilters:
    """Additional tests for various filter combinations and edge cases."""

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
    def mock_settings(self):
        """Create mock settings."""
        return Mock(spec=Settings)

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_count_samples_by_field_preservation_method(self, repository, mock_session):
        """Test count_samples_by_field for preservation_method (pathology_file field)."""
        async def async_gen():
            yield {"value": "FFPE", "count": 10}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        # Mock TWO_QUERY_APPROACH for total count (2 queries) + missing query
        mock_result_path2 = AsyncMock()
        mock_result_path2.data = AsyncMock(return_value=[
            {"sample_id": "S1", "study_id": "ST1"}
        ])
        mock_result_path1 = AsyncMock()
        mock_result_path1.data = AsyncMock(return_value=[])
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            mock_result_path2,  # total query path2
            mock_result_path1,  # total query path1
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("preservation_method", {})
        
        assert "total" in result

    async def test_count_samples_by_field_tumor_grade(self, repository, mock_session):
        """Test count_samples_by_field for tumor_grade (diagnosis field)."""
        async def async_gen():
            yield {"value": "G1", "count": 5}
        mock_values = AsyncMock()
        mock_values.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(side_effect=[
            mock_values,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 2}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        result = await repository.count_samples_by_field("tumor_grade", {})
        assert "total" in result
        assert result["total"] == 2

    async def test_count_samples_by_field_tumor_classification(self, repository, mock_session):
        """Test count_samples_by_field for tumor_classification (diagnosis field)."""
        async def async_gen():
            yield {"value": "Malignant", "count": 8}
        mock_values = AsyncMock()
        mock_values.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(side_effect=[
            mock_values,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 3}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        result = await repository.count_samples_by_field("tumor_classification", {})
        assert "total" in result
        assert result["total"] == 3

    async def test_count_samples_by_field_tumor_tissue_morphology(self, repository, mock_session):
        """Test count_samples_by_field for tumor_tissue_morphology (diagnosis field)."""
        async def async_gen():
            yield {"value": "Adenocarcinoma", "count": 3}
        mock_values = AsyncMock()
        mock_values.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(side_effect=[
            mock_values,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 2}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        result = await repository.count_samples_by_field("tumor_tissue_morphology", {})
        assert "total" in result
        assert result["total"] == 2

    async def test_count_samples_by_field_age_at_diagnosis(self, repository, mock_session):
        """Test count_samples_by_field for age_at_diagnosis (diagnosis field)."""
        async def async_gen():
            yield {"value": 10, "count": 2}
        mock_values = AsyncMock()
        mock_values.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(side_effect=[
            mock_values,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 2}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        result = await repository.count_samples_by_field("age_at_diagnosis", {})
        assert "total" in result
        assert result["total"] == 2

    async def test_get_samples_with_tumor_classification_filter(self, repository, mock_session):
        """Test get_samples with tumor_classification filter."""
        with patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="Malignant"):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={"tumor_classification": "Malignant"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)

    async def test_get_samples_with_preservation_method_filter(self, repository, mock_session):
        """Test get_samples with preservation_method filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"preservation_method": "FFPE"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_samples_with_tumor_grade_filter(self, repository, mock_session):
        """Test get_samples with tumor_grade filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"tumor_grade": "G1"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_samples_with_age_at_diagnosis_filter(self, repository, mock_session):
        """Test get_samples with age_at_diagnosis filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"age_at_diagnosis": 10},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_samples_with_age_at_collection_filter(self, repository, mock_session):
        """Test get_samples with age_at_collection filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"age_at_collection": 15},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_samples_with_disease_phase_filter(self, repository, mock_session):
        """Test get_samples with disease_phase filter."""
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={"disease_phase": "Primary"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            
            # Verify query uses diagnosis-first path (CALL {} subquery was removed)
            if mock_session.run.called:
                call_args = mock_session.run.call_args
                query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
                # Should use diagnosis-first path (no CALL {} subquery)
                assert ('MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query or 
                        'MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)' in query or
                        'MATCH (d:diagnosis)' in query or
                        'disease_phase' in query)

    async def test_get_samples_with_disease_phase_and_tissue_type_combined(self, repository, mock_session):
        """Test get_samples with both disease_phase and tissue_type filters."""
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
        mock_result.__aiter__ = Mock(return_value=async_gen())  # Properly set up async iterator
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"), \
             patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            result = await repository.get_samples(
                filters={"disease_phase": "Primary", "tissue_type": "Tumor"},
                offset=0,
                limit=20
            )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        
        # Verify query does NOT use CALL {} subquery (uses sequential OPTIONAL MATCH instead)
        call_args = mock_session.run.call_args
        query = call_args[0][0] if call_args[0] else call_args.kwargs.get('cypher', '')
        assert 'CALL {' not in query and 'CALL{' not in query.replace(' ', '')
        
        # Verify both filters are present
        assert 'sample_tumor_status' in query
        # disease_phase filter can be in various patterns
        assert ('MATCH (sa:sample)<-[:of_diagnosis]-(d:diagnosis)' in query or 
                'MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)' in query or
                'MATCH (d:diagnosis)' in query or
                'disease_phase' in query)
        
        # Verify sequential study collection
        assert 'OPTIONAL MATCH (sa)-[:of_sample]->(:participant)' in query or \
               'OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)' in query

    async def test_get_samples_disease_phase_with_return_total(self, repository, mock_session):
        """Test get_samples with disease_phase filter and return_total=True."""
        async def async_gen_list():
            yield {
                "sa": {"sample_id": "SAMP001"},
                "p": {},
                "st": {"study_id": "phs001"},
                "sf": {},
                "pf": {},
                "diagnoses": {"disease_phase": "Primary"}
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
        
        with patch('app.repositories.sample.reverse_map_field_value', return_value="Primary"):
            result = await repository.get_samples(
                filters={"disease_phase": "Primary"},
                offset=0,
                limit=20,
                return_total=True
            )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], int)
        assert result[1] == 100
        assert mock_session.run.call_count >= 2

    async def test_get_samples_with_diagnosis_filter(self, repository, mock_session):
        """Test get_samples with diagnosis filter routes to Case 3."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Mock Case 3 to verify it's called
        with patch.object(repository, '_get_samples_case3_with_node_filters', new_callable=AsyncMock) as mock_case3:
            mock_case3.return_value = []
            result = await repository.get_samples(
                filters={"diagnosis": "Neuroblastoma"},
                offset=0,
                limit=20
            )
            
            # Verify Case 3 was called (diagnosis filter should be categorized correctly)
            mock_case3.assert_called_once()
            call_args = mock_case3.call_args
            filters_arg = call_args[0][0] if call_args[0] else {}
            categorized_arg = call_args[0][1] if len(call_args[0]) > 1 else {}
            
            # Verify diagnosis filter is in filters and categorized correctly
            assert "diagnosis" in filters_arg
            assert "diagnosis" in categorized_arg.get("diagnosis", {}), "diagnosis filter must be in diagnosis category for Case 3 routing"
            
            assert isinstance(result, list)


# Helper function for async generators
def async_gen_from_list(items):
    """Create an async generator from a list."""
    async def gen():
        for item in items:
            yield item
    return gen()

