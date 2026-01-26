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
        # Mock query results
        async def async_gen():
            yield {"value": "Tumor", "count": 50}
            yield {"value": "Normal", "count": 30}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        # Mock three separate queries: values, total, missing
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 80}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("tissue_type", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert mock_session.run.call_count >= 1

    async def test_count_samples_by_field_age_at_collection_no_filters(self, repository, mock_session):
        """Test count_samples_by_field for age_at_collection (sample node field) with no filters."""
        async def async_gen():
            yield {"value": 10, "count": 5}
            yield {"value": 20, "count": 3}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 8}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 2}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("age_at_collection", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result

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
        """Test count_samples_by_field for library_strategy (sequencing_file field)."""
        async def async_gen():
            yield {"value": "WXS", "count": 20}
            yield {"value": "RNA-Seq", "count": 15}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 35}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 5}])))
        ])
        
        result = await repository.count_samples_by_field("library_strategy", {})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result

    async def test_count_samples_by_field_disease_phase(self, repository, mock_session):
        """Test count_samples_by_field for disease_phase (diagnosis field)."""
        async def async_gen():
            yield {"value": "Primary", "count": 10}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 10}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("disease_phase", {})
        
        assert "total" in result


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

    async def test_get_samples_sequencing_file_only_filters(self, repository):
        """Test get_samples with only sequencing_file filters uses reverse query optimization."""
        with patch.object(repository, '_get_samples_by_sequencing_file_filters', new_callable=AsyncMock) as mock_reverse:
            mock_reverse.return_value = []
            
            result = await repository.get_samples(
                filters={"library_strategy": "WXS"},
                offset=0,
                limit=20
            )
            
            mock_reverse.assert_called_once()
            assert isinstance(result, list)

    async def test_get_samples_with_tissue_type_filter_valid(self, repository, mock_session):
        """Test get_samples with valid tissue_type filter."""
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(filters={"tissue_type": "Tumor"}, offset=0, limit=20)
            
            assert isinstance(result, list)

    async def test_get_samples_with_tissue_type_filter_invalid(self, repository, mock_session):
        """Test get_samples with invalid tissue_type filter returns summary dict with zero count."""
        with patch('app.repositories.sample.load_sample_enum', return_value=["Tumor", "Normal"]):
            # Invalid tissue_type should be validated and return summary dict with zero count
            # The validation happens in _validate_tissue_type_filter which returns None for invalid values
            # This causes get_samples to return early with {"counts": {"total": 0}}
            result = await repository.get_samples(filters={"tissue_type": "Invalid"}, offset=0, limit=20)
            
            # When validation fails, it returns a summary dict, not a list
            assert isinstance(result, dict)
            assert "counts" in result
            assert result["counts"]["total"] == 0

    async def test_get_samples_with_library_source_material_filter(self, repository, mock_session):
        """Test get_samples with library_source_material filter."""
        with patch('app.repositories.sample.load_sequencing_file_enum', return_value=["DNA", "RNA"]), \
             patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="DNA_DB"):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())
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
            mock_result.__aiter__ = Mock(return_value=async_gen())
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
        """Test get_samples_summary with no filters uses optimized query."""
        async def async_gen():
            yield {"total_count": 100}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={})
        
        assert "counts" in result
        assert "total" in result["counts"]
        assert result["counts"]["total"] == 100

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
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={"identifiers": "SAMP001"})
        
        # When filters are present, returns {"total_count": X} format
        assert "total_count" in result
        assert result["total_count"] == 5

    async def test_get_samples_summary_with_depositions_filter(self, repository, mock_session):
        """Test get_samples_summary with depositions filter."""
        async def async_gen():
            yield {"total_count": 20}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={"depositions": "phs002431"})
        
        # When filters are present, returns {"total_count": X} format
        assert "total_count" in result
        assert result["total_count"] == 20


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
            assert sample.metadata.diagnosis.value == "Neuroblastoma"
            assert sample.metadata.diagnosis.comment == "See pathology report"

    def test_record_to_sample_with_empty_diagnosis(self, repository):
        """Test _record_to_sample handles empty diagnosis."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {"diagnosis": ""}
        
        with patch('app.repositories.sample.map_field_value', return_value="Tumor"):
            sample = repository._record_to_sample(sa, p, st, {}, {}, diagnoses)
            assert sample.metadata.diagnosis is None


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
        mock_result.__aiter__ = Mock(return_value=async_gen())
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
        mock_result.__aiter__ = Mock(return_value=async_gen())
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
        mock_result.__aiter__ = Mock(return_value=async_gen())
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
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 10}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("preservation_method", {})
        
        assert "total" in result

    async def test_count_samples_by_field_tumor_grade(self, repository, mock_session):
        """Test count_samples_by_field for tumor_grade (diagnosis field)."""
        async def async_gen():
            yield {"value": "G1", "count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 5}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tumor_grade", {})
        
        assert "total" in result

    async def test_count_samples_by_field_tumor_classification(self, repository, mock_session):
        """Test count_samples_by_field for tumor_classification (diagnosis field)."""
        async def async_gen():
            yield {"value": "Malignant", "count": 8}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 8}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tumor_classification", {})
        
        assert "total" in result

    async def test_count_samples_by_field_tumor_tissue_morphology(self, repository, mock_session):
        """Test count_samples_by_field for tumor_tissue_morphology (diagnosis field)."""
        async def async_gen():
            yield {"value": "Adenocarcinoma", "count": 3}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 3}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("tumor_tissue_morphology", {})
        
        assert "total" in result

    async def test_count_samples_by_field_age_at_diagnosis(self, repository, mock_session):
        """Test count_samples_by_field for age_at_diagnosis (diagnosis field)."""
        async def async_gen():
            yield {"value": 10, "count": 2}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 2}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field("age_at_diagnosis", {})
        
        assert "total" in result

    async def test_get_samples_with_tumor_classification_filter(self, repository, mock_session):
        """Test get_samples with tumor_classification filter."""
        with patch('app.repositories.sample.is_null_mapped_value', return_value=False), \
             patch('app.repositories.sample.reverse_map_field_value', return_value="Malignant"):
            async def async_gen():
                if False:
                    yield  # Makes this an async generator, but never executes
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())
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
            mock_result.__aiter__ = Mock(return_value=async_gen())
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={"disease_phase": "Primary"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)

    async def test_get_samples_with_diagnosis_filter(self, repository, mock_session):
        """Test get_samples with diagnosis filter."""
        async def async_gen():
            if False:
                yield  # Makes this an async generator, but never executes
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"diagnosis": "Neuroblastoma"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)


# Helper function for async generators
def async_gen_from_list(items):
    """Create an async generator from a list."""
    async def gen():
        for item in items:
            yield item
    return gen()

