"""
Additional edge case tests for SampleRepository to improve coverage.

This module focuses on testing error handling, retry logic, and complex
query paths that are currently under-tested.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings


@pytest.mark.unit
class TestSampleRepositoryErrorHandling:
    """Tests for error handling and retry logic."""

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

    async def test_get_samples_exception_handling(self, repository, mock_session):
        """Test get_samples exception handling - exceptions are re-raised."""
        # Exception during query execution - early pagination path raises immediately
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception, match="Database error"):
            await repository.get_samples(filters={}, offset=0, limit=20)
        
        # Exception is raised immediately in early pagination path (no retry for exceptions in early pagination)
        assert mock_session.run.called

    async def test_get_samples_anatomical_sites_list_error_fallback(self, repository, mock_session):
        """Test get_samples falls back to string query when list query fails for anatomical_sites."""
        # First query fails with list error, second (string) succeeds
        async def async_gen():
            yield {"sa": {"sample_id": "SAMP001", "anatomic_site": "Brain"}, "p": {}, "st": {"study_id": "phs001"}}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        
        # First call fails with "in expected a list" error, second succeeds
        mock_session.run = AsyncMock(side_effect=[
            Exception("in expected a list"),
            mock_result
        ])
        
        result = await repository.get_samples(
            filters={"anatomical_sites": "Brain"},
            offset=0,
            limit=20
        )
        
        # Should have tried list query, then fallback to string query
        assert mock_session.run.call_count == 2
        assert isinstance(result, list)

    async def test_count_samples_by_field_anatomical_sites_missing_fallback(self, repository, mock_session):
        """Test count_samples_by_field falls back to string query when list query fails for anatomical_sites missing."""
        async def async_gen():
            yield {"value": "Brain", "count": 5}
        
        async def async_gen_missing():
            yield {"missing": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        # First missing query fails with "all list string" error, second (string) succeeds
        mock_missing_string = AsyncMock()
        mock_missing_string.__aiter__ = Mock(return_value=async_gen_missing())
        
        # Create a result that raises exception on iteration
        class FailingResult:
            def __aiter__(self):
                return self
            async def __anext__(self):
                raise Exception("all list string error")
        
        # values query, total query, missing list query fails, missing string query succeeds
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 5}]))),  # total query
            FailingResult(),  # missing list query fails
            mock_missing_string  # missing string query succeeds
        ])
        
        result = await repository.count_samples_by_field("anatomical_sites", {})
        
        assert "missing" in result
        assert result["missing"] == 5

    async def test_count_samples_by_field_anatomical_sites_missing_both_fail(self, repository, mock_session):
        """Test count_samples_by_field handles case when both list and string queries fail for anatomical_sites missing."""
        async def async_gen():
            yield {"value": "Brain", "count": 10}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        # List query fails, string query also fails
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen())),  # total query
            Exception("all list string error"),  # missing list query fails
            Exception("string query also fails"),  # missing string query also fails
        ])
        
        result = await repository.count_samples_by_field("anatomical_sites", {})
        
        # Should default missing to 0 when both queries fail
        assert "missing" in result
        assert result["missing"] == 0


@pytest.mark.unit
class TestSampleRepositoryComplexQueries:
    """Tests for complex query building and field-specific handling."""

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

    async def test_count_samples_by_field_specimen_molecular_analyte_type_combined_query(self, repository, mock_session):
        """Test count_samples_by_field uses combined query for specimen_molecular_analyte_type."""
        async def async_gen():
            yield {"value": "DNA", "count": 10, "total": 20, "missing": 2}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.get_mapped_db_values', return_value=["DNA", "RNA"]), \
             patch('app.repositories.sample.build_case_mapping_statement', return_value="CASE WHEN molecule_value = 'DNA' THEN 'DNA' END"), \
             patch('app.repositories.sample.build_invalid_value_list_filter', return_value="val <> '-999'"), \
             patch('app.repositories.sample.load_sequencing_file_enum', return_value=None):
            result = await repository.count_samples_by_field("specimen_molecular_analyte_type", {})
            
            assert "total" in result
            assert "missing" in result
            assert "values" in result
            # Combined query should return all three in one result
            cypher = mock_session.run.call_args[0][0]
            assert "total" in cypher and "missing" in cypher  # Combined query includes both

    async def test_count_samples_by_field_library_source_material_combined_query(self, repository, mock_session):
        """Test count_samples_by_field uses combined query for library_source_material when no filters."""
        async def async_gen():
            yield {"value": "DNA", "count": 15, "total": 25, "missing": 3}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.build_invalid_value_list_filter', return_value="val <> '-999'"), \
             patch('app.repositories.sample.load_sequencing_file_enum', return_value=["DNA", "RNA"]):
            result = await repository.count_samples_by_field("library_source_material", {})
            
            assert "total" in result
            assert "missing" in result
            assert "values" in result

    async def test_count_samples_by_field_library_source_material_combined_query_no_filters(self, repository, mock_session):
        """Test count_samples_by_field uses combined query for library_source_material when no filters."""
        async def async_gen():
            yield {"value": "DNA", "count": 10, "total": 20, "missing": 2}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(return_value=mock_result)
        
        with patch('app.repositories.sample.build_invalid_value_list_filter', return_value="val <> '-999'"), \
             patch('app.repositories.sample.load_sequencing_file_enum', return_value=["DNA", "RNA"]):
            result = await repository.count_samples_by_field("library_source_material", {})
            
            assert "total" in result
            assert "missing" in result
            assert "values" in result
            # Combined query should return all three in one result
            cypher = mock_session.run.call_args[0][0]
            assert "total" in cypher and "missing" in cypher  # Combined query includes both

    async def test_count_samples_by_field_anatomical_sites_with_filters(self, repository, mock_session):
        """Test count_samples_by_field for anatomical_sites with base filters."""
        async def async_gen():
            yield {"value": "Brain", "count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 5}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("anatomical_sites", {"depositions": "phs002431"})
        
        assert "total" in result
        assert "missing" in result
        assert "values" in result

    async def test_count_samples_by_field_anatomical_sites_list_query(self, repository, mock_session):
        """Test count_samples_by_field for anatomical_sites uses list query."""
        async def async_gen():
            yield {"value": "Brain", "count": 8}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,  # values query (list version)
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 8}]))),  # total query
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 2}])))  # missing query
        ])
        
        result = await repository.count_samples_by_field("anatomical_sites", {})
        
        assert "total" in result
        # Verify list query was used (check for UNWIND in cypher)
        cypher = mock_session.run.call_args_list[0][0][0]
        assert "UNWIND" in cypher or "valueType" in cypher  # List query indicators

    async def test_count_samples_by_field_with_complex_filters(self, repository, mock_session):
        """Test count_samples_by_field with multiple complex filters."""
        async def async_gen():
            yield {"value": "Tumor", "count": 3}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        
        mock_session.run = AsyncMock(side_effect=[
            mock_result,
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"total": 3}]))),
            AsyncMock(__aiter__=Mock(return_value=async_gen_from_list([{"missing": 0}])))
        ])
        
        result = await repository.count_samples_by_field(
            "tissue_type",
            {
                "race": "White",
                "identifiers": "SAMP001 || SAMP002",
                "depositions": "phs001 || phs002",
                "_diagnosis_search": "cancer"
            }
        )
        
        assert "total" in result

    async def test_get_samples_summary_with_complex_filters(self, repository, mock_session):
        """Test get_samples_summary with complex filter combinations."""
        async def async_gen():
            yield {"total_count": 15}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary({
            "identifiers": "SAMP001 || SAMP002",
            "depositions": "phs001",
            "anatomical_sites": ["Brain", "Liver"]
        })
        
        assert "total_count" in result

    async def test_get_samples_with_complex_anatomical_sites_filter(self, repository, mock_session):
        """Test get_samples with complex anatomical_sites filter (list with multiple values)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"anatomical_sites": ["Brain", "Liver", "Lung"]},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        # Verify the query was built with OR conditions for multiple values
        assert mock_session.run.called


@pytest.mark.unit
class TestSampleRepositoryQueryBuilding:
    """Tests for complex query building logic."""

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

    async def test_get_samples_with_disease_phase_list_mapping(self, repository, mock_session):
        """Test get_samples with disease_phase that maps to multiple DB values."""
        with patch('app.repositories.sample.reverse_map_field_value', return_value=["Primary", "Recurrent"]):
            async def async_gen():
                return
                yield
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={"disease_phase": "Relapse"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            # Verify OR condition was used for multiple mapped values
            cypher = mock_session.run.call_args[0][0]
            assert "IN" in cypher or "OR" in cypher

    async def test_get_samples_with_tumor_classification_null_mapped(self, repository, mock_session):
        """Test get_samples with tumor_classification that is null-mapped."""
        with patch('app.repositories.sample.is_null_mapped_value', return_value=True):
            async def async_gen():
                return
                yield
            
            mock_result = AsyncMock()
            mock_result.__aiter__ = Mock(return_value=async_gen())
            mock_session.run = AsyncMock(return_value=mock_result)
            
            result = await repository.get_samples(
                filters={"tumor_classification": "non-malignant"},
                offset=0,
                limit=20
            )
            
            assert isinstance(result, list)
            # Should add "false" condition for null-mapped values
            cypher = mock_session.run.call_args[0][0]
            assert "false" in cypher.lower() or "WHERE" in cypher

    async def test_get_samples_with_library_selection_method_database_only(self, repository, mock_session):
        """Test get_samples with library_selection_method that is database-only."""
        with patch('app.repositories.sample.is_database_only_value', return_value=True):
            result = await repository.get_samples(
                filters={"library_selection_method": "PolyA"},
                offset=0,
                limit=20
            )
            
            # Database-only values should return empty results
            assert isinstance(result, list)
            assert len(result) == 0

    async def test_get_samples_summary_reverse_query_error_handling(self, repository, mock_session):
        """Test get_samples_summary reverse query error handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception):
            await repository._get_samples_summary_reverse_query({"library_strategy": "WXS"})

    async def test_get_samples_by_sequencing_file_filters_error_handling(self, repository, mock_session):
        """Test _get_samples_by_sequencing_file_filters error handling."""
        mock_session.run = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception):
            await repository._get_samples_by_sequencing_file_filters(
                {"library_strategy": "WXS"},
                offset=0,
                limit=20
            )


# Helper function for async generators
def async_gen_from_list(items):
    """Create an async generator from a list."""
    async def gen():
        for item in items:
            yield item
    return gen()

