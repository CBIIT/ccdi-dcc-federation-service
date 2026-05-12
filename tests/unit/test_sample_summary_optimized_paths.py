"""
Unit tests for the two optimized summary query methods in sample_summary.py:
  - _get_samples_summary_diagnosis_filters_optimized
  - _get_samples_summary_reverse_query
"""

import pytest
from unittest.mock import AsyncMock, Mock
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings
from tests.unit.helpers import make_async_result, make_single_result


@pytest.fixture
def mock_session():
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_allowlist():
    al = Mock(spec=FieldAllowlist)
    al.is_field_allowed = Mock(return_value=True)
    al.is_allowed = Mock(return_value=True)
    return al


@pytest.fixture
def mock_settings():
    s = Mock(spec=Settings)
    s.subject_count_fields = []
    return s


@pytest.fixture
def repository(mock_session, mock_allowlist, mock_settings):
    return SampleRepository(mock_session, mock_allowlist, mock_settings)


# ---------------------------------------------------------------------------
# Tests: _get_samples_summary_diagnosis_filters_optimized
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDiagnosisFiltersOptimized:
    """Tests for _get_samples_summary_diagnosis_filters_optimized."""

    async def test_diagnosis_optimized_no_filters(self, repository, mock_session):
        """Empty filters → no dx_conditions → returns total=0 without hitting session."""
        result = await repository._get_samples_summary_diagnosis_filters_optimized({})
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_diagnosis_optimized_identifiers_single(self, repository, mock_session):
        """Single identifier + disease_phase → session called once."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 5}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"identifiers": "SAMPLE1", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 5}}
        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]
        assert "sa.sample_id" in query

    async def test_diagnosis_optimized_identifiers_multi(self, repository, mock_session):
        """Multi-value identifiers (||) → uses IN clause."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 3}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"identifiers": "SAMPLE1||SAMPLE2", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 3}}
        query = mock_session.run.call_args[0][0]
        params = mock_session.run.call_args[0][1]
        id_param_value = next(
            (v for v in params.values() if isinstance(v, list)), None
        )
        assert id_param_value == ["SAMPLE1", "SAMPLE2"]
        assert "IN $" in query

    async def test_diagnosis_optimized_anatomical_sites_multi(self, repository, mock_session):
        """Multi-value anatomical_sites → OR conditions."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 7}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"anatomical_sites": "Brain||Lung", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 7}}
        query = mock_session.run.call_args[0][0]
        assert "anatomic_site" in query

    async def test_diagnosis_optimized_anatomical_sites_single(self, repository, mock_session):
        """Single anatomical_site (no ||) → single condition."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 2}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"anatomical_sites": "Brain", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 2}}
        query = mock_session.run.call_args[0][0]
        assert "anatomic_site" in query

    async def test_diagnosis_optimized_tissue_type_only(self, repository, mock_session):
        """tissue_type alone with no diagnosis filter → no dx_conditions → total=0."""
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tissue_type": "Tumor"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_diagnosis_optimized_tissue_type_with_disease_phase(self, repository, mock_session):
        """tissue_type + disease_phase → builds sample WHERE and dx condition."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 10}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tissue_type": "Tumor", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 10}}
        query = mock_session.run.call_args[0][0]
        assert "sample_tumor_status" in query
        assert "disease_phase" in query

    async def test_diagnosis_optimized_depositions_multi(self, repository, mock_session):
        """Multi-value depositions (||) → dep_where_clause uses IN."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 15}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"depositions": "STUDY1||STUDY2", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 15}}
        params = mock_session.run.call_args[0][1]
        dep_list = next((v for v in params.values() if isinstance(v, list)), None)
        assert dep_list == ["STUDY1", "STUDY2"]

    async def test_diagnosis_optimized_disease_phase(self, repository, mock_session):
        """disease_phase='Relapse' reverse-maps to list → dx condition uses IN."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 8}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 8}}
        query = mock_session.run.call_args[0][0]
        params = mock_session.run.call_args[0][1]
        assert "disease_phase" in query
        assert "IN $" in query
        reverse_val = next(
            (v for v in params.values() if isinstance(v, list)), None
        )
        assert reverse_val is not None
        assert "Recurrent Disease" in reverse_val

    async def test_diagnosis_optimized_disease_phase_db_only(self, repository, mock_session):
        """'Recurrent Disease' is a DB-only value → early return 0."""
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"disease_phase": "Recurrent Disease"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_diagnosis_optimized_age_at_diagnosis(self, repository, mock_session):
        """age_at_diagnosis string → converted to int for Cypher."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 4}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"age_at_diagnosis": "5"}
        )
        assert result == {"counts": {"total": 4}}
        params = mock_session.run.call_args[0][1]
        int_val = next((v for v in params.values() if isinstance(v, int)), None)
        assert int_val == 5

    async def test_diagnosis_optimized_diagnosis_category_only(self, repository, mock_session):
        """diagnosis_category is not handled as a dx condition → no dx_conditions → total=0."""
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"diagnosis_category": "Leukemia"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_diagnosis_optimized_combined(self, repository, mock_session):
        """tissue_type + disease_phase + identifiers together."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 6}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {
                "tissue_type": "Tumor",
                "disease_phase": "Relapse",
                "identifiers": "S1",
            }
        )
        assert result == {"counts": {"total": 6}}
        query = mock_session.run.call_args[0][0]
        assert "sample_tumor_status" in query
        assert "disease_phase" in query
        assert "sample_id" in query

    async def test_diagnosis_optimized_tumor_grade(self, repository, mock_session):
        """tumor_grade → simple equality condition."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 3}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tumor_grade": "Grade 1"}
        )
        assert result == {"counts": {"total": 3}}
        query = mock_session.run.call_args[0][0]
        assert "tumor_grade" in query

    async def test_diagnosis_optimized_tumor_classification_null_mapped(self, repository, mock_session):
        """'non-malignant' is null-mapped for tumor_classification → early return 0."""
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tumor_classification": "non-malignant"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_diagnosis_optimized_tumor_tissue_morphology(self, repository, mock_session):
        """tumor_tissue_morphology → simple equality condition."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 1}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tumor_tissue_morphology": "8000/3"}
        )
        assert result == {"counts": {"total": 1}}
        query = mock_session.run.call_args[0][0]
        assert "tumor_tissue_morphology" in query

    async def test_diagnosis_optimized_depositions_single(self, repository, mock_session):
        """Single deposition (no ||) → dep_where_clause uses =."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 9}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"depositions": "STUDY1", "disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 9}}
        query = mock_session.run.call_args[0][0]
        assert "WHERE sid =" in query or "sid =" in query

    async def test_diagnosis_optimized_empty_db_result(self, repository, mock_session):
        """No records returned from DB → total=0."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"disease_phase": "Relapse"}
        )
        assert result == {"counts": {"total": 0}}

    async def test_diagnosis_optimized_age_at_diagnosis_non_numeric(self, repository, mock_session):
        """Non-numeric age_at_diagnosis → kept as original string value."""
        mock_session.run = AsyncMock(
            return_value=make_async_result([{"total_count": 0}])
        )
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"age_at_diagnosis": "unknown"}
        )
        assert result == {"counts": {"total": 0}}
        params = mock_session.run.call_args[0][1]
        assert "unknown" in params.values()


# ---------------------------------------------------------------------------
# Tests: _get_samples_summary_reverse_query
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestReverseQuery:
    """Tests for _get_samples_summary_reverse_query."""

    async def test_reverse_query_library_source_material(self, repository, mock_session):
        """library_source_material → sf.library_source_material = $param."""
        mock_record = {"total_count": 20}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"library_source_material": "Genomic DNA"}
        )
        assert result == {"counts": {"total": 20}}
        query = mock_session.run.call_args[0][0]
        assert "sf.library_source_material" in query

    async def test_reverse_query_library_strategy(self, repository, mock_session):
        """library_strategy='WGS' → no reverse mapping → direct equality."""
        mock_record = {"total_count": 30}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"library_strategy": "WGS"}
        )
        assert result == {"counts": {"total": 30}}
        query = mock_session.run.call_args[0][0]
        assert "sf.library_strategy" in query

    async def test_reverse_query_library_selection_method(self, repository, mock_session):
        """library_selection_method → sf.library_selection = $param."""
        mock_record = {"total_count": 11}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"library_selection_method": "PCR"}
        )
        assert result == {"counts": {"total": 11}}
        query = mock_session.run.call_args[0][0]
        assert "sf.library_selection" in query

    async def test_reverse_query_specimen_molecular_analyte_type_list(self, repository, mock_session):
        """'RNA' reverse-maps to ['Transcriptomic','Viral RNA'] → IN literal list."""
        mock_record = {"total_count": 14}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"specimen_molecular_analyte_type": "RNA"}
        )
        assert result == {"counts": {"total": 14}}
        query = mock_session.run.call_args[0][0]
        assert "library_source_molecule" in query
        assert "Transcriptomic" in query
        assert "Viral RNA" in query

    async def test_reverse_query_specimen_molecular_analyte_type_string(self, repository, mock_session):
        """'DNA' reverse-maps to 'Genomic' (string) → = $param."""
        mock_record = {"total_count": 6}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"specimen_molecular_analyte_type": "DNA"}
        )
        assert result == {"counts": {"total": 6}}
        query = mock_session.run.call_args[0][0]
        assert "library_source_molecule" in query
        params = mock_session.run.call_args[0][1]
        assert "Genomic" in params.values()

    async def test_reverse_query_null_mapped_library_source(self, repository, mock_session):
        """'Other' is null-mapped for library_source_material → returns 0 without session."""
        result = await repository._get_samples_summary_reverse_query(
            {"library_source_material": "Other"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_reverse_query_db_only_library_strategy(self, repository, mock_session):
        """'Archer Fusion' is a DB-only value → returns 0 without session."""
        result = await repository._get_samples_summary_reverse_query(
            {"library_strategy": "Archer Fusion"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_reverse_query_combined_fields(self, repository, mock_session):
        """Multiple sequencing file fields → multiple WHERE conditions ANDed."""
        mock_record = {"total_count": 25}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {
                "library_strategy": "WGS",
                "library_source_material": "Genomic DNA",
            }
        )
        assert result == {"counts": {"total": 25}}
        query = mock_session.run.call_args[0][0]
        assert "sf.library_strategy" in query
        assert "sf.library_source_material" in query
        assert " AND " in query

    async def test_reverse_query_exception(self, repository, mock_session):
        """Exception from session.run is re-raised."""
        mock_session.run = AsyncMock(side_effect=RuntimeError("DB connection lost"))

        with pytest.raises(RuntimeError, match="DB connection lost"):
            await repository._get_samples_summary_reverse_query(
                {"library_strategy": "WGS"}
            )

    async def test_reverse_query_library_strategy_other_mapped(self, repository, mock_session):
        """'Other' reverse-maps to 'Archer Fusion' → OR clause with both values."""
        mock_record = {"total_count": 2}
        mock_session.run = AsyncMock(return_value=make_single_result(mock_record))

        result = await repository._get_samples_summary_reverse_query(
            {"library_strategy": "Other"}
        )
        assert result == {"counts": {"total": 2}}
        query = mock_session.run.call_args[0][0]
        assert "OR" in query
        params = mock_session.run.call_args[0][1]
        assert "Archer Fusion" in params.values()
        assert "Other" in params.values()

    async def test_reverse_query_null_single_result(self, repository, mock_session):
        """result.single() returns None → total=0."""
        mock_session.run = AsyncMock(return_value=make_single_result(None))

        result = await repository._get_samples_summary_reverse_query(
            {"library_strategy": "WGS"}
        )
        assert result == {"counts": {"total": 0}}

    async def test_reverse_query_specimen_null_mapped(self, repository, mock_session):
        """'Not Reported' is null-mapped for specimen_molecular_analyte_type → returns 0."""
        result = await repository._get_samples_summary_reverse_query(
            {"specimen_molecular_analyte_type": "Not Reported"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()
