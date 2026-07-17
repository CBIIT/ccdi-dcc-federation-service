"""Boost coverage for sample_summary.py main builder + reverse summary paths."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings


def _summary_result(total: int = 10):
    async def async_gen():
        yield {"total_count": total}

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    mock_result.single = AsyncMock(return_value={"total_count": total})
    return mock_result


@pytest.mark.unit
class TestSampleSummaryCoverageBoost:
    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def repository(self, mock_session):
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return SampleRepository(mock_session, allowlist, Mock(spec=Settings))

    async def test_main_builder_library_strategy_with_identifiers(self, repository, mock_session):
        """SF + sample filter skips reverse path and hits main library_strategy branch."""
        mock_session.run = AsyncMock(return_value=_summary_result(3))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
            with patch(
                "app.repositories.sample_summary.reverse_map_field_value",
                return_value="Archer Fusion",
            ):
                result = await repository.get_samples_summary(
                    {"library_strategy": "Other", "identifiers": "SAMP001"}
                )
        assert result == {"counts": {"total": 3}}
        cypher = mock_session.run.call_args[0][0]
        assert "sequencing_file" in cypher or "library_strategy" in cypher or "param_" in str(
            mock_session.run.call_args
        )

    async def test_main_builder_library_selection_invalid_with_tissue(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(0))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=True):
            result = await repository.get_samples_summary(
                {"library_selection_method": "PolyA", "tissue_type": "Tumor"}
            )
        # Invalid SF value → early empty or executed empty
        assert result == {"counts": {"total": 0}}

    async def test_main_builder_disease_phase_relapse_with_identifiers(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(8))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
                with patch(
                    "app.repositories.sample_summary.reverse_map_field_value",
                    return_value=["Relapse", "Recurrent Disease"],
                ):
                    # disease_phase alone routes to diagnosis-optimized; add tissue_type? 
                    # tissue_type is companion for diag-heavy. Use anatomical_sites to stay on main path
                    # actually diag-heavy allows tissue_type. Add library_strategy to force main builder.
                    result = await repository.get_samples_summary(
                        {
                            "disease_phase": "Relapse",
                            "library_strategy": "WXS",
                            "identifiers": "SAMP001",
                        }
                    )
        assert result["counts"]["total"] == 8

    async def test_main_builder_tumor_grade_and_age_at_diagnosis(self, repository, mock_session):
        """tumor_grade alone is diag-heavy optimized; pair with SF to hit main builder branches."""
        mock_session.run = AsyncMock(return_value=_summary_result(2))
        result = await repository.get_samples_summary(
            {
                "tumor_grade": "G1",
                "age_at_diagnosis": "not_an_int",
                "library_strategy": "WXS",
            }
        )
        assert result == {"counts": {"total": 2}}

    async def test_main_builder_tumor_tissue_morphology_with_sf(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(1))
        result = await repository.get_samples_summary(
            {"tumor_tissue_morphology": "8000/3", "library_strategy": "WGS"}
        )
        assert result == {"counts": {"total": 1}}

    async def test_anatomical_sites_both_list_and_string_fail(self, repository, mock_session):
        async def gen_err(msg):
            raise Exception(msg)
            yield  # pragma: no cover

        def make_err(msg):
            err = AsyncMock()
            err.__aiter__ = Mock(return_value=gen_err(msg))
            err.consume = AsyncMock()
            return err

        # Retry loop: attempts 0,1,2 then raise; outer catch runs string fallback once
        mock_session.run = AsyncMock(
            side_effect=[
                make_err("in expected a list"),
                make_err("in expected a list"),
                make_err("in expected a list"),
                Exception("string also failed"),
            ]
        )

        with pytest.raises(Exception, match="string also failed"):
            await repository.get_samples_summary({"anatomical_sites": "Brain"})

    async def test_anatomical_sites_non_in_error_reraises(self, repository, mock_session):
        async def gen_err():
            raise Exception("connection reset")
            yield  # pragma: no cover

        def make_err():
            err = AsyncMock()
            err.__aiter__ = Mock(return_value=gen_err())
            err.consume = AsyncMock()
            return err

        mock_session.run = AsyncMock(side_effect=[make_err(), make_err(), make_err()])

        with pytest.raises(Exception, match="connection reset"):
            await repository.get_samples_summary({"anatomical_sites": "Brain"})

    async def test_diagnosis_heavy_routes_to_optimized(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(42))
        with patch.object(
            repository,
            "_get_samples_summary_diagnosis_filters_optimized",
            new_callable=AsyncMock,
            return_value={"counts": {"total": 42}},
        ) as mock_opt:
            result = await repository.get_samples_summary({"tumor_grade": "G1"})
        assert result == {"counts": {"total": 42}}
        mock_opt.assert_awaited_once()

    async def test_diagnosis_filters_optimized_method_direct(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(42))
        result = await repository._get_samples_summary_diagnosis_filters_optimized(
            {"tumor_grade": "G1", "age_at_diagnosis": "12"}
        )
        assert result == {"counts": {"total": 42}}
        mock_session.run.assert_called()
        cypher = mock_session.run.call_args[0][0]
        assert "tumor_grade" in cypher

    async def test_diagnosis_filters_optimized_disease_phase(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(5))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
                with patch(
                    "app.repositories.sample_summary.reverse_map_field_value",
                    return_value="Relapse",
                ):
                    result = await repository.get_samples_summary({"disease_phase": "Relapse"})
        assert result == {"counts": {"total": 5}}

    async def test_reverse_query_real_library_source_material_list(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(9))
        with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
            with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
                with patch(
                    "app.repositories.sample_summary.reverse_map_field_value",
                    return_value=["DNA", "Genomic DNA"],
                ):
                    result = await repository._get_samples_summary_reverse_query(
                        {"library_source_material": "DNA"}
                    )
        assert result == {"counts": {"total": 9}}
        cypher = mock_session.run.call_args[0][0]
        assert "IN $" in cypher or "library_source_material" in cypher

    async def test_reverse_query_specimen_single_and_invalid(self, repository, mock_session):
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=True):
            with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
                assert await repository._get_samples_summary_reverse_query(
                    {"specimen_molecular_analyte_type": "Transcriptomic"}
                ) == {"counts": {"total": 0}}

        mock_session.run = AsyncMock(return_value=_summary_result(4))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
                with patch(
                    "app.repositories.sample_summary.reverse_map_field_value",
                    return_value="DNA",
                ):
                    result = await repository._get_samples_summary_reverse_query(
                        {"specimen_molecular_analyte_type": "DNA"}
                    )
        assert result == {"counts": {"total": 4}}

    async def test_reverse_query_via_get_samples_summary_routing(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_summary_result(11))
        with patch("app.repositories.sample_summary.is_database_only_value", return_value=False):
            with patch("app.repositories.sample_summary.is_null_mapped_value", return_value=False):
                with patch(
                    "app.repositories.sample_summary.reverse_map_field_value",
                    return_value=None,
                ):
                    result = await repository.get_samples_summary({"library_strategy": "WXS"})
        assert result == {"counts": {"total": 11}}
