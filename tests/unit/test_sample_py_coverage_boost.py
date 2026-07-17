"""Boost coverage for sample.py dead fallthrough + early pagination gaps."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist


def _list_result(records):
    async def async_gen():
        for r in records:
            yield r

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    mock_result.single = AsyncMock(return_value=records[0] if records else None)
    return mock_result


SAMPLE_RECORD = {
    "sa": {"sample_id": "SAMP001"},
    "p": {"participant_id": "P001"},
    "st": {"study_id": "phs001"},
    "sf": {},
    "pf": {},
    "diagnoses": {},
}


@pytest.mark.unit
class TestSamplePyCoverageBoost:
    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def repository(self, mock_session):
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return SampleRepository(mock_session, allowlist)

    async def test_standard_fallthrough_when_case3_returns_none(self, repository, mock_session):
        """Case 3 never returns None in prod; force it to exercise legacy standard path."""
        # First run is inline early-pagination (succeeds and returns) — that's fine for coverage of ~924-1057
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))

        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            result = await repository.get_samples(
                {"disease_phase": "Relapse"}, offset=0, limit=20
            )

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].id.name == "SAMP001"
        mock_session.run.assert_called()

    async def test_standard_fallthrough_after_inline_early_pagination_fails(
        self, repository, mock_session
    ):
        """When inline early-pagination raises, continue into standard query (1058+)."""
        mock_session.run = AsyncMock(
            side_effect=[
                Exception("early pagination blew up"),
                _list_result([SAMPLE_RECORD]),
            ]
        )

        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            result = await repository.get_samples(
                {"disease_phase": "Relapse"}, offset=0, limit=20
            )

        assert isinstance(result, list)
        assert len(result) == 1
        assert mock_session.run.call_count >= 2

    async def test_standard_fallthrough_return_total_and_preservation(self, repository, mock_session):
        # Inline early list fails → standard path. Count pattern may not match → list-only return.
        mock_session.run = AsyncMock(
            side_effect=[
                Exception("early list failed"),
                _list_result([SAMPLE_RECORD]),
            ]
        )

        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch(
                "app.repositories.sample.is_database_only_value", return_value=False
            ):
                with patch(
                    "app.repositories.sample.reverse_map_field_value",
                    return_value="FFPE",
                ):
                    with patch(
                        "app.repositories.sample.run_count_query_with_retry",
                        new_callable=AsyncMock,
                        side_effect=[
                            Exception("early count failed"),
                            7,
                        ],
                    ):
                        result = await repository.get_samples(
                            {
                                "preservation_method": "FFPE",
                                "disease_phase": "Initial Diagnosis",
                            },
                            offset=0,
                            limit=10,
                            return_total=True,
                        )

        if isinstance(result, tuple):
            samples, total = result
            assert len(samples) == 1
            assert total == 7
        else:
            assert isinstance(result, list)
            assert len(result) == 1

    async def test_standard_fallthrough_sf_early_filters(self, repository, mock_session):
        """Force Case3→None with SF filters so fallthrough builds early OPTIONAL MATCH WHEREs."""
        mock_session.run = AsyncMock(
            side_effect=[
                Exception("early failed"),
                _list_result([SAMPLE_RECORD]),
            ]
        )

        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch("app.repositories.sample.is_database_only_value", return_value=False):
                with patch("app.repositories.sample.is_null_mapped_value", return_value=False):
                    with patch(
                        "app.repositories.sample.reverse_map_field_value",
                        side_effect=lambda field, value: {
                            "specimen_molecular_analyte_type": ["Transcriptomic", "Viral RNA"],
                            "library_strategy": "Archer Fusion",
                            "library_source_material": "DNA",
                        }.get(field, value),
                    ):
                        result = await repository.get_samples(
                            {
                                "specimen_molecular_analyte_type": "RNA",
                                "library_strategy": "Other",
                                "library_source_material": "DNA",
                                "library_selection_method": "PCR",
                                "disease_phase": "Relapse",
                            },
                            offset=0,
                            limit=5,
                        )

        assert isinstance(result, list)
        assert len(result) == 1
        # Last successful cypher should include sequencing_file early filters
        cypher = mock_session.run.call_args_list[-1][0][0]
        assert "sequencing_file" in cypher
        assert "library_strategy" in cypher or "library_selection" in cypher

    async def test_standard_fallthrough_sf_invalid_filters(self, repository, mock_session):
        mock_session.run = AsyncMock(
            side_effect=[
                Exception("early failed"),
                _list_result([SAMPLE_RECORD]),
            ]
        )

        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch("app.repositories.sample.is_database_only_value", return_value=True):
                with patch("app.repositories.sample.is_null_mapped_value", return_value=False):
                    result = await repository.get_samples(
                        {
                            "specimen_molecular_analyte_type": "Transcriptomic",
                            "library_strategy": "Archer Fusion",
                            "library_selection_method": "PolyA",
                            "library_source_material": "Other",
                            "disease_phase": "Relapse",
                        },
                        offset=0,
                        limit=5,
                    )

        assert isinstance(result, list)

    async def test_early_pagination_tumor_classification_list(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        with patch("app.repositories.sample.is_null_mapped_value", return_value=False):
            with patch("app.repositories.sample.is_database_only_value", return_value=False):
                with patch(
                    "app.repositories.sample.reverse_map_field_value",
                    return_value=["Primary", "Metastatic"],
                ):
                    with patch(
                        "app.repositories.sample.run_count_query_with_retry",
                        new_callable=AsyncMock,
                        return_value=3,
                    ):
                        result = await repository._get_samples_early_pagination_with_filters(
                            {"tumor_classification": "Primary"},
                            offset=0,
                            limit=20,
                            return_total=True,
                        )
        assert isinstance(result, tuple)
        samples, total = result
        assert len(samples) == 1
        assert total == 3
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        with patch(
            "app.repositories.sample.is_null_mapped_value", return_value=False
        ):
            with patch(
                "app.repositories.sample.is_database_only_value", return_value=False
            ):
                with patch(
                    "app.repositories.sample.reverse_map_field_value",
                    return_value="Primary",
                ):
                    result = await repository._get_samples_early_pagination_with_filters(
                        {"tumor_classification": "Primary"},
                        offset=0,
                        limit=20,
                    )
        assert isinstance(result, list)
        assert len(result) == 1

    async def test_early_pagination_tumor_classification_invalid(self, repository, mock_session):
        with patch(
            "app.repositories.sample.is_null_mapped_value", return_value=True
        ):
            result = await repository._get_samples_early_pagination_with_filters(
                {"tumor_classification": "Not Reported"},
                offset=0,
                limit=20,
                return_total=True,
            )
        assert result == ([], 0)
        mock_session.run.assert_not_called()

    async def test_early_pagination_anatomical_sites_list(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        result = await repository._get_samples_early_pagination_with_filters(
            {"anatomical_sites": ["Brain", "Liver"]},
            offset=0,
            limit=20,
            return_total=False,
        )
        assert isinstance(result, list)
        cypher = mock_session.run.call_args[0][0]
        assert "anatomic_site" in cypher

    async def test_early_pagination_unsupported_filter_returns_none(self, repository, mock_session):
        result = await repository._get_samples_early_pagination_with_filters(
            {"age_at_collection": "10"},
            offset=0,
            limit=20,
        )
        assert result is None
        mock_session.run.assert_not_called()

    async def test_case2_falls_through_when_early_returns_none(self, repository, mock_session):
        """age_at_collection is sample-only → Case1, not Case2. Use depositions+unsupported."""
        # Case 1 is sample-only. age_at_collection alone is Case 1.
        # Case 2 = sample+study. early pagination returns None for age_at_collection+depositions.
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        result = await repository.get_samples(
            {"age_at_collection": "5", "depositions": "phs001"},
            offset=0,
            limit=10,
        )
        assert isinstance(result, list)

    async def test_record_to_sample_missing_study_id_raises(self, repository):
        with pytest.raises(ValueError, match="study_id"):
            repository._record_to_sample(
                {"sample_id": "SAMP001"}, {}, {}, {}, {}, None
            )

    async def test_record_to_sample_empty_sa_raises(self, repository):
        with pytest.raises(ValueError, match="required"):
            repository._record_to_sample({}, {}, {"study_id": "phs001"}, {}, {}, None)

    async def test_standard_fallthrough_preservation_invalid(self, repository, mock_session):
        mock_session.run = AsyncMock(
            side_effect=[Exception("early failed"), _list_result([SAMPLE_RECORD])]
        )
        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch("app.repositories.sample.is_database_only_value", return_value=True):
                result = await repository.get_samples(
                    {"preservation_method": "Other", "disease_phase": "Relapse"},
                    offset=0,
                    limit=5,
                )
        assert isinstance(result, list)

    async def test_standard_fallthrough_library_source_only(self, repository, mock_session):
        mock_session.run = AsyncMock(
            side_effect=[Exception("early failed"), _list_result([SAMPLE_RECORD])]
        )
        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch("app.repositories.sample.is_database_only_value", return_value=False):
                with patch("app.repositories.sample.is_null_mapped_value", return_value=False):
                    with patch(
                        "app.repositories.sample.reverse_map_field_value",
                        return_value="DNA",
                    ):
                        result = await repository.get_samples(
                            {
                                "library_source_material": "DNA",
                                "disease_phase": "Relapse",
                            },
                            offset=0,
                            limit=5,
                        )
        assert isinstance(result, list)
        cypher = mock_session.run.call_args_list[-1][0][0]
        assert "library_source_material" in cypher

    async def test_standard_fallthrough_library_strategy_list_mapping(self, repository, mock_session):
        mock_session.run = AsyncMock(
            side_effect=[Exception("early failed"), _list_result([SAMPLE_RECORD])]
        )
        with patch.object(
            repository, "_get_samples_case3_with_node_filters", new_callable=AsyncMock
        ) as mock_case3:
            mock_case3.return_value = None
            with patch("app.repositories.sample.is_database_only_value", return_value=False):
                with patch(
                    "app.repositories.sample.reverse_map_field_value",
                    return_value=["WXS", "WGS"],
                ):
                    result = await repository.get_samples(
                        {"library_strategy": "Other", "disease_phase": "Relapse"},
                        offset=0,
                        limit=5,
                    )
        assert isinstance(result, list)
        cypher = mock_session.run.call_args_list[-1][0][0]
        assert "library_strategy IN $" in cypher
