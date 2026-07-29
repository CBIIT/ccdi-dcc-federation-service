"""Boost coverage for sample.py early pagination + record-conversion gaps.

Note: tests that forced get_samples()'s now-removed "standard fallthrough"
block (via mocking _get_samples_case3_with_node_filters to return None, which
never happens in production) were deleted along with that dead code.
"""

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

