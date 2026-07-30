"""Coverage campaign for sample.py miss clusters (converters + specialized query paths).

Note: tests that forced get_samples()'s now-removed "standard fallthrough" block
(via mocking _get_samples_case3_with_node_filters to return None, which never
happens in production) were deleted along with that dead code. Remaining tests
target:
- record conversion skip / _record_to_sample edge values
- SF/PF specialized path edge mappings
- diagnosis endpoint summary fallback behavior
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
    "sa": {
        "sample_id": "SAMP001",
        "participant_age_at_collection": -999,
        "anatomic_site": 42,
    },
    "p": {"participant_id": "P001"},
    "st": {"study_id": "phs001"},
    "sf": {
        "library_selection": "PCR",
        "library_strategy": "WXS",
        "library_source_material": "DNA",
        "library_source_molecule": "Transcriptomic",
    },
    "pf": {"fixation_embedding_method": "Invalid value"},
    "diagnoses": {
        "disease_phase": "Invalid value",
        "age_at_diagnosis": -999,
        "diagnosis": "Dx",
    },
}


@pytest.mark.unit
class TestSamplePyCoverageCampaign:
    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def repository(self, mock_session):
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return SampleRepository(mock_session, allowlist)

    async def test_record_to_sample_invalid_and_neg999_metadata(self, repository):
        sample = repository._record_to_sample(
            SAMPLE_RECORD["sa"],
            SAMPLE_RECORD["p"],
            SAMPLE_RECORD["st"],
            SAMPLE_RECORD["sf"],
            SAMPLE_RECORD["pf"],
            SAMPLE_RECORD["diagnoses"],
        )
        assert sample.id.name == "SAMP001"
        md = sample.metadata
        assert md is not None
        # -999 sentinels nulled; "Invalid value" disease_phase / preservation nulled
        assert md.age_at_collection is None
        assert md.age_at_diagnosis is None
        assert md.disease_phase is None
        assert md.preservation_method is None

    async def test_record_to_sample_semicolon_and_non_string_anatomical_sites(
        self, repository
    ):
        sample = repository._record_to_sample(
            {
                "sample_id": "SAMP002",
                "anatomic_site": "Brain; Liver; Invalid value",
            },
            {"participant_id": "P2"},
            {"study_id": "phs002"},
            {},
            {},
            None,
        )
        assert sample.id.name == "SAMP002"
        sites = sample.metadata.anatomical_sites
        assert sites is not None
        values = [s.value for s in sites]
        assert values == ["Brain", "Liver"]


    async def test_early_pagination_tumor_classification_invalid_no_total(
        self, repository, mock_session
    ):
        with patch("app.repositories.sample.is_null_mapped_value", return_value=True):
            result = await repository._get_samples_early_pagination_with_filters(
                {"tumor_classification": "Not Reported"},
                offset=0,
                limit=20,
                return_total=False,
            )
        assert result == []
        mock_session.run.assert_not_called()

    async def test_sf_only_library_source_single_and_selection(
        self, repository, mock_session
    ):
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        with patch("app.repositories.sample.is_null_mapped_value", return_value=False):
            with patch("app.repositories.sample.is_database_only_value", return_value=False):
                with patch(
                    "app.repositories.sample.reverse_map_field_value",
                    side_effect=lambda field, value: {
                        "library_source_material": "DNA",
                        "library_strategy": "WXS",
                    }.get(field, value),
                ):
                    with patch.object(
                        SampleRepository,
                        "_reverse_map_library_selection_method_static",
                        return_value="PCR",
                    ):
                        result = await repository._get_samples_by_sequencing_file_filters(
                            {
                                "library_source_material": "DNA",
                                "library_selection_method": "PCR",
                                "library_strategy": "WXS",
                            },
                            offset=0,
                            limit=10,
                        )
        assert isinstance(result, list)
        cypher = mock_session.run.call_args[0][0]
        assert "library_source_material = $" in cypher
        assert "library_selection = $" in cypher

    async def test_pf_only_preservation_list_mapping(self, repository, mock_session):
        mock_session.run = AsyncMock(return_value=_list_result([SAMPLE_RECORD]))
        with patch("app.repositories.sample.is_database_only_value", return_value=False):
            with patch(
                "app.repositories.sample.reverse_map_field_value",
                return_value=["FFPE", "Frozen"],
            ):
                result = await repository._get_samples_by_pathology_file_filters(
                    {"preservation_method": "FFPE"},
                    offset=0,
                    limit=10,
                )
        assert isinstance(result, list)
        cypher = mock_session.run.call_args[0][0]
        assert "fixation_embedding_method IN $" in cypher

    async def test_null_if_invalid_list_and_neg999_and_blank_wrap(self, repository):
        sample = repository._record_to_sample(
            {
                "sample_id": "SAMP003",
                "participant_age_at_collection": "not-a-number",
                "anatomic_site": ["Brain", "Invalid value", ""],
                "tumor_spatial_extent": -999,
            },
            {"participant_id": "P3"},
            {"study_id": "phs003"},
            {"library_selection": ["PCR", "Invalid value"]},
            {},
            {
                "disease_phase": ["Initial Diagnosis", "Invalid value"],
                "age_at_diagnosis": "bad",
                "diagnosis": "Dx",
            },
        )
        assert sample.id.name == "SAMP003"
        assert sample.metadata is not None

    async def test_reverse_map_library_selection_list_in_record(self, repository):
        with patch(
            "app.repositories.sample.reverse_map_field_value",
            side_effect=lambda field, value: (
                ["PolyA", "PCR"] if field == "library_selection_method" else value
            ),
        ):
            sample = repository._record_to_sample(
                {"sample_id": "SAMP004"},
                {"participant_id": "P4"},
                {"study_id": "phs004"},
                {"library_selection": "PolyA"},
                {},
                None,
            )
        assert sample.id.name == "SAMP004"

    async def test_diagnosis_endpoint_summary_total_zero_not_replaced_by_page_len(
        self, repository
    ):
        """Legitimate summary total=0 must not be replaced by len(samples)."""
        with patch.object(
            repository, "get_samples", new_callable=AsyncMock, return_value=[Mock()]
        ):
            with patch.object(
                repository,
                "get_samples_summary",
                new_callable=AsyncMock,
                return_value={"counts": {"total": 0}},
            ):
                samples, total = await repository.get_samples_for_diagnosis_endpoint(
                    {"tissue_type": "Tumor"}, offset=0, limit=5
                )
        assert len(samples) == 1
        assert total == 0

    async def test_get_samples_summary_does_not_mutate_caller_filters(
        self, repository, mock_session
    ):
        """Source-level copy: caller dict retains keys after summary .pop()."""
        async def async_gen():
            yield {"total_count": 3}

        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)

        # tissue_type keeps this off the diagnosis-search-only route so the
        # main-path .pop("identifiers") / .pop("_diagnosis_search") run.
        filters = {
            "identifiers": "SAMP001",
            "_diagnosis_search": "glioma",
            "tissue_type": "Tumor",
        }
        with patch.object(repository, "_validate_tissue_type_filter", return_value=True):
            result = await repository.get_samples_summary(filters)
        assert filters == {
            "identifiers": "SAMP001",
            "_diagnosis_search": "glioma",
            "tissue_type": "Tumor",
        }
        assert result["counts"]["total"] == 3

    async def test_diagnosis_endpoint_summary_fallback_uses_summary_total(
        self, repository
    ):
        with patch.object(
            repository, "get_samples", new_callable=AsyncMock, return_value=[Mock()]
        ):
            with patch.object(
                repository,
                "get_samples_summary",
                new_callable=AsyncMock,
                return_value={"counts": {"total": 99}},
            ):
                samples, total = await repository.get_samples_for_diagnosis_endpoint(
                    {"_diagnosis_search": "glioma", "identifiers": "SAMP001"},
                    offset=0,
                    limit=5,
                )
        assert len(samples) == 1
        assert total == 99
