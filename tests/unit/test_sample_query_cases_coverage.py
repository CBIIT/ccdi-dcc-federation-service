"""
Targeted coverage for app.repositories.sample_query_cases — Case 1/2/3 branches.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch

from app.repositories.sample import SampleRepository
from app.repositories.sample_helpers import SD_CAT_MARKER
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings


def _empty_result():
    async def async_gen():
        yield {
            "sa": {"sample_id": "SAMP001"},
            "p": {"participant_id": "PART001"},
            "st": {"study_id": "phs001"},
            "sf": {},
            "pf": {},
            "diagnoses": [],
        }

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    return mock_result


def _count_result(total: int):
    async def async_gen():
        yield {"total_count": total}

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    return mock_result


def _categorized(
    sample=None,
    study=None,
    diagnosis=None,
    sequencing_file=None,
    pathology_file=None,
):
    return {
        "sample": sample or {},
        "study": study or {},
        "diagnosis": diagnosis or {},
        "sequencing_file": sequencing_file or {},
        "pathology_file": pathology_file or {},
    }


@pytest.mark.unit
class TestCase1DirectCoverage:
    @pytest.fixture
    def repository(self):
        session = AsyncMock()
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        settings = Mock(spec=Settings)
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.sample_count_fields = []
        return SampleRepository(session, allowlist, settings)

    @pytest.mark.asyncio
    async def test_invalid_tissue_return_total_returns_empty_tuple(self, repository):
        result = await repository._get_samples_case1_sample_only(
            {"tissue_type": "NotARealTissue"},
            offset=0,
            limit=20,
            base_url=None,
            return_total=True,
        )
        assert result == ([], 0)
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_age_at_collection_non_integer_kept_as_string(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        await repository._get_samples_case1_sample_only(
            {"age_at_collection": "not-a-number"},
            offset=0,
            limit=20,
            base_url=None,
            return_total=False,
        )
        params = repository.session.run.call_args[0][1]
        assert params.get("param_1") == "not-a-number"

    @pytest.mark.asyncio
    async def test_identifiers_pipe_list_uses_in_clause(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        await repository._get_samples_case1_sample_only(
            {"identifiers": "SAMP001||SAMP002"},
            offset=0,
            limit=20,
            base_url=None,
            return_total=False,
        )
        query = repository.session.run.call_args[0][0]
        assert "sa.sample_id IN" in query
        params = repository.session.run.call_args[0][1]
        assert ["SAMP001", "SAMP002"] in list(params.values())

    @pytest.mark.asyncio
    async def test_anatomical_sites_list_builds_or_conditions(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        await repository._get_samples_case1_sample_only(
            {"anatomical_sites": ["Brain", "Liver"]},
            offset=0,
            limit=20,
            base_url=None,
            return_total=False,
        )
        query = repository.session.run.call_args[0][0]
        assert "any(tok IN coalesce(sa.anatomic_site, [])" in query
        assert "param_1_0" in repository.session.run.call_args[0][1]

    @pytest.mark.asyncio
    async def test_count_query_failure_returns_bare_list(self, repository):
        """Count query failure must not report total=0 while samples has real rows.

        Returning a bare list signals SampleService to fall back to a summary
        recompute instead of surfacing a fabricated zero total.
        """
        repository.session.run = AsyncMock(
            side_effect=[Exception("count failed"), _empty_result()]
        )
        result = await repository._get_samples_case1_sample_only(
            {"tissue_type": "Tumor"},
            offset=0,
            limit=20,
            base_url=None,
            return_total=True,
        )
        assert isinstance(result, list)
        assert len(result) == 1


@pytest.mark.unit
class TestCase2DirectCoverage:
    @pytest.fixture
    def repository(self):
        session = AsyncMock()
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        settings = Mock(spec=Settings)
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.sample_count_fields = []
        return SampleRepository(session, allowlist, settings)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_early_pagination_empty(self, repository):
        with patch.object(
            repository,
            "_get_samples_early_pagination_with_filters",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await repository._get_samples_case2_sample_study(
                {"depositions": "phs001"},
                offset=0,
                limit=20,
                base_url=None,
                return_total=False,
            )
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_none_when_early_pagination_unsupported(self, repository):
        with patch.object(
            repository,
            "_get_samples_early_pagination_with_filters",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await repository._get_samples_case2_sample_study(
                {"depositions": "phs001"},
                offset=0,
                limit=20,
                base_url=None,
                return_total=False,
            )
        assert result is None


@pytest.mark.unit
class TestCase3FilterBranches:
    @pytest.fixture
    def repository(self):
        session = AsyncMock()
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        allowlist.is_allowed = Mock(return_value=True)
        settings = Mock(spec=Settings)
        settings.pagination = Mock()
        settings.pagination.max_page_size = 1000
        settings.sample_count_fields = []
        return SampleRepository(session, allowlist, settings)

    @pytest.mark.asyncio
    async def test_sample_identifiers_and_depositions_in_query(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sample={"identifiers": "SAMP001||SAMP002"},
            study={"depositions": "phs001 || phs002"},
            diagnosis={"tumor_grade": "G1"},
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "sa.sample_id IN" in query
        assert "st.study_id IN" in query
        assert "d.tumor_grade" in query

    @pytest.mark.asyncio
    async def test_invalid_tissue_type_return_total(self, repository):
        cat = _categorized(
            sample={"tissue_type": "BadTissue"},
            diagnosis={"disease_phase": "Initial Diagnosis"},
        )
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=True
        )
        assert result == ([], 0)

    @pytest.mark.asyncio
    async def test_anatomical_sites_and_age_at_collection(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sample={"anatomical_sites": "Brain", "age_at_collection": "abc"},
            diagnosis={"tumor_tissue_morphology": "Astrocytoma"},
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "sa.anatomic_site IS NOT NULL" in query
        assert "participant_age_at_collection" in query
        assert "d.tumor_tissue_morphology" in query

    @pytest.mark.asyncio
    async def test_tumor_classification_database_only_returns_empty(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(sample={"tumor_classification": "Local"})
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_diagnosis_text_and_age_at_diagnosis_non_int(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            diagnosis={"diagnosis": "Neuroblastoma", "age_at_diagnosis": "unknown"},
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "see diagnosis_comment" in query
        assert "d.age_at_diagnosis" in query
        assert repository.session.run.call_args[0][1].get("param_2") == "unknown"

    @pytest.mark.asyncio
    async def test_diagnosis_category_substring_marker(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            diagnosis={
                SD_CAT_MARKER: True,
                "diagnosis_category": "lymphoma",
            },
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "diag_category_contains_term" in repository.session.run.call_args[0][1]
        assert "CONTAINS" in query

    @pytest.mark.asyncio
    async def test_search_plus_tumor_grade_combined_in_pre_unwind(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            diagnosis={"_diagnosis_search": "leukemia", "tumor_grade": "G2"},
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "coalesce(d.diagnosis" in query
        assert "d.tumor_grade" in query
        assert "size(all_diagnoses) > 0" in query

    @pytest.mark.asyncio
    async def test_library_selection_method_valid(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(sequencing_file={"library_selection_method": "Hybrid Selection"})
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=False,
        ):
            with patch.object(
                repository,
                "_reverse_map_library_selection_method_static",
                return_value="Hybrid Selection",
            ):
                await repository._get_samples_case3_with_node_filters(
                    {}, cat, offset=0, limit=20, base_url=None, return_total=False
                )
        query = repository.session.run.call_args[0][0]
        assert "sf.library_selection" in query

    @pytest.mark.asyncio
    async def test_library_selection_method_invalid_early_exit(self, repository):
        cat = _categorized(
            sequencing_file={"library_selection_method": "InvalidMethod"},
        )
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=True,
        ):
            result = await repository._get_samples_case3_with_node_filters(
                {}, cat, offset=0, limit=20, base_url=None, return_total=True
            )
        assert result == ([], 0)

    @pytest.mark.asyncio
    async def test_library_source_material_list_uses_in(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(sequencing_file={"library_source_material": "Bulk Tissue"})
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=False,
        ):
            with patch(
                "app.repositories.sample_query_cases.is_null_mapped_value",
                return_value=False,
            ):
                with patch(
                    "app.repositories.sample_query_cases.reverse_map_field_value",
                    return_value=["Bulk Tissue", "Cell Line"],
                ):
                    await repository._get_samples_case3_with_node_filters(
                        {}, cat, offset=0, limit=20, base_url=None, return_total=False
                    )
        query = repository.session.run.call_args[0][0]
        assert "sf.library_source_material IN" in query

    @pytest.mark.asyncio
    async def test_specimen_molecular_analyte_type_list_embeds_in_literal(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sequencing_file={"specimen_molecular_analyte_type": "RNA"},
        )
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=False,
        ):
            with patch(
                "app.repositories.sample_query_cases.reverse_map_field_value",
                return_value=["MicroRNA", "Total RNA"],
            ):
                await repository._get_samples_case3_with_node_filters(
                    {}, cat, offset=0, limit=20, base_url=None, return_total=False
                )
        query = repository.session.run.call_args[0][0]
        assert "sf.library_source_molecule IN ['MicroRNA', 'Total RNA']" in query

    @pytest.mark.asyncio
    async def test_specimen_molecular_analyte_type_db_only_early_exit(self, repository):
        """DB-only 'Total RNA' is rejected; clients must filter by API PV 'RNA'."""
        cat = _categorized(
            sequencing_file={"specimen_molecular_analyte_type": "Total RNA"},
        )
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_specimen_molecular_analyte_type_null_mapped_early_exit(self, repository):
        """Null-mapped 'Not Reported' is rejected as a filter."""
        cat = _categorized(
            sequencing_file={"specimen_molecular_analyte_type": "Not Reported"},
        )
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_preservation_method_db_only_early_exit(self, repository):
        """DB-only 'Cytospin Slide' is rejected; clients must filter by API PV 'Unknown'."""
        cat = _categorized(pathology_file={"preservation_method": "Cytospin Slide"})
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_preservation_method_unknown_is_valid_filter(self, repository):
        """'Unknown' is null-mapped for responses but must still be accepted as a filter."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(pathology_file={"preservation_method": "Unknown"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        params = repository.session.run.call_args[0][1]
        assert "pf.fixation_embedding_method IN" in query
        assert ["Cytospin Slide", "Other"] in params.values()

    @pytest.mark.asyncio
    async def test_pathology_only_enrichment_without_node_filters(self, repository):
        """Only pathology filter: diagnosis enrichment projection, head(sf) pick."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(pathology_file={"preservation_method": "FFPE"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "OPTIONAL MATCH (d:diagnosis)" in query
        assert "head(collect(DISTINCT sf)) AS sf" in query
        assert "diagnoses" in query

    @pytest.mark.asyncio
    async def test_return_total_with_pathology_uses_pf_count_branches(self, repository):
        repository.session.run = AsyncMock(
            side_effect=[_count_result(15), _empty_result()]
        )
        cat = _categorized(
            diagnosis={"disease_phase": "Initial Diagnosis"},
            pathology_file={"preservation_method": "FFPE"},
        )
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=True
        )
        count_query = repository.session.run.call_args_list[0][0][0]
        assert "collect(DISTINCT pf) AS all_pfs" in count_query
        assert "size([pf IN all_pfs WHERE pf IS NOT NULL]) > 0" in count_query
        assert result[1] == 15

    @pytest.mark.asyncio
    async def test_count_query_failure_returns_bare_list(self, repository):
        """Count query failure must not report total=0 while samples has real rows.

        Returning a bare list signals SampleService to fall back to a summary
        recompute instead of surfacing a fabricated zero total.
        """
        repository.session.run = AsyncMock(
            side_effect=[Exception("count boom"), _empty_result()]
        )
        cat = _categorized(diagnosis={"disease_phase": "Initial Diagnosis"})
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=True
        )
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_single_identifier_and_anatomical_sites_list(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sample={"identifiers": "SAMP999", "anatomical_sites": ["Brain", "Liver"]},
            diagnosis={"disease_phase": "Relapse"},
        )
        with patch(
            "app.repositories.sample_query_cases.reverse_map_field_value",
            return_value=["Recurrent Disease", "Relapse"],
        ):
            await repository._get_samples_case3_with_node_filters(
                {}, cat, offset=0, limit=20, base_url=None, return_total=False
            )
        query = repository.session.run.call_args[0][0]
        assert "sa.sample_id = $param_1" in query
        assert "param_2_0" in repository.session.run.call_args[0][1]
        assert "d.disease_phase IN" in query

    @pytest.mark.asyncio
    async def test_disease_phase_unknown_uses_in_clause(self, repository):
        """API 'Unknown' reverse-maps to multiple DB spellings → IN clause."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"disease_phase": "Unknown"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        params = repository.session.run.call_args[0][1]
        query = repository.session.run.call_args[0][0]
        assert "d.disease_phase IN" in query
        assert ["Not Applicable", "Prior Primary", "Synchronous", "Unknown"] in params.values()

    @pytest.mark.asyncio
    async def test_disease_phase_db_only_early_exit(self, repository):
        """DB-only 'Not Applicable' is rejected as an API filter."""
        cat = _categorized(diagnosis={"disease_phase": "Not Applicable"})
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "categorized,expected",
        [
            (
                {"diagnosis": {"disease_phase": "Not Applicable"}},
                ([], 0),
            ),
            (
                {"diagnosis": {"disease_phase": "Recurrent Disease"}},
                ([], 0),
            ),
            (
                {"pathology_file": {"preservation_method": "Cytospin Slide"}},
                ([], 0),
            ),
            (
                {"pathology_file": {"preservation_method": "Other"}},
                ([], 0),
            ),
            (
                {"sequencing_file": {"specimen_molecular_analyte_type": "Total RNA"}},
                ([], 0),
            ),
            (
                {"sequencing_file": {"library_source_material": "Other"}},
                ([], 0),
            ),
            (
                {"sequencing_file": {"library_strategy": "Archer Fusion"}},
                ([], 0),
            ),
        ],
    )
    async def test_case3_db_only_return_total_tuple(self, repository, categorized, expected):
        """DB-only / invalid filters with return_total=True must return ([], 0), not []."""
        cat = _categorized(**categorized)
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=True
        )
        assert result == expected
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_disease_phase_relapse_uses_in_clause(self, repository):
        """API 'Relapse' reverse-maps to Recurrent Disease + Relapse → IN clause."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"disease_phase": "Relapse"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        params = repository.session.run.call_args[0][1]
        query = repository.session.run.call_args[0][0]
        assert "d.disease_phase IN" in query
        assert ["Recurrent Disease", "Relapse"] in params.values()

    @pytest.mark.asyncio
    async def test_diagnosis_category_low_grade_gliomas_lowered(self, repository):
        """Canonical Low-Grade Gliomas expands to lowercased DB alias spellings."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"diagnosis_category": "Low-Grade Gliomas"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        params = repository.session.run.call_args[0][1]
        assert params["diag_category_filters"] == ["low-grade gliomas"]

    @pytest.mark.asyncio
    async def test_disease_phase_null_mapped_with_search(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            diagnosis={"_diagnosis_search": "tumor", "disease_phase": "Not Reported"},
        )
        with patch(
            "app.repositories.sample_query_cases.is_null_mapped_value",
            return_value=True,
        ):
            await repository._get_samples_case3_with_node_filters(
                {}, cat, offset=0, limit=20, base_url=None, return_total=False
            )
        query = repository.session.run.call_args[0][0]
        assert "d.disease_phase = $" in query

    @pytest.mark.asyncio
    async def test_diagnosis_category_exact_token_predicate(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"diagnosis_category": "Lymphoma"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        params = repository.session.run.call_args[0][1]
        assert "diag_category_filters" in params
        assert params["diag_category_filters"] == ["lymphoma"]
        # 3.11 list-native: exact-token match iterates the LIST, no SPLIT idiom
        assert "coalesce(d.diagnosis_category, [])" in query
        assert "$diag_category_filters" in query
        assert "split(toString" not in query

    @pytest.mark.asyncio
    async def test_diagnosis_category_reverse_maps_myeloid_leukemia(self, repository):
        """API 'Myeloid Leukemia' must also match DB 'Myeloid leukemias'."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"diagnosis_category": "Myeloid Leukemia"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        params = repository.session.run.call_args[0][1]
        assert params["diag_category_filters"] == ["myeloid leukemias", "myeloid leukemia"]

    @pytest.mark.asyncio
    async def test_library_selection_fallback_to_raw_value(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sequencing_file={"library_selection_method": "PCR"},
        )
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=False,
        ):
            with patch.object(
                repository,
                "_reverse_map_library_selection_method_static",
                return_value=None,
            ):
                await repository._get_samples_case3_with_node_filters(
                    {}, cat, offset=0, limit=20, base_url=None, return_total=False
                )
        params = repository.session.run.call_args[0][1]
        assert params.get("param_1") == "PCR"

    @pytest.mark.asyncio
    async def test_library_source_material_db_only_early_exit(self, repository):
        cat = _categorized(
            sequencing_file={"library_source_material": "Other"},
        )
        # Real is_database_only_value: "Other" is DB-only (maps to API "Not Reported")
        result = await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        assert result == []
        repository.session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_library_source_material_not_reported_uses_in(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sequencing_file={"library_source_material": "Not Reported"},
        )
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        params = repository.session.run.call_args[0][1]
        assert "sf.library_source_material IN $" in query
        assert ["Other", "Not Reported"] in params.values()

    @pytest.mark.asyncio
    async def test_specimen_molecular_analyte_type_scalar_mapping(self, repository):
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(
            sequencing_file={"specimen_molecular_analyte_type": "Protein"},
        )
        with patch(
            "app.repositories.sample_query_cases.is_database_only_value",
            return_value=False,
        ):
            with patch(
                "app.repositories.sample_query_cases.reverse_map_field_value",
                return_value="Protein",
            ):
                await repository._get_samples_case3_with_node_filters(
                    {}, cat, offset=0, limit=20, base_url=None, return_total=False
                )
        query = repository.session.run.call_args[0][0]
        assert "sf.library_source_molecule = $param_1" in query

    @pytest.mark.asyncio
    async def test_diagnosis_only_pick_uses_head_sf(self, repository):
        """Diagnosis-only filter without sf filters uses bare sf in pick clause."""
        repository.session.run = AsyncMock(return_value=_empty_result())
        cat = _categorized(diagnosis={"tumor_grade": "G3"})
        await repository._get_samples_case3_with_node_filters(
            {}, cat, offset=0, limit=20, base_url=None, return_total=False
        )
        query = repository.session.run.call_args[0][0]
        assert "head([sf IN all_sf" not in query
        assert "head(collect(DISTINCT sf)) AS sf" in query or ", sf" in query.split("pick")[0]
