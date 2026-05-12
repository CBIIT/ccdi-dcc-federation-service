"""
Unit tests for uncovered branches in sample_diagnosis_search.py.

Covers additional filter branches in both:
- _get_samples_by_diagnosis_search (list/pagination method)
- _get_samples_summary_diagnosis_search (summary method)

Filter branches tested: disease_phase (single + list mapping), tumor_grade,
tumor_classification (valid + null-mapped), tumor_tissue_morphology,
age_at_diagnosis (int + invalid), diagnosis_category, identifiers (single + multi),
depositions (single + multi), return_total, and combined filters.
"""

import pytest
from unittest.mock import AsyncMock, Mock
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings
from tests.unit.helpers import make_async_result


def _sample_record(sample_id: str = "SAMP001", study_id: str = "phs001") -> dict:
    """Minimal record dict that can be used as a fake DB row."""
    return {
        "sa": {"sample_id": sample_id},
        "p": {"participant_id": "PART001"},
        "st": {"study_id": study_id},
        "sf": {},
        "pf": {},
        "diagnoses": [{"diagnosis": "leukemia"}],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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
    s.pagination = Mock()
    s.pagination.max_page_size = 1000
    s.sample_count_fields = []
    return s


@pytest.fixture
def repository(mock_session, mock_allowlist, mock_settings):
    return SampleRepository(mock_session, mock_allowlist, mock_settings)


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — no diagnosis term
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryNoDiagnosisTerm:
    """Early-exit branch: missing/empty _diagnosis_search."""

    async def test_summary_search_no_diagnosis_term(self, repository, mock_session):
        """Empty filters returns {"counts": {"total": 0}} without touching the session."""
        result = await repository._get_samples_summary_diagnosis_search({})
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()

    async def test_summary_search_empty_string_diagnosis_term(self, repository, mock_session):
        """Empty-string _diagnosis_search also short-circuits."""
        result = await repository._get_samples_summary_diagnosis_search({"_diagnosis_search": ""})
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — basic call
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryBasicCall:
    """Summary with only a search term hits session.run exactly once."""

    async def test_summary_search_basic(self, repository, mock_session):
        mock_session.run.return_value = make_async_result([{"total_count": 42}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia"}
        )

        mock_session.run.assert_called_once()
        assert result == {"counts": {"total": 42}}

    async def test_summary_returns_zero_on_empty_db_result(self, repository, mock_session):
        """When DB returns no rows, total should be 0."""
        mock_session.run.return_value = make_async_result([])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia"}
        )
        assert result == {"counts": {"total": 0}}


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — disease_phase filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryDiseasePhaseFilter:
    """disease_phase single-value and list-mapping branches."""

    async def test_summary_search_with_disease_phase_single(self, repository, mock_session):
        """disease_phase that reverse-maps to a single string uses = clause."""
        mock_session.run.return_value = make_async_result([{"total_count": 10}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Initial Diagnosis"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        # "Initial Diagnosis" has no reverse mapping → treated as-is with = clause
        assert "dx.disease_phase" in query
        assert result["counts"]["total"] == 10

    async def test_summary_search_with_disease_phase_list_mapping(self, repository, mock_session):
        """'Relapse' reverse-maps to ['Recurrent Disease', 'Relapse'] → IN clause."""
        mock_session.run.return_value = make_async_result([{"total_count": 5}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Relapse"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        # Should use IN clause, not = clause
        assert "IN $" in query
        # The param value should be the list produced by the reverse mapping
        param_values = list(params.values())
        list_params = [v for v in param_values if isinstance(v, list)]
        assert any(
            set(lp) == {"Recurrent Disease", "Relapse"} for lp in list_params
        ), f"Expected ['Recurrent Disease', 'Relapse'] in params, got: {params}"
        assert result["counts"]["total"] == 5

    async def test_summary_search_database_only_disease_phase_returns_zero(
        self, repository, mock_session
    ):
        """A database-only disease_phase value (e.g., 'Recurrent Disease') short-circuits to 0."""
        # "Recurrent Disease" is in forward mappings but NOT in reverse_mappings →
        # is_database_only_value returns True → early return without calling session
        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Recurrent Disease"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — tumor_grade filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryTumorGradeFilter:

    async def test_summary_search_with_tumor_grade(self, repository, mock_session):
        mock_session.run.return_value = make_async_result([{"total_count": 7}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_grade": "Grade 1"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "dx.tumor_grade" in query
        assert "Grade 1" in params.values()
        assert result["counts"]["total"] == 7


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — tumor_classification filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryTumorClassificationFilter:

    async def test_summary_search_with_tumor_classification_valid(self, repository, mock_session):
        mock_session.run.return_value = make_async_result([{"total_count": 3}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_classification": "Primary"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        assert "dx.tumor_classification" in query
        assert result["counts"]["total"] == 3

    async def test_summary_search_with_invalid_tumor_classification(
        self, repository, mock_session
    ):
        """'non-malignant' is in null_mappings → early return without calling session."""
        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_classification": "non-malignant"}
        )
        assert result == {"counts": {"total": 0}}
        mock_session.run.assert_not_called()


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — tumor_tissue_morphology filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryTumorTissueMorphologyFilter:

    async def test_summary_search_with_tumor_tissue_morphology(self, repository, mock_session):
        mock_session.run.return_value = make_async_result([{"total_count": 2}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_tissue_morphology": "8000/0"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "dx.tumor_tissue_morphology" in query
        assert "8000/0" in params.values()
        assert result["counts"]["total"] == 2


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — age_at_diagnosis filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryAgeAtDiagnosisFilter:

    async def test_summary_search_with_age_at_diagnosis_int(self, repository, mock_session):
        """Numeric string is converted to int and injected into the query."""
        mock_session.run.return_value = make_async_result([{"total_count": 4}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "age_at_diagnosis": "5"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "toInteger(dx.age_at_diagnosis)" in query
        assert 5 in params.values(), f"Expected int 5 in params, got: {params}"
        assert result["counts"]["total"] == 4

    async def test_summary_search_with_age_at_diagnosis_invalid(self, repository, mock_session):
        """Non-numeric age_at_diagnosis falls through to the raw string value."""
        mock_session.run.return_value = make_async_result([{"total_count": 0}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "age_at_diagnosis": "not_a_number"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        params: dict = call_args[0][1]

        # Invalid conversion: raw string stays in params
        assert "not_a_number" in params.values()


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — diagnosis_category filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryDiagnosisCategoryFilter:

    async def test_summary_search_with_diagnosis_category(self, repository, mock_session):
        mock_session.run.return_value = make_async_result([{"total_count": 8}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "diagnosis_category": "Leukemia"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "dx.diagnosis_category" in query
        assert "Leukemia" in params.values()
        assert result["counts"]["total"] == 8

    async def test_summary_search_with_empty_diagnosis_category_skipped(
        self, repository, mock_session
    ):
        """Whitespace-only diagnosis_category is skipped (no filter appended)."""
        mock_session.run.return_value = make_async_result([{"total_count": 1}])

        await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "diagnosis_category": "   "}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        assert "dx.diagnosis_category" not in query


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — identifiers filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryIdentifiersFilter:

    async def test_summary_search_with_identifiers_single(self, repository, mock_session):
        """Single identifier string uses = clause in WHERE."""
        mock_session.run.return_value = make_async_result([{"total_count": 1}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "identifiers": "SAMPLE1"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sa.sample_id" in query
        assert "SAMPLE1" in params.values()
        assert result["counts"]["total"] == 1

    async def test_summary_search_with_identifiers_multi(self, repository, mock_session):
        """|| separated identifiers are split into a list and use IN clause."""
        mock_session.run.return_value = make_async_result([{"total_count": 2}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "identifiers": "SAMPLE1||SAMPLE2"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sa.sample_id IN $" in query
        list_params = [v for v in params.values() if isinstance(v, list)]
        assert any(
            set(lp) == {"SAMPLE1", "SAMPLE2"} for lp in list_params
        ), f"Expected ['SAMPLE1', 'SAMPLE2'] in params, got: {params}"
        assert result["counts"]["total"] == 2


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — depositions filter
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryDepositionsFilter:

    async def test_summary_search_with_depositions_single(self, repository, mock_session):
        """Single deposition uses = clause."""
        mock_session.run.return_value = make_async_result([{"total_count": 11}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "depositions": "STUDY1"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sid" in query
        assert "STUDY1" in params.values()
        assert result["counts"]["total"] == 11

    async def test_summary_search_with_depositions_multi(self, repository, mock_session):
        """|| separated depositions are split into list and use IN clause."""
        mock_session.run.return_value = make_async_result([{"total_count": 20}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "depositions": "STUDY1||STUDY2"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sid IN $" in query
        list_params = [v for v in params.values() if isinstance(v, list)]
        assert any(
            set(lp) == {"STUDY1", "STUDY2"} for lp in list_params
        ), f"Expected ['STUDY1', 'STUDY2'] in params, got: {params}"
        assert result["counts"]["total"] == 20

    async def test_summary_search_with_depositions_pipe_single_element(
        self, repository, mock_session
    ):
        """|| string with only one real element after split falls into the single-element branch."""
        mock_session.run.return_value = make_async_result([{"total_count": 6}])

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia", "depositions": "STUDY1||"}
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        params: dict = call_args[0][1]

        # After stripping blanks, only one element remains → single = clause
        assert "STUDY1" in params.values()


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — error path
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryErrorHandling:

    async def test_summary_returns_zero_on_db_exception(self, repository, mock_session):
        """Exception during session.run is caught and returns {"counts": {"total": 0}}."""
        mock_session.run.side_effect = Exception("DB exploded")

        result = await repository._get_samples_summary_diagnosis_search(
            {"_diagnosis_search": "leukemia"}
        )

        assert result == {"counts": {"total": 0}}


# ---------------------------------------------------------------------------
# _get_samples_summary_diagnosis_search — all filters combined
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSummaryAllFiltersCombined:

    async def test_summary_search_all_filters_combined(self, repository, mock_session):
        """All optional filters combined: one session.run call, query contains all clauses."""
        mock_session.run.return_value = make_async_result([{"total_count": 99}])

        result = await repository._get_samples_summary_diagnosis_search(
            {
                "_diagnosis_search": "leukemia",
                "disease_phase": "Initial Diagnosis",
                "tumor_grade": "Grade 2",
                "tumor_classification": "Primary",
                "tumor_tissue_morphology": "9590/3",
                "age_at_diagnosis": "10",
                "diagnosis_category": "Leukemia",
                "identifiers": "SAMP001||SAMP002",
                "depositions": "STUDY1",
            }
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "dx.disease_phase" in query
        assert "dx.tumor_grade" in query
        assert "dx.tumor_classification" in query
        assert "dx.tumor_tissue_morphology" in query
        assert "toInteger(dx.age_at_diagnosis)" in query
        assert "dx.diagnosis_category" in query
        assert "sa.sample_id" in query
        assert "sid" in query

        assert "Grade 2" in params.values()
        assert "9590/3" in params.values()
        assert 10 in params.values()
        assert "Leukemia" in params.values()

        assert result["counts"]["total"] == 99


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — identifiers multi branch
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListIdentifiersMultiBranch:

    async def test_list_search_with_identifiers_multi(self, repository, mock_session):
        """|| separated identifiers expand into a list and use IN clause in list query."""
        mock_session.run.return_value = make_async_result([_sample_record()])

        await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "identifiers": "S1||S2"},
            offset=0,
            limit=20,
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sa.sample_id IN $" in query
        list_params = [v for v in params.values() if isinstance(v, list)]
        assert any(set(lp) == {"S1", "S2"} for lp in list_params), (
            f"Expected list ['S1','S2'] in params. Params: {params}"
        )


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — depositions multi branch
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListDepositionsMultiBranch:

    async def test_list_search_with_depositions_multi(self, repository, mock_session):
        """|| separated depositions expand into a list and appear in query."""
        mock_session.run.return_value = make_async_result([_sample_record()])

        await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "depositions": "STUDY1||STUDY2"},
            offset=0,
            limit=20,
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "sid IN $" in query
        list_params = [v for v in params.values() if isinstance(v, list)]
        assert any(
            set(lp) == {"STUDY1", "STUDY2"} for lp in list_params
        ), f"Expected list in params. Params: {params}"

    async def test_list_search_with_depositions_single_pipe(self, repository, mock_session):
        """|| string with one real token after strip goes into the single-element sub-branch."""
        mock_session.run.return_value = make_async_result([_sample_record()])

        await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "depositions": "STUDY1||"},
            offset=0,
            limit=20,
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        params: dict = call_args[0][1]
        assert "STUDY1" in params.values()


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — return_total=True
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListReturnTotal:

    async def test_list_search_return_total(self, repository, mock_session):
        """return_total=True causes two session.run calls: count query then list query."""

        async def _count_gen():
            yield {"total_count": 55}

        count_result = AsyncMock()
        count_result.__aiter__ = Mock(return_value=_count_gen())
        count_result.consume = AsyncMock()

        list_result = make_async_result([_sample_record()])
        mock_session.run = AsyncMock(side_effect=[count_result, list_result])

        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia"},
            offset=0,
            limit=20,
            return_total=True,
        )

        assert mock_session.run.call_count == 2, (
            f"Expected 2 session.run calls, got {mock_session.run.call_count}"
        )
        assert isinstance(result, tuple)
        samples, total = result
        assert isinstance(samples, list)
        assert total == 55

    async def test_list_search_return_total_with_identifiers(self, repository, mock_session):
        """return_total with identifiers filter: identifiers appear in count query too."""

        async def _count_gen():
            yield {"total_count": 2}

        count_result = AsyncMock()
        count_result.__aiter__ = Mock(return_value=_count_gen())
        count_result.consume = AsyncMock()

        list_result = make_async_result([_sample_record()])
        mock_session.run = AsyncMock(side_effect=[count_result, list_result])

        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "identifiers": "SAMPLE1"},
            offset=0,
            limit=20,
            return_total=True,
        )

        assert mock_session.run.call_count == 2
        # Count query (first call) should also contain sample_id filter
        count_query: str = mock_session.run.call_args_list[0][0][0]
        assert "sa.sample_id" in count_query
        samples, total = result
        assert total == 2

    async def test_list_search_return_total_count_error_falls_through(
        self, repository, mock_session
    ):
        """If the count query raises, fall through without total (list still returned)."""
        count_result = AsyncMock()
        count_result.__aiter__ = Mock(side_effect=Exception("count DB failure"))
        count_result.consume = AsyncMock()

        list_result = make_async_result([_sample_record()])
        mock_session.run = AsyncMock(side_effect=[count_result, list_result])

        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia"},
            offset=0,
            limit=20,
            return_total=True,
        )

        # Even with count failure the list query runs and returns samples (no total)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — disease_phase filter (list-mapping branch)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListDiseasePhaseFilter:

    async def test_list_search_with_disease_phase_list_mapping(self, repository, mock_session):
        """'Relapse' reverse-maps to a list → list query uses IN clause."""
        mock_session.run.return_value = make_async_result([_sample_record()])

        await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Relapse"},
            offset=0,
            limit=20,
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        params: dict = call_args[0][1]

        assert "IN $" in query
        list_params = [v for v in params.values() if isinstance(v, list)]
        assert any(
            set(lp) == {"Recurrent Disease", "Relapse"} for lp in list_params
        ), f"Expected Relapse list mapping in params. Params: {params}"

    async def test_list_search_database_only_disease_phase_returns_empty(
        self, repository, mock_session
    ):
        """database-only disease_phase value returns empty list without calling session."""
        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Recurrent Disease"},
            offset=0,
            limit=20,
        )
        assert result == []
        mock_session.run.assert_not_called()

    async def test_list_search_database_only_disease_phase_return_total(
        self, repository, mock_session
    ):
        """database-only disease_phase with return_total=True returns ([], 0)."""
        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "disease_phase": "Recurrent Disease"},
            offset=0,
            limit=20,
            return_total=True,
        )
        assert result == ([], 0)
        mock_session.run.assert_not_called()


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — tumor_classification null-mapped
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListTumorClassificationFilter:

    async def test_list_search_with_invalid_tumor_classification_returns_empty(
        self, repository, mock_session
    ):
        """null-mapped tumor_classification returns empty list without session call."""
        result = await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_classification": "non-malignant"},
            offset=0,
            limit=20,
        )
        assert result == []
        mock_session.run.assert_not_called()

    async def test_list_search_with_valid_tumor_classification(self, repository, mock_session):
        """Valid tumor_classification is passed through to query params."""
        mock_session.run.return_value = make_async_result([_sample_record()])

        await repository._get_samples_by_diagnosis_search(
            {"_diagnosis_search": "leukemia", "tumor_classification": "Primary"},
            offset=0,
            limit=20,
        )

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query: str = call_args[0][0]
        assert "dx.tumor_classification" in query


# ---------------------------------------------------------------------------
# _get_samples_by_diagnosis_search — no search term early exit
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestListNoSearchTermEarlyExit:

    async def test_list_no_search_term_returns_empty(self, repository, mock_session):
        result = await repository._get_samples_by_diagnosis_search({}, offset=0, limit=20)
        assert result == []
        mock_session.run.assert_not_called()

    async def test_list_no_search_term_return_total_returns_tuple(
        self, repository, mock_session
    ):
        result = await repository._get_samples_by_diagnosis_search(
            {}, offset=0, limit=20, return_total=True
        )
        assert result == ([], 0)
        mock_session.run.assert_not_called()
