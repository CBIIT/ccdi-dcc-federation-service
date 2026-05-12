"""
Unit tests targeting branches in app/repositories/subject_summary.py.

Covers survival + identifiers / depositions paths, ethnicity via p.race predicates,
diagnosis-related filters, race normalization edge cases, derived-field edge cases,
and session retry / error handling.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from neo4j import AsyncSession

from app.core.config import Settings
from app.lib.field_allowlist import FieldAllowlist
from app.repositories.subject import SubjectRepository
from tests.unit.helpers import make_async_result


def _count_record(total: int = 5) -> dict:
    return {"total_count": total}


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
    s.subject_count_fields = [
        "sex",
        "race",
        "ethnicity",
        "vital_status",
        "age_at_vital_status",
        "associated_diagnoses",
    ]
    s.sex_value_mappings = {}
    return s


@pytest.fixture
def repository(mock_session, mock_allowlist, mock_settings):
    return SubjectRepository(mock_session, mock_allowlist, mock_settings)


# ---------------------------------------------------------------------------
# get_subjects_summary – survival + identifiers path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_filter_with_identifiers(repository, mock_session):
    """vital_status + identifiers triggers needs_survival_processing=True and identifiers_condition."""
    mock_session.run.return_value = make_async_result([_count_record(3)])

    result = await repository.get_subjects_summary(
        {"vital_status": "Alive", "identifiers": "P001"}
    )

    assert result.get("total_count") == 3


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_with_identifiers_and_depositions(
    repository, mock_session
):
    """vital_status + identifiers + depositions exercises the dep_param branch inside the identifiers+survival path."""
    mock_session.run.return_value = make_async_result([_count_record(2)])

    result = await repository.get_subjects_summary(
        {"vital_status": "Alive", "identifiers": "P001", "depositions": "STUDY1"}
    )

    assert result.get("total_count") == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_with_multiple_identifiers_and_depositions(
    repository, mock_session
):
    """
    vital_status + multiple-value identifiers (||) + depositions
    exercises the dep_param branch with a LIST identifier value.
    """
    mock_session.run.return_value = make_async_result([_count_record(4)])

    result = await repository.get_subjects_summary(
        {"vital_status": "Alive", "identifiers": "P001||P002", "depositions": "STUDY1"}
    )

    assert result.get("total_count") == 4


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_age_at_vital_status_filter(repository, mock_session):
    """age_at_vital_status triggers survival processing without identifiers."""
    mock_session.run.return_value = make_async_result([_count_record(7)])

    result = await repository.get_subjects_summary({"age_at_vital_status": "10"})

    assert result.get("total_count") == 7


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_no_identifiers_with_depositions(
    repository, mock_session
):
    """vital_status + depositions (no identifiers) exercises the dep_param branch inside needs_survival_processing."""
    mock_session.run.return_value = make_async_result([_count_record(6)])

    result = await repository.get_subjects_summary(
        {"vital_status": "Dead", "depositions": "phs001234"}
    )

    assert result.get("total_count") == 6


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_retry_on_empty(repository, mock_session):
    """First two calls return empty → retry logic fires; third call returns results."""
    # Side effect: first two empty, third returns data
    mock_session.run.side_effect = [
        make_async_result([]),
        make_async_result([]),
        make_async_result([_count_record(1)]),
    ]

    result = await repository.get_subjects_summary({"vital_status": "Alive"})

    # After retries the result is returned (may be 0 if retries exhausted to max_retries=2)
    assert "total_count" in result


# ---------------------------------------------------------------------------
# get_subjects_summary – ethnicity (p.race predicates, non-survival path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_with_identifiers(repository, mock_session):
    """
    ethnicity + identifiers: ethnicity is applied as p.race CONTAINS predicates
    on the simple-query path with identifiers dedupe.
    """
    mock_session.run.return_value = make_async_result([_count_record(5)])

    result = await repository.get_subjects_summary(
        {"ethnicity": "Hispanic or Latino", "identifiers": "P001"}
    )

    assert result.get("total_count") == 5


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_not_hispanic_with_identifiers(
    repository, mock_session
):
    """ethnicity=Not reported + identifiers."""
    mock_session.run.return_value = make_async_result([_count_record(8)])

    result = await repository.get_subjects_summary(
        {"ethnicity": "Not reported", "identifiers": "P001"}
    )

    assert result.get("total_count") == 8


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_with_identifiers_and_depositions(
    repository, mock_session
):
    """ethnicity + identifiers + depositions (required MATCH for study)."""
    mock_session.run.return_value = make_async_result([_count_record(3)])

    result = await repository.get_subjects_summary(
        {
            "ethnicity": "Hispanic or Latino",
            "identifiers": "P001",
            "depositions": "STUDY1",
        }
    )

    assert result.get("total_count") == 3


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_no_identifiers(repository, mock_session):
    """ethnicity alone (no identifiers, no depositions)."""
    mock_session.run.return_value = make_async_result([_count_record(10)])

    result = await repository.get_subjects_summary({"ethnicity": "Hispanic or Latino"})

    assert result.get("total_count") == 10


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_no_identifiers_with_depositions(
    repository, mock_session
):
    """ethnicity + depositions (no identifiers)."""
    mock_session.run.return_value = make_async_result([_count_record(4)])

    result = await repository.get_subjects_summary(
        {"ethnicity": "Hispanic or Latino", "depositions": "phs001234"}
    )

    assert result.get("total_count") == 4


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_ethnicity_with_multiple_identifiers(repository, mock_session):
    """ethnicity + multiple identifiers via || separator (LIST id_param)."""
    mock_session.run.return_value = make_async_result([_count_record(9)])

    result = await repository.get_subjects_summary(
        {"ethnicity": "Hispanic or Latino", "identifiers": "P001||P002"}
    )

    assert result.get("total_count") == 9


# ---------------------------------------------------------------------------
# get_subjects_summary – race / diagnosis / derived / session edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_race_non_string_non_list(repository, mock_session):
    """Race value that is neither str nor list yields empty race_list (fallback branch)."""
    mock_session.run.return_value = make_async_result([_count_record(1)])

    result = await repository.get_subjects_summary({"race": 42})

    assert result.get("total_count") == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_race_not_reported_includes_hispanic_only_pr_tokens(
    repository, mock_session
):
    """API race 'Not Reported' uses the expanded race_filter_condition (Hispanic-only pr_tokens)."""
    mock_session.run.return_value = make_async_result([_count_record(2)])

    result = await repository.get_subjects_summary({"race": "Not Reported"})

    assert result.get("total_count") == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_vital_status_database_only_filter_value(
    repository, mock_session
):
    """
    vital_status 'Not Reported' (DB-shaped) is rejected via is_database_only_value
    and adds impossible derived predicate 'false'.
    """
    mock_session.run.return_value = make_async_result([_count_record(0)])

    result = await repository.get_subjects_summary({"vital_status": "Not Reported"})

    assert result.get("total_count") == 0


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_invalid_age_at_vital_status_warns(repository, mock_session):
    """Non-integer age_at_vital_status triggers logger.warning and keeps value as-is."""
    mock_session.run.return_value = make_async_result([_count_record(3)])

    with patch("app.repositories.subject_summary.logger") as mock_log:
        result = await repository.get_subjects_summary(
            {"vital_status": "Alive", "age_at_vital_status": "not-a-number"}
        )

    mock_log.warning.assert_called()
    assert result.get("total_count") == 3


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_list_filter_uses_in_clause(repository, mock_session):
    """Participant filter with list value uses p.<field> IN $param (survival path)."""
    mock_session.run.return_value = make_async_result([_count_record(4)])

    result = await repository.get_subjects_summary(
        {
            "vital_status": "Alive",
            "associated_diagnoses": ["ICD-O-3:foo", "ICD-O-3:bar"],
        }
    )

    assert result.get("total_count") == 4


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_diagnosis_category_filters(repository, mock_session):
    """associated_diagnosis_categories + _associated_diagnosis_categories_contains."""
    mock_session.run.return_value = make_async_result([_count_record(5)])

    result = await repository.get_subjects_summary(
        {
            "_diagnosis_search": "leukemia",
            "associated_diagnosis_categories": "Hematologic",
            "_associated_diagnosis_categories_contains": "Lymph",
        }
    )

    assert result.get("total_count") == 5


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_raises_after_session_retries_exhausted(repository, mock_session):
    """Session.run keeps failing: retry loop exhausts then re-raises."""
    mock_session.run.side_effect = RuntimeError("bolt error")

    with pytest.raises(RuntimeError, match="bolt error"):
        await repository.get_subjects_summary({"vital_status": "Alive"})

    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# get_subjects_summary – no filters / race-only fallback paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_no_filters(repository, mock_session):
    """No filters at all hits the simple count Cypher."""
    mock_session.run.return_value = make_async_result([_count_record(100)])

    result = await repository.get_subjects_summary({})

    assert result.get("total_count") == 100


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_returns_zero_when_no_records(repository, mock_session):
    """Empty DB result returns {"total_count": 0}."""
    mock_session.run.return_value = make_async_result([])

    result = await repository.get_subjects_summary({})

    assert result == {"total_count": 0}


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_identifiers_only(repository, mock_session):
    """identifiers without survival/ethnicity hits the non-survival, non-ethnicity identifiers branch."""
    mock_session.run.return_value = make_async_result([_count_record(2)])

    result = await repository.get_subjects_summary({"identifiers": "P001"})

    assert result.get("total_count") == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_summary_identifiers_with_depositions(repository, mock_session):
    """identifiers + depositions without ethnicity/survival hits dep_param branch inside the non-survival identifiers path."""
    mock_session.run.return_value = make_async_result([_count_record(2)])

    result = await repository.get_subjects_summary(
        {"identifiers": "P001", "depositions": "STUDY1"}
    )

    assert result.get("total_count") == 2


# ---------------------------------------------------------------------------
# get_subjects_summary_for_diagnosis_endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_search_term(repository, mock_session):
    """_diagnosis_search present: exercises the diagnosis-first optimized query path without depositions."""
    mock_result = make_async_result([_count_record(12)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia"}
    )

    assert result.get("total_count") == 12


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_category_contains(
    repository, mock_session
):
    """_associated_diagnosis_categories_contains present exercises the diagnosis category substring path."""
    mock_result = make_async_result([_count_record(7)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_associated_diagnosis_categories_contains": "Leukemia"}
    )

    assert result.get("total_count") == 7


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_no_diagnosis_params(repository, mock_session):
    """Neither _diagnosis_search nor _associated_diagnosis_categories_contains present: delegates to get_subjects_summary."""
    mock_session.run.return_value = make_async_result([_count_record(20)])

    result = await repository.get_subjects_summary_for_diagnosis_endpoint({})

    assert result.get("total_count") == 20


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_race_filter(repository, mock_session):
    """_diagnosis_search + race filter exercises the race normalization code inside get_subjects_summary_for_diagnosis_endpoint."""
    mock_result = make_async_result([_count_record(3)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "race": "White"}
    )

    assert result.get("total_count") == 3


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_vital_status_filter(
    repository, mock_session
):
    """_diagnosis_search + vital_status: exercises derived_filters handling + survival processing in the optimized path."""
    mock_result = make_async_result([_count_record(5)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "vital_status": "Alive"}
    )

    assert result.get("total_count") == 5


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_depositions(repository, mock_session):
    """_diagnosis_search + depositions exercises the dep_param branch."""
    mock_result = make_async_result([_count_record(8)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "depositions": "phs001234"}
    )

    assert result.get("total_count") == 8


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_empty_result(repository, mock_session):
    """Empty result from DB returns {"total_count": 0}."""
    mock_result = make_async_result([])
    mock_result.data = AsyncMock(return_value=[])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia"}
    )

    assert result == {"total_count": 0}


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_both_diagnosis_params(
    repository, mock_session
):
    """
    Both _diagnosis_search and _associated_diagnosis_categories_contains present.
    """
    mock_result = make_async_result([_count_record(2)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {
            "_diagnosis_search": "leukemia",
            "_associated_diagnosis_categories_contains": "Leukemia",
        }
    )

    assert result.get("total_count") == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_sex_filter(repository, mock_session):
    """_diagnosis_search + sex (API value 'M') exercises sex_mapping code in get_subjects_summary_for_diagnosis_endpoint."""
    mock_result = make_async_result([_count_record(6)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "sex": "M"}
    )

    assert result.get("total_count") == 6


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_with_identifiers(repository, mock_session):
    """_diagnosis_search + identifiers exercises identifiers handling in get_subjects_summary_for_diagnosis_endpoint."""
    mock_result = make_async_result([_count_record(1)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "identifiers": "P001"}
    )

    assert result.get("total_count") == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_summary_whitespace_only_category_contains(
    repository, mock_session
):
    """
    _associated_diagnosis_categories_contains with whitespace-only value
    should be treated as absent and delegate to get_subjects_summary.
    """
    mock_session.run.return_value = make_async_result([_count_record(15)])

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_associated_diagnosis_categories_contains": "   "}
    )

    # Whitespace-only -> diagnosis_category_contains is None -> delegates to get_subjects_summary
    assert result.get("total_count") == 15


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_race_non_string_non_list(repository, mock_session):
    """Diagnosis summary: race value neither str nor list -> empty race_list."""
    mock_result = make_async_result([_count_record(2)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "race": 99}
    )

    assert result.get("total_count") == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_race_not_reported_expanded_filter(
    repository, mock_session
):
    """Diagnosis summary: API race 'Not Reported' expands race_filter_condition."""
    mock_result = make_async_result([_count_record(3)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "race": "Not Reported"}
    )

    assert result.get("total_count") == 3


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_vital_status_database_only(repository, mock_session):
    """Diagnosis summary: invalid vital_status shape adds 'false' derived predicate."""
    mock_result = make_async_result([_count_record(0)])
    mock_session.run.return_value = mock_result

    result = await repository.get_subjects_summary_for_diagnosis_endpoint(
        {"_diagnosis_search": "leukemia", "vital_status": "Not Reported"}
    )

    assert result.get("total_count") == 0


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_invalid_age_at_vital_status_warns(
    repository, mock_session
):
    mock_result = make_async_result([_count_record(4)])
    mock_session.run.return_value = mock_result

    with patch("app.repositories.subject_summary.logger") as mock_log:
        result = await repository.get_subjects_summary_for_diagnosis_endpoint(
            {
                "_diagnosis_search": "leukemia",
                "vital_status": "Alive",
                "age_at_vital_status": "bad-age",
            }
        )

    mock_log.warning.assert_called()
    assert result.get("total_count") == 4


@pytest.mark.asyncio
@pytest.mark.unit
async def test_diagnosis_endpoint_session_error_is_logged_and_raised(
    repository, mock_session
):
    mock_session.run.side_effect = RuntimeError("memgraph down")

    with pytest.raises(RuntimeError, match="memgraph down"):
        await repository.get_subjects_summary_for_diagnosis_endpoint(
            {"_diagnosis_search": "leukemia"}
        )
