"""
Tests for SampleRepository._count_samples_by_diagnosis_category.

Covers:
- Basic successful 3-query flow
- Empty results triggering retry loop
- Exception on first attempt with successful retry
- Exhausted retries (all 3 attempts raise) → re-raise
- public count_samples_by_field dispatching to the private method
- Multiple diagnosis_category values in output
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings
from tests.unit.helpers import make_async_result


@pytest.fixture
def mock_session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_allowlist() -> Mock:
    al = Mock(spec=FieldAllowlist)
    al.is_field_allowed = Mock(return_value=True)
    al.is_allowed = Mock(return_value=True)
    return al


@pytest.fixture
def mock_settings() -> Mock:
    s = Mock(spec=Settings)
    s.subject_count_fields = []
    s.sex_value_mappings = {"Male": "M", "Female": "F", "Not Reported": "U"}
    s.identifier_server_url = "https://example.com"
    return s


@pytest.fixture
def repository(mock_session: AsyncMock, mock_allowlist: Mock, mock_settings: Mock) -> SampleRepository:
    return SampleRepository(mock_session, mock_allowlist, mock_settings)


async def test_count_diagnosis_category_basic(repository, mock_session):
    """Three successful session.run calls return correct total/missing/values."""
    mock_session.run.side_effect = [
        make_async_result([{"total": 100}]),
        make_async_result([{"missing": 5}]),
        make_async_result([
            {"value": "Leukemia", "count": 50},
            {"value": "Lymphoma", "count": 30},
        ]),
    ]

    result = await repository._count_samples_by_diagnosis_category({})

    assert result["total"] == 100
    assert result["missing"] == 5
    assert result["values"] == [
        {"value": "Leukemia", "count": 50},
        {"value": "Lymphoma", "count": 30},
    ]


@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_count_diagnosis_category_empty_results_retries(mock_sleep, repository, mock_session):
    """Empty results (total=0, values=[]) on attempt 1 trigger retry; attempt 2 returns data."""
    mock_session.run.side_effect = [
        make_async_result([{"total": 0}]),
        make_async_result([{"missing": 0}]),
        make_async_result([]),
        make_async_result([{"total": 20}]),
        make_async_result([{"missing": 2}]),
        make_async_result([{"value": "Neuroblastoma", "count": 15}]),
    ]

    result = await repository._count_samples_by_diagnosis_category({})

    assert result["total"] == 20
    assert result["missing"] == 2
    assert len(result["values"]) == 1
    assert result["values"][0]["value"] == "Neuroblastoma"
    mock_sleep.assert_called_once()
    assert mock_session.run.call_count == 6


@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_count_diagnosis_category_retry_on_exception(mock_sleep, repository, mock_session):
    """An exception on the first session.run is caught; second attempt succeeds."""
    mock_session.run.side_effect = [
        Exception("DB error"),
        make_async_result([{"total": 42}]),
        make_async_result([{"missing": 1}]),
        make_async_result([{"value": "Renal Tumors", "count": 10}]),
    ]

    result = await repository._count_samples_by_diagnosis_category({})

    assert result["total"] == 42
    assert result["missing"] == 1
    assert result["values"] == [{"value": "Renal Tumors", "count": 10}]
    mock_sleep.assert_called_once()


@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_count_diagnosis_category_exhausted_retries(mock_sleep, repository, mock_session):
    """After max_retries (2) exhausted, the last exception is re-raised."""
    mock_session.run.side_effect = [
        Exception("failure 0"),
        Exception("failure 1"),
        Exception("failure 2"),
    ]

    with pytest.raises(Exception, match="failure 2"):
        await repository._count_samples_by_diagnosis_category({})

    assert mock_sleep.call_count == 2


async def test_count_samples_by_field_routes_to_diagnosis_category(repository, mock_session):
    """count_samples_by_field('diagnosis_category', ...) delegates to _count_samples_by_diagnosis_category."""
    mock_session.run.side_effect = [
        make_async_result([{"total": 10}]),
        make_async_result([{"missing": 1}]),
        make_async_result([{"value": "Bone Tumors", "count": 9}]),
    ]

    result = await repository.count_samples_by_field("diagnosis_category", {})

    assert result["total"] == 10
    assert result["missing"] == 1
    assert result["values"] == [{"value": "Bone Tumors", "count": 9}]


async def test_count_diagnosis_category_multiple_values(repository, mock_session):
    """All returned diagnosis_category rows appear in result['values'] in order."""
    categories = [
        {"value": "Leukemia", "count": 80},
        {"value": "Brain Tumors", "count": 60},
        {"value": "Neuroblastoma", "count": 40},
        {"value": "Renal Tumors", "count": 20},
        {"value": "Bone Tumors", "count": 5},
    ]
    mock_session.run.side_effect = [
        make_async_result([{"total": 205}]),
        make_async_result([{"missing": 0}]),
        make_async_result(categories),
    ]

    result = await repository._count_samples_by_diagnosis_category({})

    assert result["total"] == 205
    assert result["missing"] == 0
    assert result["values"] == [{"value": c["value"], "count": c["count"]} for c in categories]
