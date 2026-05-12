"""
Coverage tests for count_samples_by_field filter-building branches.

Targets uncovered branches in app/repositories/sample_count.py:
- race filter as list / non-str-non-list
- depositions single item from || split
- anatomical_sites filter as list / non-str-non-list
- diagnosis filter fields loop: disease_phase (scalar + list reverse-map + db-only),
  tumor_classification (null-mapped), tumor_grade, tumor_tissue_morphology,
  age_at_diagnosis (int + non-int), diagnosis
- regular filters loop: string and list values
- query execution exception
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
    return Mock(spec=Settings)


@pytest.fixture
def repository(mock_session, mock_allowlist, mock_settings) -> SampleRepository:
    return SampleRepository(mock_session, mock_allowlist, mock_settings)


def std_runs(values=None, total=15, missing=5):
    """Three mock session.run results: values query, total query, missing query."""
    if values is None:
        values = [{"value": "Tumor", "count": 10}]
    return [
        make_async_result(values),
        make_async_result([{"total": total}]),
        make_async_result([{"missing": missing}]),
    ]


# ---------------------------------------------------------------------------
# race filter branches
# ---------------------------------------------------------------------------

async def test_race_filter_as_list(repository, mock_session):
    """race filter passed as a list takes the list branch (lines 121-122)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"race": ["White", "Black"]}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_race_filter_non_string_non_list(repository, mock_session):
    """race filter that is neither str nor list produces empty race_list (lines 123-124)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"race": 42}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# depositions single-item from || split
# ---------------------------------------------------------------------------

async def test_depositions_single_item_from_pipe_split(repository, mock_session):
    """depositions '||' that yields exactly one item uses = not IN (lines 166-167)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"depositions": "phs001 || "}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# anatomical_sites filter branches
# ---------------------------------------------------------------------------

async def test_anatomical_sites_filter_as_list(repository, mock_session):
    """anatomical_sites filter as a list builds per-value OR conditions (lines 195-209)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"anatomical_sites": ["Brain", "Kidney"]}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_anatomical_sites_filter_non_string_non_list(repository, mock_session):
    """anatomical_sites filter that is neither str nor list uses the else clause (lines 223-230)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"anatomical_sites": 123}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# diagnosis filter fields loop
# ---------------------------------------------------------------------------

async def test_disease_phase_filter_scalar_reverse_map(repository, mock_session):
    """disease_phase with a scalar reverse-mapped value builds an = condition (lines 257-258)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"disease_phase": "Initial Diagnosis"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


@patch("app.repositories.sample_count.reverse_map_field_value", return_value=["Recurrent Disease", "Relapse"])
async def test_disease_phase_filter_list_reverse_map(mock_rmv, repository, mock_session):
    """disease_phase with a list reverse-mapped value builds an IN condition (lines 253-255)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"disease_phase": "Relapse"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


@patch("app.repositories.sample_count.is_database_only_value", return_value=True)
async def test_disease_phase_filter_database_only_value(mock_db_only, repository, mock_session):
    """disease_phase db-only value injects 'false' into WHERE (lines 249-250)."""
    mock_session.run.side_effect = std_runs(values=[])
    result = await repository.count_samples_by_field(
        "tissue_type", {"disease_phase": "SomeDBOnlyValue"}
    )
    assert mock_session.run.call_count == 3


@patch("app.repositories.sample_count.is_null_mapped_value", return_value=True)
async def test_tumor_classification_null_mapped_value(mock_nm, repository, mock_session):
    """tumor_classification null-mapped value injects 'false' into WHERE (lines 260-261)."""
    mock_session.run.side_effect = std_runs(values=[])
    result = await repository.count_samples_by_field(
        "tissue_type", {"tumor_classification": "Not Reported"}
    )
    assert mock_session.run.call_count == 3


async def test_tumor_grade_filter(repository, mock_session):
    """tumor_grade filter builds a simple = condition (lines 266-268)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"tumor_grade": "Grade III"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_tumor_tissue_morphology_filter(repository, mock_session):
    """tumor_tissue_morphology filter builds a simple = condition (lines 269-271)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"tumor_tissue_morphology": "8010/3"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_age_at_diagnosis_filter_string_int(repository, mock_session):
    """age_at_diagnosis filter converts string to int (lines 272-277)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"age_at_diagnosis": "5"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_age_at_diagnosis_filter_non_int_value(repository, mock_session):
    """age_at_diagnosis non-int value is kept as-is via the ValueError path (lines 275-276)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"age_at_diagnosis": "not_a_number"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_diagnosis_filter_in_loop(repository, mock_session):
    """'diagnosis' in filters dict takes the elif branch (lines 278-284)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"diagnosis": "Leukemia"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# regular filters loop
# ---------------------------------------------------------------------------

async def test_regular_filter_sex_string(repository, mock_session):
    """sex string filter uses sex_at_birth DB field and builds = condition (lines 287-298)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"sex": "Male"}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


async def test_regular_filter_as_list(repository, mock_session):
    """Regular filter with a list value builds an IN condition (lines 295-296)."""
    mock_session.run.side_effect = std_runs()
    result = await repository.count_samples_by_field(
        "tissue_type", {"vital_status": ["Alive", "Deceased"]}
    )
    assert result["total"] == 15
    assert mock_session.run.call_count == 3


# ---------------------------------------------------------------------------
# query execution exception
# ---------------------------------------------------------------------------

async def test_query_execution_exception_is_reraised(repository, mock_session):
    """Exception raised by session.run during values query propagates to caller (lines 1058-1068)."""
    mock_session.run.side_effect = Exception("DB connection failed")
    with pytest.raises(Exception, match="DB connection failed"):
        await repository.count_samples_by_field("tissue_type", {})
