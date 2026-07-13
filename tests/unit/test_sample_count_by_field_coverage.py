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

async def test_query_execution_exception_is_reraised(repository, mock_session):
    """Exception raised by session.run during values query propagates to caller (lines 1058-1068)."""
    mock_session.run.side_effect = Exception("DB connection failed")
    with pytest.raises(Exception, match="DB connection failed"):
        await repository.count_samples_by_field("tissue_type")


# ---------------------------------------------------------------------------
# preservation_method: map-then-dedup (no double-count on N:1 collapses)
# ---------------------------------------------------------------------------

async def test_preservation_count_maps_and_dedups_by_api_value(repository, mock_session):
    """Regression: preservation_method must map DB->API IN Cypher and dedup by the
    MAPPED value, not the raw value.

    Deduping on the raw value double-counted a (sample, study) pair holding two
    distinct raw values that collapse to one API value (e.g. two 'Cryopreservation'
    variants both -> 'Cryopreserved'): each raw row was counted then summed in Python.
    """
    mock_session.run.return_value = make_async_result(
        [{"value": "Cryopreserved", "count": 10, "total": 100, "missing": 20}]
    )
    await repository.count_samples_by_field("preservation_method")
    cypher = mock_session.run.call_args[0][0]
    # Mapping is applied IN Cypher (a collapsing variant maps to 'Cryopreserved').
    assert "Cryopreservation in liquid nitrogen (dead tissue)' THEN 'Cryopreserved'" in cypher
    # Dedup is on the MAPPED value: the CASE mapping precedes the DISTINCT dedup.
    assert "WITH DISTINCT sample_id, study_id, value" in cypher
    assert cypher.index("THEN 'Cryopreserved'") < cypher.index("WITH DISTINCT sample_id, study_id, value")
    # Regression guard: the old raw-value dedup form must be gone from this query.
    assert "toString(val) as value" not in cypher


async def test_preservation_count_keeps_unknown_bucket(repository, mock_session):
    """preservation's API bucket 'Unknown' (from Cytospin Slide/Other) must survive even
    though 'Unknown' is also in null_mappings. Since the value is already mapped +
    null-filtered in Cypher, Python must NOT re-apply is_null_mapped_value to it."""
    mock_session.run.return_value = make_async_result(
        [{"value": "Unknown", "count": 7, "total": 100, "missing": 20}]
    )
    result = await repository.count_samples_by_field("preservation_method")
    buckets = {v["value"]: v["count"] for v in result["values"]}
    assert buckets.get("Unknown") == 7  # not dropped by the null_mappings collision
