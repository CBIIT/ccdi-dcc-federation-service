"""
Regression tests for the intermittent "all: 0, current: N" bug on GET /subject.

Root cause: get_subjects(return_total=True) runs a heavy count query separately from
the paginated data query. A transient count failure (timeout / Memgraph memory limit /
connection blip) used to be swallowed to total_count=0 while the cheap data query still
returned rows -> the aggregator saw all:0 alongside real data.

Fix: retry the count query on retryable errors, and floor the reported total at the
number of rows actually returned so "all" can never be less than "current".
"""

import pytest
from neo4j.exceptions import TransientError

from app.repositories.subject import SubjectRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings

_COUNT_MARKER = "count(*) AS total_count"


def _data_row(name: str) -> dict:
    """A minimal fast-path participant row that _record_to_subject can convert."""
    return {
        "name": name,
        "race": "White",
        "ethnicity": "Not reported",
        "age_at_vital_status": None,
        "vital_status": None,
        "associated_diagnoses": None,
        "survival_records": [],
        "diagnosis_nodes": [],
        "sex": "F",
        "namespace": "phs002431",
        "depositions": ["phs002431"],
    }


class _MockResult:
    def __init__(self, rows):
        self._rows = rows

    async def __aiter__(self):
        for row in self._rows:
            yield row

    async def consume(self):
        pass


class CountControlSession:
    """Fails the count query `count_fail_times` times (retryable), then returns count_value.

    The data (non-count) query always returns `data_rows`.
    """

    def __init__(self, data_rows, count_value, count_fail_times):
        self.data_rows = data_rows
        self.count_value = count_value
        self.count_fail_times = count_fail_times
        self.count_calls = 0

    async def run(self, cypher, params=None):
        if _COUNT_MARKER in cypher:
            self.count_calls += 1
            if self.count_calls <= self.count_fail_times:
                raise TransientError("Memgraph.TransientError: transaction conflict")
            return _MockResult([{"total_count": self.count_value}])
        return _MockResult(list(self.data_rows))


def _make_repo(session) -> SubjectRepository:
    return SubjectRepository(session=session, allowlist=FieldAllowlist(), settings=Settings())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_query_retries_after_transient_failure():
    """A single transient count failure is retried, so 'all' is the real total, not 0."""
    session = CountControlSession(
        data_rows=[_data_row("subjectA"), _data_row("subjectB")],
        count_value=500,
        count_fail_times=1,  # fail once, then succeed
    )
    repo = _make_repo(session)

    subjects, total_count = await repo.get_subjects({}, offset=0, limit=2, return_total=True)

    assert len(subjects) == 2
    assert total_count == 500  # pre-fix this was 0 (no retry)
    assert session.count_calls == 2  # proves the retry happened


@pytest.mark.unit
@pytest.mark.asyncio
async def test_all_is_never_below_current_when_count_fails():
    """If the count keeps failing, 'all' floors at the returned row count, never 0."""
    session = CountControlSession(
        data_rows=[_data_row("subjectA"), _data_row("subjectB")],
        count_value=500,
        count_fail_times=99,  # always fails, retries exhausted
    )
    repo = _make_repo(session)

    subjects, total_count = await repo.get_subjects({}, offset=0, limit=2, return_total=True)

    assert len(subjects) == 2
    # Invariant: all (total_count) >= current (len(subjects)); never the absurd 0-with-data.
    assert total_count >= len(subjects)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_healthy_count_is_unchanged():
    """When the count query succeeds first try, the real total is returned as-is."""
    session = CountControlSession(
        data_rows=[_data_row("subjectA"), _data_row("subjectB")],
        count_value=42,
        count_fail_times=0,
    )
    repo = _make_repo(session)

    subjects, total_count = await repo.get_subjects({}, offset=0, limit=2, return_total=True)

    assert total_count == 42
    assert session.count_calls == 1
