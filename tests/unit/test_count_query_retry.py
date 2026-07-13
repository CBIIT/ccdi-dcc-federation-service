"""
Unit tests for run_count_query_with_retry — the shared count-query executor used by
/subject, /sample, and /file to retry transient failures instead of silently
reporting a total of 0 while a paginated data query still returns rows.
"""

import pytest
from neo4j.exceptions import TransientError

from app.db.memgraph import run_count_query_with_retry


class _Result:
    def __init__(self, rows):
        self._rows = rows

    async def __aiter__(self):
        for row in self._rows:
            yield row

    async def consume(self):
        pass


class _Session:
    """Fails `fail_times` times with `error`, then returns `rows`."""

    def __init__(self, rows, fail_times=0, error=None):
        self.rows = rows
        self.fail_times = fail_times
        self.error = error
        self.calls = 0

    async def run(self, cypher, params=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.error
        return _Result(self.rows)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_count_on_success():
    session = _Session(rows=[{"total_count": 123}])
    assert await run_count_query_with_retry(session, "RETURN count(*) AS total_count", {}) == 123
    assert session.calls == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_zero_on_empty_records():
    session = _Session(rows=[])
    assert await run_count_query_with_retry(session, "…", {}) == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_retries_transient_error_then_succeeds():
    session = _Session(
        rows=[{"total_count": 77}],
        fail_times=2,
        error=TransientError("Memgraph.TransientError: transaction conflict"),
    )
    assert await run_count_query_with_retry(session, "…", {}, retry_delay=0.0) == 77
    assert session.calls == 3  # 2 failures + 1 success


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_after_exhausting_retries():
    session = _Session(
        rows=[{"total_count": 1}],
        fail_times=99,
        error=TransientError("Memgraph.TransientError: transaction conflict"),
    )
    with pytest.raises(TransientError):
        await run_count_query_with_retry(session, "…", {}, max_retries=2, retry_delay=0.0)
    assert session.calls == 3  # initial + 2 retries, then give up


@pytest.mark.unit
@pytest.mark.asyncio
async def test_non_retryable_error_raises_immediately_without_retry():
    session = _Session(rows=[{"total_count": 1}], fail_times=99, error=ValueError("bad query"))
    with pytest.raises(ValueError):
        await run_count_query_with_retry(session, "…", {}, retry_delay=0.0)
    assert session.calls == 1  # no retry on a non-retryable error


@pytest.mark.unit
@pytest.mark.asyncio
async def test_memory_limit_error_not_retried():
    """Memgraph memory-limit errors are deterministic → raise immediately, no retry."""
    session = _Session(
        rows=[{"total_count": 1}],
        fail_times=99,
        error=TransientError(
            "Memgraph.TransientError.MemgraphError.MemgraphError: Memory limit exceeded! "
            "Attempting to allocate a chunk of 160.00MiB"
        ),
    )
    with pytest.raises(TransientError):
        await run_count_query_with_retry(session, "…", {}, max_retries=2, retry_delay=0.0)
    assert session.calls == 1  # no retry — retrying a too-heavy query just OOMs again
