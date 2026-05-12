"""Shared async mock helpers for unit tests."""
from unittest.mock import AsyncMock, Mock


def make_async_result(records: list) -> AsyncMock:
    """Return a mock neo4j AsyncResult that yields records when iterated."""
    mock_result = AsyncMock()
    records_copy = list(records)

    async def aiter_impl():
        for r in records_copy:
            yield r

    # Wrap in a lambda so AsyncMock can call it with self; each call gets a fresh generator
    mock_result.__aiter__ = lambda *_: aiter_impl()
    mock_result.consume = AsyncMock()
    mock_result.single = AsyncMock(return_value=records_copy[0] if records_copy else None)
    mock_result.data = AsyncMock(return_value=records_copy)
    return mock_result


def make_single_result(record) -> AsyncMock:
    """Return a mock neo4j AsyncResult that supports .single() only."""
    mock_result = AsyncMock()
    mock_result.single = AsyncMock(return_value=record)
    mock_result.consume = AsyncMock()
    return mock_result
