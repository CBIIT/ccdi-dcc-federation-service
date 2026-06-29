"""
Unit tests for FileRepository.count_for_pagination().
"""

import pytest
from unittest.mock import AsyncMock, Mock
from neo4j import AsyncSession

from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY
from app.repositories.file import FileRepository
from app.lib.field_allowlist import FieldAllowlist


def make_repo(config=None):
    """Return (repo, mock_session) for the given FileNodeConfig.

    Defaults to FILE_NODE_REGISTRY[1] (sequencing_file).
    """
    session = AsyncMock(spec=AsyncSession)
    allowlist = Mock(spec=FieldAllowlist)
    return FileRepository(session, allowlist, config or FILE_NODE_REGISTRY[1]), session


class _AsyncIterator:
    """Minimal async iterator that wraps a plain list."""

    def __init__(self, rows: list) -> None:
        self._iter = iter(rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _mock_result(rows: list) -> AsyncMock:
    """Build a mock AsyncResult that asynchronously yields the given rows."""
    mock_result = _AsyncIterator(rows)
    mock_result.consume = AsyncMock()  # type: ignore[attr-defined]
    return mock_result  # type: ignore[return-value]


@pytest.mark.unit
class TestCountForPagination:

    @pytest.mark.asyncio
    async def test_returns_integer_count(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_mock_result([{"total_count": 42}]))

        count = await repo.count_for_pagination({})

        assert count == 42

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_records(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_mock_result([]))

        count = await repo.count_for_pagination({})

        assert count == 0

    @pytest.mark.asyncio
    async def test_uses_config_node_label_in_cypher(self):
        maf_config = FileNodeConfig("methylation_array_file", "of_methylation_array_file")
        repo, session = make_repo(maf_config)
        session.run = AsyncMock(return_value=_mock_result([{"total_count": 5}]))

        await repo.count_for_pagination({})

        cypher_used = session.run.call_args[0][0]
        assert "methylation_array_file" in cypher_used
        assert "of_methylation_array_file" in cypher_used
        assert "sequencing_file" not in cypher_used

    @pytest.mark.asyncio
    async def test_sequencing_config_uses_sequencing_label(self):
        repo, session = make_repo(FILE_NODE_REGISTRY[1])
        session.run = AsyncMock(return_value=_mock_result([{"total_count": 100}]))

        await repo.count_for_pagination({})

        cypher_used = session.run.call_args[0][0]
        assert "sequencing_file" in cypher_used
        assert "of_sequencing_file" in cypher_used

    @pytest.mark.asyncio
    async def test_invalid_file_type_short_circuits_without_db_call(self):
        """Invalid file_type enum value must be caught in Python — session.run must NOT be called."""
        repo, session = make_repo()
        session.run = AsyncMock()

        count = await repo.count_for_pagination({"file_type": "INVALID"})

        assert count == 0
        session.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_filters_uses_collected_study_paths(self):
        """Pattern 3 count: collect each study path to prevent Cartesian product before size() check."""
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_mock_result([{"total_count": 100}]))

        await repo.count_for_pagination({})

        cypher = session.run.call_args[0][0]
        assert "collect(DISTINCT st1) AS st1_list" in cypher
        assert "collect(DISTINCT st2) AS st2_list" in cypher
        assert "size(st1_list) > 0 OR size(st2_list) > 0" in cypher
        assert "RETURN count(DISTINCT sf)" in cypher

    @pytest.mark.asyncio
    async def test_raises_on_database_error(self):
        """DB failures propagate — caller handles partial-failure policy."""
        repo, session = make_repo()
        session.run = AsyncMock(side_effect=Exception("connection lost"))

        with pytest.raises(Exception, match="connection lost"):
            await repo.count_for_pagination({})
