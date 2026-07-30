"""
Additional unit tests for Memgraph database utilities.

Covers global connection helpers, async generator session, write queries, and count queries.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, MagicMock, patch

import pytest
from neo4j import AsyncSession
from neo4j.exceptions import ServiceUnavailable

import app.db.memgraph as memgraph
from app.db.memgraph import MemgraphConnection, DatabaseConnectionError


@pytest.mark.unit
class TestMemgraphModuleHelpers:
    """Tests for module-level helpers in memgraph."""

    async def test_get_connection_initializes_and_handles_failure(self):
        """get_connection should return a connection even if connect fails."""
        # Ensure clean global state
        memgraph._connection = None

        with patch.object(MemgraphConnection, "connect", side_effect=DatabaseConnectionError("fail")):
            conn = await memgraph.get_connection()

        assert isinstance(conn, MemgraphConnection)
        assert conn._driver is None

        # Clean up global
        memgraph._connection = None

    async def test_close_connection_disconnects(self):
        """close_connection should disconnect and clear global."""
        mock_conn = AsyncMock(spec=MemgraphConnection)
        memgraph._connection = mock_conn

        await memgraph.close_connection()

        mock_conn.disconnect.assert_called_once()
        assert memgraph._connection is None

    async def test_get_session_generator_success(self):
        """get_session generator yields a session and closes it."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_connection.get_session = AsyncMock(return_value=mock_session)

        async def fake_get_connection():
            return mock_connection

        with patch.object(memgraph, "get_connection", side_effect=fake_get_connection):
            gen = memgraph.get_session()
            session = await gen.__anext__()
            # Explicitly close generator to trigger cleanup
            await gen.aclose()

        assert session is mock_session
        mock_session.close.assert_called()

    async def test_memgraph_lifespan_calls_close(self):
        """memgraph_lifespan should call close_connection on exit."""
        with patch.object(memgraph, "get_connection", AsyncMock()) as mock_get_conn:
            with patch.object(memgraph, "close_connection", AsyncMock()) as mock_close:
                async with memgraph.memgraph_lifespan(Mock()):
                    pass
                mock_get_conn.assert_called_once()
                mock_close.assert_called_once()


@pytest.mark.unit
class TestMemgraphConnectionWriteAndCount:
    """Tests for write queries and count queries."""

    @pytest.fixture
    def connection(self):
        with patch("app.db.memgraph.get_settings", return_value=Mock()):
            return MemgraphConnection()

