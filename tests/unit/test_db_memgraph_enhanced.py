"""
Enhanced unit tests for database connection management.

Tests MemgraphConnection class methods with comprehensive mocking.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.db.memgraph import MemgraphConnection, DatabaseConnectionError, is_retryable_error


@pytest.mark.unit
class TestMemgraphConnection:
    """Test cases for MemgraphConnection class."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.memgraph_uri = "bolt://localhost:7687"
        settings.memgraph_user = "neo4j"
        settings.memgraph_password = "password"
        settings.memgraph_database = "memgraph"
        settings.memgraph_max_connection_lifetime = 300
        settings.memgraph_max_connection_pool_size = 50
        return settings

    @pytest.fixture
    def connection(self, mock_settings):
        """Create MemgraphConnection instance with mocked settings."""
        with patch('app.db.memgraph.get_settings', return_value=mock_settings):
            conn = MemgraphConnection()
            return conn

    async def test_initialization(self, connection):
        """Test connection initialization."""
        assert connection._driver is None
        assert connection._settings is not None

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_connect_success(self, mock_graph_db, connection, mock_settings):
        """Test successful connection."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        
        await connection.connect()
        
        assert connection._driver is not None
        mock_driver.verify_connectivity.assert_called_once()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_connect_without_auth(self, mock_graph_db, connection, mock_settings):
        """Test connection without authentication."""
        mock_settings.memgraph_user = None
        mock_settings.memgraph_password = None
        
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        
        await connection.connect()
        
        # Should call driver with auth=None
        mock_graph_db.driver.assert_called_once()
        call_kwargs = mock_graph_db.driver.call_args[1]
        assert call_kwargs.get("auth") is None

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_connect_service_unavailable(self, mock_graph_db, connection):
        """Test connection failure with ServiceUnavailable."""
        mock_graph_db.driver.side_effect = ServiceUnavailable("Service unavailable")
        
        with pytest.raises(DatabaseConnectionError):
            await connection.connect()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_connect_auth_error(self, mock_graph_db, connection):
        """Test connection failure with AuthError."""
        mock_graph_db.driver.side_effect = AuthError("Authentication failed")
        
        with pytest.raises(DatabaseConnectionError):
            await connection.connect()

    async def test_disconnect_with_driver(self, connection):
        """Test disconnecting when driver exists."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        connection._driver = mock_driver
        
        await connection.disconnect()
        
        mock_driver.close.assert_called_once()
        assert connection._driver is None

    async def test_disconnect_without_driver(self, connection):
        """Test disconnecting when no driver."""
        connection._driver = None
        
        # Should not raise
        await connection.disconnect()
        
        assert connection._driver is None

    async def test_verify_connectivity_success(self, connection):
        """Test successful connectivity verification."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        connection._driver = mock_driver
        
        await connection.verify_connectivity()
        
        mock_driver.verify_connectivity.assert_called_once()

    async def test_verify_connectivity_no_driver(self, connection):
        """Test connectivity verification without driver."""
        connection._driver = None
        
        with pytest.raises(DatabaseConnectionError):
            await connection.verify_connectivity()

    async def test_verify_connectivity_failure(self, connection):
        """Test connectivity verification failure."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.verify_connectivity()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_success(self, mock_graph_db, connection, mock_settings):
        """Test getting a session successfully."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        session = await connection.get_session()
        
        assert session is mock_session
        mock_driver.session.assert_called_once_with(database=mock_settings.memgraph_database)

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_reconnect(self, mock_graph_db, connection, mock_settings):
        """Test getting session when driver is None triggers reconnect."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_driver.session.return_value = mock_session
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        connection._driver = None
        
        session = await connection.get_session()
        
        assert session is mock_session
        assert connection._driver is not None

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_retry_on_error(self, mock_graph_db, connection, mock_settings):
        """Test get_session retries on connection errors."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        
        # First call fails, second succeeds
        mock_driver.session.side_effect = [
            ServiceUnavailable("Service unavailable"),
            mock_session
        ]
        mock_driver.close = AsyncMock()
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        connection._driver = mock_driver
        
        session = await connection.get_session(retry_on_error=True)
        
        assert session is mock_session
        assert mock_driver.session.call_count == 2

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_auth_error_no_retry(self, mock_graph_db, connection):
        """Test get_session does not retry on AuthError."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.session.side_effect = AuthError("Authentication failed")
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.get_session(retry_on_error=True)
        
        # Should not retry auth errors
        assert mock_driver.session.call_count == 1

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_success(self, mock_graph_db, connection, mock_settings):
        """Test executing a query successfully."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_result = AsyncMock()
        
        async def async_gen():
            yield {"key": "value"}
        
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.close = AsyncMock()
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        result = await connection.execute_query("MATCH (n) RETURN n", {})
        
        assert len(result) == 1
        assert result[0] == {"key": "value"}
        mock_session.close.assert_called_once()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_retry_on_error(self, mock_graph_db, connection, mock_settings):
        """Test execute_query retries on connection errors."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_result = AsyncMock()
        
        async def async_gen():
            yield {"key": "value"}
        
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.close = AsyncMock()
        
        # First call fails, second succeeds
        mock_driver.session.side_effect = [
            ServiceUnavailable("Service unavailable"),
            mock_session
        ]
        mock_driver.close = AsyncMock()
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        connection._driver = mock_driver
        
        result = await connection.execute_query("MATCH (n) RETURN n", {}, max_retries=3)
        
        assert len(result) == 1
        assert mock_driver.session.call_count == 2

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_max_retries_exceeded(self, mock_graph_db, connection):
        """Test execute_query raises error after max retries."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.session.side_effect = ServiceUnavailable("Service unavailable")
        mock_driver.close = AsyncMock()
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_query("MATCH (n) RETURN n", {}, max_retries=2)

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_auth_error_no_retry(self, mock_graph_db, connection):
        """Test execute_query does not retry on AuthError."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=AuthError("Authentication failed"))
        mock_session.close = AsyncMock()
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_query("MATCH (n) RETURN n", {})
        
        # Should not retry auth errors
        assert mock_session.run.call_count == 1

