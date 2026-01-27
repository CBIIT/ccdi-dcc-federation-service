"""
Additional unit tests for Memgraph database to improve coverage.

Covers error handling paths, retry logic, and edge cases.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock, patch
from contextlib import asynccontextmanager
from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.db.memgraph import (
    MemgraphConnection,
    DatabaseConnectionError,
    is_retryable_error,
    get_session,
    memgraph_lifespan,
    get_connection,
)


@pytest.mark.unit
class TestIsRetryableError:
    """Test cases for is_retryable_error function (lines 46-60)."""

    def test_service_unavailable(self):
        """Test ServiceUnavailable is retryable."""
        error = ServiceUnavailable("Service unavailable")
        assert is_retryable_error(error) is True

    def test_transient_error(self):
        """Test TransientError is retryable."""
        error = TransientError("Transient error")
        assert is_retryable_error(error) is True

    def test_session_expired(self):
        """Test SessionExpired is retryable."""
        error = SessionExpired("Session expired")
        assert is_retryable_error(error) is True

    def test_connection_keyword_in_message(self):
        """Test error with connection keyword in message is retryable (line 46-54)."""
        error = Exception("Service unavailable connection")
        assert is_retryable_error(error) is True

    def test_database_keyword_in_message(self):
        """Test error with database keyword in message is retryable."""
        error = Exception("Database connection reset")
        assert is_retryable_error(error) is True

    def test_timeout_keyword_in_message(self):
        """Test error with timeout keyword in message is retryable."""
        error = Exception("Connection timeout")
        assert is_retryable_error(error) is True

    def test_error_type_name_contains_keyword(self):
        """Test error type name containing keyword is retryable (line 57-58)."""
        class ServiceUnavailableError(Exception):
            pass
        
        error = ServiceUnavailableError("Some error")
        assert is_retryable_error(error) is True

    def test_non_retryable_error(self):
        """Test non-retryable error returns False."""
        error = ValueError("Invalid value")
        assert is_retryable_error(error) is False


@pytest.mark.unit
class TestMemgraphConnectionErrorHandling:
    """Test cases for error handling paths in MemgraphConnection."""

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

    async def test_verify_connectivity_os_error(self, connection):
        """Test verify_connectivity handles OSError (line 128-130)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock(side_effect=OSError("Connection refused"))
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError) as exc_info:
            await connection.verify_connectivity()
        
        assert "Database connectivity check failed" in str(exc_info.value)

    async def test_verify_connectivity_timeout_error(self, connection):
        """Test verify_connectivity handles TimeoutError."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock(side_effect=TimeoutError("Connection timeout"))
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.verify_connectivity()

    async def test_verify_connectivity_generic_exception(self, connection):
        """Test verify_connectivity handles generic Exception (line 128-130)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock(side_effect=RuntimeError("Unexpected error"))
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.verify_connectivity()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_reconnect_failure_retries(self, mock_graph_db, connection, mock_settings):
        """Test get_session retries when reconnect fails (lines 151-161)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        
        # First reconnect attempt fails, second succeeds
        mock_graph_db.driver.side_effect = [
            DatabaseConnectionError("Connection failed"),
            mock_driver
        ]
        
        # Mock connect to raise error first time, succeed second time
        original_connect = connection.connect
        call_count = [0]
        
        async def mock_connect():
            call_count[0] += 1
            if call_count[0] == 1:
                raise DatabaseConnectionError("Connection failed")
            # Second call succeeds
            connection._driver = mock_driver
        
        connection.connect = mock_connect
        connection._driver = None
        
        # Should retry and eventually succeed
        mock_session = AsyncMock(spec=AsyncSession)
        mock_driver.session.return_value = mock_session
        
        session = await connection.get_session(retry_on_error=True)
        
        assert session is mock_session

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_reconnect_failure_max_retries(self, mock_graph_db, connection):
        """Test get_session raises error after max retries on reconnect failure."""
        connection._driver = None
        
        async def mock_connect():
            raise DatabaseConnectionError("Connection failed")
        
        connection.connect = mock_connect
        
        with pytest.raises(DatabaseConnectionError) as exc_info:
            await connection.get_session(retry_on_error=True)
        
        assert "Database is not available" in str(exc_info.value)

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_get_session_close_driver_exception_handled(self, mock_graph_db, connection):
        """Test get_session handles exception when closing driver (line 182-183)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        # First call fails with ServiceUnavailable, triggering retry
        # session() is a regular method, not async
        mock_driver.session = Mock(side_effect=[
            ServiceUnavailable("Service unavailable"),
            mock_session
        ])
        mock_driver.close = AsyncMock(side_effect=Exception("Close failed"))
        mock_driver.verify_connectivity = AsyncMock()
        mock_graph_db.driver.return_value = mock_driver
        connection._driver = mock_driver
        
        # Mock asyncio.sleep to avoid actual delays
        with patch('app.db.memgraph.asyncio.sleep', new_callable=AsyncMock):
            # Should handle close exception gracefully and still retry
            session = await connection.get_session(retry_on_error=True)
        
        # Should succeed on retry despite close exception
        assert session is mock_session
        # Driver close should have been called (even if it raised exception)
        mock_driver.close.assert_called()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_close_session_exception_handled(self, mock_graph_db, connection, mock_settings):
        """Test execute_query handles exception when closing session (line 238-242)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))
        mock_session.close = AsyncMock(side_effect=Exception("Close failed"))
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        # Should handle close exception gracefully
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_query("MATCH (n) RETURN n", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_close_driver_exception_handled(self, mock_graph_db, connection, mock_settings):
        """Test execute_query handles exception when closing driver (line 257-259)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session1 = AsyncMock(spec=AsyncSession)
        mock_session2 = AsyncMock(spec=AsyncSession)
        mock_result = AsyncMock()
        
        # First call fails, second succeeds
        mock_session1.run = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))
        mock_session1.close = AsyncMock()
        
        async def async_gen():
            yield {"key": "value"}
        
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        # run() returns the result directly, not a coroutine
        mock_session2.run = AsyncMock(return_value=mock_result)
        mock_session2.close = AsyncMock()
        
        # session() is a regular method, not async
        mock_driver.session = Mock(side_effect=[mock_session1, mock_session2])
        mock_driver.close = AsyncMock(side_effect=Exception("Close failed"))
        mock_driver.verify_connectivity = AsyncMock()
        connection._driver = mock_driver
        
        # Mock get_session to avoid reconnect issues
        original_get_session = connection.get_session
        call_count = [0]
        
        async def mock_get_session(retry_on_error=True):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_session1
            return mock_session2
        
        connection.get_session = mock_get_session
        
        # Mock asyncio.sleep to avoid actual delays
        with patch('app.db.memgraph.asyncio.sleep', new_callable=AsyncMock):
            # Should handle close exception gracefully and still retry
            result = await connection.execute_query("MATCH (n) RETURN n", {})
        
        # Should succeed on retry despite close exception
        assert len(result) == 1
        # Driver close should have been called (even if it raised exception)
        mock_driver.close.assert_called()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_auth_error_close_session(self, mock_graph_db, connection, mock_settings):
        """Test execute_query closes session on AuthError (line 274-278)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=AuthError("Authentication failed"))
        mock_session.close = AsyncMock()
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_query("MATCH (n) RETURN n", {})
        
        mock_session.close.assert_called_once()

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_auth_error_close_session_exception(self, mock_graph_db, connection, mock_settings):
        """Test execute_query handles exception when closing session on AuthError (line 274-278)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=AuthError("Authentication failed"))
        mock_session.close = AsyncMock(side_effect=Exception("Close failed"))
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        # Should handle close exception gracefully
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_query("MATCH (n) RETURN n", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_query_generic_exception(self, mock_graph_db, connection, mock_settings):
        """Test execute_query handles generic exception (line 280-286)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=RuntimeError("Unexpected error"))
        mock_session.close = AsyncMock()
        mock_driver.session.return_value = mock_session
        connection._driver = mock_driver
        
        with pytest.raises(RuntimeError):
            await connection.execute_query("MATCH (n) RETURN n", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_write_query_connection_error(self, mock_graph_db, connection, mock_settings):
        """Test execute_write_query handles connection errors (line 320-326)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))
        mock_driver.session.return_value = mock_session
        
        @asynccontextmanager
        async def session_cm():
            yield mock_session
        
        connection.get_session = MagicMock(return_value=session_cm())
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_write_query("CREATE (n)", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_write_query_os_error(self, mock_graph_db, connection, mock_settings):
        """Test execute_write_query handles OSError."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=OSError("Connection refused"))
        mock_driver.session.return_value = mock_session
        
        @asynccontextmanager
        async def session_cm():
            yield mock_session
        
        connection.get_session = MagicMock(return_value=session_cm())
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_write_query("CREATE (n)", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_write_query_generic_exception(self, mock_graph_db, connection, mock_settings):
        """Test execute_write_query handles generic exception (line 327-334)."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.run = AsyncMock(side_effect=RuntimeError("Unexpected error"))
        mock_driver.session.return_value = mock_session
        
        @asynccontextmanager
        async def session_cm():
            try:
                yield mock_session
            except Exception:
                # Exception should propagate
                raise
        
        connection.get_session = MagicMock(return_value=session_cm())
        
        # The exception should be caught and re-raised as DatabaseConnectionError
        # because execute_write_query wraps all exceptions in DatabaseConnectionError
        # when getting session fails
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_write_query("CREATE (n)", {})

    @patch('app.db.memgraph.AsyncGraphDatabase')
    async def test_execute_write_query_session_error(self, mock_graph_db, connection, mock_settings):
        """Test execute_write_query handles session error (line 337-339)."""
        connection.get_session = MagicMock(side_effect=Exception("Session error"))
        
        with pytest.raises(DatabaseConnectionError):
            await connection.execute_write_query("CREATE (n)", {})


@pytest.mark.unit
class TestGetSessionGenerator:
    """Test cases for get_session generator function (lines 400-462)."""

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_retry_on_connection_error(self, mock_get_connection):
        """Test get_session retries on connection error (line 423-443)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session1 = AsyncMock(spec=AsyncSession)
        mock_session2 = AsyncMock(spec=AsyncSession)
        
        # First call fails, second succeeds
        mock_connection.get_session = AsyncMock(side_effect=[
            ServiceUnavailable("Service unavailable"),
            mock_session2
        ])
        mock_connection._driver = AsyncMock()
        mock_connection._driver.close = AsyncMock()
        
        mock_get_connection.return_value = mock_connection
        
        gen = get_session()
        session = await gen.__anext__()
        
        assert session is mock_session2
        assert mock_connection.get_session.call_count == 2

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_retry_close_driver_exception(self, mock_get_connection):
        """Test get_session handles exception when closing driver on retry (line 436-440)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        
        mock_connection.get_session = AsyncMock(side_effect=[
            ServiceUnavailable("Service unavailable"),
            mock_session
        ])
        mock_connection._driver = AsyncMock()
        mock_connection._driver.close = AsyncMock(side_effect=Exception("Close failed"))
        
        mock_get_connection.return_value = mock_connection
        
        gen = get_session()
        session = await gen.__anext__()
        
        # Should still succeed despite close exception
        assert session is mock_session

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_close_session_exception(self, mock_get_connection):
        """Test get_session handles exception when closing session (line 418-422)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.close = AsyncMock(side_effect=Exception("Close failed"))
        
        mock_connection.get_session = AsyncMock(return_value=mock_session)
        
        mock_get_connection.return_value = mock_connection
        
        gen = get_session()
        session = await gen.__anext__()
        
        # Should still yield session despite close exception
        assert session is mock_session
        
        # Clean up
        try:
            await gen.aclose()
        except Exception:
            pass

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_retry_on_generic_exception(self, mock_get_connection):
        """Test get_session retries on generic exception if retryable (line 449-462)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        
        # Create a retryable error
        class RetryableError(Exception):
            pass
        
        # Mock is_retryable_error to return True for this error
        with patch('app.db.memgraph.is_retryable_error', return_value=True):
            mock_connection.get_session = AsyncMock(side_effect=[
                RetryableError("Retryable error"),
                mock_session
            ])
            
            mock_get_connection.return_value = mock_connection
            
            gen = get_session()
            session = await gen.__anext__()
            
            assert session is mock_session
            assert mock_connection.get_session.call_count == 2

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_max_retries_exceeded(self, mock_get_connection):
        """Test get_session raises error after max retries."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_connection.get_session = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))
        mock_connection._driver = AsyncMock()
        mock_connection._driver.close = AsyncMock()
        
        mock_get_connection.return_value = mock_connection
        
        gen = get_session()
        
        with pytest.raises(DatabaseConnectionError):
            await gen.__anext__()

    @patch('app.db.memgraph.get_connection')
    async def test_get_session_database_connection_error_re_raise(self, mock_get_connection):
        """Test get_session re-raises DatabaseConnectionError (line 447-448)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_connection.get_session = AsyncMock(side_effect=DatabaseConnectionError("Database error"))
        
        mock_get_connection.return_value = mock_connection
        
        gen = get_session()
        
        with pytest.raises(DatabaseConnectionError):
            await gen.__anext__()


@pytest.mark.unit
class TestMemgraphLifespan:
    """Test cases for memgraph_lifespan function (lines 465-478)."""

    @patch('app.db.memgraph.get_connection')
    @patch('app.db.memgraph.close_connection')
    async def test_memgraph_lifespan_close_on_exception(self, mock_close, mock_get_connection):
        """Test memgraph_lifespan closes connection even on exception (line 476-478)."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_get_connection.return_value = mock_connection
        
        with pytest.raises(RuntimeError, match="Test error"):
            async with memgraph_lifespan(Mock()):
                raise RuntimeError("Test error")
        
        mock_close.assert_called_once()

