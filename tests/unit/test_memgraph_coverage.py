"""
Additional unit tests for memgraph.py to improve coverage.

Tests missing error paths, retry logic, and edge cases.
"""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, MagicMock, patch

import pytest

from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.db.memgraph import (
    MemgraphConnection,
    DatabaseConnectionError,
    is_retryable_error,
    get_session,
    get_connection,
    close_connection,
    memgraph_lifespan,
)


@pytest.mark.unit
class TestMemgraphConnectionAdditional:
    """Additional test cases for MemgraphConnection."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.memgraph_uri = "bolt://localhost:7687"
        settings.memgraph_user = "neo4j"
        settings.memgraph_password = "password"
        settings.memgraph_database = "memgraph"
        settings.memgraph_max_connection_lifetime = 500
        settings.memgraph_max_connection_pool_size = 50
        return settings

    @pytest.fixture
    def connection(self, mock_settings):
        """Create MemgraphConnection instance."""
        with patch('app.db.memgraph.get_settings', return_value=mock_settings):
            conn = MemgraphConnection()
            return conn

    async def test_connect_without_auth(self, connection):
        """Test connect when user/password not provided."""
        connection._settings.memgraph_user = None
        connection._settings.memgraph_password = None
        
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        
        with patch('neo4j.AsyncGraphDatabase.driver', return_value=mock_driver):
            await connection.connect()
            
            assert connection._driver is not None

    async def test_connect_connection_lifetime_capped(self, connection):
        """Test that connection lifetime is capped at 300s."""
        connection._settings.memgraph_max_connection_lifetime = 1000  # > 300
        
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.verify_connectivity = AsyncMock()
        
        with patch('neo4j.AsyncGraphDatabase.driver', return_value=mock_driver) as mock_driver_class:
            await connection.connect()
            
            # Check that max_connection_lifetime was capped at 300
            call_kwargs = mock_driver_class.call_args[1]
            assert call_kwargs['max_connection_lifetime'] == 300

    async def test_get_session_retry_exhausted(self, connection):
        """Test get_session when retries are exhausted."""
        connection._driver = None
        
        with patch.object(connection, 'connect', side_effect=DatabaseConnectionError("Connection failed")):
            with pytest.raises(DatabaseConnectionError):
                await connection.get_session(retry_on_error=True)

    async def test_get_session_invalidate_close_error(self, connection):
        """Session-creation failure invalidates driver; close errors are swallowed."""
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_session = AsyncMock(spec=AsyncSession)

        call_count = 0
        def session_factory(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ServiceUnavailable("Service unavailable")
            return mock_session

        mock_driver.session = Mock(side_effect=session_factory)
        mock_driver.close = AsyncMock(side_effect=Exception("Close error"))
        connection._driver = mock_driver

        with patch('asyncio.sleep'):
            async def mock_connect():
                connection._driver = mock_driver
            connection.connect = mock_connect

            session = await connection.get_session(retry_on_error=True)
            assert session is mock_session
            mock_driver.close.assert_called()

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

    async def test_get_connection_creates_new(self, mock_settings):
        """Test get_connection creates new connection."""
        import app.db.memgraph as memgraph_module
        memgraph_module._connection = None
        
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_connection.connect = AsyncMock()
        
        with patch('app.db.memgraph.MemgraphConnection', return_value=mock_connection):
            with patch('app.db.memgraph.get_settings', return_value=mock_settings):
                result = await get_connection()
                
                assert result is not None
                mock_connection.connect.assert_called_once()

    async def test_get_connection_handles_connection_error(self, mock_settings):
        """Test get_connection handles connection error gracefully."""
        import app.db.memgraph as memgraph_module
        memgraph_module._connection = None
        
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_connection.connect = AsyncMock(side_effect=DatabaseConnectionError("Connection failed"))
        
        with patch('app.db.memgraph.MemgraphConnection', return_value=mock_connection):
            with patch('app.db.memgraph.get_settings', return_value=mock_settings):
                with patch('app.db.memgraph.logger'):
                    result = await get_connection()
                    
                    # Should return connection even if connect fails
                    assert result is not None
                    assert result._driver is None

    async def test_close_connection_none(self):
        """Test close_connection when connection is None."""
        import app.db.memgraph as memgraph_module
        memgraph_module._connection = None
        
        # Should not raise
        await close_connection()


@pytest.mark.unit
class TestGetSessionCoverage:
    """Test cases for get_session function."""

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

    async def test_get_session_retry_on_connection_error(self, mock_settings):
        """Test get_session retries on connection error."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        
        # First call raises error, second succeeds
        call_count = 0
        async def mock_get_session(retry_on_error):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ServiceUnavailable("Service unavailable")
            return mock_session
        
        mock_connection.get_session = mock_get_session
        
        with patch('app.db.memgraph.get_connection', return_value=mock_connection):
            with patch('asyncio.sleep'):
                async for session in get_session():
                    assert session is mock_session
                    break

    async def test_get_session_retry_exhausted_without_closing_driver(self, mock_settings):
        """Module get_session exhausts retries without tearing down the shared driver."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_driver = AsyncMock(spec=AsyncDriver)
        mock_driver.close = AsyncMock()
        mock_connection._driver = mock_driver
        mock_connection._invalidate_driver = AsyncMock()
        mock_connection.get_session = AsyncMock(side_effect=ServiceUnavailable("Service unavailable"))

        with patch('app.db.memgraph.get_connection', return_value=mock_connection):
            with patch('asyncio.sleep'):
                with pytest.raises(DatabaseConnectionError):
                    async for _ in get_session():
                        pass

        mock_driver.close.assert_not_called()
        mock_connection._invalidate_driver.assert_not_called()

    async def test_get_session_non_retryable_error(self, mock_settings):
        """Test get_session doesn't retry on non-retryable error."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        # get_session raises ValueError (non-retryable) - this is not caught by the retry logic
        # The module-level get_session wraps connection.get_session and re-raises non-retryable errors
        async def mock_get_session(retry_on_error):
            raise ValueError("Not retryable")
        
        mock_connection.get_session = mock_get_session
        
        with patch('app.db.memgraph.get_connection', return_value=mock_connection):
            # Non-retryable errors should be re-raised as-is (wrapped in DatabaseConnectionError by module-level get_session)
            # But the actual error message should indicate it's not retryable
            with pytest.raises(DatabaseConnectionError, match="Database is not available"):
                async for _ in get_session():
                    pass

    async def test_get_session_session_close_error(self, mock_settings):
        """Test get_session handles session.close() error."""
        mock_connection = AsyncMock(spec=MemgraphConnection)
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.close = AsyncMock(side_effect=Exception("Close error"))
        mock_connection.get_session = AsyncMock(return_value=mock_session)
        
        with patch('app.db.memgraph.get_connection', return_value=mock_connection):
            async for session in get_session():
                # Should handle close error gracefully
                assert session is mock_session
                break


@pytest.mark.unit
class TestIsRetryableErrorAdditional:
    """Additional test cases for is_retryable_error."""

    def test_network_keyword(self):
        """Test error with 'network' keyword is retryable."""
        error = Exception("Network error occurred")
        assert is_retryable_error(error) is True

    def test_broken_pipe_keyword(self):
        """Test error with 'broken pipe' keyword is retryable."""
        error = Exception("Broken pipe error")
        assert is_retryable_error(error) is True

    def test_connection_reset_keyword(self):
        """Test error with 'connection reset' keyword is retryable."""
        error = Exception("Connection reset by peer")
        assert is_retryable_error(error) is True

    def test_connection_closed_keyword(self):
        """Test error with 'connection closed' keyword is retryable."""
        error = Exception("Connection closed")
        assert is_retryable_error(error) is True

    def test_transient_in_type_name(self):
        """Test error type name containing 'Transient' is retryable."""
        class TransientDatabaseError(Exception):
            pass
        
        error = TransientDatabaseError("Some error")
        assert is_retryable_error(error) is True
