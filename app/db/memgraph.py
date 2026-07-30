"""
Memgraph database connection and session management.

This module provides a connection pool and session management for Memgraph
using the Neo4j Python driver (which is compatible with Memgraph).
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional, TypeVar

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')

# Serializes driver create / invalidate so one request's retry cannot close the
# shared AsyncDriver out from under other in-flight sessions without coordination.
_driver_lock = asyncio.Lock()
# Serializes module-level MemgraphConnection singleton construction (cold start).
_connection_lock = asyncio.Lock()


class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass


def is_retryable_error(error: Exception) -> bool:
    """
    Check if an error is retryable (connection-related, transient).
    
    Args:
        error: The exception to check
        
    Returns:
        True if the error should trigger a retry
    """
    error_type = type(error).__name__
    error_str = str(error).lower()
    
    # Check for Neo4j retryable exceptions
    if isinstance(error, (ServiceUnavailable, TransientError, SessionExpired)):
        return True
    
    # Check for connection-related keywords
    connection_keywords = [
        'service unavailable', 'defunct connection', 'connection', 'database',
        'unavailable', 'timeout', 'network', 'broken pipe',
        'connection reset', 'connection closed', 'session expired',
        'transient', 'temporary',
    ]
    
    if any(keyword in error_str for keyword in connection_keywords):
        return True
    
    # Check error type name
    if any(keyword in error_type for keyword in ['ServiceUnavailable', 'Transient', 'SessionExpired']):
        return True
    
    return False


def is_memory_limit_error(error: Exception) -> bool:
    """
    True for Memgraph "Memory limit exceeded" errors.

    These are deterministic for a given query — the query needs more memory than the
    instance allows, so re-running it just OOMs again. Such errors are technically a
    `TransientError` (so `is_retryable_error` returns True), but retrying them only
    multiplies latency before the same failure, so callers should NOT retry them.
    """
    return "memory limit" in str(error).lower()


async def run_count_query_with_retry(
    session: AsyncSession,
    cypher: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    max_retries: int = 2,
    retry_delay: float = 0.1,
) -> int:
    """
    Execute a `RETURN count(*) AS total_count` query on the given session, retrying
    transient (retryable) failures before giving up.

    A count query is typically a heavy full-scan aggregation; a transient failure
    (timeout, connection blip) must not be silently reported as 0 while a paginated
    data query still returns rows. Retries retryable errors with exponential backoff;
    re-raises the last error after exhausting retries, or immediately on a
    non-retryable error, so the caller can apply its own fallback.

    Memgraph "Memory limit exceeded" errors are NOT retried: they are deterministic
    for a given query (retrying just OOMs again after multiplying latency), so they
    raise immediately.

    Returns the total_count (0 only if the query legitimately yields no count row).
    """
    for attempt in range(max_retries + 1):
        try:
            result = await session.run(cypher, params or {})
            records = []
            async for row in result:
                records.append(dict(row))
            await result.consume()
            return records[0].get("total_count", 0) if records else 0
        except Exception as exc:
            if (
                is_retryable_error(exc)
                and not is_memory_limit_error(exc)
                and attempt < max_retries
            ):
                logger.warning(
                    "Count query transient failure, retrying",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            raise


class MemgraphConnection:
    """Memgraph database connection manager."""
    
    def __init__(self):
        """Initialize the connection manager."""
        self._driver: Optional[AsyncDriver] = None
        self._settings = get_settings()

    async def _invalidate_driver(self) -> None:
        """
        Drop and close the current driver under lock (compare-and-swap).

        Only the coroutine that still sees this instance's current driver clears
        it, so concurrent invalidations do not double-close or race with connect().
        Prefer session-only retries for transient query errors; call this when
        session *creation* fails or the driver is known defunct.
        """
        async with _driver_lock:
            driver = self._driver
            if driver is None:
                return
            self._driver = None
        try:
            await driver.close()
        except Exception:
            pass

    async def connect(self) -> None:
        """Establish connection to Memgraph."""
        async with _driver_lock:
            if self._driver is not None:
                return
            driver: Optional[AsyncDriver] = None
            try:
                # Prepare authentication only if both user and password are provided (password may be blank in some deployments)
                auth = None
                if self._settings.memgraph_user and self._settings.memgraph_password:
                    auth = (
                        self._settings.memgraph_user,
                        self._settings.memgraph_password,
                    )
                # Use environment-configured URI & pool settings from `Settings`
                # Set max_connection_lifetime lower than any upstream idle timeout (LB/NAT)
                # If LB idle is 350s, set lifetime to ~300s to prevent stale connections
                # Default is 3600s, but should be adjusted based on infrastructure
                connection_lifetime = min(
                    self._settings.memgraph_max_connection_lifetime,
                    300  # Cap at 300s to prevent stale connections from idle timeouts
                )

                driver = AsyncGraphDatabase.driver(
                    self._settings.memgraph_uri,
                    auth=auth,
                    max_connection_lifetime=connection_lifetime,
                    max_connection_pool_size=self._settings.memgraph_max_connection_pool_size,
                )
                # Verify on the local handle before publishing to self._driver so
                # concurrent get_session callers never observe an unverified driver.
                await driver.verify_connectivity()
                self._driver = driver
                driver = None  # ownership transferred; skip close in except

                logger.info(
                    "Connected to Memgraph",
                    uri=self._settings.memgraph_uri,
                    database=self._settings.memgraph_database
                )

            except (ServiceUnavailable, AuthError, OSError, TimeoutError) as e:
                logger.error("Failed to connect to Memgraph", error=str(e))
                # Don't raise - allow app to start, connection will be retried on first use
                if driver is not None:
                    try:
                        await driver.close()
                    except Exception:
                        pass
                self._driver = None
                raise DatabaseConnectionError(f"Database connection failed: {str(e)}") from e

    async def disconnect(self) -> None:
        """Close the connection to Memgraph."""
        await self._invalidate_driver()
        logger.info("Disconnected from Memgraph")
    
    async def verify_connectivity(self) -> None:
        """Verify connection to Memgraph."""
        if not self._driver:
            raise DatabaseConnectionError("Driver not initialized")
        
        try:
            await self._driver.verify_connectivity()
        except (ServiceUnavailable, AuthError, OSError, TimeoutError) as e:
            logger.error("Memgraph connectivity check failed", error=str(e))
            raise DatabaseConnectionError(f"Database connectivity check failed: {str(e)}") from e
        except Exception as e:
            logger.error("Memgraph connectivity check failed", error=str(e))
            raise DatabaseConnectionError(f"Database connectivity check failed: {str(e)}") from e
    
    async def get_session(self, retry_on_error: bool = True) -> AsyncSession:
        """
        Get a database session with optional retry logic.
        
        Args:
            retry_on_error: If True, will retry session creation on connection errors
            
        Returns:
            AsyncSession instance
        """
        max_retries = 3 if retry_on_error else 0
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                if not self._driver:
                    # Try to reconnect if driver is not initialized
                    try:
                        await self.connect()
                    except DatabaseConnectionError:
                        if retry_count < max_retries:
                            backoff_time = 0.5 * (retry_count + 1)
                            logger.warning(
                                f"Driver not initialized, retrying connection (attempt {retry_count + 1}/{max_retries})",
                                backoff_seconds=backoff_time
                            )
                            await asyncio.sleep(backoff_time)
                            retry_count += 1
                            continue
                        raise DatabaseConnectionError("Database is not available")
                
                session = self._driver.session(
                    database=self._settings.memgraph_database
                )
                return session
                
            except (ServiceUnavailable, TransientError, SessionExpired, OSError, TimeoutError) as e:
                if retry_on_error and retry_count < max_retries and is_retryable_error(e):
                    backoff_time = 0.5 * (retry_count + 1)
                    logger.warning(
                        f"Failed to create database session, retrying (attempt {retry_count + 1}/{max_retries})",
                        error=str(e),
                        error_type=type(e).__name__,
                        backoff_seconds=backoff_time,
                        is_connection_error=True
                    )
                    # Session creation failed — invalidate driver under lock, then retry
                    await self._invalidate_driver()
                    await asyncio.sleep(backoff_time)
                    retry_count += 1
                    continue
                else:
                    logger.error("Failed to create database session", error=str(e), error_type=type(e).__name__)
                    raise DatabaseConnectionError(f"Failed to create database session: {str(e)}") from e
            except AuthError as e:
                # Auth errors are not retryable
                logger.error("Authentication failed", error=str(e))
                raise DatabaseConnectionError(f"Authentication failed: {str(e)}") from e


# Global connection instance
_connection: Optional[MemgraphConnection] = None


async def get_connection() -> MemgraphConnection:
    """Get the global Memgraph connection (singleton; locked at cold start)."""
    global _connection

    if _connection is not None:
        return _connection

    async with _connection_lock:
        if _connection is not None:
            return _connection

        _connection = MemgraphConnection()
        try:
            await _connection.connect()
        except DatabaseConnectionError as e:
            # Log but don't raise - allow app to start
            logger.warning(
                "Database connection failed during initialization. "
                "Application will start but database operations will fail until connection is established.",
                error=str(e)
            )
            # Set driver to None so it can be retried later
            _connection._driver = None

        return _connection


async def close_connection() -> None:
    """Close the global Memgraph connection."""
    global _connection

    async with _connection_lock:
        if _connection:
            await _connection.disconnect()
            _connection = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session with retry logic (async generator for dependency injection).

    Retries apply only to session *acquisition*. The yield is outside the retry
    try/except so exceptions from the request (thrown back into the generator at
    yield) propagate normally and are not misclassified as connection failures.
    """
    max_retries = 3
    retry_count = 0
    session: Optional[AsyncSession] = None

    while retry_count <= max_retries:
        try:
            connection = await get_connection()
            session = await connection.get_session(retry_on_error=(retry_count == 0))
            break
        except (ServiceUnavailable, TransientError, SessionExpired, OSError, TimeoutError) as e:
            if retry_count < max_retries and is_retryable_error(e):
                backoff_time = 0.5 * (retry_count + 1)
                logger.warning(
                    f"Failed to get database session, retrying (attempt {retry_count + 1}/{max_retries})",
                    error=str(e),
                    error_type=type(e).__name__,
                    backoff_seconds=backoff_time,
                    is_connection_error=True,
                )
                # Do not close the shared driver on acquisition retry — that would
                # race other concurrent requests still using the pool.
                await asyncio.sleep(backoff_time)
                retry_count += 1
                continue
            raise DatabaseConnectionError(f"Database connection error: {str(e)}") from e
        except DatabaseConnectionError:
            raise
        except Exception as e:
            if retry_count < max_retries and is_retryable_error(e):
                backoff_time = 0.5 * (retry_count + 1)
                logger.warning(
                    f"Unexpected error getting session, retrying (attempt {retry_count + 1}/{max_retries})",
                    error=str(e),
                    error_type=type(e).__name__,
                    backoff_seconds=backoff_time,
                )
                await asyncio.sleep(backoff_time)
                retry_count += 1
                continue
            raise DatabaseConnectionError(f"Database is not available: {str(e)}") from e

    if session is None:
        raise DatabaseConnectionError("Database is not available: failed to acquire session")

    try:
        yield session
    finally:
        try:
            await session.close()
        except Exception:
            pass


@asynccontextmanager
async def memgraph_lifespan(settings: Settings):
    """
    Context manager for Memgraph lifespan.
    
    Args:
        settings: Application settings
    """
    # Startup - try to initialize the connection, but don't fail if it's unavailable
    try:
        await get_connection()
    except DatabaseConnectionError as e:
        # Log warning but allow app to start
        logger.warning(
            "Database connection unavailable at startup. "
            "Application will start but database operations will return 500 errors until connection is established.",
            error=str(e)
        )

    try:
        yield
    finally:
        # Shutdown - close the connection
        await close_connection()
