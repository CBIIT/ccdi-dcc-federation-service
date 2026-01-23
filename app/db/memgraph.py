"""
Memgraph database connection and session management.

This module provides a connection pool and session management for Memgraph
using the Neo4j Python driver (which is compatible with Memgraph).
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, TypeVar

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError, SessionExpired

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


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
        'transient'
    ]
    
    if any(keyword in error_str for keyword in connection_keywords):
        return True
    
    # Check error type name
    if any(keyword in error_type for keyword in ['ServiceUnavailable', 'Transient', 'SessionExpired']):
        return True
    
    return False


class MemgraphConnection:
    """Memgraph database connection manager."""
    
    def __init__(self):
        """Initialize the connection manager."""
        self._driver: Optional[AsyncDriver] = None
        self._settings = get_settings()
    
    async def connect(self) -> None:
        """Establish connection to Memgraph."""
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
            
            self._driver = AsyncGraphDatabase.driver(
                self._settings.memgraph_uri,
                auth=auth,
                max_connection_lifetime=connection_lifetime,
                max_connection_pool_size=self._settings.memgraph_max_connection_pool_size,
            )
            # Test the connection
            await self.verify_connectivity()
            
            logger.info(
                "Connected to Memgraph",
                uri=self._settings.memgraph_uri,
                database=self._settings.memgraph_database
            )
            
        except (ServiceUnavailable, AuthError, OSError, TimeoutError) as e:
            logger.error("Failed to connect to Memgraph", error=str(e))
            # Don't raise - allow app to start, connection will be retried on first use
            self._driver = None
            raise DatabaseConnectionError(f"Database connection failed: {str(e)}") from e
    
    async def disconnect(self) -> None:
        """Close the connection to Memgraph."""
        if self._driver:
            await self._driver.close()
            self._driver = None
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
                    # Close driver to force fresh connection on retry
                    if self._driver:
                        try:
                            await self._driver.close()
                        except Exception:
                            pass
                        self._driver = None
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
    
    async def execute_query(
        self, 
        query: str, 
        parameters: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query with retry logic for connection errors.
        
        Wraps the entire operation: session creation + query execution + result consumption.
        On retryable errors, closes the session and gets a fresh one.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            max_retries: Maximum number of retries (default: 3)
            
        Returns:
            List of result records as dictionaries
        """
        retry_count = 0
        
        while retry_count <= max_retries:
            session = None
            try:
                # Get fresh session for each attempt
                session = await self.get_session(retry_on_error=(retry_count == 0))
                
                # Execute query and consume results
                result = await session.run(query, parameters or {})
                records = []
                async for record in result:
                    records.append(dict(record))
                # Ensure result is fully consumed
                await result.consume()
                
                # Success - close session and return
                await session.close()
                return records
                
            except (ServiceUnavailable, TransientError, SessionExpired, OSError, TimeoutError) as e:
                # Close session on error to ensure clean state
                if session:
                    try:
                        await session.close()
                    except Exception:
                        pass
                
                if retry_count < max_retries and is_retryable_error(e):
                    backoff_time = 0.5 * (retry_count + 1)
                    logger.warning(
                        f"Query execution failed with connection error, retrying (attempt {retry_count + 1}/{max_retries})",
                        query=query[:100] if query else None,
                        error=str(e),
                        error_type=type(e).__name__,
                        backoff_seconds=backoff_time,
                        is_connection_error=True
                    )
                    # Force driver refresh on retry
                    if self._driver:
                        try:
                            await self._driver.close()
                        except Exception:
                            pass
                        self._driver = None
                    await asyncio.sleep(backoff_time)
                    retry_count += 1
                    continue
                else:
                    logger.error(
                        "Query execution failed - database connection error",
                        query=query[:100] if query else None,
                        error=str(e),
                        error_type=type(e).__name__,
                        retry_count=retry_count
                    )
                    raise DatabaseConnectionError(f"Database connection error: {str(e)}") from e
            except AuthError as e:
                if session:
                    try:
                        await session.close()
                    except Exception:
                        pass
                logger.error("Authentication failed", error=str(e))
                raise DatabaseConnectionError(f"Authentication failed: {str(e)}") from e
            except Exception as e:
                if session:
                    try:
                        await session.close()
                    except Exception:
                        pass
                logger.error(
                    "Query execution failed",
                    query=query[:100] if query else None,
                    parameters=parameters,
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise e
    
    async def execute_write_query(
        self, 
        query: str, 
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a write Cypher query and return results.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        try:
            async with self.get_session() as session:
                try:
                    result = await session.run(query, parameters or {})
                    records = []
                    async for record in result:
                        records.append(dict(record))
                    await session.commit()
                    return records
                except (ServiceUnavailable, AuthError, OSError, TimeoutError) as e:
                    logger.error(
                        "Write query execution failed - database connection error",
                        query=query[:100] if query else None,
                        error=str(e)
                    )
                    raise DatabaseConnectionError(f"Database connection error: {str(e)}") from e
                except Exception as e:
                    logger.error(
                        "Write query execution failed",
                        query=query[:100] if query else None,
                        parameters=parameters,
                        error=str(e)
                    )
                    raise e
        except DatabaseConnectionError:
            raise
        except Exception as e:
            logger.error(
                "Failed to get database session for write query",
                error=str(e)
            )
            raise DatabaseConnectionError(f"Database is not available: {str(e)}") from e
    
    async def count_query(
        self, 
        query: str, 
        parameters: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Execute a count query and return the count.
        
        Args:
            query: Cypher count query
            parameters: Query parameters
            
        Returns:
            Count result
        """
        records = await self.execute_query(query, parameters)
        if records and 'count' in records[0]:
            return records[0]['count']
        return 0


# Global connection instance
_connection: Optional[MemgraphConnection] = None


async def get_connection() -> MemgraphConnection:
    """Get the global Memgraph connection."""
    global _connection
    
    if _connection is None:
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
    
    if _connection:
        await _connection.disconnect()
        _connection = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session with retry logic (async generator for dependency injection).
    
    Wraps session creation with retry logic for connection errors.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count <= max_retries:
        session = None
        try:
            connection = await get_connection()
            session = await connection.get_session(retry_on_error=(retry_count == 0))
            try:
                yield session
                # Success - break out of retry loop
                break
            finally:
                if session:
                    try:
                        await session.close()
                    except Exception:
                        pass
        except (ServiceUnavailable, TransientError, SessionExpired, OSError, TimeoutError) as e:
            if retry_count < max_retries and is_retryable_error(e):
                backoff_time = 0.5 * (retry_count + 1)
                logger.warning(
                    f"Failed to get database session, retrying (attempt {retry_count + 1}/{max_retries})",
                    error=str(e),
                    error_type=type(e).__name__,
                    backoff_seconds=backoff_time,
                    is_connection_error=True
                )
                # Force connection refresh
                global _connection
                if _connection and _connection._driver:
                    try:
                        await _connection._driver.close()
                    except Exception:
                        pass
                    _connection._driver = None
                await asyncio.sleep(backoff_time)
                retry_count += 1
                continue
            else:
                # Re-raise as DatabaseConnectionError so it can be caught by exception handlers
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
                    backoff_seconds=backoff_time
                )
                await asyncio.sleep(backoff_time)
                retry_count += 1
                continue
            else:
                raise DatabaseConnectionError(f"Database is not available: {str(e)}") from e


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
