"""
Memgraph database connection and session management.

This module provides a connection pool and session management for Memgraph
using the Neo4j Python driver (which is compatible with Memgraph).
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


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
            self._driver = AsyncGraphDatabase.driver(
                self._settings.memgraph_uri,
                auth=auth,
                max_connection_lifetime=self._settings.memgraph_max_connection_lifetime,
                max_connection_pool_size=self._settings.memgraph_max_connection_pool_size,
            )
            # Test the connection
            await self.verify_connectivity()
            
            logger.info(
                "Connected to Memgraph",
                uri=self._settings.memgraph_uri,
                database=self._settings.memgraph_database
            )
            
        except (ServiceUnavailable, AuthError) as e:
            logger.error("Failed to connect to Memgraph", error=str(e))
            raise e
    
    async def disconnect(self) -> None:
        """Close the connection to Memgraph."""
        if self._driver:
            await self._driver.close()
            self._driver = None
            logger.info("Disconnected from Memgraph")
    
    async def verify_connectivity(self) -> None:
        """Verify connection to Memgraph."""
        if not self._driver:
            raise RuntimeError("Driver not initialized")
        
        try:
            await self._driver.verify_connectivity()
        except Exception as e:
            logger.error("Memgraph connectivity check failed", error=str(e))
            raise e
    
    async def get_session(self) -> AsyncSession:
        """Get a database session."""
        if not self._driver:
            raise RuntimeError("Driver not initialized")
        
        return self._driver.session(
            database=self._settings.memgraph_database
        )
    
    async def execute_query(
        self, 
        query: str, 
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        async with self.get_session() as session:
            try:
                result = await session.run(query, parameters or {})
                records = []
                async for record in result:
                    records.append(dict(record))
                return records
            except Exception as e:
                logger.error(
                    "Query execution failed",
                    query=query,
                    parameters=parameters,
                    error=str(e)
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
        async with self.get_session() as session:
            try:
                result = await session.run(query, parameters or {})
                records = []
                async for record in result:
                    records.append(dict(record))
                await session.commit()
                return records
            except Exception as e:
                logger.error(
                    "Write query execution failed",
                    query=query,
                    parameters=parameters,
                    error=str(e)
                )
                raise e
    
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
        await _connection.connect()
    
    return _connection


async def close_connection() -> None:
    """Close the global Memgraph connection."""
    global _connection
    
    if _connection:
        await _connection.disconnect()
        _connection = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session (async generator for dependency injection)."""
    connection = await get_connection()
    session = await connection.get_session()
    try:
        yield session
    finally:
        await session.close()


@asynccontextmanager
async def memgraph_lifespan(settings: Settings):
    """
    Context manager for Memgraph lifespan.
    
    Args:
        settings: Application settings
    """
    # Startup - initialize the connection
    await get_connection()
    
    try:
        yield
    finally:
        # Shutdown - close the connection
        await close_connection()
