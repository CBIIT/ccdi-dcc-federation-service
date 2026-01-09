"""
Cache service for the CCDI Federation Service.

This module provides caching functionality using Redis
for count and summary operations.
"""

import json
from typing import Any, Optional, Dict
try:
    # Optional dependency: environments without Redis deployed may not install redis client.
    from redis.asyncio import Redis  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    Redis = None  # type: ignore[assignment]
from contextlib import asynccontextmanager

from app.core.config import Settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class CacheService:
    """Service for caching operations using Redis."""
    
    def __init__(self, redis_client: Redis):
        """Initialize cache service with Redis client."""
        self.redis = redis_client
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached value by key.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value as dictionary or None if not found
        """
        try:
            cached_value = await self.redis.get(key)
            if cached_value:
                logger.debug("Cache hit", key=key)
                return json.loads(cached_value)
            else:
                logger.debug("Cache miss", key=key)
                return None
        except Exception as e:
            logger.warning("Cache get error", key=key, error=str(e))
            return None
    
    async def set(
        self,
        key: str,
        value: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set cached value with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            serialized_value = json.dumps(value, default=str)
            if ttl:
                result = await self.redis.setex(key, ttl, serialized_value)
            else:
                result = await self.redis.set(key, serialized_value)
            
            logger.debug("Cache set", key=key, ttl=ttl, success=bool(result))
            return bool(result)
        except Exception as e:
            logger.warning("Cache set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete cached value by key.
        
        Args:
            key: Cache key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            result = await self.redis.delete(key)
            logger.debug("Cache delete", key=key, success=bool(result))
            return bool(result)
        except Exception as e:
            logger.warning("Cache delete error", key=key, error=str(e))
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching a pattern.
        
        Args:
            pattern: Key pattern (supports wildcards)
            
        Returns:
            Number of keys deleted
        """
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                count = await self.redis.delete(*keys)
                logger.info("Cache clear pattern", pattern=pattern, count=count)
                return count
            else:
                logger.debug("Cache clear pattern - no keys found", pattern=pattern)
                return 0
        except Exception as e:
            logger.warning("Cache clear pattern error", pattern=pattern, error=str(e))
            return 0
    
    async def ping(self) -> bool:
        """
        Check if Redis connection is healthy.
        
        Returns:
            True if Redis is responsive, False otherwise
        """
        try:
            result = await self.redis.ping()
            return result
        except Exception as e:
            logger.warning("Cache ping error", error=str(e))
            return False


# ============================================================================
# Redis Connection Management
# ============================================================================

_redis_client: Optional[Redis] = None
_cache_service: Optional[CacheService] = None


async def init_redis(settings: Settings) -> Optional[Redis]:
    """
    Initialize Redis client.
    
    Args:
        settings: Application settings
        
    Returns:
        Redis client instance
    """
    global _redis_client
    
    if not settings.cache.enabled:
        logger.info("Cache disabled, skipping Redis initialization")
        return None

    if Redis is None:
        logger.warning("Redis client library not installed; caching disabled")
        return None
    
    try:
        _redis_client = Redis(
            host=settings.cache.redis_host,
            port=settings.cache.redis_port,
            db=settings.cache.redis_db,
            password=settings.cache.redis_password,
            decode_responses=False,  # We handle JSON encoding/decoding ourselves
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        # Test connection
        await _redis_client.ping()
        logger.info("Redis connection initialized", 
                   host=settings.cache.redis_host, 
                   port=settings.cache.redis_port)
        
        return _redis_client
    except Exception as e:
        logger.error("Failed to initialize Redis", error=str(e))
        _redis_client = None
        return None


async def close_redis() -> None:
    """Close Redis connection."""
    global _redis_client
    
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis connection closed")


def get_cache_service() -> Optional[CacheService]:
    """
    Get cache service instance.
    
    Returns:
        CacheService instance or None if caching is disabled
    """
    global _cache_service
    
    if not _redis_client:
        return None
    
    if not _cache_service:
        _cache_service = CacheService(_redis_client)
    
    return _cache_service


@asynccontextmanager
async def redis_lifespan(settings: Settings):
    """
    Context manager for Redis lifespan.
    
    Args:
        settings: Application settings
    """
    # Startup
    await init_redis(settings)
    
    try:
        yield
    finally:
        # Shutdown
        await close_redis()
