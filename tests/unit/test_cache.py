"""
Unit tests for cache service.

Tests Redis caching operations and connection management.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
import json

from app.core.cache import (
    CacheService,
    init_redis,
    close_redis,
    get_cache_service,
    redis_lifespan,
    _redis_client,
    _cache_service
)


@pytest.mark.unit
class TestCacheService:
    """Test cases for CacheService class."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        return redis

    @pytest.fixture
    def cache_service(self, mock_redis):
        """Create a CacheService instance."""
        return CacheService(mock_redis)

    async def test_get_cache_hit(self, cache_service, mock_redis):
        """Test getting cached value (cache hit)."""
        cached_data = {"key": "value"}
        mock_redis.get.return_value = json.dumps(cached_data).encode()
        
        result = await cache_service.get("test_key")
        
        assert result == cached_data
        mock_redis.get.assert_called_once_with("test_key")

    async def test_get_cache_miss(self, cache_service, mock_redis):
        """Test getting cached value (cache miss)."""
        mock_redis.get.return_value = None
        
        result = await cache_service.get("test_key")
        
        assert result is None
        mock_redis.get.assert_called_once_with("test_key")

    async def test_get_cache_error(self, cache_service, mock_redis):
        """Test getting cached value with error."""
        mock_redis.get.side_effect = Exception("Redis error")
        
        result = await cache_service.get("test_key")
        
        assert result is None

    async def test_set_with_ttl(self, cache_service, mock_redis):
        """Test setting cached value with TTL."""
        value = {"key": "value"}
        mock_redis.setex.return_value = True
        
        result = await cache_service.set("test_key", value, ttl=60)
        
        assert result is True
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        assert call_args[0][0] == "test_key"
        assert call_args[0][1] == 60

    async def test_set_without_ttl(self, cache_service, mock_redis):
        """Test setting cached value without TTL."""
        value = {"key": "value"}
        mock_redis.set.return_value = True
        
        result = await cache_service.set("test_key", value)
        
        assert result is True
        mock_redis.set.assert_called_once()

    async def test_set_error(self, cache_service, mock_redis):
        """Test setting cached value with error."""
        mock_redis.set.side_effect = Exception("Redis error")
        
        result = await cache_service.set("test_key", {"key": "value"})
        
        assert result is False

    async def test_delete_success(self, cache_service, mock_redis):
        """Test deleting cached value successfully."""
        mock_redis.delete.return_value = 1
        
        result = await cache_service.delete("test_key")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("test_key")

    async def test_delete_not_found(self, cache_service, mock_redis):
        """Test deleting non-existent cached value."""
        mock_redis.delete.return_value = 0
        
        result = await cache_service.delete("test_key")
        
        assert result is False

    async def test_delete_error(self, cache_service, mock_redis):
        """Test deleting cached value with error."""
        mock_redis.delete.side_effect = Exception("Redis error")
        
        result = await cache_service.delete("test_key")
        
        assert result is False

    async def test_clear_pattern_with_keys(self, cache_service, mock_redis):
        """Test clearing cache pattern with matching keys."""
        mock_redis.keys.return_value = [b"key1", b"key2", b"key3"]
        mock_redis.delete.return_value = 3
        
        result = await cache_service.clear_pattern("test:*")
        
        assert result == 3
        mock_redis.keys.assert_called_once_with("test:*")
        mock_redis.delete.assert_called_once_with(b"key1", b"key2", b"key3")

    async def test_clear_pattern_no_keys(self, cache_service, mock_redis):
        """Test clearing cache pattern with no matching keys."""
        mock_redis.keys.return_value = []
        
        result = await cache_service.clear_pattern("test:*")
        
        assert result == 0
        mock_redis.keys.assert_called_once_with("test:*")
        mock_redis.delete.assert_not_called()

    async def test_clear_pattern_error(self, cache_service, mock_redis):
        """Test clearing cache pattern with error."""
        mock_redis.keys.side_effect = Exception("Redis error")
        
        result = await cache_service.clear_pattern("test:*")
        
        assert result == 0

    async def test_ping_success(self, cache_service, mock_redis):
        """Test ping with successful response."""
        mock_redis.ping.return_value = True
        
        result = await cache_service.ping()
        
        assert result is True
        mock_redis.ping.assert_called_once()

    async def test_ping_error(self, cache_service, mock_redis):
        """Test ping with error."""
        mock_redis.ping.side_effect = Exception("Redis error")
        
        result = await cache_service.ping()
        
        assert result is False


@pytest.mark.unit
class TestRedisConnection:
    """Test cases for Redis connection management."""

    @pytest.mark.skip(reason="Redis is disabled for this app")
    @patch('app.core.cache.Redis')
    @patch('app.core.cache.get_settings')
    async def test_init_redis_success(self, mock_get_settings, mock_redis_class):
        """Test successful Redis initialization (skipped - Redis disabled)."""
        pass

    async def test_init_redis_disabled(self):
        """Test Redis initialization when cache is disabled."""
        mock_settings = Mock()
        mock_settings.cache.enabled = False
        
        result = await init_redis(mock_settings)
        
        assert result is None

    @pytest.mark.skip(reason="Redis is disabled for this app")
    @patch('app.core.cache.Redis')
    @patch('app.core.cache.get_settings')
    async def test_init_redis_not_installed(self, mock_get_settings, mock_redis_class):
        """Test Redis initialization when Redis library is not installed (skipped - Redis disabled)."""
        pass

    @pytest.mark.skip(reason="Redis is disabled for this app")
    @patch('app.core.cache.get_settings')
    async def test_init_redis_connection_error(self, mock_get_settings):
        """Test Redis initialization with connection error (skipped - Redis disabled)."""
        pass

    async def test_close_redis(self):
        """Test closing Redis connection."""
        import app.core.cache as cache_module
        
        mock_redis = AsyncMock()
        cache_module._redis_client = mock_redis
        
        await close_redis()
        
        mock_redis.close.assert_called_once()
        assert cache_module._redis_client is None

    async def test_close_redis_none(self):
        """Test closing Redis when client is None."""
        import app.core.cache as cache_module
        
        cache_module._redis_client = None
        
        # Should not raise
        await close_redis()
        
        assert cache_module._redis_client is None

    def test_get_cache_service_with_client(self):
        """Test getting cache service when Redis client exists."""
        import app.core.cache as cache_module
        
        mock_redis = AsyncMock()
        cache_module._redis_client = mock_redis
        cache_module._cache_service = None
        
        service = get_cache_service()
        
        assert service is not None
        assert isinstance(service, CacheService)

    def test_get_cache_service_without_client(self):
        """Test getting cache service when Redis client is None."""
        import app.core.cache as cache_module
        
        cache_module._redis_client = None
        cache_module._cache_service = None
        
        service = get_cache_service()
        
        assert service is None

    def test_get_cache_service_cached(self):
        """Test getting cache service returns cached instance."""
        import app.core.cache as cache_module
        
        mock_redis = AsyncMock()
        cache_module._redis_client = mock_redis
        existing_service = CacheService(mock_redis)
        cache_module._cache_service = existing_service
        
        service = get_cache_service()
        
        assert service is existing_service

    @patch('app.core.cache.init_redis')
    @patch('app.core.cache.close_redis')
    async def test_redis_lifespan(self, mock_close, mock_init):
        """Test Redis lifespan context manager."""
        mock_settings = Mock()
        mock_init.return_value = AsyncMock()
        mock_close.return_value = None
        
        async with redis_lifespan(mock_settings):
            pass
        
        mock_init.assert_called_once_with(mock_settings)
        mock_close.assert_called_once()

