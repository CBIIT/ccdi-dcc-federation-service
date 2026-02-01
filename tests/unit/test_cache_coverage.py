"""
Additional unit tests for cache.py to improve coverage.

Tests missing error paths and edge cases.
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
)


@pytest.mark.unit
class TestCacheServiceCoverage:
    """Test cases for CacheService to improve coverage."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        return redis

    @pytest.fixture
    def cache_service(self, mock_redis):
        """Create a CacheService instance."""
        return CacheService(mock_redis)

    async def test_get_cache_hit_with_json_decode_error(self, cache_service, mock_redis):
        """Test get() when JSON decode fails."""
        mock_redis.get.return_value = b"invalid json"
        
        result = await cache_service.get("test_key")
        
        # Should return None on JSON decode error
        assert result is None

    async def test_set_with_ttl_error_during_json_dumps(self, cache_service, mock_redis):
        """Test set() when json.dumps fails."""
        # Create a value that can't be serialized
        class Unserializable:
            pass
        
        mock_redis.setex.side_effect = Exception("Redis error")
        
        result = await cache_service.set("test_key", {"key": Unserializable()}, ttl=60)
        
        # Should return False on error
        assert result is False

    async def test_set_without_ttl_error_during_json_dumps(self, cache_service, mock_redis):
        """Test set() without TTL when json.dumps fails."""
        class Unserializable:
            pass
        
        mock_redis.set.side_effect = Exception("Redis error")
        
        result = await cache_service.set("test_key", {"key": Unserializable()})
        
        # Should return False on error
        assert result is False

    async def test_set_with_ttl_returns_false(self, cache_service, mock_redis):
        """Test set() with TTL when Redis returns False."""
        mock_redis.setex.return_value = False
        
        result = await cache_service.set("test_key", {"key": "value"}, ttl=60)
        
        assert result is False

    async def test_set_without_ttl_returns_false(self, cache_service, mock_redis):
        """Test set() without TTL when Redis returns False."""
        mock_redis.set.return_value = False
        
        result = await cache_service.set("test_key", {"key": "value"})
        
        assert result is False

    async def test_clear_pattern_with_delete_error(self, cache_service, mock_redis):
        """Test clear_pattern() when delete fails."""
        mock_redis.keys.return_value = [b"key1", b"key2"]
        mock_redis.delete.side_effect = Exception("Delete error")
        
        result = await cache_service.clear_pattern("test:*")
        
        assert result == 0


@pytest.mark.unit
class TestRedisConnectionCoverage:
    """Test cases for Redis connection management to improve coverage."""

    @patch('app.core.cache.Redis')
    async def test_init_redis_redis_not_installed(self, mock_redis_class):
        """Test init_redis when Redis library is not installed."""
        import app.core.cache as cache_module
        
        # Temporarily set Redis to None
        original_redis = cache_module.Redis
        cache_module.Redis = None
        
        try:
            mock_settings = Mock()
            mock_settings.cache.enabled = True
            
            result = await init_redis(mock_settings)
            
            assert result is None
        finally:
            # Restore Redis
            cache_module.Redis = original_redis

    @patch('app.core.cache.Redis')
    async def test_init_redis_connection_error(self, mock_redis_class):
        """Test init_redis when connection fails."""
        mock_settings = Mock()
        mock_settings.cache.enabled = True
        mock_settings.cache.redis_host = "localhost"
        mock_settings.cache.redis_port = 6379
        mock_settings.cache.redis_db = 0
        mock_settings.cache.redis_password = None
        
        # Mock Redis client that fails on ping
        mock_redis_instance = AsyncMock()
        mock_redis_instance.ping.side_effect = Exception("Connection failed")
        mock_redis_class.return_value = mock_redis_instance
        
        result = await init_redis(mock_settings)
        
        # Should return None on connection error
        assert result is None

    @patch('app.core.cache.init_redis')
    @patch('app.core.cache.close_redis')
    async def test_redis_lifespan_with_exception(self, mock_close, mock_init):
        """Test redis_lifespan context manager when exception occurs."""
        mock_settings = Mock()
        mock_init.return_value = AsyncMock()
        
        # Test that close_redis is called even if exception occurs
        try:
            async with redis_lifespan(mock_settings):
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        mock_init.assert_called_once_with(mock_settings)
        mock_close.assert_called_once()
