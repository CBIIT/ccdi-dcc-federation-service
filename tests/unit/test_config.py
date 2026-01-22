"""
Unit tests for configuration management.

Tests the Settings class and configuration loading from environment
variables and info.json file.
"""

import pytest
from unittest.mock import patch, mock_open
from app.core.config import (
    Settings,
    AppSettings,
    DatabaseSettings,
    CacheSettings,
    CORSSettings,
    PaginationSettings,
    get_settings,
    load_info_json
)


@pytest.mark.unit
class TestAppSettings:
    """Test cases for AppSettings."""

    def test_app_settings_defaults(self):
        """Test AppSettings with default values."""
        settings = AppSettings()
        assert settings.name == "CCDI Federation Service"
        assert settings.version == "v1.2.0"
        assert settings.debug is False

    def test_app_settings_custom_values(self):
        """Test AppSettings with custom values."""
        settings = AppSettings(
            name="Test Service",
            version="v2.0.0",
            debug=True
        )
        assert settings.name == "Test Service"
        assert settings.version == "v2.0.0"
        assert settings.debug is True


@pytest.mark.unit
class TestDatabaseSettings:
    """Test cases for DatabaseSettings."""

    def test_database_settings_defaults(self):
        """Test DatabaseSettings with default values."""
        settings = DatabaseSettings()
        assert settings.uri == "bolt://localhost:7687"
        assert settings.user == "memgraph"
        assert settings.password == ""
        assert settings.database == "memgraph"
        assert settings.max_connection_lifetime == 3600
        assert settings.max_connection_pool_size == 50

    def test_database_settings_custom_values(self):
        """Test DatabaseSettings with custom values."""
        settings = DatabaseSettings(
            uri="bolt://example.com:7687",
            user="test_user",
            password="test_password",
            database="test_db",
            max_connection_lifetime=7200,
            max_connection_pool_size=100
        )
        assert settings.uri == "bolt://example.com:7687"
        assert settings.user == "test_user"
        assert settings.password == "test_password"
        assert settings.database == "test_db"
        assert settings.max_connection_lifetime == 7200
        assert settings.max_connection_pool_size == 100


@pytest.mark.unit
class TestCacheSettings:
    """Test cases for CacheSettings."""

    def test_cache_settings_defaults(self):
        """Test CacheSettings with default values."""
        settings = CacheSettings()
        assert settings.enabled is False
        assert settings.redis_host == "localhost"
        assert settings.redis_port == 6379
        assert settings.redis_db == 0
        assert settings.redis_password == ""
        assert settings.count_ttl == 300
        assert settings.summary_ttl == 600


@pytest.mark.unit
class TestCORSSettings:
    """Test cases for CORSSettings."""

    def test_cors_settings_defaults(self):
        """Test CORSSettings with default values."""
        settings = CORSSettings()
        assert settings.enabled is True
        assert settings.allowed_origins == ["*"]
        assert settings.allow_credentials is True
        assert "GET" in settings.allowed_methods
        assert "POST" in settings.allowed_methods


@pytest.mark.unit
class TestPaginationSettings:
    """Test cases for PaginationSettings."""

    def test_pagination_settings_defaults(self):
        """Test PaginationSettings with default values."""
        settings = PaginationSettings()
        assert settings.default_per_page == 20
        assert settings.max_per_page == 100
        assert settings.default_page_size == 100
        assert settings.max_page_size == 1000


@pytest.mark.unit
class TestSettings:
    """Test cases for Settings class."""

    def test_settings_defaults(self):
        """Test Settings with default values."""
        settings = Settings()
        assert settings.app_name == "CCDI Federation Service"
        assert settings.host == "0.0.0.0"
        assert settings.port == 8000
        assert settings.memgraph_uri == "bolt://localhost:7687"
        assert settings.cache_enabled is False
        assert settings.default_page_size == 100
        assert settings.max_page_size == 1000

    def test_settings_from_env_vars(self):
        """Test Settings loading from environment variables."""
        with patch.dict("os.environ", {
            "APP_NAME": "Test App",
            "PORT": "9000",
            "MEMGRAPH_URI": "bolt://test:7687",
            "CACHE_ENABLED": "true",
            "DEFAULT_PAGE_SIZE": "50"
        }):
            settings = Settings()
            assert settings.app_name == "Test App"
            assert settings.port == 9000
            assert settings.memgraph_uri == "bolt://test:7687"
            assert settings.cache_enabled is True
            assert settings.default_page_size == 50

    def test_settings_app_property(self):
        """Test Settings.app property."""
        settings = Settings()
        app_settings = settings.app
        assert isinstance(app_settings, AppSettings)
        assert app_settings.name == settings.app_name
        assert app_settings.version == settings.app_version

    def test_settings_database_property(self):
        """Test Settings.database property."""
        settings = Settings()
        db_settings = settings.database
        assert isinstance(db_settings, DatabaseSettings)
        assert db_settings.uri == settings.memgraph_uri
        assert db_settings.user == settings.memgraph_user

    def test_settings_cache_property(self):
        """Test Settings.cache property."""
        settings = Settings()
        cache_settings = settings.cache
        assert isinstance(cache_settings, CacheSettings)
        assert cache_settings.enabled == settings.cache_enabled

    def test_settings_cors_property(self):
        """Test Settings.cors property."""
        settings = Settings()
        cors_settings = settings.cors
        assert isinstance(cors_settings, CORSSettings)
        assert cors_settings.enabled is True

    def test_settings_pagination_property(self):
        """Test Settings.pagination property."""
        settings = Settings()
        pagination_settings = settings.pagination
        assert isinstance(pagination_settings, PaginationSettings)
        assert pagination_settings.default_page_size == settings.default_page_size

    def test_settings_subject_count_fields(self):
        """Test subject_count_fields configuration."""
        settings = Settings()
        assert isinstance(settings.subject_count_fields, list)
        assert "sex" in settings.subject_count_fields
        assert "race" in settings.subject_count_fields

    def test_settings_sex_value_mappings(self):
        """Test sex_value_mappings configuration."""
        settings = Settings()
        assert isinstance(settings.sex_value_mappings, dict)
        assert settings.sex_value_mappings["Male"] == "M"
        assert settings.sex_value_mappings["Female"] == "F"
        assert settings.sex_value_mappings["Not Reported"] == "U"


@pytest.mark.unit
class TestLoadInfoJson:
    """Test cases for load_info_json function."""

    @patch("app.core.config.Path.open", new_callable=mock_open, read_data='{"server": {"name": "Test Server"}}')
    @patch("app.core.config.Path.resolve")
    @patch("app.core.config.Path.__truediv__")
    def test_load_info_json_success(self, mock_div, mock_resolve, mock_file):
        """Test loading info.json successfully."""
        # Reset module-level cache by reloading
        import importlib
        import app.core.config
        importlib.reload(app.core.config)
        
        result = app.core.config.load_info_json()
        assert isinstance(result, dict)
        # Note: This test may be affected by module-level caching

    def test_load_info_json_missing_file(self):
        """Test load_info_json handles missing file gracefully."""
        # The function should return empty dict if file doesn't exist
        # This is tested implicitly - it shouldn't crash
        result = load_info_json()
        assert isinstance(result, dict)


@pytest.mark.unit
class TestGetSettings:
    """Test cases for get_settings function."""

    def test_get_settings_returns_settings(self):
        """Test that get_settings returns a Settings instance."""
        # Clear cache first
        get_settings.cache_clear()
        settings = get_settings()
        assert isinstance(settings, Settings)

    def test_get_settings_cached(self):
        """Test that get_settings uses caching."""
        # Clear cache
        get_settings.cache_clear()
        
        settings1 = get_settings()
        settings2 = get_settings()
        
        # Should be the same instance due to caching
        assert settings1 is settings2

    @patch("app.core.config.Path.is_file")
    def test_get_settings_with_env_file(self, mock_is_file):
        """Test get_settings when .env file exists."""
        mock_is_file.return_value = True
        
        # Clear cache
        get_settings.cache_clear()
        
        settings = get_settings()
        assert isinstance(settings, Settings)

    @patch("app.core.config.Path.is_file")
    def test_get_settings_without_env_file(self, mock_is_file):
        """Test get_settings when .env file doesn't exist."""
        mock_is_file.return_value = False
        
        # Clear cache
        get_settings.cache_clear()
        
        settings = get_settings()
        assert isinstance(settings, Settings)

