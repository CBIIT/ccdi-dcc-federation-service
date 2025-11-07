"""
Core configuration settings for the CCDI Federation Service.

This module uses pydantic-settings for configuration management, supporting
environment variables and .env files.
"""

from functools import lru_cache
from typing import Optional, List, Dict

from pydantic import Field, BaseModel
from pydantic_settings import BaseSettings


class AppSettings(BaseModel):
    """Application-specific settings."""
    name: str = "CCDI Federation Service"
    version: str = "v1.2.0" 
    debug: bool = False


class DatabaseSettings(BaseModel):
    """Database connection settings."""
    # NOTE: Defaults intentionally point to localhost with no password.
    # Real values should be injected via environment variables consumed by `Settings`.
    uri: str = "bolt://localhost:7687"
    user: str = "memgraph"
    password: str = ""
    database: str = "memgraph"
    max_connection_lifetime: int = 3600
    max_connection_pool_size: int = 50


class CacheSettings(BaseModel):
    """Cache and Redis settings."""
    enabled: bool = True
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""
    count_ttl: int = 300
    summary_ttl: int = 600
    ttl_count_endpoints: int = 1800
    ttl_summary_endpoints: int = 900
    ttl_list_endpoints: int = 300


class CORSSettings(BaseModel):
    """CORS configuration settings."""
    enabled: bool = True
    allowed_origins: List[str] = ["*"]
    allow_credentials: bool = True
    allowed_methods: List[str] = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    allowed_headers: List[str] = ["*"]
    origins: List[str] = ["*"]
    credentials: bool = True
    methods: List[str] = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    headers: List[str] = ["*"]


class PaginationSettings(BaseModel):
    """Pagination settings."""
    default_per_page: int = 20
    max_per_page: int = 100
    default_page_size: int = 100
    max_page_size: int = 1000


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    app_name: str = Field(default="CCDI Federation Service", alias="APP_NAME")
    app_version: str = Field(default="v1.2.0", alias="APP_VERSION")
    debug: bool = Field(default=False, alias="DEBUG")
    
    # Server
    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8000, alias="PORT")
    
    # Database (Memgraph)
    memgraph_uri: str = Field(
        # Default kept generic; override with MEMGRAPH_URI in env (.env, container secrets, etc.)
        default="bolt://localhost:7687", 
        alias="MEMGRAPH_URI"
    )
    memgraph_user: str = Field(default="memgraph", alias="MEMGRAPH_USER")
    memgraph_password: str = Field(default="", alias="MEMGRAPH_PASSWORD")
    memgraph_database: str = Field(default="memgraph", alias="MEMGRAPH_DATABASE")
    memgraph_max_connection_lifetime: int = Field(
        default=3600, 
        alias="MEMGRAPH_MAX_CONNECTION_LIFETIME"
    )
    memgraph_max_connection_pool_size: int = Field(
        default=50, 
        alias="MEMGRAPH_MAX_CONNECTION_POOL_SIZE"
    )
    
    # Redis (optional for caching)
    redis_url: Optional[str] = Field(
        default=None, 
        alias="REDIS_URL"
    )
    
    # Pagination defaults
    default_page_size: int = Field(default=100, alias="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=1000, alias="MAX_PAGE_SIZE")
    
    # Cache settings
    cache_enabled: bool = Field(default=True, alias="CACHE_ENABLED")
    cache_ttl_count_endpoints: int = Field(
        default=1800,  # 30 minutes
        alias="CACHE_TTL_COUNT_ENDPOINTS"
    )
    cache_ttl_summary_endpoints: int = Field(
        default=900,   # 15 minutes
        alias="CACHE_TTL_SUMMARY_ENDPOINTS"
    )
    cache_ttl_list_endpoints: int = Field(
        default=300,   # 5 minutes
        alias="CACHE_TTL_LIST_ENDPOINTS"
    )
    
    # Security
    cors_origins: list[str] = Field(
        default=["*"], 
        alias="CORS_ORIGINS"
    )
    cors_credentials: bool = Field(default=True, alias="CORS_CREDENTIALS")
    cors_methods: list[str] = Field(
        default=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        alias="CORS_METHODS"
    )
    cors_headers: list[str] = Field(
        default=["*"],
        alias="CORS_HEADERS"
    )
    
    # Rate limiting
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")
    rate_limit_requests_per_minute: int = Field(
        default=60, 
        alias="RATE_LIMIT_REQUESTS_PER_MINUTE"
    )
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")  # json or text
    
    # Data sharing policies
    share_line_level_data: bool = Field(
        default=True, 
        alias="SHARE_LINE_LEVEL_DATA"
    )
    
    # Namespace prefix
    namespace_prefix: str = Field(
        default="dbGaP_",
        alias="NAMESPACE_PREFIX"
    )
    
    # Organization and server info
    organization_name: str = Field(
        default="Example Organization",
        alias="ORGANIZATION_NAME"
    )
    server_name: str = Field(
        default="Example CCDI Federation Node",
        alias="SERVER_NAME"
    )
    contact_email: str = Field(
        default="support@example.com",
        alias="CONTACT_EMAIL"
    )
    
    # Database settings
    database_url: str = Field(default="bolt://localhost:7687", description="Database connection URL")

    # Cache/Redis settings
    cache_redis_host: Optional[str] = Field(default="localhost", description="Redis host")
    cache_redis_port: Optional[int] = Field(default=6379, description="Redis port")
    cache_redis_db: Optional[int] = Field(default=0, description="Redis database")
    cache_redis_password: Optional[str] = Field(default="", description="Redis password")
    cache_count_ttl: Optional[int] = Field(default=300, description="Cache TTL for count queries")
    cache_summary_ttl: Optional[int] = Field(default=600, description="Cache TTL for summary queries")
    
    # CORS settings
    cors_enabled: Optional[bool] = Field(default=True, description="Enable CORS")
    cors_allowed_origins: Optional[List[str]] = Field(default=["*"], description="CORS allowed origins")
    cors_allow_credentials: Optional[bool] = Field(default=True, description="CORS allow credentials")
    cors_allowed_methods: Optional[List[str]] = Field(default=["*"], description="CORS allowed methods")
    cors_allowed_headers: Optional[List[str]] = Field(default=["*"], description="CORS allowed headers")
    
    # Pagination settings
    pagination_default_per_page: Optional[int] = Field(default=20, description="Default items per page")
    pagination_max_per_page: Optional[int] = Field(default=100, description="Maximum items per page")
    
    # Valid fields for subject count operations (case-sensitive)
    subject_count_fields: List[str] = Field(
        default=["sex", "race", "ethnicity", "vital_status", "age_at_vital_status","associated_diagnoses"],
        alias="SUBJECT_COUNT_FIELDS",
        description="Valid field names for /subject/by/{field}/count endpoint"
    )
    
    # Sex value mappings: database values -> normalized output values
    sex_value_mappings: Dict[str, str] = Field(
        default={
            "Male": "M",
            "Female": "F",
            "Not Reported": "U"
        },
        alias="SEX_VALUE_MAPPINGS",
        description="Mapping of database sex values to normalized output values"
    )
    
    model_config = {
        "extra": "allow",  # Allow extra fields that aren't defined
        "env_file": ".env",
        "case_sensitive": False
    }
    
    # Nested settings properties
    @property
    def app(self) -> AppSettings:
        """Get application settings."""
        return AppSettings(
            name=self.app_name,
            version=self.app_version,
            debug=self.debug
        )
    
    @property
    def database(self) -> DatabaseSettings:
        """Get database settings."""
        return DatabaseSettings(
            uri=self.memgraph_uri,
            user=self.memgraph_user,
            password=self.memgraph_password,
            database=self.memgraph_database,
            max_connection_lifetime=self.memgraph_max_connection_lifetime,
            max_connection_pool_size=self.memgraph_max_connection_pool_size
        )
    
    @property
    def cache(self) -> CacheSettings:
        """Get cache settings."""
        return CacheSettings(
            enabled=self.cache_enabled,
            redis_host=self.cache_redis_host or "localhost",
            redis_port=self.cache_redis_port or 6379,
            redis_db=self.cache_redis_db or 0,
            redis_password=self.cache_redis_password or "",
            count_ttl=self.cache_count_ttl or 300,
            summary_ttl=self.cache_summary_ttl or 600,
            ttl_count_endpoints=self.cache_ttl_count_endpoints,
            ttl_summary_endpoints=self.cache_ttl_summary_endpoints,
            ttl_list_endpoints=self.cache_ttl_list_endpoints
        )
    
    @property
    def cors(self) -> CORSSettings:
        """Get CORS settings."""
        return CORSSettings(
            enabled=self.cors_enabled or True,
            allowed_origins=self.cors_allowed_origins or ["*"],
            allow_credentials=self.cors_allow_credentials or True,
            allowed_methods=self.cors_allowed_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allowed_headers=self.cors_allowed_headers or ["*"],
            origins=self.cors_origins,
            credentials=self.cors_credentials,
            methods=self.cors_methods,
            headers=self.cors_headers
        )
    
    @property
    def pagination(self) -> PaginationSettings:
        """Get pagination settings."""
        return PaginationSettings(
            default_per_page=self.pagination_default_per_page or 20,
            max_per_page=self.pagination_max_per_page or 100,
            default_page_size=self.default_page_size,
            max_page_size=self.max_page_size
        )


# Create settings instance
settings = Settings()


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()
