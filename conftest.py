"""
Pytest configuration and fixtures for the CCDI Federation Service tests.
"""

import pytest
from typing import AsyncGenerator, Generator
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock

from app.main import app
from app.core.config import Settings, get_settings
from app.api.v1.deps import get_database_session


# Test settings
@pytest.fixture
def test_settings() -> Settings:
    """Provide test settings."""
    return Settings(
        app_name="CCDI Federation Service Test",
        app_version="0.1.0",
        environment="test",
        log_level="DEBUG",
        memgraph_uri="bolt://localhost:7687",
        memgraph_username="test",
        memgraph_password="test",
        redis_url="redis://localhost:6379",
        cache_ttl=300,
        rate_limit_enabled=False,  # Disable rate limiting in tests
    )


# Override settings dependency
@pytest.fixture
def override_settings(test_settings: Settings):
    """Override the settings dependency."""
    app.dependency_overrides[get_settings] = lambda: test_settings
    yield
    app.dependency_overrides.clear()


# Mock database session
@pytest.fixture
def mock_db_session() -> AsyncMock:
    """Provide a mock database session."""
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.__aexit__.return_value = None
    return session


@pytest.fixture
def override_db_session(mock_db_session: AsyncMock):
    """Override the database session dependency."""
    app.dependency_overrides[get_database_session] = lambda: mock_db_session
    yield mock_db_session
    app.dependency_overrides.clear()


# Synchronous test client
@pytest.fixture
def client(override_settings) -> Generator[TestClient, None, None]:
    """Provide a synchronous test client."""
    with TestClient(app) as test_client:
        yield test_client


# Async test client
@pytest.fixture
async def async_client(override_settings) -> AsyncGenerator[AsyncClient, None]:
    """Provide an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# Sample data fixtures
@pytest.fixture
def sample_subject_data():
    """Provide sample subject data for testing."""
    return {
        "id": "subject-001",
        "sex": "Female",
        "race": "White",
        "ethnicity": "Not Hispanic or Latino",
        "vital_status": "Alive",
        "identifiers": [
            {"system": "PDC", "value": "PDC001"}
        ]
    }


@pytest.fixture
def sample_subjects_list(sample_subject_data):
    """Provide a list of sample subjects."""
    return [
        sample_subject_data,
        {
            "id": "subject-002",
            "sex": "Male",
            "race": "Asian",
            "ethnicity": "Not Hispanic or Latino",
            "vital_status": "Alive",
            "identifiers": [
                {"system": "GDC", "value": "GDC002"}
            ]
        },
        {
            "id": "subject-003",
            "sex": "Female",
            "race": "Black or African American",
            "ethnicity": "Hispanic or Latino",
            "vital_status": "Dead",
            "identifiers": [
                {"system": "PDC", "value": "PDC003"}
            ]
        }
    ]


@pytest.fixture
def sample_file_data():
    """Provide sample file data for testing."""
    return {
        "id": "file-001",
        "file_name": "sample_data.txt",
        "file_size": 1024,
        "file_type": "txt",
        "checksum": "abc123",
        "identifiers": [
            {"system": "PDC", "value": "FILE001"}
        ]
    }


@pytest.fixture
def sample_api_info():
    """Provide sample API info data."""
    return {
        "server": {
            "name": "CCDI Federation Service",
            "version": "0.1.0"
        },
        "api": {
            "api_version": "1.0.0",
            "documentation_url": "https://docs.example.com"
        },
        "data": {
            "version": {
                "version": "1.0.0",
                "about_url": "https://example.com/about"
            },
            "last_updated": "2025-12-01",
            "wiki_url": "https://wiki.example.com",
            "documentation_url": "https://docs.example.com"
        }
    }
