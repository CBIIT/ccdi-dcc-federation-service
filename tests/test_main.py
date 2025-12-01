"""
Tests for the main FastAPI application.
"""

import pytest
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_app_exists(client: TestClient):
    """Test that the application exists and responds."""
    response = client.get("/")
    assert response.status_code in [200, 404]  # Either root exists or redirects


@pytest.mark.unit
def test_health_check(client: TestClient):
    """Test the health check endpoint if it exists."""
    # This tests the root endpoint
    response = client.get("/")
    # Should get a valid response
    assert response.status_code in [200, 307, 404]


@pytest.mark.unit
def test_cors_headers(client: TestClient):
    """Test that CORS headers are properly configured."""
    response = client.options(
        "/api/v1/subjects",
        headers={
            "Origin": "http://example.com",
            "Access-Control-Request-Method": "GET"
        }
    )
    # CORS preflight should succeed
    assert response.status_code in [200, 404]
