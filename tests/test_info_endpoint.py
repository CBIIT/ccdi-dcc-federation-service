"""
Tests for the Info API endpoint.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import patch, mock_open
from fastapi.testclient import TestClient


# @pytest.mark.unit
# def test_get_info_success(client: TestClient, sample_api_info):
#     """Test successful retrieval of API info."""
#     mock_data = json.dumps(sample_api_info)
    
#     with patch("builtins.open", mock_open(read_data=mock_data)):
#         response = client.get("/info")
        
#         assert response.status_code == 200
#         data = response.json()
        
#         # Verify structure
#         assert "server" in data
#         assert "api" in data
#         assert "data" in data
        
#         # Verify API field only contains expected keys
#         assert "api_version" in data["api"]
#         assert "documentation_url" in data["api"]


# @pytest.mark.unit
# def test_get_info_file_not_found(client: TestClient):
#     """Test info endpoint when config file is missing."""
#     with patch("builtins.open", side_effect=FileNotFoundError):
#         response = client.get("/info")
        
#         assert response.status_code == 404
#         data = response.json()
#         assert "errors" in data
#         assert len(data["errors"]) > 0
#         assert data["errors"][0]["kind"] == "NotFound"


@pytest.mark.unit
def test_get_info_invalid_json(client: TestClient):
    """Test info endpoint with invalid JSON data."""
    with patch("builtins.open", mock_open(read_data="invalid json {")):
        response = client.get("/info")
        
        assert response.status_code == 404
        data = response.json()
        assert "errors" in data


# @pytest.mark.unit
# def test_info_response_structure(client: TestClient, sample_api_info):
#     """Test that info response has correct structure."""
#     mock_data = json.dumps(sample_api_info)
    
#     with patch("builtins.open", mock_open(read_data=mock_data)):
#         response = client.get("/info")
        
#         assert response.status_code == 200
#         data = response.json()
        
#         # Check server info
#         assert "name" in data["server"]
#         assert "version" in data["server"]
        
#         # Check API info (filtered)
#         assert set(data["api"].keys()) == {"api_version", "documentation_url"}
        
#         # Check data info
#         assert "version" in data["data"]
#         assert "last_updated" in data["data"]
