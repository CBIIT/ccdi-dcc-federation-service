"""
Comprehensive error response testing for all API endpoints.

Tests:
1. Invalid routes return 404
2. Invalid query parameters return 400
3. Invalid path parameters return 404
4. Unsupported fields return 400
5. Error response format validation
6. No 500 errors verification
7. No internal error messages exposed
"""

import json
import pytest
from fastapi.testclient import TestClient


def validate_error_response(data: dict, expected_status: int):
    """Validate error response format and content."""
    issues = []
    
    # Check for errors array
    if "errors" not in data:
        issues.append("Missing 'errors' field in response")
        return issues
    
    if not isinstance(data["errors"], list):
        issues.append("'errors' field is not an array")
        return issues
    
    if len(data["errors"]) == 0:
        issues.append("'errors' array is empty")
        return issues
    
    # Validate each error object
    for i, error in enumerate(data["errors"]):
        if not isinstance(error, dict):
            issues.append(f"Error {i} is not an object")
            continue
        
        # Check required fields
        if "kind" not in error:
            issues.append(f"Error {i} missing 'kind' field")
        
        # Check for internal error messages (should not expose str(exc) or stack traces)
        error_str = str(error).lower()
        forbidden_patterns = [
            "traceback",
            "file \"",
            "line ",
            "exception:",
            "error:",
            "typeerror",
            "valueerror",
            "attributeerror",
            "keyerror",
            "indexerror",
            "connectionerror",
            "timeout",
            "database",
            "neo4j",
            "cypher",
            "query",
            "sql",
        ]
        
        for pattern in forbidden_patterns:
            if pattern in error_str:
                issues.append(f"Error {i} exposes internal details: contains '{pattern}'")
        
        # Check error kind matches expected status
        kind = error.get("kind", "")
        if expected_status == 400:
            if kind not in ["InvalidParameters", "UnsupportedField"]:
                issues.append(f"Error {i} has kind '{kind}' but expected InvalidParameters or UnsupportedField for 400")
        elif expected_status == 404:
            if kind not in ["NotFound", "InvalidRoute"]:
                issues.append(f"Error {i} has kind '{kind}' but expected NotFound or InvalidRoute for 404")
    
    return issues


class TestInvalidRoutes:
    """Test invalid routes return 404."""
    
    def test_nonexistent_endpoint(self, client: TestClient):
        """Test non-existent endpoint returns 404."""
        r = client.get("/api/v1/nonexistent")
        assert r.status_code == 404
        body = r.json()
        issues = validate_error_response(body, 404)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_typo_in_endpoint(self, client: TestClient):
        """Test typo in endpoint returns 404."""
        r = client.get("/api/v1/subjec")  # typo: subject -> subjec
        assert r.status_code == 404
        body = r.json()
        issues = validate_error_response(body, 404)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_invalid_route_structure(self, client: TestClient):
        """Test invalid route structure returns 404."""
        r = client.get("/api/v1/subject/by/invalid_field/count")
        # Should be 400 (UnsupportedField) or 404, but not 500
        assert r.status_code != 500, "Should not return 500 error"
        assert r.status_code in (400, 404), f"Expected 400 or 404, got {r.status_code}"


class TestInvalidQueryParameters:
    """Test invalid query parameters return 400."""
    
    def test_unknown_query_param_subject(self, client: TestClient):
        """Test unknown query parameter on subject endpoint."""
        r = client.get("/api/v1/subject?unknown_param=value")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_unknown_query_param_sample(self, client: TestClient):
        """Test unknown query parameter on sample endpoint."""
        r = client.get("/api/v1/sample?invalid_filter=test")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_unknown_query_param_file(self, client: TestClient):
        """Test unknown query parameter on file endpoint."""
        r = client.get("/api/v1/file?bad_param=123")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_reserved_search_param_on_sample(self, client: TestClient):
        """Test reserved 'search' parameter on /sample endpoint."""
        r = client.get("/api/v1/sample?search=test")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"


class TestUnsupportedFields:
    """Test unsupported fields return 400."""
    
    def test_unsupported_field_subject_count(self, client: TestClient):
        """Test unsupported field in subject count endpoint."""
        r = client.get("/api/v1/subject/by/handedness/count")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
        assert body["errors"][0]["kind"] == "UnsupportedField"
    
    def test_unsupported_field_sample_count(self, client: TestClient):
        """Test unsupported field in sample count endpoint."""
        r = client.get("/api/v1/sample/by/invalid_field/count")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_unsupported_field_file_count(self, client: TestClient):
        """Test unsupported field in file count endpoint."""
        r = client.get("/api/v1/file/by/invalid_field/count")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"


class TestMalformedRequests:
    """Test malformed requests return appropriate errors."""
    
    def test_negative_page(self, client: TestClient):
        """Test negative page number."""
        r = client.get("/api/v1/subject?page=-1")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_zero_per_page(self, client: TestClient):
        """Test zero per_page."""
        r = client.get("/api/v1/subject?per_page=0")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
    
    def test_too_large_per_page(self, client: TestClient):
        """Test per_page exceeds maximum."""
        r = client.get("/api/v1/subject?per_page=10000")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"


class TestNo500Errors:
    """Test that no 500 errors are returned."""
    
    @pytest.mark.parametrize("endpoint", [
        "/api/v1/subject",
        "/api/v1/sample",
        "/api/v1/file",
        "/api/v1/subject/summary",
        "/api/v1/sample/summary",
        "/api/v1/file/summary",
        "/api/v1/subject/by/sex/count",
        "/api/v1/sample/by/tissue_type/count",
        "/api/v1/file/by/type/count",
    ])
    def test_no_500_error(self, client: TestClient, endpoint: str):
        """Test that endpoint does not return 500 error."""
        r = client.get(endpoint)
        assert r.status_code != 500, f"Endpoint {endpoint} returned 500 error (no 500 errors allowed!)"
        # If it's an error, validate the format
        if r.status_code >= 400:
            body = r.json()
            issues = validate_error_response(body, r.status_code)
            assert not issues, f"Error response validation failed: {issues}"


class TestErrorResponseFormat:
    """Test error response format compliance."""
    
    def test_error_response_has_errors_array(self, client: TestClient):
        """Test that error responses have 'errors' array."""
        r = client.get("/api/v1/subject?unknown_param=value")
        assert r.status_code == 400
        body = r.json()
        assert "errors" in body
        assert isinstance(body["errors"], list)
        assert len(body["errors"]) > 0
    
    def test_error_response_no_internal_details(self, client: TestClient):
        """Test that error responses don't expose internal details."""
        r = client.get("/api/v1/subject?unknown_param=value")
        assert r.status_code == 400
        body = r.json()
        error_str = json.dumps(body).lower()
        
        forbidden_patterns = [
            "traceback",
            "file \"",
            "line ",
            "exception:",
            "database",
            "neo4j",
            "cypher",
        ]
        
        for pattern in forbidden_patterns:
            assert pattern not in error_str, f"Error response exposes internal details: contains '{pattern}'"
    
    def test_error_response_sanitized_messages(self, client: TestClient):
        """Test that error messages are sanitized."""
        r = client.get("/api/v1/subject?unknown_param=value")
        assert r.status_code == 400
        body = r.json()
        
        for error in body["errors"]:
            message = error.get("message", "").lower()
            # Should use generic messages, not expose specific parameter values
            assert "invalid" in message or "parameter" in message or "query" in message
            # Should not expose the actual parameter name in a way that reveals internals
            if "parameters" in error:
                # Parameters array should be empty or sanitized
                params = error.get("parameters", [])
                # Should not expose internal parameter names
                assert all(not param.startswith("_") for param in params), "Exposes internal parameter names"

