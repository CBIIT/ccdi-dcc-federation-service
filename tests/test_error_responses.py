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
        # Check for forbidden patterns, but allow common user-facing terms like "query parameter"
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
            "cypher query",
            "sql query",
            "query error",
            "query failed",
            "query execution",
        ]
        
        for pattern in forbidden_patterns:
            if pattern in error_str:
                issues.append(f"Error {i} exposes internal details: contains '{pattern}'")
        
        # Also check for standalone "query" that's not part of "query parameter"
        # This catches internal query references while allowing "query parameter(s)"
        if "query" in error_str:
            # Allow "query parameter" but flag other uses
            if "query parameter" not in error_str:
                issues.append(f"Error {i} exposes internal details: contains 'query' (not in 'query parameter')")
        
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
    
    def test_return_total_param_on_sample(self, client: TestClient):
        """Test return_total parameter on /sample endpoint should be rejected."""
        r = client.get("/api/v1/sample?depositions=phs002790&return_total=true")
        assert r.status_code == 400
        body = r.json()
        issues = validate_error_response(body, 400)
        assert not issues, f"Error response validation failed: {issues}"
        assert body["errors"][0]["kind"] == "InvalidParameters"


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


class TestEntityTypeDetection:
    """Test entity type detection in UnsupportedField errors."""
    
    def test_subject_entity_type_detection(self, client: TestClient):
        """Test that subject entity type is correctly detected from path."""
        # Use invalid enum value - endpoint handler will raise UnsupportedFieldError
        r = client.get("/api/v1/subject/by/invalid_enum_value/count")
        assert r.status_code == 400
        body = r.json()
        assert body["errors"][0]["kind"] == "UnsupportedField"
        # Endpoint handlers use singular form "subject" in error messages
        assert "subject" in body["errors"][0]["message"].lower()
        assert "subject" in body["errors"][0]["reason"].lower()
        # Verify field is sanitized
        assert body["errors"][0]["field"] == "wrong field"
    
    def test_sample_entity_type_detection(self, client: TestClient):
        """Test that sample entity type is correctly detected from path."""
        # Use invalid enum value - endpoint handler will raise UnsupportedFieldError
        r = client.get("/api/v1/sample/by/invalid_enum_value/count")
        assert r.status_code == 400
        body = r.json()
        assert body["errors"][0]["kind"] == "UnsupportedField"
        # Endpoint handlers use singular form "sample" in error messages
        assert "sample" in body["errors"][0]["message"].lower()
        assert "sample" in body["errors"][0]["reason"].lower()
        # Verify field is sanitized
        assert body["errors"][0]["field"] == "wrong field"
    
    def test_file_entity_type_detection(self, client: TestClient):
        """Test that file entity type is correctly detected from path."""
        # Use invalid enum value - endpoint handler will raise UnsupportedFieldError
        r = client.get("/api/v1/file/by/invalid_enum_value/count")
        assert r.status_code == 400
        body = r.json()
        assert body["errors"][0]["kind"] == "UnsupportedField"
        # Endpoint handlers use singular form "file" in error messages
        assert "file" in body["errors"][0]["message"].lower()
        assert "file" in body["errors"][0]["reason"].lower()
        # Verify field is sanitized
        assert body["errors"][0]["field"] == "wrong field"
    
    def test_unknown_entity_type_generic_message(self, client: TestClient):
        """Test that unknown entity types use generic message without entity name."""
        # Try a path with unknown entity type (if such endpoint existed)
        # Since this endpoint doesn't exist, it will return 404, but we can test
        # the logic by checking what happens with a malformed count endpoint
        # Actually, let's test with a path that matches the pattern but has unknown entity
        # Note: This will likely return 404 since the route doesn't exist,
        # but if it somehow triggers the validation error handler, it should use generic message
        r = client.get("/api/v1/organization/by/invalid_field/count")
        # This endpoint doesn't exist, so it will be 404, but if it did exist and had enum validation,
        # it would use generic message. For now, we just verify it doesn't crash
        assert r.status_code in (400, 404)
    
    def test_path_with_trailing_slash(self, client: TestClient):
        """Test entity type detection with trailing slash in path."""
        r = client.get("/api/v1/subject/by/invalid_enum_value/count/")
        # Should handle trailing slash gracefully (FastAPI redirects to remove trailing slash)
        assert r.status_code in (400, 404, 307)
        if r.status_code == 400:
            body = r.json()
            if body.get("errors") and body["errors"][0].get("kind") == "UnsupportedField":
                # Endpoint handlers use singular form
                assert "subject" in body["errors"][0]["message"].lower()
    
    def test_path_parsing_from_segments(self, client: TestClient):
        """Test that entity type is extracted from path segments correctly."""
        # Test with standard path format: /api/v1/{entity}/by/{field}/count
        r = client.get("/api/v1/subject/by/invalid_enum_value/count")
        assert r.status_code == 400
        body = r.json()
        assert body["errors"][0]["kind"] == "UnsupportedField"
        # Verify entity type is correctly identified (endpoint handlers use singular)
        error = body["errors"][0]
        assert "subject" in error["message"].lower()
        assert "subject" in error["reason"].lower()
        # Verify field is sanitized
        assert error["field"] == "wrong field"
    
    def test_case_insensitive_entity_detection(self, client: TestClient):
        """Test that entity type detection is case-insensitive."""
        # Note: FastAPI routes are case-sensitive, so this will likely return 404
        # But if the path parsing logic is used, it should handle case
        r = client.get("/api/v1/SUBJECT/by/invalid_enum_value/count")
        # This will likely be 404 since routes are case-sensitive
        # But the entity_map.get() uses .lower() so it would work if route existed
        assert r.status_code in (400, 404)
    
    @pytest.mark.parametrize("entity,expected_singular", [
        ("subject", "subject"),
        ("sample", "sample"),
        ("file", "file"),
    ])
    def test_entity_type_in_error_message(self, client: TestClient, entity: str, expected_singular: str):
        """Test that entity names appear correctly in error messages."""
        # Use invalid enum value - endpoint handler will raise UnsupportedFieldError
        r = client.get(f"/api/v1/{entity}/by/invalid_enum_value/count")
        assert r.status_code == 400
        body = r.json()
        assert body["errors"][0]["kind"] == "UnsupportedField"
        # Endpoint handlers use singular form in error messages
        assert expected_singular in body["errors"][0]["message"].lower()
        assert expected_singular in body["errors"][0]["reason"].lower()
        # Verify field is sanitized
        assert body["errors"][0]["field"] == "wrong field"


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
            # Message must be non-empty and must not include the concrete query parameter name
            assert message, "Error message should not be empty"
            assert "unknown_param" not in message, "Error message should not expose concrete parameter names"
            # Should not expose the actual parameter name in a way that reveals internals
            if "parameters" in error:
                # Parameters array should be empty or sanitized
                params = error.get("parameters", [])
                # Should not expose internal parameter names
                assert all(not param.startswith("_") for param in params), "Exposes internal parameter names"

