"""
Unit tests for URL builder utility.

Tests the build_identifier_server_url function which generates
server URLs for identifiers in the CCDI Federation API format.
"""

import pytest
from app.lib.url_builder import build_identifier_server_url


@pytest.mark.unit
class TestBuildIdentifierServerUrl:
    """Test cases for build_identifier_server_url function."""

    def test_build_subject_url(self):
        """Test building URL for subject entity."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="0061cbb0846973206fcf"
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002431/0061cbb0846973206fcf"
        assert result == expected

    def test_build_sample_url(self):
        """Test building URL for sample entity."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="sample",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="sample_123"
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/sample/CCDI-DCC/phs002431/sample_123"
        assert result == expected

    def test_build_file_url(self):
        """Test building URL for file entity."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="file",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="file_456"
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/file/CCDI-DCC/phs002431/file_456"
        assert result == expected

    def test_removes_trailing_slash_from_base_url(self):
        """Test that trailing slash is removed from base_url."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov/",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="test_id"
        )
        # Should not have double slashes
        assert "//api" not in result
        assert result.startswith("https://dcc.ccdi.cancer.gov/api")

    def test_with_empty_study_id(self):
        """Test building URL with empty study_id."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="",
            name="test_id"
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC//test_id"
        assert result == expected

    def test_with_empty_name(self):
        """Test building URL with empty name."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="phs002431",
            name=""
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002431/"
        assert result == expected

    def test_with_custom_organization(self):
        """Test building URL with custom organization."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            organization="CUSTOM-ORG",
            study_id="phs002431",
            name="test_id"
        )
        expected = "https://dcc.ccdi.cancer.gov/api/v1/subject/CUSTOM-ORG/phs002431/test_id"
        assert result == expected

    def test_with_default_organization(self):
        """Test building URL with default organization."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            study_id="phs002431",
            name="test_id"
        )
        # Should use default "CCDI-DCC"
        assert "CCDI-DCC" in result

    def test_with_different_base_url(self):
        """Test building URL with different base URL."""
        result = build_identifier_server_url(
            base_url="http://localhost:8000",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="test_id"
        )
        expected = "http://localhost:8000/api/v1/subject/CCDI-DCC/phs002431/test_id"
        assert result == expected

    def test_url_structure(self):
        """Test that URL follows expected structure."""
        result = build_identifier_server_url(
            base_url="https://example.com",
            entity_type="subject",
            organization="ORG",
            study_id="STUDY",
            name="NAME"
        )
        # Verify structure: {base}/api/v1/{entity}/{org}/{study}/{name}
        # URL: https://example.com/api/v1/subject/ORG/STUDY/NAME
        parts = result.split("/")
        assert parts[-6] == "api"
        assert parts[-5] == "v1"
        assert parts[-4] == "subject"
        assert parts[-3] == "ORG"
        assert parts[-2] == "STUDY"
        assert parts[-1] == "NAME"

