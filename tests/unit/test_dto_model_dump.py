"""
Unit tests for DTO model_dump overrides.

Tests the model_dump methods that exclude gateways from serialization.
"""

import pytest
from unittest.mock import Mock
from app.models.dto import (
    SamplesResponse,
    FilesResponse,
    SubjectResponse,
    FileResponse,
    SummaryCounts
)


@pytest.mark.unit
class TestModelDumpOverrides:
    """Test model_dump overrides that exclude gateways."""

    def test_samples_response_model_dump_without_gateways(self):
        """Test SamplesResponse.model_dump when gateways is None."""
        response = SamplesResponse(
            data=[],
            counts=SummaryCounts(total=0)
        )
        object.__setattr__(response, 'gateways', None)
        
        result = response.model_dump()
        
        # Should not raise error and should work normally
        assert "data" in result
        assert "counts" in result

    def test_files_response_model_dump_without_gateways(self):
        """Test FilesResponse.model_dump when gateways is None."""
        response = FilesResponse(files=[])
        object.__setattr__(response, 'gateways', None)
        
        result = response.model_dump()
        
        # Should not raise error
        assert "files" in result

    def test_subject_response_model_dump_excludes_gateways(self):
        """Test SubjectResponse.model_dump excludes gateways field."""
        response = SubjectResponse(data=[])
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded
        assert "gateways" not in result
        assert "data" in result

    def test_subject_response_model_dump_without_gateways(self):
        """Test SubjectResponse.model_dump when gateways is None."""
        response = SubjectResponse(data=[])
        object.__setattr__(response, 'gateways', None)
        
        result = response.model_dump()
        
        # Should not raise error
        assert "data" in result

    def test_file_response_model_dump_without_gateways(self):
        """Test FileResponse.model_dump when gateways is None."""
        response = FileResponse(file=None)
        object.__setattr__(response, 'gateways', None)
        
        result = response.model_dump()
        
        # Should not raise error
        assert result is not None

    def test_file_response_model_dump_with_files_list(self):
        """Test FileResponse.model_dump with files list."""
        response = FileResponse(files=[])
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded
        assert "gateways" not in result
        assert "files" in result

    def test_samples_response_model_dump_excludes_gateways(self):
        """Test SamplesResponse.model_dump excludes gateways field (lines 556-558)."""
        from app.models.dto import SamplesResponse, SummaryCounts
        
        response = SamplesResponse(data=[], counts=SummaryCounts(total=0))
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded (lines 556-558)
        assert "gateways" not in result
        assert "data" in result

    def test_files_response_model_dump_excludes_gateways(self):
        """Test FilesResponse.model_dump excludes gateways field (lines 574-576)."""
        from app.models.dto import FilesResponse
        
        response = FilesResponse(files=[])
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded (lines 574-576)
        assert "gateways" not in result
        assert "files" in result

    def test_file_response_model_dump_excludes_gateways(self):
        """Test FileResponse.model_dump excludes gateways field (lines 646-648)."""
        from app.models.dto import FileResponse
        
        response = FileResponse(file=None)
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded (lines 646-648)
        assert "gateways" not in result

    def test_sample_response_model_dump_excludes_gateways(self):
        """Test SampleResponse.model_dump excludes gateways field (lines 619-621)."""
        from app.models.dto import SampleResponse
        
        response = SampleResponse(samples=[])
        object.__setattr__(response, 'gateways', {"gateway1": Mock()})
        
        result = response.model_dump()
        
        # Gateways should be excluded (lines 619-621)
        assert "gateways" not in result
        assert "samples" in result

