"""
Unit tests for DTO __init__ methods.

Tests missing lines in dto.py __init__ methods for better coverage.
"""

import pytest
from app.models.dto import Subject, Sample, File, SubjectId, SampleIdentifier, SubjectMetadata, SampleMetadata, NamespaceIdentifier


@pytest.mark.unit
class TestDTOInitMethods:
    """Test __init__ methods in DTO classes."""

    def test_subject_init_without_kind(self):
        """Test Subject.__init__ when kind not provided (line 421)."""
        # Create minimal valid Subject
        subject = Subject(
            id=SubjectId(
                namespace=NamespaceIdentifier(organization="CCDI-DCC", name="phs001"),
                name="P001"
            ),
            metadata=SubjectMetadata()
        )
        
        # Should default kind to "Participant" (line 421)
        assert subject.kind == "Participant"

    def test_subject_init_without_gateways(self):
        """Test Subject.__init__ when gateways not provided (line 423)."""
        subject = Subject(
            id=SubjectId(
                namespace=NamespaceIdentifier(organization="CCDI-DCC", name="phs001"),
                name="P001"
            ),
            metadata=SubjectMetadata()
        )
        
        # Should default gateways to empty list (line 423)
        assert subject.gateways == []

    def test_sample_init_without_gateways(self):
        """Test Sample.__init__ when gateways not provided (line 444)."""
        from app.models.dto import SampleMetadata
        
        # SampleIdentifier expects namespace as a dict (BaseIdentifier structure)
        # SampleMetadata is a CommonMetadata subclass, all fields are optional
        sample = Sample(
            id=SampleIdentifier(
                namespace={"organization": "CCDI-DCC", "name": "phs001"},
                name="S001"
            ),
            metadata=SampleMetadata()  # All fields optional
        )
        
        # Should default gateways to empty list (line 444)
        # The __init__ method checks if 'gateways' not in data and sets it to []
        assert sample.gateways == []

    def test_file_init(self):
        """Test File.__init__ (line 455)."""
        file_obj = File(id="file1", file_name="test.txt")
        
        assert file_obj.id == "file1"
        assert file_obj.file_name == "test.txt"

