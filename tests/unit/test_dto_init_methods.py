"""
Unit tests for DTO __init__ methods.

Tests missing lines in dto.py __init__ methods for better coverage.
"""

import pytest
from app.models.dto import (
    Subject,
    Sample,
    File,
    SubjectId,
    SampleIdentifier,
    SubjectMetadata,
    SampleMetadata,
    NamespaceIdentifier,
)


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

    def test_namespace_identifier_instance_accepted_by_sample_and_subject_id(self):
        """One NamespaceIdentifier class must satisfy SampleIdentifier and SubjectId."""
        ns = NamespaceIdentifier(organization="CCDI-DCC", name="phs001")
        sample = Sample(
            id=SampleIdentifier(namespace=ns, name="S001"),
            metadata=SampleMetadata(),
        )
        assert sample.id.namespace is ns
        subject = Subject(
            id=SubjectId(namespace=ns, name="P001"),
            metadata=SubjectMetadata(),
        )
        assert subject.id.namespace is ns

    def test_sample_init_without_gateways(self):
        """Test Sample.__init__ when gateways not provided (line 444)."""
        # SampleIdentifier accepts NamespaceIdentifier instances (single shared class)
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
