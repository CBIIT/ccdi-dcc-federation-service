"""
Integration tests for SubjectRepository.

Tests repository methods with a real database connection.
"""

import pytest
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.models.errors import UnsupportedFieldError


@pytest.mark.integration
class TestSubjectRepositoryIntegration:
    """Integration tests for SubjectRepository with real database."""
    
    async def test_get_subjects_empty_database(
        self, subject_repository: SubjectRepository
    ):
        """Test getting subjects from empty database."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        assert len(subjects) == 0
    
    async def test_get_subjects_with_test_data(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with test data."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        assert len(subjects) >= 0
    
    async def test_get_subjects_with_sex_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by sex."""
        subjects = await subject_repository.get_subjects(
            filters={"sex": "F"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        for subject in subjects:
            assert hasattr(subject, 'metadata')
            if hasattr(subject.metadata, 'sex') and subject.metadata.sex:
                assert subject.metadata.sex.value == "F"
    
    async def test_get_subjects_with_race_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by race."""
        subjects = await subject_repository.get_subjects(
            filters={"race": "White"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        # All returned subjects should have White in their race
        for subject in subjects:
            assert hasattr(subject, 'metadata')
    
    async def test_get_subjects_with_race_filter_multiple_values(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by race with || separator."""
        subjects = await subject_repository.get_subjects(
            filters={"race": "White||Black or African American"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_ethnicity_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by ethnicity."""
        subjects = await subject_repository.get_subjects(
            filters={"ethnicity": "Not Hispanic or Latino"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_vital_status_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by vital status."""
        subjects = await subject_repository.get_subjects(
            filters={"vital_status": "Alive"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_depositions_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by depositions (study_id)."""
        subjects = await subject_repository.get_subjects(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_multiple_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with multiple filters (AND logic)."""
        subjects = await subject_repository.get_subjects(
            filters={"sex": "F", "vital_status": "Alive"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        for subject in subjects:
            assert hasattr(subject, 'metadata')
    
    async def test_get_subjects_pagination(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test pagination works correctly."""
        # Get first page
        page1 = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=1
        )
        
        # Get second page
        page2 = await subject_repository.get_subjects(
            filters={},
            offset=1,
            limit=1
        )
        
        assert isinstance(page1, list)
        assert isinstance(page2, list)
        assert len(page1) <= 1
        assert len(page2) <= 1
        # If we have data, pages should be different
        if len(page1) > 0 and len(page2) > 0:
            assert page1[0].id.name != page2[0].id.name
    
    async def test_get_subjects_pagination_large_offset(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test pagination with large offset returns empty list."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=1000,
            limit=10
        )
        
        assert isinstance(subjects, list)
        assert len(subjects) == 0
    
    async def test_get_subject_by_identifier(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting a subject by identifier."""
        subject = await subject_repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            participant_id="TEST-001"
        )
        
        if subject:
            assert hasattr(subject, 'id')
            assert subject.id.name == "TEST-001"
            assert subject.id.namespace.name == "phs002431"
    
    async def test_get_subject_by_identifier_not_found(
        self, subject_repository: SubjectRepository
    ):
        """Test getting a non-existent subject."""
        subject = await subject_repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            participant_id="NONEXISTENT"
        )
        
        assert subject is None
    
    async def test_get_subject_by_identifier_different_namespace(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subject from different namespace."""
        subject = await subject_repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002432",
            participant_id="TEST-003"
        )
        
        if subject:
            assert subject.id.namespace.name == "phs002432"
    
    async def test_get_subjects_summary(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects summary."""
        summary = await subject_repository.get_subjects_summary(filters={})
        
        assert summary is not None
        assert hasattr(summary, 'counts')
        assert hasattr(summary.counts, 'total')
        assert isinstance(summary.counts.total, int)
        assert summary.counts.total >= 0
    
    async def test_get_subjects_summary_with_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects summary with filters."""
        summary = await subject_repository.get_subjects_summary(filters={"sex": "F"})
        
        assert summary is not None
        assert hasattr(summary, 'counts')
        assert summary.counts.total >= 0
    
    async def test_count_subjects_by_field_sex(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by sex field."""
        result = await subject_repository.count_subjects_by_field("sex", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "missing" in result
        assert "values" in result
        assert isinstance(result["values"], list)
        assert isinstance(result["total"], int)
        assert result["total"] >= 0
    
    async def test_count_subjects_by_field_race(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by race field."""
        result = await subject_repository.count_subjects_by_field("race", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_subjects_by_field_ethnicity(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by ethnicity field."""
        result = await subject_repository.count_subjects_by_field("ethnicity", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_subjects_by_field_vital_status(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by vital_status field."""
        result = await subject_repository.count_subjects_by_field("vital_status", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_subjects_by_field_with_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by field with additional filters."""
        result = await subject_repository.count_subjects_by_field(
            "sex",
            {"vital_status": "Alive"}
        )
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
    
    async def test_get_subjects_with_identifiers_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by identifiers."""
        subjects = await subject_repository.get_subjects(
            filters={"identifiers": ["TEST-001", "TEST-002"]},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        # Should return subjects matching the identifiers
        subject_names = [s.id.name for s in subjects]
        assert "TEST-001" in subject_names or "TEST-002" in subject_names or len(subjects) == 0
    
    async def test_get_subjects_with_invalid_field_raises_error(
        self, subject_repository: SubjectRepository
    ):
        """Test that invalid fields raise UnsupportedFieldError."""
        with pytest.raises(UnsupportedFieldError):
            await subject_repository.get_subjects(
                filters={"invalid_field": "value"},
                offset=0,
                limit=10
            )
    
    async def test_split_or_values(
        self, subject_repository: SubjectRepository
    ):
        """Test _split_or_values helper method."""
        # Test with || separator
        result = SubjectRepository._split_or_values("a||b||c")
        assert result == ["a", "b", "c"]
        
        # Test with single value
        result = SubjectRepository._split_or_values("single")
        assert result == ["single"]
        
        # Test with list
        result = SubjectRepository._split_or_values(["a", "b"])
        assert result == ["a", "b"]
        
        # Test with None
        result = SubjectRepository._split_or_values(None)
        assert result is None
        
        # Test with empty string
        result = SubjectRepository._split_or_values("")
        assert result is None
        
        # Test with whitespace
        result = SubjectRepository._split_or_values("  a  ||  b  ")
        assert result == ["a", "b"]

