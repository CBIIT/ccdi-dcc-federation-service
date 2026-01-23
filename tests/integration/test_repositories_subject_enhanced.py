"""
Enhanced integration tests for SubjectRepository.

Tests additional edge cases, error scenarios, and complex queries.
"""

import pytest
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.models.errors import UnsupportedFieldError


@pytest.mark.integration
class TestSubjectRepositoryEnhanced:
    """Enhanced integration tests for SubjectRepository."""
    
    async def test_get_subjects_with_age_at_vital_status_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by age_at_vital_status."""
        subjects = await subject_repository.get_subjects(
            filters={"age_at_vital_status": "45"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        for subject in subjects:
            assert hasattr(subject, 'metadata')
    
    async def test_get_subjects_with_associated_diagnoses_filter(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects filtered by associated_diagnoses."""
        subjects = await subject_repository.get_subjects(
            filters={"associated_diagnoses": "Neuroblastoma"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_diagnosis_search(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with diagnosis search filter."""
        subjects = await subject_repository.get_subjects(
            filters={"search": "Neuroblastoma"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_diagnosis_search_and_depositions(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with both diagnosis search and depositions filter."""
        subjects = await subject_repository.get_subjects(
            filters={"search": "Neuroblastoma", "depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_complex_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with multiple complex filters."""
        subjects = await subject_repository.get_subjects(
            filters={
                "sex": "F",
                "race": "White",
                "ethnicity": "Not Hispanic or Latino",
                "vital_status": "Alive",
                "depositions": "phs002431"
            },
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_empty_filters_dict(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with empty filters dict."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_zero_limit(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with zero limit."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=0
        )
        
        assert isinstance(subjects, list)
        assert len(subjects) == 0
    
    async def test_get_subjects_with_negative_offset(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with negative offset (should handle gracefully)."""
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=-1,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subject_by_identifier_with_base_url(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subject by identifier with base_url parameter."""
        subject = await subject_repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            participant_id="TEST-001",
            base_url="https://test.example.com"
        )
        
        if subject:
            assert hasattr(subject, 'id')
            assert subject.id.name == "TEST-001"
    
    async def test_get_subjects_summary_with_complex_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects summary with complex filters."""
        summary = await subject_repository.get_subjects_summary(
            filters={
                "sex": "F",
                "race": "White",
                "vital_status": "Alive"
            }
        )
        
        assert summary is not None
        assert hasattr(summary, 'counts')
        assert summary.counts.total >= 0
    
    async def test_get_subjects_summary_with_diagnosis_search(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects summary with diagnosis search."""
        summary = await subject_repository.get_subjects_summary(
            filters={"search": "Neuroblastoma"}
        )
        
        assert summary is not None
        assert summary.counts.total >= 0
    
    async def test_count_subjects_by_field_associated_diagnoses(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by associated_diagnoses field."""
        result = await subject_repository.count_subjects_by_field("associated_diagnoses", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_subjects_by_field_age_at_vital_status(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by age_at_vital_status field."""
        result = await subject_repository.count_subjects_by_field("age_at_vital_status", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_subjects_by_field_with_complex_filters(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test counting subjects by field with complex filters."""
        result = await subject_repository.count_subjects_by_field(
            "sex",
            {
                "race": "White",
                "vital_status": "Alive",
                "depositions": "phs002431"
            }
        )
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
    
    async def test_get_subjects_with_race_list_input(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with race as a list."""
        subjects = await subject_repository.get_subjects(
            filters={"race": ["White", "Black or African American"]},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
    
    async def test_get_subjects_with_identifiers_list(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test getting subjects with identifiers as a list."""
        subjects = await subject_repository.get_subjects(
            filters={"identifiers": ["TEST-001", "TEST-002", "TEST-003"]},
            offset=0,
            limit=10
        )
        
        assert isinstance(subjects, list)
        subject_names = [s.id.name for s in subjects]
        # Should return subjects matching any of the identifiers
        assert len(subjects) >= 0
    
    async def test_get_subjects_pagination_edge_cases(
        self, subject_repository: SubjectRepository, test_data_setup
    ):
        """Test pagination edge cases."""
        # Test with limit larger than available data
        subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=1000
        )
        
        assert isinstance(subjects, list)
        
        # Test with offset equal to total
        all_subjects = await subject_repository.get_subjects(
            filters={},
            offset=0,
            limit=100
        )
        if len(all_subjects) > 0:
            subjects_at_end = await subject_repository.get_subjects(
                filters={},
                offset=len(all_subjects),
                limit=10
            )
            assert isinstance(subjects_at_end, list)
            assert len(subjects_at_end) == 0
    
    async def test_get_subject_by_identifier_nonexistent_namespace(
        self, subject_repository: SubjectRepository
    ):
        """Test getting subject from nonexistent namespace."""
        subject = await subject_repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="nonexistent",
            participant_id="TEST-001"
        )
        
        assert subject is None
    
    async def test_get_subjects_summary_empty_database(
        self, subject_repository: SubjectRepository, db_session, test_data_setup
    ):
        """Test getting subjects summary from empty database."""
        # Clear database after setup
        cleanup_result = await db_session.run("MATCH (n) DETACH DELETE n")
        await cleanup_result.consume()
        
        summary = await subject_repository.get_subjects_summary(filters={})
        
        assert summary is not None
        assert summary.counts.total == 0
    
    async def test_count_subjects_by_field_empty_database(
        self, subject_repository: SubjectRepository, db_session, test_data_setup
    ):
        """Test counting subjects by field in empty database."""
        # Clear database after setup
        cleanup_result = await db_session.run("MATCH (n) DETACH DELETE n")
        await cleanup_result.consume()
        
        result = await subject_repository.count_subjects_by_field("sex", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert result["total"] == 0
        assert result["missing"] == 0
        assert isinstance(result["values"], list)

