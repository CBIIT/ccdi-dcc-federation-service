"""
Unit tests to verify consistency fixes for subject endpoints.

Tests verify:
1. vital_status and age_at_vital_status are consistent between list and individual endpoints
2. participant-study matching is consistent (namespace matches study_id)
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from typing import Dict, Any, List

from app.repositories.subject import SubjectRepository
from app.models.dto import Subject, SubjectId, NamespaceIdentifier, SubjectMetadata, MetadataField
from app.core.config import Settings


class TestSubjectVitalStatusConsistency:
    """Test that vital_status and age_at_vital_status are consistent between endpoints."""
    
    @pytest.mark.asyncio
    async def test_list_endpoint_fetches_survival_records(self):
        """Test that list endpoint (fast path) fetches survival records."""
        from app.lib.field_allowlist import FieldAllowlist
        
        # Create a mock session that captures the Cypher query
        class CapturingSession:
            def __init__(self):
                self.last_cypher = None
                self.last_params = None
            
            async def run(self, cypher, params=None):
                self.last_cypher = cypher
                self.last_params = params
                
                # Return a mock result with survival records
                class MockResult:
                    async def __aiter__(self):
                        # Return a record with survival_records
                        yield {
                            "name": "00301d78915737fa100f",
                            "race": "White",
                            "ethnicity": "Not reported",
                            "age_at_vital_status": None,
                            "vital_status": None,
                            "associated_diagnoses": None,
                            "survival_records": [
                                {"last_known_survival_status": "Dead", "age_at_last_known_survival_status": 50}
                            ],
                            "diagnosis_nodes": [],
                            "sex": "F",
                            "namespace": "phs002431",
                            "depositions": ["phs002431"]
                        }
                    
                    async def consume(self):
                        pass
                
                return MockResult()
        
        session = CapturingSession()
        allowlist = FieldAllowlist()
        repo = SubjectRepository(session=session, allowlist=allowlist, settings=Settings())
        
        # Call get_subjects with no filters (fast path)
        subjects = await repo.get_subjects({}, offset=0, limit=1)
        
        # Verify the query fetches survival records
        assert session.last_cypher is not None
        assert "OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)" in session.last_cypher
        assert "survival_records AS survival_records" in session.last_cypher
        assert "diagnosis_nodes AS diagnosis_nodes" in session.last_cypher
    
    @pytest.mark.asyncio
    async def test_list_endpoint_computes_vital_status_from_survival_records(self):
        """Test that _record_to_subject computes vital_status from survival_records."""
        from app.repositories.subject import SubjectRepository
        from app.lib.field_allowlist import FieldAllowlist
        
        repo = SubjectRepository(session=AsyncMock(), allowlist=FieldAllowlist(), settings=Settings())
        
        # Create a mock record with survival records
        record = {
            "name": "00301d78915737fa100f",
            "race": "White",
            "ethnicity": "Not reported",
            "age_at_vital_status": None,
            "vital_status": None,
            "associated_diagnoses": None,
            "survival_records": [
                {"last_known_survival_status": "Dead", "age_at_last_known_survival_status": 50}
            ],
            "diagnosis_nodes": [],
            "sex": "F",
            "namespace": "phs002431",
            "depositions": ["phs002431"]
        }
        
        # Convert record to subject
        subject = repo._record_to_subject(record)
        
        # Verify vital_status is computed from survival records
        assert subject is not None
        assert subject.metadata.vital_status is not None
        assert subject.metadata.vital_status.value == "Dead"
        assert subject.metadata.age_at_vital_status is not None
        assert subject.metadata.age_at_vital_status.value == 50


class TestSubjectStudyMatchingConsistency:
    """Test that participant-study matching is consistent between endpoints."""
    
    @pytest.mark.asyncio
    async def test_list_endpoint_groups_by_participant_study_pairs(self):
        """Test that list endpoint (fast path) groups by (participant_id, study_id) pairs."""
        from app.lib.field_allowlist import FieldAllowlist
        
        # Create a mock session that captures the Cypher query
        class CapturingSession:
            def __init__(self):
                self.last_cypher = None
                self.last_params = None
            
            async def run(self, cypher, params=None):
                self.last_cypher = cypher
                self.last_params = params
                
                # Return a mock result
                class MockResult:
                    async def __aiter__(self):
                        yield {
                            "name": "00301d78915737fa100f",
                            "race": "White",
                            "ethnicity": "Not reported",
                            "age_at_vital_status": None,
                            "vital_status": None,
                            "associated_diagnoses": None,
                            "survival_records": [],
                            "diagnosis_nodes": [],
                            "sex": "F",
                            "namespace": "phs002431",
                            "depositions": ["phs002430", "phs002431"]
                        }
                    
                    async def consume(self):
                        pass
                
                return MockResult()
        
        session = CapturingSession()
        allowlist = FieldAllowlist()
        repo = SubjectRepository(session=session, allowlist=allowlist, settings=Settings())
        
        # Call get_subjects with no filters (fast path)
        subjects = await repo.get_subjects({}, offset=0, limit=1)
        
        # Verify the query groups by (participant_id, study_id) pairs
        assert session.last_cypher is not None
        assert "WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id" in session.last_cypher
        assert "ORDER BY participant_id, study_id" in session.last_cypher
        assert "toString(study_id) AS namespace" in session.last_cypher  # Uses study_id directly, not head(study_ids)
    
    @pytest.mark.asyncio
    async def test_list_endpoint_returns_correct_namespace_for_study(self):
        """Test that list endpoint returns the correct namespace matching the study_id."""
        from app.repositories.subject import SubjectRepository
        from app.lib.field_allowlist import FieldAllowlist
        
        repo = SubjectRepository(session=AsyncMock(), allowlist=FieldAllowlist(), settings=Settings())
        
        # Create a mock record with specific study_id
        record = {
            "name": "00301d78915737fa100f",
            "race": "White",
            "ethnicity": "Not reported",
            "age_at_vital_status": None,
            "vital_status": None,
            "associated_diagnoses": None,
            "survival_records": [],
            "diagnosis_nodes": [],
            "sex": "F",
            "namespace": "phs002431",  # Specific study_id
            "depositions": ["phs002430", "phs002431"]  # All studies for this participant
        }
        
        # Convert record to subject
        subject = repo._record_to_subject(record)
        
        # Verify namespace matches the study_id
        assert subject is not None
        assert subject.id.namespace.name == "phs002431"
        # Verify depositions contains all studies
        assert "phs002430" in [d.value for d in (subject.metadata.depositions or [])]
        assert "phs002431" in [d.value for d in (subject.metadata.depositions or [])]


class TestSubjectEndpointConsistency:
    """Integration tests to verify list and individual endpoints return consistent data."""
    
    @pytest.mark.asyncio
    async def test_same_participant_returns_same_vital_status(self):
        """Test that the same participant returns the same vital_status in both endpoints."""
        from app.repositories.subject import SubjectRepository
        from app.lib.field_allowlist import FieldAllowlist
        
        # Mock survival record data
        survival_data = [
            {"last_known_survival_status": "Dead", "age_at_last_known_survival_status": 50}
        ]
        
        # Create mock session for list endpoint
        class ListSession:
            async def run(self, cypher, params=None):
                class MockResult:
                    async def __aiter__(self):
                        yield {
                            "name": "00301d78915737fa100f",
                            "race": "White",
                            "ethnicity": "Not reported",
                            "age_at_vital_status": None,
                            "vital_status": None,
                            "associated_diagnoses": None,
                            "survival_records": survival_data,
                            "diagnosis_nodes": [],
                            "sex": "F",
                            "namespace": "phs002431",
                            "depositions": ["phs002431"]
                        }
                    
                    async def consume(self):
                        pass
                
                return MockResult()
        
        # Create mock session for individual endpoint
        class IndividualSession:
            async def run(self, cypher, params=None):
                class MockResult:
                    async def __aiter__(self):
                        yield {
                            "name": "00301d78915737fa100f",
                            "race": "White",
                            "ethnicity": "Not reported",
                            "age_at_vital_status": 50,
                            "vital_status": "Dead",
                            "associated_diagnoses": None,
                            "sex": "F",
                            "namespace": "phs002431",
                            "depositions": ["phs002431"]
                        }
                    
                    async def consume(self):
                        pass
                
                return MockResult()
        
        allowlist = FieldAllowlist()
        
        # Test list endpoint
        list_repo = SubjectRepository(session=ListSession(), allowlist=allowlist, settings=Settings())
        list_subjects = await list_repo.get_subjects({}, offset=0, limit=1)
        
        # Test individual endpoint
        individual_repo = SubjectRepository(session=IndividualSession(), allowlist=allowlist, settings=Settings())
        individual_subject = await individual_repo.get_subject_by_identifier("CCDI-DCC", "phs002431", "00301d78915737fa100f")
        
        # Verify both return the same vital_status
        if list_subjects and individual_subject:
            list_vital_status = list_subjects[0].metadata.vital_status.value if list_subjects[0].metadata.vital_status else None
            individual_vital_status = individual_subject.metadata.vital_status.value if individual_subject.metadata.vital_status else None
            
            assert list_vital_status == individual_vital_status, \
                f"List endpoint returned vital_status={list_vital_status}, but individual endpoint returned {individual_vital_status}"
            
            list_age = list_subjects[0].metadata.age_at_vital_status.value if list_subjects[0].metadata.age_at_vital_status else None
            individual_age = individual_subject.metadata.age_at_vital_status.value if individual_subject.metadata.age_at_vital_status else None
            
            assert list_age == individual_age, \
                f"List endpoint returned age_at_vital_status={list_age}, but individual endpoint returned {individual_age}"

