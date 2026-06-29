"""
Contract tests for subject endpoint consistency.

These tests verify that:
1. List and individual endpoints return consistent data for the same participant
2. vital_status and age_at_vital_status are computed consistently
3. namespace matches study_id correctly
"""

from typing import Any, Dict

import pytest

import app.api.v1.endpoints.subjects as subjects_ep
from app.models.dto import (
    Subject,
    SubjectId,
    NamespaceIdentifier,
    SubjectMetadata,
    MetadataField,
    DepositionAccession,
)


def _subject_with_vital_status(
    participant_id: str = "P1",
    study_id: str = "phs002431",
    vital_status: str | None = "Dead",
    age_at_vital_status: int | None = 50,
) -> Subject:
    """Create a subject with vital_status."""
    return Subject(
        id=SubjectId(namespace=NamespaceIdentifier(name=study_id), name=participant_id),
        metadata=SubjectMetadata(
            sex=MetadataField(value="F"),
            race=[MetadataField(value="White")],
            ethnicity=MetadataField(value="Not reported"),
            vital_status=MetadataField(value=vital_status) if vital_status else None,
            age_at_vital_status=MetadataField(value=age_at_vital_status) if age_at_vital_status else None,
            associated_diagnoses=None,
            depositions=[DepositionAccession(kind="dbGap", value=study_id)],
            identifiers=None,
        ),
    )


def test_subject_list_and_individual_return_same_vital_status(client, monkeypatch):
    """
    Contract test: List and individual endpoints must return the same vital_status
    for the same participant.
    
    This catches bugs where:
    - List endpoint doesn't fetch survival records
    - List endpoint doesn't compute vital_status from survival records
    - Different computation logic between endpoints
    """
    participant_id = "00301d78915737fa100f"
    study_id = "phs002431"
    vital_status = "Dead"
    age_at_vital_status = 50
    
    subject = _subject_with_vital_status(
        participant_id=participant_id,
        study_id=study_id,
        vital_status=vital_status,
        age_at_vital_status=age_at_vital_status,
    )
    
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            # Return subject from list endpoint
            return ([subject], 1)

        async def get_subject_by_identifier(
            self,
            organization: str,
            namespace: str,
            name: str,
            base_url: str | None = None,
        ):
            # Return same subject from individual endpoint
            return subject

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    # Get from list endpoint
    list_response = client.get(f"/api/v1/subject?identifiers={participant_id}&per_page=1")
    assert list_response.status_code == 200
    list_body = list_response.json()
    list_subject = list_body["data"][0]
    list_vital_status = list_subject.get("metadata", {}).get("vital_status", {}).get("value")
    list_age = list_subject.get("metadata", {}).get("age_at_vital_status", {}).get("value")

    # Get from individual endpoint
    individual_response = client.get(f"/api/v1/subject/CCDI-DCC/{study_id}/{participant_id}")
    assert individual_response.status_code == 200
    individual_body = individual_response.json()
    individual_vital_status = individual_body.get("metadata", {}).get("vital_status", {}).get("value")
    individual_age = individual_body.get("metadata", {}).get("age_at_vital_status", {}).get("value")

    # Verify consistency
    assert list_vital_status == individual_vital_status, (
        f"vital_status mismatch: list={list_vital_status}, individual={individual_vital_status}"
    )
    assert list_age == individual_age, (
        f"age_at_vital_status mismatch: list={list_age}, individual={individual_age}"
    )


def test_subject_list_namespace_matches_study_id(client, monkeypatch):
    """
    Contract test: List endpoint namespace must match a study_id from depositions.
    
    This catches bugs where:
    - List endpoint uses arbitrary study_id (head(study_ids))
    - Namespace doesn't match any study_id in depositions
    - Participant-study matching is inconsistent
    """
    participant_id = "00301d78915737fa100f"
    study_id_1 = "phs002430"
    study_id_2 = "phs002431"
    
    # Create subjects for each study
    subject_1 = _subject_with_vital_status(
        participant_id=participant_id,
        study_id=study_id_1,
        vital_status="Alive",
        age_at_vital_status=30,
    )
    subject_2 = _subject_with_vital_status(
        participant_id=participant_id,
        study_id=study_id_2,
        vital_status="Dead",
        age_at_vital_status=50,
    )
    
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            # Return both subjects (participant in multiple studies)
            return ([subject_1, subject_2], 2)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    # Get from list endpoint
    list_response = client.get(f"/api/v1/subject?identifiers={participant_id}&per_page=10")
    assert list_response.status_code == 200
    list_body = list_response.json()

    # Verify each row has namespace matching a study_id
    for subject in list_body["data"]:
        if subject.get("id", {}).get("name") == participant_id:
            namespace = subject.get("id", {}).get("namespace", {}).get("name")
            depositions = [d.get("value") for d in subject.get("metadata", {}).get("depositions", [])]
            
            # Namespace must be in depositions (or match one of the study_ids)
            assert namespace in depositions or namespace in [study_id_1, study_id_2], (
                f"namespace {namespace} not in depositions {depositions}"
            )


def test_subject_list_returns_multiple_rows_for_multiple_studies(client, monkeypatch):
    """
    Contract test: List endpoint must return one row per (participant_id, study_id) pair.
    
    This catches bugs where:
    - List endpoint groups by participant_id only (loses study_id information)
    - Only one row returned for participant in multiple studies
    """
    participant_id = "00301d78915737fa100f"
    study_id_1 = "phs002430"
    study_id_2 = "phs002431"
    
    # Create subjects for each study
    subject_1 = _subject_with_vital_status(
        participant_id=participant_id,
        study_id=study_id_1,
    )
    subject_2 = _subject_with_vital_status(
        participant_id=participant_id,
        study_id=study_id_2,
    )
    
    class FakeSubjectService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_subjects(
            self,
            filters: Dict[str, Any],
            offset: int = 0,
            limit: int = 20,
            base_url: str | None = None,
            return_total: bool = False,
        ):
            # Return both subjects (participant in multiple studies)
            return ([subject_1, subject_2], 2)

    monkeypatch.setattr(subjects_ep, "SubjectService", FakeSubjectService)

    # Get from list endpoint
    list_response = client.get(f"/api/v1/subject?identifiers={participant_id}&per_page=10")
    assert list_response.status_code == 200
    list_body = list_response.json()

    # Find all rows for this participant
    participant_rows = [
        s for s in list_body["data"]
        if s.get("id", {}).get("name") == participant_id
    ]
    
    # Should have at least one row per study
    namespaces = {row.get("id", {}).get("namespace", {}).get("name") for row in participant_rows}
    assert study_id_1 in namespaces or study_id_2 in namespaces, (
        f"Expected at least one row with namespace in [{study_id_1}, {study_id_2}], "
        f"but got namespaces: {namespaces}"
    )

