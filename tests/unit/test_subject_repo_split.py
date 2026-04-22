"""Smoke tests: mixin classes importable and SubjectRepository still exposes all methods."""
from unittest.mock import MagicMock, AsyncMock
import pytest
from app.repositories.subject_count import SubjectCount


def test_subject_count_importable():
    assert hasattr(SubjectCount, "count_subjects_by_field")
    assert hasattr(SubjectCount, "_count_subjects_by_race")
    assert hasattr(SubjectCount, "_count_subjects_by_ethnicity")
    assert hasattr(SubjectCount, "_count_subjects_by_associated_diagnoses")
    assert hasattr(SubjectCount, "_count_subjects_by_diagnosis_category")
    assert hasattr(SubjectCount, "_get_field_path")
    assert hasattr(SubjectCount, "_build_sex_normalization_case")


def test_subject_summary_importable():
    from app.repositories.subject_summary import SubjectSummary
    assert hasattr(SubjectSummary, "get_subjects_summary")
    assert hasattr(SubjectSummary, "get_subjects_summary_for_diagnosis_endpoint")


def test_subject_repository_still_has_all_methods():
    from app.repositories.subject import SubjectRepository
    repo_methods = dir(SubjectRepository)
    for method in [
        "get_subjects", "get_subject_by_identifier", "_record_to_subject",
        "count_subjects_by_field", "_count_subjects_by_race",
        "get_subjects_summary", "get_subjects_summary_for_diagnosis_endpoint",
    ]:
        assert method in repo_methods, f"Missing: {method}"


def _make_repo():
    from app.repositories.subject import SubjectRepository
    session = MagicMock()
    allowlist = MagicMock()
    settings = MagicMock()
    settings.sex_value_mappings = {"Male": "M", "Female": "F", "Not Reported": "U"}
    settings.identifier_server_url = "https://example.com"
    settings.subject_count_fields = ["sex"]
    return SubjectRepository(session, allowlist, settings)


def _mock_empty_run(session):
    """Set up session.run to return an empty async result for all calls."""
    class AsyncEmptyIter:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def consume(self):
            pass

    async def fake_run(cypher, params=None):
        return AsyncEmptyIter()

    session.run = fake_run


@pytest.mark.asyncio
async def test_get_subjects_return_total_returns_tuple():
    """When return_total=True, get_subjects returns (list, int)."""
    repo = _make_repo()
    _mock_empty_run(repo.session)
    result = await repo.get_subjects({}, return_total=True)
    assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
    subjects, total = result
    assert isinstance(subjects, list)
    assert isinstance(total, int)


@pytest.mark.asyncio
async def test_get_subjects_without_return_total_returns_list():
    """Default: get_subjects returns a plain list."""
    repo = _make_repo()
    _mock_empty_run(repo.session)
    result = await repo.get_subjects({})
    assert isinstance(result, list), f"Expected list, got {type(result)}"
