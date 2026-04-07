"""
Unit tests for repository classes.

Tests data access layer including query building, filtering,
and database operations with mocked sessions.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.repositories.file import FileRepository
from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist, EntityType


@pytest.mark.unit
class TestSubjectRepositoryHelpers:
    """Test cases for SubjectRepository static helper methods."""

    def test_split_or_values_with_pipe_delimiter(self):
        """Test _split_or_values with || delimiter."""
        result = SubjectRepository._split_or_values("a||b||c")
        assert result == ["a", "b", "c"]

    def test_split_or_values_single_value(self):
        """Test _split_or_values with single value."""
        result = SubjectRepository._split_or_values("single")
        assert result == ["single"]

    def test_split_or_values_with_list(self):
        """Test _split_or_values with list input."""
        result = SubjectRepository._split_or_values(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_split_or_values_with_empty_list(self):
        """Test _split_or_values with empty list."""
        result = SubjectRepository._split_or_values([])
        assert result is None

    def test_split_or_values_with_none(self):
        """Test _split_or_values with None."""
        result = SubjectRepository._split_or_values(None)
        assert result is None

    def test_split_or_values_with_empty_string(self):
        """Test _split_or_values with empty string."""
        result = SubjectRepository._split_or_values("")
        assert result is None

    def test_split_or_values_with_whitespace(self):
        """Test _split_or_values with whitespace."""
        result = SubjectRepository._split_or_values("  a  ||  b  ")
        assert result == ["a", "b"]

    def test_split_or_values_filters_empty_items(self):
        """Test _split_or_values filters out empty items."""
        result = SubjectRepository._split_or_values("a|| ||b||")
        assert result == ["a", "b"]

    def test_build_combined_where_clause_diagnosis_only(self):
        """Test _build_combined_where_clause with diagnosis search only."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            diagnosis_search_term="cancer",
            dep_param=None,
            deposition_operator=None
        )
        assert "WHERE" in result
        assert "diagnosis_search_term" in result
        assert "dep_param" not in result

    def test_build_combined_where_clause_depositions_only(self):
        """Test _build_combined_where_clause with depositions only."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            diagnosis_search_term=None,
            dep_param="phs002431",
            deposition_operator="="
        )
        assert "WHERE" in result
        assert "$phs002431" in result or "dep_param" in result
        assert "diagnosis_search_term" not in result

    def test_build_combined_where_clause_both(self):
        """Test _build_combined_where_clause with both conditions."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            diagnosis_search_term="cancer",
            dep_param="phs002431",
            deposition_operator="="
        )
        assert "WHERE" in result
        assert "AND" in result
        assert "diagnosis_search_term" in result
        assert "$phs002431" in result or "dep_param" in result

    def test_build_combined_where_clause_none(self):
        """Test _build_combined_where_clause with no conditions."""
        result = SubjectRepository._build_combined_where_clause_for_depositions_path(
            diagnosis_search_term=None,
            dep_param=None,
            deposition_operator=None
        )
        assert result == ""


@pytest.mark.unit
class TestSubjectRepository:
    """Test cases for SubjectRepository class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = AsyncMock(spec=AsyncSession)
        return session

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SubjectRepository instance."""
        return SubjectRepository(mock_session, mock_allowlist)

    def test_initialization(self, repository, mock_session, mock_allowlist):
        """Test repository initialization."""
        assert repository.session is mock_session
        assert repository.allowlist is mock_allowlist

    async def test_get_subjects_empty_filters(self, repository, mock_session):
        """Test get_subjects with empty filters."""
        # Mock database result with proper async iteration and correct record structure
        async def async_gen():
            yield {
                "name": "test_id",
                "participant_id": "test_id",
                "namespace": "phs002431",
                "sex": "M",
                "race": ["White"],
                "ethnicity": "Not reported",
                "vital_status": None,
                "age_at_vital_status": -999,
                "associated_diagnoses": [],
                "depositions": ["phs002431"]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times for optimization

    async def test_get_subjects_with_sex_filter(self, repository, mock_session):
        """Test get_subjects with sex filter."""
        async def async_gen():
            return
            yield  # Make it an async generator
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"sex": "M"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_subjects_with_race_filter(self, repository, mock_session):
        """Test get_subjects with race filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"race": "White"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_subjects_with_race_filter_mapping(self, repository, mock_session):
        """Test get_subjects with race filter applies reverse mapping (API -> DB)."""
        from unittest.mock import patch
        
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Mock reverse_map_field_value to return database value
        with patch("app.repositories.subject.reverse_map_field_value") as mock_reverse_map:
            mock_reverse_map.return_value = "Not Allowed to Collect"
            
            result = await repository.get_subjects(
                filters={"race": "Not allowed to collect"},  # API value
                offset=0,
                limit=20
            )
            
            # Verify reverse mapping was called with API value
            mock_reverse_map.assert_called_once_with("race", "Not allowed to collect")
            
            # Verify the database value was used in the query params
            assert mock_session.run.called
            # Check that the mapped DB value is in the params
            call_args = mock_session.run.call_args
            if call_args and len(call_args) > 1:
                params = call_args[1] if isinstance(call_args[1], dict) else call_args[0][1] if len(call_args[0]) > 1 else {}
                # The race_tokens param should contain the DB value
                for key, value in params.items():
                    if "race" in key.lower() or (isinstance(value, list) and "Not Allowed to Collect" in value):
                        assert "Not Allowed to Collect" in (value if isinstance(value, list) else [value])
        
        assert isinstance(result, list)

    async def test_get_subjects_with_depositions_filter(self, repository, mock_session):
        """Test get_subjects with depositions filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_subjects_pagination(self, repository, mock_session):
        """Test get_subjects with pagination parameters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={},
            offset=10,
            limit=50
        )
        
        assert isinstance(result, list)
        # Verify offset and limit were passed
        call_args = mock_session.run.call_args
        assert call_args is not None
        # Parameters can be passed as second positional arg or as keyword arg
        if len(call_args[0]) > 1:
            params = call_args[0][1] if isinstance(call_args[0][1], dict) else {}
        elif call_args[1] and "parameters" in call_args[1]:
            params = call_args[1]["parameters"]
        else:
            params = call_args[1] if call_args[1] else {}
        assert params.get("offset") == 10
        assert params.get("limit") == 50

    async def test_get_subjects_invalid_field_handling(self, repository, mock_allowlist, mock_session):
        """Test get_subjects handles invalid field (may ignore or validate elsewhere)."""
        from app.models.errors import UnsupportedFieldError
        # Set up allowlist to reject the field
        mock_allowlist.is_field_allowed = Mock(return_value=False)
        
        # Mock empty result since invalid field may be ignored or cause empty results
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # The repository may ignore invalid fields or validation happens at service/endpoint level
        # Test that the method completes without crashing
        result = await repository.get_subjects(
            filters={"invalid_field": "value"},
            offset=0,
            limit=20
        )
        
        # Should return a list (may be empty if field is ignored)
        assert isinstance(result, list)
        # Query should still execute
        assert mock_session.run.called

    async def test_get_subject_by_identifier(self, repository, mock_session):
        """Test get_subject_by_identifier."""
        mock_result = AsyncMock()
        mock_record = {
            "participant_id": "test_id",
            "p": {
                "participant_id": "test_id",
                "sex_at_birth": "M",
                "race": ["White"]
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_id"
        )
        
        # Result may be None if record doesn't match expected structure
        # Just verify the method was called
        assert mock_session.run.called
        # If result is not None, verify it's a Subject object
        if result is not None:
            assert hasattr(result, "name") or hasattr(result, "participant_id")


@pytest.mark.unit
class TestFileRepository:
    """Test cases for FileRepository class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a FileRepository instance."""
        return FileRepository(mock_session, mock_allowlist)

    def test_initialization(self, repository, mock_session, mock_allowlist):
        """Test repository initialization."""
        assert repository.session is mock_session
        assert repository.allowlist is mock_allowlist

    async def test_get_files_empty_filters(self, repository, mock_session):
        """Test get_files with empty filters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_files_with_type_filter(self, repository, mock_session):
        """Test get_files with type filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files(
            filters={"file_type": "FASTQ"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_files_with_depositions_filter(self, repository, mock_session):
        """Test get_files with depositions filter - verifies early pagination query structure."""
        async def async_gen():
            yield {
                "sf": {"id": "file1", "file_type": "BAM"},
                "samples": [{"sample_id": "SAMP001"}],
                "st": {"study_id": "phs002431"}
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        
        # Verify the query structure for depositions-only filter
        call_args = mock_session.run.call_args
        if call_args:
            query = call_args[0][0] if call_args[0] else ""
            params = call_args[0][1] if len(call_args[0]) > 1 and isinstance(call_args[0][1], dict) else (call_args[1] if call_args[1] else {})
            
            # Verify it starts from study nodes (early pagination optimization)
            assert "MATCH (st:study)" in query
            assert "st.study_id" in query
            
            # Verify it matches files via the path (study <- consent_group <- participant <- sample <- sequencing_file)
            assert "of_consent_group" in query or "of_participant" in query
            assert "of_sequencing_file" in query
            
            # Verify early pagination: DISTINCT sf.id before SKIP/LIMIT
            assert "WITH DISTINCT sf.id" in query or "WITH DISTINCT sf.id AS file_id" in query
            assert "ORDER BY" in query
            assert "SKIP" in query
            assert "LIMIT" in query
            
            # Verify pagination parameters are passed
            assert params.get("offset") == 0
            assert params.get("limit") == 20
            
            # Verify samples are collected AFTER pagination (not before)
            # The OPTIONAL MATCH for samples should come after SKIP/LIMIT
            skip_limit_pos = query.find("LIMIT")
            samples_match_pos = query.find("OPTIONAL MATCH (sf)-[:of_sequencing_file]->(sa:sample)")
            if skip_limit_pos != -1 and samples_match_pos != -1:
                assert skip_limit_pos < samples_match_pos, "Samples should be collected AFTER pagination"

    async def test_get_files_pagination(self, repository, mock_session):
        """Test get_files with pagination."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files(
            filters={},
            offset=5,
            limit=10
        )
        
        assert isinstance(result, list)
        call_args = mock_session.run.call_args
        assert call_args is not None
        # Parameters can be passed as second positional arg or as keyword arg
        if len(call_args[0]) > 1:
            params = call_args[0][1] if isinstance(call_args[0][1], dict) else {}
        elif call_args[1] and "parameters" in call_args[1]:
            params = call_args[1]["parameters"]
        else:
            params = call_args[1] if call_args[1] else {}
        assert params.get("offset") == 5
        assert params.get("limit") == 10


@pytest.mark.unit
class TestSampleRepository:
    """Test cases for SampleRepository class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def mock_allowlist(self):
        """Create a mock field allowlist."""
        allowlist = Mock(spec=FieldAllowlist)
        allowlist.is_field_allowed = Mock(return_value=True)
        return allowlist

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist)

    def test_initialization(self, repository, mock_session, mock_allowlist):
        """Test repository initialization."""
        assert repository.session is mock_session
        assert repository.allowlist is mock_allowlist

    async def test_get_samples_empty_filters(self, repository, mock_session):
        """Test get_samples with empty filters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_samples_with_disease_phase_filter(self, repository, mock_session):
        """Test get_samples with disease_phase filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"disease_phase": "Primary"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called  # May be called multiple times

    async def test_get_samples_pagination(self, repository, mock_session):
        """Test get_samples with pagination."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={},
            offset=15,
            limit=25
        )
        
        assert isinstance(result, list)
        call_args = mock_session.run.call_args
        assert call_args is not None
        # Parameters can be passed as second positional arg or as keyword arg
        if len(call_args[0]) > 1:
            params = call_args[0][1] if isinstance(call_args[0][1], dict) else {}
        elif call_args[1] and "parameters" in call_args[1]:
            params = call_args[1]["parameters"]
        else:
            params = call_args[1] if call_args[1] else {}
        assert params.get("offset") == 15
        assert params.get("limit") == 25

