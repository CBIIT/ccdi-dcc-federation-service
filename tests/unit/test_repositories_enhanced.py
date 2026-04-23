"""
Enhanced unit tests for repository classes.

Tests additional edge cases, error handling, and complex scenarios
with comprehensive mocking.
"""

import pytest
from unittest.mock import AsyncMock, Mock, MagicMock
from neo4j import AsyncSession

from app.repositories.subject import SubjectRepository
from app.repositories.file import FileRepository
from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist, EntityType
from app.models.errors import UnsupportedFieldError
from app.models.dto import Subject, File, Sample


@pytest.mark.unit
class TestSubjectRepositoryEnhanced:
    """Enhanced test cases for SubjectRepository."""

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
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.subject_count_fields = ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses"]
        settings.sex_value_mappings = {}
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SubjectRepository instance."""
        return SubjectRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_subjects_with_multiple_filters(self, repository, mock_session):
        """Test get_subjects with multiple filters."""
        async def async_gen():
            yield {
                "name": "test_id",
                "participant_id": "test_id",
                "namespace": "phs002431",
                "sex": "F",
                "race": ["White"],
                "ethnicity": "Not Hispanic or Latino",
                "vital_status": "Alive",
                "age_at_vital_status": 45,
                "associated_diagnoses": [],
                "depositions": ["phs002431"]
            }
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"sex": "F", "vital_status": "Alive", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_race_list_filter(self, repository, mock_session):
        """Test get_subjects with race as a list."""
        # This test covers the race filter path but may not cover line 129 (empty race_list)
        # Let's add a test for that
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"race": ["White", "Black or African American"]},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_race_filter_reverse_mapping(self, repository, mock_session):
        """Test get_subjects with race filter applies reverse mapping for API values."""
        from unittest.mock import patch
        
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with API value that needs mapping
        with patch("app.repositories.subject.reverse_map_field_value") as mock_reverse_map:
            # Mock reverse mapping: API -> DB
            def reverse_map_side_effect(field, value):
                if field == "race" and value == "Not allowed to collect":
                    return "Not Allowed to Collect"
                return value  # No mapping for other values
            
            mock_reverse_map.side_effect = reverse_map_side_effect
            
            result = await repository.get_subjects(
                filters={"race": "Not allowed to collect"},  # API value
                offset=0,
                limit=20
            )
            
            # Verify reverse mapping was called
            mock_reverse_map.assert_called_with("race", "Not allowed to collect")
            
            # Verify query was executed
            assert mock_session.run.called
        
        assert isinstance(result, list)

    async def test_get_subjects_with_race_list_filter_reverse_mapping(self, repository, mock_session):
        """Test get_subjects with race list filter applies reverse mapping for each value."""
        from unittest.mock import patch
        
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with mixed API values (some need mapping, some don't)
        with patch("app.repositories.subject.reverse_map_field_value") as mock_reverse_map:
            def reverse_map_side_effect(field, value):
                if field == "race" and value == "Not allowed to collect":
                    return "Not Allowed to Collect"
                return value  # No mapping for other values
            
            mock_reverse_map.side_effect = reverse_map_side_effect
            
            result = await repository.get_subjects(
                filters={"race": ["White", "Not allowed to collect"]},  # Mixed: API value + mapped value
                offset=0,
                limit=20
            )
            
            # Verify reverse mapping was called for each race value
            assert mock_reverse_map.call_count == 2
            mock_reverse_map.assert_any_call("race", "White")
            mock_reverse_map.assert_any_call("race", "Not allowed to collect")
            
            assert mock_session.run.called
        
        assert isinstance(result, list)

    async def test_get_subjects_with_diagnosis_search(self, repository, mock_session):
        """Test get_subjects with diagnosis search filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"search": "cancer"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_identifiers_filter(self, repository, mock_session):
        """Test get_subjects with identifiers filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"identifiers": ["id1", "id2", "id3"]},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_subjects_with_age_at_vital_status_filter(self, repository, mock_session):
        """Test get_subjects with age_at_vital_status filter."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(
            filters={"age_at_vital_status": "45"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    @pytest.mark.skip(reason="Retry logic testing requires complex async mock setup")
    async def test_get_subjects_retry_logic(self, repository, mock_session):
        """Test get_subjects retry logic on error."""
        # This test requires complex async exception handling
        # Skipped for now - retry logic is tested in integration scenarios
        pass

    async def test_get_subjects_empty_result(self, repository, mock_session):
        """Test get_subjects with empty result."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects(filters={}, offset=0, limit=20)
        
        assert isinstance(result, list)
        assert len(result) == 0

    async def test_get_subject_by_identifier_not_found(self, repository, mock_session):
        """Test get_subject_by_identifier when subject not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subject_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None
        assert mock_session.run.called

    async def test_get_subject_by_identifier_with_base_url(self, repository, mock_session):
        """Test get_subject_by_identifier with base_url parameter."""
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
            name="test_id",
            base_url="https://test.example.com"
        )
        
        assert mock_session.run.called

    async def test_count_subjects_by_field_sex(self, repository, mock_session):
        """Test count_subjects_by_field with sex field."""
        # count_subjects_by_field uses complex query patterns
        # Mock async iteration for values and single() for totals
        async def async_gen():
            yield {"value": "M", "count": 10}
            yield {"value": "F", "count": 15}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.single = AsyncMock(return_value={"total": 25, "missing": 0})
        
        # The method may call run multiple times
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.count_subjects_by_field("sex", {})
        
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
        assert "missing" in result
        assert mock_session.run.called

    async def test_count_subjects_by_field_with_filters(self, repository, mock_session):
        """Test count_subjects_by_field with additional filters."""
        async def async_gen():
            yield {"value": "Alive", "count": 20}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.count_subjects_by_field(
            "vital_status",
            {"sex": "F"}
        )
        
        assert isinstance(result, dict)
        assert "total" in result

    async def test_count_subjects_by_field_invalid_field(self, repository, mock_allowlist, mock_session):
        """Test count_subjects_by_field with invalid field raises error."""
        # Field not in allowed count fields
        with pytest.raises(UnsupportedFieldError):
            await repository.count_subjects_by_field("invalid_field", {})

    async def test_get_subjects_summary(self, repository, mock_session):
        """Test get_subjects_summary."""
        # get_subjects_summary uses async iteration, returns {"total_count": ...}
        async def async_gen():
            yield {"total_count": 100}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects_summary(filters={})
        
        assert isinstance(result, dict)
        # The result structure is {"total_count": ...}
        assert "total_count" in result

    async def test_get_subjects_summary_with_filters(self, repository, mock_session):
        """Test get_subjects_summary with filters."""
        async def async_gen():
            yield {"total_count": 50}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_subjects_summary(
            filters={"sex": "F", "vital_status": "Alive"}
        )
        
        assert isinstance(result, dict)
        # The result structure is {"total_count": ...}
        assert "total_count" in result

    async def test_get_subjects_summary_with_race_filter_mapping(self, repository, mock_session):
        """Test get_subjects_summary with race filter applies reverse mapping."""
        from unittest.mock import patch
        
        async def async_gen():
            yield {"total_count": 5}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)

        # Test with API value that needs mapping
        # Patch the module where get_subjects_summary lives (subject_summary mixin)
        with patch("app.repositories.subject_summary.reverse_map_field_value") as mock_reverse_map:
            def reverse_map_side_effect(field, value):
                if field == "race" and value == "Not allowed to collect":
                    return "Not Allowed to Collect"
                return value
            
            mock_reverse_map.side_effect = reverse_map_side_effect
            
            result = await repository.get_subjects_summary(
                filters={"race": "Not allowed to collect"}  # API value
            )
            
            # Verify reverse mapping was called
            mock_reverse_map.assert_called_with("race", "Not allowed to collect")
            assert mock_session.run.called
        
        assert isinstance(result, dict)

    async def test_get_subjects_with_empty_race_list(self, repository, mock_session):
        """Test get_subjects with empty race list (covers line 129)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with race filter that results in empty list (line 129)
        result = await repository.get_subjects(
            filters={"race": ""},  # Empty string should result in empty race_list
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_subjects_with_race_not_reported(self, repository, mock_session):
        """Test get_subjects with race filter including 'Not Reported' (covers line 145)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with race filter that includes "Not Reported" (line 145)
        result = await repository.get_subjects(
            filters={"race": "Not Reported"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)

    async def test_get_subjects_with_ethnicity_filter(self, repository, mock_session):
        """Test get_subjects with ethnicity filter (covers lines 255-268)."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # Test with ethnicity filter (covers the ethnicity handling path)
        result = await repository.get_subjects(
            filters={"ethnicity": "Hispanic or Latino"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        
        # Test with "Not reported" ethnicity
        result2 = await repository.get_subjects(
            filters={"ethnicity": "Not reported"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result2, list)

    async def test_get_subjects_summary_for_diagnosis_fallback_without_search(self, repository):
        """Test diagnosis-summary path falls back to standard summary when _diagnosis_search is missing."""
        repository.get_subjects_summary = AsyncMock(return_value={"total_count": 11})

        result = await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"sex": "F", "_invalid_sex": True}
        )

        assert result == {"total_count": 11}
        repository.get_subjects_summary.assert_called_once_with({"sex": "F", "_invalid_sex": True})

    async def test_get_subjects_summary_for_diagnosis_category_contains_only_runs_query(
        self, repository, mock_session
    ):
        """Substring category filter without search still uses diagnosis-first Cypher path."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 7}])
        mock_session.run = AsyncMock(return_value=mock_result)
        repository.get_subjects_summary = AsyncMock()

        result = await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_associated_diagnosis_categories_contains": "brain"}
        )

        assert result == {"total_count": 7}
        repository.get_subjects_summary.assert_not_called()
        assert mock_session.run.called
        cypher = mock_session.run.call_args.args[0]
        run_params = mock_session.run.call_args.args[1]
        assert "$diag_category_contains_term" in cypher
        assert "diag_category_contains_term" in run_params

    async def test_get_subjects_summary_for_diagnosis_ethnicity_hispanic_predicate(
        self, repository, mock_session
    ):
        """Ethnicity filter must apply p.race predicates on diagnosis summary path."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 2}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Neuro", "ethnicity": "Hispanic or Latino"}
        )

        cypher = mock_session.run.call_args.args[0]
        run_params = mock_session.run.call_args.args[1]
        assert "toString(p.race) CONTAINS" in cypher
        assert any(v == "Hispanic or Latino" for v in run_params.values())

    async def test_get_subjects_summary_for_diagnosis_ethnicity_not_reported_predicate(
        self, repository, mock_session
    ):
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 1}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Glioma", "ethnicity": "Not reported"}
        )

        cypher = mock_session.run.call_args.args[0]
        run_params = mock_session.run.call_args.args[1]
        assert "NOT toString(p.race) CONTAINS" in cypher or "NOT toString(p.race)" in cypher
        assert any(v == "Hispanic or Latino" for v in run_params.values())

    async def test_get_subjects_summary_for_diagnosis_maps_sex_values(self, repository, mock_session):
        """Test diagnosis-summary path maps API sex values (F -> Female)."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 5}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Neuroblastoma", "sex": "F"}
        )

        assert mock_session.run.called
        _, params = mock_session.run.call_args.args
        assert any(v == "Female" for v in params.values())
        assert not any(v == "F" for v in params.values())

    async def test_get_subjects_summary_for_diagnosis_skips_internal_marker_keys(self, repository, mock_session):
        """Test diagnosis-summary path does not convert internal marker keys into participant filters."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 3}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "_unknown_parameters": ["bad_param"],
                "_invalid_sex": "X",
                "_age_at_vital_status_reason": "invalid",
                "sex": "F",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "p._unknown_parameters" not in cypher
        assert "p._invalid_sex" not in cypher
        assert "p._age_at_vital_status_reason" not in cypher

    async def test_get_subjects_summary_for_diagnosis_identifiers_with_depositions_has_id_list(
        self, repository, mock_session
    ):
        """Test diagnosis-summary depositions branch includes id_list scope for identifiers filter."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 2}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "identifiers": "IID_H211943",
                "depositions": "phs002620",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "AS id_list" in cypher
        assert "p.participant_id IN id_list" in cypher

    async def test_get_subjects_summary_for_diagnosis_identifiers_without_depositions_has_id_list(
        self, repository, mock_session
    ):
        """Test diagnosis-summary non-depositions branch includes id_list scope for identifiers filter."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 4}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "identifiers": "IID_H211943",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "AS id_list" in cypher
        assert "p.participant_id IN id_list" in cypher

    async def test_get_subjects_summary_for_diagnosis_race_not_reported(self, repository, mock_session):
        """Test diagnosis-summary path handles race filter with 'Not Reported' correctly."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 8}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "race": "Not Reported",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        # Should include special logic for "Not Reported" race
        assert "Not Reported" in cypher or "reduce" in cypher

    async def test_get_subjects_summary_for_diagnosis_vital_status_filter(self, repository, mock_session):
        """Test diagnosis-summary path applies vital_status derived filter correctly."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 12}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "vital_status": "Alive",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "final_vital_status" in cypher
        # Check params for vital_status value
        _, params = mock_session.run.call_args.args
        assert any("Alive" in str(v) or "alive" in str(v).lower() for v in params.values())

    async def test_get_subjects_summary_for_diagnosis_age_at_vital_status_filter(self, repository, mock_session):
        """Test diagnosis-summary path applies age_at_vital_status derived filter correctly."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 6}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "age_at_vital_status": 3407,
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "final_age_at_vital_status" in cypher

    async def test_get_subjects_summary_for_diagnosis_multiple_depositions(self, repository, mock_session):
        """Test diagnosis-summary path handles multiple depositions with || delimiter."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 15}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "depositions": "phs002620||phs003111",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        assert "IN" in cypher or "=" in cypher

    async def test_get_subjects_summary_for_diagnosis_empty_result(self, repository, mock_session):
        """Test diagnosis-summary path handles empty result correctly."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[])
        mock_session.run = AsyncMock(return_value=mock_result)

        result = await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "NonExistentDiagnosis",
            }
        )

        assert result == {"total_count": 0}

    async def test_get_subjects_summary_for_diagnosis_sex_mapping_all_values(self, repository, mock_session):
        """Test diagnosis-summary path maps all API sex values correctly."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 1}])
        mock_session.run = AsyncMock(return_value=mock_result)

        # Test M -> Male
        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Cancer", "sex": "M"}
        )
        _, params_m = mock_session.run.call_args.args
        assert any(v == "Male" for v in params_m.values())

        # Test U -> Not Reported
        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={"_diagnosis_search": "Cancer", "sex": "U"}
        )
        _, params_u = mock_session.run.call_args.args
        assert any(v == "Not Reported" for v in params_u.values())

    async def test_get_subjects_summary_for_diagnosis_where_clause_hardening(self, repository, mock_session):
        """Test diagnosis-summary path WHERE clause hardening removes malformed fragments."""
        mock_result = AsyncMock()
        mock_result.data = AsyncMock(return_value=[{"total_count": 3}])
        mock_session.run = AsyncMock(return_value=mock_result)

        await repository.get_subjects_summary_for_diagnosis_endpoint(
            filters={
                "_diagnosis_search": "Neuroblastoma",
                "sex": "F",
            }
        )

        cypher = mock_session.run.call_args.args[0]
        # Should not contain malformed WHERE clauses
        assert "WHERE AND" not in cypher
        assert "WHERE OR" not in cypher


@pytest.mark.unit
class TestFileRepositoryEnhanced:
    """Enhanced test cases for FileRepository."""

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

    async def test_get_files_with_multiple_filters(self, repository, mock_session):
        """Test get_files with multiple filters (file_type + depositions)."""
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
            filters={"file_type": "BAM", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        
        # Verify that when depositions is combined with file filters, it uses a different query pattern
        # (not the depositions-only early pagination path)
        call_args = mock_session.run.call_args
        if call_args:
            query = call_args[0][0] if call_args[0] else ""
            # With file filters + depositions, should use pattern 1 (file filters first, then study traversal)
            # Not the depositions-only pattern (which starts from study)
            assert "MATCH (sf:sequencing_file)" in query or "MATCH (st:study)" in query

    async def test_get_files_depositions_only_early_pagination(self, repository, mock_session):
        """Test depositions-only query uses early pagination (starts from study, paginates by sf.id)."""
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
            filters={"depositions": "phs002431"},  # Depositions-only, no other filters
            offset=10,
            limit=5
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called
        
        # Verify the query structure matches the new early pagination implementation
        call_args = mock_session.run.call_args
        assert call_args is not None
        
        query = call_args[0][0] if call_args[0] else ""
        params = call_args[0][1] if len(call_args[0]) > 1 and isinstance(call_args[0][1], dict) else (call_args[1] if call_args[1] else {})
        
        # Verify it starts from study (early pagination optimization)
        assert "MATCH (st:study)" in query
        assert "WHERE st.study_id" in query
        
        # Verify it matches files via the path (study <- consent_group <- participant <- sample <- sequencing_file)
        assert "of_consent_group" in query
        assert "of_participant" in query
        assert "of_sample" in query
        assert "of_sequencing_file" in query
        
        # Verify pagination by unique file ID (sf.id), not file-study pairs
        assert "WITH DISTINCT sf.id" in query or "WITH DISTINCT sf.id AS file_id" in query
        assert "ORDER BY" in query
        assert "ORDER BY file_id" in query or "ORDER BY sf.id" in query
        
        # Verify pagination happens BEFORE collecting samples (early pagination)
        skip_limit_pos = query.find("SKIP")
        limit_pos = query.find("LIMIT")
        samples_match_pos = query.find("OPTIONAL MATCH (sf)-[:of_sequencing_file]->(sa:sample)")
        
        assert skip_limit_pos != -1, "Query should have SKIP"
        assert limit_pos != -1, "Query should have LIMIT"
        assert samples_match_pos != -1, "Query should collect samples"
        
        # Samples should be collected AFTER pagination
        if skip_limit_pos != -1 and samples_match_pos != -1:
            assert skip_limit_pos < samples_match_pos, "Pagination (SKIP/LIMIT) should come BEFORE collecting samples"
        
        # Verify pagination parameters
        assert params.get("offset") == 10
        assert params.get("limit") == 5

    async def test_get_file_by_identifier(self, repository, mock_session):
        """Test get_file_by_identifier."""
        mock_result = AsyncMock()
        mock_record = {
            "file_id": "test_file",
            "f": {
                "file_id": "test_file",
                "file_type": "BAM"
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_file"
        )
        
        assert mock_session.run.called

    async def test_get_file_by_identifier_not_found(self, repository, mock_session):
        """Test get_file_by_identifier when file not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_file_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None

    async def test_get_files_summary(self, repository, mock_session):
        """Test get_files_summary."""
        async def async_gen():
            yield {"total_count": 200}
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_files_summary(filters={})
        
        assert isinstance(result, dict)
        # The summary returns a dict with total_count key
        assert "total_count" in result


@pytest.mark.unit
class TestSampleRepositoryEnhanced:
    """Enhanced test cases for SampleRepository."""

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
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.sample_count_fields = ["tissue_type", "diagnosis", "disease_phase"]
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    async def test_get_samples_with_multiple_filters(self, repository, mock_session):
        """Test get_samples with multiple filters."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples(
            filters={"tissue_type": "Tumor", "diagnosis": "Cancer", "depositions": "phs002431"},
            offset=0,
            limit=20
        )
        
        assert isinstance(result, list)
        assert mock_session.run.called

    async def test_get_sample_by_identifier(self, repository, mock_session):
        """Test get_sample_by_identifier."""
        mock_result = AsyncMock()
        mock_record = {
            "sample_id": "test_sample",
            "sa": {
                "sample_id": "test_sample",
                "tissue_type": "Tumor"
            },
            "study_ids": ["phs002431"]
        }
        mock_result.single = AsyncMock(return_value=mock_record)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="test_sample"
        )
        
        assert mock_session.run.called

    async def test_get_sample_by_identifier_not_found(self, repository, mock_session):
        """Test get_sample_by_identifier when sample not found."""
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=None)
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            name="nonexistent"
        )
        
        assert result is None

    async def test_get_samples_summary(self, repository, mock_session):
        """Test get_samples_summary."""
        # No filters: get_samples_summary calls session.run() once, returns total_count
        async def async_gen():
            yield {"total_count": 4}
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_result.consume = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        
        result = await repository.get_samples_summary(filters={})
        
        assert isinstance(result, dict)
        assert "counts" in result
        assert result["counts"]["total"] == 4

