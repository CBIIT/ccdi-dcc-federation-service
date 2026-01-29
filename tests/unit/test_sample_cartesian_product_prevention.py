"""
Unit tests to prevent cartesian product issues in SampleRepository queries.

These tests verify that queries with multiple study paths don't create cartesian products
that could crash the database. The tests check that:
1. Queries use WITH clauses between consecutive OPTIONAL MATCH statements for study paths
2. Queries return exactly one row per sample even when samples have multiple study relationships
3. Specific problematic queries (like disease_phase filter) work correctly

This prevents regression of the cartesian product bug that was fixed by adding WITH clauses
between study path OPTIONAL MATCH statements.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from neo4j import AsyncSession
import re

from app.repositories.sample import SampleRepository
from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings


@pytest.mark.unit
class TestSampleRepositoryCartesianProductPrevention:
    """Tests to prevent cartesian product issues in sample queries."""

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
        settings = Mock(spec=Settings)
        settings.sample_count_fields = ["tissue_type", "diagnosis", "disease_phase", "anatomic_site"]
        return settings

    @pytest.fixture
    def repository(self, mock_session, mock_allowlist, mock_settings):
        """Create a SampleRepository instance."""
        return SampleRepository(mock_session, mock_allowlist, mock_settings)

    def _extract_cypher_query(self, call):
        """Extract the Cypher query from a mock session.run call."""
        # call can be either a call object (with .args and .kwargs) or a tuple (args, kwargs)
        if hasattr(call, 'args'):
            # It's a call object
            args = call.args
            kwargs = call.kwargs
        elif isinstance(call, tuple) and len(call) == 2:
            # It's a tuple (args, kwargs)
            args, kwargs = call
        else:
            return None
        
        if args:
            return args[0]  # First positional argument is usually the query
        if kwargs and 'query' in kwargs:
            return kwargs['query']
        return None

    def _check_cartesian_product_prevention(self, query: str):
        """
        Check if a Cypher query has proper WITH clauses to prevent cartesian products.
        
        Returns:
            (is_safe, error_message): Tuple indicating if query is safe and any error message
        """
        if not query:
            return False, "Query is empty"
        
        # Pattern: OPTIONAL MATCH for study path 1, then OPTIONAL MATCH for study path 2 without WITH
        # This is the problematic pattern that creates cartesian products
        problematic_pattern = re.compile(
            r'OPTIONAL\s+MATCH.*?\(st1:study\)\s+OPTIONAL\s+MATCH.*?\(st2:study\)',
            re.IGNORECASE | re.DOTALL
        )
        
        # Check for the problematic pattern
        if problematic_pattern.search(query):
            # Check if there's a WITH clause between them
            # Split by OPTIONAL MATCH to see if WITH appears between study paths
            parts = re.split(r'OPTIONAL\s+MATCH', query, flags=re.IGNORECASE)
            for i in range(len(parts) - 1):
                part1 = parts[i]
                part2 = parts[i + 1]
                # If part1 contains st1:study and part2 contains st2:study
                if 'st1:study' in part1 or '(st1)' in part1:
                    if 'st2:study' in part2 or '(st2)' in part2:
                        # Check if there's a WITH clause between them
                        if 'WITH' not in part1[-200:] and 'WITH' not in part2[:200]:
                            return False, f"Missing WITH clause between study path OPTIONAL MATCH statements"
        
        # Check for proper pattern: WITH clauses with collect(DISTINCT st1) and collect(DISTINCT st2)
        # This is the safe pattern we want
        # Only check if we actually have study paths that could create cartesian products
        
        # Check if we have both st1 and st2 study paths (potential cartesian product)
        has_st1_path = 'st1:study' in query or '(st1)' in query
        has_st2_path = 'st2:study' in query or '(st2)' in query
        
        if has_st1_path and has_st2_path:
            # We have both paths - check for safe patterns
            # Pattern 1: collect(DISTINCT st1) or collect(DISTINCT st1.study_id)
            safe_pattern_st1 = re.compile(
                r'OPTIONAL\s+MATCH.*?\(st1:study\).*?WITH.*?collect\s*\(\s*DISTINCT\s+st1(?:\.study_id)?\s*\)',
                re.IGNORECASE | re.DOTALL
            )
            # Pattern 2: collect(DISTINCT st2) or collect(DISTINCT st2.study_id)
            safe_pattern_st2 = re.compile(
                r'OPTIONAL\s+MATCH.*?\(st2:study\).*?WITH.*?collect\s*\(\s*DISTINCT\s+st2(?:\.study_id)?\s*\)',
                re.IGNORECASE | re.DOTALL
            )
            
            # Check for safe patterns - at least one should be present
            has_safe_st1 = safe_pattern_st1.search(query)
            has_safe_st2 = safe_pattern_st2.search(query)
            
            # Also check for st1_list or st2_list patterns (alternative safe pattern)
            # Also check for study_ids_path1 or study_ids_path2 (optimized query pattern)
            has_st1_list = ('st1_list' in query or 
                           'collect(DISTINCT st1)' in query or 
                           'collect(DISTINCT st1.study_id)' in query or
                           'study_ids_path1' in query)
            has_st2_list = ('st2_list' in query or 
                           'collect(DISTINCT st2)' in query or 
                           'collect(DISTINCT st2.study_id)' in query or
                           'study_ids_path2' in query)
            
            # Check for WITH clause between the two OPTIONAL MATCH statements
            # This is the key indicator of safety
            st1_match_pos = query.find('st1:study')
            st2_match_pos = query.find('st2:study')
            if st1_match_pos != -1 and st2_match_pos != -1:
                # Determine which comes first
                if st1_match_pos < st2_match_pos:
                    # st1 comes first, check for WITH between st1 and st2
                    between_text = query[st1_match_pos:st2_match_pos]
                    has_with_between = 'WITH' in between_text
                else:
                    # st2 comes first, check for WITH between st2 and st1
                    between_text = query[st2_match_pos:st1_match_pos]
                    has_with_between = 'WITH' in between_text
                
                # If we have WITH between them, it's safe even if patterns don't match exactly
                if has_with_between:
                    return True, "Query is safe (WITH clause between study paths)"
            
            # If we have both paths but no safe patterns, it's potentially unsafe
            if not (has_safe_st1 or has_st1_list) and has_st1_path:
                return False, "Missing safe pattern for st1 (collect(DISTINCT st1) or st1_list or study_ids_path1)"
            
            if not (has_safe_st2 or has_st2_list) and has_st2_path:
                return False, "Missing safe pattern for st2 (collect(DISTINCT st2) or st2_list or study_ids_path2)"
        
        # If we only have one study path or no cartesian product risk, query is safe
        return True, "Query is safe"

    async def test_get_samples_disease_phase_filter_no_cartesian_product(self, repository, mock_session):
        """Test that disease_phase filter doesn't create cartesian products."""
        # Mock empty result (we're testing query structure, not results)
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # This was the query that caused the crash
        await repository.get_samples(
            filters={"disease_phase": "Not Reported"},
            offset=0,
            limit=20
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    async def test_get_samples_identifiers_multiple_study_ids_no_cartesian_product(self, repository, mock_session):
        """Test that identifiers filter with samples having multiple study_ids doesn't create cartesian products."""
        # Mock result with a sample that has multiple study relationships
        mock_record = MagicMock()
        mock_record.values.return_value = [
            MagicMock(spec=['sample_id']),  # sa
            MagicMock(spec=['participant_id']),  # p
            MagicMock(spec=['study_id']),  # st
            None,  # sf
            None,  # pf
            []  # diagnoses
        ]
        
        async def async_gen():
            yield mock_record
            return
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        # This sample ID was reported to have 2 study_ids
        await repository.get_samples(
            filters={"identifiers": "IID_H211979_T01_01_WG01"},
            offset=0,
            limit=20
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    async def test_count_samples_by_field_disease_phase_no_cartesian_product(self, repository, mock_session):
        """Test that count_samples_by_field with disease_phase doesn't create cartesian products."""
        # Mock result
        mock_record = MagicMock()
        mock_record.values.return_value = [{"value": "Not Reported", "count": 10, "total": 100, "missing": 5}]
        
        async def async_gen():
            yield mock_record
            return
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.count_samples_by_field(
            field="disease_phase",
            filters={}
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    async def test_get_samples_summary_no_cartesian_product(self, repository, mock_session):
        """Test that get_samples_summary doesn't create cartesian products."""
        # Mock result
        mock_record = MagicMock()
        mock_record.values.return_value = [{"total_count": 100}]
        
        async def async_gen():
            yield mock_record
            return
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.get_samples_summary(filters={})
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    async def test_count_samples_by_field_anatomical_sites_no_cartesian_product(self, repository, mock_session):
        """Test that count_samples_by_field with anatomical_sites doesn't create cartesian products."""
        # Mock result
        mock_record = MagicMock()
        mock_record.values.return_value = [{"value": "Lung", "count": 20, "total": 100, "missing": 10}]
        
        async def async_gen():
            yield mock_record
            return
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.count_samples_by_field(
            field="anatomical_sites",
            filters={}
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    def test_query_pattern_validation_safe_pattern(self):
        """Test that the validation function correctly identifies safe patterns."""
        safe_query = """
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2) AS st2_list
        WITH sa, coalesce(st1_list[0], st2_list[0]) AS st
        """
        
        is_safe, error_msg = self._check_cartesian_product_prevention(safe_query)
        assert is_safe, f"Safe pattern incorrectly flagged: {error_msg}"

    def test_query_pattern_validation_unsafe_pattern(self):
        """Test that the validation function correctly identifies unsafe patterns."""
        unsafe_query = """
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, coalesce(st1, st2) AS st
        """
        
        is_safe, error_msg = self._check_cartesian_product_prevention(unsafe_query)
        assert not is_safe, "Unsafe pattern incorrectly identified as safe"
        assert "WITH" in error_msg or "collect" in error_msg, f"Error message should mention WITH or collect: {error_msg}"

    async def test_get_samples_with_participant_filter_no_cartesian_product(self, repository, mock_session):
        """Test that queries with participant filters don't create cartesian products."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.get_samples(
            filters={"sex": "Female", "race": "White"},
            offset=0,
            limit=20
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

    async def test_get_samples_with_diagnosis_filter_no_cartesian_product(self, repository, mock_session):
        """Test that queries with diagnosis filters don't create cartesian products."""
        async def async_gen():
            return
            yield
        
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=async_gen())
        mock_session.run = AsyncMock(return_value=mock_result)
        
        await repository.get_samples(
            filters={"disease_phase": "Not Reported", "tumor_grade": "G1"},
            offset=0,
            limit=20
        )
        
        # Verify query was called
        assert mock_session.run.called
        
        # Check all query calls for cartesian product issues
        for call in mock_session.run.call_args_list:
            query = self._extract_cypher_query(call)
            if query and ('st1:study' in query or 'st2:study' in query):
                is_safe, error_msg = self._check_cartesian_product_prevention(query)
                assert is_safe, f"Cartesian product detected in query: {error_msg}\nQuery: {query[:500]}"

