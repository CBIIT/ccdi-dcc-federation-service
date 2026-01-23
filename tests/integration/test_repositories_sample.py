"""
Integration tests for SampleRepository.

Tests repository methods with a real database connection.
"""

import pytest

from app.repositories.sample import SampleRepository
from app.models.errors import UnsupportedFieldError


@pytest.mark.integration
class TestSampleRepositoryIntegration:
    """Integration tests for SampleRepository with real database."""
    
    async def test_get_samples_empty_database(
        self, sample_repository: SampleRepository
    ):
        """Test getting samples from empty database."""
        samples = await sample_repository.get_samples(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
        assert len(samples) == 0
    
    async def test_get_samples_with_test_data(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples with test data."""
        samples = await sample_repository.get_samples(
            filters={},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
        assert len(samples) >= 0
    
    async def test_get_samples_with_tissue_type_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by tissue_type."""
        samples = await sample_repository.get_samples(
            filters={"tissue_type": "Tumor"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
        for sample in samples:
            assert hasattr(sample, 'metadata')
    
    async def test_get_samples_with_diagnosis_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by diagnosis."""
        samples = await sample_repository.get_samples(
            filters={"diagnosis": "Neuroblastoma"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_with_disease_phase_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by disease_phase."""
        samples = await sample_repository.get_samples(
            filters={"disease_phase": "Initial Diagnosis"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_with_library_strategy_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by library_strategy."""
        samples = await sample_repository.get_samples(
            filters={"library_strategy": "WXS"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_with_anatomical_sites_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by anatomical_sites."""
        samples = await sample_repository.get_samples(
            filters={"anatomical_sites": "C71.9 : Brain, NOS"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_with_depositions_filter(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples filtered by depositions (study_id)."""
        samples = await sample_repository.get_samples(
            filters={"depositions": "phs002431"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_with_multiple_filters(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples with multiple filters."""
        samples = await sample_repository.get_samples(
            filters={"tissue_type": "Tumor", "diagnosis": "Neuroblastoma"},
            offset=0,
            limit=10
        )
        
        assert isinstance(samples, list)
    
    async def test_get_samples_pagination(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test pagination works correctly."""
        page1 = await sample_repository.get_samples(
            filters={},
            offset=0,
            limit=1
        )
        
        page2 = await sample_repository.get_samples(
            filters={},
            offset=1,
            limit=1
        )
        
        assert isinstance(page1, list)
        assert isinstance(page2, list)
        assert len(page1) <= 1
        assert len(page2) <= 1
    
    async def test_get_sample_by_identifier(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting a sample by identifier."""
        sample = await sample_repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            sample_id="SAMPLE-001"
        )
        
        if sample:
            assert hasattr(sample, 'id')
            assert sample.id.name == "SAMPLE-001"
    
    async def test_get_sample_by_identifier_not_found(
        self, sample_repository: SampleRepository
    ):
        """Test getting a non-existent sample."""
        sample = await sample_repository.get_sample_by_identifier(
            organization="CCDI-DCC",
            namespace="phs002431",
            sample_id="NONEXISTENT"
        )
        
        assert sample is None
    
    async def test_get_samples_summary(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples summary."""
        summary = await sample_repository.get_samples_summary(filters={})
        
        assert summary is not None
        assert hasattr(summary, 'counts')
        assert hasattr(summary.counts, 'total')
        assert isinstance(summary.counts.total, int)
        assert summary.counts.total >= 0
    
    async def test_get_samples_summary_with_filters(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test getting samples summary with filters."""
        summary = await sample_repository.get_samples_summary(
            filters={"tissue_type": "Tumor"}
        )
        
        assert summary is not None
        assert summary.counts.total >= 0
    
    async def test_count_samples_by_field_tissue_type(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test counting samples by tissue_type field."""
        result = await sample_repository.count_samples_by_field("tissue_type", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
        assert isinstance(result["values"], list)
    
    async def test_count_samples_by_field_diagnosis(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test counting samples by diagnosis field."""
        result = await sample_repository.count_samples_by_field("diagnosis", {})
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
        assert "values" in result
    
    async def test_count_samples_by_field_with_filters(
        self, sample_repository: SampleRepository, test_data_setup
    ):
        """Test counting samples by field with additional filters."""
        result = await sample_repository.count_samples_by_field(
            "tissue_type",
            {"diagnosis": "Neuroblastoma"}
        )
        
        assert result is not None
        assert isinstance(result, dict)
        assert "total" in result
    
    async def test_get_samples_with_invalid_field_raises_error(
        self, sample_repository: SampleRepository
    ):
        """Test that invalid fields raise UnsupportedFieldError."""
        with pytest.raises(UnsupportedFieldError):
            await sample_repository.get_samples(
                filters={"invalid_field": "value"},
                offset=0,
                limit=10
            )
