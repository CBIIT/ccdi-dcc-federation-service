"""
Unit tests for SampleHelpers filter categorization.

Tests that filters are correctly categorized into:
- sample filters
- study filters  
- diagnosis filters
- sequencing_file filters
- pathology_file filters
"""

import pytest
from app.repositories.sample_helpers import SampleHelpers


@pytest.mark.unit
class TestFilterCategorization:
    """Test filter categorization logic."""
    
    def test_categorize_sample_filters(self):
        """Test that sample filters are categorized correctly."""
        filters = {
            "tissue_type": "Tumor",
            "anatomical_sites": "C72.9",
            "age_at_collection": 10,
            "identifiers": "SAMP001"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        assert "tissue_type" in categorized["sample"]
        assert "anatomical_sites" in categorized["sample"]
        assert "age_at_collection" in categorized["sample"]
        assert "identifiers" in categorized["sample"]
        
        # Should not be in other categories
        assert len(categorized["study"]) == 0
        assert len(categorized["diagnosis"]) == 0
        assert len(categorized["sequencing_file"]) == 0
        assert len(categorized["pathology_file"]) == 0
    
    def test_categorize_study_filters(self):
        """Test that study filters are categorized correctly."""
        filters = {
            "depositions": "phs002431"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        assert "depositions" in categorized["study"]
        assert len(categorized["sample"]) == 0
        assert len(categorized["diagnosis"]) == 0
    
    def test_categorize_diagnosis_filters(self):
        """Test that diagnosis filters are categorized correctly."""
        filters = {
            "diagnosis": "Neuroblastoma",
            "disease_phase": "Primary",
            "tumor_grade": "G2",
            "tumor_classification": "Primary",
            "tumor_tissue_morphology": "Neuroblastoma",
            "age_at_diagnosis": 5,
            "_diagnosis_search": "cancer"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        # CRITICAL: Verify "diagnosis" is categorized as diagnosis filter
        assert "diagnosis" in categorized["diagnosis"], "diagnosis filter must be categorized as diagnosis filter"
        assert "disease_phase" in categorized["diagnosis"]
        assert "tumor_grade" in categorized["diagnosis"]
        assert "tumor_classification" in categorized["diagnosis"]
        assert "tumor_tissue_morphology" in categorized["diagnosis"]
        assert "age_at_diagnosis" in categorized["diagnosis"]
        assert "_diagnosis_search" in categorized["diagnosis"]
        
        # Should not be in other categories
        assert len(categorized["sample"]) == 0
        assert len(categorized["study"]) == 0
        assert len(categorized["sequencing_file"]) == 0
        assert len(categorized["pathology_file"]) == 0
    
    def test_categorize_sequencing_file_filters(self):
        """Test that sequencing_file filters are categorized correctly."""
        filters = {
            "library_selection_method": "Hybrid Selection",
            "library_strategy": "WXS",
            "library_source_material": "Genomic DNA",
            "specimen_molecular_analyte_type": "DNA"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        assert "library_selection_method" in categorized["sequencing_file"]
        assert "library_strategy" in categorized["sequencing_file"]
        assert "library_source_material" in categorized["sequencing_file"]
        assert "specimen_molecular_analyte_type" in categorized["sequencing_file"]
        
        assert len(categorized["sample"]) == 0
        assert len(categorized["diagnosis"]) == 0
    
    def test_categorize_pathology_file_filters(self):
        """Test that pathology_file filters are categorized correctly."""
        filters = {
            "preservation_method": "FFPE"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        assert "preservation_method" in categorized["pathology_file"]
        assert len(categorized["sample"]) == 0
        assert len(categorized["diagnosis"]) == 0
    
    def test_categorize_mixed_filters(self):
        """Test categorization with filters from multiple categories."""
        filters = {
            "tissue_type": "Tumor",
            "diagnosis": "Neuroblastoma",
            "library_strategy": "WXS",
            "preservation_method": "FFPE",
            "depositions": "phs002431"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        assert "tissue_type" in categorized["sample"]
        assert "diagnosis" in categorized["diagnosis"]
        assert "library_strategy" in categorized["sequencing_file"]
        assert "preservation_method" in categorized["pathology_file"]
        assert "depositions" in categorized["study"]
    
    def test_categorize_diagnosis_filter_only(self):
        """Test that diagnosis filter alone is correctly categorized (routes to Case 3)."""
        filters = {
            "diagnosis": "Neuroblastoma"
        }
        
        categorized = SampleHelpers._categorize_filters(filters)
        
        # CRITICAL: diagnosis filter must be in diagnosis category
        assert "diagnosis" in categorized["diagnosis"], "diagnosis filter must be categorized as diagnosis filter for Case 3 routing"
        assert len(categorized["diagnosis"]) == 1
        assert len(categorized["sample"]) == 0
        assert len(categorized["study"]) == 0
        assert len(categorized["sequencing_file"]) == 0
        assert len(categorized["pathology_file"]) == 0
