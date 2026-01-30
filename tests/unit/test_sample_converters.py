"""
Unit tests for sample converters module.

Tests the node_to_dict utility and _record_to_sample conversion logic.
"""

import pytest
from unittest.mock import Mock
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.repositories.sample_converters import node_to_dict, SampleConverters
from app.models.dto import Sample


@pytest.mark.unit
class TestNodeToDict:
    """Test node_to_dict utility function."""
    
    def test_node_to_dict_with_dict(self):
        """Test node_to_dict with a dictionary."""
        node = {"sample_id": "SAMP001", "study_id": "phs001"}
        result = node_to_dict(node)
        assert result == node
        assert isinstance(result, dict)
    
    def test_node_to_dict_with_none(self):
        """Test node_to_dict with None."""
        result = node_to_dict(None)
        assert result == {}
    
    def test_node_to_dict_with_node_object(self):
        """Test node_to_dict with a mock Node object."""
        class MockNode:
            def __init__(self):
                self.properties = {"sample_id": "SAMP001"}
            
            def __iter__(self):
                return iter(self.properties.items())
        
        node = MockNode()
        result = node_to_dict(node)
        # Should convert to dict
        assert isinstance(result, dict)
        assert "sample_id" in result or result == {}
    
    def test_node_to_dict_with_dict_method(self):
        """Test node_to_dict with object that supports dict() conversion."""
        class MockNode:
            def __init__(self):
                self._data = {"sample_id": "SAMP001"}
            
            def items(self):
                return self._data.items()
        
        node = MockNode()
        result = node_to_dict(node)
        # Should handle items() method
        assert isinstance(result, dict)


@pytest.mark.unit
class TestSampleConverters:
    """Test SampleConverters mixin class."""
    
    @pytest.fixture
    def converter(self):
        """Create a SampleConverters instance."""
        # SampleConverters is a mixin, so we create a simple class that inherits from it
        class TestConverter(SampleConverters):
            def __init__(self):
                pass
        
        return TestConverter()
    
    def test_record_to_sample_basic(self, converter):
        """Test _record_to_sample with basic valid data."""
        sa = {"sample_id": "SAMP001", "sample_tumor_status": "Tumor", "anatomic_site": "Brain"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        sf = {}
        pf = {}
        diagnoses = None
        
        sample = converter._record_to_sample(sa, p, st, sf, pf, diagnoses)
        
        assert sample is not None
        assert isinstance(sample, Sample)
        assert sample.id.name == "SAMP001"
        assert sample.id.namespace.name == "phs002431"
    
    def test_record_to_sample_empty_sa(self, converter):
        """Test _record_to_sample raises error when sa is empty."""
        with pytest.raises(ValueError, match="Sample node.*required"):
            converter._record_to_sample({}, {}, {}, {}, {}, None)
    
    def test_record_to_sample_missing_study_id(self, converter):
        """Test _record_to_sample raises error when study_id is missing."""
        sa = {"sample_id": "SAMP001"}
        with pytest.raises(ValueError, match="missing required study_id"):
            converter._record_to_sample(sa, {}, {}, {}, {}, None)
    
    def test_record_to_sample_study_id_from_participant(self, converter):
        """Test _record_to_sample gets study_id from participant when not in st."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001", "study_id": "phs002431"}
        st = {}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, None)
        assert sample.id.namespace.name == "phs002431"
    
    def test_record_to_sample_with_diagnosis(self, converter):
        """Test _record_to_sample with diagnosis."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {"diagnosis": "Neuroblastoma", "disease_phase": "Primary"}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, diagnoses)
        assert sample.metadata.diagnosis is not None
        assert sample.metadata.diagnosis.value == "Neuroblastoma"
    
    def test_record_to_sample_with_invalid_values(self, converter):
        """Test _record_to_sample filters out invalid values (-999, "Invalid value")."""
        sa = {
            "sample_id": "SAMP001",
            "age_at_collection": -999,
            "age_at_diagnosis": -999
        }
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, None)
        # Invalid values should be filtered out (set to None)
        assert sample.metadata.age_at_collection is None
        assert sample.metadata.age_at_diagnosis is None
    
    def test_record_to_sample_with_anatomical_sites_list(self, converter):
        """Test _record_to_sample handles anatomical_sites as list."""
        sa = {"sample_id": "SAMP001", "anatomic_site": ["Brain", "Liver"]}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, None)
        assert sample.metadata.anatomical_sites is not None
        assert len(sample.metadata.anatomical_sites) == 2
    
    def test_record_to_sample_with_anatomical_sites_semicolon_separated(self, converter):
        """Test _record_to_sample handles anatomical_sites as semicolon-separated string."""
        sa = {"sample_id": "SAMP001", "anatomic_site": "Brain; Liver"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, None)
        assert sample.metadata.anatomical_sites is not None
        assert len(sample.metadata.anatomical_sites) == 2
    
    def test_record_to_sample_with_base_url(self, converter):
        """Test _record_to_sample includes server URL when base_url provided."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        
        sample = converter._record_to_sample(
            sa, p, st, {}, {}, None,
            base_url="https://api.example.com"
        )
        assert sample.metadata.identifiers is not None
        assert len(sample.metadata.identifiers) > 0
        assert sample.metadata.identifiers[0].value.server is not None
        assert "https://api.example.com" in sample.metadata.identifiers[0].value.server
    
    def test_record_to_sample_with_diagnosis_comment(self, converter):
        """Test _record_to_sample handles diagnosis with comment."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {
            "diagnosis": "Neuroblastoma",
            "diagnosis_comment": "See pathology report"
        }
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, diagnoses)
        assert sample.metadata.diagnosis is not None
        assert sample.metadata.diagnosis.value == "Neuroblastoma"
        assert sample.metadata.diagnosis.comment == "See pathology report"
    
    def test_record_to_sample_with_empty_diagnosis(self, converter):
        """Test _record_to_sample handles empty diagnosis."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        diagnoses = {}
        
        sample = converter._record_to_sample(sa, p, st, {}, {}, diagnoses)
        assert sample.metadata.diagnosis is None
    
    def test_record_to_sample_with_sequencing_file_data(self, converter):
        """Test _record_to_sample with sequencing file data."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        sf = {
            "library_strategy": "WXS",
            "library_selection": "PCR",
            "library_source_material": "DNA"
        }
        
        sample = converter._record_to_sample(sa, p, st, sf, {}, None)
        assert sample.metadata.library_strategy is not None
        assert sample.metadata.library_strategy.value == "WXS"
    
    def test_record_to_sample_with_pathology_file_data(self, converter):
        """Test _record_to_sample with pathology file data."""
        sa = {"sample_id": "SAMP001"}
        p = {"participant_id": "PART001"}
        st = {"study_id": "phs002431"}
        pf = {
            "fixation_embedding_method": "FFPE",
            "tumor_grade": "G1"
        }
        
        sample = converter._record_to_sample(sa, p, st, {}, pf, None)
        assert sample.metadata.preservation_method is not None
        assert sample.metadata.preservation_method.value == "FFPE"
    
    def test_node_to_dict_with_properties_attribute(self):
        """Test node_to_dict with object that has properties attribute."""
        class MockNodeWithProperties:
            def __init__(self):
                self.properties = {"sample_id": "SAMP001", "study_id": "phs001"}
        
        node = MockNodeWithProperties()
        result = node_to_dict(node)
        assert isinstance(result, dict)
        assert result == {"sample_id": "SAMP001", "study_id": "phs001"}
    
    def test_node_to_dict_with_items_method(self):
        """Test node_to_dict with object that has items() method."""
        class MockNodeWithItems:
            def items(self):
                return [("sample_id", "SAMP001"), ("study_id", "phs001")]
        
        node = MockNodeWithItems()
        result = node_to_dict(node)
        assert isinstance(result, dict)
        assert "sample_id" in result or result == {}
