"""
Sample repository data conversion methods.

This module contains methods for converting database records to Sample objects.
These methods are provided as a mixin class that can be inherited by SampleRepository.
"""

from typing import Dict, Any, Optional
from app.core.logging import get_logger
from app.models.dto import Sample
from app.core.field_mappings import map_field_value, reverse_map_field_value
from app.core.serialization import convert_date_time_to_string

logger = get_logger(__name__)


def node_to_dict(node):
    """Convert a Node object to a dictionary.
    
    Utility function to convert Neo4j/Memgraph Node objects to dictionaries.
    Used throughout the repository for consistent node conversion.
    
    Handles date/time objects (ZONED_DATE_TIME, LocalDateTime, etc.) by converting
    them to ISO format strings to prevent serialization errors.
    
    Args:
        node: Node object from Neo4j/Memgraph, dict, or None
        
    Returns:
        Dictionary representation of the node, or empty dict if None
        Date/time properties are converted to ISO format strings
    """
    if node is None:
        return {}
    if isinstance(node, dict):
        # Still need to convert date/time values in dict
        return {k: convert_date_time_to_string(v) for k, v in node.items()}
    
    # Try dict() conversion first (works for Neo4j/Memgraph Node objects)
    try:
        node_dict = dict(node)
        # Convert date/time values in the dictionary
        return {k: convert_date_time_to_string(v) for k, v in node_dict.items()}
    except (TypeError, ValueError):
        # Fall back to accessing properties
        if hasattr(node, 'properties'):
            props = node.properties
            if isinstance(props, dict):
                return {k: convert_date_time_to_string(v) for k, v in props.items()}
            return props
        elif hasattr(node, 'items'):
            node_dict = dict(node.items())
            return {k: convert_date_time_to_string(v) for k, v in node_dict.items()}
        else:
            # Last resort: return empty dict to avoid expensive dir() call
            # If node conversion fails, return empty dict rather than scanning all attributes
            return {}


class SampleConverters:
    """Mixin class providing data conversion methods for SampleRepository."""
    
    def _record_to_sample(
        self, 
        sa: Dict[str, Any], 
        p: Dict[str, Any], 
        st: Dict[str, Any], 
        sf: Dict[str, Any], 
        pf: Dict[str, Any], 
        diagnoses: Optional[Dict[str, Any]],
        base_url: Optional[str] = None
    ) -> Sample:
        """
        Convert database records to a Sample object with proper field mappings.
        
        Args:
            sa: Sample node dictionary
            p: Participant node dictionary
            st: Study node dictionary
            sf: Sequencing file node dictionary
            pf: Pathology file node dictionary
            diagnoses: Single diagnosis node dictionary (or None)
            
        Returns:
            Sample object with proper structure
        """
        from app.models.dto import (
            Sample, SampleIdentifier, NamespaceIdentifier, SubjectId,
            SampleMetadata, DiagnosisField
        )
        
        # Build sample ID: namespace from study, name from sample_id
        # Handle case where sa might be empty or None
        if not sa:
            logger.warning("Sample node (sa) is empty or None, skipping record")
            raise ValueError("Sample node (sa) is required but was empty or None")
        
        # Try to get study_id from multiple sources
        study_id = ""
        if st and isinstance(st, dict):
            study_id = st.get("study_id", "")
        
        # If study_id is still empty, try to get it from the sample node itself
        if not study_id and sa and isinstance(sa, dict):
            study_id = sa.get("study_id", "")
        
        # If still empty, try to get it from participant
        if not study_id and p and isinstance(p, dict):
            study_id = p.get("study_id", "")
        
        sample_id = sa.get("sample_id", "") if isinstance(sa, dict) else ""
        
        # If sample_id is not available, try other possible fields
        if not sample_id:
            sample_id = sa.get("id", "") if isinstance(sa, dict) else ""
        if not sample_id:
            sample_id = sa.get("name", "") if isinstance(sa, dict) else ""
        
        # Validate required fields - both study_id and sample_id are required
        if not study_id or not study_id.strip():
            logger.warning(
                "Sample record missing study_id (namespace), skipping",
                sa_keys=list(sa.keys()) if isinstance(sa, dict) else [],
                st_keys=list(st.keys()) if st and isinstance(st, dict) else [],
                p_keys=list(p.keys()) if p and isinstance(p, dict) else [],
                sample_id=sample_id
            )
            raise ValueError(f"Sample record missing required study_id (namespace). Sample ID: {sample_id}")
        
        if not sample_id or not sample_id.strip():
            logger.warning(
                "Sample node missing sample_id field, skipping",
                sa_keys=list(sa.keys()) if isinstance(sa, dict) else [],
                study_id=study_id
            )
            raise ValueError(f"Sample record missing required sample_id. Study ID: {study_id}")
        
        # Create namespace and sample identifier
        # Pass as dictionary to avoid Pydantic validation issues
        sample_identifier = SampleIdentifier(
            namespace={
                "organization": "CCDI-DCC",
                "name": study_id.strip()
            },
            name=sample_id.strip()
        )
        
        # Build subject reference: name from participant, namespace from study
        subject = None
        if p and isinstance(p, dict) and st and isinstance(st, dict):
            participant_id = str(p.get("participant_id", ""))
            if not participant_id or participant_id == "":
                participant_id = str(p.get("id", "")) if isinstance(p, dict) else ""
            if participant_id:
                subject_namespace = NamespaceIdentifier(
                    organization="CCDI-DCC",
                    name=study_id
                )
                subject = SubjectId(
                    namespace=subject_namespace,
                    name=participant_id
                )
        
        # Build metadata with proper field mappings
        def _null_if_invalid(value):
            """Replace 'Invalid value' with None, and -999 with None."""
            if value is None:
                return None
            if isinstance(value, (int, float)) and value == -999:
                return None
            if isinstance(value, str) and value.strip().lower() == "invalid value":
                return None
            # Handle arrays/lists - filter out "Invalid value" entries
            if isinstance(value, (list, tuple)):
                filtered = [v for v in value if v is not None and str(v).strip().lower() != "invalid value"]
                # Return None if all values were invalid, otherwise return the filtered list
                return filtered if filtered else None
            return value
        
        def _null_if_neg999(value):
            """Replace -999 with None, return value as-is (not converted to string)."""
            if value is None:
                return None
            if isinstance(value, (int, float)) and value == -999:
                return None
            return value  # Return as-is, not as string
        
        # Get depositions from study - format as objects with kind and value
        depositions = None
        if st and isinstance(st, dict) and st.get("study_id"):
            study_id = st.get("study_id")
            if study_id:
                depositions = [{"kind": "dbGaP", "value": study_id}]
        
        # Build diagnosis field
        # If empty data, return null; otherwise return {value: diagnosis, comment: diagnosis_comment}
        diagnosis_field = None
        if diagnoses and isinstance(diagnoses, dict):
            # diagnoses is now a single node (or None)
            diagnosis_value = diagnoses.get("diagnosis")
            diagnosis_comment = diagnoses.get("diagnosis_comment")
            
            # Check if diagnosis_value is empty/null/whitespace
            if diagnosis_value and str(diagnosis_value).strip():
                # Has diagnosis value - return object with value and comment
                diagnosis_field = DiagnosisField(
                    value=str(diagnosis_value).strip(),
                    comment=str(diagnosis_comment).strip() if diagnosis_comment and str(diagnosis_comment).strip() else None
                )
            # If diagnosis_value is empty/null/whitespace, diagnosis_field remains None (returns null)
        
        # Helper function to wrap value in ValueField if not None and not empty
        def _wrap_value(value):
            """Wrap value in ValueField if not None and not empty, otherwise return None."""
            if value is None:
                return None
            # Convert to string and check if it's empty or just whitespace
            str_value = str(value).strip()
            if not str_value:
                return None
            from app.models.dto import ValueField
            return ValueField(value=str_value)
        
        def _wrap_list_value(value_list):
            """Wrap list of values in list of ValueField objects if not None and not empty, otherwise return None."""
            if value_list is None or not isinstance(value_list, list) or len(value_list) == 0:
                return None
            from app.models.dto import ValueField
            # Filter out empty strings and create ValueField for each valid value
            wrapped = [ValueField(value=str(v).strip()) for v in value_list if v is not None and str(v).strip()]
            return wrapped if wrapped else None
        
        def _map_library_selection_method(db_value):
            """Map database value to API value for library_selection_method.
            
            Uses centralized field mappings from config_data/field_mappings.json.
            """
            return map_field_value("library_selection_method", db_value)
        
        def _reverse_map_library_selection_method(api_value):
            """Reverse map API value to database value for library_selection_method.
            
            Used for filtering - maps API values back to DB values.
            Uses centralized field mappings from config_data/field_mappings.json.
            """
            result = reverse_map_field_value("library_selection_method", api_value)
            # reverse_map_field_value can return a list, but for library_selection_method it should be a string
            return result if isinstance(result, str) else (result[0] if isinstance(result, list) and result else None)
        
        
        # Helper function to wrap integer value in IntegerValueField if not None
        def _wrap_integer_value(value):
            """Wrap integer value in IntegerValueField if not None, otherwise return None."""
            if value is None:
                return None
            # Convert to int, handling both int and float values
            try:
                int_value = int(float(value))  # Convert float to int (e.g., 10.0 -> 10)
                from app.models.dto import IntegerValueField
                return IntegerValueField(value=int_value)
            except (ValueError, TypeError):
                return None
        
        # Helper function to handle anatomical_sites (may be array or string)
        def _process_anatomical_sites(value):
            """Process anatomical_sites - handle arrays and strings, return list of all valid values."""
            if value is None:
                return None
            result = []
            # If it's an array/list, process each value
            if isinstance(value, (list, tuple)):
                for v in value:
                    if v is not None and str(v).strip() != "" and str(v).strip().lower() != "invalid value":
                        result.append(str(v).strip())
            # If it's a string, check if it's semicolon-separated or a single value
            elif isinstance(value, str):
                value_stripped = value.strip()
                if value_stripped and value_stripped.lower() != "invalid value":
                    # Check if it contains semicolons (semicolon-separated values)
                    if ';' in value_stripped:
                        # Split by semicolon and process each part
                        parts = value_stripped.split(';')
                        for part in parts:
                            part_stripped = part.strip()
                            if part_stripped and part_stripped.lower() != "invalid value":
                                result.append(part_stripped)
                    else:
                        # Single value
                        result.append(value_stripped)
            else:
                # For other types, convert to string and check
                value_str = str(value).strip() if value else ""
                if value_str and value_str.lower() != "invalid value":
                    result.append(value_str)
            # Return None if no valid values, otherwise return the list
            return result if result else None
        
        # Build identifiers - reference the subject (participant)
        identifiers = None
        # Ensure we have study_id from st if not already set
        if not study_id and st and isinstance(st, dict):
            study_id = st.get("study_id", "")
        
        if p and isinstance(p, dict) and study_id and sample_id:
            participant_id = str(p.get("participant_id", ""))
            if not participant_id or participant_id == "":
                participant_id = str(p.get("id", "")) if isinstance(p, dict) else ""

            if participant_id and study_id and sample_id:
                from app.models.dto import IdentifierField, IdentifierValue
                
                # Build server URL - format: /api/v1/sample/CCDI-DCC/{study_id}/{sample_id}
                # Note: This format doesn't include entity type, matching user's example
                server_url = None
                if base_url:
                    server_url = f"{base_url}/api/v1/sample/CCDI-DCC/{study_id}/{sample_id}"
                
                identifier_value = IdentifierValue(
                    namespace={
                        "organization": "CCDI-DCC",
                        "name": study_id
                    },
                    name=sample_id,
                    type="Linked",
                    server=server_url
                )
                identifiers = [IdentifierField(value=identifier_value)]
            else:
                logger.debug(
                    "Cannot build identifier - missing participant_id, study_id, or sample_id",
                    has_participant_id=bool(participant_id),
                    has_study_id=bool(study_id),
                    has_sample_id=bool(sample_id),
                    p_keys=list(p.keys()) if isinstance(p, dict) else []
                )
        else:
            logger.debug(
                "Cannot build identifier - missing participant, study, or sample_id",
                has_p=bool(p),
                p_is_dict=isinstance(p, dict) if p else False,
                has_st=bool(st),
                study_id=study_id,
                sample_id=sample_id
            )
        
        # Build metadata with updated field mappings
        # disease_phase: d.disease_phase (from diagnoses, not sa)
        disease_phase_value = None
        if diagnoses and isinstance(diagnoses, dict):
            disease_phase_value = diagnoses.get("disease_phase")
        
        # anatomical_sites: sa.anatomic_site - "Invalid value" to be replaced with null (already handled in _process_anatomical_sites)
        anatomical_sites_value = sa.get("anatomic_site") if sa else None
        
        # library_selection_method: sf.library_selection
        library_selection_value = sf.get("library_selection") if sf else None
        
        # library_strategy: sf.library_strategy
        library_strategy_value = sf.get("library_strategy") if sf else None
        
        # library_source_material: sf.library_source_material
        library_source_material_value = sf.get("library_source_material") if sf else None
        
        # specimen_molecular_analyte_type: sf.library_source_molecule
        specimen_molecular_analyte_type_value = sf.get("library_source_molecule") if sf else None
        
        # preservation_method: pf.fixation_embedding_method
        preservation_method_value = pf.get("fixation_embedding_method") if pf else None
        
        # tumor_grade: d.tumor_grade
        tumor_grade_value = None
        if diagnoses and isinstance(diagnoses, dict):
            tumor_grade_value = diagnoses.get("tumor_grade")
        
        # age_at_diagnosis: d.age_at_diagnosis or null if -999
        age_at_diagnosis_value = None
        if diagnoses and isinstance(diagnoses, dict):
            age_at_diagnosis_value = diagnoses.get("age_at_diagnosis")
        
        # age_at_collection: sa.participant_age_at_collection or null if -999
        age_at_collection_value = sa.get("participant_age_at_collection") if sa else None
        
        # tumor_classification: d.tumor_classification
        tumor_classification_value = None
        if diagnoses and isinstance(diagnoses, dict):
            tumor_classification_value = diagnoses.get("tumor_classification")
        
        # tissue_type: sa.sample_tumor_status (mapped from sample_tumor_status field)
        tissue_type_value = sa.get("sample_tumor_status") if sa else None
        
        # Build metadata with field mappings applied
        metadata = SampleMetadata(
            disease_phase=_wrap_value(map_field_value("disease_phase", _null_if_invalid(disease_phase_value))),
            anatomical_sites=_wrap_list_value(_process_anatomical_sites(anatomical_sites_value)),
            library_selection_method=_wrap_value(_map_library_selection_method(_null_if_invalid(library_selection_value))),
            library_strategy=_wrap_value(map_field_value("library_strategy", _null_if_invalid(library_strategy_value))),
            library_source_material=_wrap_value(map_field_value("library_source_material", _null_if_invalid(library_source_material_value))),
            preservation_method=_wrap_value(_null_if_invalid(preservation_method_value)),
            tumor_grade=_wrap_value(_null_if_invalid(tumor_grade_value)),
            specimen_molecular_analyte_type=_wrap_value(map_field_value("specimen_molecular_analyte_type", _null_if_invalid(specimen_molecular_analyte_type_value))),
            tissue_type=_wrap_value(_null_if_invalid(tissue_type_value)),
            tumor_classification=_wrap_value(map_field_value("tumor_classification", _null_if_invalid(tumor_classification_value))),
            age_at_diagnosis=_wrap_integer_value(_null_if_neg999(age_at_diagnosis_value)),
            age_at_collection=_wrap_integer_value(_null_if_neg999(age_at_collection_value)),
            tumor_tissue_morphology=None,  # Not in the provided mapping
            depositions=depositions,
            diagnosis=diagnosis_field,
            identifiers=identifiers
        )
        
        # Create Sample object
        sample = Sample(
            id=sample_identifier,
            subject=subject,
            metadata=metadata
        )
        
        return sample
