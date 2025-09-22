"""
Field allowlist management for the CCDI Federation Service.

This module manages allowed fields for filtering and counting operations
to prevent unauthorized database access and Cypher injection.
"""

from typing import Dict, List, Set, Optional
from enum import Enum

from app.core.logging import get_logger

logger = get_logger(__name__)


class EntityType(Enum):
    """Supported entity types."""
    SUBJECT = "subject"
    SAMPLE = "sample" 
    FILE = "file"


# Harmonized fields allowlist based on OpenAPI specification
HARMONIZED_FIELDS: Dict[EntityType, Set[str]] = {
    EntityType.SUBJECT: {
        "sex",
        "race",
        "ethnicity", 
        "identifiers",
        "vital_status",
        "age_at_vital_status",
        "depositions"
    },
    EntityType.SAMPLE: {
        "disease_phase",
        "anatomical_sites",
        "library_selection_method",
        "library_strategy",
        "library_source_material",
        "preservation_method",
        "tumor_grade",
        "specimen_molecular_analyte_type",
        "tissue_type",
        "tumor_classification",
        "age_at_diagnosis",
        "age_at_collection",
        "tumor_tissue_morphology",
        "depositions",
        "diagnosis"
    },
    EntityType.FILE: {
        "type",
        "size",
        "checksums",
        "description",
        "depositions"
    }
}

# Common unharmonized field patterns (examples - should be loaded from metadata in practice)
COMMON_UNHARMONIZED_PATTERNS: Set[str] = {
    "study_id",
    "patient_id",
    "sample_id",
    "batch",
    "cohort",
    "platform",
    "instrument",
    "library_prep",
    "sequencing_center",
    "data_type",
    "experimental_strategy",
    "workflow_type"
}


class FieldAllowlist:
    """Manages allowed fields for database operations."""
    
    def __init__(self):
        """Initialize the field allowlist."""
        self._harmonized_fields = HARMONIZED_FIELDS.copy()
        self._unharmonized_fields: Dict[EntityType, Set[str]] = {
            EntityType.SUBJECT: set(),
            EntityType.SAMPLE: set(),
            EntityType.FILE: set()
        }
        self._loaded = False
    
    def is_harmonized_field_allowed(self, entity_type: EntityType, field: str) -> bool:
        """
        Check if a harmonized field is allowed for the given entity type.
        
        Args:
            entity_type: The entity type
            field: The field name
            
        Returns:
            True if field is allowed, False otherwise
        """
        return field in self._harmonized_fields.get(entity_type, set())
    
    def is_unharmonized_field_allowed(self, entity_type: EntityType, field: str) -> bool:
        """
        Check if an unharmonized field is allowed for the given entity type.
        
        Args:
            entity_type: The entity type
            field: The unharmonized field name (without metadata.unharmonized prefix)
            
        Returns:
            True if field is allowed, False otherwise
        """
        # If we haven't loaded from metadata repository, allow common patterns
        if not self._loaded:
            return field in COMMON_UNHARMONIZED_PATTERNS
        
        return field in self._unharmonized_fields.get(entity_type, set())
    
    def is_field_allowed(self, entity_type: EntityType, field: str) -> bool:
        """
        Check if a field (harmonized or unharmonized) is allowed.
        
        Args:
            entity_type: The entity type
            field: The field name (may include metadata.unharmonized prefix)
            
        Returns:
            True if field is allowed, False otherwise
        """
        # Check if it's an unharmonized field
        if field.startswith("metadata.unharmonized."):
            unharmonized_field = field.replace("metadata.unharmonized.", "")
            return self.is_unharmonized_field_allowed(entity_type, unharmonized_field)
        
        # Check harmonized fields
        return self.is_harmonized_field_allowed(entity_type, field)
    
    def get_allowed_harmonized_fields(self, entity_type: EntityType) -> List[str]:
        """
        Get all allowed harmonized fields for an entity type.
        
        Args:
            entity_type: The entity type
            
        Returns:
            List of allowed harmonized field names
        """
        return sorted(list(self._harmonized_fields.get(entity_type, set())))
    
    def get_allowed_unharmonized_fields(self, entity_type: EntityType) -> List[str]:
        """
        Get all allowed unharmonized fields for an entity type.
        
        Args:
            entity_type: The entity type
            
        Returns:
            List of allowed unharmonized field names (without prefix)
        """
        if not self._loaded:
            return sorted(list(COMMON_UNHARMONIZED_PATTERNS))
        
        return sorted(list(self._unharmonized_fields.get(entity_type, set())))
    
    def add_harmonized_field(self, entity_type: EntityType, field: str) -> None:
        """
        Add a harmonized field to the allowlist.
        
        Args:
            entity_type: The entity type
            field: The field name to add
        """
        if entity_type not in self._harmonized_fields:
            self._harmonized_fields[entity_type] = set()
        
        self._harmonized_fields[entity_type].add(field)
        logger.debug(f"Added harmonized field {field} for {entity_type.value}")
    
    def add_unharmonized_field(self, entity_type: EntityType, field: str) -> None:
        """
        Add an unharmonized field to the allowlist.
        
        Args:
            entity_type: The entity type
            field: The unharmonized field name (without prefix) to add
        """
        if entity_type not in self._unharmonized_fields:
            self._unharmonized_fields[entity_type] = set()
        
        self._unharmonized_fields[entity_type].add(field)
        logger.debug(f"Added unharmonized field {field} for {entity_type.value}")
    
    def load_from_database(self) -> None:
        """
        Load allowed fields from the metadata repository.
        
        This method should be called during application startup to populate
        the allowlist with fields discovered from the database.
        """
        # TODO: Implement database loading
        # This would query the metadata repository to discover available fields
        # For now, we'll use the common patterns
        
        for entity_type in EntityType:
            for field in COMMON_UNHARMONIZED_PATTERNS:
                self.add_unharmonized_field(entity_type, field)
        
        self._loaded = True
        logger.info("Loaded field allowlist from database")
    
    def validate_count_field(self, entity_type: EntityType, field: str) -> None:
        """
        Validate that a field is allowed for count operations.
        
        Args:
            entity_type: The entity type
            field: The field name
            
        Raises:
            ValueError: If field is not allowed
        """
        if not self.is_field_allowed(entity_type, field):
            raise ValueError(
                f"Field '{field}' is not supported for {entity_type.value} count operations"
            )
    
    def validate_filter_field(self, entity_type: EntityType, field: str) -> None:
        """
        Validate that a field is allowed for filter operations.
        
        Args:
            entity_type: The entity type
            field: The field name
            
        Raises:
            ValueError: If field is not allowed
        """
        if not self.is_field_allowed(entity_type, field):
            raise ValueError(
                f"Field '{field}' is not supported for {entity_type.value} filtering"
            )


# Global allowlist instance
_allowlist: Optional[FieldAllowlist] = None


def get_field_allowlist() -> FieldAllowlist:
    """Get the global field allowlist instance."""
    global _allowlist
    
    if _allowlist is None:
        _allowlist = FieldAllowlist()
        # Load from database during initialization
        _allowlist.load_from_database()
    
    return _allowlist
