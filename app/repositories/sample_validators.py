"""
Sample repository validators and helper methods.

This module contains validation and helper methods used by SampleRepository.
These methods are provided as a mixin class that can be inherited by SampleRepository.
"""

from typing import Dict, Any, List, Optional
from app.core.logging import get_logger
from app.core.field_mappings import (
    load_sample_enum,
    load_sequencing_file_enum,
    reverse_map_field_value,
    is_null_mapped_value
)
from app.models.errors import UnsupportedFieldError
from app.lib.field_allowlist import FieldAllowlist

logger = get_logger(__name__)


class SampleValidators:
    """Mixin class providing validation methods for SampleRepository."""
    
    @staticmethod
    def _reverse_map_library_selection_method_static(api_value):
        """Reverse map API value to database value for library_selection_method.
        
        Used for filtering - maps API values back to DB values.
        Uses centralized field mappings from config_data/field_mappings.json.
        """
        result = reverse_map_field_value("library_selection_method", api_value)
        # reverse_map_field_value can return a list, but for library_selection_method it should be a string
        return result if isinstance(result, str) else (result[0] if isinstance(result, list) and result else None)
    
    @staticmethod
    def _validate_tissue_type_filter(
        value: Any,
        param_name: str,
        params: Dict[str, Any],
        with_conditions: List[Any]
    ) -> Optional[bool]:
        """
        Validate tissue_type filter value against sample_tumor_status enum.
        
        This helper function centralizes the tissue_type validation logic that was
        duplicated across get_samples and get_samples_summary methods.
        
        Args:
            value: Filter value (string or list of strings)
            param_name: Parameter name for Cypher query
            params: Parameters dictionary to update
            with_conditions: List of WHERE conditions to update
            
        Returns:
            None if validation fails (caller should return empty results)
            True if validation succeeds (condition and param have been added)
        """
        # Validate against sample_tumor_status enum
        valid_values = load_sample_enum("sample_tumor_status")
        if valid_values:
            # Handle both string and list values (defensive programming)
            if isinstance(value, list):
                # Empty list should return None (no matches possible)
                if not value:
                    return None
                # If it's a list, validate each value
                invalid_values = [v for v in value if v not in valid_values]
                if invalid_values:
                    # At least one value is invalid - return empty results immediately
                    logger.info(
                        "tissue_type filter contains invalid enum values - returning empty results",
                        tissue_type_value=value,
                        invalid_values=invalid_values,
                        valid_values=valid_values
                    )
                    return None
                # All values are valid - use IN clause for list
                # No need to filter NULL/empty/-999 - enum validation ensures only valid values
                with_conditions.append(f"sa.sample_tumor_status IN ${param_name}")
                params[param_name] = value
            else:
                # Single string value
                if value not in valid_values:
                    # Value doesn't match any enum value - return empty results immediately
                    logger.info(
                        "tissue_type filter value does not match any enum value - returning empty results",
                        tissue_type_value=value,
                        valid_values=valid_values
                    )
                    return None
                # Value is valid - add filter condition
                # No need to filter NULL/empty/-999 - enum validation ensures only valid values
                with_conditions.append(f"sa.sample_tumor_status = ${param_name}")
                params[param_name] = value
        else:
            # Enum not available - fallback to direct comparison (shouldn't happen in production)
            logger.warning("sample_tumor_status enum not available, using direct comparison")
            # No need to filter NULL/empty/-999 - enum validation ensures only valid values
            with_conditions.append(f"sa.sample_tumor_status = ${param_name}")
            params[param_name] = value
        
        return True
    
    @staticmethod
    def _get_next_param_name(params: Dict[str, Any], param_counter: int) -> str:
        """
        Get the next available parameter name to avoid conflicts.
        
        Finds the highest parameter number currently in use and returns
        a new parameter name with the next number.
        
        Args:
            params: Dictionary of current parameters
            param_counter: Current parameter counter value
            
        Returns:
            Next available parameter name (e.g., "param_8")
        """
        max_param_num = param_counter
        for key in params.keys():
            if key.startswith("param_"):
                try:
                    num = int(key.split("_")[1])
                    max_param_num = max(max_param_num, num)
                except (ValueError, IndexError):
                    pass
        return f"param_{max_param_num + 1}"
    
    @staticmethod
    def _validate_library_source_material_filter(
        value: Any,
        param_name: str,
        params: Dict[str, Any],
        with_conditions: List[Any]
    ) -> Optional[bool]:
        """
        Validate library_source_material filter value with enum check and reverse mapping.
        
        This helper function centralizes the library_source_material validation logic that was
        duplicated across get_samples and get_samples_summary methods.
        
        Args:
            value: Filter value (string)
            param_name: Parameter name for Cypher query
            params: Parameters dictionary to update
            with_conditions: List of WHERE conditions to update (uses tuple format)
            
        Returns:
            None if validation fails (value is in null_mappings - caller should handle invalid case)
            True if validation succeeds (condition and param have been added)
        """
        # Check if value is in null_mappings (e.g., "Other")
        # Values in null_mappings are treated as missing and should not match any records
        if is_null_mapped_value("library_source_material", value):
            # This value is treated as NULL/missing and is not valid for filtering
            # Add an impossible condition to return empty results
            with_conditions.append(("library_source_material_invalid", "invalid"))
            return None
        
        # Load enum values and use IN clause for filtering
        enum_values = load_sequencing_file_enum("library_source_material")
        if enum_values:
            # Apply reverse mapping for the filter value to get DB value
            reverse_mapped = reverse_map_field_value("library_source_material", value)
            # Use IN clause with the mapped DB value (as list for consistency)
            params[param_name] = [reverse_mapped] if reverse_mapped else [value]
            with_conditions.append(("library_source_material", param_name))
        else:
            # Fallback to original logic if enum not available
            reverse_mapped = reverse_map_field_value("library_source_material", value)
            params[param_name] = reverse_mapped
            with_conditions.append(("library_source_material", param_name))
        
        return True
    
    def _build_sex_normalization_case(self, field: str) -> str:
        """
        Build Cypher CASE statement for sex value normalization from config.
        
        Args:
            field: Field name to check
            
        Returns:
            Cypher CASE statement string for sex normalization, or empty string if not sex field
        """
        if field != "sex" or not self.settings or not hasattr(self.settings, 'sex_value_mappings'):
            return ""
        
        mappings = self.settings.sex_value_mappings
        if not mappings:
            return ""
        
        # Build CASE statement from config mappings
        case_parts = []
        for db_value, normalized_value in mappings.items():
            case_parts.append(f"WHEN toString(value) = '{db_value}' OR toString(value) = '{normalized_value}' THEN '{normalized_value}'")
        
        # Default fallback (prefer 'U' for unknown/Not Reported if available)
        default_value = mappings.get("Not Reported", "U") if "Not Reported" in mappings else list(mappings.values())[0] if mappings else "U"
        case_parts.append(f"ELSE '{default_value}'")
        
        return f"CASE {' '.join(case_parts)} END"
    
    def _validate_filters(self, filters: Dict[str, Any], entity_type: str) -> None:
        """
        Validate that all filter fields are allowed.
        
        Args:
            filters: Dictionary of filters to validate
            entity_type: Type of entity for allowlist checking
            
        Raises:
            UnsupportedFieldError: If any field is not allowed
        """
        for field in filters.keys():
            # Skip special fields
            if field.startswith("_"):
                continue
                
            if not self.allowlist.is_field_allowed(entity_type, field):
                # Log the invalid field but don't include it in the error message
                logger.warning(
                    "Unsupported field in filter",
                    field=field,
                    entity_type=entity_type
                )
                raise UnsupportedFieldError(field, entity_type)
