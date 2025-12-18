"""
Constants and enumerations for data validation.

This module contains valid values and enums for various data fields used throughout
the application, such as race, ethnicity, sex, etc.
"""

import json
from enum import Enum
from pathlib import Path


class Race(str, Enum):
    """
    Valid race values for participants.
    
    This enum represents the standard race categories used in the CCDI-DCC
    federation service. All race values in the system should conform to one
    of these values.
    """
    NOT_ALLOWED_TO_COLLECT = "Not allowed to collect"
    NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = "Native Hawaiian or other Pacific Islander"
    NOT_REPORTED = "Not Reported"
    UNKNOWN = "Unknown"
    AMERICAN_INDIAN_OR_ALASKA_NATIVE = "American Indian or Alaska Native"
    ASIAN = "Asian"
    BLACK_OR_AFRICAN_AMERICAN = "Black or African American"
    WHITE = "White"

    @classmethod
    def values(cls) -> list[str]:
        """
        Get a list of all valid race values as strings.
        
        Returns:
            List of all valid race values
        """
        return [race.value for race in cls]

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """
        Check if a given value is a valid race value.
        
        Args:
            value: The race value to validate
            
        Returns:
            True if the value is valid, False otherwise
        """
        return value in cls.values()


class Ethnicity(str, Enum):
    """
    Valid ethnicity values for participants.
    
    This enum represents the standard ethnicity categories used in the CCDI-DCC
    federation service. All ethnicity values in the system should conform to one
    of these values.
    """
    HISPANIC_OR_LATINO = "Hispanic or Latino"
    NOT_HISPANIC_OR_LATINO = "Not reported"

    @classmethod
    def values(cls) -> list[str]:
        """
        Get a list of all valid ethnicity values as strings.
        
        Returns:
            List of all valid ethnicity values
        """
        return [ethnicity.value for ethnicity in cls]

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """
        Check if a given value is a valid ethnicity value.
        
        Args:
            value: The ethnicity value to validate
            
        Returns:
            True if the value is valid, False otherwise
        """
        return value in cls.values()


class VitalStatus(str, Enum):
    """
    Valid vital status values for participants.
    
    This enum represents the standard vital status categories used in the CCDI-DCC
    federation service. All vital status values in the system should conform to one
    of these values.
    """
    ALIVE = "Alive"
    DEAD = "Dead"
    NOT_REPORTED = "Not reported"
    UNKNOWN = "Unknown"
    UNSPECIFIED = "Unspecified"

    @classmethod
    def values(cls) -> list[str]:
        """
        Get a list of all valid vital status values as strings.
        
        Returns:
            List of all valid vital status values
        """
        return [vital_status.value for vital_status in cls]

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """
        Check if a given value is a valid vital status value.
        
        Args:
            value: The vital status value to validate
            
        Returns:
            True if the value is valid, False otherwise
        """
        return value in cls.values()


def load_file_enum() -> list[str]:
    """
    Load file type enum values from config_data/file_type_enum.json.
    
    The file structure is: { "file_type": ["value1", "value2", ...] }
    Also supports legacy format: ["value1", "value2", ...] (for backward compatibility)
    
    Returns:
        List of file type strings. Returns empty list if file not found or invalid.
    """
    # From app/core/constants.py, go up 1 level to reach app/, then config_data/
    file_enum_path = Path(__file__).resolve().parents[1] / "config_data" / "file_type_enum.json"
    
    try:
        with file_enum_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            # New format: { "file_type": [...] }
            if isinstance(data, dict) and "file_type" in data:
                file_types = data["file_type"]
                if isinstance(file_types, list):
                    return file_types
            # Legacy format: [...] (for backward compatibility)
            elif isinstance(data, list):
                return data
            return []
    except (FileNotFoundError, json.JSONDecodeError):
        # Return empty list if file doesn't exist or is invalid
        return []


# Load file types from config file
_file_type_values = load_file_enum()


def _to_enum_name(value: str) -> str:
    """
    Convert a file type string value to a valid Python enum member name.
    
    Args:
        value: The file type string value
        
    Returns:
        A valid Python identifier for use as an enum member name
    """
    # Replace spaces, slashes, and special characters with underscores
    name = value.replace(" ", "_").replace("/", "_").replace("-", "_")
    # Remove any remaining special characters and ensure it starts with a letter
    name = "".join(c if c.isalnum() or c == "_" else "" for c in name)
    # Ensure it starts with a letter or underscore
    if name and not (name[0].isalpha() or name[0] == "_"):
        name = "_" + name
    # Handle empty names
    if not name:
        name = "UNNAMED"
    # Convert to uppercase for enum naming convention
    return name.upper()


# Dynamically create FileType enum from loaded values
if _file_type_values:
    # Create enum members dictionary: {enum_name: enum_value}
    enum_members = {_to_enum_name(ft): ft for ft in _file_type_values}
    
    # Create the enum using functional API
    FileType = Enum("FileType", enum_members, type=str)
    
    # Add helper methods to the dynamically created enum
    def values(cls) -> list[str]:
        """
        Get a list of all valid file type values as strings.
        
        Returns:
            List of all valid file type values
        """
        return [file_type.value for file_type in cls]
    
    def is_valid(cls, value: str) -> bool:
        """
        Check if a given value is a valid file type value.
        
        Args:
            value: The file type value to validate
            
        Returns:
            True if the value is valid, False otherwise
        """
        return value in cls.values()
    
    # Attach methods as classmethods to the enum class
    FileType.values = classmethod(values)
    FileType.is_valid = classmethod(is_valid)
    
    # Add docstring
    FileType.__doc__ = """
    Valid file type values for sequencing files.
    
    This enum represents the standard file type categories used in the CCDI-DCC
    federation service. All file type values in the system should conform to one
    of these values.
    
    Note: File types are loaded from config_data/file_type_enum.json.
    """
else:
    # Fallback: create empty enum if file couldn't be loaded
    class FileType(str, Enum):
        """
        Valid file type values for sequencing files.
        
        This enum represents the standard file type categories used in the CCDI-DCC
        federation service. All file type values in the system should conform to one
        of these values.
        
        Note: File types are loaded from config_data/file_type_enum.json.
        If the file cannot be loaded, this enum will be empty.
        """
        
        @classmethod
        def values(cls) -> list[str]:
            """
            Get a list of all valid file type values as strings.
            
            Returns:
                List of all valid file type values
            """
            return [file_type.value for file_type in cls]

        @classmethod
        def is_valid(cls, value: str) -> bool:
            """
            Check if a given value is a valid file type value.
            
            Args:
                value: The file type value to validate
                
            Returns:
                True if the value is valid, False otherwise
            """
            return value in cls.values()

