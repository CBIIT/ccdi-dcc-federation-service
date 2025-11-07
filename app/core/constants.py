"""
Constants and enumerations for data validation.

This module contains valid values and enums for various data fields used throughout
the application, such as race, ethnicity, sex, etc.
"""

from enum import Enum


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
    NOT_REPORTED = "Not Reported"
    UNKNOWN = "Unknown"

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

