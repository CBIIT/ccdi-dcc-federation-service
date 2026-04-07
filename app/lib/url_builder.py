"""
URL builder utility for generating server URLs in API responses.

This module provides functions to generate server URLs for identifiers
in the CCDI Federation API format.
"""

from typing import Literal


def build_identifier_server_url(
    base_url: str,
    entity_type: Literal["subject", "sample", "file"],
    organization: str = "CCDI-DCC",
    study_id: str = "",
    name: str = ""
) -> str:
    """
    Build a server URL for an identifier.
    
    Format: {base_url}/api/v1/{entity_type}/{organization}/{study_id}/{name}
    
    Args:
        base_url: Base URL of the server (e.g., "https://dcc.ccdi.cancer.gov")
        entity_type: Type of entity ("subject", "sample", or "file")
        organization: Organization identifier (default: "CCDI-DCC")
        study_id: Study ID (namespace name)
        name: Entity name (participant_id, sample_id, or file_id)
        
    Returns:
        Complete server URL string
        
    Examples:
        >>> build_identifier_server_url(
        ...     "https://dcc.ccdi.cancer.gov",
        ...     "subject",
        ...     "CCDI-DCC",
        ...     "phs002431",
        ...     "0061cbb0846973206fcf"
        ... )
        'https://dcc.ccdi.cancer.gov/api/v1/subject/CCDI-DCC/phs002431/0061cbb0846973206fcf'
    """
    # Remove trailing slash from base_url if present
    base_url = base_url.rstrip("/")
    
    # Build the URL path
    url = f"{base_url}/api/v1/{entity_type}/{organization}/{study_id}/{name}"
    
    return url

