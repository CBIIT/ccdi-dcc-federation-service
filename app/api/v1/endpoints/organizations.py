"""
Organizations API endpoint for the CCDI Federation Service.

This module provides the organizations endpoint that returns organization information.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, status

from app.core.logging import get_logger
from app.models.dto import OrganizationsResponse, OrganizationResponse, Organization
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind

logger = get_logger(__name__)

router = APIRouter(prefix="/organization", tags=["Organization"])

# Expect the file at: app/config_data/info.json
# From app/api/v1/endpoints/organizations.py, go up 3 levels to reach app/, then config_data/
DATA_PATH = Path(__file__).resolve().parents[3] / "config_data" / "info.json"


@router.get("", response_model=OrganizationsResponse, summary="Get organizations")
def get_organizations():
    """
    Returns the list of organizations.
    
    This endpoint serves the organizations information from the info.json file.
    """
    try:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Get organizations from info.json
        organizations_data = data.get("organizations", [])
        
        # Convert to Organization models
        organizations = [
            Organization(
                identifier=org.get("identifier"),
                name=org.get("name")
            )
            for org in organizations_data
        ]
        
        logger.info(
            "Get organizations response",
            organizations_count=len(organizations)
        )
        
        return OrganizationsResponse(organizations=organizations)
        
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail=f"Missing file: {DATA_PATH}"
        )
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Invalid JSON in info.json"
        )
    except Exception as e:
        logger.error("Error getting organizations", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{name}", response_model=OrganizationResponse, summary="Get organization by name")
def get_organization_by_name(name: str):
    """
    Returns the organization matching the provided name (if it exists).
    
    Returns 404 with error response if the organization is not found.
    The name can match either the identifier or the name field.
    """
    with DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Get organizations from info.json
    organizations_data = data.get("organizations", [])
    
    # Find organization by identifier or name (case-insensitive)
    found_org = None
    for org in organizations_data:
        org_identifier = org.get("identifier", "").lower()
        org_name = org.get("name", "").lower()
        name_lower = name.lower()
        
        if org_identifier == name_lower or org_name == name_lower:
            found_org = org
            break
    
    # If not found, return 404 with error response
    if found_org is None:
        logger.info(
            "Get organization by name response - not found",
            name=name
        )
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Organizations",
            message="Organizations not found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    
    # Convert to Organization model
    organization = Organization(
        identifier=found_org.get("identifier"),
        name=found_org.get("name")
    )
    
    logger.info(
        "Get organization by name response",
        name=name,
        identifier=organization.identifier
    )
    
    return OrganizationResponse(organization=organization)

