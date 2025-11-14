"""
Namespace API routes for the CCDI Federation Service.

This module provides REST endpoints for namespace operations
including listing namespaces and getting individual namespace details.
"""

from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    check_rate_limit
)
from app.core.config import Settings
from app.core.logging import get_logger
from app.models.dto import (
    Namespace, 
    Organization, 
    NamespacesResponse, 
    NamespaceResponse,
    NamespaceIdentifier,
    NamespaceMetadata,
    UnharmonizedField
)

logger = get_logger(__name__)

router = APIRouter(prefix="/namespace", tags=["namespaces"])


# ============================================================================
# Namespace Services
# ============================================================================

class NamespaceService:
    """Service for namespace operations."""
    
    def __init__(self, session: AsyncSession, settings: Settings):
        """Initialize service with dependencies."""
        self.session = session
        self.settings = settings
    
    async def get_namespaces(self) -> List[Namespace]:
        """
        Get all available namespaces (unique study_ids).
        
        Returns:
            List of Namespace objects, one for each unique study_id
        """
        logger.debug("Getting all namespaces")
        
        # Query to get all unique study nodes with their properties and study_funding grant_ids
        cypher = """
        MATCH (st:study)
        WHERE st.study_id IS NOT NULL AND st.study_id <> ''
        OPTIONAL MATCH (st)-[r]-(sf:study_funding)
        WHERE sf.grant_id IS NOT NULL AND sf.grant_id <> ''
        WITH st, COLLECT(DISTINCT sf.grant_id) AS grant_ids
        RETURN DISTINCT 
            st.study_id AS study_id,
            COALESCE(st.study_description, '') AS study_description,
            COALESCE(st.study_acronym, '') AS study_acronym,
            COALESCE(st.study_name, '') AS study_name,
            grant_ids
        ORDER BY st.study_id
        """
        
        # Execute query
        result = await self.session.run(cypher)
        records = await result.data()
        
        # Build namespace objects
        namespaces = []
        contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
        
        for record in records:
            study_id = record.get("study_id")
            study_description = record.get("study_description", "")
            study_acronym = record.get("study_acronym", "")
            study_name = record.get("study_name", "")
            grant_ids = record.get("grant_ids", [])
            
            if not study_id:
                continue
            
            # Build study_funding_id array from distinct grant_ids
            study_funding_id = None
            if grant_ids:
                # Filter out None/empty values and create UnharmonizedField objects
                study_funding_id = [
                    UnharmonizedField(value=grant_id) 
                    for grant_id in grant_ids 
                    if grant_id
                ]
            
            # Build metadata
            metadata = NamespaceMetadata(
                study_short_title=UnharmonizedField(value=study_acronym) if study_acronym else None,
                study_name=UnharmonizedField(value=study_name) if study_name else None,
                study_funding_id=study_funding_id,
                study_id=UnharmonizedField(value=study_id),
                depositions=[{"kind": "dbGaP", "value": study_id}]
            )
            
            # Build namespace object
            namespace = Namespace(
                id=NamespaceIdentifier(
                    organization="CCDI-DCC",
                    name=study_id
                ),
                description=study_description if study_description else f"Study {study_id}",
                contact_email=contact_email,
                metadata=metadata
            )
            
            namespaces.append(namespace)
        
        logger.info("Retrieved namespaces", count=len(namespaces))
        
        return namespaces
    
    async def get_namespace_detail(self, organization: str, namespace: str) -> Optional[Namespace]:
        """
        Get details for a specific namespace (study_id).
        
        Args:
            organization: Organization identifier (must be "CCDI-DCC")
            namespace: Namespace identifier (study_id)
            
        Returns:
            Namespace object with details for the specified study_id, or None if not found
        """
        logger.debug("Getting namespace detail", organization=organization, namespace=namespace)
        
        # Validate that organization is CCDI-DCC (case-insensitive)
        if organization.upper() != "CCDI-DCC":
            logger.debug("Organization not matched", organization=organization)
            return None
        
        # Query to get the specific study by study_id with study_funding grant_ids
        cypher = """
        MATCH (st:study)
        WHERE st.study_id = $study_id
        OPTIONAL MATCH (st)-[r]-(sf:study_funding)
        WHERE sf.grant_id IS NOT NULL AND sf.grant_id <> ''
        WITH st, COLLECT(DISTINCT sf.grant_id) AS grant_ids
        RETURN 
            st.study_id AS study_id,
            COALESCE(st.study_description, '') AS study_description,
            COALESCE(st.study_acronym, '') AS study_acronym,
            COALESCE(st.study_name, '') AS study_name,
            grant_ids
        LIMIT 1
        """
        
        # Execute query
        result = await self.session.run(cypher, {"study_id": namespace})
        records = await result.data()
        
        if not records or not records[0].get("study_id"):
            logger.debug("Study ID not found", study_id=namespace)
            return None
        
        record = records[0]
        study_id = record.get("study_id")
        study_description = record.get("study_description", "")
        study_acronym = record.get("study_acronym", "")
        study_name = record.get("study_name", "")
        grant_ids = record.get("grant_ids", [])
        
        contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
        
        # Build study_funding_id array from distinct grant_ids
        study_funding_id = None
        if grant_ids:
            # Filter out None/empty values and create UnharmonizedField objects
            study_funding_id = [
                UnharmonizedField(value=grant_id) 
                for grant_id in grant_ids 
                if grant_id
            ]
        
        # Build metadata
        metadata = NamespaceMetadata(
            study_short_title=UnharmonizedField(value=study_acronym) if study_acronym else None,
            study_name=UnharmonizedField(value=study_name) if study_name else None,
            study_funding_id=study_funding_id,
            study_id=UnharmonizedField(value=study_id),
            depositions=[{"kind": "dbGaP", "value": study_id}]
        )
        
        # Build namespace object
        namespace_obj = Namespace(
            id=NamespaceIdentifier(
                organization="CCDI-DCC",
                name=study_id
            ),
            description=study_description if study_description else f"Study {study_id}",
            contact_email=contact_email,
            metadata=metadata
        )
        
        logger.info(
            "Retrieved namespace detail",
            organization=organization,
            namespace=namespace,
            study_id=study_id
        )
        
        return namespace_obj


# ============================================================================
# List All Namespaces
# ============================================================================

@router.get(
    "",
    response_model=NamespacesResponse,
    summary="List namespaces",
    description="Get all available namespaces"
)
async def list_namespaces(
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List all available namespaces."""
    logger.info(
        "List namespaces request",
        path=request.url.path
    )
    
    try:
        # Create service
        service = NamespaceService(session, settings)
        
        # Get namespaces
        namespaces = await service.get_namespaces()
        
        logger.info(
            "List namespaces response",
            namespace_count=len(namespaces)
        )
        
        # Return NamespacesResponse with namespaces array
        return NamespacesResponse(namespaces=namespaces)
        
    except Exception as e:
        logger.error("Error listing namespaces", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Get Specific Namespace
# ============================================================================

@router.get(
    "/{organization}/{namespace}",
    response_model=NamespacesResponse,
    summary="Get namespace details",
    description="Get details for a specific namespace. Returns empty array if organization or namespace doesn't match."
)
async def get_namespace(
    organization: str,
    namespace: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    _rate_limit: None = Depends(check_rate_limit)
):
    """
    Get details for a specific namespace.
    
    Returns empty array [] if:
    - Organization is not "CCDI-DCC"
    - Study ID (namespace) is not found in the database
    """
    logger.info(
        "Get namespace request",
        organization=organization,
        namespace=namespace,
        path=request.url.path
    )
    
    try:
        # Create service
        service = NamespaceService(session, settings)
        
        # Get namespace
        result = await service.get_namespace_detail(organization, namespace)
        
        # If result is None, return empty array
        if result is None:
            logger.info(
                "Get namespace response - not found",
                organization=organization,
                namespace=namespace
            )
            return NamespacesResponse(namespaces=[])
        
        logger.info(
            "Get namespace response",
            organization=organization,
            namespace=namespace
        )
        
        # Return NamespacesResponse with single namespace in array
        return NamespacesResponse(namespaces=[result])
        
    except Exception as e:
        logger.error("Error getting namespace", error=str(e), exc_info=True)
        # Return empty array for any error (not found, invalid organization, etc.)
        return NamespacesResponse(namespaces=[])
