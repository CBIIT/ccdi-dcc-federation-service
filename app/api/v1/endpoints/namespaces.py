"""
Namespace API routes for the CCDI Federation Service.

This module provides REST endpoints for namespace operations
including listing namespaces and getting individual namespace details.
"""

from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Request
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
    DepositionAccession
)
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind, InvalidParametersError

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
            COALESCE(st.study_dd, st.study_id) AS study_dd,
            grant_ids
        ORDER BY st.study_id
        """
        
        # Execute query with proper async result consumption
        result = await self.session.run(cypher)
        records = []
        async for record in result:
            records.append(dict(record))
        
        # Build namespace objects
        namespaces = []
        contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
        
        for record in records:
            study_id = record.get("study_id")
            study_description = record.get("study_description", "")
            study_acronym = record.get("study_acronym", "")
            study_name = record.get("study_name", "")
            study_dd = record.get("study_dd", study_id)
            grant_ids = record.get("grant_ids", [])
            
            if not study_id:
                continue
            
            # Build study_funding_id array from distinct grant_ids (as objects with "value" key)
            study_funding_id = None
            if grant_ids:
                # Filter out None/empty values and wrap in {"value": "string"} format
                study_funding_id = [
                    {"value": grant_id}
                    for grant_id in grant_ids 
                    if grant_id
                ]
            
            # Build metadata with value-wrapped fields
            # Use null for missing data, JSON objects for found data
            metadata = NamespaceMetadata(
                study_short_title={"value": study_acronym} if study_acronym else None,
                study_name={"value": study_name} if study_name else None,
                study_funding_id=study_funding_id if study_funding_id else None,
                study_id={"value": study_id} if study_id else None,
                depositions=[DepositionAccession(kind="dbGaP", value=study_id)] if study_id else None
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
        
        # Organization is always CCDI-DCC (only one organization supported)
        # No need to validate - just use CCDI-DCC regardless of what's passed
        
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
            COALESCE(st.study_dd, st.study_id) AS study_dd,
            grant_ids
        LIMIT 1
        """
        
        # Execute query with proper async result consumption
        result = await self.session.run(cypher, {"study_id": namespace})
        records = []
        async for record in result:
            records.append(dict(record))
        
        if not records or not records[0].get("study_id"):
            logger.debug("Study ID not found", study_id=namespace)
            return None
        
        record = records[0]
        study_id = record.get("study_id")
        study_description = record.get("study_description", "")
        study_acronym = record.get("study_acronym", "")
        study_name = record.get("study_name", "")
        study_dd = record.get("study_dd", study_id)
        grant_ids = record.get("grant_ids", [])
        
        contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
        
        # Build study_funding_id array from distinct grant_ids (as objects with "value" key)
        study_funding_id = None
        if grant_ids:
            # Filter out None/empty values and wrap in {"value": "string"} format
            study_funding_id = [
                {"value": grant_id}
                for grant_id in grant_ids 
                if grant_id
            ]
        
        # Build metadata with value-wrapped fields
        # Use null for missing data, JSON objects for found data
        metadata = NamespaceMetadata(
            study_short_title={"value": study_acronym} if study_acronym else None,
            study_name={"value": study_name} if study_name else None,
            study_funding_id=study_funding_id if study_funding_id else None,
            study_id={"value": study_id} if study_id else None,
            depositions=[DepositionAccession(kind="dbGaP", value=study_id)] if study_id else None
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
    response_model=List[Namespace],
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
        
        # Return namespaces array directly
        return namespaces
        
    except Exception as e:
        logger.error("Error listing namespaces", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Get Specific Namespace
# ============================================================================

@router.get(
    "/{organization}/{namespace}",
    response_model=Namespace,
    summary="Get namespace details",
    description="Get details for a specific namespace. Returns 404 if organization or namespace doesn't match."
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
    
    Organization is always "CCDI-DCC" (only one organization supported).
    
    Returns 404 if:
    - Study ID (namespace) is not found in the database
    """
    logger.info(
        "Get namespace request",
        organization=organization,
        namespace=namespace,
        path=request.url.path
    )
    
    try:
        # Validate organization - must be "CCDI-DCC"
        if organization.strip().upper() != "CCDI-DCC":
            logger.info(
                "Get namespace response - invalid organization",
                organization=organization,
                namespace=namespace
            )
            raise InvalidParametersError(
                parameters=["organization"],
                message="Invalid query parameter(s) provided.",
                reason=f"Organization must be 'CCDI-DCC', but received '{organization}'"
            )
        
        # Create service
        service = NamespaceService(session, settings)
        
        # Get namespace
        result = await service.get_namespace_detail(organization, namespace)
        
        # If result is None (namespace not found), return namespace with metadata: null
        if result is None:
            logger.info(
                "Get namespace response - namespace not found, returning with null metadata",
                organization=organization,
                namespace=namespace
            )
            # Return namespace object with metadata: null
            contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
            namespace_obj = Namespace(
                id=NamespaceIdentifier(
                    organization="CCDI-DCC",
                    name=namespace
                ),
                description=f"Study {namespace}",
                contact_email=contact_email,
                metadata=None  # Set metadata to null for invalid namespace
            )
            return namespace_obj
        
        logger.info(
            "Get namespace response",
            organization=organization,
            namespace=namespace
        )
        
        # Return namespace object directly
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except InvalidParametersError as e:
        # Re-raise InvalidParametersError to let the exception handler process it
        raise e.to_http_exception()
    except Exception as e:
        logger.error("Error getting namespace", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        # For any other error, return namespace with metadata: null
        contact_email = "NCIChildhoodCancerDataInitiative@mail.nih.gov"
        namespace_obj = Namespace(
            id=NamespaceIdentifier(
                organization="CCDI-DCC",
                name=namespace
            ),
            description=f"Study {namespace}",
            contact_email=contact_email,
            metadata=None
        )
        return namespace_obj
