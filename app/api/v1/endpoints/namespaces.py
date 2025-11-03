"""
Namespace API routes for the CCDI Federation Service.

This module provides REST endpoints for namespace operations
including listing namespaces and getting individual namespace details.
"""

from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    check_rate_limit
)
from app.core.config import Settings
from app.core.logging import get_logger
from app.models.dto import Namespace, Organization, NamespacesResponse, NamespaceResponse

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
        Get all available namespaces.
        
        Returns:
            List of Namespace objects
        """
        logger.debug("Getting all namespaces")
        
        # Query to get distinct Study_ID values from participant nodes
        # For CCDI-DCC, organization is always "CCDI-DCC" and namespace is "phs"
        cypher = """
        MATCH (p:participant)
        WHERE p.Study_ID IS NOT NULL AND p.Study_ID <> ''
        WITH DISTINCT p.Study_ID AS study_id
        RETURN study_id
        ORDER BY study_id
        """
        
        # Execute query
        result = await self.session.run(cypher)
        records = await result.data()
        
        # Build namespace objects
        # For CCDI-DCC, we return a single namespace "phs" regardless of Study_ID values
        # since all Study_IDs belong to the same namespace
        namespaces = []
        if records:
            # Return one namespace entry: CCDI-DCC/phs
            namespaces.append(Namespace(
                organization="CCDI-DCC",
                name="phs",
                description="Namespace for CCDI-DCC phs studies (dbGaP study identifiers)",
                contact_email="support@ccdi.org"
            ))
        
        logger.info("Retrieved namespaces", count=len(namespaces))
        
        return namespaces
    
    async def get_namespace_detail(self, organization: str, namespace: str) -> Namespace:
        """
        Get details for a specific namespace.
        
        Args:
            organization: Organization identifier
            namespace: Namespace identifier
            
        Returns:
            Namespace object with details
            
        Raises:
            NotFoundError: If namespace is not found
        """
        logger.debug("Getting namespace detail", organization=organization, namespace=namespace)
        
        # Validate that organization is CCDI-DCC and namespace is phs (case-insensitive)
        if organization.upper() != "CCDI-DCC":
            from app.models.errors import NotFoundError
            raise NotFoundError(entity="Namespace", reason=f"Organization '{organization}' not found")
        
        if namespace.lower() != "phs":
            from app.models.errors import NotFoundError
            raise NotFoundError(entity="Namespace", reason=f"Namespace '{namespace}' not found")
        
        # Query to check if namespace exists and get participant count
        cypher = """
        MATCH (p:participant)
        WHERE p.Study_ID IS NOT NULL AND p.Study_ID <> ''
        RETURN COUNT(DISTINCT p) AS participant_count
        """
        
        # Execute query
        result = await self.session.run(cypher)
        records = await result.data()
        
        participant_count = records[0]["participant_count"] if records else 0
        
        if participant_count == 0:
            from app.models.errors import NotFoundError
            raise NotFoundError(entity="Namespace", reason=f"Namespace '{organization}/{namespace}' not found")
        
        # Build namespace with details
        namespace_obj = Namespace(
            organization="CCDI-DCC",
            name="phs",
            description="Namespace for CCDI-DCC phs studies (dbGaP study identifiers)",
            contact_email="support@ccdi.org"
        )
        
        logger.info(
            "Retrieved namespace detail",
            organization=organization,
            namespace=namespace,
            participant_count=participant_count
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
    response_model=NamespaceResponse,
    summary="Get namespace details",
    description="Get details for a specific namespace"
)
async def get_namespace(
    organization: str,
    namespace: str,
    request: Request,
    session: AsyncSession = Depends(get_database_session),
    settings: Settings = Depends(get_app_settings),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get details for a specific namespace."""
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
        
        logger.info(
            "Get namespace response",
            organization=organization,
            namespace=namespace
        )
        
        # Return NamespaceResponse with namespace object
        return NamespaceResponse(namespace=result)
        
    except Exception as e:
        logger.error("Error getting namespace", error=str(e), exc_info=True)
        if hasattr(e, 'to_http_exception'):
            raise e.to_http_exception()
        elif "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
