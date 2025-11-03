"""
Metadata API routes for the CCDI Federation Service.

This module provides REST endpoints for metadata operations
including field information for subjects, samples, and files.
"""

from typing import Dict, List, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from neo4j import AsyncSession

from app.api.v1.deps import (
    get_database_session,
    get_app_settings,
    get_allowlist,
    check_rate_limit
)
from app.core.config import Settings
from app.core.logging import get_logger
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import MetadataFieldsResponse

logger = get_logger(__name__)

# router = APIRouter(prefix="/metadata", tags=["metadata"])


# ============================================================================
# Metadata Services
# ============================================================================

class MetadataService:
    """Service for metadata operations."""
    
    def __init__(
        self,
        session: AsyncSession,
        allowlist: FieldAllowlist,
        settings: Settings
    ):
        """Initialize service with dependencies."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings
    
    async def get_fields_for_entity(self, entity_type: str) -> MetadataFieldsResponse:
        """
        Get available fields for a given entity type.
        
        Args:
            entity_type: Type of entity (subject, sample, file)
            
        Returns:
            MetadataFieldsResponse with available fields
        """
        logger.debug("Getting metadata fields", entity_type=entity_type)
        
        # Get harmonized fields from allowlist
        harmonized_fields = self.allowlist.get_harmonized_fields(entity_type)
        
        # Get unharmonized fields (these would typically come from database schema)
        # For now, we'll return a static set based on the entity type
        unharmonized_fields = self._get_unharmonized_fields(entity_type)
        
        response = MetadataFieldsResponse(
            harmonized=harmonized_fields,
            unharmonized=unharmonized_fields
        )
        
        logger.info(
            "Retrieved metadata fields",
            entity_type=entity_type,
            harmonized_count=len(harmonized_fields),
            unharmonized_count=len(unharmonized_fields)
        )
        
        return response
    
    def _get_unharmonized_fields(self, entity_type: str) -> List[str]:
        """Get unharmonized fields for entity type."""
        # In a real implementation, this would query the database
        # to discover actual unharmonized fields present in the data
        unharmonized_fields = {
            "subject": [
                "metadata.unharmonized.custom_field_1",
                "metadata.unharmonized.custom_field_2",
                "metadata.unharmonized.site_specific_data"
            ],
            "sample": [
                "metadata.unharmonized.processing_notes",
                "metadata.unharmonized.quality_metrics",
                "metadata.unharmonized.lab_specific_data"
            ],
            "file": [
                "metadata.unharmonized.processing_pipeline",
                "metadata.unharmonized.file_format_version",
                "metadata.unharmonized.analysis_parameters"
            ]
        }
        
        return unharmonized_fields.get(entity_type, [])


# ============================================================================
# Subject Metadata Fields
# ============================================================================

# @router.get(
#     "/fields/subject",
#     response_model=MetadataFieldsResponse,
#     summary="Get subject metadata fields",
#     description="Get available metadata fields for subjects"
# )
# async def get_subject_fields(
#     request: Request,
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Get available metadata fields for subjects."""
#     logger.info(
#         "Get subject metadata fields request",
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         service = MetadataService(session, allowlist, settings)
#         
#         # Get fields
#         result = await service.get_fields_for_entity("subject")
#         
#         logger.info(
#             "Get subject metadata fields response",
#             harmonized_count=len(result.harmonized),
#             unharmonized_count=len(result.unharmonized)
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error getting subject metadata fields", error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Sample Metadata Fields
# ============================================================================

# @router.get(
#     "/fields/sample",
#     response_model=MetadataFieldsResponse,
#     summary="Get sample metadata fields",
#     description="Get available metadata fields for samples"
# )
# async def get_sample_fields(
#     request: Request,
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Get available metadata fields for samples."""
#     logger.info(
#         "Get sample metadata fields request",
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         service = MetadataService(session, allowlist, settings)
#         
#         # Get fields
#         result = await service.get_fields_for_entity("sample")
#         
#         logger.info(
#             "Get sample metadata fields response",
#             harmonized_count=len(result.harmonized),
#             unharmonized_count=len(result.unharmonized)
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error getting sample metadata fields", error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# File Metadata Fields
# ============================================================================

# @router.get(
#     "/fields/file",
#     response_model=MetadataFieldsResponse,
#     summary="Get file metadata fields",
#     description="Get available metadata fields for files"
# )
# async def get_file_fields(
#     request: Request,
#     session: AsyncSession = Depends(get_database_session),
#     settings: Settings = Depends(get_app_settings),
#     allowlist: FieldAllowlist = Depends(get_allowlist),
#     _rate_limit: None = Depends(check_rate_limit)
# ):
#     """Get available metadata fields for files."""
#     logger.info(
#         "Get file metadata fields request",
#         path=request.url.path
#     )
#     
#     try:
#         # Create service
#         service = MetadataService(session, allowlist, settings)
#         
#         # Get fields
#         result = await service.get_fields_for_entity("file")
#         
#         logger.info(
#             "Get file metadata fields response",
#             harmonized_count=len(result.harmonized),
#             unharmonized_count=len(result.unharmonized)
#         )
#         
#         return result
#         
#     except Exception as e:
#         logger.error("Error getting file metadata fields", error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail="Internal server error")
