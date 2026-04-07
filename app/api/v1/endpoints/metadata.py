"""
Metadata API routes for the CCDI Federation Service.

This module provides REST endpoints for metadata operations
including field information for subjects, samples, and files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Request, Path as PathParam
from fastapi.responses import JSONResponse

from app.core.logging import get_logger
from app.models.dto import MetadataFieldsInfoResponse, MetadataFieldInfo, HarmonizedStandard
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind
from fastapi import status

logger = get_logger(__name__)

router = APIRouter(prefix="/metadata", tags=["Metadata"])

# Expect the file at: app/config_data/metadata_fields.json
# From app/api/v1/endpoints/metadata.py, go up 3 levels to reach app/, then config_data/
DATA_PATH = Path(__file__).resolve().parents[3] / "config_data" / "metadata_fields.json"


def load_metadata_fields() -> Dict[str, Any]:
    """Load metadata fields from JSON config file."""
    try:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Metadata",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    except json.JSONDecodeError:
        # Return 404 instead of 500 - no 500 errors allowed (don't expose error details)
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Metadata",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )


def convert_to_response(data: Dict[str, Any]) -> MetadataFieldsInfoResponse:
    """Convert JSON data to response model."""
    fields = []
    for field_data in data.get("fields", []):
        # Handle optional standard field
        standard_data = field_data.get("standard", {})
        standard = HarmonizedStandard(
            name=standard_data.get("name") if standard_data else None,
            url=standard_data.get("url") if standard_data else None
        )
        
        # Handle optional wiki_url field
        wiki_url = field_data.get("wiki_url", "")
        
        field_info = MetadataFieldInfo(
            path=field_data["path"],
            harmonized=field_data.get("harmonized", True),
            wiki_url=wiki_url,
            standard=standard
        )
        fields.append(field_info)
    
    return MetadataFieldsInfoResponse(fields=fields)


def _get_metadata_fields_for_type(field_type: str, request: Request) -> MetadataFieldsInfoResponse:
    """
    Helper function to get metadata fields for a specific entity type.
    
    Args:
        field_type: The normalized entity type ("subjects", "samples", or "file")
        request: The request object for logging
    
    Returns:
        MetadataFieldsInfoResponse with the fields for the specified type
    """
    logger.info(
        "Get metadata fields request",
        field_type=field_type,
        path=request.url.path
    )
    
    try:
        # Load metadata fields from config
        metadata_data = load_metadata_fields()
        
        # Get fields for the requested type
        if field_type not in metadata_data:
            logger.info(
                "Get metadata fields response - type not found in config",
                field_type=field_type
            )
            return MetadataFieldsInfoResponse(fields=[])
        
        entity_data = metadata_data[field_type]
        
        # Convert to response model
        response = convert_to_response(entity_data)
        
        logger.info(
            "Get metadata fields response",
            field_type=field_type,
            fields_count=len(response.fields)
        )
        
        return response
        
    except Exception as e:
        logger.error("Error getting metadata fields", error=str(e), exc_info=True)
        # Return empty array on any error
        return MetadataFieldsInfoResponse(fields=[])


@router.get(
    "/fields/subject",
    response_model=MetadataFieldsInfoResponse,
    summary="Get subject metadata fields",
    description="Get metadata fields for subjects. Returns the list of metadata fields with their harmonization status, wiki URLs, and standard information."
)
async def get_subject_metadata_fields(
    request: Request
):
    """
    Get metadata fields for subjects.
    
    Returns the list of metadata fields with their harmonization status,
    wiki URLs, and standard information.
    """
    return _get_metadata_fields_for_type("subjects", request)


@router.get(
    "/fields/sample",
    response_model=MetadataFieldsInfoResponse,
    summary="Get sample metadata fields",
    description="Get metadata fields for samples. Returns the list of metadata fields with their harmonization status, wiki URLs, and standard information."
)
async def get_sample_metadata_fields(
    request: Request
):
    """
    Get metadata fields for samples.
    
    Returns the list of metadata fields with their harmonization status,
    wiki URLs, and standard information.
    """
    return _get_metadata_fields_for_type("samples", request)


@router.get(
    "/fields/file",
    response_model=MetadataFieldsInfoResponse,
    summary="Get file metadata fields",
    description="Get metadata fields for files. Returns the list of metadata fields with their harmonization status, wiki URLs, and standard information."
)
async def get_file_metadata_fields(
    request: Request
):
    """
    Get metadata fields for files.
    
    Returns the list of metadata fields with their harmonization status,
    wiki URLs, and standard information.
    """
    return _get_metadata_fields_for_type("file", request)
