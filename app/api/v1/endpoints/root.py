"""
Root API endpoint for the CCDI Federation Service.

This module provides the root API endpoint that returns the API root JSON.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind

router = APIRouter(tags=["Root"])

# Expect the file at: app/config_data/info.json
# From app/api/v1/endpoints/root.py, go up 3 levels to reach app/, then config_data/
DATA_PATH = Path(__file__).resolve().parents[3] / "config_data" / "info.json"


@router.get("/", summary="API root JSON")
def api_root():
    """
    Returns the API root JSON configuration.
    
    This endpoint serves the root API configuration file that describes
    the available endpoints and API metadata.
    """
    try:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            # Return only the API section (title, version, description, endpoints)
            # to maintain backward compatibility with the original dcc_api_root.json structure
            return {
                "title": data.get("api", {}).get("title", ""),
                "version": data.get("api", {}).get("api_version", ""),
                "description": data.get("api", {}).get("description", ""),
                "endpoints": data.get("api", {}).get("endpoints", {})
            }
    except FileNotFoundError:
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Root",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )
    except json.JSONDecodeError:
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Root",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

