"""
Info API endpoint for the CCDI Federation Service.

This module provides the info endpoint that returns server, API, and data information.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from app.models.errors import ErrorDetail, ErrorsResponse, ErrorKind

router = APIRouter(tags=["Info"])

# Expect the file at: app/config_data/info.json
# From app/api/v1/endpoints/info.py, go up 3 levels to reach app/, then config_data/
DATA_PATH = Path(__file__).resolve().parents[3] / "config_data" / "info.json"


@router.get("/info", summary="API info")
def api_info():
    """
    Returns the API information including server, API, and data details.
    
    This endpoint serves the API information configuration file that describes
    the server, API version, and data version information.
    """
    try:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            
            # Filter the response:
            # 1. Keep only "api_version" and "documentation_url" in the "api" field
            # 2. Remove "organizations" field
            # 3. Return the entire "data" object which includes:
            #    - "version" object with "version" and "about_url" fields
            #    - "last_updated", "wiki_url", "documentation_url"
            filtered_data = {
                "server": data.get("server", {}),
                "api": {
                    "api_version": data.get("api", {}).get("api_version"),
                    "documentation_url": data.get("api", {}).get("documentation_url")
                },
                "data": data.get("data", {})
            }
            
            return filtered_data
    except FileNotFoundError:
        # Return 404 instead of 500 - no 500 errors allowed
        error_detail = ErrorDetail(
            kind=ErrorKind.NOT_FOUND,
            entity="Info",
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
            entity="Info",
            message="Unable to find data for your request.",
            reason="No data found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorsResponse(errors=[error_detail]).model_dump(exclude_none=True)
        )

