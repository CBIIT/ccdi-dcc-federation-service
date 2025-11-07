"""
Info API endpoint for the CCDI Federation Service.

This module provides the info endpoint that returns server, API, and data information.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

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
            return json.load(f)
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

