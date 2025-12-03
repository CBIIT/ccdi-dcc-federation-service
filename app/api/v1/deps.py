"""
FastAPI dependencies for the CCDI Federation Service.

This module provides dependency injection for common resources like
database sessions, configuration, and pagination parameters.
"""

from typing import Optional, Dict, Any, List

from fastapi import Depends, Query, HTTPException, Request
from neo4j import AsyncSession

from app.core.config import Settings, get_settings
from app.core.pagination import PaginationParams, parse_pagination_params
from app.core.logging import get_logger
from app.core.constants import Race, Ethnicity, VitalStatus
from app.db.memgraph import get_session
from app.lib.field_allowlist import get_field_allowlist, FieldAllowlist
from app.models.errors import create_pagination_error, InvalidParametersError

logger = get_logger(__name__)


# ============================================================================
# Core Dependencies
# ============================================================================

async def get_database_session() -> AsyncSession:
    """Get database session dependency."""
    async for session in get_session():
        yield session


def get_app_settings() -> Settings:
    """Get application settings dependency."""
    return get_settings()


def get_allowlist() -> FieldAllowlist:
    """Get field allowlist dependency."""
    return get_field_allowlist()


# ============================================================================
# Pagination Dependencies
# ============================================================================

def get_pagination_params(
    page: Optional[int] = Query(
        default=1,
        ge=1,
        description="The page to retrieve.\n\nThis is a 1-based index of a page within a page set. The value of page must default to 1 when this parameter is not provided."
    ),
    per_page: Optional[int] = Query(
        default=None,
        ge=1,
        description="The number of results per page.\n\nEach server can select its own default value for per_page when this parameter is not provided. \n\nThat said, the convention within the community is to use 100 as a default value if any value is equally reasonable."
    )
) -> PaginationParams:
    """
    Get and validate pagination parameters.
    
    Raises:
        HTTPException: If pagination parameters are invalid
    """
    try:
        return parse_pagination_params(page, per_page)
    except ValueError as e:
        error = create_pagination_error(page, per_page)
        raise error.to_http_exception()


# ============================================================================
# Filter Dependencies  
# ============================================================================

def get_subject_filters(
    sex: Optional[str] = Query(
        None, 
        description="Matches any subject where the `sex` field matches the string provided.",
        enum=["M", "F", "U"]
    ),
    race: Optional[str] = Query(
        None, 
        description="Matches any subject where any member of the `race` field matches any of the provided values. Multiple race values can be provided separated by `||` (double pipe). The race field in the database may contain semicolon-separated values (e.g., 'Asian;White'), and the filter will match if any of the provided values is found within those values. Only `||` is accepted as a delimiter; all other characters are treated as part of a single value.",
        enum=[r.value for r in Race]
    ),
    ethnicity: Optional[str] = Query(
        None, 
        description="Matches any subject where the `ethnicity` field matches the string provided. Ethnicity is derived from race values: if race contains 'Hispanic or Latino', ethnicity is 'Hispanic or Latino'; otherwise 'Not reported'. Only these two values are accepted.",
        enum=[e.value for e in Ethnicity]
    ),
    identifiers: Optional[str] = Query(None, description="Matches any subject where any member of the `identifiers` field matches the string provided. **Note:** a logical OR (`||`) is performed across the values when determining whether the subject should be included in the results."),
    vital_status: Optional[str] = Query(
        None, 
        description="Matches any subject where the `vital_status` field matches the string provided.",
        enum=[v.value for v in VitalStatus]
    ),
    age_at_vital_status: Optional[str] = Query(None, description="Matches any subject where the `age_at_vital_status` field matches the string provided."),
    depositions: Optional[str] = Query(
        None, 
        description="Filter by study_id. Matches any subject where the `depositions` field contains the specified study_id value (e.g., `phs002431`). Returns all participants that belong to the specified study. Example: `depositions=phs002431` will return all participants in study `phs002431`.",
        example="phs002431"
    ),
    request: Request = None
) -> Dict[str, Any]:
    """Get subject filter parameters."""
    filters = {}
    
    # Validate that no unknown query parameters are provided
    if request:
        # Define all allowed query parameter names
        allowed_params = {
            "sex", "race", "ethnicity", "identifiers", "vital_status", 
            "age_at_vital_status", "depositions", "page", "per_page"
        }
        
        # Check for unknown parameters (excluding unharmonized fields)
        unknown_params = []
        for key in request.query_params.keys():
            if not key.startswith("metadata.unharmonized.") and key not in allowed_params:
                unknown_params.append(key)
        
        if unknown_params:
            # Store unknown parameters for error handling in endpoint
            filters["_unknown_parameters"] = unknown_params
            return filters
    
    # Validate ethnicity first if provided
    if ethnicity is not None:
        # Validate ethnicity - only accept the two valid values
        from app.core.constants import Ethnicity
        # Strip whitespace and normalize the value
        ethnicity_str = str(ethnicity).strip() if ethnicity else None
        if ethnicity_str:
            if ethnicity_str not in Ethnicity.values():
                # Invalid ethnicity value - store for error handling in endpoint
                filters["_invalid_ethnicity"] = ethnicity_str
                return filters
            filters["ethnicity"] = ethnicity_str
    
    # Validate sex if provided
    if sex is not None:
        # Validate sex - only accept valid values (M, F, U)
        valid_sex_values = ["M", "F", "U"]
        if sex not in valid_sex_values:
            # Invalid sex value - store for error handling in endpoint
            filters["_invalid_sex"] = sex
            return filters
        filters["sex"] = sex
    
    # Validate race if provided
    if race is not None:
        # Handle race as string input with || delimiter
        from app.core.constants import Race
        valid_race_values = Race.values()
        
        race_str = str(race).strip() if race else None
        race_list = []
        
        if race_str:
            # Handle URL-encoded version (%7C%7C = double pipe encoded)
            if '%7C%7C' in race_str:
                race_str = race_str.replace('%7C%7C', '||')
            
            # Split ONLY on || delimiter - comma, ampersand, etc. are treated as part of the value
            if '||' in race_str:
                race_list = [r.strip() for r in race_str.split('||') if r.strip()]
            else:
                # No || delimiter found - treat entire value as single race value
                race_list = [race_str]
        
        # Filter out invalid race values - keep only valid ones
        if race_list:
            valid_race_list = [r for r in race_list if r in valid_race_values]
            invalid_races = [r for r in race_list if r not in valid_race_values]
            
            if valid_race_list:
                # Use list if multiple, single string if one
                if len(valid_race_list) > 1:
                    filters["race"] = valid_race_list
                elif len(valid_race_list) == 1:
                    filters["race"] = valid_race_list[0]
            else:
                # All values were invalid - return error
                filters["_invalid_race"] = invalid_races[0] if len(invalid_races) == 1 else invalid_races
                return filters
    if identifiers is not None:
        # Handle identifiers: only '||' is allowed as delimiter for multiple values
        # If no '||' found, treat as single identifier value
        identifiers_str = str(identifiers).strip()
        if identifiers_str:
            if '||' in identifiers_str:
                # Split by '||' delimiter
                identifiers_list = [id.strip() for id in identifiers_str.split('||') if id.strip()]
                if len(identifiers_list) > 1:
                    filters["identifiers"] = identifiers_list
                elif len(identifiers_list) == 1:
                    filters["identifiers"] = identifiers_list[0]
            else:
                # No '||' delimiter found - treat as single identifier value
                filters["identifiers"] = identifiers_str
    
    # Validate vital_status if provided
    if vital_status is not None:
        # Validate vital_status - only accept valid enum values
        from app.core.constants import VitalStatus
        vital_status_str = str(vital_status).strip() if vital_status else None
        if vital_status_str:
            if vital_status_str not in VitalStatus.values():
                # Invalid vital_status value - store for error handling in endpoint
                filters["_invalid_vital_status"] = vital_status_str
                return filters
            filters["vital_status"] = vital_status_str
    
    # Validate age_at_vital_status if provided
    if age_at_vital_status is not None:
        age_str = str(age_at_vital_status).strip() if age_at_vital_status else None
        if age_str:
            try:
                # Try to convert to integer
                # Note: age_at_vital_status is stored in days in the database
                age_int = int(age_str)
                # Validate it's a reasonable age (0-73000 days = ~200 years)
                if age_int < 0 or age_int > 73000:
                    filters["_invalid_age_at_vital_status"] = age_str
                    filters["_age_at_vital_status_reason"] = "Age must be a valid non-negative integer (stored in days)."
                    return filters
                filters["age_at_vital_status"] = age_int
            except ValueError:
                # Invalid integer format
                filters["_invalid_age_at_vital_status"] = age_str
                filters["_age_at_vital_status_reason"] = "Age must be a valid integer (stored in days)."
                return filters
    if depositions is not None:
        filters["depositions"] = depositions
    
    # Handle unharmonized fields from query parameters
    if request:
        for key, value in request.query_params.items():
            if key.startswith("metadata.unharmonized."):
                filters[key] = value
    
    return filters


def get_sample_filters(
    disease_phase: Optional[str] = Query(
        None, 
        description="Matches any sample where the `disease_phase` field matches the string provided."
    ),
    anatomical_sites: Optional[str] = Query(
        None, 
        description="Matches any sample where the `anatomical_sites` field matches the string provided.\n\n**Note:** a logical OR (`||`) is performed across the values when determining whether the subject should be included in the results."
    ),
    library_selection_method: Optional[str] = Query(
        None, 
        description="Matches any sample where the `library_selection_method` field matches the string provided."
    ),
    library_strategy: Optional[str] = Query(
        None, 
        description="Matches any sample where the `library_strategy` field matches the string provided."
    ),
    library_source_material: Optional[str] = Query(
        None, 
        description="Matches any sample where the `library_source_material` field matches the string provided."
    ),
    preservation_method: Optional[str] = Query(
        None, 
        description="Matches any sample where the `preservation_method` field matches the string provided."
    ),
    tumor_grade: Optional[str] = Query(
        None, 
        description="Matches any sample where the `tumor_grade` field matches the string provided."
    ),
    specimen_molecular_analyte_type: Optional[str] = Query(
        None, 
        description="Matches any sample where the `specimen_molecular_analyte_type` field matches the string provided."
    ),
    tissue_type: Optional[str] = Query(
        None, 
        description="Matches any sample where the `tissue_type` field matches the string provided."
    ),
    tumor_classification: Optional[str] = Query(
        None, 
        description="Matches any sample where the `tumor_classification` field matches the string provided."
    ),
    age_at_diagnosis: Optional[str] = Query(
        None, 
        description="Matches any sample where the `age_at_diagnosis` field matches the string provided."
    ),
    age_at_collection: Optional[str] = Query(
        None, 
        description="Matches any sample where the `age_at_collection` field matches the string provided."
    ),
    tumor_tissue_morphology: Optional[str] = Query(
        None, 
        description="Matches any sample where the `tumor_tissue_morphology` field matches the string provided."
    ),
    depositions: Optional[str] = Query(
        None, 
        description="Matches any sample where any member of the `depositions` fields match the string provided.\n\n**Note:** a logical OR (`||`) is performed across the values when determining whether the sample should be included in the results."
    ),
    diagnosis: Optional[str] = Query(
        None, 
        description="Matches any sample where the `diagnosis` field matches the string provided."
    ),
    request: Request = None
) -> Dict[str, Any]:
    """Get sample filter parameters."""
    filters = {}
    
    # Add non-null filters
    if disease_phase is not None:
        filters["disease_phase"] = disease_phase
    if anatomical_sites is not None:
        filters["anatomical_sites"] = anatomical_sites
    if library_selection_method is not None:
        filters["library_selection_method"] = library_selection_method
    if library_strategy is not None:
        filters["library_strategy"] = library_strategy
    if library_source_material is not None:
        filters["library_source_material"] = library_source_material
    if preservation_method is not None:
        filters["preservation_method"] = preservation_method
    if tumor_grade is not None:
        filters["tumor_grade"] = tumor_grade
    if specimen_molecular_analyte_type is not None:
        filters["specimen_molecular_analyte_type"] = specimen_molecular_analyte_type
    if tissue_type is not None:
        filters["tissue_type"] = tissue_type
    if tumor_classification is not None:
        filters["tumor_classification"] = tumor_classification
    if age_at_diagnosis is not None:
        filters["age_at_diagnosis"] = age_at_diagnosis
    if age_at_collection is not None:
        filters["age_at_collection"] = age_at_collection
    if tumor_tissue_morphology is not None:
        filters["tumor_tissue_morphology"] = tumor_tissue_morphology
    if depositions is not None:
        filters["depositions"] = depositions
    if diagnosis is not None:
        filters["diagnosis"] = diagnosis
    
    # Handle unharmonized fields from query parameters
    if request:
        for key, value in request.query_params.items():
            if key.startswith("metadata.unharmonized."):
                filters[key] = value
            # Reject singular form - only accept plural
            elif key == "anatomical_site":
                from app.models.errors import InvalidParametersError
                raise InvalidParametersError(
                    parameters=[]
                )
    
    return filters


def get_sample_filters_no_descriptions(
    disease_phase: Optional[str] = Query(None, include_in_schema=False),
    anatomical_sites: Optional[str] = Query(None, include_in_schema=False),
    library_selection_method: Optional[str] = Query(None, include_in_schema=False),
    library_strategy: Optional[str] = Query(None, include_in_schema=False),
    library_source_material: Optional[str] = Query(None, include_in_schema=False),
    preservation_method: Optional[str] = Query(None, include_in_schema=False),
    tumor_grade: Optional[str] = Query(None, include_in_schema=False),
    specimen_molecular_analyte_type: Optional[str] = Query(None, include_in_schema=False),
    tissue_type: Optional[str] = Query(None, include_in_schema=False),
    tumor_classification: Optional[str] = Query(None, include_in_schema=False),
    age_at_diagnosis: Optional[str] = Query(None, include_in_schema=False),
    age_at_collection: Optional[str] = Query(None, include_in_schema=False),
    tumor_tissue_morphology: Optional[str] = Query(None, include_in_schema=False),
    depositions: Optional[str] = Query(None, include_in_schema=False),
    diagnosis: Optional[str] = Query(None, include_in_schema=False),
    request: Request = None
) -> Dict[str, Any]:
    """Get sample filter parameters without descriptions (for count endpoint)."""
    filters = {}
    
    # Add non-null filters
    if disease_phase is not None:
        filters["disease_phase"] = disease_phase
    if anatomical_sites is not None:
        filters["anatomical_sites"] = anatomical_sites
    if library_selection_method is not None:
        filters["library_selection_method"] = library_selection_method
    if library_strategy is not None:
        filters["library_strategy"] = library_strategy
    if library_source_material is not None:
        filters["library_source_material"] = library_source_material
    if preservation_method is not None:
        filters["preservation_method"] = preservation_method
    if tumor_grade is not None:
        filters["tumor_grade"] = tumor_grade
    if specimen_molecular_analyte_type is not None:
        filters["specimen_molecular_analyte_type"] = specimen_molecular_analyte_type
    if tissue_type is not None:
        filters["tissue_type"] = tissue_type
    if tumor_classification is not None:
        filters["tumor_classification"] = tumor_classification
    if age_at_diagnosis is not None:
        filters["age_at_diagnosis"] = age_at_diagnosis
    if age_at_collection is not None:
        filters["age_at_collection"] = age_at_collection
    if tumor_tissue_morphology is not None:
        filters["tumor_tissue_morphology"] = tumor_tissue_morphology
    if depositions is not None:
        filters["depositions"] = depositions
    if diagnosis is not None:
        filters["diagnosis"] = diagnosis
    
    # Handle unharmonized fields from query parameters
    if request:
        for key, value in request.query_params.items():
            if key.startswith("metadata.unharmonized."):
                filters[key] = value
            # Reject singular form - only accept plural
            elif key == "anatomical_site":
                from app.models.errors import InvalidParametersError
                raise InvalidParametersError(
                    parameters=[]
                )
    
    return filters


def get_file_filters(
    type: Optional[str] = Query(
        None, 
        description="Matches any sequencing file where the `file_type` field matches the string provided.",
        alias="type"
    ),
    size: Optional[str] = Query(
        None, 
        description="Matches any sequencing file where the `file_size` field matches the string provided."
    ),
    checksums: Optional[str] = Query(
        None, 
        description="Matches any sequencing file where the `md5sum` or `checksum_value` field matches the string provided.\n\n**Note:** a logical OR (`||`) is performed across the values when determining whether the file should be included in the results."
    ),
    description: Optional[str] = Query(
        None, 
        description="Matches any sequencing file where the `file_description` field matches the string provided.\n\n**Note:** a file is returned if the value provided is a substring of the description."
    ),
    depositions: Optional[str] = Query(
        None, 
        description="Matches any sequencing file where any member of the `depositions` fields match the string provided.\n\n**Note:** a logical OR (`||`) is performed across the values when determining whether the file should be included in the results."
    )
) -> Dict[str, Any]:
    """Get sequencing file filter parameters."""
    filters = {}
    
    # Map generic field names to sequencing_file field names
    if type is not None:
        filters["file_type"] = type
    if size is not None:
        filters["file_size"] = size
    if checksums is not None:
        # For checksums, check both md5sum and checksum_value fields
        # This will be handled in the repository query
        filters["md5sum"] = checksums
    if description is not None:
        filters["file_description"] = description
    if depositions is not None:
        filters["depositions"] = depositions
    
    return filters


def get_file_filters_no_descriptions(
    type: Optional[str] = Query(None, include_in_schema=False, alias="type"),
    size: Optional[str] = Query(None, include_in_schema=False),
    checksums: Optional[str] = Query(None, include_in_schema=False),
    description: Optional[str] = Query(None, include_in_schema=False),
    depositions: Optional[str] = Query(None, include_in_schema=False)
) -> Dict[str, Any]:
    """Get sequencing file filter parameters without descriptions (for count endpoint)."""
    filters = {}
    
    # Map generic field names to sequencing_file field names
    if type is not None:
        filters["file_type"] = type
    if size is not None:
        filters["file_size"] = size
    if checksums is not None:
        filters["md5sum"] = checksums
    if description is not None:
        filters["file_description"] = description
    if depositions is not None:
        filters["depositions"] = depositions
    
    return filters


# ============================================================================
# Diagnosis Search Dependencies
# ============================================================================

def get_diagnosis_search_params(
    search: Optional[str] = Query(None, description="Diagnosis search term")
) -> Optional[str]:
    """Get diagnosis search parameters."""
    return search


def get_subject_diagnosis_filters(
    search: Optional[str] = Query(None, description="Diagnosis search term"),
    sex: Optional[str] = Query(None, description="Filter by sex"),
    race: Optional[List[str]] = Query(None, description="Filter by race. Can be provided multiple times: `?race=White&race=Asian`"),
    ethnicity: Optional[str] = Query(None, description="Filter by ethnicity"),
    identifiers: Optional[str] = Query(None, description="Filter by identifiers"),
    vital_status: Optional[str] = Query(None, description="Filter by vital status"),
    age_at_vital_status: Optional[str] = Query(None, description="Filter by age at vital status"),
    depositions: Optional[str] = Query(None, description="Filter by depositions"),
    request: Request = None
) -> Dict[str, Any]:
    """Get subject diagnosis search filters."""
    filters = get_subject_filters(
        sex=sex,
        race=race,
        ethnicity=ethnicity,
        identifiers=identifiers,
        vital_status=vital_status,
        age_at_vital_status=age_at_vital_status,
        depositions=depositions,
        request=request
    )
    
    if search:
        filters["_diagnosis_search"] = search
    
    return filters


def get_sample_diagnosis_filters(
    search: Optional[str] = Query(None, description="Diagnosis search term"),
    disease_phase: Optional[str] = Query(None, description="Filter by disease phase"),
    anatomical_sites: Optional[str] = Query(None, description="Filter by anatomical sites"),
    library_selection_method: Optional[str] = Query(None, description="Filter by library selection method"),
    library_strategy: Optional[str] = Query(None, description="Filter by library strategy"),
    library_source_material: Optional[str] = Query(None, description="Filter by library source material"),
    preservation_method: Optional[str] = Query(None, description="Filter by preservation method"),
    specimen_molecular_analyte_type: Optional[str] = Query(None, description="Filter by specimen molecular analyte type"),
    tissue_type: Optional[str] = Query(None, description="Filter by tissue type"),
    tumor_classification: Optional[str] = Query(None, description="Filter by tumor classification"),
    age_at_diagnosis: Optional[str] = Query(None, description="Filter by age at diagnosis"),
    age_at_collection: Optional[str] = Query(None, description="Filter by age at collection"),
    tumor_tissue_morphology: Optional[str] = Query(None, description="Filter by tumor tissue morphology"),
    depositions: Optional[str] = Query(None, description="Filter by depositions"),
    diagnosis: Optional[str] = Query(None, description="Filter by diagnosis"),
    request: Request = None
) -> Dict[str, Any]:
    """Get sample diagnosis search filters."""
    filters = get_sample_filters(
        disease_phase=disease_phase,
        anatomical_sites=anatomical_sites,
        library_selection_method=library_selection_method,
        library_strategy=library_strategy,
        library_source_material=library_source_material,
        preservation_method=preservation_method,
        specimen_molecular_analyte_type=specimen_molecular_analyte_type,
        tissue_type=tissue_type,
        tumor_classification=tumor_classification,
        age_at_diagnosis=age_at_diagnosis,
        age_at_collection=age_at_collection,
        tumor_tissue_morphology=tumor_tissue_morphology,
        depositions=depositions,
        diagnosis=diagnosis,
        request=request
    )
    
    if search:
        filters["_diagnosis_search"] = search
    
    return filters


# ============================================================================
# Rate Limiting Dependencies
# ============================================================================

async def check_rate_limit(
    request: Request,
    settings: Settings = Depends(get_app_settings)
) -> None:
    """Check rate limiting (placeholder for slowapi integration)."""
    # This would be implemented with slowapi rate limiting
    # For now, we'll just log the request
    logger.debug(
        "Request received",
        path=request.url.path,
        method=request.method,
        client_ip=request.client.host if request.client else None
    )
