"""Tests that parameter enums declared via json_schema_extra reach the OpenAPI schema.

FastAPI >= 0.155.0 rejects the legacy ``Query(enum=[...])`` kwarg, so enums are
declared with ``json_schema_extra={"enum": [...]}``. These tests guard that the
enum hints still appear in the generated schema.
"""

from typing import Any

import pytest

from app.core.constants import Ethnicity, Race, VitalStatus
from app.main import app

SAMPLE_COUNT_FIELDS: list[str] = [
    "disease_phase", "anatomical_sites", "library_selection_method", "library_strategy",
    "library_source_material", "preservation_method", "tumor_grade", "specimen_molecular_analyte_type",
    "tissue_type", "tumor_classification", "age_at_diagnosis", "age_at_collection",
    "tumor_tissue_morphology", "diagnosis", "diagnosis_category",
]

SUBJECT_COUNT_FIELDS: list[str] = [
    "sex", "race", "ethnicity", "vital_status", "age_at_vital_status",
    "associated_diagnoses", "associated_diagnosis_categories",
]

SUBJECT_FILTER_ENUMS: dict[str, list[str]] = {
    "sex": ["M", "F", "U"],
    "race": [race.value for race in Race],
    "ethnicity": [ethnicity.value for ethnicity in Ethnicity],
    "vital_status": [status.value for status in VitalStatus],
}


def _parameter_enum(path: str, parameter_name: str) -> list[str]:
    """Return the enum list for a GET parameter in the generated OpenAPI schema."""
    operation: dict[str, Any] = app.openapi()["paths"][path]["get"]
    parameter = next(p for p in operation["parameters"] if p["name"] == parameter_name)
    return parameter["schema"]["enum"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("path", "parameter_name", "expected"),
    [
        ("/api/v1/sample/by/{field}/count", "field", SAMPLE_COUNT_FIELDS),
        ("/api/v1/subject/by/{field}/count", "field", SUBJECT_COUNT_FIELDS),
        ("/api/v1/file/by/{field}/count", "field", ["type", "depositions"]),
        (
            "/api/v1/errors/examples",
            "error_type",
            ["InvalidRoute", "InvalidParameters", "NotFound", "UnshareableData", "UnsupportedField", "all"],
        ),
    ],
)
def test_count_and_error_parameter_enums_in_openapi(path: str, parameter_name: str, expected: list[str]) -> None:
    assert _parameter_enum(path, parameter_name) == expected


@pytest.mark.unit
@pytest.mark.parametrize("path", ["/api/v1/subject", "/api/v1/subject-diagnosis"])
@pytest.mark.parametrize(("parameter_name", "expected"), list(SUBJECT_FILTER_ENUMS.items()))
def test_subject_filter_enums_in_openapi(path: str, parameter_name: str, expected: list[str]) -> None:
    assert _parameter_enum(path, parameter_name) == expected
