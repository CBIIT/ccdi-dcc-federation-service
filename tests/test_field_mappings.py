import json
from pathlib import Path

import pytest

from app.core.field_mappings import map_field_value, reverse_map_field_value, is_null_mapped_value


MAPPING_FIELDS = {
    "subject": ["vital_status"],
    "diagnosis": ["disease_phase", "diagnosis_category"],
    "sequencing_file": [
        "library_strategy",
        "specimen_molecular_analyte_type",
        "library_source_material",
    ],
}


def _load_mapping_json():
    config_path = Path(__file__).parent.parent / "app" / "config_data" / "field_mappings.json"
    with open(config_path, "r") as f:
        return json.load(f)


def _get_field_config(mapping_json, node_type: str, field_name: str):
    assert node_type in mapping_json, f"Missing node_type in field_mappings.json: {node_type}"
    node_cfg = mapping_json[node_type]
    assert field_name in node_cfg, f"Missing field in field_mappings.json: {node_type}.{field_name}"
    return node_cfg[field_name]


def test_expected_field_mapping_entries_exist():
    mapping_json = _load_mapping_json()
    for node_type, fields in MAPPING_FIELDS.items():
        for field_name in fields:
            _get_field_config(mapping_json, node_type, field_name)


@pytest.mark.parametrize(
    "field_name,db_value,expected_api",
    [
        ("vital_status", "Not Reported", "Not reported"),
        ("disease_phase", "Recurrent Disease", "Relapse"),
        ("disease_phase", "Not Applicable", "Unknown"),
        ("disease_phase", "Prior Primary", "Unknown"),
        ("disease_phase", "Synchronous", "Unknown"),
        ("library_strategy", "Archer Fusion", "Other"),
        ("library_source_material", "Other", "Not Reported"),
        ("diagnosis_category", "Low-grade Gliomas", "Low-Grade Gliomas"),
        ("diagnosis_category", "Myeloid leukemias", "Myeloid Leukemia"),
        ("specimen_molecular_analyte_type", "DNA", "DNA"),
        ("specimen_molecular_analyte_type", "Total RNA", "RNA"),
        ("specimen_molecular_analyte_type", "Protein", "Protein"),
    ],
)
def test_map_field_value_matches_config_examples(field_name: str, db_value: str, expected_api: str):
    assert map_field_value(field_name, db_value) == expected_api


@pytest.mark.parametrize(
    "field_name,api_value,expected_db",
    [
        ("vital_status", "Not reported", "Not Reported"),
        ("library_strategy", "Other", "Archer Fusion"),
        ("library_source_material", "Not Reported", ["Other", "Not Reported"]),
        ("diagnosis_category", "Low-Grade Gliomas", ["Low-grade Gliomas", "Low-Grade Gliomas"]),
        ("diagnosis_category", "Myeloid Leukemia", ["Myeloid leukemias", "Myeloid Leukemia"]),
        ("disease_phase", "Relapse", ["Recurrent Disease", "Relapse"]),
        ("disease_phase", "Unknown", ["Not Applicable", "Prior Primary", "Synchronous", "Unknown"]),
        ("specimen_molecular_analyte_type", "DNA", ["Circulating cell-free DNA", "Circulating Tumor-Derived DNA", "DNA"]),
        ("specimen_molecular_analyte_type", "RNA", ["MicroRNA", "Messenger RNA", "Nucleic RNA Sample", "RNA Specimen", "Total RNA"]),
    ],
)
def test_reverse_map_field_value_matches_config_examples(field_name: str, api_value: str, expected_db):
    assert reverse_map_field_value(field_name, api_value) == expected_db


def test_null_mappings_are_treated_as_null():
    # From field_mappings.json:
    # - sequencing_file.specimen_molecular_analyte_type: null_mappings ["Not Reported"]
    # library_source_material no longer null-maps "Other" (maps Other → Not Reported instead)
    assert is_null_mapped_value("library_source_material", "Other") is False
    assert map_field_value("library_source_material", "Other") == "Not Reported"

    assert is_null_mapped_value("specimen_molecular_analyte_type", "Not Reported") is True
    assert map_field_value("specimen_molecular_analyte_type", "Not Reported") is None


def test_library_source_material_other_is_database_only():
    """DB-only 'Other' is not a valid API filter; use 'Not Reported' instead."""
    from app.core.field_mappings import is_database_only_value

    assert is_database_only_value("library_source_material", "Other") is True
    assert is_database_only_value("library_source_material", "Not Reported") is False


def test_reverse_mappings_are_consistent_with_forward_mappings_for_tracked_fields():
    """
    Regression guard:
    Every reverse mapping entry should map to DB value(s) that forward-map back to the same API value.
    """
    mapping_json = _load_mapping_json()
    # Collect only for the fields requested by the user
    fields = []
    for node_type, names in MAPPING_FIELDS.items():
        for name in names:
            fields.append((node_type, name))

    for node_type, field_name in fields:
        cfg = _get_field_config(mapping_json, node_type, field_name)
        rev = cfg.get("reverse_mappings", {}) or {}
        for api_value, db_value_or_list in rev.items():
            db_values = db_value_or_list if isinstance(db_value_or_list, list) else [db_value_or_list]
            for db_value in db_values:
                # Skip if that DB value is treated as null
                if db_value in (cfg.get("null_mappings", []) or []):
                    continue
                assert map_field_value(field_name, db_value) == api_value


