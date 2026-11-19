"""Config-driven details blocks for unharmonized subject fields."""

import logging

import pytest

from app.core.unharmonized_details import (
    _load_metadata_fields,
    attach_unharmonized_details,
    build_unharmonized_details,
    clear_untransformed_category_methods,
    strip_category_ownership_fields,
)

_SEX_URL = "https://clinicalcommons.ccdi.cancer.gov/exploreParticipants?sex_at_birth={value}"
_DIAG_URL = "https://clinicalcommons.ccdi.cancer.gov/exploreParticipants?diagnosis_category={value}"


def _config():
    return {
        "subjects": {
            "fields": [
                {
                    "path": "sex",
                    "harmonized": True,
                    "details": {"method": "Mapped", "harmonizer": "Nope", "url_template": _SEX_URL},
                },
                {
                    "path": "unharmonized.dcc_sex_at_birth",
                    "harmonized": False,
                    "details": {
                        "method": "Mapped",
                        "harmonizer": "DomainExpert",
                        "url_template": _SEX_URL,
                    },
                },
                {
                    "path": "unharmonized.dcc_associated_diagnosis_categories_<n>",
                    "harmonized": False,
                    "details": {
                        "method": "Mapped",
                        "harmonizer": "diagnosis_classification_system",
                        "url_template": _DIAG_URL,
                    },
                },
                {
                    "path": "unharmonized.dcc_bad",
                    "harmonized": False,
                    "details": "not-an-object",
                },
                {
                    "path": "unharmonized.dcc_bad_template",
                    "harmonized": False,
                    "details": {"method": "Mapped", "url_template": 12},
                },
            ]
        }
    }


@pytest.fixture(autouse=True)
def _patched_config(monkeypatch):
    _load_metadata_fields.cache_clear()
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: _config(),
    )
    yield
    _load_metadata_fields.cache_clear()


def test_details_for_plain_key():
    d = build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female")
    assert d == {
        "method": "Mapped",
        "harmonizer": "DomainExpert",
        "url": "https://clinicalcommons.ccdi.cancer.gov/exploreParticipants?sex_at_birth=Female",
    }


def test_numbered_key_uses_n_entry():
    d = build_unharmonized_details("subjects", "dcc_associated_diagnosis_categories_3", "Gliomas")
    assert d["url"].endswith("diagnosis_category=Gliomas")


def test_url_value_is_percent_encoded():
    d = build_unharmonized_details(
        "subjects", "dcc_associated_diagnosis_categories_1", "Glioblastoma, NOS & A/B"
    )
    assert d["url"].endswith("diagnosis_category=Glioblastoma%2C%20NOS%20%26%20A%2FB")


def test_null_value_omits_url_keeps_rest():
    assert build_unharmonized_details("subjects", "dcc_sex_at_birth", None) == {
        "method": "Mapped",
        "harmonizer": "DomainExpert",
    }


def test_unknown_key_returns_none():
    assert build_unharmonized_details("subjects", "dcc_not_configured", "x") is None


def test_harmonized_entry_details_ignored():
    assert build_unharmonized_details("subjects", "sex", "F") is None


def test_malformed_details_entry_is_skipped(caplog):
    with caplog.at_level(logging.WARNING):
        assert build_unharmonized_details("subjects", "dcc_bad", "x") is None
        assert build_unharmonized_details("subjects", "dcc_bad_template", "x") is None
    assert "unharmonized details" in caplog.text


def test_attach_preserves_existing_entry_keys():
    out = attach_unharmonized_details(
        "subjects",
        {
            "dcc_associated_diagnosis_categories_1": {
                "value": "Myeloid leukemias",
                "comment": "Myeloid Leukemia",
                "owned": True,
            }
        },
    )
    entry = out["dcc_associated_diagnosis_categories_1"]
    assert entry["comment"] == "Myeloid Leukemia"
    assert entry["owned"] is True
    assert entry["details"]["method"] == "Mapped"
    # Alias link uses the harmonized PV (comment), not the raw DB spelling.
    assert entry["details"]["url"].endswith("diagnosis_category=Myeloid%20Leukemia")


def test_clear_keeps_method_on_alias_and_promoted_case_only():
    payload = {
        "dcc_associated_diagnosis_categories_1": {
            "value": "medulloblastoma",
            "details": {
                "method": "Mapped",
                "harmonizer": "diagnosis_classification_system",
                "url": "https://example/medulloblastoma",
            },
        },
        "dcc_associated_diagnosis_categories_2": {
            "value": "Custom Category",
            "details": {
                "method": "Mapped",
                "harmonizer": "diagnosis_classification_system",
                "url": "https://example/Custom%20Category",
            },
        },
        "dcc_associated_diagnosis_categories_3": {
            "value": "Myeloid leukemias",
            "comment": "Myeloid Leukemia",
            "owned": True,
            "details": {
                "method": "Mapped",
                "harmonizer": "diagnosis_classification_system",
                "url": "https://example/Myeloid%20Leukemia",
            },
        },
        "dcc_sex_at_birth": {
            "value": "Female",
            "details": {"method": "Mapped", "harmonizer": "DomainExpert"},
        },
    }
    clear_untransformed_category_methods(
        payload,
        "dcc_associated_diagnosis_categories_",
        {"dcc_associated_diagnosis_categories_1"},
    )
    assert payload["dcc_associated_diagnosis_categories_1"]["details"]["method"] == "Mapped"
    assert "method" not in payload["dcc_associated_diagnosis_categories_2"]["details"]
    assert payload["dcc_associated_diagnosis_categories_2"]["details"]["harmonizer"]
    assert payload["dcc_associated_diagnosis_categories_3"]["details"]["method"] == "Mapped"
    assert payload["dcc_sex_at_birth"]["details"]["method"] == "Mapped"


def test_strip_category_ownership_fields_leaves_sex_alone():
    payload = {
        "dcc_associated_diagnosis_categories_1": {
            "value": "Myeloid leukemias",
            "comment": "Myeloid Leukemia",
            "owned": True,
            "details": {"method": "Mapped"},
        },
        "dcc_sex_at_birth": {"value": "Female", "comment": "keep-me"},
    }
    strip_category_ownership_fields(payload, "dcc_associated_diagnosis_categories_")
    assert "comment" not in payload["dcc_associated_diagnosis_categories_1"]
    assert "owned" not in payload["dcc_associated_diagnosis_categories_1"]
    assert payload["dcc_associated_diagnosis_categories_1"]["details"]["method"] == "Mapped"
    assert payload["dcc_sex_at_birth"]["comment"] == "keep-me"


def test_malformed_entity_section_does_not_raise(monkeypatch):
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: {"subjects": "not-a-section"},
    )
    assert build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female") is None


def test_malformed_fields_list_does_not_raise(monkeypatch):
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: {"subjects": {"fields": "not-a-list"}},
    )
    assert build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female") is None


def test_metadata_fields_endpoint_does_not_expose_details():
    from app.api.v1.endpoints.metadata import convert_to_response, load_metadata_fields

    dumped = convert_to_response(load_metadata_fields()["subjects"]).model_dump()
    assert all("details" not in f for f in dumped["fields"])
