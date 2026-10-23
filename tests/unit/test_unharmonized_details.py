"""Config-driven details blocks for unharmonized subject fields."""

import logging

import pytest

from app.core.unharmonized_details import (
    _load_metadata_fields,
    attach_unharmonized_details,
    build_unharmonized_details,
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
                    "path": "unharmonized.dcc_associated_diagnosis_category_<n>",
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
    d = build_unharmonized_details("subjects", "dcc_associated_diagnosis_category_3", "Gliomas")
    assert d["url"].endswith("diagnosis_category=Gliomas")


def test_url_value_is_percent_encoded():
    d = build_unharmonized_details(
        "subjects", "dcc_associated_diagnosis_category_1", "Glioblastoma, NOS & A/B"
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
            "dcc_associated_diagnosis_category_1": {
                "value": "Myeloid leukemias",
                "comment": "Myeloid Leukemia",
                "owned": True,
            }
        },
    )
    entry = out["dcc_associated_diagnosis_category_1"]
    assert entry["comment"] == "Myeloid Leukemia"
    assert entry["owned"] is True
    assert "details" in entry


def test_metadata_fields_endpoint_does_not_expose_details():
    from app.api.v1.endpoints.metadata import convert_to_response, load_metadata_fields

    dumped = convert_to_response(load_metadata_fields()["subjects"]).model_dump()
    assert all("details" not in f for f in dumped["fields"])
