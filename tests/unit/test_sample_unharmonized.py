"""Sample unharmonized entry builder (FEDERATION-768)."""

import json
import logging

import pytest

from app.core import sample_unharmonized
from app.core.sample_unharmonized import (
    build_sample_unharmonized_entry,
    clear_sample_unharmonized_cache,
)

BASE = "https://clinicalcommons.ccdi.cancer.gov"


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_sample_unharmonized_cache()
    yield
    clear_sample_unharmonized_cache()


def _use_config(monkeypatch, tmp_path, data):
    path = tmp_path / "attrs.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.setattr(sample_unharmonized, "_CONFIG_PATH", path)
    clear_sample_unharmonized_cache()


def test_transformed_value_builds_entry_with_harmonized_url():
    assert build_sample_unharmonized_entry(
        "specimen_molecular_analyte_type", "Circulating cell-free DNA", "DNA"
    ) == {
        "value": "Circulating cell-free DNA",
        "details": {
            "method": "Mapped",
            "harmonizer": "DomainExpert",
            "url": f"{BASE}/exploreFiles?library_source_molecule=DNA",
        },
    }


def test_filled_file_url_is_rebuilt_with_harmonized_value():
    # file has ...disease_phase=Prior+Primary; CZ: Prior Primary -> Unknown
    entry = build_sample_unharmonized_entry("disease_phase", "Prior Primary", "Unknown")
    assert (
        entry["details"]["url"] == f"{BASE}/exploreParticipants?disease_phase=Unknown"
    )


def test_url_encodes_spaces_as_percent20():
    entry = build_sample_unharmonized_entry(
        "library_selection_method", "PolyA", "Poly-A Enriched Genomic Library"
    )
    assert entry["details"]["url"] == (
        f"{BASE}/exploreFiles?library_selection=Poly-A%20Enriched%20Genomic%20Library"
    )


def test_pv_listed_in_file_gets_no_entry():
    assert (
        build_sample_unharmonized_entry(
            "library_selection_method",
            "5-Methylcytidine Antibody",
            "5-Methylcytidine Antibody",
        )
        is None
    )
    assert (
        build_sample_unharmonized_entry("preservation_method", "Fixation", "Fixation")
        is None
    )


def test_empty_file_url_gives_entry_without_url():
    assert build_sample_unharmonized_entry(
        "preservation_method", "Cytospin Slide", "Unknown"
    ) == {
        "value": "Cytospin Slide",
        "details": {"method": "Mapped", "harmonizer": "DomainExpert"},
    }


def test_null_method_is_omitted():
    entry = build_sample_unharmonized_entry(
        "library_strategy", "Archer Fusion", "Other"
    )
    assert entry["details"] == {
        "harmonizer": "DomainExpert",
        "url": f"{BASE}/exploreFiles?library_strategy=Other",
    }


def test_null_harmonized_value_keeps_the_file_url():
    # cDNA maps to null, so the file URL is used as written (nothing after '=').
    assert build_sample_unharmonized_entry("library_selection_method", "cDNA", None) == {
        "value": "cDNA",
        "details": {
            "harmonizer": "diagnosis_classification_system",
            "url": f"{BASE}/exploreFiles?library_selection=",
        },
    }


def test_gate_rejects():
    assert (
        build_sample_unharmonized_entry("disease_phase", "Not In File", "Unknown")
        is None
    )
    assert (
        build_sample_unharmonized_entry("library_selection_method", "polya", "x")
        is None
    )
    assert build_sample_unharmonized_entry("diagnosis_category", "Gliomas", "x") is None
    assert build_sample_unharmonized_entry("disease_phase", None, "Unknown") is None
    assert (
        build_sample_unharmonized_entry("disease_phase", ["Prior Primary"], "Unknown")
        is None
    )


def test_raw_value_is_stripped_before_match():
    assert (
        build_sample_unharmonized_entry("disease_phase", " Prior Primary ", "Unknown")[
            "value"
        ]
        == "Prior Primary"
    )


def test_whitespace_only_difference_is_not_a_transformation():
    assert (
        build_sample_unharmonized_entry("preservation_method", "Fixation ", "Fixation")
        is None
    )


def test_case_only_mapping_counts_as_transformation():
    # CZ 2026-11-09: an explicit field_mappings rule counts even when only case changes.
    entry = build_sample_unharmonized_entry(
        "library_selection_method", "Not Applicable", "Not applicable"
    )
    assert entry is not None
    assert entry["details"]["url"].endswith("library_selection=Not%20applicable")


def test_placeholders_and_optional_keys(monkeypatch, tmp_path):
    _use_config(
        monkeypatch,
        tmp_path,
        {
            "disease_phase": {
                "A": {
                    "value": "A",
                    "details": {"method": "string", "harmonizer": "", "url": "string"},
                    "comment": "c",
                    "owned": True,
                },
                "B": {
                    "value": "B",
                    "details": {"method": "string", "harmonizer": "", "url": "string"},
                    "comment": "string",
                    "owned": "yes",
                },
            }
        },
    )
    assert build_sample_unharmonized_entry("disease_phase", "A", "Unknown") == {
        "value": "A",
        "comment": "c",
        "owned": True,
    }
    assert build_sample_unharmonized_entry("disease_phase", "B", "Unknown") == {
        "value": "B"
    }


def test_non_object_level1_field_is_skipped_with_warning(monkeypatch, tmp_path, caplog):
    _use_config(
        monkeypatch,
        tmp_path,
        {"disease_phase": ["not", "an", "object"], "library_strategy": {}},
    )
    with caplog.at_level(logging.WARNING):
        assert (
            build_sample_unharmonized_entry("disease_phase", "Prior Primary", "Unknown")
            is None
        )
    assert any("disease_phase" in record.message for record in caplog.records)


def test_url_without_equals_warns_once(monkeypatch, tmp_path, caplog):
    _use_config(
        monkeypatch,
        tmp_path,
        {
            "disease_phase": {
                "Prior Primary": {
                    "value": "Prior Primary",
                    "details": {"url": "https://example.org/page"},
                },
                "Synchronous": {
                    "value": "Synchronous",
                    "details": {"url": "https://example.org/page"},
                },
            }
        },
    )
    with caplog.at_level(logging.WARNING):
        first = build_sample_unharmonized_entry(
            "disease_phase", "Prior Primary", "Unknown"
        )
        second = build_sample_unharmonized_entry(
            "disease_phase", "Synchronous", "Unknown"
        )
    assert first["details"]["url"] == "https://example.org/page"
    assert second["details"]["url"] == "https://example.org/page"
    warnings = [
        record.message
        for record in caplog.records
        if "https://example.org/page" in record.message
    ]
    assert len(warnings) == 1


def test_url_without_equals_passes_through_with_warning(monkeypatch, tmp_path, caplog):
    _use_config(
        monkeypatch,
        tmp_path,
        {
            "disease_phase": {
                "Prior Primary": {
                    "value": "Prior Primary",
                    "details": {"url": "https://example.org/page"},
                }
            }
        },
    )
    with caplog.at_level(logging.WARNING):
        entry = build_sample_unharmonized_entry(
            "disease_phase", "Prior Primary", "Unknown"
        )
    assert entry["details"]["url"] == "https://example.org/page"
    assert any(
        "https://example.org/page" in record.message for record in caplog.records
    )


def test_missing_file_returns_none_and_warns(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(sample_unharmonized, "_CONFIG_PATH", tmp_path / "missing.json")
    clear_sample_unharmonized_cache()
    with caplog.at_level(logging.WARNING):
        assert (
            build_sample_unharmonized_entry("disease_phase", "Prior Primary", "Unknown")
            is None
        )
    assert caplog.records


def test_malformed_json_returns_none_and_warns(monkeypatch, tmp_path, caplog):
    path = tmp_path / "attrs.json"
    path.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(sample_unharmonized, "_CONFIG_PATH", path)
    clear_sample_unharmonized_cache()
    with caplog.at_level(logging.WARNING):
        assert (
            build_sample_unharmonized_entry("disease_phase", "Prior Primary", "Unknown")
            is None
        )
    assert caplog.records


def test_top_level_list_returns_none_and_warns(monkeypatch, tmp_path, caplog):
    path = tmp_path / "attrs.json"
    path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(sample_unharmonized, "_CONFIG_PATH", path)
    clear_sample_unharmonized_cache()
    with caplog.at_level(logging.WARNING):
        assert (
            build_sample_unharmonized_entry("disease_phase", "Prior Primary", "Unknown")
            is None
        )
    assert caplog.records


def test_non_object_entry_is_skipped_others_work(monkeypatch, tmp_path, caplog):
    _use_config(
        monkeypatch,
        tmp_path,
        {
            "disease_phase": {
                "A": "bad",
                "B": {"value": "B", "details": {"harmonizer": "H"}},
            }
        },
    )
    with caplog.at_level(logging.WARNING):
        assert build_sample_unharmonized_entry("disease_phase", "A", "Unknown") is None
        entry = build_sample_unharmonized_entry("disease_phase", "B", "Unknown")
    assert entry == {"value": "B", "details": {"harmonizer": "H"}}
    assert any("A" in record.message for record in caplog.records)


def test_unknown_level1_key_is_ignored_with_warning(monkeypatch, tmp_path, caplog):
    _use_config(
        monkeypatch,
        tmp_path,
        {
            "diagnosis_category": {"Gliomas": {"value": "Gliomas"}},
            "disease_phase": {
                "Prior Primary": {
                    "value": "Prior Primary",
                    "details": {"harmonizer": "H"},
                }
            },
        },
    )
    with caplog.at_level(logging.WARNING):
        entry = build_sample_unharmonized_entry(
            "disease_phase", "Prior Primary", "Unknown"
        )
    assert entry == {"value": "Prior Primary", "details": {"harmonizer": "H"}}
    assert any("diagnosis_category" in record.message for record in caplog.records)
