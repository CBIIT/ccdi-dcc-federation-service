"""Fail-closed behavior when gateway / metadata config is missing or malformed."""

from pathlib import Path

import pytest

from app.core.gateways import _load_gateways_config, get_gateways
from app.core.unharmonized_details import (
    _load_metadata_fields,
    attach_unharmonized_details,
    build_unharmonized_details,
)


@pytest.fixture(autouse=True)
def _clear_loader_caches():
    _load_gateways_config.cache_clear()
    _load_metadata_fields.cache_clear()
    yield
    _load_gateways_config.cache_clear()
    _load_metadata_fields.cache_clear()


# ---------------------------------------------------------------------------
# gateways.json loader
# ---------------------------------------------------------------------------


def test_gateways_missing_file_returns_empty(monkeypatch):
    monkeypatch.setattr(
        "app.core.gateways._CONFIG_PATH",
        Path("/nonexistent/gateways.json"),
    )
    assert _load_gateways_config() == {}
    assert get_gateways("subjects") == []


def test_gateways_invalid_json_returns_empty(tmp_path, monkeypatch, caplog):
    path = tmp_path / "gateways.json"
    path.write_text("{ not valid json", encoding="utf-8")
    monkeypatch.setattr("app.core.gateways._CONFIG_PATH", path)
    with caplog.at_level("WARNING"):
        assert _load_gateways_config() == {}
        assert get_gateways("subjects") == []
    assert "Could not load gateways config" in caplog.text


def test_gateways_root_not_object_returns_empty(tmp_path, monkeypatch, caplog):
    path = tmp_path / "gateways.json"
    path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("app.core.gateways._CONFIG_PATH", path)
    with caplog.at_level("WARNING"):
        assert _load_gateways_config() == {}
    assert "JSON object" in caplog.text


def test_gateways_entity_not_list_returns_empty(monkeypatch):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {"subjects": "not-a-list"},
    )
    assert get_gateways("subjects") == []


def test_gateways_skips_non_object_entries(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.gateways._load_gateways_config",
        lambda: {
            "subjects": [
                "skip-me",
                {
                    "name": "Valid",
                    "kind": "Open",
                    "link": {
                        "kind": "Approximate",
                        "url": "https://example.com",
                        "instructions": "Go",
                    },
                },
            ]
        },
    )
    with caplog.at_level("WARNING"):
        gws = get_gateways("subjects")
    assert len(gws) == 1
    assert gws[0]["name"] == "Valid"


# ---------------------------------------------------------------------------
# metadata_fields.json loader (unharmonized details)
# ---------------------------------------------------------------------------


def test_metadata_fields_invalid_json_returns_empty(tmp_path, monkeypatch, caplog):
    path = tmp_path / "metadata_fields.json"
    path.write_text("{ broken", encoding="utf-8")
    monkeypatch.setattr("app.core.unharmonized_details._CONFIG_PATH", path)
    with caplog.at_level("WARNING"):
        assert _load_metadata_fields() == {}
        assert build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female") is None
    assert "Could not load metadata field config" in caplog.text


def test_metadata_fields_root_not_object_yields_no_details(monkeypatch):
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: ["not", "a", "dict"],
    )
    assert build_unharmonized_details("subjects", "dcc_sex_at_birth", "Female") is None


def test_attach_unharmonized_details_none_passthrough():
    assert attach_unharmonized_details("subjects", None) is None


def test_attach_unharmonized_details_preserves_non_dict_entry(monkeypatch):
    monkeypatch.setattr(
        "app.core.unharmonized_details._load_metadata_fields",
        lambda: {
            "subjects": {
                "fields": [
                    {
                        "path": "unharmonized.dcc_sex_at_birth",
                        "harmonized": False,
                        "details": {
                            "method": "Mapped",
                            "harmonizer": "DomainExpert",
                            "url_template": "https://example.com?sex_at_birth={value}",
                        },
                    }
                ]
            }
        },
    )
    out = attach_unharmonized_details(
        "subjects",
        {"dcc_sex_at_birth": {"value": "Female"}, "bad": "scalar"},
    )
    assert out["bad"] == "scalar"
    assert "details" in out["dcc_sex_at_birth"]
