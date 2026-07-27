"""Unit tests for file_type_mappings helpers and FileRepository mapping."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.core.file_type_mappings import (
    clear_file_type_mappings_cache,
    get_db_values_for_api_file_type,
    map_file_type_db_to_api,
)
from app.repositories.file import FileRepository


@pytest.fixture(autouse=True)
def _clear_mappings_cache():
    clear_file_type_mappings_cache()
    yield
    clear_file_type_mappings_cache()


class TestFileTypeMappingsConfig:
    def test_mappings_json_valid_and_consistent(self):
        path = Path("app/config_data/file_type_mappings.json")
        data = json.loads(path.read_text())
        cfg = data["file_type"]
        mappings = cfg["mappings"]
        nulls = set(cfg["null_mappings"])
        reverse = cfg["reverse_mappings"]

        assert "sf" in mappings and mappings["sf"] == "TSV"
        assert "sf" not in nulls
        assert nulls == set()  # revised spreadsheet: no null transforms
        assert set(mappings.keys()).isdisjoint(nulls)
        assert mappings["tiff"] == "TIFF"
        assert mappings["bcf"] == "BCR Biotab"

        # reverse_mappings consistent with forward mappings
        for db, api in mappings.items():
            rev = reverse[api]
            rev_list = rev if isinstance(rev, list) else [rev]
            assert db in rev_list

    def test_enum_includes_jpg_and_parquet(self):
        path = Path("app/config_data/file_type_enum.json")
        data = json.loads(path.read_text())
        assert "JPG" in data["file_type"]
        assert "Parquet" in data["file_type"]
        assert "null" not in data["file_type"]
        assert None not in data["file_type"]


class TestMapFileTypeDbToApi:
    def test_basic_and_alias(self):
        assert map_file_type_db_to_api("bam") == "BAM"
        assert map_file_type_db_to_api("bam_index") == "BAI"
        assert map_file_type_db_to_api("sf") == "TSV"

    def test_case_insensitive_db_lookup(self):
        assert map_file_type_db_to_api("BAM") == "BAM"
        assert map_file_type_db_to_api("Bam") == "BAM"
        assert map_file_type_db_to_api("TIFF") == "TIFF"
        assert map_file_type_db_to_api("tiff") == "TIFF"

    def test_former_nulls_now_mapped(self):
        assert map_file_type_db_to_api("bcf") == "BCR Biotab"
        assert map_file_type_db_to_api("h5") == "HDF5"
        assert map_file_type_db_to_api("bw") == "bigWig"
        assert map_file_type_db_to_api("psm") == "TSV"
        assert map_file_type_db_to_api("selfsm") == "Plain Text Data Format"

    def test_unmapped_and_empty(self):
        assert map_file_type_db_to_api("JPEG") is None  # enum-only, not col D mapping
        assert map_file_type_db_to_api("unknown_xyz") is None
        assert map_file_type_db_to_api(None) is None
        assert map_file_type_db_to_api("   ") is None

    def test_null_mappings_branch_returns_none(self):
        """Cover null_mappings hit path (empty in production JSON)."""
        patched = {
            "mappings": {"bam": "BAM"},
            "null_mappings": ["legacy_null"],
            "reverse_mappings": {"BAM": "bam"},
        }
        with patch("app.core.file_type_mappings._load_config", return_value=patched):
            assert map_file_type_db_to_api("legacy_null") is None
            assert map_file_type_db_to_api("LEGACY_NULL") is None
            assert map_file_type_db_to_api("bam") == "BAM"


class TestReverseMap:
    def test_bai_includes_alias(self):
        vals = get_db_values_for_api_file_type("BAI")
        assert set(vals) == {"bai", "bam_index"}

    def test_tiff_reverse_includes_aliases(self):
        vals = get_db_values_for_api_file_type("TIFF")
        assert set(vals) == {"ndpi", "scn", "tif", "tiff", "vms"}

    def test_legacy_enum_direct(self):
        assert get_db_values_for_api_file_type("JPEG") == ["jpeg"]


class TestRepositoryMapper:
    def test_repository_delegates_to_mappings(self):
        repo = FileRepository(MagicMock(), MagicMock())
        assert repo._map_file_type_to_enum("bam") == "BAM"
        assert repo._map_file_type_to_enum("bam_index") == "BAI"
        assert repo._map_file_type_to_enum("tiff") == "TIFF"
        assert repo._map_file_type_to_enum("JPEG") is None
        assert repo._map_file_type_to_enum("") is None
