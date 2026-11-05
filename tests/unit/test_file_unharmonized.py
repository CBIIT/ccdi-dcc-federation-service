"""File unharmonized builder: eight dcc_ keys, Explore Files links, acl/authz de-list."""

import json
import logging
from pathlib import Path

import pytest

from app.core.file_unharmonized import (
    build_file_unharmonized,
    clear_file_unharmonized_cache,
    delist_access_value,
)

BASE = "https://clinicalcommons.ccdi.cancer.gov/exploreFiles"
NODE = {
    "file_name": "sample1.bw",
    "file_access": "Controlled",
    "file_type": "bw",
    "data_category": ["Sequencing", "Simple Nucleotide Variation"],
    "dcf_indexd_guid": "dg.4DFC/8b0c",
    "authz": "['/programs/phs002431.c1']",
    "acl": "['phs002431.c1']",
    "file_mapping_level": "Sample",
}
_KEYS = [
    "dcc_file_name",
    "dcc_file_access",
    "dcc_file_type",
    "dcc_data_category",
    "dcc_dcf_indexd_guid",
    "dcc_authz",
    "dcc_acl",
    "dcc_file_mapping_level",
]


@pytest.fixture(autouse=True)
def _clear_map_cache():
    clear_file_unharmonized_cache()
    yield
    clear_file_unharmonized_cache()


def test_builds_eight_keys_in_order():
    assert list(build_file_unharmonized(NODE)) == _KEYS


def test_plain_entries_have_value_only():
    out = build_file_unharmonized(NODE)
    assert out["dcc_file_name"] == {"value": "sample1.bw"}
    assert out["dcc_file_access"] == {"value": "Controlled"}
    assert out["dcc_dcf_indexd_guid"] == {"value": "dg.4DFC/8b0c"}


def test_file_type_entry_from_map():
    assert build_file_unharmonized(NODE)["dcc_file_type"] == {
        "value": "bw",
        "details": {"url": f"{BASE}?file_type=bw"},
        "comment": "Compressed genomic signal/coverage track",
    }


def test_file_type_not_in_map_is_omitted():
    node = {**NODE, "file_type": "bam"}
    assert "dcc_file_type" not in build_file_unharmonized(node)


def test_file_type_null_comment_omits_comment():
    node = {**NODE, "file_type": "bam_index"}
    entry = build_file_unharmonized(node)["dcc_file_type"]
    assert "comment" not in entry
    assert entry["details"]["url"] == f"{BASE}?file_type=bam_index"
    assert entry["value"] == "bam_index"


def test_file_type_entry_case_insensitive_keeps_raw_value():
    node = {**NODE, "file_type": "BW"}
    entry = build_file_unharmonized(node)["dcc_file_type"]
    assert entry["value"] == "BW"
    assert entry["details"]["url"] == f"{BASE}?file_type=BW"
    assert entry["comment"] == "Compressed genomic signal/coverage track"


def test_data_category_joined_and_linked():
    entry = build_file_unharmonized(NODE)["dcc_data_category"]
    assert entry["value"] == "Sequencing;Simple Nucleotide Variation"
    assert entry["details"]["url"] == (
        f"{BASE}?data_category=Sequencing|Simple%20Nucleotide%20Variation"
    )


def test_data_category_url_encodes_each_value():
    node = {**NODE, "data_category": ["A&B", "C/D"]}
    url = build_file_unharmonized(node)["dcc_data_category"]["details"]["url"]
    assert url == f"{BASE}?data_category=A%26B|C%2FD"


@pytest.mark.parametrize("value", [[], None])
def test_empty_data_category_list_omitted(value):
    node = {**NODE, "data_category": value}
    assert "dcc_data_category" not in build_file_unharmonized(node)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("['phs1']", "phs1"),
        ("['a', 'b']", "a;b"),
        ('["x"]', "x"),
        ("[]", None),
        ("plain", "plain"),
        (["a", "b"], "a;b"),
        (None, None),
        ("['/open']", "/open"),
        ("['*']", "*"),
    ],
)
def test_delist_access_value_shapes(raw, expected):
    assert delist_access_value(raw) == expected


def test_mapping_level_url():
    assert build_file_unharmonized(NODE)["dcc_file_mapping_level"] == {
        "value": "Sample",
        "details": {"url": f"{BASE}?file_mapping_level=Sample"},
    }


def test_no_owned_method_or_harmonizer_anywhere():
    def walk(node):
        if isinstance(node, dict):
            assert "owned" not in node
            assert "method" not in node
            assert "harmonizer" not in node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(build_file_unharmonized(NODE))


def test_missing_map_file_omits_file_type_and_warns(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.core.file_unharmonized._MAP_PATH",
        Path("/nonexistent/unharmonized_file_type_map.json"),
    )
    clear_file_unharmonized_cache()
    with caplog.at_level(logging.WARNING):
        out = build_file_unharmonized(NODE)
    assert "dcc_file_type" not in out
    assert "dcc_file_name" in out
    assert caplog.records
    assert caplog.records[0].levelno == logging.WARNING


def test_invalid_map_json_omits_file_type(tmp_path, monkeypatch):
    path = tmp_path / "map.json"
    path.write_text("{", encoding="utf-8")
    monkeypatch.setattr("app.core.file_unharmonized._MAP_PATH", path)
    clear_file_unharmonized_cache()
    assert "dcc_file_type" not in build_file_unharmonized(NODE)


def test_map_file_has_twenty_one_raw_types():
    data = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "app/config_data/unharmonized_file_type_map.json"
        ).read_text()
    )
    assert "unharmonized" not in data
    assert len(data) == 21


def test_wrapped_unharmonized_key_is_unwrapped(tmp_path, monkeypatch):
    """Ticket attachment nests raw types under a top-level unharmonized key."""
    path = tmp_path / "map.json"
    path.write_text(
        json.dumps(
            {
                "unharmonized": {
                    "bw": {
                        "dcc_file_type": {
                            "value": "bw",
                            "comment": "from wrapped map",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("app.core.file_unharmonized._MAP_PATH", path)
    clear_file_unharmonized_cache()
    entry = build_file_unharmonized(NODE)["dcc_file_type"]
    assert entry["value"] == "bw"
    assert entry["comment"] == "from wrapped map"


@pytest.mark.parametrize("payload", [{}, {"unharmonized": {}}])
def test_empty_map_omits_file_type_and_warns(tmp_path, monkeypatch, caplog, payload):
    path = tmp_path / "map.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr("app.core.file_unharmonized._MAP_PATH", path)
    clear_file_unharmonized_cache()
    with caplog.at_level(logging.WARNING):
        out = build_file_unharmonized(NODE)
    assert "dcc_file_type" not in out
    assert "dcc_file_name" in out
    assert caplog.records
    assert any("empty" in record.message.lower() for record in caplog.records)
