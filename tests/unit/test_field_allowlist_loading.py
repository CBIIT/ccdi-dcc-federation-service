import json
from pathlib import Path
from unittest.mock import patch

from app.lib.field_allowlist import FieldAllowlist, EntityType


def test_unharmonized_allowlist_loaded_from_metadata_config():
    allowlist = FieldAllowlist()
    allowlist.load_from_database()

    # file filters are a constant, not the metadata_fields.json listing
    assert allowlist.get_allowed_unharmonized_fields(EntityType.FILE) == [
        "dcc_data_category",
        "dcc_file_name",
    ]
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "dcc_file_name") is True
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "dcc_data_category") is True
    # bare DB property names are not filter names
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "file_name") is False
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "data_category") is False
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "file_access") is False
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "anything") is False

    # subject/sample expose exactly the configured unharmonized fields
    assert allowlist.is_unharmonized_field_allowed(
        EntityType.SUBJECT, "dcc_associated_diagnosis_category_<n>"
    ) is True
    assert allowlist.is_unharmonized_field_allowed(
        EntityType.SUBJECT, "dcc_sex_at_birth"
    ) is True
    assert allowlist.is_unharmonized_field_allowed(
        EntityType.SAMPLE, "dcc_diagnosis_category_<n>"
    ) is True
    # a fabricated name is still rejected for subject/sample
    assert allowlist.is_unharmonized_field_allowed(EntityType.SAMPLE, "evil") is False


def test_malformed_config_structure_fails_closed():
    # Valid JSON but wrong shape (top-level list) must NOT fall back to the
    # permissive COMMON_UNHARMONIZED_PATTERNS defaults; it must default-deny.
    allowlist = FieldAllowlist()
    with patch("json.load", return_value=["not", "a", "dict"]):
        allowlist.load_from_database()

    assert allowlist._loaded is True
    for entity_type in EntityType:
        assert allowlist.get_allowed_unharmonized_fields(entity_type) == []
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "study_id") is False
    assert allowlist.is_unharmonized_field_allowed(EntityType.SUBJECT, "patient_id") is False


def test_file_metadata_lists_dcc_paths_not_file_name():
    path = Path(__file__).resolve().parents[2] / "app/config_data/metadata_fields.json"
    fields = json.loads(path.read_text())["file"]["fields"]
    paths = [field["path"] for field in fields]
    expected = [
        "unharmonized.dcc_file_name",
        "unharmonized.dcc_file_access",
        "unharmonized.dcc_file_type",
        "unharmonized.dcc_data_category",
        "unharmonized.dcc_dcf_indexd_guid",
        "unharmonized.dcc_authz",
        "unharmonized.dcc_acl",
        "unharmonized.dcc_file_mapping_level",
    ]
    for item in expected:
        assert item in paths
    assert "unharmonized.file_name" not in paths
