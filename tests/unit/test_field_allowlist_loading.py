from unittest.mock import patch

from app.lib.field_allowlist import FieldAllowlist, EntityType


def test_unharmonized_allowlist_loaded_from_metadata_config():
    allowlist = FieldAllowlist()
    allowlist.load_from_database()

    # file declares exactly one unharmonized field (file_name); everything else denied
    assert allowlist.get_allowed_unharmonized_fields(EntityType.FILE) == ["file_name"]
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "file_name") is True
    assert allowlist.is_unharmonized_field_allowed(EntityType.FILE, "anything") is False

    # subject/sample expose exactly the configured unharmonized fields
    assert allowlist.is_unharmonized_field_allowed(
        EntityType.SUBJECT, "associated_diagnosis_categories"
    ) is True
    assert allowlist.is_unharmonized_field_allowed(
        EntityType.SAMPLE, "diagnosis_category"
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
