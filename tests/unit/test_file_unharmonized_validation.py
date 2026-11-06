import pytest
from unittest.mock import MagicMock
from app.repositories.file import FileRepository
from app.lib.field_allowlist import FieldAllowlist
from app.config_data.file_node_registry import FILE_NODE_REGISTRY
from app.models.errors import InvalidParametersError, UnsupportedFieldError


def _repo() -> FileRepository:
    allowlist = FieldAllowlist()
    allowlist.load_from_database()
    return FileRepository(MagicMock(), allowlist, FILE_NODE_REGISTRY[0])


def test_injection_payload_field_name_is_rejected_as_invalid_parameters():
    repo = _repo()
    payload = "guid = sf.guid RETURN sf UNION MATCH (n) DETACH DELETE n //"
    with pytest.raises(InvalidParametersError):
        repo._validate_unharmonized_file_field(payload, "metadata.unharmonized." + payload)


def test_wellformed_but_not_allowlisted_field_is_unsupported():
    repo = _repo()
    # syntactically valid name, but not in the file filter set (dcc_file_name, dcc_data_category)
    with pytest.raises(UnsupportedFieldError):
        repo._validate_unharmonized_file_field("batch_id", "metadata.unharmonized.batch_id")


def test_allowlisted_unharmonized_file_field_passes():
    repo = _repo()
    repo._validate_unharmonized_file_field("dcc_file_name", "metadata.unharmonized.dcc_file_name")
    repo._validate_unharmonized_file_field(
        "dcc_data_category", "metadata.unharmonized.dcc_data_category"
    )


def test_dcc_response_keys_beyond_the_filter_set_are_not_filters():
    repo = _repo()
    for name in ("dcc_file_access", "file_access"):
        with pytest.raises(UnsupportedFieldError):
            repo._validate_unharmonized_file_field(name, "metadata.unharmonized." + name)


def test_bare_property_names_are_no_longer_filters():
    # Filter names carry the dcc_ prefix. The old names are unsupported, not aliases.
    repo = _repo()
    for name in ("file_name", "data_category"):
        with pytest.raises(UnsupportedFieldError):
            repo._validate_unharmonized_file_field(name, "metadata.unharmonized." + name)


def test_error_message_does_not_leak_the_field_name():
    repo = _repo()
    try:
        repo._validate_unharmonized_file_field("secret_field", "metadata.unharmonized.secret_field")
    except UnsupportedFieldError as exc:
        detail = exc.to_error_detail()
        assert "secret_field" not in (detail.message or "")
        assert detail.field == "wrong field"
    else:
        pytest.fail("expected UnsupportedFieldError")


async def test_build_count_query_rejects_injection_payload_in_unharmonized_field():
    """count_for_pagination -> _build_count_query must guard field names too,
    since it runs before get_files and previously bypassed the allowlist check."""
    repo = _repo()
    payload_field = "metadata.unharmonized.guid) RETURN sf //"
    with pytest.raises(InvalidParametersError):
        await repo._build_count_query({payload_field: "x"})


async def test_build_count_query_rejects_unallowlisted_unharmonized_field():
    repo = _repo()
    with pytest.raises(UnsupportedFieldError):
        await repo._build_count_query({"metadata.unharmonized.batch_id": "x"})
