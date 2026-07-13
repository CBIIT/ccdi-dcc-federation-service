from unittest.mock import MagicMock
from app.repositories.file import FileRepository
from app.lib.field_allowlist import FieldAllowlist
from app.config_data.file_node_registry import FILE_NODE_REGISTRY


def _repo() -> FileRepository:
    allowlist = FieldAllowlist()
    allowlist.load_from_database()
    return FileRepository(MagicMock(), allowlist, FILE_NODE_REGISTRY[0])


def test_record_to_file_uses_guid_for_id():
    repo = _repo()
    record = {"guid": "f8422193-56f2-5e3e-877d-514854ae4096", "file_name": "x.bam"}
    result = repo._record_to_file(record, samples=[], study=None)
    assert result.id["name"] == "f8422193-56f2-5e3e-877d-514854ae4096"
