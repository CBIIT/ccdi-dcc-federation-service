import pytest
from unittest.mock import Mock, AsyncMock
from neo4j import AsyncSession

from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY
from app.repositories.file import FileRepository
from app.lib.field_allowlist import FieldAllowlist


def make_repo(config=None):
    return FileRepository(
        AsyncMock(spec=AsyncSession),
        Mock(spec=FieldAllowlist),
        config or FILE_NODE_REGISTRY[1],
    )


@pytest.mark.unit
class TestRecordToFile:

    def test_default_config_produces_dcc_entries(self):
        repo = make_repo()
        sf = {"id": "uuid-1", "file_name": "test.fastq", "file_size": 100,
              "file_type": "FASTQ", "md5sum": "abc", "file_description": "desc"}
        result = repo._record_to_file(sf, [], {"study_id": "phs001"})
        unharmonized = result.metadata["unharmonized"]
        assert "file_name" not in unharmonized
        assert unharmonized["dcc_file_name"] == {"value": "test.fastq"}

    def test_type_gets_ancestors_when_raw_type_in_map(self):
        repo = make_repo()
        sf = {"id": "uuid-1", "file_name": "x.psm", "file_type": "psm"}
        result = repo._record_to_file(sf, [], {"study_id": "phs001"})
        assert result.metadata["type"] == {
            "value": "TSV",
            "ancestors": ["unharmonized.dcc_file_type"],
        }

    def test_type_has_no_ancestors_when_raw_type_not_in_map(self):
        repo = make_repo()
        sf = {"id": "uuid-1", "file_name": "x.bam", "file_type": "bam"}
        result = repo._record_to_file(sf, [], {"study_id": "phs001"})
        assert result.metadata["type"] == {"value": "BAM"}
        assert "dcc_file_type" not in result.metadata["unharmonized"]

    def test_type_null_mapping_gets_no_ancestors(self, monkeypatch):
        repo = make_repo()
        monkeypatch.setattr(repo, "_map_file_type_to_enum", lambda raw: None)
        sf = {"id": "uuid-1", "file_name": "x.bw", "file_type": "bw"}
        result = repo._record_to_file(sf, [], {"study_id": "phs001"})
        assert result.metadata["type"] is None
        assert "dcc_file_type" in result.metadata["unharmonized"]

    def test_single_and_list_share_converter(self):
        repo = make_repo()
        sf = {"id": "uuid-1", "file_name": "x.psm", "file_type": "psm"}
        dumped = repo._record_to_file(sf, [], {"study_id": "phs001"}).model_dump()
        assert dumped["metadata"]["type"]["ancestors"] == ["unharmonized.dcc_file_type"]

    def test_empty_unharmonized_fields_config_no_extra_keys(self):
        repo = make_repo()
        sf = {"id": "uuid-1", "file_name": "x.bam", "methylation_platform": "HM450"}
        result = repo._record_to_file(sf, [], {"study_id": "phs001"})
        unharmonized = result.metadata.get("unharmonized") or {}
        assert "methylation_platform" not in unharmonized

    def test_config_with_extra_field_includes_it_when_present(self):
        cfg = FileNodeConfig(
            node_label="methylation_array_file",
            rel_name="of_methylation_array_file",
            unharmonized_fields={"methylation_platform": "methylation_platform"},
        )
        repo = make_repo(cfg)
        sf = {"id": "uuid-2", "file_name": "sample.idat",
              "methylation_platform": "HM450"}
        result = repo._record_to_file(sf, [], {"study_id": "phs002"})
        unharmonized = result.metadata["unharmonized"]
        assert "methylation_platform" in unharmonized
        assert unharmonized["methylation_platform"] == {"value": "HM450"}

    def test_extra_field_absent_from_db_not_included(self):
        cfg = FileNodeConfig(
            node_label="methylation_array_file",
            rel_name="of_methylation_array_file",
            unharmonized_fields={"methylation_platform": "methylation_platform"},
        )
        repo = make_repo(cfg)
        sf = {"id": "uuid-3", "file_name": "sample.idat"}  # no methylation_platform
        result = repo._record_to_file(sf, [], {"study_id": "phs002"})
        unharmonized = result.metadata.get("unharmonized") or {}
        assert "methylation_platform" not in unharmonized
