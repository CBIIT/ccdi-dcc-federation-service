import pytest
from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY


@pytest.mark.unit
class TestFileNodeRegistry:

    def test_file_node_config_has_required_fields(self):
        cfg = FileNodeConfig(node_label="sequencing_file", rel_name="of_sequencing_file")
        assert cfg.node_label == "sequencing_file"
        assert cfg.rel_name == "of_sequencing_file"
        assert cfg.unharmonized_fields == {}

    def test_file_node_config_with_unharmonized_fields(self):
        cfg = FileNodeConfig(
            node_label="methylation_array_file",
            rel_name="of_methylation_array_file",
            unharmonized_fields={"methylation_platform": "methylation_platform"},
        )
        assert cfg.unharmonized_fields == {"methylation_platform": "methylation_platform"}

    def test_file_node_config_is_immutable(self):
        cfg = FileNodeConfig(node_label="sequencing_file", rel_name="of_sequencing_file")
        with pytest.raises((AttributeError, TypeError)):
            cfg.node_label = "other"
        # Also verify the dict interior cannot be mutated
        with pytest.raises(TypeError):
            cfg.unharmonized_fields["injected"] = "value"

    def test_registry_has_two_entries(self):
        assert len(FILE_NODE_REGISTRY) == 2

    def test_registry_methylation_first(self):
        assert FILE_NODE_REGISTRY[0].node_label == "methylation_array_file"
        assert FILE_NODE_REGISTRY[0].rel_name == "of_methylation_array_file"

    def test_registry_sequencing_second(self):
        assert FILE_NODE_REGISTRY[1].node_label == "sequencing_file"
        assert FILE_NODE_REGISTRY[1].rel_name == "of_sequencing_file"

    def test_registry_all_have_empty_unharmonized_fields(self):
        # Both entries ship empty in this release; fields are deferred to a future release.
        # When unharmonized_fields are populated, update or remove this test accordingly.
        for cfg in FILE_NODE_REGISTRY:
            assert cfg.unharmonized_fields == {}
