from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType


@dataclass(frozen=True)
class FileNodeConfig:
    """Configuration for a file node type in the graph.

    Attributes:
        node_label: The graph node label (e.g., 'sequencing_file', 'methylation_array_file').
        rel_name: The relationship name from sample to this file type (e.g., 'of_sequencing_file').
        unharmonized_fields: Maps API unharmonized field name -> DB property name.
                           Empty dict = use default mapping only (f.file_name -> metadata.unharmonized.file_name).
    """
    node_label: str
    rel_name: str
    unharmonized_fields: dict[str, str] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        # Wrap in MappingProxyType so the dict cannot be mutated after construction,
        # and so hash() works correctly on this frozen dataclass.
        object.__setattr__(
            self, "unharmonized_fields", MappingProxyType(self.unharmonized_fields)
        )


FILE_NODE_REGISTRY: list[FileNodeConfig] = [
    FileNodeConfig(
        node_label="methylation_array_file",
        rel_name="of_methylation_array_file",
    ),
    FileNodeConfig(
        node_label="sequencing_file",
        rel_name="of_sequencing_file",
    ),
]
