"""Tests for OpenAPI doc generation normalization."""

import importlib.util
from pathlib import Path

import pytest

_SPEC_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "scripts" / "generate_openapi.py"
)
_spec = importlib.util.spec_from_file_location("generate_openapi", _SPEC_PATH)
assert _spec and _spec.loader
generate_openapi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generate_openapi)


@pytest.mark.unit
def test_normalize_moves_openapi_examples_map_off_schema():
    spec = {
        "paths": {
            "/subject": {
                "get": {
                    "parameters": [
                        {
                            "name": "depositions",
                            "in": "query",
                            "schema": {
                                "type": "string",
                                "examples": {
                                    "default": {
                                        "summary": "Example study_id",
                                        "value": "phs002431",
                                    }
                                },
                            },
                        }
                    ]
                }
            }
        }
    }

    moved = generate_openapi.normalize_parameter_schema_examples(spec)

    param = spec["paths"]["/subject"]["get"]["parameters"][0]
    assert moved == 1
    assert "examples" not in param["schema"]
    assert param["examples"]["default"]["value"] == "phs002431"


@pytest.mark.unit
def test_normalize_leaves_json_schema_examples_array_on_schema():
    spec = {
        "paths": {
            "/sample": {
                "get": {
                    "parameters": [
                        {
                            "name": "name",
                            "in": "query",
                            "schema": {
                                "type": "string",
                                "examples": ["SampleName001"],
                            },
                        }
                    ]
                }
            }
        }
    }

    moved = generate_openapi.normalize_parameter_schema_examples(spec)

    param = spec["paths"]["/sample"]["get"]["parameters"][0]
    assert moved == 0
    assert param["schema"]["examples"] == ["SampleName001"]
    assert "examples" not in param


@pytest.mark.unit
def test_patch_index_html_replaces_remote_spec_urls(tmp_path):
    index_path = tmp_path / "index.html"
    index_path.write_text(
        "url: 'https://cbiit.github.io/ccdi-dcc-federation-service/docs/swagger.yml',\n",
        encoding="utf-8",
    )

    patched = generate_openapi.patch_index_html_for_local_spec(index_path)

    content = index_path.read_text(encoding="utf-8")
    assert patched is True
    assert "./swagger.yml" in content
    assert "cbiit.github.io" not in content
