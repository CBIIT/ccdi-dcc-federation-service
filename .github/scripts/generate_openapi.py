#!/usr/bin/env python3
"""Generate OpenAPI JSON and Swagger YAML under docs/ (deploy-docs CI workflow)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from app.main import app

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs"
INDEX_HTML = DOCS_DIR / "index.html"
LOCAL_SPEC_PATH = "./swagger.yml"
# Remote spec URLs in index.html that should load the co-located swagger.yml artifact.
REMOTE_SPEC_URLS = (
    "https://raw.githubusercontent.com/CBIIT/ccdi-dcc-federation-service/main/swagger.yml",
    "https://cbiit.github.io/ccdi-dcc-federation-service/docs/swagger.yml",
)

_HTTP_METHODS = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)


def _iter_operations(spec: dict[str, Any]):
    """Yield each HTTP operation dict from spec['paths']."""
    for path_item in (spec.get("paths") or {}).values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in _HTTP_METHODS or not isinstance(operation, dict):
                continue
            yield operation


def _is_openapi_examples_map(examples: Any) -> bool:
    """True when `examples` is an OpenAPI Example map (invalid on JSON Schema)."""
    if not isinstance(examples, dict) or not examples:
        return False
    for value in examples.values():
        if isinstance(value, dict) and any(
            key in value for key in ("summary", "description", "value", "externalValue")
        ):
            return True
    return False


def _normalize_parameter_list(parameters: list[Any]) -> int:
    moved = 0
    for param in parameters:
        if not isinstance(param, dict):
            continue
        schema = param.get("schema")
        if not isinstance(schema, dict):
            continue
        examples = schema.get("examples")
        if not _is_openapi_examples_map(examples):
            continue
        del schema["examples"]
        param.setdefault("examples", examples)
        moved += 1
    return moved


def normalize_parameter_schema_examples(spec: dict[str, Any]) -> int:
    """Move OpenAPI Example maps from parameter.schema.examples to parameter.examples."""
    moved = 0
    paths = spec.get("paths")
    if not isinstance(paths, dict):
        return moved

    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        path_params = path_item.get("parameters")
        if isinstance(path_params, list):
            moved += _normalize_parameter_list(path_params)

        for method, operation in path_item.items():
            if method not in _HTTP_METHODS or not isinstance(operation, dict):
                continue
            op_params = operation.get("parameters")
            if isinstance(op_params, list):
                moved += _normalize_parameter_list(op_params)

    return moved


def normalize_validation_error_status(spec: dict[str, Any]) -> int:
    """Rename HTTP 422 → 400 in all path responses to match the runtime handler."""
    renamed = 0
    for operation in _iter_operations(spec):
        responses = operation.get("responses")
        if not isinstance(responses, dict):
            continue
        entry = responses.pop("422", None)
        if entry is not None:
            responses.setdefault("400", entry)
            renamed += 1
    return renamed


def patch_index_html_for_local_spec(index_path: Path = INDEX_HTML) -> bool:
    """Point Swagger UI at ./swagger.yml for local preview and CI artifacts."""
    if not index_path.is_file():
        return False
    content = index_path.read_text(encoding="utf-8")
    patched = content
    changed = False
    for remote_url in REMOTE_SPEC_URLS:
        if remote_url in patched:
            patched = patched.replace(remote_url, LOCAL_SPEC_PATH)
            changed = True
    if not changed:
        return False
    index_path.write_text(patched, encoding="utf-8")
    return True


def main() -> None:
    openapi_spec = app.openapi()
    moved = normalize_parameter_schema_examples(openapi_spec)
    if moved:
        print(
            f"Normalized {moved} parameter example(s) from schema.examples to parameter.examples"
        )
    renamed = normalize_validation_error_status(openapi_spec)
    if renamed:
        print(f"Renamed {renamed} response(s) from HTTP 422 to 400")

    with (DOCS_DIR / "openapi.json").open("w", encoding="utf-8") as handle:
        json.dump(openapi_spec, handle, indent=2)
        handle.write("\n")

    with (DOCS_DIR / "swagger.yml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            openapi_spec,
            handle,
            default_flow_style=False,
            sort_keys=False,
        )

    if patch_index_html_for_local_spec():
        print(f"Patched {INDEX_HTML.name} to use {LOCAL_SPEC_PATH}")

    print("OpenAPI spec generated successfully")


if __name__ == "__main__":
    main()
