#!/usr/bin/env python3
"""Generate OpenAPI JSON and Swagger YAML under docs/ (deploy-docs CI workflow)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from app.main import app

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs"
INDEX_HTML = DOCS_DIR / "index.html"
RAW_GITHUB_SPEC_URL = (
    "https://raw.githubusercontent.com/CBIIT/ccdi-dcc-federation-service/main/swagger.yml"
)
LOCAL_SPEC_PATH = "./swagger.yml"


def main() -> None:
    openapi_spec = app.openapi()

    with (DOCS_DIR / "openapi.json").open("w", encoding="utf-8") as handle:
        json.dump(openapi_spec, handle, indent=2)
        handle.write("\n")

    with (DOCS_DIR / "swagger.yml").open("w", encoding="utf-8") as handle:
        yaml.dump(
            openapi_spec,
            handle,
            default_flow_style=False,
            sort_keys=False,
        )

    if INDEX_HTML.is_file():
        content = INDEX_HTML.read_text(encoding="utf-8")
        if RAW_GITHUB_SPEC_URL in content:
            INDEX_HTML.write_text(
                content.replace(RAW_GITHUB_SPEC_URL, LOCAL_SPEC_PATH),
                encoding="utf-8",
            )

    print("OpenAPI spec generated successfully")


if __name__ == "__main__":
    main()
