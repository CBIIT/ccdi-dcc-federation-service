from __future__ import annotations

from fastapi.routing import APIRoute

from tests.test_contract_subjects import COVERED_ROUTES as SUBJECT_ROUTES
from tests.test_contract_samples import COVERED_ROUTES as SAMPLE_ROUTES
from tests.test_contract_files import COVERED_ROUTES as FILE_ROUTES
from tests.test_contract_misc import COVERED_ROUTES as MISC_ROUTES


def _iter_api_v1_routes(app):
    for r in app.routes:
        if not isinstance(r, APIRoute):
            continue
        if not r.path.startswith("/api/v1"):
            continue
        methods = set(r.methods or set()) - {"HEAD", "OPTIONS"}
        for m in methods:
            yield (m, r.path)


def test_all_api_v1_routes_are_registered_in_contract_tests(app):
    """
    Registry-style guard:
    every /api/v1 route must be explicitly listed in a COVERED_ROUTES set in tests/.

    When adding/removing endpoints, update:
    - tests/test_contract_subjects.py
    - tests/test_contract_samples.py
    - tests/test_contract_files.py
    - tests/test_contract_misc.py
    """
    covered = set().union(SUBJECT_ROUTES, SAMPLE_ROUTES, FILE_ROUTES, MISC_ROUTES)
    actual = set(_iter_api_v1_routes(app))

    missing = sorted(actual - covered)
    assert not missing, (
        "Some /api/v1 routes are missing contract-test registry entries.\n"
        "Add them to the appropriate tests/test_contract_*.py COVERED_ROUTES set:\n"
        + "\n".join([f"- {m} {p}" for (m, p) in missing])
    )


