"""Sample metadata.unharmonized.dcc_* filters match the raw property."""

from unittest.mock import AsyncMock, Mock

import pytest

from app.lib.field_allowlist import FieldAllowlist
from app.repositories.sample import SampleRepository
from app.repositories.sample_helpers import SampleHelpers
from app.repositories.sample_unharmonized_filters import (
    canonical_dcc_filter_name,
    dcc_match_condition,
)


def _repository():
    session = AsyncMock()
    result = AsyncMock()
    result.__aiter__ = Mock(return_value=async_iter_empty())
    result.consume = AsyncMock()
    session.run = AsyncMock(return_value=result)
    allowlist = Mock(spec=FieldAllowlist)
    return SampleRepository(session, allowlist), session


async def async_iter_empty():
    if False:
        yield None


def _query(session):
    call = session.run.call_args
    return call.args[0] if call.args else call.kwargs.get("cypher", "")


def _params(session):
    call = session.run.call_args
    return call.args[1] if len(call.args) > 1 else call.kwargs.get("params", {})


@pytest.mark.unit
def test_numbered_diagnosis_category_key_is_the_same_filter():
    assert canonical_dcc_filter_name(
        "metadata.unharmonized.dcc_diagnosis_category_2"
    ) == ("dcc_diagnosis_category")
    assert (
        canonical_dcc_filter_name("metadata.unharmonized.dcc_diagnosis_category_2 OR 1")
        is None
    )


@pytest.mark.unit
def test_categorize_routes_the_six_fields_and_leaves_other_keys_alone():
    categorized = SampleHelpers._categorize_filters(
        {
            "metadata.unharmonized.dcc_library_strategy": "Archer Fusion",
            "metadata.unharmonized.dcc_preservation_method": "Cytospin Slide",
            "metadata.unharmonized.dcc_disease_phase": "Prior Primary",
            "metadata.unharmonized.dcc_diagnosis_category_1": "Adenomas",
            "metadata.unharmonized.batch_id": "x",
        }
    )
    assert categorized["sequencing_file"]["dcc_library_strategy"] == "Archer Fusion"
    assert categorized["pathology_file"]["dcc_preservation_method"] == "Cytospin Slide"
    assert categorized["diagnosis"]["dcc_disease_phase"] == "Prior Primary"
    assert categorized["diagnosis"]["dcc_diagnosis_category"] == "Adenomas"
    assert categorized["sample"] == {}


@pytest.mark.unit
def test_match_is_case_insensitive_raw_property_and_blank_matches_nothing():
    params = {}
    condition = dcc_match_condition(
        "dcc_library_strategy", "sf", "Archer Fusion||Other", "param_1", params
    )
    assert "toLower(trim(toString(sf.library_strategy))) IN $param_1" == condition
    assert params["param_1"] == ["archer fusion", "other"]
    assert (
        dcc_match_condition("dcc_disease_phase", "d", " || ", "param_2", {}) == "false"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_library_strategy_filter_uses_the_raw_value():
    repository, session = _repository()
    await repository.get_samples(
        {"metadata.unharmonized.dcc_library_strategy": "Archer Fusion"},
        return_total=False,
    )
    query = _query(session)
    params = _params(session)
    assert "toLower(trim(toString(sf.library_strategy))) IN $param_1" in query
    assert params["param_1"] == ["archer fusion"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_disease_phase_filter_uses_the_raw_diagnosis_property():
    repository, session = _repository()
    await repository.get_samples(
        {"metadata.unharmonized.dcc_disease_phase": "Prior Primary"},
        return_total=False,
    )
    query = _query(session)
    assert "toLower(trim(toString(d.disease_phase))) IN $param_1" in query
    assert _params(session)["param_1"] == ["prior primary"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_fallback_applies_the_same_raw_match():
    repository, session = _repository()
    await repository.get_samples_summary(
        {"metadata.unharmonized.dcc_library_strategy": "Archer Fusion"}
    )
    query = _query(session)
    assert "toLower(trim(toString(sf.library_strategy))) IN $param_1" in query
    assert "has_matching_sf" in query
    assert _params(session)["param_1"] == ["archer fusion"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_blank_dcc_value_returns_zero_without_querying():
    """A blank dcc_ value matches nothing on the list side, so the summary is 0 too."""
    repository, session = _repository()
    result = await repository.get_samples_summary(
        {"metadata.unharmonized.dcc_disease_phase": " || "}
    )
    assert result == {"counts": {"total": 0}}
    session.run.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_search_puts_dcc_phase_on_the_search_diagnosis_node():
    """With _diagnosis_search, dcc_disease_phase is ANDed with the search on the same diagnosis."""
    repository, session = _repository()
    await repository.get_samples_summary(
        {
            "_diagnosis_search": "glioma",
            "metadata.unharmonized.dcc_disease_phase": "Relapse",
        }
    )
    query = _query(session)
    assert ["relapse"] in _params(session).values()
    match_lines = [line for line in query.splitlines() if "AS has_matching_diagnosis" in line]
    assert match_lines, query
    assert "toLower(trim(toString(d.disease_phase))) IN" in match_lines[0]
    assert "diagnosis_search_term_lower" in match_lines[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_phase_and_category_share_one_diagnosis_check():
    """dcc_disease_phase and dcc_diagnosis_category must be satisfied by the same diagnosis node."""
    repository, session = _repository()
    await repository.get_samples_summary(
        {
            "metadata.unharmonized.dcc_disease_phase": "Relapse",
            "metadata.unharmonized.dcc_diagnosis_category_1": "Gliomas",
        }
    )
    query = _query(session)
    checks = [
        line
        for line in query.splitlines()
        if "size([d IN all_diagnoses WHERE d IS NOT NULL AND (" in line
        and "d.disease_phase" in line
    ]
    assert len(checks) == 1, query
    assert "d.diagnosis_category" in checks[0]


@pytest.mark.unit
def test_repeated_dcc_key_is_rejected_but_different_keys_are_allowed():
    """A repeated dcc_ key would keep only the last value, so it is a 400 (like harmonized keys)."""
    from fastapi import Request
    from starlette.datastructures import QueryParams

    from app.api.v1.deps import get_sample_filters
    from app.models.errors import InvalidParametersError

    repeated = Mock(spec=Request)
    repeated.query_params = QueryParams(
        "metadata.unharmonized.dcc_disease_phase=Relapse"
        "&metadata.unharmonized.dcc_disease_phase=Unknown"
    )
    with pytest.raises(InvalidParametersError):
        get_sample_filters(request=repeated)

    two_keys = Mock(spec=Request)
    two_keys.query_params = QueryParams(
        "metadata.unharmonized.dcc_disease_phase=Relapse"
        "&metadata.unharmonized.dcc_diagnosis_category_1=Gliomas"
    )
    filters = get_sample_filters(request=two_keys)
    assert filters["metadata.unharmonized.dcc_disease_phase"] == "Relapse"
    assert filters["metadata.unharmonized.dcc_diagnosis_category_1"] == "Gliomas"


def _param_holding(params, value):
    """Name of the param whose value matches ``value`` (case-insensitive, list or scalar)."""
    wanted = value.lower()
    for name, held in params.items():
        values = held if isinstance(held, list) else [held]
        if any(isinstance(one, str) and one.lower() == wanted for one in values):
            return name
    raise AssertionError(f"no param holds {value!r}: {params}")


def _lines_with(query, *fragments):
    return [line for line in query.splitlines() if all(f in line for f in fragments)]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_preservation_only_filter_uses_the_raw_pathology_property():
    """dcc_preservation_method alone takes the pathology-file-first path."""
    repository, session = _repository()
    await repository.get_samples(
        {"metadata.unharmonized.dcc_preservation_method": "Cytospin Slide"},
        return_total=False,
    )
    query = _query(session)
    param = _param_holding(_params(session), "Cytospin Slide")
    assert _lines_with(query, "pf.fixation_embedding_method", f"${param}"), query


@pytest.mark.unit
@pytest.mark.asyncio
async def test_case3_sequencing_dcc_filter_is_anded_into_the_sf_condition():
    """A dcc sequencing-file filter plus a diagnosis filter goes through Case 3."""
    repository, session = _repository()
    await repository.get_samples(
        {
            "metadata.unharmonized.dcc_library_strategy": "Archer Fusion",
            "metadata.unharmonized.dcc_disease_phase": "Relapse",
        },
        return_total=False,
    )
    query = _query(session)
    params = _params(session)
    sf_param = _param_holding(params, "Archer Fusion")
    d_param = _param_holding(params, "Relapse")
    assert _lines_with(query, "sf.library_strategy", f"${sf_param}"), query
    assert _lines_with(query, "d.disease_phase", f"${d_param}"), query


@pytest.mark.unit
@pytest.mark.asyncio
async def test_case3_preservation_dcc_filter_is_on_the_pathology_match():
    repository, session = _repository()
    await repository.get_samples(
        {
            "metadata.unharmonized.dcc_preservation_method": "Cytospin Slide",
            "metadata.unharmonized.dcc_disease_phase": "Relapse",
        },
        return_total=False,
    )
    query = _query(session)
    pf_param = _param_holding(_params(session), "Cytospin Slide")
    assert _lines_with(query, "pf.fixation_embedding_method", f"${pf_param}"), query


@pytest.mark.unit
def test_numbered_diagnosis_category_keys_merge_into_one_list():
    categorized = SampleHelpers._categorize_filters(
        {
            "metadata.unharmonized.dcc_diagnosis_category_1": "Gliomas",
            "metadata.unharmonized.dcc_diagnosis_category_2": "Adenomas",
            "metadata.unharmonized.dcc_diagnosis_category_3": "Lymphomas",
        }
    )
    assert categorized["diagnosis"]["dcc_diagnosis_category"] == [
        "Gliomas",
        "Adenomas",
        "Lymphomas",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_two_diagnosis_category_keys_each_get_their_own_predicate():
    """Two numbered keys are AND'd: one diagnosis_category predicate per value."""
    repository, session = _repository()
    await repository.get_samples(
        {
            "metadata.unharmonized.dcc_diagnosis_category_1": "Gliomas",
            "metadata.unharmonized.dcc_diagnosis_category_2": "Adenomas",
        },
        return_total=False,
    )
    query = _query(session)
    params = _params(session)
    first = _param_holding(params, "Gliomas")
    second = _param_holding(params, "Adenomas")
    assert first != second
    assert _lines_with(query, "d.diagnosis_category", f"${first}"), query
    assert _lines_with(query, "d.diagnosis_category", f"${second}"), query


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_fallback_applies_preservation_dcc_filter():
    repository, session = _repository()
    await repository.get_samples_summary(
        {"metadata.unharmonized.dcc_preservation_method": "Cytospin Slide"}
    )
    query = _query(session)
    pf_param = _param_holding(_params(session), "Cytospin Slide")
    assert _lines_with(query, "pf.fixation_embedding_method", f"${pf_param}"), query


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_dcc_sf_filter_is_anded_with_one_harmonized_sf_filter():
    repository, session = _repository()
    await repository.get_samples_summary(
        {
            "metadata.unharmonized.dcc_library_strategy": "Archer Fusion",
            "library_strategy": "WGS",
        }
    )
    query = _query(session)
    dcc_param = _param_holding(_params(session), "Archer Fusion")
    checks = _lines_with(query, "AS has_matching_sf")
    assert len(checks) == 1, query
    assert "sf.library_strategy = $" in checks[0]
    assert f"${dcc_param}" in checks[0]
    assert ") AND (" in checks[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_summary_dcc_sf_filter_is_anded_with_ored_harmonized_sf_filters():
    """Harmonized sf filters stay OR'd together; the dcc filter ANDs with that group."""
    repository, session = _repository()
    await repository.get_samples_summary(
        {
            "metadata.unharmonized.dcc_library_strategy": "Archer Fusion",
            "library_strategy": "WGS",
            "library_source_material": "Bulk Cells",
        }
    )
    query = _query(session)
    dcc_param = _param_holding(_params(session), "Archer Fusion")
    checks = _lines_with(query, "AS has_matching_sf")
    assert len(checks) == 1, query
    assert " OR " in checks[0]
    assert f"${dcc_param}" in checks[0]
    assert ") AND (" in checks[0]
