from app.core.diagnosis_category import (
    build_diagnosis_category_fields,
    build_sample_diagnosis_category_fields,
)

PREFIX = "dcc_associated_diagnosis_category"


def test_first_native_wins_rest_go_to_prefixed_keys():
    regular, unh = build_diagnosis_category_fields(
        ["Medulloblastoma", "Renal Tumors"], PREFIX
    )
    assert regular == {"value": "Medulloblastoma"}
    assert unh == {f"{PREFIX}_1": {"value": "Renal Tumors"}}


def test_promoted_alias_ancestor_uses_prefix():
    regular, unh = build_diagnosis_category_fields(["Myeloid leukemias"], PREFIX)
    assert regular["ancestors"] == [f"unharmonized.{PREFIX}_1"]
    assert unh[f"{PREFIX}_1"]["value"] == "Myeloid leukemias"


def test_case_normalized_native_keeps_raw_in_unharmonized():
    regular, unh = build_diagnosis_category_fields(["medulloblastoma"], PREFIX)
    assert regular == {
        "value": "Medulloblastoma",
        "ancestors": [f"unharmonized.{PREFIX}_1"],
    }
    assert unh == {f"{PREFIX}_1": {"value": "medulloblastoma"}}


def test_exact_native_pv_has_no_unharmonized_entry():
    regular, unh = build_diagnosis_category_fields(["Medulloblastoma"], PREFIX)
    assert regular == {"value": "Medulloblastoma"}
    assert unh is None


def test_sample_wrapper_unchanged():
    assert build_sample_diagnosis_category_fields(["Medulloblastoma", "Gliomas"]) == (
        build_diagnosis_category_fields(["Medulloblastoma", "Gliomas"], "dcc_diagnosis_category")
    )


def test_sample_wrapper_does_not_promote_case_variant():
    # /sample is out of scope for FEDERATION-766: a case-variant native stays an exact match.
    regular, unh = build_sample_diagnosis_category_fields(
        ["medulloblastoma", "Adenomas and adenocarcinomas"]
    )
    assert regular == {"value": "Medulloblastoma"}
    assert unh == {"dcc_diagnosis_category_1": {"value": "Adenomas and adenocarcinomas"}}


def test_subject_builder_promotes_case_variant_by_default():
    regular, unh = build_diagnosis_category_fields(
        ["medulloblastoma", "Adenomas and adenocarcinomas"], PREFIX
    )
    assert regular == {
        "value": "Medulloblastoma",
        "ancestors": [f"unharmonized.{PREFIX}_1"],
    }
    assert unh == {
        f"{PREFIX}_1": {"value": "medulloblastoma"},
        f"{PREFIX}_2": {"value": "Adenomas and adenocarcinomas"},
    }
