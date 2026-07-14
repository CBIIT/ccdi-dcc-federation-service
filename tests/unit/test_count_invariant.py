"""Tests for the sample-count invariant check.

Counts on `/sample/by/{field}/count` are per (sample_id, study_id) pair. The invariant
deliberately does NOT assert `total == values_sum + missing`, because that identity is
false by design for any field where one pair can carry several values.
"""

import pytest

from app.repositories.sample_count import check_count_invariant


@pytest.mark.unit
class TestCountInvariant:
    """check_count_invariant returns violations, empty when the counts are sound."""

    def test_single_value_field_reconciles_exactly(self):
        """A scalar sample-node field: every pair has exactly one value."""
        # tissue_type on live data: total == values_sum, nothing missing.
        assert check_count_invariant(total=73510, values_sum=73510, missing=0) == []

    @pytest.mark.parametrize(
        "field,total,values_sum,missing",
        [
            # Live numbers. Each pair can hold several values, so values_sum + missing
            # exceeds total. The old `total == values_sum + missing` check warned on every
            # one of these, on every request.
            ("anatomical_sites", 73510, 80820, 0),        # list property
            ("library_strategy", 73510, 63005, 16791),    # many sequencing_files
            ("library_selection_method", 73510, 62726, 16791),
            ("library_source_material", 73510, 54475, 19213),
            ("specimen_molecular_analyte_type", 73510, 48021, 26658),
            ("diagnosis", 73510, 18813, 54746),           # many diagnoses
            ("diagnosis_category", 73510, 10713, 62821),
        ],
    )
    def test_multi_value_fields_are_not_flagged(self, field, total, values_sum, missing):
        """values_sum + missing > total is expected, not a defect."""
        assert check_count_invariant(total, values_sum, missing) == [], field

    def test_all_pairs_missing(self):
        """tumor_tissue_morphology on live data: no pair has a value."""
        assert check_count_invariant(total=73510, values_sum=0, missing=73510) == []

    def test_catches_missing_undercount(self):
        """The real latent bug: `missing` counting sample NODES where total counts PAIRS.

        One sample node reaching two studies with no diagnosis is two pairs, both missing.
        A missing query using count(DISTINCT sa) sees one node and reports missing=1, so a
        pair is neither counted as missing nor represented in values.
        """
        violations = check_count_invariant(total=2, values_sum=0, missing=1)

        assert violations, "an unaccounted-for pair must be flagged"
        assert any("non-missing pairs" in v for v in violations)
        # The correctly-counted version must not warn.
        assert check_count_invariant(total=2, values_sum=0, missing=2) == []

    def test_catches_missing_exceeding_total(self):
        """missing can never exceed the pair count."""
        violations = check_count_invariant(total=10, values_sum=5, missing=12)

        assert any("outside [0, total" in v for v in violations)

    def test_catches_negative_missing(self):
        violations = check_count_invariant(total=10, values_sum=10, missing=-1)

        assert any("outside [0, total" in v for v in violations)

    def test_catches_values_present_while_all_pairs_missing(self):
        """If every pair is missing, no value can have been counted."""
        violations = check_count_invariant(total=10, values_sum=3, missing=10)

        assert any("disagree on whether" in v for v in violations)

    def test_catches_no_values_while_pairs_are_non_missing(self):
        """If some pair is non-missing, at least one value must have been counted."""
        violations = check_count_invariant(total=10, values_sum=0, missing=4)

        assert violations
        assert any("disagree on whether" in v for v in violations)

    def test_empty_result_set_is_sound(self):
        """A field with no data at all: zeroes are consistent."""
        assert check_count_invariant(total=0, values_sum=0, missing=0) == []
