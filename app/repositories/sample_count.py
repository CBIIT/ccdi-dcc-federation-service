"""
Count methods for SampleRepository.

This module contains methods for counting samples by field values.
"""

import asyncio
from typing import Any

from app.core.diagnosis_category import HARMONIZED_DIAGNOSIS_CATEGORIES
from app.core.field_mappings import (
    build_case_mapping_statement,
    build_invalid_value_all_clause,
    build_invalid_value_filter,
    build_invalid_value_list_filter,
    get_mapped_db_values,
    is_null_mapped_value,
    load_sequencing_file_enum,
    map_field_value,
)
from app.core.logging import get_logger
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)

_HARMONIZED_PVS_SORTED: list[str] = sorted(HARMONIZED_DIAGNOSIS_CATEGORIES)
_HARMONIZED_PVS_LOWER: list[str] = [pv.lower() for pv in _HARMONIZED_PVS_SORTED]


class SampleCount:
    """Mixin class providing count methods for SampleRepository."""

    async def count_samples_by_field(
        self,
        field: str
    ) -> dict[str, Any]:
        """
        Count samples grouped by a specific field value.

        Counts are always unfiltered: ``GET /sample/by/{field}/count`` rejects
        every query parameter with ``InvalidParametersError``, so there is no
        filter state to apply.

        Args:
            field: Field to group by and count

        Returns:
            List of dictionaries with value and count

        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug("Counting samples by field", field=field)

        # Special handling for diagnosis field - use dedicated method with conversion logic
        if field == "diagnosis":
            return await self._count_samples_by_associated_diagnoses()

        # Special handling for diagnosis_category — harmonized PV count
        if field == "diagnosis_category":
            return await self._count_samples_by_diagnosis_category()

        # Validate field is allowed for count operations
        # Only sample-specific metadata fields are allowed (participant fields are not supported for samples)
        sample_metadata_fields = {
            "disease_phase", "anatomical_sites", "library_selection_method", "library_strategy",
            "library_source_material", "preservation_method", "tumor_grade", "specimen_molecular_analyte_type",
            "tissue_type", "tumor_classification", "age_at_diagnosis", "age_at_collection",
            "tumor_tissue_morphology", "diagnosis", "diagnosis_category"
        }
        allowed_fields = sample_metadata_fields
        if field not in allowed_fields:
            raise UnsupportedFieldError(
                field=field,
                entity_type="sample"
            )

        # Sample metadata field mapping - maps to the actual node and property
        sample_metadata_field_mapping = {
            "disease_phase": ("d", "disease_phase"),  # From diagnosis node
            "anatomical_sites": ("sa", "anatomic_site"),  # From sample node
            "library_selection_method": ("sf", "library_selection"),  # From sequencing_file node
            "library_strategy": ("sf", "library_strategy"),  # From sequencing_file node
            "library_source_material": ("sf", "library_source_material"),  # From sequencing_file node
            "preservation_method": ("pf", "fixation_embedding_method"),  # From pathology_file node
            "tumor_grade": ("d", "tumor_grade"),  # From diagnosis node
            "specimen_molecular_analyte_type": ("sf", "library_source_molecule"),  # From sequencing_file node
            "tissue_type": ("sa", "sample_tumor_status"),  # From sample node (sample_tumor_status field)
            "tumor_classification": ("sa", "tumor_spatial_extent"),  # From sample node
            "age_at_diagnosis": ("d", "age_at_diagnosis"),  # From diagnosis node
            "age_at_collection": ("sa", "participant_age_at_collection"),  # From sample node
            "tumor_tissue_morphology": ("d", "tumor_tissue_morphology"),  # From diagnosis node
            "depositions": ("st", "study_id"),  # From study node
            "diagnosis": ("d", "diagnosis")  # From diagnosis node
        }

        is_sample_metadata_field = field in sample_metadata_field_mapping

        # Flag to track if we're using a combined query (total + missing + values in one query)
        # Currently only used for library_source_material when no filters
        is_combined_query = False

        node_alias, property_name = sample_metadata_field_mapping[field]
        node_field = f"{node_alias}.{property_name}"

        # Counts are always unfiltered: GET /sample/by/{field}/count rejects every
        # query parameter with InvalidParametersError, so no filter state reaches here.
        params: dict[str, Any] = {}

        # Standard field handling
        if field == "anatomical_sites":
            # anatomic_site is a list property on the sample node: unwind it and count
            # each value. Counting unit is the (sample_id, study_id) pair -- the same unit
            # used by this field's total/missing queries and by every other counted field.
            # Two study paths are collected:
            #   sample -> cell_line -> study
            #   sample -> participant -> consent_group -> study
            # `st2_list + st1_list` is list CONCATENATION, not union: a sample reaching the
            # same study via BOTH paths yields that study twice. The `WITH DISTINCT` below
            # is what collapses that (and the second one collapses to pair+value), so the
            # count stays pair-unit. Do not drop either DISTINCT.
            # A sample with no path to a study is excluded.
            cypher = """
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            WITH DISTINCT sa.sample_id AS sample_id, sid AS study_id, sa.anatomic_site AS sites
            WHERE sites IS NOT NULL
            UNWIND sites AS site_value
            WITH sample_id, study_id, trim(toString(site_value)) AS trimmed_value
            WHERE trimmed_value <> ''
              AND toLower(trimmed_value) <> 'invalid value'
            WITH DISTINCT sample_id, study_id, trimmed_value
            RETURN trimmed_value as value, count(*) AS count
            ORDER BY count DESC, value ASC
            """.strip()
        else:
            # Every remaining countable field lives on the diagnosis (d), sample (sa),
            # sequencing_file (sf) or pathology_file (pf) node -- see
            # sample_metadata_field_mapping. `depositions` (st) is rejected above as an
            # unsupported count field and `diagnosis` is diverted to its own method, so
            # the node_alias branches below are exhaustive.
            if is_sample_metadata_field:
                # For specimen_molecular_analyte_type, use a different query structure:
                # 1. First find samples with valid study
                # 2. Then find sequencing_file associated with these samples
                # 3. Then categorize by specimen_molecular_analyte_type per sample
                if field == "specimen_molecular_analyte_type":
                    # Optimized query for specimen_molecular_analyte_type:
                    # 1. Start from sequencing_file (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Map DB values to API values in Cypher using CASE statement
                    # 4. Group by API value and count distinct samples directly in Cypher
                    # Performance improvements:
                    # - Start from sequencing_file instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Do mapping and counting in Cypher (eliminates Python-side processing overhead)
                    # - Returns aggregated results directly (much fewer rows)
                    # Optimized: Start from sample (like other sequencing_file fields), collect distinct values per sample first
                    # Then map the collected values (much fewer rows to process in CASE statement)
                    # Profile showed CASE evaluation on 1.1M rows was 30% of time - this reduces that significantly
                    # Strategy: Collect distinct molecule values per sample → Map → Deduplicate → Aggregate
                    # Note: IN clause uses mapped DB values from field_mappings.json, CASE statement built dynamically
                    mapped_db_values = get_mapped_db_values(field)
                    if not mapped_db_values:
                        # No mappings configured, return empty results
                        cypher = "RETURN '' as value, 0 AS count LIMIT 0"
                    else:
                        # Build CASE statement dynamically from mappings
                        case_statement = build_case_mapping_statement(field, "molecule_value")
                        if not case_statement:
                            # No mappings, return empty results
                            cypher = "RETURN '' as value, 0 AS count LIMIT 0"
                        else:
                            # COMBINED QUERY: Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Performance: Collect → Filter → Map → Calculate total/missing/values
                            # Use parameterized query for better index usage
                            params["mapped_db_values"] = mapped_db_values
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT sf.library_source_molecule) as molecule_values
                WITH sample_id, study_id, molecule_values,
                     // Filter to only mapped DB values using parameterized query
                     [val IN molecule_values WHERE val IS NOT NULL AND val IN $mapped_db_values] as mapped_db_values_list,
                     // For MISSING: Check if no valid values (NULL/empty/-999/null_mappings)
                     CASE WHEN size([val IN molecule_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                // IMPORTANT: Calculate BEFORE unwinding to get correct totals
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, mapped_db_values_list: mapped_db_values_list}}) as all_samples
                // Now unwind mapped values and map to API values
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.mapped_db_values_list as mapped_db_values_list,
                     total, missing,
                     CASE WHEN size(sample_data.mapped_db_values_list) = 0 THEN [null] ELSE sample_data.mapped_db_values_list END as values_to_unwind
                UNWIND values_to_unwind as molecule_value
                WITH sample_id, study_id, molecule_value, total, missing
                WHERE molecule_value IS NOT NULL  // Filter out the null placeholder rows
                WITH sample_id, study_id,
                     {case_statement} as api_value,
                     total, missing
                WHERE api_value IS NOT NULL
                // Deduplicate by (sample_id, study_id, api_value) before counting
                WITH DISTINCT sample_id, study_id, api_value, total, missing
                WITH api_value as value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                elif node_alias == "sf":
                    # Special handling for library_source_material: Combined query (total + missing + values)
                    # For other sequencing_file fields: Use separate queries
                    if field == "library_source_material":
                        # COMBINED QUERY: Returns total, missing, and values in one pass
                        # This avoids running 3 separate queries and processes samples once
                        # IMPORTANT: Missing check should NOT include enum validation - only check for NULL/empty/-999/null_mappings
                        # Values check SHOULD include enum validation to only count valid enum values
                        invalid_list_filter = build_invalid_value_list_filter(field)
                        # Load enum values to filter FOR valid values in the values query
                        enum_values = load_sequencing_file_enum("library_source_material")
                        if enum_values:
                            # Validate enum values don't contain dangerous characters
                            # This is a security safeguard even though enum values come from JSON files
                            dangerous_chars = ['"', "'", '\\', '`', '{', '}', '[', ']']
                            for enum_val in enum_values:
                                if any(char in str(enum_val) for char in dangerous_chars):
                                    logger.error(
                                        "Invalid enum value contains dangerous characters",
                                        field=field,
                                        enum_value=enum_val,
                                        dangerous_chars=[char for char in dangerous_chars if char in str(enum_val)]
                                    )
                                    raise ValueError(f"Enum value '{enum_val}' contains dangerous characters")

                            # Use parameterized query instead of string interpolation for security
                            # Pass enum_values as a parameter to prevent Cypher injection
                            params["enum_values"] = enum_values
                            # Separate logic:
                            # - valid_values: Filter for enum values AND exclude null_mappings (for counting values)
                            # - is_missing: Only check if no valid values exist (NULL/empty/-999/null_mappings), WITHOUT enum check
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid enum values AND exclude null_mappings
                     // Use parameterized query ($enum_values) instead of string interpolation for security
                     [val IN field_values WHERE val IS NOT NULL
                      AND {invalid_list_filter}
                      AND val IN $enum_values] as valid_values,
                     // For MISSING: Only check if no valid values (NULL/empty/-999/null_mappings), WITHOUT enum check
                     // This matches the original missing query logic
                     // IMPORTANT: null_mappings like "Other" are excluded by invalid_list_filter (val <> 'Other')
                     // So samples with only "Other" will have empty filtered list → size = 0 → counted as missing ✓
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                // IMPORTANT: The original missing query filters samples WHERE size(...) = 0
                // Our combined query calculates is_missing per sample, then sums
                // These should be equivalent, but we need to ensure we're grouping correctly
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                        else:
                            # Fallback to original approach if enum not available
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     [val IN field_values WHERE val IS NOT NULL
                      AND {invalid_list_filter}] as valid_values,
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                        # Mark this as a combined query so we can handle it differently
                        is_combined_query = True
                    else:
                        # Check if this field should use combined query for better performance
                        if field == "library_strategy":
                            # COMBINED QUERY for library_strategy:
                            # Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Requirements:
                            # - Count samples by unique (sample_id + study_id) pairs
                            # - Total: all samples with study paths
                            # - Missing: samples without valid values (no sequencing_file or all invalid)
                            # - Values: samples with valid values, grouped by strategy
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999)
                     [val IN field_values WHERE val IS NOT NULL
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                        else:
                            # COMBINED QUERY for library_selection_method:
                            # Returns total, missing, and values in one pass
                            # This avoids running 3 separate queries and processes samples once
                            # Requirements:
                            # - Count samples by unique (sample_id + study_id) pairs
                            # - Total: all samples with study paths
                            # - Missing: samples without valid values (no sequencing_file or all invalid)
                            # - Values: samples with valid values, grouped by selection method
                            # Constraint: Total ≤ Values_sum + Missing
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999, not null_mappings)
                     [val IN field_values WHERE val IS NOT NULL
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, toString(val) as value, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Deduplicate by (sample_id, study_id, value) before counting
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                            # Mark this as a combined query so we can handle it differently
                            is_combined_query = True
                elif node_alias == "pf":
                    # COMBINED QUERY for pathology_file fields (preservation_method):
                    # Returns total, missing, and values in one pass
                    # This avoids running 3 separate queries and processes samples once
                    # Requirements:
                    # - Count samples by unique (sample_id + study_id) pairs
                    # - Total: all samples with study paths
                    # - Missing: samples without valid values (no pathology_file or all invalid)
                    # - Values: samples with valid values, grouped by method
                    invalid_list_filter = build_invalid_value_list_filter(field)
                    # Map DB values to API values IN Cypher (like specimen_molecular_analyte_type)
                    # so we can dedup by the API value and not double-count a (sample, study) pair
                    # whose raw values collapse to one API value (e.g. Cryopreservation variants
                    # -> 'Cryopreserved'). ELSE passes unmapped values through unchanged.
                    case_statement = build_case_mapping_statement(field, "val")
                    # Guard: build_case_mapping_statement returns "" when the field has no
                    # `mappings` — interpolating that would render malformed Cypher
                    # ("WITH ...,  as value"). With no mappings the raw value IS the API value,
                    # so dedup on it directly (equivalent to the pre-mapping behaviour).
                    value_expr = case_statement if case_statement else "toString(val)"
                    cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH sample_id, study_id, field_values,
                     // For VALUES: Filter for valid values (not null, not empty, not -999)
                     [val IN field_values WHERE val IS NOT NULL
                      AND {invalid_list_filter}] as valid_values,
                     // For MISSING: Check if no valid values exist
                     CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                     AND {invalid_list_filter}]) = 0
                          THEN 1 ELSE 0 END as is_missing
                // Calculate total and missing counts (aggregate across all samples)
                WITH count(*) as total,
                     sum(is_missing) as missing,
                     collect({{sample_id: sample_id, study_id: study_id, valid_values: valid_values}}) as all_samples
                // Now unwind valid values to count by value
                // IMPORTANT: total and missing are calculated per (sample_id, study_id) pair
                // and should be preserved through UNWIND
                UNWIND all_samples as sample_data
                WITH sample_data.sample_id as sample_id,
                     sample_data.study_id as study_id,
                     sample_data.valid_values as valid_values,
                     total, missing,
                     CASE WHEN size(sample_data.valid_values) = 0 THEN [null] ELSE sample_data.valid_values END as values_to_unwind
                UNWIND values_to_unwind as val
                WITH sample_id, study_id, val, total, missing
                WHERE val IS NOT NULL  // Filter out the null placeholder rows
                // Map raw DB value -> API value, then dedup by (sample_id, study_id, api_value).
                // Deduping on the RAW value double-counted a pair holding two distinct raw values
                // that collapse to one API value (e.g. two Cryopreservation variants -> 'Cryopreserved');
                // mapping-then-dedup counts that pair once per API value.
                WITH sample_id, study_id, {value_expr} as value, total, missing
                WHERE value IS NOT NULL
                WITH DISTINCT sample_id, study_id, value, total, missing
                WITH value, count(*) as count, total, missing
                RETURN value, count, total, missing
                ORDER BY count DESC, value ASC
                """.strip()
                    # Mark this as a combined query so we can handle it differently
                    is_combined_query = True
                elif node_alias == "d":
                    # Optimized query for diagnosis fields (disease_phase, tumor_grade, etc.):
                    # 1. Start from diagnosis (more selective) and filter invalid values early
                    # 2. Match to sample and check study path
                    # 3. Use head() to get one diagnosis per sample, then group by field value
                    # Performance improvements:
                    # - Start from diagnosis instead of sample (fewer nodes to process)
                    # - Filter invalid values early before study path check
                    # - Simplify redundant WHERE conditions (assume string fields)
                    invalid_filter = build_invalid_value_filter(node_field, field)
                    cypher = f"""
                MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                  AND {invalid_filter}
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, d, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, d, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, d, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                WITH sa, d, toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id
                WITH sample_id, study_id,
                     head(collect(DISTINCT {node_field})) as value
                WITH DISTINCT sample_id, study_id, value
                WHERE value IS NOT NULL
                  AND toString(value) <> ''
                  AND trim(toString(value)) <> ''
                  AND toString(value) <> '-999'
                  AND trim(toString(value)) <> '-999'
                RETURN toString(value) as value, count(*) AS count
                ORDER BY count DESC, value ASC
                """.strip()
                else:  # node_alias == "sa"
                    # Optimized query for sample node fields (tissue_type, age_at_collection):
                    # 1. Filter invalid values early
                    # 2. Group by field value and count distinct samples
                    # Performance improvements:
                    # - Consistent query structure with total and missing queries
                    cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                  AND {node_field} IS NOT NULL
                  AND toString({node_field}) <> ''
                  AND trim(toString({node_field})) <> ''
                  AND toString({node_field}) <> '-999'
                  AND trim(toString({node_field})) <> '-999'
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                WITH sa.sample_id as sample_id, st.study_id as study_id, {node_field} as value
                RETURN toString(value) as value, count(*) AS count
                ORDER BY count DESC, value ASC
                """.strip()

        logger.info(
            "Executing count_samples_by_field Cypher query",
            cypher=cypher[:200],
            params=params,
            field=field
        )

        # Execute query with proper result consumption
        records = []
        try:
            result = await self.session.run(cypher, params)
            async for record in result:
                records.append(dict(record))
        except Exception as e:
            logger.error(
                "Error executing count_samples_by_field Cypher query",
                error=str(e),
                error_type=type(e).__name__,
                field=field,
                cypher=cypher[:500],
                params=params,
                exc_info=True
            )
            raise

        logger.info(
            "Count query results",
            field=field,
            records_count=len(records),
            sample_records=records[:5] if records else [],
            query=cypher[:500] if field == "anatomical_sites" else None
        )

        # Format results
        counts = []

        # Special handling for combined query (library_source_material)
        # Combined query returns: value, count, total, missing
        # Extract total and missing from first row, then process values
        # IMPORTANT: If all samples have no valid values, records will be empty
        # In that case, we need to run a separate query to get total and missing
        total = 0
        missing = 0
        if is_combined_query:
            if records:
                # Extract total and missing from first row (they're the same in all rows)
                total = records[0].get("total", 0)
                missing = records[0].get("missing", 0)
                logger.debug(
                    "Combined query results",
                    field=field,
                    total=total,
                    missing=missing,
                    records_count=len(records)
                )
                # Process values (skip total/missing columns). Multiple DB values can
                # map to the same API value, so aggregate after applying mappings.
                mapped_counts: dict[str, int] = {}
                for record in records:
                    value = record.get("value")
                    count = record.get("count", 0)

                    if not value or count == 0:
                        continue

                    # specimen_molecular_analyte_type and preservation_method are mapped to the API
                    # value IN Cypher and deduped by (sample, study, api_value); raw null-mapped
                    # values were already excluded in the query. So take the value as-is and do NOT
                    # re-apply map_field_value / is_null_mapped_value here — preservation's "Unknown"
                    # API bucket (from Cytospin Slide/Other) collides with its "Unknown" null_mapping
                    # and would otherwise be wrongly dropped.
                    if field in ("specimen_molecular_analyte_type", "preservation_method"):
                        mapped_value = value  # Already mapped + null-filtered in Cypher
                    else:
                        mapped_value = map_field_value(field, value)
                        # Explicitly filter values in null_mappings (e.g. "Other" for
                        # library_source_material) — counted as missing, not a bucket.
                        if is_null_mapped_value(field, value):
                            continue

                    # If mapping returns None, skip this value (it should be counted as missing).
                    if mapped_value is None:
                        continue

                    mapped_counts[mapped_value] = mapped_counts.get(mapped_value, 0) + count

                counts.extend(
                    {"value": value, "count": count}
                    for value, count in mapped_counts.items()
                )
                counts.sort(key=lambda x: (-x["count"], x["value"]))

                # Aggregate counts for fields where multiple DB values map to the same API value
                aggregated_counts = {}
                for item in counts:
                    val = item["value"]
                    cnt = item["count"]
                    if val in aggregated_counts:
                        aggregated_counts[val] += cnt
                    else:
                        aggregated_counts[val] = cnt
                # Rebuild counts list and sort
                counts = [{"value": val, "count": cnt} for val, cnt in aggregated_counts.items()]
                counts.sort(key=lambda x: (-x["count"], x["value"]))
            else:
                # If records is empty, all samples have no valid values
                # We need to get total and missing from a separate query
                # This happens when ALL samples have is_missing=1 and no valid enum values
                logger.warning(
                    "Combined query returned empty results - all samples may be missing",
                    field=field
                )
                # Run a simple query to get total and missing
                # Use the same logic as the combined query but without UNWIND
                # Determine the node and field based on field name
                invalid_list_filter = build_invalid_value_list_filter(field)
                # For combined queries, we know the field structure:
                # - library_source_material, library_strategy, library_selection_method, specimen_molecular_analyte_type: sequencing_file (sf)
                # - preservation_method: pathology_file (pf)
                if field in ["library_source_material", "library_strategy", "library_selection_method", "specimen_molecular_analyte_type"]:
                    node_alias = "sf"
                    relationship = "-[:of_sequencing_file]->"
                    node_type = "sequencing_file"
                elif field == "preservation_method":
                    node_alias = "pf"
                    relationship = "-[:of_pathology_file]->"
                    node_type = "pathology_file"
                else:
                    # Fallback - should not happen for combined queries
                    node_alias = "sf"
                    relationship = "-[:of_sequencing_file]->"
                    node_type = "sequencing_file"

                # Determine the correct node_field based on field name
                if field == "specimen_molecular_analyte_type":
                    node_field = "sf.library_source_molecule"
                elif field in ["library_source_material", "library_strategy", "library_selection_method"]:
                    node_field = f"{node_alias}.{sample_metadata_field_mapping[field][1]}"
                elif field == "preservation_method":
                    node_field = f"{node_alias}.{sample_metadata_field_mapping[field][1]}"
                else:
                    # Fallback
                    node_field = f"{node_alias}.{sample_metadata_field_mapping.get(field, ('', field))[1]}"

                fallback_cypher = f"""
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                MATCH (st:study)
                WHERE st.study_id = sid
                OPTIONAL MATCH ({node_alias}:{node_type}){relationship}(sa)
                WITH toString(sa.sample_id) AS sample_id,
                     toString(st.study_id) AS study_id,
                     collect(DISTINCT {node_field}) as field_values
                WITH count(*) as total,
                     sum(CASE WHEN size([val IN field_values WHERE val IS NOT NULL
                                         AND {invalid_list_filter}]) = 0
                              THEN 1 ELSE 0 END) as missing
                RETURN total, missing
                """.strip()
                try:
                    fallback_result = await self.session.run(fallback_cypher, params)
                    fallback_records = []
                    async for record in fallback_result:
                        fallback_records.append(dict(record))
                    if fallback_records:
                        total = fallback_records[0].get("total", 0)
                        missing = fallback_records[0].get("missing", 0)
                except Exception as e:
                    logger.error(
                        "Error executing fallback query for combined query",
                        error=str(e),
                        field=field,
                        exc_info=True
                    )

        # Special handling for specimen_molecular_analyte_type
        # Query now returns aggregated results (value, count) with mapping done in Cypher
        # This eliminates Python-side processing overhead and reduces returned rows significantly
        elif field == "specimen_molecular_analyte_type":
            # Query already returns value and count, just process like other fields
            # But need to handle deduplication: one sample can have multiple DB values that map to same API value
            # The Cypher query handles this with DISTINCT sample_id per api_value
            for record in records:
                value = record.get("value")
                count = record.get("count", 0)

                if not value or count == 0:
                    continue

                counts.append({
                    "value": value,
                    "count": count
                })
            counts.sort(key=lambda x: (-x["count"], x["value"]))
        else:
            # Standard processing for other fields
            for record in records:
                    value = record.get("value")
                    # Filter out empty strings and "-999" - skip them (they should be counted as missing)
                    if value == "" or (isinstance(value, str) and value.strip() == ""):
                        continue  # Skip empty values
                    # Filter out "-999" for age fields (sentinel value for missing data)
                    if str(value) == "-999" or (isinstance(value, str) and value.strip() == "-999"):
                        continue  # Skip "-999" values

                    # Apply field mapping (DB value -> API value) using centralized mappings
                    mapped_value = map_field_value(field, value)

                    # If mapping returns None, skip this value (it should be counted as missing)
                    # This handles null_mappings (e.g., "Not Reported" for specimen_molecular_analyte_type)
                    if mapped_value is None:
                        continue

                    counts.append({
                        "value": mapped_value,
                        "count": record.get("count", 0)
                    })

            # Aggregate counts for fields where multiple DB values map to the same API value
            # (e.g., disease_phase: "Recurrent Disease" and "Relapse" both map to "Relapse")
            # Note: specimen_molecular_analyte_type is handled above, so this is for other fields
            aggregated_counts = {}
            for item in counts:
                val = item["value"]
                cnt = item["count"]
                if val in aggregated_counts:
                    aggregated_counts[val] += cnt
                else:
                    aggregated_counts[val] = cnt
            # Rebuild counts list and sort
            counts = [{"value": val, "count": cnt} for val, cnt in aggregated_counts.items()]
            counts.sort(key=lambda x: (-x["count"], x["value"]))

        # Calculate total and missing counts
        # Total: count of all distinct samples matching filters
        # IMPORTANT: Total must match /sample/summary, which only counts samples with a path to a study
        # Skip if we already have total/missing from combined query
        if is_combined_query:
            # Total and missing already extracted from combined query above
            pass
        elif field == "anatomical_sites":
            # Total: every (sample_id, study_id) pair with a path to a study.
            total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id
                RETURN count(*) as total
                """.strip()

            # Missing: pairs whose anatomic_site is NULL/empty, or whose values are all
            # "Invalid value".
            # NOTE: the DISTINCT below keys on (sample_id, study_id, sites), not just the
            # pair, so it only agrees with the pair-unit total while a given
            # (sample_id, study_id) is carried by a single sample node -- true today. Two
            # nodes sharing a pair but differing in anatomic_site would be counted twice.
            # `st2_list + st1_list` is list concatenation, not union: a sample reaching one
            # study via BOTH paths yields that study twice, and the DISTINCT is what
            # collapses it. There are 0 such samples today; do not drop the DISTINCT.
            missing_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT sa.sample_id as sample_id, sid as study_id, sa.anatomic_site as sites
                WHERE sites IS NULL
                   OR size(sites) = 0
                   OR ALL(site IN sites WHERE site IS NULL OR toString(site) = '' OR toLower(trim(toString(site))) = 'invalid value')
                RETURN count(*) as missing
                """.strip()

            # Execute total and missing queries
            total_result = await self.session.run(total_cypher, params)
            total_records = []
            async for record in total_result:
                total_records.append(dict(record))
            total = total_records[0].get("total", 0) if total_records else 0

            # A failed missing query degrades to 0 rather than failing the whole count.
            missing = 0
            try:
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                missing = missing_records[0].get("missing", 0) if missing_records else 0
            except Exception as e:
                logger.error(
                    "Error executing anatomical_sites missing query",
                    error=str(e),
                    error_type=type(e).__name__,
                    field=field,
                    exc_info=True
                )
                missing = 0
        else:
            # For other standard fields (vital_status, age_at_vital_status, or sample metadata fields)
            # Total: all samples with a path to a study (matching /sample/summary - 50211 when no filters)
            # For specimen_molecular_analyte_type, total should only count samples with sequencing_file nodes
            # Total: every (sample_id, study_id) pair with a path to a study.
            total_cypher = """
                MATCH (sa:sample)
                WHERE sa.sample_id IS NOT NULL
                  AND sa.sample_id <> ''
                OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
                WITH sa, collect(DISTINCT st1.study_id) AS st1_list
                OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
                WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
                WITH sa, (st2_list + st1_list) AS combined
                UNWIND combined AS sid
                WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
                RETURN count(*) as total
                """.strip()

            # Missing: samples without the field value (NULL or missing relationship)
            # No filters - count samples with NULL field
            if is_sample_metadata_field:
                node_alias, _ = sample_metadata_field_mapping[field]
                optional_matches = []
                # Only add OPTIONAL MATCH if field is on a related node (not on sample node itself)
                # If node_alias == "sa", no joins needed - field is directly on sample node
                if node_alias == "sf":
                    optional_matches.append("OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)")
                if node_alias == "pf":
                    optional_matches.append("OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)")
                if node_alias == "d":
                    optional_matches.append("OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)")
                if node_alias == "st":
                    optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)")
                # For fields on sample node (sa), we still need study paths to match summary
                if node_alias == "sa":
                    # Add participant and study paths if not already included
                    if not any("p:participant" in match for match in optional_matches):
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                    if not any("st1:study" in match for match in optional_matches):
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                    if not any("st2:study" in match for match in optional_matches):
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p2:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")

                # For all related node fields (sf, pf, d, st), need to include study paths in missing query
                # to match the values and total queries (only count samples WITH studies)
                if node_alias in ["sf", "pf", "d", "st"]:
                    # Add study paths if not already included
                    if not any("st1" in match for match in optional_matches):
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                    if not any("st2" in match for match in optional_matches):
                        # Check if participant is already included
                        if not any("p:participant" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")

                # For diagnosis fields (d.*), we need to check if ALL diagnoses have NULL/empty values
                # For other fields, check if the field is NULL/empty
                if node_alias == "d":
                    # Diagnosis fields: count as missing if sample has NO diagnoses with valid values
                    # i.e., all diagnoses have NULL/empty, OR no diagnoses exist
                    # IMPORTANT: Must match values query structure - only count samples WITH studies
                    # Need to ensure study paths are included in optional_matches
                    # Add study paths if not already included
                    if not any("st1:study" in match for match in optional_matches):
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)")
                    if not any("st2:study" in match for match in optional_matches):
                        if not any("p:participant" in match for match in optional_matches):
                            optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p:participant)")
                        optional_matches.append("OPTIONAL MATCH (sa)-[:of_sample]->(p3:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)")

                    optional_matches_str = "\n                ".join(optional_matches) if optional_matches else ""

                    # Build invalid value conditions based on null_mappings for this field
                    invalid_all_clause = build_invalid_value_all_clause(field)
                    missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL AND sa.sample_id <> ''
            {optional_matches_str}
            WITH sa, d, p,
                 size([(sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(:study) | 1]) AS has_study1,
                 size([(sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(:study) | 1]) AS has_study2
            WHERE has_study1 > 0 OR has_study2 > 0
            WITH DISTINCT sa, collect(DISTINCT {node_field}) as all_values
            WITH sa,
                 [val IN all_values WHERE val IS NOT NULL] as non_null_values
            WHERE size(non_null_values) = 0
               OR ALL(val IN non_null_values WHERE {invalid_all_clause})
            RETURN count(DISTINCT sa) as missing
            """.strip()
                else:
                    # Non-diagnosis fields: check if field is NULL/empty or "-999"
                    # For fields on sample node, also need to check st IS NOT NULL to match summary
                    # For specimen_molecular_analyte_type, use a different query structure:
                    # 1. First find samples with valid study
                    # 2. Then find sequencing_file associated with these samples
                    # 3. Count as missing if value is invalid/Not Reported (but NOT if sequencing_file is NULL, because those samples shouldn't be in the total)
                    if field == "specimen_molecular_analyte_type":
                        # Missing: samples with study path that either:
                        # 1. Don't have any sequencing_file, OR
                        # 2. Have sequencing_file(s) but all have null/invalid/Not Reported values
                        invalid_list_filter = build_invalid_value_list_filter("specimen_molecular_analyte_type")
                        missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(DISTINCT sf.library_source_molecule) as molecule_values
            WHERE size([val IN molecule_values WHERE val IS NOT NULL
                         AND {invalid_list_filter}]) = 0
            RETURN count(*) as missing
            """.strip()
                    elif node_alias == "sf":
                        # Skip missing query for combined query fields (handled by combined query)
                        if field in ["library_source_material", "library_strategy", "library_selection_method"]:
                            # Missing count is already included in combined query
                            missing_cypher = None
                        else:
                            # Optimized missing count for other sequencing_file fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any sequencing_file, OR
                            # 2. Have sequencing_file(s) but all have null/invalid values (based on null_mappings)
                            # Performance: Collect only field values (strings), not nodes - this is more efficient
                            # OPTIMIZATION: Collect DISTINCT field values per (sample_id, study_id) pair
                            # Then filter invalid values in WHERE clause - avoids scanning all sequencing_file records
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (sf:sequencing_file)-[:of_sequencing_file]->(sa)
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(DISTINCT {node_field}) as field_values
            WHERE size([val IN field_values WHERE val IS NOT NULL
                         AND {invalid_list_filter}]) = 0
            RETURN count(*) as missing
            """.strip()
                    elif node_alias == "pf":
                        # Skip missing query for preservation_method (handled by combined query)
                        if field == "preservation_method":
                            # Missing count is already included in combined query
                            missing_cypher = None
                        else:
                            # Optimized missing count for other pathology_file fields
                            # Missing: samples with study path that either:
                            # 1. Don't have any pathology_file, OR
                            # 2. Have pathology_file(s) but all have null/invalid values (based on null_mappings)
                            # IMPORTANT: When OPTIONAL MATCH doesn't find a pathology_file, pf is NULL
                            # collect(DISTINCT pf.fixation_embedding_method) on NULL returns [null]
                            # So samples without pathology_file will have field_values = [null] and size([val IN [null] WHERE val IS NOT NULL ...]) = 0, counted as missing ✅
                            # Counts unique (sample_id + study_id) pairs (consistent with values query)
                            invalid_list_filter = build_invalid_value_list_filter(field)
                            missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            MATCH (st:study)
            WHERE st.study_id = sid
            OPTIONAL MATCH (pf:pathology_file)-[:of_pathology_file]->(sa)
            WITH toString(sa.sample_id) AS sample_id,
                 toString(st.study_id) AS study_id,
                 collect(DISTINCT {node_field}) as field_values
            WHERE size([val IN field_values WHERE val IS NOT NULL
                         AND {invalid_list_filter}]) = 0
            RETURN count(*) as missing
            """.strip()
                    else:  # node_alias == "sa"
                        # Field lives on the sample node: same (sample_id, study_id)
                        # unit as the total query.
                        missing_cypher = f"""
            MATCH (sa:sample)
            WHERE sa.sample_id IS NOT NULL
              AND sa.sample_id <> ''
              AND ({node_field} IS NULL OR toString({node_field}) = '' OR trim(toString({node_field})) = '' OR toString({node_field}) = '-999' OR trim(toString({node_field})) = '-999')
            OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
            WITH sa, collect(DISTINCT st1.study_id) AS st1_list
            OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
            WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
            WITH sa, (st2_list + st1_list) AS combined
            UNWIND combined AS sid
            WITH DISTINCT sa.sample_id as sample_id, sid as study_id
            RETURN count(*) as missing
            """.strip()

            # Execute total and missing queries (skip if using combined query)
            if total_cypher is not None:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                total = total_records[0].get("total", 0) if total_records else 0
            # else: total already extracted from combined query

            if missing_cypher is not None:
                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                missing = missing_records[0].get("missing", 0) if missing_records else 0
            # else: missing already extracted from combined query

        # Verify: total should equal sum of values + missing
        # IMPORTANT: Skip adjustment for combined queries (library_source_material, library_strategy, preservation_method)
        # because missing count comes directly from the database query and is correct
        # IMPORTANT: For fields where samples can have multiple values (e.g., library_strategy),
        # the formula Total = Values sum + Missing doesn't hold because:
        # - Total = unique (sample_id, study_id) pairs
        # - Values sum = sum of all (sample_id, study_id, value) combinations
        # - Missing = unique (sample_id, study_id) pairs with no valid values
        # So we keep the original missing count from the database query
        if not is_combined_query:
            values_sum = sum(item["count"] for item in counts)
            if total != values_sum + missing:
                # Fields where samples can have multiple values per (sample_id, study_id) pair
                multi_value_fields = {"library_strategy", "library_selection_method", "anatomical_sites"}
                if field in multi_value_fields:
                    # For multi-value fields, Total can be < Values sum + Missing
                    # This is expected when samples have multiple values
                    logger.debug(
                        "Total count relationship for multi-value field",
                        field=field,
                        total=total,
                        values_sum=values_sum,
                        missing=missing,
                        difference=total - (values_sum + missing),
                        note="For multi-value fields, Total = unique samples, Values sum = sum of (sample, value) combinations"
                    )
                else:
                    # For single-value fields, log warning but don't adjust
                    logger.warning(
                        "Total count mismatch for field",
                        field=field,
                        total=total,
                        values_sum=values_sum,
                        missing=missing,
                        difference=total - (values_sum + missing),
                        values_count=len(counts),
                        note="Keeping original missing count from database query"
                    )
                # Do NOT adjust missing count - keep the original from database query
                # The missing count is correct and should not be modified
        else:
            # For combined queries, just log the verification without adjusting
            values_sum = sum(item["count"] for item in counts)
            if total != values_sum + missing:
                logger.warning(
                    "Total count mismatch for combined query (should not happen)",
                    field=field,
                    total=total,
                    values_sum=values_sum,
                    missing=missing,
                    difference=total - (values_sum + missing),
                    values_count=len(counts),
                    note="Missing count comes from database query and should be correct"
                )

        logger.debug(
            "Completed sample count by field",
            field=field,
            results_count=len(counts),
            total=total,
            missing=missing,
            values_sum=sum(item["count"] for item in counts)
        )

        # Per SAMPLE_ENDPOINT_RULES rule 2: counts are by (sample_id, study_id) per value.
        # One (sample_id, study_id) can contribute to multiple value buckets, so
        # sum(value counts) + missing may be greater than total. This is expected.
        return {
            "total": total,
            "missing": missing,
            "values": counts  # counts already has format [{"value": ..., "count": ...}]
        }

    # REMOVED: _count_samples_by_race and _count_samples_by_ethnicity methods
    # These are not needed because sex, race, and ethnicity are not valid sample count fields.
    # The validation at line 1787-1791 rejects these fields before these methods can be called.

    async def _count_samples_by_associated_diagnoses(self) -> dict[str, Any]:
        """
        Count distinct samples by associated diagnoses.

        For samples with multiple diagnoses, the sample is counted
        for each diagnosis they have.

        Counts are always unfiltered — see :meth:`count_samples_by_field`.

        Returns:
            Dictionary with total, missing, and values (list of diagnosis counts)
        """
        logger.debug("Counting samples by associated diagnoses")

        params: dict[str, Any] = {}

        # Query 1: Get total count of all unique samples matching filters
        # Use multi-hop traversal for study paths
        total_cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        WITH DISTINCT toString(sa.sample_id) AS sample_id, toString(sid) AS study_id
        RETURN count(*) as total
        """.strip()
        # Query 2: Get count of samples with no valid diagnoses (missing)
        # Only count samples with a study path (matching summary endpoint)
        # Missing = samples with no diagnoses OR ALL diagnoses are invalid
        # IMPORTANT: Check ALL diagnoses, not just the first one
        # Use multi-hop traversal for study paths
        missing_cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        WITH toString(sa.sample_id) AS sample_id,
             toString(st.study_id) AS study_id,
             collect(d) as diagnoses
        WITH sample_id, study_id,
             [d IN diagnoses WHERE d IS NOT NULL |
               CASE
                 WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                      AND d.diagnosis_comment IS NOT NULL
                      AND trim(toString(d.diagnosis_comment)) <> ''
                 THEN d.diagnosis_comment
                 WHEN toLower(trim(toString(d.diagnosis))) = 'see diagnosis_comment'
                 THEN null
                 ELSE d.diagnosis
               END
             ] as diagnosis_values
        WITH sample_id, study_id,
             [val IN diagnosis_values WHERE val IS NOT NULL
              AND toString(val) <> ''
              AND trim(toString(val)) <> ''] as valid_values
        WHERE size(valid_values) = 0
        RETURN count(*) as missing
        """.strip()
        # Query 3: Count by diagnosis values
        # Check ALL diagnoses of each sample (not just first)
        # d.diagnosis can be a STRING or LIST - handle both
        # Multiple diagnosis nodes can link to one sample, so each contributes values
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(sa:sample)
        # If diagnosis is "see diagnosis_comment", use diagnosis_comment as the value
        # Filter out "see diagnosis_comment" if diagnosis_comment is NULL or empty
        # Use multi-hop traversal for study paths
        values_cypher = """
        MATCH (sa:sample)
        WHERE sa.sample_id IS NOT NULL
          AND sa.sample_id <> ''
        OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
        WITH sa, collect(DISTINCT st1.study_id) AS st1_list
        OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
        WITH sa, st1_list, collect(DISTINCT st2.study_id) AS st2_list
        WITH sa, (st2_list + st1_list) AS combined
        UNWIND combined AS sid
        MATCH (st:study)
        WHERE st.study_id = sid
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
        WITH toString(sa.sample_id) AS sample_id,
             toString(st.study_id) AS study_id,
             collect(d) as diagnoses
        // UNWIND all diagnoses to count each one
        UNWIND diagnoses AS diag_node
        WITH sample_id, study_id, diag_node
        WHERE diag_node IS NOT NULL
        WITH sample_id, study_id,
             CASE
               WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment'
                    AND diag_node.diagnosis_comment IS NOT NULL
                    AND trim(toString(diag_node.diagnosis_comment)) <> ''
               THEN diag_node.diagnosis_comment
               WHEN toLower(trim(toString(diag_node.diagnosis))) = 'see diagnosis_comment'
               THEN null
               ELSE diag_node.diagnosis
             END AS diagnosis_value
        WHERE diagnosis_value IS NOT NULL
          AND toString(diagnosis_value) <> ''
          AND trim(toString(diagnosis_value)) <> ''
        WITH DISTINCT sample_id, study_id, toString(diagnosis_value) as value
        RETURN value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        logger.info(
            "Executing count_samples_by_associated_diagnoses Cypher queries",
            params_count=len(params),
            values_query=values_cypher
        )

        # Execute all three queries with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records = []

        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0

                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing_count = missing_records[0].get("missing", 0) if missing_records else 0

                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()

                # If we got results or it's the last retry, break out of retry loop
                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break

                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying count_samples_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_samples_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_samples_by_field query after retries", error=str(e), exc_info=True)
                    raise

        # Format results and filter out any "see diagnosis_comment" that might have slipped through
        counts = []
        for record in values_records:
            value = record.get("value")
            # Additional safety check: filter out "see diagnosis_comment" if it somehow appears
            if value and "see diagnosis_comment" in str(value).lower():
                logger.warning(
                    "Filtering out 'see diagnosis_comment' from results",
                    value=value,
                    count=record.get("count", 0)
                )
                continue
            counts.append({
                "value": value,
                "count": record.get("count", 0)
            })

        # Sort by count descending, then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))

        logger.info(
            "Completed sample count by associated diagnoses",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

    async def _count_samples_by_diagnosis_category(self) -> dict[str, Any]:
        """
        Count distinct (sample, study) combinations by harmonized diagnosis_category.

        Graph path: (d:diagnosis)-[:of_diagnosis]->(sa:sample),
        then (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)
                   -[:of_consent_group]->(st:study)
        OR         (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st:study)

        Counts are always unfiltered — see :meth:`count_samples_by_field`.
        """
        logger.debug("Counting samples by diagnosis_category")

        params: dict[str, Any] = {
            "harmonized_pvs": _HARMONIZED_PVS_SORTED,
            "harmonized_pvs_lower": _HARMONIZED_PVS_LOWER,
        }

        total_cypher = """
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa, [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids
UNWIND study_ids AS study_id
RETURN count(*) AS total
""".strip()

        missing_cypher = """
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND trim(toString(sa.sample_id)) <> ''
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa, [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids
UNWIND study_ids AS study_id
OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(sa)
WITH sa.sample_id AS sample_id, study_id, collect(d) AS diagnoses
WHERE size([
    d IN diagnoses WHERE d IS NOT NULL
    AND d.diagnosis_category IS NOT NULL
    AND size(coalesce(d.diagnosis_category, [])) > 0
    AND any(tok IN coalesce(d.diagnosis_category, [])
            WHERE toLower(trim(toString(tok))) IN $harmonized_pvs_lower)
]) = 0
RETURN count(*) AS missing
""".strip()

        values_cypher = """
MATCH (d:diagnosis)-[:of_diagnosis]->(sa:sample)
WHERE d.diagnosis_category IS NOT NULL AND size(coalesce(d.diagnosis_category, [])) > 0
WITH sa, d
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
WITH sa, d, collect(DISTINCT st1.study_id) AS st1_ids
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, d, st1_ids, collect(DISTINCT st2.study_id) AS st2_ids
WITH sa.sample_id AS sample_id,
     [sid IN (st1_ids + st2_ids) WHERE sid IS NOT NULL] AS study_ids,
     [tok IN coalesce(d.diagnosis_category, []) WHERE trim(toString(tok)) <> ''] AS tokens
UNWIND study_ids AS study_id
UNWIND tokens AS raw_token
WITH sample_id, study_id, trim(toString(raw_token)) AS token
WITH sample_id, study_id, token,
     [pv IN $harmonized_pvs WHERE toLower(pv) = toLower(token)][0] AS matched_pv
WHERE matched_pv IS NOT NULL
WITH DISTINCT sample_id, study_id, matched_pv
RETURN matched_pv AS value, count(*) AS count
ORDER BY count DESC, value ASC
""".strip()

        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records: list[dict[str, Any]] = []

        while retry_count <= max_retries:
            try:
                total_result = await self.session.run(total_cypher, params)
                total_records = []
                async for record in total_result:
                    total_records.append(dict(record))
                await total_result.consume()
                total_count = total_records[0].get("total", 0) if total_records else 0

                missing_result = await self.session.run(missing_cypher, params)
                missing_records = []
                async for record in missing_result:
                    missing_records.append(dict(record))
                await missing_result.consume()
                missing_count = missing_records[0].get("missing", 0) if missing_records else 0

                values_result = await self.session.run(values_cypher, params)
                values_records = []
                async for record in values_result:
                    values_records.append(dict(record))
                await values_result.consume()

                if (total_count > 0 or len(values_records) > 0) or retry_count >= max_retries:
                    break

                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning("Error in count_samples_by_diagnosis_category, retrying", error=str(e))
                else:
                    logger.error("Error in count_samples_by_diagnosis_category after retries",
                                 error=str(e), exc_info=True)
                    raise

        counts = [
            {"value": r.get("value"), "count": r.get("count", 0)}
            for r in values_records
        ]

        logger.info(
            "Completed sample count by diagnosis_category",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

