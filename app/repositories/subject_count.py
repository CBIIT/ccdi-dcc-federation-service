"""
Count methods for SubjectRepository.

This module contains methods for counting subjects by field values.
"""

import asyncio
from typing import Dict, Any, List
from app.core.logging import get_logger
from app.core.constants import Race
from app.core.field_mappings import (
    map_field_value,
    build_case_mapping_statement,
)
from app.core.diagnosis_category import HARMONIZED_DIAGNOSIS_CATEGORIES
from app.models.errors import UnsupportedFieldError

_HARMONIZED_PVS_SORTED: List[str] = sorted(HARMONIZED_DIAGNOSIS_CATEGORIES)
_HARMONIZED_PVS_LOWER: List[str] = [pv.lower() for pv in _HARMONIZED_PVS_SORTED]
from app.utils.cypher_builder import combine_where_clauses

logger = get_logger(__name__)


def _strip_remaining_internal_subject_count_keys(filters: Dict[str, Any]) -> None:
    """
    Remove underscore-prefixed keys not consumed by this count implementation.

    Prevents generic loops from emitting invalid predicates like p._associated_diagnosis_categories_contains.
    """
    for key in list(filters.keys()):
        if key.startswith("_"):
            filters.pop(key, None)


class SubjectCount:
    """Mixin providing count methods for SubjectRepository."""

    def _get_field_path(self, field: str) -> str:
        """
        Map field name to its database path for counting operations.

        Args:
            field: Field name to map

        Returns:
            Database path for the field
        """
        # Field name to database property mapping
        field_mapping = {
            "sex": "p.sex_at_birth",
            "race": "p.race",
            "vital_status": "final_vital_status",  # Derived from survival records
            "age_at_vital_status": "final_age_at_vital_status",  # Derived from survival records
            "associated_diagnoses": "d.diagnosis",  # From diagnosis nodes
        }
        return field_mapping.get(field, f"p.{field}")

    def _build_sex_normalization_case(self, field: str) -> str:
        """
        Build Cypher CASE statement for sex value normalization from config.

        Args:
            field: Field name to check

        Returns:
            Cypher CASE statement string for sex normalization, or empty string if not sex field
        """
        if field != "sex" or not self.settings or not hasattr(self.settings, 'sex_value_mappings'):
            return ""

        mappings = self.settings.sex_value_mappings
        if not mappings:
            return ""

        # Build CASE statement from config mappings
        case_parts = []
        for db_value, normalized_value in mappings.items():
            case_parts.append(f"WHEN toString(value) = '{db_value}' OR toString(value) = '{normalized_value}' THEN '{normalized_value}'")

        # Default fallback (prefer 'U' for unknown/Not Reported if available)
        default_value = mappings.get("Not Reported", "U") if "Not Reported" in mappings else list(mappings.values())[0] if mappings else "U"
        case_parts.append(f"ELSE '{default_value}'")

        return f"""
                 CASE
                   {' '.join(case_parts)}
                 END"""

    async def count_subjects_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count subjects grouped by a specific field value.

        Note: This endpoint does not accept filters - the filters parameter is always empty {}
        as the API endpoint explicitly rejects query parameters.

        Args:
            field: Field to group by and count
            filters: Additional filters to apply (always empty {} - not used)

        Returns:
            Dictionary with total, missing, and values (list of count results)

        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug("Counting subjects by field", field=field)

        # Note: This endpoint does not accept filters - filters parameter is always empty {}
        # The API endpoint explicitly rejects query parameters

        # Validate field is allowed for count operations (case-sensitive)
        allowed_fields = set(self.settings.subject_count_fields if self.settings else ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses", "associated_diagnosis_categories"])
        if field not in allowed_fields:
            raise UnsupportedFieldError(
                field=field,
                entity_type="subject"
            )

        # Special handling for race field
        if field == "race":
            return await self._count_subjects_by_race(filters)

        # Special handling for ethnicity field (derived from race)
        if field == "ethnicity":
            return await self._count_subjects_by_ethnicity(filters)

        # Special handling for associated_diagnoses field (list field from diagnosis nodes)
        if field == "associated_diagnoses":
            return await self._count_subjects_by_associated_diagnoses(filters)

        # Special handling for associated_diagnosis_categories field
        if field == "associated_diagnosis_categories":
            return await self._count_subjects_by_diagnosis_category(filters)

        # No filter handling needed - filters are always empty {}
        params = {}

        # Check if field requires survival record processing
        is_survival_field = field in {"vital_status", "age_at_vital_status"}

        # Build survival processing logic if needed
        survival_processing = ""
        field_access = self._get_field_path(field)

        # Note: For non-survival fields, we always use the "no filters" path since filters are always empty
        # For survival fields, we also use the "no filters" path but still need survival processing

        if is_survival_field:
            survival_processing = """
WITH p, d, c, st,
     // Collect all survival records for this participant
     collect(s) AS survival_records
WITH p, d, c, st, survival_records,
     // Keep only records with a status
     [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
WITH p, d, c, st, survs,
     // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age,
     // Check if all ages are -999 (for counting as missing)
    all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
    // Check if Dead age is -999
    any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
WITH p, d, c, st,
     // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
     // If no record matches max_age, fall back to first available status
     CASE
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead THEN 'Dead'
       ELSE CASE
         WHEN head([sr IN survs
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                     | sr.last_known_survival_status]) IS NOT NULL
         THEN head([sr IN survs
                     WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                     | sr.last_known_survival_status])
         ELSE head([sr IN survs | sr.last_known_survival_status])
       END
     END AS final_vital_status,
     // Age: If no vital_status, age should also be NULL
     // If all ages are -999, or Dead age is -999, set to NULL (count as missing)
     // Otherwise use max Dead age or max age
     CASE
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead AND dead_age_is_999 THEN NULL
       WHEN has_dead THEN max_dead_age
       WHEN all_ages_are_999 THEN NULL
       ELSE max_age
     END AS final_age_at_vital_status"""
        else:
            survival_processing = ""

        # Query 1: Get total count of all unique participant + study combinations
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Total count should match summary endpoint - use same logic
        # Note: No filters - always use simple MATCH (no OPTIONAL MATCH)
        if is_survival_field:
            # For survival fields, use simple MATCH (no survival processing needed for total count)
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # For non-survival fields, use simple MATCH
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()

        # Query 2: Get count of participants with null field value (missing)
        # Missing is calculated directly by querying participants without valid field values
        # Note: No filters - always use simple MATCH
        null_check = f"{field_access} IS NULL" if not is_survival_field else f"final_{field} IS NULL"

        if is_survival_field:
            # For survival fields, missing logic depends on the field:
            # - vital_status: missing when final_vital_status IS NULL
            # - age_at_vital_status: missing when final_age_at_vital_status IS NULL (includes -999 cases)
            if field == "age_at_vital_status":
                # Compute derived age ONCE per participant, then expand to studies
                missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, collect(s) AS survival_records
        WITH p, [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age,
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p,
             CASE
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, final_age_at_vital_status
        WHERE final_age_at_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
            else:
                # For vital_status, missing means no vital_status
                missing_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, st,
             collect(s) AS survival_records
        WITH p, st, survival_records,
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, st, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age
        WITH p, st,
             CASE
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, final_vital_status
        WHERE final_vital_status IS NULL
        RETURN count(*) as missing
        """.strip()
        else:
            # For non-survival fields, missing means NULL field value
            missing_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE {null_check}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()

        # Query 3: Get counts by field values
        not_null_check = f"{field_access} IS NOT NULL" if not is_survival_field else f"final_{field} IS NOT NULL"
        field_value_expr = f"{field_access}" if not is_survival_field else f"final_{field}"

        # Build sex normalization CASE statement from config
        sex_normalization = self._build_sex_normalization_case(field)

        # Build normalization expression: use sex normalization if field is sex and normalization exists
        if field == "sex" and sex_normalization:
            normalization_expr = sex_normalization
        else:
            normalization_expr = "toString(value)"

        if is_survival_field:
            # For survival fields, need survival processing
            # Note: No filters - always use simple MATCH
            if field == "age_at_vital_status":
                # Compute derived age ONCE per participant, then expand to studies
                values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, collect(s) AS survival_records
        WITH p, [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, survs,
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age,
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p,
             CASE
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, final_age_at_vital_status as field_val
        WHERE field_val IS NOT NULL
        WITH DISTINCT participant_id, study_id, toString(field_val) AS normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
            else:
                # For vital_status
                values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        WITH p, st,
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, st, survival_records,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.last_known_survival_status = 'Dead'
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > dead_max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE dead_max_age
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                           AND coalesce(toInteger(sr.age_at_last_known_survival_status), -999) > max_age
                      THEN coalesce(toInteger(sr.age_at_last_known_survival_status), -999)
                      ELSE max_age
                    END) AS max_age
        WITH p, st, survs, has_dead, max_dead_age, max_age,
             // Check if all ages are -999 (for counting as missing)
             all(sr IN survs WHERE sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999) AS all_ages_are_999,
             // Check if Dead age is -999
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead' AND (sr.age_at_last_known_survival_status IS NULL OR toInteger(sr.age_at_last_known_survival_status) = -999)) AS dead_age_is_999
        WITH p, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN 'Dead'
               ELSE CASE
                 WHEN head([sr IN survs
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                             | sr.last_known_survival_status]) IS NOT NULL
                 THEN head([sr IN survs
                             WHERE sr.age_at_last_known_survival_status IS NOT NULL AND toInteger(sr.age_at_last_known_survival_status) = max_age
                             | sr.last_known_survival_status])
                 ELSE head([sr IN survs | sr.last_known_survival_status])
               END
             END AS final_vital_status,
             // Age: If no vital_status, age should also be NULL
             CASE
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead AND dead_age_is_999 THEN NULL
               WHEN has_dead THEN max_dead_age
               WHEN all_ages_are_999 THEN NULL
               ELSE max_age
             END AS final_age_at_vital_status
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, final_vital_status as field_val
        WHERE field_val IS NOT NULL
        WITH DISTINCT participant_id, study_id, toString(field_val) AS normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # For non-survival fields, use simple MATCH
            # Note: No filters - always use simple MATCH
            values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE {not_null_check}
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id,
             CASE
               WHEN {field_value_expr} IS NULL THEN []
               ELSE
                 // Wrap in list for UNWIND - works for both strings and lists
                 [{field_value_expr}]
             END as field_values
        UNWIND field_values as value
        WITH participant_id, study_id, value,
             CASE
               WHEN '{field}' = 'sex' THEN{normalization_expr}
               ELSE toString(value)
             END as normalized_value
        WITH DISTINCT participant_id, study_id, normalized_value
        RETURN normalized_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_field Cypher queries",
            field=field,
            params_count=len(params)
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
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise

        # Format values results
        counts = []
        for record in values_records:
            value = record.get("value")
            # Apply field mapping if needed (e.g., "Not Reported" -> "Not reported" for vital_status)
            if value is not None:
                mapped_value = map_field_value(field, value)
            else:
                mapped_value = value
            counts.append({
                "value": mapped_value,
                "count": record.get("count", 0)
            })

        logger.debug(
            "Completed subject count by field",
            field=field,
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

    async def _count_subjects_by_race(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by race.

        For race values like "Asian;White", the participant is counted
        for both "Asian" and "White".

        Note: This endpoint does not accept filters - the filters parameter is always empty {}
        as the API endpoint explicitly rejects query parameters.

        Args:
            filters: Additional filters to apply (always empty {} - not used)

        Returns:
            Dictionary with total, missing, and values (list of race counts)
        """
        logger.debug("Counting subjects by race with enum validation")

        # Note: This endpoint does not accept filters - filters parameter is always empty {}
        # The API endpoint explicitly rejects query parameters

        # Get all valid race enum values
        valid_races = Race.values()
        params = {"valid_races": valid_races}

        # Build race mapping CASE statement for Cypher queries
        # Maps database values to API values (e.g., "Not Allowed to Collect" -> "Not allowed to collect")
        race_mapping_case = build_case_mapping_statement("race", "race_candidate")

        # Query 1: Get total count of all unique participant + study combinations
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Use required MATCH for study (same as summary query) to avoid duplicates
        # DISTINCT ensures we count each (participant_id, study_id) pair only once
        # This handles cases where a participant might be linked to the same study through multiple consent groups
        total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()

        # Query 2: Count participant + study combinations WITHOUT any valid race value
        # This includes:
        # - NULL or empty race values
        # - Invalid race values (not in enum)
        # - Race values that, after processing (splitting, filtering Hispanic), have no valid races
        # We calculate this directly by finding participants that don't have at least one valid race
        # after processing, rather than using subtraction, to ensure accuracy
        missing_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Process race values: split by semicolon, filter Hispanic, validate
             CASE
                 WHEN race_raw IS NULL THEN []
                 WHEN toString(race_raw) = '' OR trim(toString(race_raw)) = '' THEN []
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 ELSE [trim(toString(race_raw))]
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id,
             CASE
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        // Apply race value mappings before validation
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, {race_mapping_case if race_mapping_case else 'race_candidate'} as mapped_race_candidate
        // Check if participant has at least one valid race (after mapping)
        WITH participant_id, study_id, collect(DISTINCT mapped_race_candidate) as mapped_races
        WITH participant_id, study_id, mapped_races,
             any(race IN mapped_races WHERE race IN $valid_races) as has_valid_race
        WHERE NOT has_valid_race
        RETURN count(*) as missing
        """.strip()

        # Query 3: Create a single query that counts distinct participant + study combinations for each valid race
        # Special handling: if race is only "Hispanic or Latino", count as "Not Reported"
        # Split race by semicolon, remove "Hispanic or Latino", then match against valid races
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Use required MATCH for study (same as summary query) to avoid duplicates
        # Apply race value mappings (e.g., "Not Allowed to Collect" -> "Not allowed to collect")
        values_cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NOT NULL
          AND toString(p.race) <> ''
          AND trim(toString(p.race)) <> ''
        WITH DISTINCT p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             // Data is always string format with semicolon separator
             CASE
                 WHEN toString(race_raw) CONTAINS ';' THEN [r IN SPLIT(toString(race_raw), ';') | trim(r)]
                 WHEN race_raw IS NOT NULL THEN [trim(toString(race_raw))]
                 ELSE []
             END as race_parts
        WITH participant_id, study_id, race_raw, race_parts,
             // Check if original race contained "Hispanic or Latino"
             any(r IN race_parts WHERE r = 'Hispanic or Latino') as had_hispanic,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, study_id, race_raw, race_list_filtered, had_hispanic
        // Process race values: if only "Hispanic or Latino", convert to "Not Reported" if valid
        // Otherwise, use the filtered race values
        WITH participant_id, study_id,
             CASE
               WHEN size(race_list_filtered) = 0 AND had_hispanic THEN ['Not Reported']
               ELSE race_list_filtered
             END as processed_races
        UNWIND processed_races as race_candidate
        WITH participant_id, study_id, race_candidate
        // Apply race value mappings (e.g., "Not Allowed to Collect" -> "Not allowed to collect")
        WITH participant_id, study_id, {race_mapping_case if race_mapping_case else 'race_candidate'} as mapped_race_candidate
        WHERE mapped_race_candidate IN $valid_races
        WITH DISTINCT participant_id, study_id, mapped_race_candidate as race_value
        RETURN race_value as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_race Cypher queries",
            race_count=len(valid_races),
            params_count=len(params)
        )

        # Execute queries with proper result consumption and retry logic
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

                # Count missing directly: participants without any valid race value
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
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise

        # Format results - only include races with count > 0
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}

        # Build final counts list - only include races with count > 0
        counts = []
        for race_value in valid_races:
            count = counts_by_value.get(race_value, 0)
            if count > 0:
                counts.append({
                    "value": race_value,
                    "count": count
                })

        # Sort by count descending (numeric), then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))

        # Calculate sum of values for logging
        # Note: sum(values) can be >= total because participants with multiple races
        # (e.g., "White;Asian") are counted multiple times in values but only once in total
        sum_values = sum(counts_by_value.values())

        # Note: We do NOT assume total = unique_with_valid_race + missing
        # The total and missing queries may use different matching logic (especially with filters),
        # so they may not be mutually exclusive and exhaustive
        # Also, sum(values) can be > total because participants with multiple races
        # are counted multiple times in values (once per race value)
        logger.info(
            "Completed subject count by race",
            total=total_count,
            missing=missing_count,
            sum_values=sum_values,
            values_count=len(counts),
            note="sum(values) can be > total because participants with multiple races are counted multiple times in values"
        )

        return {
            "total": total_count,
            "missing": missing_count,  # Directly calculated: participants without any valid race value
            "values": counts
        }

    async def _count_subjects_by_ethnicity(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by ethnicity (derived from race field).

        Ethnicity is determined from race:
        - If race contains 'Hispanic or Latino' → 'Hispanic or Latino'
        - Otherwise → 'Not reported'

        Args:
            filters: Additional filters to apply

        Returns:
            Dictionary with total, missing, and values (list with only 2 ethnicity options)
        """
        logger.debug("Counting subjects by ethnicity (derived from race)", filters=filters)

        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0

        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")

        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            where_conditions.append("""(
                ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
                OR ANY(key IN keys(p.metadata.unharmonized)
                       WHERE toLower(key) CONTAINS 'diagnos'
                       AND toLower(toString(p.metadata.unharmonized[key])) CONTAINS toLower($diagnosis_search_term))
            )""")
            params["diagnosis_search_term"] = search_term

        _strip_remaining_internal_subject_count_keys(filters)

        # Add regular filters (excluding ethnicity since we're counting by ethnicity)
        for filter_field, value in filters.items():
            if filter_field == "ethnicity":
                continue  # Skip ethnicity filter when counting by ethnicity
            param_counter += 1
            param_name = f"param_{param_counter}"

            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value

        # Build WHERE clause
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            where_clause = "WHERE " + " AND ".join(filtered_conditions) if filtered_conditions else ""
        else:
            where_clause = ""

        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_total = combine_where_clauses(where_clause, "st IS NOT NULL")
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_total}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()

        # Query 2: Get count of participant + study combinations with null race (missing ethnicity)
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_missing = combine_where_clauses(where_clause, "st IS NOT NULL AND p.race IS NULL")
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_missing}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            missing_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as missing
        """.strip()

        # Query 3: Count by ethnicity (derived from race)
        # Use participant_id + study_id as unique identifier
        # Optimize based on whether we have filters
        if where_clause or identifiers_condition:
            combined_where_values = combine_where_clauses(where_clause, "st IS NOT NULL AND p.race IS NOT NULL")
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where_values}
        WITH p.participant_id AS participant_id, st.study_id AS study_id, toString(p.race) as race
        WITH participant_id, study_id, race,
             CASE
               WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        WITH DISTINCT participant_id, study_id, ethnicity_value
        RETURN ethnicity_value as value, count(*) as count
        ORDER BY value ASC
        """.strip()
        else:
            # No filters - use participant -> consent_group -> study relationship
            values_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WHERE p.race IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, p.race as race_raw
        WITH participant_id, study_id, race_raw,
             CASE
               WHEN toString(race_raw) CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        WITH DISTINCT participant_id, study_id, ethnicity_value
        RETURN ethnicity_value as value, count(*) as count
        ORDER BY value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_ethnicity Cypher queries",
            params_count=len(params)
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
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise

        # Format results - ensure both ethnicity options are included (even with 0 count)
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}

        # Build final counts list with both ethnicity options
        ethnicity_options = ["Hispanic or Latino", "Not reported"]
        counts = []
        for ethnicity_value in ethnicity_options:
            counts.append({
                "value": ethnicity_value,
                "count": counts_by_value.get(ethnicity_value, 0)
            })

        # Sort by ethnicity value alphabetically
        counts.sort(key=lambda x: x["value"])

        logger.info(
            "Completed subject count by ethnicity",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

    async def _count_subjects_by_associated_diagnoses(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct participants by associated diagnoses.

        For participants with multiple diagnoses, the participant is counted
        for each diagnosis they have.

        Args:
            filters: Additional filters to apply

        Returns:
            Dictionary with total, missing, and values (list of diagnosis counts)
        """
        logger.debug("Counting subjects by associated diagnoses", filters=filters)

        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0

        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None and str(identifiers_value).strip():
                param_counter += 1
                id_param = f"param_{param_counter}"
                params[id_param] = identifiers_value
                identifiers_condition = f""",
                // normalize $identifiers: STRING -> [trimmed], LIST -> trimmed list
                CASE
                  WHEN ${id_param} IS NULL THEN NULL
                  WHEN valueType(${id_param}) = 'LIST'   THEN [id IN ${id_param} | trim(id)]
                  WHEN valueType(${id_param}) = 'STRING' THEN [trim(${id_param})]
                  ELSE []
                END AS id_list"""
                where_conditions.append("p.participant_id IN id_list")

        # Handle diagnosis search - we skip this for diagnosis counting since we're counting by diagnosis
        if "_diagnosis_search" in filters:
            filters.pop("_diagnosis_search")  # Remove it to avoid circular filtering

        _strip_remaining_internal_subject_count_keys(filters)

        # Add regular filters (excluding associated_diagnoses since we're counting by it)
        for filter_field, value in filters.items():
            if filter_field == "associated_diagnoses":
                continue  # Skip diagnosis filter when counting by diagnosis
            param_counter += 1
            param_name = f"param_{param_counter}"

            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value

        # Build WHERE clause
        if where_conditions:
            # Filter out empty strings to avoid "WHERE  AND ..." issues
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            where_clause = "WHERE " + " AND ".join(filtered_conditions) if filtered_conditions else ""
        else:
            where_clause = ""

        # Query 1: Get total count of all unique participant + study combinations matching filters
        # Use participant_id + study_id as unique identifier (same participant_id can be in different studies)
        # Only include OPTIONAL MATCHes if we have filters that need them
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(where_clause, "st IS NOT NULL")
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()
        else:
            # No filters - simple count
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            total_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH p.participant_id AS participant_id, st.study_id AS study_id
        RETURN count(*) as total
        """.strip()

        # Query 2: Get count of participant + study combinations with no diagnoses (missing)
        # Optimized to avoid unnecessary OPTIONAL MATCHes
        if where_clause or identifiers_condition:
            combined_where = combine_where_clauses(where_clause, "st IS NOT NULL")
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {combined_where}
        // combined_where includes "st IS NOT NULL", so st is guaranteed to be non-NULL here
        WITH p.participant_id AS participant_id, st.study_id AS study_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(*) as missing
        """.strip()
        else:
            # No filters - simple check for missing diagnoses
            # Use participant -> consent_group -> study relationship
            # Note: WHERE st IS NOT NULL is redundant since the relationship is a required match
            missing_cypher = """
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p.participant_id AS participant_id, st.study_id AS study_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(*) as missing
        """.strip()

        # Query 3: Count by diagnosis values
        # d.diagnosis is a STRING (not a list) - each diagnosis node has one diagnosis value
        # Multiple diagnosis nodes can link to one participant, so each contributes one value
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(p:participant)
        # Use participant_id + study_id as unique identifier
        if where_clause or (identifiers_condition and identifiers_condition.strip()):
            # Has filters - need to apply them but keep it efficient
            values_cypher = f"""
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, d.diagnosis as diagnosis_value
        MATCH (p2:participant {{participant_id: participant_id}})
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p2)
        OPTIONAL MATCH (p2)-[:of_participant]->(c2:consent_group)-[:of_consent_group]->(st2:study)
        WHERE st2.study_id = study_id
        WITH p2, s, c2, st2, participant_id, study_id, diagnosis_value{identifiers_condition}
        {combine_where_clauses(where_clause, "toString(diagnosis_value) <> ''")}
        WITH DISTINCT participant_id, study_id, toString(diagnosis_value) as value
        RETURN value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # No filters - use participant_id + study_id
            values_cypher = """
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WHERE st IS NOT NULL
        WITH p.participant_id AS participant_id, st.study_id AS study_id, d.diagnosis as diagnosis_value
        WHERE toString(diagnosis_value) <> ''
        WITH DISTINCT participant_id, study_id, toString(diagnosis_value) as value
        RETURN value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_associated_diagnoses Cypher queries",
            values_query=values_cypher,
            params_count=len(params),
            has_identifiers_condition=bool(identifiers_condition),
            has_where_clause=bool(where_clause)
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
                    logger.debug(f"Retrying count_subjects_by_field query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in count_subjects_by_field query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_field query after retries", error=str(e), exc_info=True)
                    raise

        # Format results
        counts = []
        for record in values_records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })

        # Sort by count descending, then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))

        logger.info(
            "Completed subject count by associated diagnoses",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )

        return {
            "total": total_count,
            "missing": missing_count,
            "values": counts
        }

    async def _count_subjects_by_diagnosis_category(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Count distinct (participant, study) combinations by harmonized diagnosis_category.

        Only counts tokens that are in HARMONIZED_DIAGNOSIS_CATEGORIES (after splitting on ';').
        One participant is counted once per harmonized token across all their diagnosis nodes.
        Subjects with no harmonized token in any diagnosis_category are counted as 'missing'.
        """
        logger.debug("Counting subjects by diagnosis_category", filters=filters)

        params: Dict[str, Any] = {"harmonized_pvs": _HARMONIZED_PVS_SORTED, "harmonized_pvs_lower": _HARMONIZED_PVS_LOWER}

        total_cypher = """
MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
WITH p.participant_id AS participant_id, st.study_id AS study_id
RETURN count(*) AS total
""".strip()

        missing_cypher = """
MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
WITH p.participant_id AS participant_id, st.study_id AS study_id, collect(d) AS diagnoses
WHERE size([
    d IN diagnoses WHERE d IS NOT NULL
    AND d.diagnosis_category IS NOT NULL
    AND any(tok IN split(toString(d.diagnosis_category), ';')
            WHERE toLower(trim(tok)) IN $harmonized_pvs_lower)
]) = 0
RETURN count(*) AS missing
""".strip()

        values_cypher = """
MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
WHERE d.diagnosis_category IS NOT NULL
MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
WITH p.participant_id AS participant_id, st.study_id AS study_id,
     [tok IN split(toString(d.diagnosis_category), ';') WHERE trim(tok) <> ''] AS tokens
UNWIND tokens AS raw_token
WITH participant_id, study_id, trim(raw_token) AS token
WITH participant_id, study_id, token,
     [pv IN $harmonized_pvs WHERE toLower(pv) = toLower(token)][0] AS matched_pv
WHERE matched_pv IS NOT NULL
WITH DISTINCT participant_id, study_id, matched_pv
RETURN matched_pv AS value, count(*) AS count
ORDER BY count DESC, value ASC
""".strip()

        max_retries = 2
        retry_count = 0
        total_count = 0
        missing_count = 0
        values_records: list = []

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
                    logger.debug(f"Retrying count_subjects_by_diagnosis_category (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning("Error in count_subjects_by_diagnosis_category, retrying", error=str(e))
                else:
                    logger.error("Error in count_subjects_by_diagnosis_category after retries",
                                 error=str(e), exc_info=True)
                    raise

        counts = [
            {"value": r.get("value"), "count": r.get("count", 0)}
            for r in values_records
        ]
        counts.sort(key=lambda x: (-x["count"], x["value"]))

        logger.info(
            "Completed subject count by diagnosis_category",
            total=total_count, missing=missing_count, values_count=len(counts)
        )
        return {"total": total_count, "missing": missing_count, "values": counts}
