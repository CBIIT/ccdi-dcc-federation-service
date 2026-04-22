"""
Subject repository for the CCDI Federation Service.

This module provides data access operations for subjects
using Cypher queries to Memgraph.
"""

import asyncio
import dataclasses
import re
from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.core.constants import Race
from app.core.diagnosis_category import HARMONIZED_DIAGNOSIS_CATEGORIES
from app.core.field_mappings import map_field_value, reverse_map_field_value, is_database_only_value, build_case_mapping_statement
from app.lib.field_allowlist import FieldAllowlist
from app.lib.url_builder import build_identifier_server_url
from app.models.dto import Subject
from app.models.errors import UnsupportedFieldError
from app.utils.cypher_builder import combine_where_clauses, append_where_conditions
from app.repositories.subject_count import SubjectCount
from app.repositories.subject_summary import SubjectSummary

logger = get_logger(__name__)


@dataclasses.dataclass
class SubjectFilterState:
    """Filter state produced by _build_subject_where."""
    where_conditions: list
    params: dict
    param_counter: int
    race_condition: str = ""
    race_filter_condition: str = ""
    race_where_clause: str = ""
    identifiers_condition: str = ""
    identifiers_early_filter: Optional[str] = None
    dep_param: Optional[str] = None
    deposition_operator: Optional[str] = None
    depositions_list: Optional[list] = None
    diagnosis_search_term: Optional[str] = None
    diag_category_filter: Optional[str] = None
    early_participant_filters: list = dataclasses.field(default_factory=list)
    late_participant_filters: list = dataclasses.field(default_factory=list)
    where_clause: str = ""
    derived_where_clause: str = ""
    derived_filters: dict = dataclasses.field(default_factory=dict)
    derived_conditions: list = dataclasses.field(default_factory=list)
    needs_survival_processing: bool = False
    needs_diagnosis_processing: bool = False
    needs_race_processing: bool = False


class SubjectRepository(SubjectCount, SubjectSummary):
    """Repository for subject data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings=None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings

    @staticmethod
    def _split_or_values(value: Any) -> Optional[List[str]]:
        """
        Normalize `||`-separated query params into a clean list of strings.

        Examples:
        - "a||b" -> ["a","b"]
        - "a" -> ["a"]
        - ["a", "b"] -> ["a", "b"]  # Already a list, return as-is
        - None / "" -> None
        """
        if value is None:
            return None
        # If value is already a list, return it as-is (don't convert to string)
        if isinstance(value, list):
            # Filter out empty strings and return
            items = [str(v).strip() for v in value if v]
            return items or None
        s = str(value).strip()
        if not s:
            return None
        if "||" in s:
            items = [v.strip() for v in s.split("||")]
            items = [v for v in items if v]
            return items or None
        return [s]
    
    @staticmethod
    def _diagnosis_conditions(diagnosis_search_term: Optional[str], diag_category_filter: Optional[str]) -> list[str]:
        conditions = []
        if diagnosis_search_term:
            conditions.append(
                "size([node IN diagnosis_nodes WHERE node IS NOT NULL AND ANY(diag IN CASE WHEN valueType(node.diagnosis) = 'LIST' THEN node.diagnosis ELSE [node.diagnosis] END WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))]) > 0"
            )
        if diag_category_filter:
            conditions.append(
                "size([node IN diagnosis_nodes WHERE node IS NOT NULL AND any(token IN split(toString(coalesce(node.diagnosis_category, '')), ';') WHERE toLower(trim(token)) = toLower($diag_category_filter))]) > 0"
            )
        return conditions

    @staticmethod
    def _build_diagnosis_filter_where(diagnosis_search_term: Optional[str], diag_category_filter: Optional[str]) -> str:
        conditions = SubjectRepository._diagnosis_conditions(diagnosis_search_term, diag_category_filter)
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    @staticmethod
    def _build_combined_where_clause_for_depositions_path(
        diagnosis_search_term: Optional[str],
        dep_param: Optional[str],
        deposition_operator: Optional[str],
        diag_category_filter: Optional[str] = None,
    ) -> str:
        conditions = SubjectRepository._diagnosis_conditions(diagnosis_search_term, diag_category_filter)
        if dep_param:
            conditions.append(f"size([sid IN study_ids WHERE sid IS NOT NULL AND sid {deposition_operator} ${dep_param}]) > 0")
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    def _build_subject_where(
        self,
        filters: Dict[str, Any],
        initial_params: Optional[Dict[str, Any]] = None,
    ) -> "SubjectFilterState":
        """
        Build the WHERE filter state for subject queries.

        Extracts all filter logic from get_subjects so that other methods
        (e.g., a count query) can share the same filter state without
        duplicating logic.

        Args:
            filters: Dictionary of field filters (will be copied — caller's dict is not mutated).
            initial_params: Seed params (e.g., {"offset": ..., "limit": ...}) to include in the
                            returned params dict.

        Returns:
            SubjectFilterState with all filter variables populated.
        """
        # IMPORTANT: avoid mutating the caller's dict
        filters = dict(filters or {})
        params: Dict[str, Any] = dict(initial_params or {})
        param_counter = 0

        # Handle race parameter normalization
        # Race filter must be applied after WITH clause defines variables
        race_condition = ""
        race_filter_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                # Normalize race value to a list (handle both string and list inputs)
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []

                if race_list:
                    # Apply reverse mapping: convert API values to database values for filtering
                    db_race_list = []
                    for api_race in race_list:
                        db_race = reverse_map_field_value("race", api_race)
                        if db_race:
                            if isinstance(db_race, list):
                                db_race_list.extend(db_race)
                            else:
                                db_race_list.append(db_race)
                        else:
                            db_race_list.append(api_race)

                    # Remove duplicates while preserving order
                    db_race_list = list(dict.fromkeys(db_race_list))

                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = db_race_list

                    # Check if "Not Reported" is in the filter
                    includes_not_reported = any(r.strip() == "Not Reported" for r in race_list)

                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""

                    if includes_not_reported:
                        race_filter_condition = """(reduce(found = false, tok IN race_tokens | found OR tok IN pr_tokens) OR \n                        (size(pr_tokens) > 0 AND reduce(all_hispanic = true, pt IN pr_tokens | all_hispanic AND pt = 'Hispanic or Latino') AND 'Not Reported' IN race_tokens))"""
                    else:
                        race_filter_condition = "reduce(found = false, tok IN race_tokens | found OR (tok IN pr_tokens AND tok <> 'Hispanic or Latino'))"

        race_where_clause = f"WHERE {race_filter_condition}" if race_filter_condition else ""

        # Handle identifiers parameter normalization
        identifiers_condition = ""
        identifiers_early_filter = None
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            identifiers_list = self._split_or_values(identifiers_value)
            if identifiers_list:
                identifiers_value = identifiers_list[0] if len(identifiers_list) == 1 else identifiers_list
                if identifiers_value:
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
                    if isinstance(identifiers_value, list):
                        identifiers_early_filter = f"p.participant_id IN ${id_param}"
                    else:
                        identifiers_early_filter = f"p.participant_id = ${id_param}"

        # where_conditions list — collects conditions appended during filter building
        where_conditions: list = []
        if identifiers_condition:
            where_conditions.append("p.participant_id IN id_list")

        # Handle depositions filter (study_id)
        dep_param = None
        depositions_list = None
        deposition_operator = None
        if "depositions" in filters:
            depositions_value = filters.pop("depositions")
            depositions_list = self._split_or_values(depositions_value)
            logger.debug(f"Depositions filter provided: {depositions_value}, parsed list: {depositions_list}")
            if depositions_list:
                param_counter += 1
                dep_param = f"param_{param_counter}"
                if len(depositions_list) == 1:
                    params[dep_param] = depositions_list[0]
                    deposition_operator = "="
                else:
                    params[dep_param] = depositions_list
                    deposition_operator = "IN"
                logger.debug(f"Depositions filter: param={dep_param}, operator={deposition_operator}, value={params[dep_param]}")
            else:
                logger.debug("Depositions filter provided but depositions_list is empty/None")
        else:
            logger.debug("No depositions filter in filters dict")

        # Handle diagnosis search
        diagnosis_search_term = None
        if "_diagnosis_search" in filters:
            diagnosis_search_term = filters.pop("_diagnosis_search")
            params["diagnosis_search_term"] = diagnosis_search_term

        # Handle associated_diagnosis_categories filter
        diag_category_filter: Optional[str] = None
        if "associated_diagnosis_categories" in filters:
            raw_cat = filters.pop("associated_diagnosis_categories")
            if raw_cat and str(raw_cat).strip():
                diag_category_filter = str(raw_cat).strip()
                params["diag_category_filter"] = diag_category_filter

        # Separate derived fields from direct participant fields
        derived_filters: dict = {}
        derived_conditions: list = []

        # Map API sex values to database values
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]

        # Field name mapping for participant properties
        field_name_mapping = {
            "sex": "sex_at_birth",
        }

        # Separate early filters (MATCH WHERE) from late filters (WITH clause)
        early_participant_filters: list = []
        late_participant_filters: list = []

        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            if field == "ethnicity":
                desired = str(value).strip() if value is not None else ""
                desired_lower = desired.lower()
                param_counter += 1
                param_name = f"param_{param_counter}"
                params[param_name] = "Hispanic or Latino"
                if desired_lower == "hispanic or latino":
                    condition = f"(p.race IS NOT NULL AND toString(p.race) CONTAINS ${param_name})"
                else:
                    condition = f"(p.race IS NULL OR trim(toString(p.race)) = '' OR NOT toString(p.race) CONTAINS ${param_name})"
                early_participant_filters.append(condition)
                continue

            if field in {"vital_status", "age_at_vital_status"}:
                derived_filters[field] = value
                continue

            db_field = field_name_mapping.get(field, field)
            param_counter += 1
            param_name = f"param_{param_counter}"
            condition = f"p.{db_field} IN ${param_name}" if isinstance(value, list) else f"p.{db_field} = ${param_name}"
            params[param_name] = value

            if field == "sex" or db_field == "sex_at_birth":
                early_participant_filters.append(condition)
            else:
                late_participant_filters.append(condition)
                where_conditions.append(condition)

        # Build WHERE clause for filters that need to be applied after WITH clause
        where_clause = ""
        if late_participant_filters:
            filtered_conditions = [c for c in late_participant_filters if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)
        elif where_conditions:
            filtered_conditions = [c for c in where_conditions if c and c.strip()]
            if filtered_conditions:
                where_clause = "WHERE " + " AND ".join(filtered_conditions)

        # Build WHERE clause for derived fields
        if derived_filters:
            for field, value in derived_filters.items():
                param_counter += 1
                param_name = f"derived_{field}_{param_counter}"
                if field == "vital_status":
                    if is_database_only_value("vital_status", value):
                        derived_conditions.append("false")
                    else:
                        derived_conditions.append(
                            f"(final_vital_status IS NOT NULL AND toLower(toString(final_vital_status)) = toLower(toString(${param_name})))"
                        )
                        value = reverse_map_field_value("vital_status", value) if value else value
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                    try:
                        value = int(value) if value is not None else value
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid age_at_vital_status value: {value}, keeping as-is")
                params[param_name] = value

        derived_where_clause = ""
        if derived_conditions:
            filtered_derived = [c for c in derived_conditions if c and c.strip()]
            filtered_derived = [c for c in filtered_derived if 'study_id' not in c.lower()]
            if filtered_derived:
                derived_where_clause = "WHERE " + " AND ".join(filtered_derived)

        needs_survival_processing = bool(
            derived_filters.get("vital_status")
            or derived_filters.get("age_at_vital_status")
        )
        needs_diagnosis_processing = bool(diagnosis_search_term or diag_category_filter)
        needs_race_processing = bool(race_condition)

        return SubjectFilterState(
            where_conditions=where_conditions,
            params=params,
            param_counter=param_counter,
            race_condition=race_condition,
            race_filter_condition=race_filter_condition,
            race_where_clause=race_where_clause,
            identifiers_condition=identifiers_condition,
            identifiers_early_filter=identifiers_early_filter,
            dep_param=dep_param,
            deposition_operator=deposition_operator,
            depositions_list=depositions_list,
            diagnosis_search_term=diagnosis_search_term,
            diag_category_filter=diag_category_filter,
            early_participant_filters=early_participant_filters,
            late_participant_filters=late_participant_filters,
            where_clause=where_clause,
            derived_where_clause=derived_where_clause,
            derived_filters=derived_filters,
            derived_conditions=derived_conditions,
            needs_survival_processing=needs_survival_processing,
            needs_diagnosis_processing=needs_diagnosis_processing,
            needs_race_processing=needs_race_processing,
        )

    async def get_subjects(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20,
        base_url: Optional[str] = None,
        return_total: bool = False,
    ) -> "List[Subject] | tuple[list, int]":
        """
        Get paginated list of subjects with filtering.

        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            base_url: Base URL for identifier links
            return_total: If True, returns (subjects, total_count) tuple instead of list

        Returns:
            List of Subject objects, or (list, int) tuple when return_total=True

        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching subjects",
            filters=filters,
            offset=offset,
            limit=limit
        )

        # Initialize cypher to avoid "variable not associated with value" error
        cypher = None

        fs = self._build_subject_where(
            filters, initial_params={"offset": offset, "limit": limit}
        )
        where_conditions = fs.where_conditions
        params = fs.params
        param_counter = fs.param_counter
        race_condition = fs.race_condition
        race_filter_condition = fs.race_filter_condition
        race_where_clause = fs.race_where_clause
        identifiers_condition = fs.identifiers_condition
        identifiers_early_filter = fs.identifiers_early_filter
        dep_param = fs.dep_param
        deposition_operator = fs.deposition_operator
        depositions_list = fs.depositions_list
        diagnosis_search_term = fs.diagnosis_search_term
        diag_category_filter = fs.diag_category_filter
        early_participant_filters = fs.early_participant_filters
        late_participant_filters = fs.late_participant_filters
        where_clause = fs.where_clause
        derived_where_clause = fs.derived_where_clause
        derived_filters = fs.derived_filters
        derived_conditions = fs.derived_conditions
        needs_survival_processing = fs.needs_survival_processing
        needs_diagnosis_processing = fs.needs_diagnosis_processing
        needs_race_processing = fs.needs_race_processing
        depositions_query_built = False

        # --- Count query when return_total is requested ---
        total_count: Optional[int] = None
        if return_total:
            # Collect all WHERE conditions into one clause (no double-WHERE risk)
            main_conditions: list = []
            if fs.race_filter_condition:
                main_conditions.append(fs.race_filter_condition)
            main_conditions.extend(c for c in fs.where_conditions if c)

            # diag_category_filter requires OPTIONAL MATCH on diagnosis nodes
            diag_cat_fragment = ""
            if fs.diag_category_filter:
                diag_cat_fragment = (
                    "\nWITH p"
                    "\nOPTIONAL MATCH (p)<-[:of_diagnosis]-(diag_cat:diagnosis)"
                    "\nWITH p, collect(DISTINCT diag_cat) AS diag_cat_nodes"
                    "\nWHERE size([dn IN diag_cat_nodes WHERE dn IS NOT NULL AND "
                    "any(token IN split(toString(coalesce(dn.diagnosis_category, '')), ';') "
                    "WHERE toLower(trim(token)) = toLower($diag_category_filter))]) > 0"
                )

            if fs.dep_param:
                dep_condition = (
                    f"size([sid IN study_ids WHERE sid IS NOT NULL"
                    f" AND sid {fs.deposition_operator} ${fs.dep_param}]) > 0"
                )
                all_conditions = [dep_condition] + main_conditions
                count_where_clause = "WHERE " + " AND ".join(all_conditions)
                count_cypher = (
                    f"MATCH (p:participant)-[:of_participant]->(:consent_group)"
                    f"-[:of_consent_group]->(st:study)\n"
                    f"WITH p, collect(DISTINCT st.study_id) AS study_ids\n"
                    f"WITH p, study_ids{fs.race_condition}{fs.identifiers_condition}\n"
                    f"{count_where_clause}"
                    f"{diag_cat_fragment}\n"
                    f"RETURN count(DISTINCT p) AS total_count"
                )
            else:
                count_where_clause = ("WHERE " + " AND ".join(main_conditions)) if main_conditions else ""
                count_cypher = (
                    f"MATCH (p:participant)\n"
                    f"WITH p{fs.race_condition}{fs.identifiers_condition}\n"
                    + (f"{count_where_clause}\n" if count_where_clause else "")
                    + f"{diag_cat_fragment}\n"
                    + "RETURN count(DISTINCT p) AS total_count"
                )

            try:
                count_result = await self.session.run(count_cypher.strip(), fs.params)
                count_records = []
                async for row in count_result:
                    count_records.append(dict(row))
                await count_result.consume()
                total_count = count_records[0].get("total_count", 0) if count_records else 0
            except Exception as exc:
                logger.warning("return_total count query failed", error=str(exc))
                total_count = 0

        # Fast path: No derived-field filtering, diagnosis-search filtering, or race token filtering needed.
        if not needs_survival_processing and not needs_diagnosis_processing and not needs_race_processing:
            # Simple query - just get participants with studies, no complex processing
            if dep_param:
                # Depositions filter present - use required MATCH
                early_where_conditions = [f"st.study_id {deposition_operator} ${dep_param}"]
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
                
                # For depositions list requests, we want to populate `vital_status`, `age_at_vital_status`,
                # and `associated_diagnoses`, but we MUST keep the Cypher very simple to avoid Memgraph
                # instability/crashes seen with complex `reduce()`/list-processing.
                #
                # Strategy: fetch survivals + diagnosis nodes as raw collections and compute derived fields in Python.
                cypher = f"""
        MATCH (p:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p,
             collect(s) AS survival_records,
             collect(DISTINCT d) AS diagnosis_nodes
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name)
        """.strip()
            else:
                # No depositions filter - use OPTIONAL MATCH for studies
                early_where_conditions = []
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""

                # If identifiers are present, return raw survival/diagnosis collections (computed in Python)
                # to match dcc-dev's populated output for identifier lookups, without requiring derived-field filtering.
                # IMPORTANT: Group by (participant_id, study_id) pairs to ensure consistency with individual endpoint
                if identifiers_early_filter:
                    cypher = f"""
        MATCH (p:participant){early_where_clause}
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        WHERE study_id IS NOT NULL
        WITH participant_id, study_id, p
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH participant_id, study_id, p, collect(s) AS survival_records
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Apply depositions filter if present
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        WHERE study_id IS NOT NULL{f" AND study_id {deposition_operator} ${dep_param}" if dep_param else ""}
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name), toString(study_id)
        """.strip()
                else:
                    # No identifiers filter - fetch survival and diagnosis records for Python computation
                    # Strategy: fetch survivals + diagnosis nodes as raw collections and compute derived fields in Python.
                    # IMPORTANT: Group by (participant_id, study_id) pairs to ensure consistency with individual endpoint
                    # This ensures that participants in multiple studies are returned with the correct study_id
                    cypher = f"""
        MATCH (p:participant){early_where_clause}
        OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        WITH toString(p.participant_id) AS participant_id, p, st.study_id AS study_id
        WHERE study_id IS NOT NULL
        WITH participant_id, study_id, p
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH participant_id, study_id, p, collect(s) AS survival_records
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH participant_id, study_id, p, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Apply depositions filter if present
        WITH participant_id, study_id, p, survival_records, diagnosis_nodes
        WHERE study_id IS NOT NULL{f" AND study_id {deposition_operator} ${dep_param}" if dep_param else ""}
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          NULL AS age_at_vital_status,
          NULL AS vital_status,
          NULL AS associated_diagnoses,
          survival_records AS survival_records,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name), toString(study_id)
        """.strip()
        else:
            # Full processing path - use existing complex query logic
            # When depositions filter is present, we need required MATCH for studies
            # But we can't put MATCH after OPTIONAL MATCH, so we need to restructure
            # Check if dep_param is set AND exists in params (more robust check)
            condition_result = bool(dep_param and dep_param in params)
            if condition_result:
                logger.debug(f"Using depositions path: dep_param={dep_param}, operator={deposition_operator}")
                # Remove depositions filter from where_clause since it's applied in the MATCH WHERE
                dep_filter_str = f"st.study_id {deposition_operator} ${dep_param}"
                # Also remove identifiers condition if present (it's applied in MATCH WHERE when identifiers_condition exists)
                id_filter_str = "p.participant_id IN id_list"
                # Also remove early participant filters (sex, etc.) since they're applied in MATCH WHERE
                # Note: race_filter_condition is NOT in where_clause (it's handled separately)
                where_clause_no_dep = where_clause.replace(f"WHERE {dep_filter_str}", "").replace(f"AND {dep_filter_str}", "").replace(f"{dep_filter_str} AND", "").replace(dep_filter_str, "").strip() if where_clause else ""
                # Remove early participant filters if they're in where_clause
                for early_filter in early_participant_filters:
                    where_clause_no_dep = where_clause_no_dep.replace(f"WHERE {early_filter}", "").replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip() if where_clause_no_dep else ""
                # Remove identifiers condition if identifiers_condition is present
                if identifiers_condition:
                    where_clause_no_dep = where_clause_no_dep.replace(f"WHERE {id_filter_str}", "").replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip() if where_clause_no_dep else ""
                # Clean up any double WHERE or AND issues
                other_filters = ""
                if where_clause_no_dep:
                    where_clause_no_dep = where_clause_no_dep.replace("WHERE WHERE", "WHERE").replace("AND AND", "AND").replace("  ", " ").strip()
                    # Remove trailing AND
                    while where_clause_no_dep.endswith(" AND"):
                        where_clause_no_dep = where_clause_no_dep[:-4].strip()
                    # Remove leading AND
                    while where_clause_no_dep.startswith("AND "):
                        where_clause_no_dep = where_clause_no_dep[4:].strip()
                    # Remove WHERE prefix if present
                    if where_clause_no_dep.startswith("WHERE "):
                        other_filters = where_clause_no_dep[6:].strip()  # Remove "WHERE " prefix
                    elif where_clause_no_dep and where_clause_no_dep != "WHERE":
                        other_filters = where_clause_no_dep
                    # Final cleanup - remove any remaining trailing/leading AND
                    while other_filters.endswith(" AND"):
                        other_filters = other_filters[:-4].strip()
                    while other_filters.startswith("AND "):
                        other_filters = other_filters[4:].strip()
                    # If other_filters is empty or just whitespace/AND after cleanup, set to empty
                    if not other_filters or other_filters.strip() == "" or other_filters.strip() == "AND":
                        other_filters = ""
                    # Remove any references to study_id (the variable) since it's not defined yet in this query path
                    if other_filters:
                        other_filters = other_filters.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                        # Clean up any resulting "AND AND" or trailing/leading AND
                        other_filters = other_filters.replace("AND AND", "AND").strip()
                        while other_filters.startswith("AND "):
                            other_filters = other_filters[4:].strip()
                        while other_filters.endswith(" AND"):
                            other_filters = other_filters[:-4].strip()
                        if not other_filters or other_filters.strip() == "" or other_filters.strip() == "AND":
                            other_filters = ""
                
                # Build WITH clause with optional WHERE
                # Note: 'c' (consent_group) is no longer needed since we use participant -> consent_group -> study relationship
                with_clause = f"WITH p, s, d, st{race_condition}{identifiers_condition}"
                # Build WHERE conditions for WITH clause
                with_where_conditions = []
                # Don't apply identifiers filter here if it's already applied early in MATCH WHERE clause
                # (identifiers_early_filter is applied in MATCH WHERE, so id_list normalization is still needed but filter is already applied)
                if identifiers_condition and not identifiers_early_filter:
                    with_where_conditions.append("p.participant_id IN id_list")
                # Add other filters if any
                if other_filters and other_filters.strip() and other_filters != "AND":
                    with_where_conditions.append(other_filters)
                # Add race filter if it exists (race_tokens/pr_tokens are defined in WITH clause)
                if race_filter_condition:
                    with_where_conditions.append(race_filter_condition)
                # Apply WHERE clause if we have any conditions
                if with_where_conditions:
                    with_clause += f"\n        WHERE {' AND '.join(with_where_conditions)}"
                
                # For the depositions path, we need to apply derived filters AFTER calculation (line 381)
                # So we keep derived_where_clause as is for later application
                
                # Required study match - put it before OPTIONAL MATCHes
                # OPTIMIZATION: Apply filters early in MATCH WHERE clause to reduce dataset before expensive OPTIONAL MATCH operations
                # This includes: depositions, identifiers, and simple participant property filters (sex, etc.)
                early_where_conditions = [f"st.study_id {deposition_operator} ${dep_param}"]
                if identifiers_early_filter:
                    early_where_conditions.append(identifiers_early_filter)
                # Add simple participant property filters (sex, etc.) for early filtering
                if early_participant_filters:
                    early_where_conditions.extend(early_participant_filters)
                early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
                logger.debug(f"Depositions path - early_where_conditions: {early_where_conditions}")
                
                # CRITICAL: When we have derived filters (vital_status, age_at_vital_status) or diagnosis search,
                # we CANNOT paginate early because we need to compute/filter first, then paginate.
                # Early pagination would skip participants before filtering, causing incorrect results.
                use_early_pagination_dep = not needs_survival_processing and not needs_diagnosis_processing
                logger.debug(f"Depositions path - use_early_pagination_dep: {use_early_pagination_dep}, needs_survival_processing: {needs_survival_processing}, needs_diagnosis_processing: {needs_diagnosis_processing}")
                
                if use_early_pagination_dep:
                    # OPTIMIZATION: Apply pagination EARLY - before expensive survival/diagnosis processing
                    # Step 1: Match participants with studies, apply filters, and paginate
                    # Step 2: Process survival/diagnosis ONLY for the paginated subset
                    # Build simplified WITH clause for pagination (without survival/diagnosis, but keep race/identifiers variables)
                    pagination_with_clause = f"WITH p, st{race_condition}{identifiers_condition}"
                    pagination_with_where_conditions = []
                    if identifiers_condition and not identifiers_early_filter:
                        pagination_with_where_conditions.append("p.participant_id IN id_list")
                    if other_filters and other_filters.strip() and other_filters != "AND":
                        pagination_with_where_conditions.append(other_filters)
                    if race_filter_condition:
                        pagination_with_where_conditions.append(race_filter_condition)
                    if pagination_with_where_conditions:
                        pagination_with_clause += f"\n        WHERE {' AND '.join(pagination_with_where_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants with studies, apply filters, and paginate EARLY
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        {pagination_with_clause}
        // Group by participant_id + study_id, then paginate BEFORE processing survival/diagnosis
        WITH toString(p.participant_id) AS participant_id, st.study_id AS study_id, p, st{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit
        // Step 2: Now process survival/diagnosis ONLY for the paginated subset
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""},
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Group by (participant_id, study_id) to preserve pairs
        // IMPORTANT: The same participant_id string can represent different DB nodes (different participant.id)
        // when associated with different study_id values. We must preserve (participant_id, study_id) pairs.
        WITH toString(p.participant_id) AS participant_id, p, d, st, st.study_id AS study_id, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        WITH participant_id, study_id,
             // Use head to get first participant node (they're all the same per participant_id)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT d) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value,
             // Collect all distinct study_ids for this participant (collect from study_id variable, not st.study_id since st is no longer in scope)
             collect(DISTINCT study_id) AS study_ids
        {self._build_combined_where_clause_for_depositions_path(diagnosis_search_term, dep_param, deposition_operator, diag_category_filter)}
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids, diagnosis_nodes,
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids, diagnosis_nodes,
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids, diagnosis_nodes,
             all_diagnoses_list AS d
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, diagnosis_nodes,
             [sid IN study_ids WHERE sid IS NOT NULL{f" AND sid {deposition_operator} ${dep_param}" if dep_param else ""}] AS study_ids_temp
        UNWIND study_ids_temp AS sid
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, diagnosis_nodes, sid
        WHERE toString(sid) <> ''
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(sid) AS namespace,
          [sid] AS depositions
        ORDER BY toString(name), toString(sid)
                    """.strip()
                else:
                    # When we have derived filters (vital_status, age_at_vital_status), we MUST process survival/diagnosis first,
                    # then filter, then paginate. We cannot paginate early because the filter depends on computed values.
                    # Build a WITH clause for this path (without s and d, which aren't matched yet)
                    alt_with_clause = f"WITH p, st{race_condition}{identifiers_condition}"
                    alt_with_where_conditions = []
                    # Apply identifiers filter if present
                    # Note: Even if identifiers_early_filter is set (applied in MATCH WHERE), we still apply it here
                    # as a backup to ensure the filter is applied correctly after OPTIONAL MATCH operations
                    if identifiers_condition:
                        alt_with_where_conditions.append("p.participant_id IN id_list")
                    # Add race filter if present
                    if race_filter_condition:
                        alt_with_where_conditions.append(race_filter_condition)
                    if alt_with_where_conditions:
                        alt_with_clause += f"\n        WHERE {' AND '.join(alt_with_where_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants with studies and apply direct filters
        MATCH (p:participant)
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study){early_where_clause}
        {alt_with_clause}
        // Step 2: Process survival/diagnosis BEFORE filtering by derived fields
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""},
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st{", race_tokens, pr_tokens" if race_condition else ""}, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Step 3: Keep (participant_id, study_id) pairs - do NOT group by participant_id only
        // This allows multiple rows per participant_id if they have multiple study_ids
        // IMPORTANT: We maintain (participant_id, study_id) pairs throughout to match the summary count
        WITH toString(p.participant_id) AS participant_id, p, d, st, st.study_id AS study_id, final_vital_status, final_age_at_vital_status,
             ethnicity_value{", race_tokens, pr_tokens" if race_condition else ""}
        // Group by (participant_id, study_id) to preserve pairs while aggregating other fields
        // IMPORTANT: The same participant_id string can represent different DB nodes (different participant.id)
        // when associated with different study_id values. We must preserve (participant_id, study_id) pairs.
        WITH participant_id, study_id,
             // Use head to get first participant node (within a (participant_id, study_id) pair, there should be one node)
             head(collect(DISTINCT p)) AS p,
             // Collect all diagnosis nodes - we'll aggregate diagnoses from all nodes
             collect(DISTINCT d) AS diagnosis_nodes,
             // final_vital_status and final_age_at_vital_status are already calculated per participant, just take first
             head(collect(DISTINCT final_vital_status)) AS final_vital_status,
             coalesce(head(collect(DISTINCT final_age_at_vital_status)), -999) AS final_age_at_vital_status,
             head(collect(DISTINCT ethnicity_value)) AS ethnicity_value{", head(collect(DISTINCT race_tokens)) AS race_tokens, head(collect(DISTINCT pr_tokens)) AS pr_tokens" if race_condition else ""},
             // Keep study_id as a single value (not a list) since we're grouping by (participant_id, study_id)
             study_id AS study_ids_single
        // Apply diagnosis search / category filter (only if present)
        // Note: Depositions filter is already applied in the MATCH clause (WHERE st.study_id = $param_1)
        // Since we're grouping by (participant_id, study_id), all rows already match the depositions filter
        {self._build_diagnosis_filter_where(diagnosis_search_term, diag_category_filter)}
        // Keep (participant_id, study_id) pairs - do NOT group by participant_id only
        WITH participant_id, study_id, p, diagnosis_nodes, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single{", race_tokens, pr_tokens" if race_condition else ""},
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single, diagnosis_nodes{", race_tokens, pr_tokens" if race_condition else ""},
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value, study_ids_single, diagnosis_nodes{", race_tokens, pr_tokens" if race_condition else ""},
             all_diagnoses_list AS d
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, diagnosis_nodes,
             study_ids_single AS namespace,
             study_ids_single AS sid{", race_tokens, pr_tokens" if race_condition else ""}
        WHERE toString(sid) <> ''{f" AND sid {deposition_operator} ${dep_param}" if dep_param else ""}
        WITH participant_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value, diagnosis_nodes, namespace, sid{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id, sid
        SKIP $offset
        LIMIT $limit
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          [sid] AS depositions
        """.strip()
                depositions_query_built = True
            else:
                # Non-depositions path — depositions_query_built remains False
                if depositions_query_built:
                    logger.error("BUG: cypher is already set from depositions path, but else block is executing! This should never happen.")
                    logger.error("Skipping non-depositions path code since cypher is already set.")
                    # Cypher already set from depositions path above
                else:
                    # No depositions filter - use OPTIONAL MATCH for studies
                    # Collect relationships separately to avoid cartesian product
                    # Build WITH clause with optional WHERE
                    with_clause = f"WITH p{race_condition}{identifiers_condition}"
            # Build WHERE conditions for WITH clause
            with_where_conditions = []
            # Always apply identifiers filter if present (id_list is created in WITH clause)
            if identifiers_condition:
                with_where_conditions.append("p.participant_id IN id_list")
            # Add other filters from where_clause (excluding identifiers condition, race_filter_condition, and st.study_id conditions)
            # Note: st.study_id conditions are for depositions filter, which is not used in non-depositions path
            if where_clause:
                # Remove "WHERE " prefix if present
                where_conditions_str = where_clause.replace("WHERE ", "").strip()
                # Remove identifiers condition if present (already added above)
                id_filter_str = "p.participant_id IN id_list"
                where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                # Remove race_filter_condition if present (it will be added separately since race_tokens is defined in WITH clause)
                if race_filter_condition:
                    where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                # Remove early participant filters (sex, etc.) since they're applied in MATCH WHERE
                for early_filter in early_participant_filters:
                    where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                # Remove st.study_id conditions (depositions filter) - these are not applicable in non-depositions path
                # Remove patterns like "st.study_id = $param" or "st.study_id IN $param" with surrounding ANDs
                where_conditions_str = where_conditions_str.replace("st.study_id =", "").replace("st.study_id IN", "").replace("st.study_id=", "").replace("st.study_idIN", "")
                # Remove any references to study_id (the variable) since it's not defined yet in this query path
                # Remove patterns like "study_id = $param", "study_id IN $param", "study_id=$param", etc.
                # Use multiple passes to catch all variations - be very aggressive
                # First, remove complete conditions with study_id
                # Remove patterns like "study_id = $param_X" or "study_id IN $param_X" or "study_id=$param_X"
                where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                # Remove patterns like "$param_X = study_id" or "$param_X IN study_id"
                where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                # Remove standalone study_id references
                where_conditions_str = where_conditions_str.replace("study_id =", "").replace("study_id IN", "").replace("study_id=", "").replace("study_idIN", "")
                where_conditions_str = where_conditions_str.replace("study_id ", "").replace(" study_id", "").replace("(study_id", "").replace("study_id)", "")
                # Remove any remaining study_id references (be very aggressive)
                where_conditions_str = re.sub(r'\bstudy_id\b', '', where_conditions_str)
                # Remove any parameter references that might be left (e.g., "$param_1" if it was for st.study_id)
                # But be careful - we can't safely remove all $param references as they might be for other fields
                # Clean up any resulting "AND AND" or trailing/leading AND
                where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                while where_conditions_str.startswith("AND "):
                    where_conditions_str = where_conditions_str[4:].strip()
                while where_conditions_str.endswith(" AND"):
                    where_conditions_str = where_conditions_str[:-4].strip()
                if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND":
                    with_where_conditions.append(where_conditions_str)
            # Add race filter if it exists (race_tokens/pr_tokens are defined in WITH clause)
            if race_filter_condition:
                with_where_conditions.append(race_filter_condition)
            # Apply WHERE clause if we have any conditions
            # Final safety check: remove any study_id references that might have slipped through
            if with_where_conditions:
                # Filter out any conditions that reference study_id (not defined yet in non-depositions path)
                safe_conditions = []
                for condition in with_where_conditions:
                    # Check if condition contains study_id (case-insensitive)
                    if condition and 'study_id' not in condition.lower():
                        safe_conditions.append(condition)
                    else:
                        logger.warning(f"Removing condition with study_id reference (not defined yet): {condition}")
                if safe_conditions:
                    with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
            
            # OPTIMIZATION: Apply filters early in MATCH WHERE clause to reduce dataset before expensive OPTIONAL MATCH operations
            # This includes: identifiers and simple participant property filters (sex, etc.)
            early_where_conditions = []
            if identifiers_early_filter:
                early_where_conditions.append(identifiers_early_filter)
            # Add simple participant property filters (sex, etc.) for early filtering
            if early_participant_filters:
                early_where_conditions.extend(early_participant_filters)
            early_where_clause = f"\n        WHERE {' AND '.join(early_where_conditions)}" if early_where_conditions else ""
            logger.debug(f"Non-depositions path - early_where_conditions: {early_where_conditions}")
            
            # Only execute non-depositions path code if depositions path did not already set cypher
            if depositions_query_built:
                logger.debug("Skipping non-depositions path - cypher already set from depositions path")
                # Skip the rest - cypher is already set
                pass
            else:
                # CRITICAL: When we have derived filters (vital_status, age_at_vital_status), we CANNOT paginate early
                # because we need to compute final_vital_status first, then filter, then paginate.
                # Early pagination only works when we're filtering on direct participant properties (sex, identifiers, etc.)
                # For diagnosis-search, paginate only after diagnosis/study filtering to keep count/page consistency.
                use_early_pagination = (not needs_survival_processing) and (not diagnosis_search_term) and (not diag_category_filter)
                logger.debug(f"Non-depositions path - use_early_pagination: {use_early_pagination}")
                
                if use_early_pagination:
                    # OPTIMIZATION: Apply pagination EARLY - before expensive survival/diagnosis processing
                    # Step 1: Match participants, apply filters, and paginate
                    # Step 2: Process survival/diagnosis/studies ONLY for the paginated subset
                    # Build simplified WITH clause for pagination (without survival/diagnosis, but keep race/identifiers variables)
                    pagination_with_clause = f"WITH p{race_condition}{identifiers_condition}"
                    pagination_with_where_conditions = []
                    if identifiers_condition:
                        pagination_with_where_conditions.append("p.participant_id IN id_list")
                    if where_clause:
                        where_conditions_str = where_clause.replace("WHERE ", "").strip()
                        id_filter_str = "p.participant_id IN id_list"
                        where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                        if race_filter_condition:
                            where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                        for early_filter in early_participant_filters:
                            where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                        # CRITICAL: Remove ALL st. references (depositions filter - not applicable in non-depositions path)
                        # Remove complete conditions with st.study_id
                        where_conditions_str = re.sub(r'\bst\.study_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                        where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bst\.study_id\b', '', where_conditions_str)
                        # Remove any partial st. references (dangling st.)
                        where_conditions_str = re.sub(r'\bAND\s+st\.\s*', ' AND ', where_conditions_str)  # Remove "AND st."
                        where_conditions_str = re.sub(r'\bst\.\s*AND', ' AND', where_conditions_str)  # Remove "st. AND"
                        where_conditions_str = re.sub(r'\bst\.\s*$', '', where_conditions_str)  # Remove trailing "st."
                        where_conditions_str = re.sub(r'^\s*st\.\s*', '', where_conditions_str)  # Remove leading "st."
                        where_conditions_str = re.sub(r'\bst\.\b', '', where_conditions_str)  # Remove any remaining "st."
                        # Remove study_id patterns (variable, not st.study_id)
                        where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                        where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                        # Clean up multiple ANDs and whitespace
                        where_conditions_str = re.sub(r'\s+', ' ', where_conditions_str)  # Normalize whitespace
                        where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                        while where_conditions_str.startswith("AND "):
                            where_conditions_str = where_conditions_str[4:].strip()
                        while where_conditions_str.endswith(" AND"):
                            where_conditions_str = where_conditions_str[:-4].strip()
                        # Final check: if the string is empty or just "AND", don't add it
                        if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND" and where_conditions_str.strip() != "AND":
                            # Final safety: ensure no st. references remain
                            if 'st.' not in where_conditions_str.lower():
                                pagination_with_where_conditions.append(where_conditions_str)
                    if race_filter_condition:
                        pagination_with_where_conditions.append(race_filter_condition)
                    if pagination_with_where_conditions:
                        safe_conditions = [c for c in pagination_with_where_conditions if c and 'study_id' not in c.lower()]
                        if safe_conditions:
                            pagination_with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
                    
                    cypher = f"""
        // Step 1: Match participants, apply filters, and paginate EARLY
        MATCH (p:participant){early_where_clause}
        {pagination_with_clause}
        // Group by participant_id, then paginate BEFORE processing survival/diagnosis
        WITH toString(p.participant_id) AS participant_id, p{", race_tokens, pr_tokens" if race_condition else ""}
        ORDER BY participant_id
        SKIP $offset
        LIMIT $limit
        // Step 2: Now process survival/diagnosis/studies ONLY for the paginated subset
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, collect(s) AS survival_records
        // Collect diagnoses separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Collect studies separately (no cartesian product)
        // Use participant -> consent_group -> study relationship
        {"MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)\n        WHERE st.study_id " + deposition_operator + " $" + dep_param if dep_param else "MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)"}
        // Bind a scalar `study_id` for the row. Avoid carrying a LIST of study IDs through long WITH chains,
        // which Memgraph can mis-handle and report as "Unbound variable".
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes,
             st.study_id AS study_id
        {self._build_diagnosis_filter_where(diagnosis_search_term, diag_category_filter)}
        // Keep `participant_id` and `survival_records` in scope (Memgraph will error if we drop them and reference later)
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes, study_id,
             // Keep only records with a status
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, participant_id{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
             // Check if any record has 'Dead' status
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, participant_id, diagnosis_nodes, study_id, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        // Everything is already aggregated per participant_id (we collected survivals/diagnoses/studies separately).
        // Everything is already aggregated per (participant_id, study_id) for this row.
        WITH participant_id, study_id, p, diagnosis_nodes, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Filter out NULL nodes first
             [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Extract diagnosis values from non-null nodes
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE 
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS all_diagnoses_list
        WITH participant_id, study_id, p, final_vital_status, final_age_at_vital_status,
             ethnicity_value,
             // Return the aggregated diagnoses list (will be processed in Python)
             all_diagnoses_list AS d
        WITH participant_id, study_id, p, d, final_vital_status, final_age_at_vital_status,
             ethnicity_value
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        ORDER BY toString(name)
                """.strip()
                else:
                    # Non-depositions path - should only be taken when dep_param is NOT set
                    # If dep_param is set, we should have taken the depositions path above
                    logger.debug(f"Taking non-depositions path (dep_param={dep_param})")
                # CRITICAL SAFEGUARD: If dep_param is set, we MUST NOT take the non-depositions path
                # This is a safeguard to prevent incorrect queries when dep_param is set but the condition above failed
                if dep_param and dep_param in params:
                    logger.error(
                        "Unexpected path selection: dep_param exists but non-depositions path taken",
                        dep_param=dep_param,
                        params_keys=list(params.keys())
                    )
                # When we have derived filters (vital_status, age_at_vital_status), we MUST process survival/diagnosis first,
                # then filter, then paginate. We cannot paginate early because the filter depends on computed values.
                # Build initial WITH clause (similar to early pagination path, but without pagination)
                initial_with_clause = f"WITH p{race_condition}{identifiers_condition}"
                initial_with_where_conditions = []
                # Apply identifiers filter if present
                # Note: Even if identifiers_early_filter is set (applied in MATCH WHERE), we still apply it here
                # as a backup to ensure the filter is applied correctly after OPTIONAL MATCH operations
                if identifiers_condition:
                    initial_with_where_conditions.append("p.participant_id IN id_list")
                if where_clause:
                    where_conditions_str = where_clause.replace("WHERE ", "").strip()
                    id_filter_str = "p.participant_id IN id_list"
                    where_conditions_str = where_conditions_str.replace(f"AND {id_filter_str}", "").replace(f"{id_filter_str} AND", "").replace(id_filter_str, "").strip()
                    if race_filter_condition:
                        where_conditions_str = where_conditions_str.replace(f"AND {race_filter_condition}", "").replace(f"{race_filter_condition} AND", "").replace(race_filter_condition, "").strip()
                    for early_filter in early_participant_filters:
                        where_conditions_str = where_conditions_str.replace(f"AND {early_filter}", "").replace(f"{early_filter} AND", "").replace(early_filter, "").strip()
                    # Remove st.study_id and study_id references (not applicable before study match)
                    where_conditions_str = re.sub(r'\bst\.study_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bst\.study_id\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bstudy_id\s*[=IN]\s*\$[a-zA-Z0-9_]+', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\$[a-zA-Z0-9_]+\s*[=IN]\s*\bstudy_id\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bst\.\b', '', where_conditions_str)
                    where_conditions_str = re.sub(r'\bstudy_id\b', '', where_conditions_str)
                    where_conditions_str = where_conditions_str.replace("AND AND", "AND").strip()
                    while where_conditions_str.startswith("AND "):
                        where_conditions_str = where_conditions_str[4:].strip()
                    while where_conditions_str.endswith(" AND"):
                        where_conditions_str = where_conditions_str[:-4].strip()
                    if where_conditions_str and where_conditions_str.strip() and where_conditions_str != "AND":
                        if 'st.' not in where_conditions_str.lower() and 'study_id' not in where_conditions_str.lower():
                            initial_with_where_conditions.append(where_conditions_str)
                if race_filter_condition:
                    initial_with_where_conditions.append(race_filter_condition)
                if initial_with_where_conditions:
                    safe_conditions = [c for c in initial_with_where_conditions if c and 'study_id' not in c.lower()]
                    if safe_conditions:
                        initial_with_clause += f"\n        WHERE {' AND '.join(safe_conditions)}"
                
                cypher = f"""
        // Step 1: Match participants and apply direct filters
        MATCH (p:participant){early_where_clause}
        {initial_with_clause}
        // Step 2: Process survival/diagnosis/studies BEFORE filtering by derived fields
        // Collect survivals separately (no cartesian product)
        OPTIONAL MATCH (p)<-[:of_survival]-(s:survival)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, collect(s) AS survival_records
        // Collect diagnoses (always — populates associated_diagnosis_categories on all subjects)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(d:diagnosis)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, collect(DISTINCT d) AS diagnosis_nodes
        // Collect studies separately (no cartesian product)
        // Use participant -> consent_group -> study relationship
        MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
        // Bind a scalar `study_id` for the row. Avoid carrying a LIST of study IDs through long WITH chains,
        // which Memgraph can mis-handle and report as "Unbound variable".
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes,
             st.study_id AS study_id
        {self._build_diagnosis_filter_where(diagnosis_search_term, diag_category_filter)}
        // Keep `participant_id` and `survival_records` in scope (Memgraph will error if we drop them and reference later)
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, survival_records, diagnosis_nodes, study_id,
             // Keep only records with a status
             [sr IN survival_records WHERE sr IS NOT NULL AND sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
             // Check if any record has 'Dead' status
             size([sr IN survs WHERE sr.last_known_survival_status = 'Dead']) > 0 AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p{", race_tokens, pr_tokens" if race_condition else ""}, diagnosis_nodes, study_id, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             toInteger(CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END) AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
                {derived_where_clause}
            WITH p, diagnosis_nodes, study_id, final_vital_status, final_age_at_vital_status, 
            ethnicity_value{", race_tokens, pr_tokens" if race_condition else ""},
            [node IN diagnosis_nodes WHERE node IS NOT NULL] AS non_null_nodes
            WITH toString(p.participant_id) AS participant_id, study_id, p,
             diagnosis_nodes, final_vital_status, final_age_at_vital_status, ethnicity_value{", race_tokens, pr_tokens" if race_condition else ""},
             reduce(all_diagnoses = [], node IN non_null_nodes |
                    CASE
                      WHEN node.diagnosis IS NOT NULL THEN
                        all_diagnoses + [node.diagnosis]
                      ELSE all_diagnoses
                    END) AS d
        ORDER BY participant_id, study_id
        SKIP $offset
        LIMIT $limit

        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          d AS associated_diagnoses,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(study_id) AS namespace,
          [study_id] AS depositions
        """.strip()
        
        # Debug logging for query details (only in debug mode)
        if getattr(self.settings, "debug", False):
            logger.debug(
                "Cypher query details",
                cypher=cypher if 'cypher' in locals() else "not defined",
                params=params,
                where_clause=where_clause if 'where_clause' in locals() else None,
                race_filter_condition=race_filter_condition if 'race_filter_condition' in locals() else None,
                race_condition=race_condition if 'race_condition' in locals() else None
            )
        
        logger.debug(
            "Executing get_subjects Cypher query",
            filters=filters
        )

        # Memgraph can behave inconsistently with `// ...` inline comments in multi-line Cypher.
        # We keep comments for local debugging output, but strip them from the actual query we execute.
        cypher_to_run = "\n".join(
            line for line in (cypher or "").splitlines() if not line.lstrip().startswith("//")
        ).strip()
        
        # Execute query with proper result consumption and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher_to_run, params)
                # Ensure the result is fully consumed - use async iteration for reliability
                records = []
                async for record in result:
                    records.append(dict(record))
                
                # Ensure result is fully consumed
                await result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if records or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_subjects query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_subjects query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    # Log the full query on error for debugging
                    # Log the full query for debugging
                    logger.error(
                        "Error executing get_subjects Cypher query after retries",
                        error=str(e),
                        error_type=type(e).__name__,
                        cypher=cypher if 'cypher' in locals() else None,
                        full_query=cypher if 'cypher' in locals() else None,  # Log full query
                        params_keys=list(params.keys()) if params else [],
                        params=params,  # Log params too
                        where_clause_original=where_clause if 'where_clause' in locals() else None,
                        dep_param=dep_param if 'dep_param' in locals() else None,
                        other_filters=other_filters if 'other_filters' in locals() else None,
                        exc_info=True
                    )
                    raise
        
        # Convert to Subject objects
        subjects = []
        for record in records:
            subjects.append(self._record_to_subject(record, base_url=base_url))
        
        logger.debug(
            "Found subjects",
            count=len(subjects),
            filters=filters
        )

        if return_total:
            return (subjects, total_count if total_count is not None else 0)
        return subjects

    async def get_subject_by_identifier(
        self,
        organization: str,
        namespace: Optional[str],
        name: str,
        base_url: Optional[str] = None
    ) -> Optional[Subject]:
        """
        Get a specific subject by organization, namespace, and name.
        
        Args:
            organization: Organization identifier (defaults to CCDI-DCC)
            namespace: Namespace identifier (study_id value, optional)
            name: Subject name/participant ID
            
        Returns:
            Subject object or None if not found
        """
        logger.debug(
            "Fetching subject by identifier",
            organization=organization,
            namespace=namespace,
            name=name
        )
        
        # Build query parameters
        params = {"participant_id": name}
        
        # If namespace (study_id) is provided, require it to match the participant's study
        # This ensures namespace is a valid study_id for the participant
        if namespace:
            # When namespace is provided, match participant and study together with study_id filter
            # This ensures we only get results where the participant belongs to the specified study
            # IMPORTANT: The WHERE clause filters by study_id FIRST, so only participants
            # belonging to the exact requested study_id will be matched
            params["namespace"] = namespace
            cypher = f"""
        // First, find the study with the requested study_id
        MATCH (st:study)
        WHERE st.study_id = $namespace
        // Then find participants that belong to this specific study
        // The relationship path must be: participant -> consent_group -> study
        // This ensures we only match participants that are actually connected to the requested study
        MATCH (p:participant)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st)
        WHERE p.participant_id = $participant_id
        // Verify that the participant is connected to the requested study
        // Use DISTINCT to ensure we only get one row per participant-study combination
        WITH DISTINCT p, st
        // Verify st.study_id matches the requested namespace (defensive check)
        WHERE st.study_id = $namespace
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH p, d, st, 
             // Collect all survival records for this participant
             collect(DISTINCT s) AS survival_records
        WITH p, d, st,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, st, survs,
             // Check if any record has 'Dead' status
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Find max age among Dead records (if any exist)
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        // Now collect all study_ids for this participant (not just the filtered one)
        WITH toString(p.participant_id) AS participant_id, p, survs, final_vital_status, final_age_at_vital_status,
        collect(DISTINCT d) AS diagnosis_nodes
        // Get all studies for this participant
        OPTIONAL MATCH (p)-[:of_participant]->(c_all:consent_group)-[:of_consent_group]->(st_all:study)
        WITH participant_id, p, diagnosis_nodes, survs, final_vital_status, final_age_at_vital_status,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT st_all.study_id) AS study_ids
        WITH participant_id, p, diagnosis_nodes, survs, final_vital_status, final_age_at_vital_status,
             // Use requested namespace for namespace (for backward compatibility)
             $namespace AS namespace,
             // All study_ids for depositions
             study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          NULL AS associated_diagnoses,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids AS depositions
        LIMIT 1
        """
        else:
            # When namespace is not provided, use optional match for study
            cypher = f"""
        MATCH (p:participant)
        WHERE p.participant_id = $participant_id
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, d, c, st
        WITH p, d, c, st, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, c, st,
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
                      THEN 
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > dead_max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE dead_max_age
                        END
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL
                      THEN
                        CASE
                          WHEN toInteger(sr.age_at_last_known_survival_status) > max_age
                          THEN toInteger(sr.age_at_last_known_survival_status)
                          ELSE max_age
                        END
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, c, st, survs,
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
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN size(survs) = 0 THEN NULL
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        // Collect all study_ids for this participant
        WITH toString(p.participant_id) AS participant_id, p, collect(DISTINCT d) AS diagnosis_nodes, survs, final_vital_status, final_age_at_vital_status,
             // Collect all distinct study_ids for this participant
             collect(DISTINCT st.study_id) AS study_ids
        WITH participant_id, p, diagnosis_nodes, survs, final_vital_status, final_age_at_vital_status,
             // Use first study_id for namespace (for backward compatibility)
             head(study_ids) AS namespace,
             // All study_ids for depositions
             study_ids
        RETURN
          toString(participant_id) AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          CASE WHEN final_age_at_vital_status IS NOT NULL THEN final_age_at_vital_status ELSE -999 END AS age_at_vital_status,
          final_vital_status AS vital_status,
          NULL AS associated_diagnoses,
          diagnosis_nodes AS diagnosis_nodes,
          p.sex_at_birth AS sex,
          toString(namespace) AS namespace,
          study_ids AS depositions
        LIMIT 1
        """
        
        logger.info(
            "Executing get_subject_by_identifier Cypher query",
            participant_id=name,
            namespace=namespace,
            params=params
        )

        # Execute query with proper error handling, result consumption, and retry logic
        max_retries = 2
        retry_count = 0
        records = []
        
        while retry_count <= max_retries:
            try:
                result = await self.session.run(cypher, params)
                # Ensure the result is fully consumed - use async iteration for reliability
                # This ensures all records are fetched before proceeding
                records = []
                async for record in result:
                    records.append(dict(record))
                
                # Ensure result is fully consumed
                await result.consume()
                
                # If we got results or it's the last retry, break out of retry loop
                if records or retry_count >= max_retries:
                    break
                
                # If no results and not the last retry, wait a bit and retry
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff: 0.1s, 0.2s
                    retry_count += 1
                    logger.debug(f"Retrying get_subject_by_identifier query (attempt {retry_count + 1})")
            except Exception as e:
                if retry_count < max_retries:
                    await asyncio.sleep(0.1 * (retry_count + 1))
                    retry_count += 1
                    logger.warning(f"Error in get_subject_by_identifier query, retrying (attempt {retry_count + 1})", error=str(e))
                else:
                    logger.error(
                        "Error executing get_subject_by_identifier Cypher query after retries",
                        error=str(e),
                        error_type=type(e).__name__,
                        participant_id=name,
                        namespace=namespace,
                        cypher=cypher[:500] if cypher else None,
                        exc_info=True
                    )
                    raise
        
        logger.debug(
            "Query execution completed",
            participant_id=name,
            namespace=namespace,
            records_count=len(records) if records else 0
        )
        
        if not records:
            logger.debug("Subject not found", participant_id=name, namespace=namespace)
            return None
        
        # Convert to Subject object
        subject = self._record_to_subject(records[0], base_url=base_url)
        
        logger.debug("Found subject", participant_id=name, namespace=namespace, subject_data=getattr(subject, 'name', str(subject)[:50]))
        
        return subject

    def _validate_filters(self, filters: Dict[str, Any], entity_type: str) -> None:
        """
        Validate that all filter fields are allowed.
        
        Args:
            filters: Dictionary of filters to validate
            entity_type: Type of entity for allowlist checking
            
        Raises:
            UnsupportedFieldError: If any field is not allowed
        """
        for field in filters.keys():
            # Skip special fields
            if field.startswith("_"):
                continue
                
            if not self.allowlist.is_field_allowed(entity_type, field):
                # Log the invalid field but don't include it in the error message
                logger.warning(
                    "Unsupported field in filter",
                    field=field,
                    entity_type=entity_type
                )
                raise UnsupportedFieldError(field, entity_type)
    
    def _record_to_subject(self, record: Dict[str, Any], base_url: Optional[str] = None) -> Subject:
        """
        Convert a database record to a Subject object with nested CCDI-DCC format.
        
        Args:
            record: Database record dictionary
            base_url: Base URL for generating server URLs in identifiers (optional)
            
        Returns:
            Subject object with nested CCDI-DCC structure
        """
        # Extract values from record
        participant_id = record.get("name")
        study_id = record.get("namespace")
        study_ids = record.get("depositions")  # This is now an array of study_ids
        row_namespace = str(study_id) if study_id is not None and str(study_id).strip() else None
        row_depositions = [row_namespace] if row_namespace else []
        race_value = record.get("race")
        sex_value_raw = record.get("sex")
        vital_status_raw = record.get("vital_status")
        age_at_vital_status = record.get("age_at_vital_status")
        associated_diagnoses_raw = record.get("associated_diagnoses")
        survival_records_raw = record.get("survival_records")
        diagnosis_nodes_raw = record.get("diagnosis_nodes")

        def _get_prop(obj: Any, key: str) -> Any:
            """Best-effort property access for neo4j Node-like objects / dicts.
            
            Handles date/time objects (ZONED_DATE_TIME, etc.) by converting them
            to ISO format strings to prevent serialization errors.
            """
            from app.core.serialization import convert_date_time_to_string
            
            if obj is None:
                return None
            
            value = None
            try:
                if hasattr(obj, "get"):
                    value = obj.get(key)
            except Exception:
                pass
            
            if value is None:
                try:
                    if isinstance(obj, dict):
                        value = obj.get(key)
                except Exception:
                    pass
            
            if value is None:
                try:
                    value = obj[key]  # type: ignore[index]
                except Exception:
                    return None
            
            # Convert date/time objects to strings
            return convert_date_time_to_string(value)

        # If the Cypher returned raw survival records, compute derived survival fields in Python.
        # This avoids complex Cypher reductions that have caused Memgraph instability/crashes.
        if vital_status_raw is None and survival_records_raw:
            survs = []
            for sr in (survival_records_raw or []):
                status = _get_prop(sr, "last_known_survival_status")
                if status is None:
                    continue
                age_raw = _get_prop(sr, "age_at_last_known_survival_status")
                age_int = None
                if age_raw is not None:
                    try:
                        age_int = int(age_raw)
                    except Exception:
                        age_int = None
                survs.append({"status": str(status), "age": age_int})

            if survs:
                dead_ages = [s["age"] for s in survs if s["status"] == "Dead" and s["age"] is not None]
                any_dead = len(dead_ages) > 0
                max_age = max([s["age"] for s in survs if s["age"] is not None], default=None)

                if any_dead:
                    vital_status_raw = "Dead"
                    age_at_vital_status = max(dead_ages) if dead_ages else None
                else:
                    # pick the status from a record with max_age if possible
                    if max_age is not None:
                        status_at_max = next((s["status"] for s in survs if s["age"] == max_age), None)
                        vital_status_raw = status_at_max or survs[0]["status"]
                        age_at_vital_status = max_age
                    else:
                        vital_status_raw = survs[0]["status"]
                        age_at_vital_status = None

        extracted_diagnoses: list = []
        harmonized_categories: list[str] = []
        unharmonized_categories: list[str] = []
        if diagnosis_nodes_raw:
            for node in diagnosis_nodes_raw:
                if not associated_diagnoses_raw:
                    diag_val = _get_prop(node, "diagnosis")
                    if diag_val is not None:
                        if isinstance(diag_val, list):
                            extracted_diagnoses.extend([d for d in diag_val if d is not None and str(d).strip()])
                        elif str(diag_val).strip():
                            extracted_diagnoses.append(diag_val)
                cat = _get_prop(node, "diagnosis_category")
                if cat is not None and str(cat).strip():
                    for token in str(cat).split(";"):
                        token = token.strip()
                        if not token:
                            continue
                        if token in HARMONIZED_DIAGNOSIS_CATEGORIES:
                            harmonized_categories.append(token)
                        else:
                            unharmonized_categories.append(token)
        if extracted_diagnoses:
            associated_diagnoses_raw = extracted_diagnoses
        harmonized_categories = list(dict.fromkeys(harmonized_categories))
        unharmonized_categories = list(dict.fromkeys(unharmonized_categories))

        # Apply field mapping to vital_status (e.g., "Not Reported" -> "Not reported")
        vital_status = map_field_value("vital_status", vital_status_raw) if vital_status_raw else vital_status_raw
        
        associated_diagnoses = []
        if associated_diagnoses_raw:
            if isinstance(associated_diagnoses_raw, list):
                for item in associated_diagnoses_raw:
                    if isinstance(item, list):
                        associated_diagnoses.extend(item)
                    elif isinstance(item, str) and item.strip():
                        associated_diagnoses.append(item)
                    elif item is not None:
                        associated_diagnoses.append(str(item))
            elif isinstance(associated_diagnoses_raw, str):
                associated_diagnoses = [associated_diagnoses_raw] if associated_diagnoses_raw.strip() else []
        
        # Remove duplicates and empty values
        associated_diagnoses = list(dict.fromkeys([d for d in associated_diagnoses if d and str(d).strip()]))
        
        # Handle race - split by semicolon if present
        original_race_list = []
        if race_value:
            if isinstance(race_value, str):
                # Split by semicolon and clean up whitespace
                original_race_list = [race.strip() for race in race_value.split(';') if race.strip()]
            elif isinstance(race_value, list):
                # If it's already a list, process each item
                original_race_list = []
                for race_item in race_value:
                    if isinstance(race_item, str):
                        original_race_list.extend([r.strip() for r in race_item.split(';') if r.strip()])
                    else:
                        original_race_list.append(str(race_item))
            else:
                original_race_list = [str(race_value)]
        
        # Determine ethnicity based on original race values
        ethnicity_value = None
        if original_race_list:
            if any('Hispanic or Latino' in race for race in original_race_list):
                ethnicity_value = 'Hispanic or Latino'
            else:
                ethnicity_value = 'Not reported'

        # Remove 'Hispanic or Latino' from reported race values
        race_list = [r for r in original_race_list if r != 'Hispanic or Latino']
        
        # If race value was only "Hispanic or Latino", replace with "Not Reported"
        # If race value contains "Hispanic or Latino" and other values, keep current behavior (already removed)
        if not race_list and original_race_list:
            # Check if original only had "Hispanic or Latino" (exact match)
            if all(race.strip() == 'Hispanic or Latino' for race in original_race_list):
                race_list = ['Not Reported']
        
        # Apply race value mappings (e.g., "Not Allowed to Collect" -> "Not allowed to collect")
        race_list = [map_field_value("race", race) or race for race in race_list]
        
        # Normalize sex using config mappings
        sex_value = None
        if sex_value_raw is not None:
            sex_str = str(sex_value_raw).strip()
            
            # Use config mappings if available
            if self.settings and hasattr(self.settings, 'sex_value_mappings') and self.settings.sex_value_mappings:
                mappings = self.settings.sex_value_mappings
                # Try exact match first
                if sex_str in mappings:
                    sex_value = mappings[sex_str]
                # Try case-insensitive match
                else:
                    sex_lower = sex_str.lower()
                    for db_val, norm_val in mappings.items():
                        if db_val.lower() == sex_lower:
                            sex_value = norm_val
                            break
                    # If already normalized or not found, use default
                    if sex_value is None:
                        if sex_str in mappings.values():
                            sex_value = sex_str  # Already normalized
                        else:
                            # Fallback to 'U' if 'Not Reported' exists, otherwise first value
                            sex_value = mappings.get("Not Reported", "U") if "Not Reported" in mappings else (list(mappings.values())[0] if mappings else "U")
            else:
                # Fallback to simple logic if no config
                sex_lower = sex_str.lower()
                if sex_lower in {"female", "f"}:
                    sex_value = "F"
                elif sex_lower in {"male", "m"}:
                    sex_value = "M"
                else:
                    sex_value = "U"

        # Build nested subject structure
        # Row mode: namespace must come from this row's study_id
        primary_study_id = row_namespace
        
        subject_data = {
            "id": {
                "namespace": {
                    "organization": "CCDI-DCC",
                    "name": primary_study_id
                },
                "name": participant_id
            },
            "kind": "Participant",
            # Note: All "ancestors" fields are excluded from results but kept as optional placeholders in DTO models
            "metadata": {
                "sex": {"value": sex_value} if sex_value else None,
                # Race reporting rule: remove 'Hispanic or Latino'. If only that term was present,
                # replace with "Not Reported"; if race was entirely missing, keep it as None.
                "race": (
                    [{"value": race} for race in race_list]
                    if race_value is not None
                    else None
                ),
                "ethnicity": {"value": ethnicity_value} if ethnicity_value else None,
                "identifiers": (
                    [
                        {
                            "value": {
                                "namespace": {
                                    "organization": "CCDI-DCC",
                                    "name": row_namespace
                                },
                                "name": participant_id,
                                "type": "Linked",
                                "server": build_identifier_server_url(
                                    base_url=base_url or "",
                                    entity_type="subject",
                                    organization="CCDI-DCC",
                                    study_id=row_namespace,
                                    name=participant_id
                                ) if base_url else None
                            }
                        }
                    ]
                    if participant_id and row_namespace else None
                ),
                "associated_diagnoses": sorted(
                    [
                        {
                            "value": diag,
                            "comment": None
                        }
                        for diag in associated_diagnoses
                    ],
                    key=lambda x: x["value"]  # Sort by diagnosis value
                ) if associated_diagnoses else None,
                "associated_diagnosis_categories": (
                    [{"value": cat} for cat in harmonized_categories] if harmonized_categories else None
                ),
                "unharmonized": (
                    {"associated_diagnosis_categories": [{"value": cat} for cat in unharmonized_categories]}
                    if unharmonized_categories else None
                ),
                "vital_status": {"value": vital_status} if vital_status else None,
                # Convert -999 to null for age_at_vital_status in response (count endpoints keep -999 as-is)
                # Also convert 0 to null if vital_status exists (0 likely came from -999 when all ages were -999)
                "age_at_vital_status": (
                    {"value": int(age_at_vital_status)} 
                    if age_at_vital_status is not None 
                       and age_at_vital_status != -999 
                       and not (age_at_vital_status == 0 and vital_status is not None)
                    else None
                ),
                "depositions": (
                    [{"kind": "dbGaP", "value": row_namespace}]
                    if row_namespace else []
                )
            },
            "gateways": []
        }
        
        # Create a Subject object with the nested structure
        return Subject(**subject_data)
