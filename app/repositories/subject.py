"""
Subject repository for the CCDI Federation Service.

This module provides data access operations for subjects
using Cypher queries to Memgraph.
"""

from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
from app.core.constants import Race
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import Subject
from app.models.errors import UnsupportedFieldError

logger = get_logger(__name__)


class SubjectRepository:
    """Repository for subject data operations."""
    
    def __init__(self, session: AsyncSession, allowlist: FieldAllowlist, settings=None):
        """Initialize repository with database session and field allowlist."""
        self.session = session
        self.allowlist = allowlist
        self.settings = settings
        
    async def get_subjects(
        self,
        filters: Dict[str, Any],
        offset: int = 0,
        limit: int = 20
    ) -> List[Subject]:
        """
        Get paginated list of subjects with filtering.
        
        Args:
            filters: Dictionary of field filters
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of Subject objects
            
        Raises:
            UnsupportedFieldError: If filter field is not allowed
        """
        logger.debug(
            "Fetching subjects",
            filters=filters,
            offset=offset,
            limit=limit
        )
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {"offset": offset, "limit": limit}
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
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    race_filter_condition = "ANY(tok IN race_tokens WHERE tok IN pr_tokens)"
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None:
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
        
        # Depositions processing disabled - using preset value only
        
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
        
        # Separate derived fields (calculated after WITH clauses) from direct participant fields
        derived_filters = {}
        derived_conditions = []
        
        # Map API sex values to database values (M -> Male, F -> Female, U -> Not Reported)
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            # If the value is a normalized API value, map it to database value
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]
        
        # Field name mapping for participant properties
        # Some API field names don't match the database property names
        field_name_mapping = {
            "sex": "sex_at_birth",
            # Add other mappings as needed
        }
        
        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status", "ethnicity"}:
                derived_filters[field] = value
                continue
            
            # Map API field name to database property name
            db_field = field_name_mapping.get(field, field)
                
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause for direct participant fields
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Add race filter to WHERE clause if it exists (after WITH defines race_tokens/pr_tokens)
        if race_filter_condition:
            if where_clause:
                where_clause = where_clause + " AND " + race_filter_condition
            else:
                where_clause = "WHERE " + race_filter_condition
        
        # Build WHERE clause for derived fields (after calculation)
        if derived_filters:
            for field, value in derived_filters.items():
                param_counter += 1
                param_name = f"derived_{field}_{param_counter}"
                if field == "vital_status":
                    derived_conditions.append(f"final_vital_status = ${param_name}")
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                elif field == "ethnicity":
                    # Ethnicity filter will be applied in the WITH clause after calculation
                    ethnicity_param = param_name
                    derived_conditions.append(f"ethnicity_value = ${ethnicity_param}")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            derived_where_clause = "WHERE " + " AND ".join(derived_conditions)
        
        cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {where_clause}
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
                           AND sr.age_at_last_known_survival_status > dead_max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND sr.age_at_last_known_survival_status > max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, c, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs 
                           WHERE sr.age_at_last_known_survival_status = max_age 
                           | sr.last_known_survival_status])
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status,
             // Calculate ethnicity for filtering
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        RETURN
          p.participant_id AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          final_age_at_vital_status AS age_at_vital_status,
          final_vital_status AS vital_status,
          d.diagnosis AS associated_diagnoses,
          p.sex_at_birth AS sex,
          st.study_id AS namespace,
          st.study_id AS depositions
        ORDER BY id(p)
        SKIP $offset
        LIMIT $limit
        """.strip()
        
        logger.info(
            "Executing get_subjects Cypher query",
            cypher=cypher,
            params=params,
            filters=filters
        )
        
        # Execute query
        try:
            result = await self.session.run(cypher, params)
            records = await result.data()
        except Exception as e:
            logger.error(
                "Error executing get_subjects Cypher query",
                error=str(e),
                error_type=type(e).__name__,
                cypher=cypher[:500] if cypher else None,
                params_keys=list(params.keys()) if params else []
            )
            raise
        
        # Convert to Subject objects
        subjects = []
        for record in records:
            subjects.append(self._record_to_subject(record))
        
        logger.debug(
            "Found subjects",
            count=len(subjects),
            filters=filters
        )
        
        return subjects
    
    async def get_subject_by_identifier(
        self,
        organization: str,
        namespace: Optional[str],
        name: str
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
                           AND sr.age_at_last_known_survival_status > dead_max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND sr.age_at_last_known_survival_status > max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs 
                           WHERE sr.age_at_last_known_survival_status = max_age 
                           | sr.last_known_survival_status])
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        RETURN
          p.participant_id AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          final_age_at_vital_status AS age_at_vital_status,
          final_vital_status AS vital_status,
          d.diagnosis AS associated_diagnoses,
          p.sex_at_birth AS sex,
          $namespace AS namespace,
          $namespace AS depositions
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
                           AND sr.age_at_last_known_survival_status > dead_max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             // Find max age across all non-null records
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND sr.age_at_last_known_survival_status > max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE max_age 
                    END) AS max_age
        WITH p, d, c, st,
             // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs 
                           WHERE sr.age_at_last_known_survival_status = max_age 
                           | sr.last_known_survival_status])
             END AS final_vital_status,
             // Age: If 'Dead' exists, use max Dead age; otherwise use max age
             CASE 
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status
        RETURN
          p.participant_id AS name,
          p.race AS race,
          CASE 
            WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
            ELSE 'Not reported'
          END AS ethnicity,
          final_age_at_vital_status AS age_at_vital_status,
          final_vital_status AS vital_status,
          d.diagnosis AS associated_diagnoses,
          p.sex_at_birth AS sex,
          st.study_id AS namespace,
          st.study_id AS depositions
        LIMIT 1
        """
        
        logger.info(
            "Executing get_subject_by_identifier Cypher query",
            participant_id=name,
            namespace=namespace,
            params=params
        )

        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        if not records:
            logger.debug("Subject not found", participant_id=name, namespace=namespace)
            return None
        
        # Convert to Subject object
        subject = self._record_to_subject(records[0])
        
        logger.debug("Found subject", participant_id=name, namespace=namespace, subject_data=getattr(subject, 'name', str(subject)[:50]))
        
        return subject
    
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
            "associated_diagnoses": "d.diagnosis"  # From diagnosis nodes
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
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of count results)
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting subjects by field",
            field=field,
            filters=filters
        )
        
        # Validate field is allowed for count operations (case-sensitive)
        allowed_fields = set(self.settings.subject_count_fields if self.settings else ["sex", "race", "ethnicity", "vital_status", "age_at_vital_status", "associated_diagnoses"])
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
        
        # Build WHERE conditions and parameters (for filtering, not for field null check yet)
        base_where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle race parameter normalization
        race_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                # Normalize race value to a list in Python (handle both string and list inputs)
                if isinstance(race_value, str):
                    race_list = [race_value.strip()] if race_value.strip() else []
                elif isinstance(race_value, list):
                    race_list = [str(r).strip() for r in race_value if r and str(r).strip()]
                else:
                    race_list = []
                
                if race_list:
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    race_condition = f""",
                    // race tokens (already normalized to list in Python)
                    ${race_param} AS race_tokens,
                    // tokenize stored semicolon-separated race string
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    base_where_conditions.append("ANY(tok IN race_tokens WHERE tok IN pr_tokens)")
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None:
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
                base_where_conditions.append("p.participant_id IN id_list")
        
        # Depositions processing disabled - using preset value only
        
        # Handle diagnosis search
        if "_diagnosis_search" in filters:
            search_term = filters.pop("_diagnosis_search")
            base_where_conditions.append("""(
                ANY(diag IN d.diagnosis WHERE toLower(toString(diag)) CONTAINS toLower($diagnosis_search_term))
                OR ANY(key IN keys(p.metadata.unharmonized) 
                       WHERE toLower(key) CONTAINS 'diagnos' 
                       AND toLower(toString(p.metadata.unharmonized[key])) CONTAINS toLower($diagnosis_search_term))
            )""")
            params["diagnosis_search_term"] = search_term
        
        # Add regular filters
        for filter_field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                base_where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                base_where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build base WHERE clause (for filtering participants)
        base_where_clause = "WHERE " + " AND ".join(base_where_conditions) if base_where_conditions else ""
        
        # Check if field requires survival record processing
        is_survival_field = field in {"vital_status", "age_at_vital_status"}
        
        # Build survival processing logic if needed
        survival_processing = ""
        field_access = self._get_field_path(field)
        
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
                   AND sr.age_at_last_known_survival_status > dead_max_age
              THEN sr.age_at_last_known_survival_status 
              ELSE dead_max_age 
            END) AS max_dead_age,
     // Find max age across all non-null records
     reduce(max_age = 0, sr IN survs |
            CASE 
              WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                   AND sr.age_at_last_known_survival_status > max_age
              THEN sr.age_at_last_known_survival_status 
              ELSE max_age 
            END) AS max_age
WITH p, d, c, st,
     // Priority: If 'Dead' exists, use 'Dead'; otherwise use status with max age
     // If survs is empty or no matching record, return NULL
     CASE 
       WHEN size(survs) = 0 THEN NULL
       WHEN has_dead THEN 'Dead'
       ELSE head([sr IN survs 
                   WHERE sr.age_at_last_known_survival_status = max_age 
                   | sr.last_known_survival_status])
     END AS final_vital_status,
     // Age: If 'Dead' exists, use max Dead age; otherwise use max age
     CASE 
       WHEN has_dead THEN max_dead_age
       ELSE max_age
     END AS final_age_at_vital_status"""
        else:
            survival_processing = ""
        
        # Query 1: Get total count of all unique participants matching filters
        # Total count should match summary endpoint - use same logic (no survival processing for total)
        if is_survival_field:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        else:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null field value
        null_check = f"{field_access} IS NULL" if not is_survival_field else f"final_{field} IS NULL"
        if is_survival_field:
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WITH p.participant_id as participant_id, final_vital_status, final_age_at_vital_status
        WITH participant_id, head(collect(final_vital_status)) as final_vital_status
        WHERE final_vital_status IS NULL
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
        else:
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE {null_check}
        RETURN count(DISTINCT p.participant_id) as missing
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
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}{survival_processing}
        WITH p.participant_id as participant_id, {field_value_expr} as field_val
        WITH participant_id, head(collect(field_val)) as field_val
        WHERE field_val IS NOT NULL
        WITH participant_id, 
             CASE 
               WHEN field_val IS NULL THEN []
               ELSE [field_val]
             END as field_values
        UNWIND field_values as value
        WITH value, participant_id,
             toString(value) as normalized_value
        RETURN normalized_value as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {base_where_clause}
        WHERE {not_null_check}
        WITH p, 
             CASE 
               WHEN {field_value_expr} IS NULL THEN []
               ELSE 
                 // Wrap in list for UNWIND - works for both strings and lists
                 [{field_value_expr}]
             END as field_values
        UNWIND field_values as value
        WITH value, p.participant_id as participant_id,
             CASE 
               WHEN '{field}' = 'sex' THEN{normalization_expr}
               ELSE toString(value)
             END as normalized_value
        RETURN normalized_value as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_field Cypher queries",
            field=field,
            params_count=len(params)
        )
        
        # Execute all three queries
        total_result = await self.session.run(total_cypher, params)
        total_records = await total_result.data()
        total_count = total_records[0].get("total", 0) if total_records else 0
        
        missing_result = await self.session.run(missing_cypher, params)
        missing_records = await missing_result.data()
        missing_count = missing_records[0].get("missing", 0) if missing_records else 0
        
        values_result = await self.session.run(values_cypher, params)
        values_records = await values_result.data()
        
        # Format values results
        counts = []
        for record in values_records:
            counts.append({
                "value": record.get("value"),
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
        
        Args:
            filters: Additional filters to apply
            
        Returns:
            Dictionary with total, missing, and values (list of race counts)
        """
        logger.debug("Counting subjects by race with enum validation", filters=filters)
        
        # Get all valid race enum values
        valid_races = Race.values()
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
        param_counter = 0
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None:
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
        
        # Add regular filters (excluding race since we're counting by race)
        for filter_field, value in filters.items():
            if filter_field == "race":
                continue  # Skip race filter when counting by race
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique participants matching filters
        total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null race value
        missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WHERE p.race IS NULL
        RETURN count(DISTINCT p.participant_id) as missing
        """.strip()
        
        # Query 3: Create a single query that counts distinct participants for each valid race
        # Special handling: if race is only "Hispanic or Latino", count as "Not Reported"
        # Split race by semicolon, remove "Hispanic or Latino", then match against valid races
        params["valid_races"] = valid_races
        
        values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id as participant_id, p.race as race
        WHERE race IS NOT NULL
        WITH participant_id, race,
             // Split race by semicolon and trim each part
             [r IN SPLIT(race, ';') | trim(r)] as race_parts
        WITH participant_id, race, race_parts,
             // Filter out "Hispanic or Latino" - it's not a valid race value
             [r IN race_parts WHERE r <> 'Hispanic or Latino'] as race_list_filtered
        WITH participant_id, race, race_list_filtered,
             CASE 
               WHEN size(race_list_filtered) = 0 THEN ['Not Reported']
               ELSE [r IN race_list_filtered WHERE r IN $valid_races]
             END as matching_races
        UNWIND matching_races as race_value
        RETURN race_value as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_race Cypher queries",
            race_count=len(valid_races),
            params_count=len(params)
        )
        
        # Execute all three queries
        total_result = await self.session.run(total_cypher, params)
        total_records = await total_result.data()
        total_count = total_records[0].get("total", 0) if total_records else 0
        
        missing_result = await self.session.run(missing_cypher, params)
        missing_records = await missing_result.data()
        missing_count = missing_records[0].get("missing", 0) if missing_records else 0
        
        values_result = await self.session.run(values_cypher, params)
        values_records = await values_result.data()
        
        # Format results - ensure all valid races are included (even with 0 count)
        counts_by_value = {record.get("value"): record.get("count", 0) for record in values_records}
        
        # Build final counts list with all valid races
        counts = []
        for race_value in valid_races:
            counts.append({
                "value": race_value,
                "count": counts_by_value.get(race_value, 0)
            })
        
        # Sort by count descending (numeric), then by value ascending
        counts.sort(key=lambda x: (-x["count"], x["value"]))
        
        logger.info(
            "Completed subject count by race",
            total=total_count,
            missing=missing_count,
            values_count=len(counts)
        )
        
        return {
            "total": total_count,
            "missing": missing_count,
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
            if identifiers_value is not None:
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
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique participants matching filters
        total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with null race (missing ethnicity)
        missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WHERE p.race IS NULL
        RETURN count(DISTINCT p.participant_id) as missing
        """.strip()
        
        # Query 3: Count by ethnicity (derived from race)
        values_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id as participant_id, p.race as race
        WHERE race IS NOT NULL
        WITH participant_id, race,
             CASE 
               WHEN race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END as ethnicity_value
        RETURN ethnicity_value as value, count(DISTINCT participant_id) as count
        ORDER BY value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_ethnicity Cypher queries",
            params_count=len(params)
        )
        
        # Execute all three queries
        total_result = await self.session.run(total_cypher, params)
        total_records = await total_result.data()
        total_count = total_records[0].get("total", 0) if total_records else 0
        
        missing_result = await self.session.run(missing_cypher, params)
        missing_records = await missing_result.data()
        missing_count = missing_records[0].get("missing", 0) if missing_records else 0
        
        values_result = await self.session.run(values_cypher, params)
        values_records = await values_result.data()
        
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
            if identifiers_value is not None:
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
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query 1: Get total count of all unique participants matching filters
        # Only include OPTIONAL MATCHes if we have filters that need them
        if where_clause or identifiers_condition:
            total_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        else:
            # No filters - simple count
            total_cypher = """
        MATCH (p:participant)
        RETURN count(DISTINCT p.participant_id) as total
        """.strip()
        
        # Query 2: Get count of participants with no diagnoses (missing)
        # Optimized to avoid unnecessary OPTIONAL MATCHes
        if where_clause or identifiers_condition:
            missing_cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id as participant_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
        else:
            # No filters - simple check for missing diagnoses
            missing_cypher = """
        MATCH (p:participant)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        WITH DISTINCT p.participant_id as participant_id, collect(d) as diagnoses
        WHERE size([d IN diagnoses WHERE d IS NOT NULL]) = 0
        RETURN count(DISTINCT participant_id) as missing
        """.strip()
        
        # Query 3: Count by diagnosis values
        # d.diagnosis is a STRING (not a list) - each diagnosis node has one diagnosis value
        # Multiple diagnosis nodes can link to one participant, so each contributes one value
        # Relationship direction: (d:diagnosis)-[:of_diagnosis]->(p:participant)
        # Optimized: Start from diagnosis nodes, apply filters only if needed
        # Use exact query that worked in Memgraph when no filters
        if where_clause or (identifiers_condition and identifiers_condition.strip()):
            # Has filters - need to apply them but keep it efficient
            values_cypher = f"""
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        WITH DISTINCT p.participant_id as participant_id, d.diagnosis as diagnosis_value
        MATCH (p2:participant {{participant_id: participant_id}})
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p2)
        OPTIONAL MATCH (p2)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p2, s, c, st, participant_id, diagnosis_value{identifiers_condition}
        {where_clause}
        WHERE toString(diagnosis_value) <> ''
        RETURN toString(diagnosis_value) as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        else:
            # No filters - use exact query that worked in Memgraph
            values_cypher = """
        MATCH (d:diagnosis)-[:of_diagnosis]->(p:participant)
        WHERE d.diagnosis IS NOT NULL
        WITH DISTINCT p.participant_id as participant_id, d.diagnosis as diagnosis_value
        RETURN toString(diagnosis_value) as value, count(DISTINCT participant_id) as count
        ORDER BY count DESC, value ASC
        """.strip()
        
        logger.info(
            "Executing count_subjects_by_associated_diagnoses Cypher queries",
            values_query=values_cypher,
            params_count=len(params),
            has_identifiers_condition=bool(identifiers_condition),
            has_where_clause=bool(where_clause)
        )
        
        # Execute all three queries
        total_result = await self.session.run(total_cypher, params)
        total_records = await total_result.data()
        total_count = total_records[0].get("total", 0) if total_records else 0
        
        missing_result = await self.session.run(missing_cypher, params)
        missing_records = await missing_result.data()
        missing_count = missing_records[0].get("missing", 0) if missing_records else 0
        
        values_result = await self.session.run(values_cypher, params)
        values_records = await values_result.data()
        
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
    
    async def get_subjects_summary(
        self,
        filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for subjects.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Dictionary with summary statistics
        """
        logger.debug("Getting subjects summary", filters=filters)
        
        # Build WHERE conditions and parameters
        where_conditions = []
        params = {}
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
                    param_counter += 1
                    race_param = f"param_{param_counter}"
                    params[race_param] = race_list
                    race_condition = f""",
                    ${race_param} AS race_tokens,
                    [pt IN SPLIT(COALESCE(p.race, ''), ';') | trim(pt)] AS pr_tokens"""
                    race_filter_condition = "ANY(tok IN race_tokens WHERE tok IN pr_tokens)"
        
        # Handle identifiers parameter normalization
        identifiers_condition = ""
        if "identifiers" in filters:
            identifiers_value = filters.pop("identifiers")
            if identifiers_value is not None:
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
        
        # Depositions processing disabled - using preset value only
        
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
        
        # Map API sex values to database values (M -> Male, F -> Female, U -> Not Reported)
        if "sex" in filters and filters["sex"]:
            sex_value = filters["sex"]
            sex_mapping = {
                "M": "Male",
                "F": "Female",
                "U": "Not Reported"
            }
            # If the value is a normalized API value, map it to database value
            if sex_value in sex_mapping:
                filters["sex"] = sex_mapping[sex_value]
        
        # Field name mapping for participant properties
        # Some API field names don't match the database property names
        field_name_mapping = {
            "sex": "sex_at_birth",
            # Add other mappings as needed
        }
        
        # Separate derived fields (calculated after WITH clauses) from direct participant fields
        derived_filters = {}
        derived_conditions = []
        
        # Add regular filters (excluding derived fields)
        for field, value in filters.items():
            # Skip derived fields - they will be handled after calculation
            if field in {"vital_status", "age_at_vital_status", "ethnicity"}:
                derived_filters[field] = value
                continue
            
            # Map API field name to database property name
            db_field = field_name_mapping.get(field, field)
                
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{db_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{db_field} = ${param_name}")
            params[param_name] = value
        
        # Build WHERE clause for direct participant fields
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Add race filter to WHERE clause if it exists (after WITH defines race_tokens/pr_tokens)
        if race_filter_condition:
            if where_clause:
                where_clause = where_clause + " AND " + race_filter_condition
            else:
                where_clause = "WHERE " + race_filter_condition
        
        # Build WHERE clause for derived fields (after calculation)
        if derived_filters:
            for field, value in derived_filters.items():
                param_counter += 1
                param_name = f"derived_{field}_{param_counter}"
                if field == "vital_status":
                    derived_conditions.append(f"final_vital_status = ${param_name}")
                elif field == "age_at_vital_status":
                    derived_conditions.append(f"final_age_at_vital_status = ${param_name}")
                elif field == "ethnicity":
                    # Ethnicity filter will be applied in the WITH clause after calculation
                    ethnicity_param = param_name
                    derived_conditions.append(f"ethnicity_value = ${ethnicity_param}")
                params[param_name] = value
        
        derived_where_clause = ""
        if derived_conditions:
            derived_where_clause = "WHERE " + " AND ".join(derived_conditions)
        
        # Build query - if we have derived filters, we need to calculate them first
        if derived_filters and ("vital_status" in derived_filters or "age_at_vital_status" in derived_filters):
            # Calculate survival records for vital_status and age_at_vital_status filters
            cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH DISTINCT p.participant_id AS participant_id, p, s, d, c, st{race_condition}{identifiers_condition}
        {where_clause}
        WITH participant_id, p, d, c, st,
             collect(s) AS survival_records
        WITH participant_id, p, d, c, st,
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH participant_id, p, d, c, st, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             reduce(dead_max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.last_known_survival_status = 'Dead' 
                           AND sr.age_at_last_known_survival_status IS NOT NULL 
                           AND sr.age_at_last_known_survival_status > dead_max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE dead_max_age 
                    END) AS max_dead_age,
             reduce(max_age = 0, sr IN survs |
                    CASE 
                      WHEN sr.age_at_last_known_survival_status IS NOT NULL 
                           AND sr.age_at_last_known_survival_status > max_age
                      THEN sr.age_at_last_known_survival_status 
                      ELSE max_age 
                    END) AS max_age
        WITH participant_id, p, d, c, st,
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs 
                           WHERE sr.age_at_last_known_survival_status = max_age 
                           | sr.last_known_survival_status])
             END AS final_vital_status,
             CASE 
               WHEN has_dead THEN max_dead_age
               ELSE max_age
             END AS final_age_at_vital_status,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        RETURN count(participant_id) as total_count
        """.strip()
        else:
            # Simple query without survival processing
            # Deduplicate by participant_id only to avoid duplicates from OPTIONAL MATCH relationships
            if where_clause or race_condition or identifiers_condition or derived_where_clause:
                # If we have ethnicity filter, we need to calculate it first
                if "ethnicity" in derived_filters:
                    cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p{race_condition}{identifiers_condition}
        {where_clause}
        WITH p,
             CASE 
               WHEN p.race CONTAINS 'Hispanic or Latino' THEN 'Hispanic or Latino'
               ELSE 'Not reported'
             END AS ethnicity_value
        {derived_where_clause}
        WITH DISTINCT p.participant_id AS participant_id
        RETURN count(participant_id) as total_count
        """.strip()
                else:
                    cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p{race_condition}{identifiers_condition}
        {where_clause}
        WITH DISTINCT p.participant_id AS participant_id
        RETURN count(participant_id) as total_count
        """.strip()
            else:
                cypher = """
        MATCH (p:participant)
        RETURN count(DISTINCT p.participant_id) as total_count
        """.strip()
        
        logger.info(
            "Executing get_subjects_summary Cypher query",
            cypher=cypher,
            params=params
        )
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        if not records:
            return {"total_count": 0}
        
        summary = records[0]
        logger.debug("Completed subjects summary", total_count=summary.get("total_count", 0))
        
        return summary
    
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
                raise UnsupportedFieldError(f"Field '{field}' is not supported for {entity_type} filtering")
    
    def _record_to_subject(self, record: Dict[str, Any]) -> Subject:
        """
        Convert a database record to a Subject object with nested CCDI-DCC format.
        
        Args:
            record: Database record dictionary
            
        Returns:
            Subject object with nested CCDI-DCC structure
        """
        # Extract values from record
        participant_id = record.get("name")
        study_id = record.get("namespace")
        race_value = record.get("race")
        sex_value_raw = record.get("sex")
        vital_status = record.get("vital_status")
        age_at_vital_status = record.get("age_at_vital_status")
        associated_diagnoses_raw = record.get("associated_diagnoses")
        
        # Handle associated_diagnoses - it might be a string or list
        if associated_diagnoses_raw:
            if isinstance(associated_diagnoses_raw, list):
                associated_diagnoses = associated_diagnoses_raw
            elif isinstance(associated_diagnoses_raw, str):
                # If it's a string, treat it as a single diagnosis
                associated_diagnoses = [associated_diagnoses_raw]
            else:
                associated_diagnoses = []
        else:
            associated_diagnoses = []
        
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
        subject_data = {
            "id": {
                "namespace": {
                    "organization": "CCDI-DCC",
                    "name": study_id
                },
                "name": participant_id
            },
            "kind": "Participant",
            # Note: All "ancestors" fields are excluded from results but kept as optional placeholders in DTO models
            "metadata": {
                "sex": {"value": sex_value} if sex_value else None,
                # Race reporting rule: remove 'Hispanic or Latino'. If only that term was present,
                # return an empty list []; if race was entirely missing, keep it as None.
                "race": (
                    [{"value": race} for race in race_list]
                    if race_value is not None
                    else None
                ),
                "ethnicity": {"value": ethnicity_value} if ethnicity_value else None,
                "identifiers": [
                    {
                        "value": {
                            "namespace": {
                                "organization": "CCDI-DCC",
                                "name": study_id
                            },
                            "name": participant_id
                        }
                    }
                ] if participant_id and study_id else None,
                "associated_diagnoses": [
                    {
                        "value": diag,
                        "owned": True,
                        "comment": None,
                        "details": {
                            "method": None,
                            "harmonizer": None,
                            "url": "https://portal.pedscommons.organization/DD?view=table"
                        }
                    }
                    for diag in associated_diagnoses
                ] if associated_diagnoses else None,
                "unharmonized": None,
                "vital_status": {"value": vital_status} if vital_status else None,
                "age_at_vital_status": {"value": int(age_at_vital_status)} if age_at_vital_status is not None else None,
                "depositions": ["db_gap"]
            },
            "gateways": []
        }
        
        # Create a Subject object with the nested structure
        return Subject(**subject_data)
