"""
Subject repository for the CCDI Federation Service.

This module provides data access operations for subjects
using Cypher queries to Memgraph.
"""

from typing import List, Dict, Any, Optional, Tuple
from neo4j import AsyncSession

from app.core.logging import get_logger
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
        race_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                param_counter += 1
                race_param = f"param_{param_counter}"
                params[race_param] = race_value
                race_condition = f""",
                // normalize $race: STRING ["<trimmed>"], LIST trimmed list
                CASE
                  WHEN ${race_param} IS NULL THEN NULL
                  WHEN valueType(${race_param}) = 'LIST'   THEN [rt IN ${race_param} | trim(rt)]
                  WHEN valueType(${race_param}) = 'STRING' THEN [trim(${race_param})]
                  ELSE []
                END AS race_tokens,
                // tokenize stored comma-separated race string
                [pt IN SPLIT(p.race, ',') | trim(pt)] AS pr_tokens"""
                where_conditions.append("ANY(tok IN race_tokens WHERE tok IN pr_tokens)")
        
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
        
        # Add regular filters
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
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
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Compute max age across non-null records
             reduce(m = 0, sr IN survs |
                    CASE WHEN sr.age_at_last_known_survival_status IS NOT NULL AND sr.age_at_last_known_survival_status > m
                         THEN sr.age_at_last_known_survival_status ELSE m END) AS max_age
        WITH p, d, c, st,
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs WHERE sr.age_at_last_known_survival_status = max_age | sr.last_known_survival_status])
             END AS final_vital_status,
             CASE 
               WHEN has_dead THEN head([sr IN survs WHERE sr.last_known_survival_status = 'Dead' | sr.age_at_last_known_survival_status])
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
        ORDER BY id(p)
        SKIP $offset
        LIMIT $limit
        """.strip()
        
        logger.info(
            "Executing get_subjects Cypher query",
            cypher=cypher,
            params=params
        )
        
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
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
        org: str,
        ns: str,
        name: str
    ) -> Optional[Subject]:
        """
        Get a specific subject by organization, namespace, and name.
        
        Args:
            org: Organization identifier
            ns: Namespace identifier
            name: Subject name/identifier
            
        Returns:
            Subject object or None if not found
        """
        logger.debug(
            "Fetching subject by identifier",
            org=org,
            ns=ns,
            name=name
        )
        
        # Build the full identifier
        identifier = f"{org}.{ns}.{name}"
        params = {"identifier": identifier}
        
        # Build query to find subject by identifier with relationships
        cypher = """
        MATCH (p:participant)
        WHERE p.participant_id = $identifier
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, d, c, st, 
             // Collect all survival records for this participant
             collect(s) AS survival_records
        WITH p, d, c, st,
             // Keep only records with a status
             [sr IN survival_records WHERE sr.last_known_survival_status IS NOT NULL] AS survs
        WITH p, d, c, st, survs,
             any(sr IN survs WHERE sr.last_known_survival_status = 'Dead') AS has_dead,
             // Compute max age across non-null records
             reduce(m = 0, sr IN survs |
                    CASE WHEN sr.age_at_last_known_survival_status IS NOT NULL AND sr.age_at_last_known_survival_status > m
                         THEN sr.age_at_last_known_survival_status ELSE m END) AS max_age
        WITH p, d, c, st,
             CASE 
               WHEN has_dead THEN 'Dead'
               ELSE head([sr IN survs WHERE sr.age_at_last_known_survival_status = max_age | sr.last_known_survival_status])
             END AS final_vital_status,
             CASE 
               WHEN has_dead THEN head([sr IN survs WHERE sr.last_known_survival_status = 'Dead' | sr.age_at_last_known_survival_status])
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
            cypher=cypher,
            params=params
        )

        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        if not records:
            logger.debug("Subject not found", identifier=identifier)
            return None
        
        # Convert to Subject object
        subject = self._record_to_subject(records[0])
        
        logger.debug("Found subject", identifier=identifier, subject_data=getattr(subject, 'name', str(subject)[:50]))
        
        return subject
    
    async def count_subjects_by_field(
        self,
        field: str,
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Count subjects grouped by a specific field value.
        
        Args:
            field: Field to group by and count
            filters: Additional filters to apply
            
        Returns:
            List of dictionaries with value and count
            
        Raises:
            UnsupportedFieldError: If field is not allowed
        """
        logger.debug(
            "Counting subjects by field",
            field=field,
            filters=filters
        )
        
        # Build WHERE conditions and parameters
        where_conditions = [f"p.{field} IS NOT NULL"]
        params = {}
        param_counter = 0
        
        # Handle race parameter normalization
        race_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                param_counter += 1
                race_param = f"param_{param_counter}"
                params[race_param] = race_value
                race_condition = f""",
                // normalize $race: STRING ["<trimmed>"], LIST trimmed list
                CASE
                  WHEN ${race_param} IS NULL THEN NULL
                  WHEN valueType(${race_param}) = 'LIST'   THEN [rt IN ${race_param} | trim(rt)]
                  WHEN valueType(${race_param}) = 'STRING' THEN [trim(${race_param})]
                  ELSE []
                END AS race_tokens,
                // tokenize stored comma-separated race string
                [pt IN SPLIT(p.race, ',') | trim(pt)] AS pr_tokens"""
                where_conditions.append("ANY(tok IN race_tokens WHERE tok IN pr_tokens)")
        
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
        
        # Add regular filters
        for filter_field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{filter_field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{filter_field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {where_clause}
        WITH p, 
             CASE 
               WHEN p.{field} IS NULL THEN []
               WHEN NOT apoc.meta.type(p.{field}) = 'LIST' THEN [p.{field}]
               ELSE p.{field}
             END as field_values
        UNWIND field_values as value
        RETURN toString(value) as value, count(*) as count
        ORDER BY count DESC, value ASC
        """.strip()

        logger.info(
            "Executing count_subjects_by_field Cypher query",
            cypher=cypher,
            params=params
        )
        # Execute query
        result = await self.session.run(cypher, params)
        records = await result.data()
        
        # Format results
        counts = []
        for record in records:
            counts.append({
                "value": record.get("value"),
                "count": record.get("count", 0)
            })
        
        logger.debug(
            "Completed subject count by field",
            field=field,
            results_count=len(counts)
        )
        
        return counts
    
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
        race_condition = ""
        if "race" in filters:
            race_value = filters.pop("race")
            if race_value is not None:
                param_counter += 1
                race_param = f"param_{param_counter}"
                params[race_param] = race_value
                race_condition = f""",
                // normalize $race: STRING ["<trimmed>"], LIST trimmed list
                CASE
                  WHEN ${race_param} IS NULL THEN NULL
                  WHEN valueType(${race_param}) = 'LIST'   THEN [rt IN ${race_param} | trim(rt)]
                  WHEN valueType(${race_param}) = 'STRING' THEN [trim(${race_param})]
                  ELSE []
                END AS race_tokens,
                // tokenize stored comma-separated race string
                [pt IN SPLIT(p.race, ',') | trim(pt)] AS pr_tokens"""
                where_conditions.append("ANY(tok IN race_tokens WHERE tok IN pr_tokens)")
        
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
        
        # Add regular filters
        for field, value in filters.items():
            param_counter += 1
            param_name = f"param_{param_counter}"
            
            if isinstance(value, list):
                where_conditions.append(f"p.{field} IN ${param_name}")
            else:
                where_conditions.append(f"p.{field} = ${param_name}")
            params[param_name] = value
        
        # Build final query
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        cypher = f"""
        MATCH (p:participant)
        OPTIONAL MATCH (s:survival)-[:of_survival]->(p)
        OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
        OPTIONAL MATCH (p)-[:of_participant]->(c:consent_group)-[:of_consent_group]->(st:study)
        WITH p, s, d, c, st{race_condition}{identifiers_condition}
        {where_clause}
        RETURN count(p) as total_count
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
        
        # Normalize sex to F/M/U
        sex_value = None
        if sex_value_raw is not None:
            s = str(sex_value_raw).strip().lower()
            if s in {"female", "f"}:
                sex_value = "F"
            elif s in {"male", "m"}:
                sex_value = "M"
            else:
                sex_value = "U"

        # Namespace policy: keep study_id as "phsXXXX" and set organization to default 'CCDI-DCC'.
        # We still keep a simple prefix value for depositions.
        prefix = "phs"
        
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
                            "url": "https://portal.pedscommons.org/DD?view=table"
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
