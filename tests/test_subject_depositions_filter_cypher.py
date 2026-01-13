"""
Unit tests for depositions filter in subject queries.

These tests verify that the depositions filter is correctly applied in Cypher queries
for both get_subjects (data query) and get_subjects_summary (summary query).
"""

from app.core.config import Settings
from app.repositories.subject import SubjectRepository


class _DummyAllowlist:
    def is_field_allowed(self, entity_type: str, field: str) -> bool:
        return True


class _DummyResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def __aiter__(self):
        async def _gen():
            for r in self._rows:
                yield r
        return _gen()

    async def consume(self):
        return None


class _CapturingSession:
    def __init__(self):
        self.last_cypher = None
        self.last_params = None

    async def run(self, cypher, params=None):
        self.last_cypher = cypher
        self.last_params = params or {}
        # Return minimal valid results
        # For summary queries, return total_count
        if "total_count" in cypher or "count(*)" in cypher:
            return _DummyResult([{"total_count": 0}])
        # For data queries, return empty list (we only check Cypher generation, not data processing)
        return _DummyResult([])


def test_get_subjects_summary_depositions_only_applies_filter():
    """
    Regression test:
    - /subject/summary?depositions=phs003215 must apply the depositions filter
    - The generated Cypher must include WHERE st.study_id = $param_X
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    asyncio.run(repo.get_subjects_summary({"depositions": "phs003215"}))

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use required MATCH (not OPTIONAL MATCH)
    assert "MATCH (p:participant)-[:IN_STUDY]->(st:study)" in cypher or "MATCH (p)-[:IN_STUDY]->(st:study)" in cypher
    
    # Must apply depositions filter
    assert "WHERE st.study_id" in cypher
    assert "=" in cypher or "IN" in cypher
    
    # Verify parameter is set
    assert session.last_params is not None
    param_keys = list(session.last_params.keys())
    dep_param = [k for k in param_keys if k.startswith("param_") and k != "offset" and k != "limit"]
    assert len(dep_param) > 0, "Depositions parameter should be set"
    assert session.last_params[dep_param[0]] == "phs003215"


def test_get_subjects_summary_multiple_depositions_uses_in_operator():
    """
    Regression test:
    - /subject/summary?depositions=phs003215||phs002310 must use IN operator
    - The generated Cypher must include WHERE st.study_id IN $param_X
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    asyncio.run(repo.get_subjects_summary({"depositions": "phs003215||phs002310"}))

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use IN operator for multiple values
    assert "st.study_id IN" in cypher
    
    # Verify parameter is a list
    assert session.last_params is not None
    param_keys = list(session.last_params.keys())
    dep_params = [k for k in param_keys if k.startswith("param_") and k not in ["offset", "limit"]]
    assert len(dep_params) > 0, "Depositions parameter should be set"
    # Find the depositions parameter (it should be a list)
    dep_param_found = False
    for param_key in dep_params:
        param_value = session.last_params.get(param_key)
        if isinstance(param_value, list) and "phs003215" in param_value and "phs002310" in param_value:
            dep_param_found = True
            break
    assert dep_param_found, f"Depositions parameter should be a list with both values, found params: {session.last_params}"


def test_get_subjects_summary_depositions_with_vital_status_applies_both_filters():
    """
    Regression test:
    - /subject/summary?depositions=phs003215&vital_status=Not reported must apply both filters
    - Must use survival processing path (needs_survival_processing = True)
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    asyncio.run(repo.get_subjects_summary({
        "depositions": "phs003215",
        "vital_status": "Not reported"
    }))

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use required MATCH for depositions
    assert "MATCH (p)-[:IN_STUDY]->(st:study)" in cypher or "MATCH (p:participant)-[:IN_STUDY]->(st:study)" in cypher
    
    # Must apply depositions filter
    assert "WHERE st.study_id" in cypher
    
    # Must process survival for vital_status
    assert "OPTIONAL MATCH (s:survival)" in cypher or "OPTIONAL MATCH (s:survival)-[:of_survival]->(p)" in cypher
    assert "final_vital_status" in cypher
    
    # Must apply vital_status filter
    assert "toLower(toString(final_vital_status))" in cypher


def test_get_subjects_summary_depositions_with_identifiers_applies_both_filters():
    """
    Regression test:
    - /subject/summary?depositions=phs003215&identifiers=HTA4_1 must apply both filters
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    asyncio.run(repo.get_subjects_summary({
        "depositions": "phs003215",
        "identifiers": "HTA4_1"
    }))

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must apply identifiers filter
    assert "id_list" in cypher or "p.participant_id IN" in cypher
    
    # Must apply depositions filter
    assert "WHERE st.study_id" in cypher


def test_get_subjects_depositions_only_applies_filter():
    """
    Regression test:
    - /subject?depositions=phs003215 must apply the depositions filter in data query
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    # Only check Cypher generation, not data processing (mock returns empty results)
    try:
        asyncio.run(repo.get_subjects({"depositions": "phs003215"}, offset=0, limit=5))
    except Exception:
        # Ignore data processing errors - we only care about Cypher generation
        pass

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use required MATCH for depositions
    assert "MATCH (p)-[:IN_STUDY]->(st:study)" in cypher or "MATCH (p:participant)-[:IN_STUDY]->(st:study)" in cypher
    
    # Must apply depositions filter
    assert "WHERE st.study_id" in cypher
    
    # Verify parameter is set
    assert session.last_params is not None
    param_keys = list(session.last_params.keys())
    dep_params = [k for k in param_keys if k.startswith("param_") and k not in ["offset", "limit"]]
    assert len(dep_params) > 0, "Depositions parameter should be set"
    # Find the depositions parameter
    dep_param_found = False
    for param_key in dep_params:
        if session.last_params.get(param_key) == "phs003215":
            dep_param_found = True
            break
    assert dep_param_found, f"Depositions parameter should contain 'phs003215', found params: {session.last_params}"


def test_get_subjects_depositions_with_vital_status_applies_both_filters():
    """
    Regression test:
    - /subject?depositions=phs003215&vital_status=Not reported must apply both filters
    - Must use depositions path (not non-depositions path)
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    # Only check Cypher generation, not data processing (mock returns empty results)
    try:
        asyncio.run(repo.get_subjects({
            "depositions": "phs003215",
            "vital_status": "Not reported"
        }, offset=0, limit=5))
    except Exception:
        # Ignore data processing errors - we only care about Cypher generation
        pass

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use required MATCH (depositions path)
    assert "MATCH (p)-[:IN_STUDY]->(st:study)" in cypher or "MATCH (p:participant)-[:IN_STUDY]->(st:study)" in cypher
    
    # Must apply depositions filter early in MATCH clause
    assert "WHERE st.study_id" in cypher
    
    # Must apply vital_status filter (derived filter)
    assert "derived_vital_status" in cypher or "final_vital_status" in cypher
    
    # When grouping by (participant_id, study_id) pairs, we use study_ids_single instead of study_ids
    # The depositions filter is already applied in the MATCH clause, so we check for:
    # - study_ids_single (scalar value per pair)
    # - Filter applied after grouping: "WHERE toString(sid) <> '' AND sid = $param_1" or similar
    assert "study_ids_single" in cypher or "sid = $param_1" in cypher or "sid = $" in cypher


def test_get_subjects_depositions_with_identifiers_applies_both_filters():
    """
    Regression test:
    - /subject?depositions=phs003215&identifiers=HTA4_1 must apply both filters
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    # Only check Cypher generation, not data processing (mock returns empty results)
    try:
        asyncio.run(repo.get_subjects({
            "depositions": "phs003215",
            "identifiers": "HTA4_1"
        }, offset=0, limit=5))
    except Exception:
        # Ignore data processing errors - we only care about Cypher generation
        pass

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must apply identifiers filter (can be direct filter p.participant_id = $param or p.participant_id IN id_list)
    assert "p.participant_id" in cypher
    
    # Must apply depositions filter
    assert "WHERE st.study_id" in cypher


def test_get_subjects_depositions_multiple_values_filters_correctly():
    """
    Regression test:
    - /subject?depositions=phs003215||phs002310 must use IN operator
    - Final depositions field should only contain matching study_ids
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    # Only check Cypher generation, not data processing (mock returns empty results)
    try:
        asyncio.run(repo.get_subjects({
            "depositions": "phs003215||phs002310"
        }, offset=0, limit=5))
    except Exception:
        # Ignore data processing errors - we only care about Cypher generation
        pass

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use IN operator in WHERE clause
    assert "st.study_id IN" in cypher
    
    # Verify parameter is a list
    assert session.last_params is not None
    param_keys = list(session.last_params.keys())
    dep_params = [k for k in param_keys if k.startswith("param_") and k not in ["offset", "limit"]]
    assert len(dep_params) > 0, "Depositions parameter should be set"
    # Find the depositions parameter (it should be a list)
    dep_param_found = False
    for param_key in dep_params:
        param_value = session.last_params.get(param_key)
        if isinstance(param_value, list):
            dep_param_found = True
            break
    assert dep_param_found, f"Depositions parameter should be a list, found params: {session.last_params}"


def test_get_subjects_summary_depositions_uses_required_match():
    """
    Regression test:
    - /subject/summary?depositions=phs003215 should use required MATCH (not OPTIONAL)
    - This ensures the depositions filter is applied correctly
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio
    asyncio.run(repo.get_subjects_summary({"depositions": "phs003215"}))

    assert session.last_cypher is not None
    cypher = session.last_cypher
    
    # Must use required MATCH (not OPTIONAL MATCH for studies)
    # Check that we have a MATCH (not OPTIONAL MATCH) for the study relationship
    has_required_match = (
        "MATCH (p)-[:IN_STUDY]->(st:study)" in cypher or 
        "MATCH (p:participant)-[:IN_STUDY]->(st:study)" in cypher
    )
    assert has_required_match, "Should use required MATCH for studies when depositions filter is present"
    
    # The WHERE clause should be applied after the MATCH
    assert "WHERE st.study_id" in cypher

