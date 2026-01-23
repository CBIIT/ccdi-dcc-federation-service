from app.core.config import Settings
from app.repositories.subject import SubjectRepository


class _DummyAllowlist:
    def is_field_allowed(self, entity_type: str, field: str) -> bool:  # pragma: no cover
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
        # Return a minimal valid summary row
        return _DummyResult([{"total_count": 0}])


def test_get_subjects_summary_includes_race_filter_condition_in_cypher():
    """
    Regression test:
    - /subject/summary?race=White must NOT fall into the "no filters" fast path.
    - The generated Cypher must include race tokenization + a WHERE clause applying the token match.
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    # Use a valid single race value (deps layer normally validates this)
    filters = {"race": "White"}

    # Run async method in a sync test by importing asyncio locally (keeps test deps minimal)
    import asyncio

    asyncio.run(repo.get_subjects_summary(filters))

    assert session.last_cypher is not None
    cypher = session.last_cypher

    # Evidence of tokenization of DB semicolon values
    assert "race_tokens" in cypher
    assert "pr_tokens" in cypher
    assert "SPLIT(COALESCE(p.race, ''), ';')" in cypher

    # Evidence of actually applying the filter (this was the missing piece)
    assert "WHERE" in cypher
    assert "tok IN pr_tokens" in cypher


