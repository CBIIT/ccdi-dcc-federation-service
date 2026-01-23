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


def test_subjects_summary_vital_status_not_reported_is_case_insensitive_and_excludes_nulls():
    """
    Regression:
    vital_status=Not reported should be a valid enum value.
    It should match both:
    - DB value variants like "Not Reported" (case-insensitive matching)
    """
    session = _CapturingSession()
    repo = SubjectRepository(session=session, allowlist=_DummyAllowlist(), settings=Settings())

    import asyncio

    asyncio.run(repo.get_subjects_summary({"vital_status": "Not reported"}))

    cypher = session.last_cypher or ""
    assert "final_vital_status IS NOT NULL" in cypher
    assert "toLower(toString(final_vital_status))" in cypher


