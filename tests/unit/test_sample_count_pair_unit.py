"""Every count query on /sample/by/{field}/count must use the (sample_id, study_id) pair unit.

sample_id is not globally unique -- the same id can exist in two studies -- so the whole
endpoint counts pairs, not sample nodes. `total`, `missing` and `values` must agree on that
unit or they stop reconciling.

The diagnosis-field `missing` query used to end in `count(DISTINCT sa)`, counting sample
NODES. That agrees with the pair count only while every sample node reaches exactly one
study, which is a property of the current data, not a schema constraint. These tests pin
the unit so it cannot regress.
"""

from unittest.mock import AsyncMock, Mock

import pytest
from neo4j import AsyncSession

from app.core.config import Settings
from app.lib.field_allowlist import FieldAllowlist
from app.repositories.sample import SampleRepository


async def _async_gen_from_list(items):
    for item in items:
        yield item


@pytest.mark.unit
class TestSampleCountPairUnit:
    """The Cypher emitted by count_samples_by_field must count pairs, never sample nodes."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=AsyncSession)

    @pytest.fixture
    def repository(self, mock_session):
        return SampleRepository(mock_session, Mock(spec=FieldAllowlist), Mock(spec=Settings))

    async def _queries_for(self, repository, mock_session, field):
        """Run a count and return the Cypher of every query it issued."""
        mock_session.run = AsyncMock(side_effect=lambda *a, **kw: AsyncMock(
            __aiter__=Mock(return_value=_async_gen_from_list([]))
        ))
        await repository.count_samples_by_field(field)
        return [call.args[0] for call in mock_session.run.call_args_list]

    # The four fields whose `missing` query lives on the diagnosis node.
    @pytest.mark.parametrize(
        "field",
        ["disease_phase", "tumor_grade", "age_at_diagnosis", "tumor_tissue_morphology"],
    )
    async def test_diagnosis_missing_query_counts_pairs_not_nodes(
        self, repository, mock_session, field
    ):
        queries = await self._queries_for(repository, mock_session, field)
        missing_query = next(q for q in queries if "as missing" in q)

        # The regression: counting sample nodes under-reports missing for any sample
        # node that reaches two studies (it owns two pairs, both missing, counted once).
        assert "count(DISTINCT sa) as missing" not in missing_query

        # It must dedupe to the pair and count those.
        assert "WITH DISTINCT sa.sample_id as sample_id, sid as study_id" in missing_query
        assert "RETURN count(*) as missing" in missing_query

    @pytest.mark.parametrize(
        "field",
        [
            "disease_phase",
            "tumor_grade",
            "age_at_diagnosis",
            "tumor_tissue_morphology",
            "tissue_type",
            "tumor_classification",
            "age_at_collection",
            "anatomical_sites",
        ],
    )
    async def test_no_query_counts_sample_nodes(self, repository, mock_session, field):
        """No count query for any field may fall back to counting sample nodes."""
        for query in await self._queries_for(repository, mock_session, field):
            assert "count(DISTINCT sa)" not in query, f"{field} counts sample nodes"
