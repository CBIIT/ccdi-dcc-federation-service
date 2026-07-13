"""
Tests for the count floor invariant (`all` can never be below `current`).

The floor lives at the service layer (floor_total_to_page_size) so every endpoint —
/sample, /sample-diagnosis, /file, /subject, /subject-diagnosis — and the Link header
derived from the same total inherit it, even when a transient count-query failure was
swallowed to 0 while the data query still returned rows.
"""

import pytest
from unittest.mock import AsyncMock, Mock

from app.core.pagination import floor_total_to_page_size
from app.services.sample import SampleService
from app.services.file import FileService


@pytest.mark.unit
class TestFloorHelper:
    def test_total_below_page_is_floored(self):
        assert floor_total_to_page_size(0, 2) == 2

    def test_total_above_page_unchanged(self):
        assert floor_total_to_page_size(500, 2) == 500

    def test_total_equal_page_unchanged(self):
        assert floor_total_to_page_size(2, 2) == 2

    def test_none_total_floored_to_page(self):
        assert floor_total_to_page_size(None, 3) == 3

    def test_none_total_empty_page_is_zero(self):
        assert floor_total_to_page_size(None, 0) == 0


def _sample_service() -> SampleService:
    svc = SampleService(AsyncMock(), Mock(), Mock())
    svc.settings.pagination.max_page_size = 1000
    return svc


@pytest.mark.unit
@pytest.mark.asyncio
class TestServiceFloorApplied:
    async def test_get_samples_floors_all_to_current(self):
        """A count of 0 alongside a non-empty page is floored to the page size."""
        svc = _sample_service()
        samples = [Mock(), Mock()]
        svc.repository.get_samples = AsyncMock(return_value=(samples, 0))

        _, total = await svc.get_samples(filters={}, offset=0, limit=20, return_total=True)

        assert total == 2  # floored to current, never the absurd all:0/current:2

    async def test_sample_diagnosis_floors_all_to_current(self):
        """/sample-diagnosis path (get_samples_for_diagnosis_endpoint) also floors."""
        svc = _sample_service()
        samples = [Mock(), Mock()]
        svc.repository.get_samples_for_diagnosis_endpoint = AsyncMock(
            return_value=(samples, 0)
        )

        _, total = await svc.get_samples_for_diagnosis_endpoint(
            filters={}, offset=0, limit=20
        )

        assert total == 2

    async def test_get_files_floors_all_to_current(self):
        """FileService floors the summed count up to the returned page size."""
        svc = FileService(AsyncMock(), Mock(), Mock())
        svc.settings.pagination.max_page_size = 1000
        repo = Mock()
        repo.count_for_pagination = AsyncMock(return_value=1)  # undercount
        repo.get_files = AsyncMock(return_value=[Mock(), Mock()])  # 2 rows returned
        svc._repos = [repo]

        files, total = await svc.get_files(filters={}, offset=0, limit=20)

        assert len(files) == 2
        assert total == 2  # floored from 1 to len(files)


class _CountResult:
    def __init__(self, rows):
        self._rows = rows

    async def __aiter__(self):
        for row in self._rows:
            yield row

    async def consume(self):
        pass


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_for_pagination_retries_transient_then_succeeds():
    """FileRepository.count_for_pagination retries a transient count failure via the helper."""
    from neo4j.exceptions import TransientError
    from app.repositories.file import FileRepository
    from app.config_data.file_node_registry import FILE_NODE_REGISTRY

    session = AsyncMock()
    session.run = AsyncMock(
        side_effect=[
            TransientError("Memgraph.TransientError: transaction conflict"),
            _CountResult([{"total_count": 7}]),
        ]
    )
    repo = FileRepository(session, Mock(), FILE_NODE_REGISTRY[1])

    total = await repo.count_for_pagination({})

    assert total == 7
    assert session.run.call_count == 2  # failed once, retried, succeeded
