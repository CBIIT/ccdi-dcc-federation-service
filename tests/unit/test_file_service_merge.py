"""
Tests for FileService parallel N-type execution (multi-repo merge logic).
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY
from app.services.file import FileService
from app.lib.field_allowlist import FieldAllowlist
from app.models.dto import File, CountResponse
from app.core.config import Settings


def make_service(session=None, counts=None, files_per_repo=None):
    """Helper: build a FileService with mocked repos."""
    session = session or AsyncMock(spec=AsyncSession)
    allowlist = Mock(spec=FieldAllowlist)
    settings = Mock(spec=Settings)
    settings.pagination = Mock()
    settings.pagination.max_page_size = 1000
    settings.query_timeout = 60
    settings.cache = Mock()
    settings.cache.count_ttl = 300
    settings.cache.summary_ttl = 300
    return FileService(session, allowlist, settings, None)


def make_mock_file(name: str) -> Mock:
    f = Mock(spec=File)
    f.id = name
    return f


@pytest.mark.unit
class TestFileServiceMerge:

    @pytest.mark.asyncio
    async def test_get_files_returns_tuple(self):
        """get_files() must return (files, total_count)."""
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_for_pagination = AsyncMock(return_value=2)
        maf_repo.get_files = AsyncMock(return_value=[make_mock_file("maf-1"), make_mock_file("maf-2")])
        sf_repo = AsyncMock()
        sf_repo.count_for_pagination = AsyncMock(return_value=98)
        sf_repo.get_files = AsyncMock(return_value=[])

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            files, total = await svc.get_files({}, offset=0, limit=20)

        assert total == 100
        assert len(files) == 2

    @pytest.mark.asyncio
    async def test_offset_split_all_from_first_type(self):
        """offset=0, limit=2, count_maf=5 → only maf repo fetched."""
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_for_pagination = AsyncMock(return_value=5)
        maf_repo.get_files = AsyncMock(return_value=[make_mock_file("m1"), make_mock_file("m2")])
        sf_repo = AsyncMock()
        sf_repo.count_for_pagination = AsyncMock(return_value=100)
        sf_repo.get_files = AsyncMock(return_value=[])

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            files, total = await svc.get_files({}, offset=0, limit=2)

        maf_repo.get_files.assert_called_once_with({}, 0, 2)
        sf_repo.get_files.assert_not_called()
        assert len(files) == 2

    @pytest.mark.asyncio
    async def test_offset_split_all_from_second_type(self):
        """offset=5, limit=10, count_maf=5 → offset into sf is 0."""
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_for_pagination = AsyncMock(return_value=5)
        maf_repo.get_files = AsyncMock(return_value=[])
        sf_repo = AsyncMock()
        sf_repo.count_for_pagination = AsyncMock(return_value=100)
        sf_repo.get_files = AsyncMock(return_value=[make_mock_file(f"s{i}") for i in range(10)])

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            files, total = await svc.get_files({}, offset=5, limit=10)

        maf_repo.get_files.assert_not_called()
        sf_repo.get_files.assert_called_once_with({}, 0, 10)
        assert len(files) == 10

    @pytest.mark.asyncio
    async def test_offset_split_boundary_page(self):
        """offset=3, limit=5, count_maf=5 → 2 from maf, 3 from sf."""
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_for_pagination = AsyncMock(return_value=5)
        maf_repo.get_files = AsyncMock(return_value=[make_mock_file("m4"), make_mock_file("m5")])
        sf_repo = AsyncMock()
        sf_repo.count_for_pagination = AsyncMock(return_value=100)
        sf_repo.get_files = AsyncMock(return_value=[make_mock_file(f"s{i}") for i in range(3)])

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            files, total = await svc.get_files({}, offset=3, limit=5)

        maf_repo.get_files.assert_called_once_with({}, 3, 2)
        sf_repo.get_files.assert_called_once_with({}, 0, 3)
        assert len(files) == 5

    @pytest.mark.asyncio
    async def test_count_files_by_field_merges_values(self):
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_files_by_field = AsyncMock(return_value={
            "total": 10, "missing": 1,
            "values": [{"value": "IDAT", "count": 9}]
        })
        sf_repo = AsyncMock()
        sf_repo.count_files_by_field = AsyncMock(return_value={
            "total": 100, "missing": 5,
            "values": [{"value": "FASTQ", "count": 60}, {"value": "BAM", "count": 35}]
        })

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            result = await svc.count_files_by_field("type", {})

        assert result.total == 110
        assert result.missing == 6
        values_dict = {v.value: v.count for v in result.values}
        assert values_dict["FASTQ"] == 60
        assert values_dict["BAM"] == 35
        assert values_dict["IDAT"] == 9

    @pytest.mark.asyncio
    async def test_count_files_by_field_merges_same_value_across_types(self):
        """Both repos return 'BAM' — counts must be summed."""
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.count_files_by_field = AsyncMock(return_value={
            "total": 5, "missing": 0,
            "values": [{"value": "BAM", "count": 5}]
        })
        sf_repo = AsyncMock()
        sf_repo.count_files_by_field = AsyncMock(return_value={
            "total": 50, "missing": 0,
            "values": [{"value": "BAM", "count": 50}]
        })

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            result = await svc.count_files_by_field("type", {})

        values_dict = {v.value: v.count for v in result.values}
        assert values_dict["BAM"] == 55

    @pytest.mark.asyncio
    async def test_get_file_by_identifier_returns_maf_match(self):
        svc = make_service()
        found = make_mock_file("maf-uuid")
        maf_repo = AsyncMock()
        maf_repo.get_file_by_identifier = AsyncMock(return_value=found)
        sf_repo = AsyncMock()
        sf_repo.get_file_by_identifier = AsyncMock(return_value=None)

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            result = await svc.get_file_by_identifier("CCDI-DCC", "phs001", "maf-uuid")

        assert result is found

    @pytest.mark.asyncio
    async def test_get_file_by_identifier_raises_not_found(self):
        from app.models.errors import NotFoundError
        svc = make_service()
        maf_repo = AsyncMock()
        maf_repo.get_file_by_identifier = AsyncMock(return_value=None)
        sf_repo = AsyncMock()
        sf_repo.get_file_by_identifier = AsyncMock(return_value=None)

        with patch.object(svc, '_repos', [maf_repo, sf_repo]):
            with pytest.raises(NotFoundError):
                await svc.get_file_by_identifier("CCDI-DCC", "phs001", "no-such-id")

    @pytest.mark.asyncio
    async def test_get_file_by_identifier_invalid_organization(self):
        from app.models.errors import ValidationError

        svc = make_service()
        with pytest.raises(ValidationError, match="Only 'CCDI-DCC'"):
            await svc.get_file_by_identifier("WRONG-ORG", "phs001", "file-id")

    @pytest.mark.asyncio
    async def test_get_file_by_identifier_invalid_characters_in_namespace(self):
        from app.models.errors import ValidationError

        svc = make_service()
        with pytest.raises(ValidationError, match="Invalid characters"):
            await svc.get_file_by_identifier("CCDI-DCC", "phs 001", "file-id")

    @pytest.mark.asyncio
    async def test_count_files_by_field_timeout_when_budget_exhausted(self):
        import asyncio
        from app.models.errors import ValidationError

        svc = make_service()
        maf_repo = AsyncMock()
        sf_repo = AsyncMock()

        with patch.object(svc, "_repos", [maf_repo, sf_repo]):
            with patch(
                "app.services.file.asyncio.wait_for",
                side_effect=asyncio.TimeoutError(),
            ):
                with pytest.raises(ValidationError, match="timeout"):
                    await svc.count_files_by_field("type", {})
