"""
Unit tests for FileRepository Cypher query patterns (_build_count_query, get_files branches).
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from neo4j import AsyncSession

from app.config_data.file_node_registry import FileNodeConfig, FILE_NODE_REGISTRY
from app.repositories.file import FileRepository, _ZERO_COUNT_SENTINEL
from app.lib.field_allowlist import FieldAllowlist
from app.models.errors import UnsupportedFieldError


def make_repo(config=None):
    session = AsyncMock(spec=AsyncSession)
    allowlist = Mock(spec=FieldAllowlist)
    return FileRepository(session, allowlist, config or FILE_NODE_REGISTRY[1]), session


def _empty_async_result():
    async def async_gen():
        return
        yield

    mock_result = AsyncMock()
    mock_result.__aiter__ = Mock(return_value=async_gen())
    mock_result.consume = AsyncMock()
    return mock_result


@pytest.mark.unit
class TestBuildCountQuery:
    """_build_count_query must mirror get_files filter logic (four patterns + sentinel)."""

    @pytest.mark.asyncio
    async def test_invalid_file_type_returns_sentinel(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"file_type": "NOT_A_TYPE"})
        assert cypher is _ZERO_COUNT_SENTINEL
        assert params == {}

    @pytest.mark.asyncio
    async def test_depositions_or_uses_in_clause(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query(
            {"depositions": "phs002431 || phs002517"}
        )
        assert "st.study_id IN" in cypher
        assert "MATCH (st:study)" in cypher
        in_params = [v for v in params.values() if isinstance(v, list)]
        assert ["phs002431", "phs002517"] in in_params

    @pytest.mark.asyncio
    async def test_depositions_only_reverse_traversal_pattern(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"depositions": "phs002431"})
        assert "MATCH (st:study)" in cypher
        assert "st.study_id =" in cypher
        assert "sf_list_path2" in cypher
        assert params and "phs002431" in params.values()

    @pytest.mark.asyncio
    async def test_file_type_only_uses_multihop_count(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"file_type": "BAM"})
        assert "toLower(sf.file_type)" in cypher
        assert "RETURN count(DISTINCT sf)" in cypher
        assert "MATCH (st:study)" not in cypher.split("RETURN")[0] or "coalesce" in cypher
        assert params.get("param_1") == "BAM"

    @pytest.mark.asyncio
    async def test_file_type_and_depositions_pattern_one(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query(
            {"file_type": "FASTQ", "depositions": "phs001"}
        )
        assert "toLower(sf.file_type)" in cypher
        assert "st2.study_id =" in cypher
        assert "st1.study_id =" in cypher
        assert "RETURN count(DISTINCT sf)" in cypher

    @pytest.mark.asyncio
    async def test_no_filters_simple_count_pattern(self):
        # No-filter summary uses two OPTIONAL MATCHes + IS NOT NULL filter — no collect.
        repo, _ = make_repo()
        cypher, _ = await repo._build_count_query({})
        assert f"MATCH (sf:{repo.config.node_label})" in cypher
        assert "WHERE" in cypher
        assert "participant" in cypher
        assert "cell_line" in cypher
        assert "RETURN count(DISTINCT sf)" in cypher

    @pytest.mark.asyncio
    async def test_single_md5sum_or_checksum_value(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"md5sum": "deadbeef"})
        assert "sf.md5sum =" in cypher
        assert "sf.checksum_value =" in cypher
        assert "deadbeef" in params.values()

    @pytest.mark.asyncio
    async def test_multiple_md5sums_use_in_clause(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"md5sum": "aaa||bbb"})
        assert "sf.md5sum IN" in cypher
        assert ["aaa", "bbb"] in list(params.values())

    @pytest.mark.asyncio
    async def test_maf_config_in_count_cypher(self):
        maf = FileNodeConfig("methylation_array_file", "of_methylation_array_file")
        repo, _ = make_repo(maf)
        cypher, _ = await repo._build_count_query({"depositions": "phs001"})
        assert "methylation_array_file" in cypher
        assert "of_methylation_array_file" in cypher
        assert "sequencing_file" not in cypher

    @pytest.mark.asyncio
    async def test_file_size_string_coerced_to_int(self):
        repo, _ = make_repo()
        _, params = await repo._build_count_query({"file_size": "'70925'"})
        assert params.get("param_1") == 70925


@pytest.mark.unit
class TestGetFilesCypherPatterns:
    """get_files() query shape for each optimization pattern."""

    @pytest.mark.asyncio
    async def test_no_filters_simple_pagination_pattern(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({}, offset=0, limit=20)
        cypher = session.run.call_args[0][0]
        assert f"MATCH (sf:{repo.config.node_label})" in cypher
        assert "SKIP $offset" in cypher
        assert "LIMIT $limit" in cypher

    @pytest.mark.asyncio
    async def test_depositions_only_early_pagination_from_study(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"depositions": "phs002431"}, offset=5, limit=10)
        cypher = session.run.call_args[0][0]
        params = session.run.call_args[0][1]
        assert "MATCH (st:study)" in cypher
        assert "WITH DISTINCT sf.id" in cypher
        assert "of_sequencing_file" in cypher
        assert params["offset"] == 5
        assert params["limit"] == 10

    @pytest.mark.asyncio
    async def test_maf_depositions_only_uses_maf_relationship(self):
        maf = FileNodeConfig("methylation_array_file", "of_methylation_array_file")
        repo, session = make_repo(maf)
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"depositions": "phs001"}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        assert "of_methylation_array_file" in cypher
        assert "methylation_array_file" in cypher
        assert "of_sequencing_file" not in cypher

    @pytest.mark.asyncio
    async def test_file_type_and_depositions_combined_pattern(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files(
            {"file_type": "BAM", "depositions": "phs002431"},
            offset=0,
            limit=20,
        )
        cypher = session.run.call_args[0][0]
        assert "toLower(sf.file_type)" in cypher
        assert "st2.study_id" in cypher
        assert "MATCH (sf:sequencing_file)" in cypher

    @pytest.mark.asyncio
    async def test_file_type_only_optimized_pattern(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"file_type": "FASTQ"}, offset=0, limit=10)
        cypher = session.run.call_args[0][0]
        assert "toLower(sf.file_type)" in cypher
        assert "MATCH (sf:sequencing_file)" in cypher
        assert "MATCH (st:study)" not in cypher.split("OPTIONAL")[0]

    @pytest.mark.asyncio
    async def test_md5sum_filter_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"md5sum": "abc123"}, offset=0, limit=10)
        cypher = session.run.call_args[0][0]
        params = session.run.call_args[0][1]
        assert "sf.md5sum" in cypher
        assert "abc123" in params.values()

    @pytest.mark.asyncio
    async def test_depositions_or_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"depositions": "phs001 || phs002"}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        params = session.run.call_args[0][1]
        assert "st.study_id IN" in cypher
        assert ["phs001", "phs002"] in list(params.values())

    @pytest.mark.asyncio
    async def test_unharmonized_file_name_list_filter(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files(
            {"metadata.unharmonized.file_name": ["a.bam", "b.bam"]},
            offset=0,
            limit=10,
        )
        cypher = session.run.call_args[0][0]
        params = session.run.call_args[0][1]
        assert "sf.file_name IN" in cypher
        assert params.get("param_1") == ["a.bam", "b.bam"]


@pytest.mark.unit
class TestGetFileByIdentifierCypher:
    @pytest.mark.asyncio
    async def test_uses_config_node_label(self):
        maf = FileNodeConfig("methylation_array_file", "of_methylation_array_file")
        repo, session = make_repo(maf)
        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=_empty_async_result().__aiter__())
        mock_result.consume = AsyncMock()
        session.run = AsyncMock(return_value=mock_result)

        await repo.get_file_by_identifier("CCDI-DCC", "phs001", "file-uuid")

        cypher = session.run.call_args[0][0]
        assert "methylation_array_file" in cypher
        assert "of_methylation_array_file" in cypher
        assert session.run.call_args[0][1]["name"] == "file-uuid"
        assert session.run.call_args[0][1]["namespace"] == "phs001"

    @pytest.mark.asyncio
    async def test_returns_file_when_record_present(self):
        repo, session = make_repo()

        async def one_row():
            yield {
                "sf": {
                    "id": "f1",
                    "file_type": "BAM",
                    "file_name": "f1.bam",
                    "md5sum": "abc",
                },
                "samples": [{"sample_id": "S1"}],
                "st": {"study_id": "phs002431"},
            }

        mock_result = AsyncMock()
        mock_result.__aiter__ = Mock(return_value=one_row())
        session.run = AsyncMock(return_value=mock_result)

        file_obj = await repo.get_file_by_identifier("CCDI-DCC", "phs002431", "f1")

        assert file_obj is not None
        assert file_obj.id["name"] == "f1"
        assert file_obj.id["namespace"]["name"] == "phs002431"


def _count_field_mock_results(total: int, missing: int, values: list) -> list:
    """Build three AsyncMock results for count_files_by_field session.run calls."""

    def _one_row(key: str, val: int) -> AsyncMock:
        mock = AsyncMock()
        mock.__aiter__.return_value = [{key: val}]
        mock.consume = AsyncMock()
        return mock

    values_mock = AsyncMock()
    values_mock.__aiter__.return_value = values
    values_mock.consume = AsyncMock()
    return [_one_row("total", total), _one_row("missing", missing), values_mock]


@pytest.mark.unit
class TestCountFilesByFieldWithFilters:
    """count_files_by_field paths when base_where_conditions is non-empty."""

    @pytest.mark.asyncio
    async def test_type_count_with_file_type_filter_uses_optimized_pattern(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(
                10,
                1,
                [{"value": "bam", "count": 9}],
            )
        )

        result = await repo.count_files_by_field("type", {"file_type": "BAM"})

        assert result["total"] == 10
        assert result["values"][0]["value"] == "BAM"
        total_cypher = session.run.call_args_list[0][0][0]
        values_cypher = session.run.call_args_list[2][0][0]
        assert "toLower(sf.file_type)" in total_cypher
        assert "MATCH (sf:sequencing_file)" in total_cypher
        assert "toLower(sf.file_type) IN" in values_cypher

    @pytest.mark.asyncio
    async def test_type_count_with_unharmonized_file_name_filter(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(3, 0, [{"value": "FASTQ", "count": 3}])
        )

        await repo.count_files_by_field(
            "type",
            {"metadata.unharmonized.file_name": ["a.bam", "b.bam"]},
        )

        total_cypher = session.run.call_args_list[0][0][0]
        params = session.run.call_args_list[0][0][1]
        assert "sf.file_name IN" in total_cypher
        assert ["a.bam", "b.bam"] in params.values()

    @pytest.mark.asyncio
    async def test_type_count_with_scalar_unharmonized_filter(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(1, 0, [{"value": "BAM", "count": 1}])
        )

        await repo.count_files_by_field(
            "type",
            {"metadata.unharmonized.file_name": "solo.bam"},
        )

        total_cypher = session.run.call_args_list[0][0][0]
        assert "sf.file_name =" in total_cypher

    @pytest.mark.asyncio
    async def test_type_count_with_file_filter_uses_enum_missing_clause(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(4, 1, [{"value": "BAM", "count": 3}])
        )

        await repo.count_files_by_field("type", {"file_type": "FASTQ"})

        missing_cypher = session.run.call_args_list[1][0][0]
        assert "sf.file_type IS NULL OR NOT" in missing_cypher

    @pytest.mark.asyncio
    async def test_type_count_with_scalar_file_size_filter(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(2, 0, [{"value": "BAM", "count": 2}])
        )

        await repo.count_files_by_field("type", {"file_size": 4096})

        total_cypher = session.run.call_args_list[0][0][0]
        assert "sf.file_size =" in total_cypher

    @pytest.mark.asyncio
    async def test_type_count_with_filters_and_no_enum_uses_simple_missing(self):
        """has_file_filters + empty enum: missing uses IS NULL only (line 690)."""
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(1, 0, [])
        )

        with patch("app.repositories.file.FileType") as mock_ft:
            mock_ft.values.return_value = []
            with patch("app.repositories.file.load_file_enum", return_value=[]):
                await repo.count_files_by_field("type", {"file_size": 100})

        missing_cypher = session.run.call_args_list[1][0][0]
        assert "sf.file_type IS NULL" in missing_cypher
        assert "NOT (toLower" not in missing_cypher


@pytest.mark.unit
class TestBuildCountQueryEdgeCases:
    """_build_count_query filter parsing edge cases."""

    @pytest.mark.asyncio
    async def test_empty_depositions_treated_as_no_depositions_filter(self):
        # Empty depositions ("  ||  ") is treated as no depositions filter — the
        # resulting query uses PATTERN 3 (no study_id WHERE clause), not PATTERN 2b.
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"depositions": "  ||  "})
        assert "study_id" not in cypher
        assert not any("study_id" in str(v) for v in params.values())

    @pytest.mark.asyncio
    async def test_empty_md5sum_omits_checksum_clause(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"md5sum": "||"})
        assert "checksum_value" not in cypher
        assert "md5sum" not in str(params)

    @pytest.mark.asyncio
    async def test_unharmonized_scalar_field_in_count_query(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query(
            {"metadata.unharmonized.file_name": "only.bam"}
        )
        assert "sf.file_name =" in cypher
        assert "only.bam" in params.values()

    @pytest.mark.asyncio
    async def test_file_size_list_uses_in_clause_in_count_query(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query({"file_size": [100, 200]})
        assert "sf.file_size IN" in cypher
        assert [100, 200] in params.values()

    @pytest.mark.asyncio
    async def test_file_size_non_numeric_string_kept_as_string(self):
        repo, _ = make_repo()
        _, params = await repo._build_count_query({"file_size": "not-a-number"})
        assert params.get("param_1") == "not-a-number"

    @pytest.mark.asyncio
    async def test_unharmonized_list_in_build_count_query(self):
        repo, _ = make_repo()
        cypher, params = await repo._build_count_query(
            {"metadata.unharmonized.file_name": ["a.bam", "b.bam"]}
        )
        assert "sf.file_name IN" in cypher
        assert ["a.bam", "b.bam"] in params.values()


@pytest.mark.unit
class TestGetFilesFilterEdgeCases:
    """get_files() filter parsing and query branches."""

    @pytest.mark.asyncio
    async def test_whitespace_only_depositions_uses_simple_list_pattern(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"depositions": " || "}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        assert "MATCH (st:study)" not in cypher.split("SKIP")[0]

    @pytest.mark.asyncio
    async def test_empty_md5sum_does_not_add_checksum_where(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"md5sum": "||"}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        assert "checksum_value" not in cypher

    @pytest.mark.asyncio
    async def test_file_size_string_coerced_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"file_size": "'1024'"}, offset=0, limit=5)
        params = session.run.call_args[0][1]
        assert params.get("param_1") == 1024

    @pytest.mark.asyncio
    async def test_file_size_scalar_filter_in_cypher(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"file_size": 5000}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        assert "sf.file_size =" in cypher

    @pytest.mark.asyncio
    async def test_multiple_md5sums_use_in_clause_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"md5sum": "aaa||bbb"}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        params = session.run.call_args[0][1]
        assert "sf.md5sum IN" in cypher
        assert ["aaa", "bbb"] in params.values()

    @pytest.mark.asyncio
    async def test_unharmonized_scalar_equals_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files(
            {"metadata.unharmonized.file_name": "x.bam"},
            offset=0,
            limit=5,
        )
        cypher = session.run.call_args[0][0]
        assert "sf.file_name =" in cypher

    @pytest.mark.asyncio
    async def test_file_size_list_filter_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"file_size": [100, 200]}, offset=0, limit=5)
        cypher = session.run.call_args[0][0]
        assert "sf.file_size IN" in cypher

    @pytest.mark.asyncio
    async def test_file_size_invalid_string_stays_string_in_get_files(self):
        repo, session = make_repo()
        session.run = AsyncMock(return_value=_empty_async_result())
        await repo.get_files({"file_size": "nope"}, offset=0, limit=5)
        params = session.run.call_args[0][1]
        assert params.get("param_1") == "nope"


@pytest.mark.unit
class TestGetFilesRetry:
    """get_files() empty-result and error retry paths."""

    def _file_row(self):
        return {
            "sf": {
                "id": "f1.bam",
                "file_type": "BAM",
                "file_name": "f1.bam",
                "md5sum": "abc",
            },
            "samples": [{"sample_id": "S1"}],
            "st": {"study_id": "phs002431"},
        }

    def _async_result_from_rows(self, rows: list):
        async def gen():
            for row in rows:
                yield row

        mock = AsyncMock()
        mock.__aiter__ = Mock(return_value=gen())
        mock.consume = AsyncMock()
        return mock

    @pytest.mark.asyncio
    async def test_retries_when_first_query_returns_no_rows(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=[
                self._async_result_from_rows([]),
                self._async_result_from_rows([self._file_row()]),
            ]
        )

        with patch("app.repositories.file.asyncio.sleep", new=AsyncMock()):
            files = await repo.get_files({}, offset=0, limit=10)

        assert len(files) == 1
        assert files[0].id["name"] == "f1.bam"
        assert session.run.call_count == 2

    @pytest.mark.asyncio
    async def test_retries_after_session_error_then_succeeds(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=[
                RuntimeError("transient"),
                self._async_result_from_rows([self._file_row()]),
            ]
        )

        with patch("app.repositories.file.asyncio.sleep", new=AsyncMock()):
            files = await repo.get_files({}, offset=0, limit=10)

        assert len(files) == 1
        assert session.run.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries_on_persistent_error(self):
        repo, session = make_repo()
        session.run = AsyncMock(side_effect=RuntimeError("persistent"))

        with patch("app.repositories.file.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(RuntimeError, match="persistent"):
                await repo.get_files({}, offset=0, limit=10)

        assert session.run.call_count == 3


@pytest.mark.unit
class TestCountFilesByFieldNoFilters:
    """count_files_by_field() with no file-level filters (simple pattern)."""

    @pytest.mark.asyncio
    async def test_type_count_no_filters_uses_simple_missing_clause(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(
                5,
                2,
                [{"value": "FASTQ", "count": 3}],
            )
        )

        result = await repo.count_files_by_field("type", {})

        assert result["total"] == 5
        missing_cypher = session.run.call_args_list[1][0][0]
        assert "sf.file_type IS NULL" in missing_cypher
        assert "toLower(sf.file_type) IN" in missing_cypher

    @pytest.mark.asyncio
    async def test_type_count_no_filters_retries_on_empty_totals(self):
        repo, session = make_repo()
        empty = _count_field_mock_results(0, 0, [])
        success = _count_field_mock_results(2, 0, [{"value": "bam", "count": 2}])
        session.run = AsyncMock(side_effect=empty + success)

        with patch("app.repositories.file.asyncio.sleep", new=AsyncMock()):
            result = await repo.count_files_by_field("type", {})

        assert result["total"] == 2
        assert session.run.call_count == 6

    @pytest.mark.asyncio
    async def test_type_count_with_list_filter_on_db_field(self):
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(1, 0, [{"value": "BAM", "count": 1}])
        )

        await repo.count_files_by_field("type", {"md5sum": ["hash1", "hash2"]})

        total_cypher = session.run.call_args_list[0][0][0]
        assert "sf.md5sum IN" in total_cypher

    @pytest.mark.asyncio
    async def test_type_count_no_filters_without_enum_uses_null_missing_only(self):
        """When FileType.values() is empty, missing query uses IS NULL only (line 745)."""
        repo, session = make_repo()
        session.run = AsyncMock(
            side_effect=_count_field_mock_results(1, 0, [])
        )

        with patch("app.repositories.file.FileType") as mock_ft:
            mock_ft.values.return_value = []
            with patch("app.repositories.file.load_file_enum", return_value=[]):
                await repo.count_files_by_field("type", {})

        missing_cypher = session.run.call_args_list[1][0][0]
        assert " AND sf.file_type IS NULL" in missing_cypher
        assert "NOT (toLower" not in missing_cypher


@pytest.mark.unit
class TestCountFilesByDepositionsFilters:
    """_count_files_by_depositions filter building."""

    @pytest.mark.asyncio
    async def test_scalar_unharmonized_filter_on_depositions_count(self):
        repo, session = make_repo()
        total = AsyncMock()
        total.__aiter__.return_value = [{"total": 1}]
        missing = AsyncMock()
        missing.__aiter__.return_value = [{"missing": 0}]
        values = AsyncMock()
        values.__aiter__.return_value = [{"value": "phs001", "count": 1}]
        session.run = AsyncMock(side_effect=[total, missing, values])

        await repo._count_files_by_depositions(
            {"metadata.unharmonized.file_name": "x.bam"}
        )

        cypher = session.run.call_args_list[0][0][0]
        params = session.run.call_args_list[0][0][1]
        assert "sf.file_name =" in cypher
        assert params["param_1"] == "x.bam"

    @pytest.mark.asyncio
    async def test_scalar_file_field_filter_on_depositions_count(self):
        repo, session = make_repo()
        total = AsyncMock()
        total.__aiter__.return_value = [{"total": 2}]
        missing = AsyncMock()
        missing.__aiter__.return_value = [{"missing": 0}]
        values = AsyncMock()
        values.__aiter__.return_value = [{"value": "phs002431", "count": 2}]
        session.run = AsyncMock(side_effect=[total, missing, values])

        await repo._count_files_by_depositions({"file_type": "BAM"})

        cypher = session.run.call_args_list[0][0][0]
        assert "sf.file_type =" in cypher

    @pytest.mark.asyncio
    async def test_list_file_field_filter_on_depositions_count(self):
        repo, session = make_repo()
        total = AsyncMock()
        total.__aiter__.return_value = [{"total": 1}]
        missing = AsyncMock()
        missing.__aiter__.return_value = [{"missing": 0}]
        values = AsyncMock()
        values.__aiter__.return_value = [{"value": "phs001", "count": 1}]
        session.run = AsyncMock(side_effect=[total, missing, values])

        await repo._count_files_by_depositions({"md5sum": ["a", "b"]})

        cypher = session.run.call_args_list[0][0][0]
        params = session.run.call_args_list[0][0][1]
        assert "sf.md5sum IN" in cypher
        assert params["param_1"] == ["a", "b"]


@pytest.mark.unit
class TestFileRepositoryHelpers:
    def test_validate_filters_raises_for_disallowed_field(self):
        repo, _ = make_repo()
        repo.allowlist.is_field_allowed = Mock(return_value=False)
        with pytest.raises(UnsupportedFieldError):
            repo._validate_filters({"bad_field": "x"}, "file")

    def test_validate_filters_skips_underscore_prefixed_fields(self):
        repo, _ = make_repo()
        repo.allowlist.is_field_allowed = Mock(return_value=False)
        repo._validate_filters({"_internal": "x"}, "file")
        repo.allowlist.is_field_allowed.assert_not_called()

    def test_map_file_type_to_enum_empty_string_returns_none(self):
        repo, _ = make_repo()
        assert repo._map_file_type_to_enum("   ") is None
        assert repo._map_file_type_to_enum(None) is None
