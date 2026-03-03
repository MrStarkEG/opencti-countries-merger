"""Tests for the 10-phase merge engine using a mock ES client."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from opencti_country_merger.es.client import ESClient
from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.merger import MergerService
from opencti_country_merger.services.planner import CountryCluster


def _make_entity(
    internal_id: str, name: str, index: str = "opencti_stix_domain_objects-000001"
) -> CountryEntity:
    return CountryEntity(
        internal_id=internal_id,
        index=index,
        name=name,
        source={
            "internal_id": internal_id,
            "name": name,
            "entity_type": "Country",
            "x_opencti_aliases": [],
            "i_aliases_ids": [],
            "standard_id": f"location--{internal_id}",
        },
    )


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock(spec=ESClient)
    client.prefix = "opencti_"
    client.count = AsyncMock(return_value=0)
    client.update_by_query = AsyncMock(
        return_value={"updated": 0, "total": 0, "failures": 0}
    )
    client.search = AsyncMock(
        return_value={
            "hits": {"hits": []},
            "aggregations": {"rel_types": {"buckets": []}},
        }
    )
    client.index_document = AsyncMock(return_value={"result": "updated"})
    client.delete_document = AsyncMock(return_value={"result": "deleted"})
    client.delete_by_query = AsyncMock(
        return_value={"deleted": 0, "total": 0, "failures": 0}
    )
    return client


class TestMergerPhases:
    @pytest.mark.asyncio
    async def test_all_10_phases_complete(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=False)
        result = await merger.merge_cluster(cluster)

        assert result.iso_code == "US"
        assert result.sources_merged == 1
        assert result.phases_completed == 10
        assert not result.errors

    @pytest.mark.asyncio
    async def test_phase1_updates_target(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=False)
        await merger.merge_cluster(cluster)

        # Phase 1 should call index_document on the target
        call_args = mock_client.index_document.call_args_list[0]
        assert call_args[0][0] == target.index
        assert call_args[0][1] == target.internal_id
        doc = call_args[0][2]
        assert doc["name"] == "United States"
        # Aliases should include the original names and country codes
        assert "US" in doc["x_opencti_aliases"]       # alpha-2
        assert "USA" in doc["x_opencti_aliases"]       # alpha-3 + original source name
        assert "United States" not in doc["x_opencti_aliases"]  # name itself not in aliases

    @pytest.mark.asyncio
    async def test_phase9_archives_source(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=False)
        await merger.merge_cluster(cluster)

        # Phase 9: archive to deleted_objects (no wildcard, no -*)
        archive_calls = [
            c
            for c in mock_client.index_document.call_args_list
            if c[0][0] == "opencti_deleted_objects"
        ]
        assert len(archive_calls) == 1
        assert archive_calls[0][0][1] == source.internal_id

    @pytest.mark.asyncio
    async def test_phase10_deletes_source(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=False)
        await merger.merge_cluster(cluster)

        mock_client.delete_document.assert_called_once_with(
            source.index, source.internal_id
        )

    @pytest.mark.asyncio
    async def test_dry_run_skips_writes(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=True)
        result = await merger.merge_cluster(cluster)

        mock_client.index_document.assert_not_called()
        mock_client.delete_document.assert_not_called()
        mock_client.update_by_query.assert_not_called()
        assert result.phases_completed == 10

    @pytest.mark.asyncio
    async def test_multiple_sources(self, mock_client: AsyncMock) -> None:
        target = _make_entity("tgt-1", "United States")
        sources = [
            _make_entity("src-1", "USA"),
            _make_entity("src-2", "America"),
        ]
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=sources,
        )

        merger = MergerService(mock_client, dry_run=False)
        result = await merger.merge_cluster(cluster)

        assert result.sources_merged == 2
        assert result.docs_deleted == 2
        assert result.docs_archived == 2


class TestPhase8DenormalizedRefs:
    @pytest.mark.asyncio
    async def test_discovers_rel_types_from_all_indices(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.search = AsyncMock(
            return_value={
                "hits": {"hits": []},
                "aggregations": {
                    "rel_types": {"buckets": [{"key": "uses", "doc_count": 3}]}
                },
            }
        )
        # Phase 8 does count-before-update; return >0 so updates fire
        mock_client.count = AsyncMock(return_value=2)

        target = _make_entity("tgt-1", "United States")
        source = _make_entity("src-1", "USA")
        cluster = CountryCluster(
            iso_code="US",
            country_name="United States",
            alpha_3="USA",
            target_entity=target,
            source_entities=[source],
        )

        merger = MergerService(mock_client, dry_run=False)
        await merger.merge_cluster(cluster)

        ubq_bodies = [
            call[0][1] for call in mock_client.update_by_query.call_args_list
        ]
        denorm_bodies = [
            b for b in ubq_bodies if "rel_uses.internal_id.keyword" in str(b)
        ]
        assert len(denorm_bodies) > 0


class TestJunkDeletion:
    @pytest.mark.asyncio
    async def test_delete_junk_entities(self, mock_client: AsyncMock) -> None:
        mock_client.count = AsyncMock(return_value=5)
        mock_client.delete_by_query = AsyncMock(
            return_value={"deleted": 5, "total": 5, "failures": 0}
        )
        junk = [_make_entity("junk-1", "??")]

        merger = MergerService(mock_client, dry_run=False)
        results = await merger.delete_junk_entities(junk)

        assert len(results) == 1
        assert results[0].rels_deleted > 0
        assert results[0].archived
        assert results[0].deleted

    @pytest.mark.asyncio
    async def test_delete_junk_dry_run(self, mock_client: AsyncMock) -> None:
        mock_client.count = AsyncMock(return_value=3)
        junk = [_make_entity("junk-1", "EU")]

        merger = MergerService(mock_client, dry_run=True)
        results = await merger.delete_junk_entities(junk)

        mock_client.delete_by_query.assert_not_called()
        mock_client.delete_document.assert_not_called()
        assert results[0].rels_deleted > 0
