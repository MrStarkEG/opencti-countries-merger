"""10-phase merge engine ported from the Rust codebase — async with concurrency."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from rich.progress import Progress, TaskID

from opencti_country_merger.es.client import ESClient
from opencti_country_merger.es import queries
from opencti_country_merger.models.errors import PhaseFailedError
from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.planner import CountryCluster


@dataclass
class MergeResult:
    iso_code: str
    target_id: str
    sources_merged: int
    phases_completed: int
    docs_updated: int = 0
    docs_deleted: int = 0
    docs_archived: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class JunkDeleteResult:
    name: str
    entity_id: str
    rels_deleted: int = 0
    archived: bool = False
    deleted: bool = False
    errors: list[str] = field(default_factory=list)


# Relationship indices used in phases 2-7
_PHASE_REL_INDICES: list[tuple[int, str, bool]] = [
    (2, "stix_core_relationships-*", False),
    (3, "stix_cyber_observable_relationships-*", False),
    (4, "stix_core_relationships-*", True),  # inferred only
    (5, "stix_meta_relationships-*", False),
    (6, "internal_relationships-*", False),
    (7, "stix_sighting_relationships-*", False),
]

# All relationship index patterns for Phase 8 discovery
_PHASE8_DISCOVERY_INDICES = [
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
    "inferred_relationships-*",
    "pir_relationships-*",
]

# Target indices for Phase 8 updates
_PHASE8_TARGET_INDICES = [
    "stix_domain_objects-*",
    "stix_cyber_observables-*",
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
    "inferred_relationships-*",
]

# Deduplicated relationship index suffixes for junk deletion
_JUNK_REL_INDICES = list(dict.fromkeys(
    [s for _, s, _ in _PHASE_REL_INDICES]
    + ["inferred_relationships-*", "pir_relationships-*"]
))


class MergerService:
    """Execute the 10-phase merge for a single cluster."""

    def __init__(self, client: ESClient, *, dry_run: bool = False) -> None:
        self._client = client
        self._dry_run = dry_run
        self._prefix = client.prefix

    async def merge_cluster(
        self,
        cluster: CountryCluster,
        progress: Progress | None = None,
        task_id: TaskID | None = None,
    ) -> MergeResult:
        result = MergeResult(
            iso_code=cluster.iso_code,
            target_id=cluster.target_entity.internal_id,
            sources_merged=len(cluster.source_entities),
            phases_completed=0,
        )
        target = cluster.target_entity

        for source in cluster.source_entities:
            try:
                await self._merge_one(target, source, cluster, result)
            except PhaseFailedError as exc:
                result.errors.append(str(exc))
            if progress and task_id is not None:
                progress.advance(task_id)

        return result

    async def _merge_one(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        """Execute all 10 phases to merge *source* into *target*."""
        try:
            # Phase 1: Update target entity
            await self._phase_update_target(target, source, cluster, result)
            result.phases_completed = 1
        except Exception as exc:
            raise PhaseFailedError(1, "Update target entity", exc) from exc

        try:
            # Phases 2-7: Redirect relationships — run concurrently
            await self._phases_2_to_7_concurrent(target, source, cluster, result)
            result.phases_completed = 7
        except Exception as exc:
            raise PhaseFailedError(7, "Redirect relationships", exc) from exc

        try:
            # Phase 8: Update denormalized refs
            await self._phase_update_denormalized_refs(target, source, cluster, result)
            result.phases_completed = 8
        except Exception as exc:
            raise PhaseFailedError(8, "Update denormalized refs", exc) from exc

        try:
            # Phase 9: Archive source
            await self._phase_archive_source(target, source, cluster, result)
            result.phases_completed = 9
        except Exception as exc:
            raise PhaseFailedError(9, "Archive source entity", exc) from exc

        try:
            # Phase 10: Delete source
            await self._phase_delete_source(target, source, cluster, result)
            result.phases_completed = 10
        except Exception as exc:
            raise PhaseFailedError(10, "Delete source entity", exc) from exc

    # ------------------------------------------------------------------
    # Phase 1: Update target entity
    # ------------------------------------------------------------------

    async def _phase_update_target(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        doc = dict(target.source)

        # Merge aliases — collect all names + codes into a single set
        existing_aliases: list[str] = doc.get("x_opencti_aliases", []) or []
        source_aliases: list[str] = source.source.get("x_opencti_aliases", []) or []
        source_name = source.source.get("name", "")

        alias_set: set[str] = set(existing_aliases + source_aliases)

        # Add original names from both target and source
        target_name = doc.get("name", "")
        if target_name:
            alias_set.add(target_name)
        if source_name:
            alias_set.add(source_name)

        # Add country codes (alpha-2 and alpha-3) as aliases
        alias_set.add(cluster.iso_code)     # e.g. "US"
        alias_set.add(cluster.alpha_3)      # e.g. "USA"

        # Remove the new canonical name from aliases (it's the name, not an alias)
        alias_set.discard(cluster.country_name)

        doc["x_opencti_aliases"] = sorted(alias_set)

        # Merge stix_ids
        existing_ids: list[str] = doc.get("i_aliases_ids", []) or []
        source_ids: list[str] = source.source.get("i_aliases_ids", []) or []
        source_std_id = source.source.get("standard_id", "")
        merged_ids = list(set(existing_ids + source_ids))
        if source_std_id and source_std_id not in merged_ids:
            merged_ids.append(source_std_id)
        doc["i_aliases_ids"] = merged_ids

        # Rename to full country name
        doc["name"] = cluster.country_name

        if not self._dry_run:
            await self._client.index_document(target.index, target.internal_id, doc)
        result.docs_updated += 1

    # ------------------------------------------------------------------
    # Phases 2-7: Redirect relationships — concurrent
    # ------------------------------------------------------------------

    async def _phases_2_to_7_concurrent(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        """Run all 6 relationship redirect phases concurrently."""

        async def _do_one(rel_index_suffix: str, inferred: bool) -> int:
            index = f"{self._prefix}{rel_index_suffix}"
            body = queries.redirect_connections(
                source_id=source.internal_id,
                target_id=target.internal_id,
                target_name=cluster.country_name,
                inferred_only=inferred,
            )
            # Skip if 0 docs match
            cnt = await self._client.count(index, body)
            if cnt == 0:
                return 0
            if self._dry_run:
                return cnt
            resp = await self._client.update_by_query(index, body)
            return resp["updated"]

        tasks = [
            _do_one(rel_index, inferred)
            for _, rel_index, inferred in _PHASE_REL_INDICES
        ]
        counts = await asyncio.gather(*tasks)
        result.docs_updated += sum(counts)

    # ------------------------------------------------------------------
    # Phase 8: Update denormalized refs — concurrent discovery + updates
    # ------------------------------------------------------------------

    async def _phase_update_denormalized_refs(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        # Discover relationship types concurrently across all indices
        rel_types = await self._discover_rel_types_concurrent(source.internal_id)
        if not rel_types:
            return

        # For each rel_type × target_index, run updates concurrently
        async def _do_update(rel_type: str, idx_suffix: str) -> int:
            index = f"{self._prefix}{idx_suffix}"
            body = queries.denormalized_ref_update(
                rel_type, source.internal_id, target.internal_id
            )
            cnt = await self._client.count(index, body)
            if cnt == 0:
                return 0
            if self._dry_run:
                return cnt
            resp = await self._client.update_by_query(index, body)
            return resp["updated"]

        tasks = [
            _do_update(rt, idx)
            for rt in rel_types
            for idx in _PHASE8_TARGET_INDICES
        ]
        counts = await asyncio.gather(*tasks)
        result.docs_updated += sum(counts)

    async def _discover_rel_types_concurrent(self, entity_id: str) -> set[str]:
        """Find all unique relationship_type values referencing *entity_id*."""

        async def _discover_one(idx_suffix: str) -> list[str]:
            index = f"{self._prefix}{idx_suffix}"
            body = queries.relationships_by_entity(entity_id)
            body["aggs"] = {
                "rel_types": {
                    "terms": {"field": "relationship_type.keyword", "size": 500}
                }
            }
            try:
                resp = await self._client.search(index, body, size=0)
                buckets = (
                    resp.get("aggregations", {})
                    .get("rel_types", {})
                    .get("buckets", [])
                )
                return [b["key"] for b in buckets]
            except Exception:
                return []

        results = await asyncio.gather(
            *[_discover_one(idx) for idx in _PHASE8_DISCOVERY_INDICES]
        )
        rel_types: set[str] = set()
        for keys in results:
            rel_types.update(keys)
        return rel_types

    # ------------------------------------------------------------------
    # Phase 9: Archive source entity
    # ------------------------------------------------------------------

    async def _phase_archive_source(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        archive_index = f"{self._prefix}deleted_objects"
        if not self._dry_run:
            await self._client.index_document(
                archive_index, source.internal_id, source.source
            )
        result.docs_archived += 1

    # ------------------------------------------------------------------
    # Phase 10: Delete source entity
    # ------------------------------------------------------------------

    async def _phase_delete_source(
        self,
        target: CountryEntity,
        source: CountryEntity,
        cluster: CountryCluster,
        result: MergeResult,
    ) -> None:
        if not self._dry_run:
            await self._client.delete_document(source.index, source.internal_id)
        result.docs_deleted += 1

    # ------------------------------------------------------------------
    # Junk entity deletion — concurrent
    # ------------------------------------------------------------------

    async def delete_junk_entities(
        self,
        junk: list[CountryEntity],
        progress: Progress | None = None,
        task_id: TaskID | None = None,
    ) -> list[JunkDeleteResult]:
        """Delete junk entities and all their relationships concurrently."""
        results: list[JunkDeleteResult] = []

        for entity in junk:
            r = JunkDeleteResult(name=entity.name, entity_id=entity.internal_id)
            try:
                # 1. Delete all relationships concurrently across indices
                async def _del_rels(idx_suffix: str) -> int:
                    index = f"{self._prefix}{idx_suffix}"
                    body = queries.delete_relationships_by_entity(entity.internal_id)
                    cnt = await self._client.count(index, body)
                    if cnt == 0:
                        return 0
                    if self._dry_run:
                        return cnt
                    resp = await self._client.delete_by_query(index, body)
                    return resp["deleted"]

                counts = await asyncio.gather(
                    *[_del_rels(idx) for idx in _JUNK_REL_INDICES]
                )
                r.rels_deleted = sum(counts)

                # 2. Archive
                archive_index = f"{self._prefix}deleted_objects"
                if not self._dry_run:
                    await self._client.index_document(
                        archive_index, entity.internal_id, entity.source
                    )
                r.archived = True

                # 3. Delete
                if not self._dry_run:
                    await self._client.delete_document(entity.index, entity.internal_id)
                r.deleted = True

            except Exception as exc:
                r.errors.append(str(exc))

            results.append(r)
            if progress and task_id is not None:
                progress.advance(task_id)

        return results
