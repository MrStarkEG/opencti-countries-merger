"""Build a merge plan by grouping country entities by ISO code."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field

from opencti_country_merger.es.client import ESClient
from opencti_country_merger.es import queries
from opencti_country_merger.services.country_mapper import CountryMapper, JUNK_COUNTRY_NAMES
from opencti_country_merger.services.discovery import CountryEntity


@dataclass
class CountryCluster:
    iso_code: str
    country_name: str
    alpha_3: str
    target_entity: CountryEntity
    source_entities: list[CountryEntity]
    target_rel_count: int = 0
    source_rel_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class MergePlan:
    clusters: list[CountryCluster]
    unresolved: list[CountryEntity]
    junk: list[CountryEntity]
    total_entities: int = 0
    total_merges: int = 0


RELATIONSHIP_INDICES = [
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
]


class PlannerService:
    """Groups entities by ISO code, selects merge targets, and builds the plan."""

    def __init__(self, client: ESClient, mapper: CountryMapper) -> None:
        self._client = client
        self._mapper = mapper

    async def build_plan(self, entities: list[CountryEntity]) -> MergePlan:
        # 1. Classify each entity: junk, resolved, or unresolved
        groups: dict[str, list[CountryEntity]] = defaultdict(list)
        unresolved: list[CountryEntity] = []
        junk: list[CountryEntity] = []

        for entity in entities:
            if entity.name in JUNK_COUNTRY_NAMES:
                junk.append(entity)
                continue

            code = self._mapper.resolve_entity(entity.source)
            if code:
                groups[code].append(entity)
            else:
                unresolved.append(entity)

        # 2. For groups with 2+ entities, count rels concurrently and pick targets
        duplicate_groups = {
            code: members for code, members in groups.items() if len(members) >= 2
        }

        # Collect all entity IDs that need relationship counts
        all_entities_to_count: list[CountryEntity] = []
        for members in duplicate_groups.values():
            all_entities_to_count.extend(members)

        # Count all relationships concurrently using msearch batches
        rel_counts = await self._count_relationships_batch(all_entities_to_count)

        # 3. Build clusters
        clusters: list[CountryCluster] = []
        total_merges = 0

        for iso_code in sorted(duplicate_groups):
            members = duplicate_groups[iso_code]

            members_sorted = sorted(
                members,
                key=lambda e: (
                    -rel_counts.get(e.internal_id, 0),
                    e.source.get("created_at", ""),
                ),
            )
            target = members_sorted[0]
            sources = members_sorted[1:]

            info = self._mapper.get_country_info(iso_code)
            cluster = CountryCluster(
                iso_code=iso_code,
                country_name=info.name,
                alpha_3=info.alpha_3,
                target_entity=target,
                source_entities=sources,
                target_rel_count=rel_counts.get(target.internal_id, 0),
                source_rel_counts={
                    e.internal_id: rel_counts.get(e.internal_id, 0) for e in sources
                },
            )
            clusters.append(cluster)
            total_merges += len(sources)

        return MergePlan(
            clusters=clusters,
            unresolved=unresolved,
            junk=junk,
            total_entities=len(entities),
            total_merges=total_merges,
        )

    async def _count_relationships_batch(
        self, entities: list[CountryEntity]
    ) -> dict[str, int]:
        """Count relationships for many entities using batched msearch."""
        if not entities:
            return {}

        # Build msearch request pairs: (index, query) for each entity × rel index
        requests: list[tuple[str, dict[str, Any]]] = []
        entity_ids: list[str] = []

        for entity in entities:
            body = queries.relationships_by_entity(entity.internal_id)
            for idx_suffix in RELATIONSHIP_INDICES:
                index = f"{self._client.prefix}{idx_suffix}"
                requests.append((index, body))
                entity_ids.append(entity.internal_id)

        # Execute in msearch batches of 50 (= 100 body lines)
        batch_size = 50
        all_counts: list[int] = []
        for i in range(0, len(requests), batch_size):
            batch = requests[i : i + batch_size]
            counts = await self._client.msearch_counts(batch)
            all_counts.extend(counts)

        # Aggregate counts per entity
        n_indices = len(RELATIONSHIP_INDICES)
        result: dict[str, int] = {}
        for idx, entity in enumerate(entities):
            start = idx * n_indices
            total = sum(all_counts[start : start + n_indices])
            result[entity.internal_id] = total

        return result
