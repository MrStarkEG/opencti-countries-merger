"""Merge Region 'Russia' into Country 'Russian Federation' (RU).

Steps:
  1. Redirect all relationships from Region Russia to Country Russian Federation
  2. Update denormalized ref arrays (Phase 8)
  3. Archive + delete the stale Region entity
  4. Verify Country Russian Federation is linked to Eastern Europe
  5. Add 'Russia' as an alias on Country Russian Federation

Usage:
    uv run python merge_russia_region.py --dry-run
    uv run python merge_russia_region.py
    uv run python merge_russia_region.py --force
"""

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.es import queries


COUNTRY_NAME = "Russian Federation"
REGION_NAME = "Russia"
EASTERN_EUROPE_NAME = "Eastern Europe"

RELATIONSHIP_PHASES: list[tuple[str, bool]] = [
    ("stix_core_relationships-*", False),
    ("stix_cyber_observable_relationships-*", False),
    ("stix_core_relationships-*", True),
    ("stix_meta_relationships-*", False),
    ("internal_relationships-*", False),
    ("stix_sighting_relationships-*", False),
]

PHASE8_DISCOVERY_INDICES = [
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
    "inferred_relationships-*",
    "pir_relationships-*",
]

PHASE8_TARGET_INDICES = [
    "stix_domain_objects-*",
    "stix_cyber_observables-*",
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
    "inferred_relationships-*",
]


@dataclass
class EntityHit:
    """Minimal entity from ES."""

    internal_id: str
    index: str
    name: str
    source: dict


async def find_entity(
    client: ESClient, name: str, entity_type: str,
) -> EntityHit | None:
    """Search for an entity by exact name and type."""
    sdo_index = f"{client.prefix}stix_domain_objects-*"
    body: dict[str, Any] = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"entity_type.keyword": entity_type}},
                    {"term": {"name.keyword": name}},
                ]
            }
        }
    }
    resp = await client.search(sdo_index, body, size=1)
    hits = resp["hits"]["hits"]
    if not hits:
        return None
    src = hits[0]["_source"]
    return EntityHit(
        internal_id=src["internal_id"],
        index=hits[0]["_index"],
        name=src.get("name", ""),
        source=src,
    )


async def count_relationships(client: ESClient, entity_id: str) -> int:
    """Count relationships across all rel indices (deduplicated)."""
    total = 0
    seen: set[str] = set()
    for idx_suffix, _ in RELATIONSHIP_PHASES:
        if idx_suffix in seen:
            continue
        seen.add(idx_suffix)
        idx = f"{client.prefix}{idx_suffix}"
        body = queries.relationships_by_entity(entity_id)
        total += await client.count(idx, body)
    return total


async def redirect_relationships(
    client: ESClient,
    source_id: str,
    target_id: str,
    target_name: str,
) -> int:
    """Redirect relationship connections (Phases 2-7)."""
    updated = 0
    for idx_suffix, inferred in RELATIONSHIP_PHASES:
        idx = f"{client.prefix}{idx_suffix}"
        body = queries.redirect_connections(
            source_id, target_id, target_name, inferred_only=inferred,
        )
        cnt = await client.count(idx, body)
        if cnt == 0:
            continue
        result = await client.update_by_query(idx, body)
        updated += result["updated"]
    return updated


async def discover_rel_types(client: ESClient, entity_id: str) -> set[str]:
    """Find all relationship_type values referencing *entity_id*."""
    rel_types: set[str] = set()
    for idx_suffix in PHASE8_DISCOVERY_INDICES:
        idx = f"{client.prefix}{idx_suffix}"
        body = queries.relationships_by_entity(entity_id)
        body["aggs"] = {
            "rel_types": {
                "terms": {"field": "relationship_type.keyword", "size": 500}
            }
        }
        try:
            resp = await client.search(idx, body, size=0)
            buckets = (
                resp.get("aggregations", {})
                .get("rel_types", {})
                .get("buckets", [])
            )
            rel_types.update(b["key"] for b in buckets)
        except Exception:
            continue
    return rel_types


async def redirect_denormalized_refs(
    client: ESClient,
    source_id: str,
    target_id: str,
    rel_types: set[str],
) -> int:
    """Update denormalized ref arrays (Phase 8)."""
    updated = 0
    for rel_type in rel_types:
        for idx_suffix in PHASE8_TARGET_INDICES:
            idx = f"{client.prefix}{idx_suffix}"
            body = queries.denormalized_ref_update(rel_type, source_id, target_id)
            cnt = await client.count(idx, body)
            if cnt == 0:
                continue
            result = await client.update_by_query(idx, body)
            updated += result["updated"]
    return updated


async def archive_entity(client: ESClient, entity: EntityHit) -> None:
    """Archive entity to deleted_objects."""
    archive_index = f"{client.prefix}deleted_objects"
    await client.index_document(archive_index, entity.internal_id, entity.source)


async def delete_entity(client: ESClient, entity: EntityHit) -> bool:
    """Delete entity from its index."""
    result = await client.delete_document(entity.index, entity.internal_id)
    return result is not None


async def add_alias(client: ESClient, entity: EntityHit, alias: str) -> None:
    """Add an alias to x_opencti_aliases if not already present."""
    aliases: list[str] = list(entity.source.get("x_opencti_aliases") or [])
    if alias in aliases:
        return
    aliases.append(alias)
    aliases.sort()
    await client.update_doc(entity.index, entity.internal_id, {
        "x_opencti_aliases": aliases,
    })


async def verify_located_at(
    client: ESClient, from_name: str, to_name: str,
) -> bool:
    """Check if a located-at relationship exists between two entities."""
    rels = f"{client.prefix}stix_core_relationships-*"
    body: dict[str, Any] = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"relationship_type.keyword": "located-at"}},
                    {"nested": {
                        "path": "connections",
                        "query": {"term": {"connections.name.keyword": from_name}},
                    }},
                    {"nested": {
                        "path": "connections",
                        "query": {"term": {"connections.name.keyword": to_name}},
                    }},
                ]
            }
        }
    }
    cnt = await client.count(rels, body)
    return cnt > 0


async def run(dry_run: bool, force: bool) -> None:
    """Main execution flow."""
    settings = Settings()
    client = ESClient(settings)

    try:
        await client.health_check()
        print("Connected to Elasticsearch.\n")

        # --- Locate entities ---
        country = await find_entity(client, COUNTRY_NAME, "Country")
        region = await find_entity(client, REGION_NAME, "Region")

        if country is None:
            print(f"Country '{COUNTRY_NAME}' not found. Aborting.")
            return
        print(f"Country: '{country.name}' ({country.internal_id})")
        print(f"  Aliases: {country.source.get('x_opencti_aliases', [])}")

        if region is None:
            print(f"Region '{REGION_NAME}' not found. Nothing to merge.")
            return
        print(f"Region:  '{region.name}' ({region.internal_id})")

        # --- Count relationships on region ---
        rel_count = await count_relationships(client, region.internal_id)
        print(f"\nRegion '{REGION_NAME}' has {rel_count} relationship(s) to redirect.")

        # --- Discover denormalized ref types ---
        rel_types = await discover_rel_types(client, region.internal_id)
        if rel_types:
            print(f"Denormalized ref types: {', '.join(sorted(rel_types))}")

        # --- Verify Eastern Europe link ---
        has_ee = await verify_located_at(client, COUNTRY_NAME, EASTERN_EUROPE_NAME)
        print(f"\n'{COUNTRY_NAME}' -> '{EASTERN_EUROPE_NAME}': {'exists' if has_ee else 'MISSING'}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            answer = input(
                f"\nMerge Region '{REGION_NAME}' into Country '{COUNTRY_NAME}'? [y/N] "
            )
            if answer.strip().lower() != "y":
                print("Aborted.")
                return

        # --- Phases 2-7: Redirect relationships ---
        if rel_count > 0:
            redirected = await redirect_relationships(
                client, region.internal_id, country.internal_id, country.name,
            )
            print(f"Phases 2-7: Redirected {redirected} relationship connection(s).")

        # --- Phase 8: Denormalized refs ---
        if rel_types:
            ref_updated = await redirect_denormalized_refs(
                client, region.internal_id, country.internal_id, rel_types,
            )
            print(f"Phase 8:    Updated {ref_updated} denormalized ref(s).")

        # --- Phase 9: Archive ---
        await archive_entity(client, region)
        print(f"Phase 9:    Archived Region '{REGION_NAME}' to deleted_objects.")

        # --- Phase 10: Delete ---
        deleted = await delete_entity(client, region)
        status = "deleted" if deleted else "not found (already gone?)"
        print(f"Phase 10:   Region '{REGION_NAME}' {status}.")

        # --- Add alias ---
        await add_alias(client, country, REGION_NAME)
        print(f"Alias:      Added '{REGION_NAME}' to '{COUNTRY_NAME}' aliases.")

        print(f"\nDone. Region '{REGION_NAME}' merged into Country '{COUNTRY_NAME}'.")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge Region 'Russia' into Country 'Russian Federation'.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview only, no changes.",
    )
    parser.add_argument(
        "--force", action="store_true", help="Skip confirmation prompt.",
    )
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, force=args.force))


if __name__ == "__main__":
    main()
