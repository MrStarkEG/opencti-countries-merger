"""Merge 'North America' region into 'Northern America' (UN M49 021).

Finds both region entities in ES, redirects all relationships and
denormalized ref arrays from the non-standard 'North America' to the
canonical 'Northern America', archives, then deletes the stale entity.

Mirrors the full 10-phase merge logic from MergerService.

Usage:
    uv run python merge_north_america.py --dry-run   # preview only
    uv run python merge_north_america.py              # apply (with confirmation)
    uv run python merge_north_america.py --force      # apply without confirmation
"""

import argparse
import asyncio
from dataclasses import dataclass

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.es import queries


CANONICAL_NAME = "Northern America"
ALIAS_NAME = "North America"

# Matches _PHASE_REL_INDICES in merger.py (phase, index, inferred_only).
RELATIONSHIP_PHASES: list[tuple[str, bool]] = [
    ("stix_core_relationships-*", False),
    ("stix_cyber_observable_relationships-*", False),
    ("stix_core_relationships-*", True),       # inferred only
    ("stix_meta_relationships-*", False),
    ("internal_relationships-*", False),        # no stix_ prefix
    ("stix_sighting_relationships-*", False),
]

# Indices scanned for relationship_type discovery (Phase 8).
PHASE8_DISCOVERY_INDICES = [
    "stix_core_relationships-*",
    "stix_cyber_observable_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "stix_sighting_relationships-*",
    "inferred_relationships-*",
    "pir_relationships-*",
]

# Indices where denormalized ref arrays are updated (Phase 8).
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
class RegionHit:
    """Minimal region entity from ES."""

    internal_id: str
    index: str
    name: str
    source: dict


async def find_region(client: ESClient, name: str) -> RegionHit | None:
    """Search for a Region entity by exact name."""
    sdo_index = f"{client.prefix}stix_domain_objects-*"
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"entity_type.keyword": "Region"}},
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
    return RegionHit(
        internal_id=src["internal_id"],
        index=hits[0]["_index"],
        name=src.get("name", ""),
        source=src,
    )


async def count_relationships(client: ESClient, entity_id: str) -> int:
    """Count relationships referencing *entity_id* across all rel indices."""
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
    """Update denormalized ref arrays on entities (Phase 8)."""
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


async def archive_entity(client: ESClient, region: RegionHit) -> None:
    """Archive the source entity to deleted_objects (Phase 9)."""
    archive_index = f"{client.prefix}deleted_objects"
    await client.index_document(archive_index, region.internal_id, region.source)


async def delete_entity(client: ESClient, region: RegionHit) -> bool:
    """Delete the source region entity (Phase 10)."""
    result = await client.delete_document(region.index, region.internal_id)
    return result is not None


async def run(dry_run: bool, force: bool) -> None:
    """Main execution flow."""
    settings = Settings()
    client = ESClient(settings)

    try:
        await client.health_check()
        print("Connected to Elasticsearch.\n")

        # --- Locate both regions ---
        canonical = await find_region(client, CANONICAL_NAME)
        alias = await find_region(client, ALIAS_NAME)

        if canonical is None:
            print(f"'{CANONICAL_NAME}' not found in ES. Nothing to merge into.")
            return
        print(f"Found canonical: '{canonical.name}' ({canonical.internal_id})")

        if alias is None:
            print(f"'{ALIAS_NAME}' not found in ES. Nothing to merge.")
            return
        print(f"Found alias:     '{alias.name}' ({alias.internal_id})")

        # --- Count relationships ---
        rel_count = await count_relationships(client, alias.internal_id)
        print(f"\n'{ALIAS_NAME}' has {rel_count} relationship(s) to redirect.")

        # --- Discover denormalized ref types ---
        rel_types = await discover_rel_types(client, alias.internal_id)
        if rel_types:
            print(f"Denormalized ref types found: {', '.join(sorted(rel_types))}")
        else:
            print("No denormalized ref types found.")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            answer = input(
                f"\nMerge '{ALIAS_NAME}' into '{CANONICAL_NAME}'? [y/N] "
            )
            if answer.strip().lower() != "y":
                print("Aborted.")
                return

        # --- Phases 2-7: Redirect relationships ---
        if rel_count > 0:
            redirected = await redirect_relationships(
                client, alias.internal_id, canonical.internal_id, canonical.name,
            )
            print(f"Phases 2-7: Redirected {redirected} relationship connection(s).")

        # --- Phase 8: Update denormalized refs ---
        if rel_types:
            ref_updated = await redirect_denormalized_refs(
                client, alias.internal_id, canonical.internal_id, rel_types,
            )
            print(f"Phase 8:    Updated {ref_updated} denormalized ref(s).")

        # --- Phase 9: Archive ---
        await archive_entity(client, alias)
        print(f"Phase 9:    Archived '{ALIAS_NAME}' to deleted_objects.")

        # --- Phase 10: Delete ---
        deleted = await delete_entity(client, alias)
        status = "deleted" if deleted else "not found (already gone?)"
        print(f"Phase 10:   '{ALIAS_NAME}' entity {status}.")

        print(f"\nDone. '{ALIAS_NAME}' merged into '{CANONICAL_NAME}'.")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge 'North America' region into 'Northern America'.",
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
