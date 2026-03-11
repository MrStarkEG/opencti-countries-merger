"""Standalone script to deduplicate country relationships in OpenCTI ES.

For each (entity, country, relationship_type) combination that appears more
than once, keeps the oldest relationship and deletes the duplicates.

SAFETY: Every duplicate is archived to deleted_objects BEFORE deletion,
so they can be recovered if anything goes wrong.

Only relationship documents are touched -- IOCs, entities, and other data
are NEVER modified or deleted.

Usage:
    uv run python fix_duplicate_rels.py --dry-run   # preview only
    uv run python fix_duplicate_rels.py              # apply fixes (with confirmation)
    uv run python fix_duplicate_rels.py --force      # apply without confirmation
"""

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


# Relationship indices to scan for duplicates.
SCAN_INDICES = [
    "stix_core_relationships-*",
    "stix_meta_relationships-*",
    "inferred_relationships-*",
]

BATCH_SIZE = 500

# Global for display helper
prefix_global = ""


@dataclass
class RelHit:
    """A single relationship document."""

    rel_id: str
    index: str
    created_at: str
    source: dict = field(default_factory=dict)


@dataclass
class DuplicateGroup:
    """A group of duplicate relationships sharing the same key."""

    entity_id: str
    entity_name: str
    country_id: str
    country_name: str
    rel_type: str
    keep: RelHit
    duplicates: list[RelHit] = field(default_factory=list)


@dataclass
class DedupStats:
    """Aggregated dedup statistics."""

    total_groups: int = 0
    total_to_delete: int = 0
    by_rel_type: dict[str, int] = field(default_factory=dict)
    by_index: dict[str, int] = field(default_factory=dict)


# ── Scanning ──


async def scan_duplicates(client: ESClient, prefix: str) -> list[DuplicateGroup]:
    """Scan relationship indices and find duplicate country relationships."""
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    total_scanned = 0

    for index_suffix in SCAN_INDICES:
        index = f"{prefix}{index_suffix}"
        body = _country_nested_query()

        count = await client.count(index, body)
        print(f"  Scanning {index_suffix}: {count} relationships...")

        async for hit in client.scroll_all(index, body):
            total_scanned += 1
            src = hit["_source"]
            conns = src.get("connections", [])
            rel_type = src.get("relationship_type", "?")
            created_at = src.get("created_at", src.get("created", "9999"))

            country_conns = _filter_country_conns(conns, is_country=True)
            non_country_conns = _filter_country_conns(conns, is_country=False)

            for nc in non_country_conns:
                for cc in country_conns:
                    key = (nc["internal_id"], cc["internal_id"], rel_type)
                    groups[key].append({
                        "rel_id": src.get("internal_id", hit["_id"]),
                        "index": hit["_index"],
                        "created_at": created_at,
                        "entity_name": nc.get("name", "?"),
                        "country_name": cc.get("name", "?"),
                        "source": src,
                    })

    print(f"  Total relationships scanned: {total_scanned}")
    return _build_groups(groups)


def _country_nested_query() -> dict:
    """Nested query to find relationships with Country connections."""
    return {
        "query": {
            "nested": {
                "path": "connections",
                "query": {
                    "term": {"connections.types.keyword": "Country"}
                },
            }
        }
    }


def _filter_country_conns(conns: list[dict], *, is_country: bool) -> list[dict]:
    """Filter connections by whether they are Country type."""
    if is_country:
        return [c for c in conns if "Country" in c.get("types", [])]
    return [c for c in conns if "Country" not in c.get("types", [])]


def _build_groups(
    raw: dict[tuple[str, str, str], list[dict]],
) -> list[DuplicateGroup]:
    """Build DuplicateGroup objects for groups with 2+ entries."""
    result: list[DuplicateGroup] = []
    for (eid, cid, rtype), hits in raw.items():
        if len(hits) < 2:
            continue

        sorted_hits = sorted(hits, key=lambda h: h["created_at"])
        keeper = sorted_hits[0]
        dups = sorted_hits[1:]

        group = DuplicateGroup(
            entity_id=eid,
            entity_name=keeper["entity_name"],
            country_id=cid,
            country_name=keeper["country_name"],
            rel_type=rtype,
            keep=RelHit(
                rel_id=keeper["rel_id"],
                index=keeper["index"],
                created_at=keeper["created_at"],
            ),
            duplicates=[
                RelHit(
                    rel_id=d["rel_id"],
                    index=d["index"],
                    created_at=d["created_at"],
                    source=d["source"],
                )
                for d in dups
            ],
        )
        result.append(group)

    return result


# ── Stats & Display ──


def compute_stats(groups: list[DuplicateGroup]) -> DedupStats:
    """Compute statistics from duplicate groups."""
    stats = DedupStats(total_groups=len(groups))
    for g in groups:
        extra = len(g.duplicates)
        stats.total_to_delete += extra
        stats.by_rel_type[g.rel_type] = (
            stats.by_rel_type.get(g.rel_type, 0) + extra
        )
        for d in g.duplicates:
            idx_short = (
                d.index.split(prefix_global)[-1] if prefix_global else d.index
            )
            stats.by_index[idx_short] = stats.by_index.get(idx_short, 0) + 1
    return stats


def display_plan(groups: list[DuplicateGroup], stats: DedupStats) -> None:
    """Display the dedup plan to the user."""
    print(f"\n{'=' * 70}")
    print("DEDUP PLAN")
    print(f"{'=' * 70}")
    print(f"  Duplicate groups found: {stats.total_groups}")
    print(f"  Relationships to delete: {stats.total_to_delete}")
    print(f"  (Each group keeps 1 oldest, deletes the rest)")

    print(f"\n  By relationship type:")
    for rtype, count in sorted(stats.by_rel_type.items()):
        print(f"    {rtype}: {count}")

    print(f"\n  By index:")
    for idx, count in sorted(stats.by_index.items()):
        print(f"    {idx}: {count}")

    sorted_groups = sorted(
        groups, key=lambda g: len(g.duplicates), reverse=True
    )
    print(f"\n  Top 20 worst duplicate groups:")
    for i, g in enumerate(sorted_groups[:20]):
        total = len(g.duplicates) + 1
        print(
            f"    [{i+1}] {g.entity_name} -> {g.country_name} "
            f"({g.rel_type}) x{total} "
            f"(deleting {len(g.duplicates)}, keeping oldest)"
        )
    if len(sorted_groups) > 20:
        print(f"    ... and {len(sorted_groups) - 20} more groups")


# ── Archive (Safety) ──


async def archive_duplicates(
    client: ESClient,
    prefix: str,
    groups: list[DuplicateGroup],
    dry_run: bool,
) -> int:
    """Archive every duplicate to deleted_objects BEFORE deletion.

    This ensures full recoverability.
    """
    archive_index = f"{prefix}deleted_objects"
    archived = 0

    for g in groups:
        for d in g.duplicates:
            if dry_run:
                archived += 1
                continue
            try:
                await client.index_document(archive_index, d.rel_id, d.source)
                archived += 1
            except Exception as e:
                print(f"  WARNING: Failed to archive {d.rel_id[:12]}...: {e}")

    return archived


# ── Delete ──


async def execute_delete(
    client: ESClient,
    groups: list[DuplicateGroup],
    dry_run: bool,
) -> dict[str, int]:
    """Delete duplicate relationship documents."""
    by_index: dict[str, list[str]] = defaultdict(list)
    for g in groups:
        for d in g.duplicates:
            by_index[d.index].append(d.rel_id)

    total_deleted = 0
    total_failed = 0

    for index, rel_ids in by_index.items():
        for chunk_start in range(0, len(rel_ids), BATCH_SIZE):
            chunk = rel_ids[chunk_start : chunk_start + BATCH_SIZE]
            batch_num = chunk_start // BATCH_SIZE + 1
            body = {"query": {"terms": {"internal_id.keyword": chunk}}}

            if dry_run:
                count = await client.count(index, body)
                total_deleted += count
                print(
                    f"  [DRY RUN] Would delete {count} rels "
                    f"from {index} (batch {batch_num})"
                )
            else:
                resp = await client.delete_by_query(index, body)
                deleted = resp.get("deleted", 0)
                failed = resp.get("failures", 0)
                total_deleted += deleted
                total_failed += failed
                print(f"  Deleted {deleted} rels from {index} (batch {batch_num})")
                if failed:
                    print(f"    WARNING: {failed} failures in this batch")

    return {"deleted": total_deleted, "failed": total_failed}


# ── Denorm Cleanup ──


async def clean_denorm_refs(
    client: ESClient,
    prefix: str,
    groups: list[DuplicateGroup],
    dry_run: bool,
) -> int:
    """Deduplicate repeated entries in denormalized ref arrays.

    Only touches rel_TYPE.internal_id arrays -- never modifies entity
    names, aliases, or any other entity data.
    """
    all_pairs: set[tuple[str, str]] = set()
    for g in groups:
        all_pairs.add((g.entity_id, g.rel_type))
        all_pairs.add((g.country_id, g.rel_type))

    if not all_pairs:
        return 0

    by_rel_type: dict[str, list[str]] = defaultdict(list)
    for entity_id, rel_type in all_pairs:
        by_rel_type[rel_type].append(entity_id)

    entity_indices = [
        f"{prefix}stix_domain_objects-*",
        f"{prefix}stix_cyber_observables-*",
    ]

    total_updated = 0

    for rel_type, entity_ids in by_rel_type.items():
        field_name = f"rel_{rel_type}.internal_id"
        script = _dedup_array_script(field_name)

        for chunk_start in range(0, len(entity_ids), BATCH_SIZE):
            chunk = entity_ids[chunk_start : chunk_start + BATCH_SIZE]
            body = {
                "query": {"terms": {"internal_id.keyword": chunk}},
                "script": {"source": script},
            }

            for idx in entity_indices:
                if dry_run:
                    count = await client.count(idx, body)
                    if count > 0:
                        print(
                            f"  [DRY RUN] Would dedup {field_name} "
                            f"on {count} entities in {idx}"
                        )
                        total_updated += count
                else:
                    try:
                        resp = await client.update_by_query(idx, body)
                        updated = resp.get("updated", 0)
                        if updated > 0:
                            print(
                                f"  Deduped {field_name} "
                                f"on {updated} entities in {idx}"
                            )
                            total_updated += updated
                    except Exception as e:
                        print(
                            f"  WARNING: Failed dedup {field_name} "
                            f"in {idx}: {e}"
                        )

    return total_updated


def _dedup_array_script(field_name: str) -> str:
    """Painless script that removes duplicate values from an array field."""
    return (
        f"def field = ctx._source['{field_name}'];"
        " if (field != null && field instanceof List) {"
        "   def seen = new HashSet();"
        "   def unique = new ArrayList();"
        "   for (def val : field) {"
        "     if (seen.add(val)) { unique.add(val); }"
        "   }"
        "   if (unique.size() != field.size()) {"
        f"    ctx._source['{field_name}'] = unique;"
        "   } else { ctx.op = 'noop'; }"
        " } else { ctx.op = 'noop'; }"
    )


# ── Main ──


async def main(dry_run: bool, force: bool) -> None:
    """Main entry point for the dedup fix."""
    global prefix_global

    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix
    prefix_global = prefix

    try:
        health = await client.health_check()
        print(f"ES cluster: {health['cluster_name']} - {health['status']}")
    except Exception as e:
        print(f"Cannot connect to ES: {e}")
        return

    # Phase 1: Scan
    print(f"\n{'=' * 70}")
    print("PHASE 1: SCANNING FOR DUPLICATE RELATIONSHIPS")
    print(f"{'=' * 70}")

    groups = await scan_duplicates(client, prefix)

    if not groups:
        print("\nNo duplicates found. Everything is clean!")
        await client.close()
        return

    stats = compute_stats(groups)
    display_plan(groups, stats)

    # Confirm
    if dry_run:
        print(f"\n{'=' * 70}")
        print("DRY RUN -- no changes will be made")
        print(f"{'=' * 70}")
    elif not force:
        print(f"\nSAFETY SUMMARY:")
        print(f"  - {stats.total_to_delete} duplicate RELATIONSHIP docs will be deleted")
        print(f"  - Each duplicate is archived to deleted_objects FIRST (recoverable)")
        print(f"  - IOCs, entities, countries = NEVER touched or deleted")
        print(f"  - Only redundant edges (same entity->same country) are removed")
        response = input("\nProceed? [y/N]: ")
        if response.lower() != "y":
            print("Aborted.")
            await client.close()
            return

    # Phase 2: Archive duplicates (safety net)
    print(f"\n{'=' * 70}")
    print("PHASE 2: ARCHIVING DUPLICATES TO deleted_objects (SAFETY NET)")
    print(f"{'=' * 70}")

    archived = await archive_duplicates(client, prefix, groups, dry_run)
    label = "[DRY RUN] Would archive" if dry_run else "Archived"
    print(f"  {label} {archived} relationship documents")

    # Phase 3: Delete duplicates
    print(f"\n{'=' * 70}")
    print("PHASE 3: DELETING DUPLICATE RELATIONSHIPS")
    print(f"{'=' * 70}")

    result = await execute_delete(client, groups, dry_run)
    print(f"\n  Total deleted: {result['deleted']}")
    if result.get("failed", 0):
        print(f"  Total failed: {result['failed']}")

    # Phase 4: Clean denormalized refs
    print(f"\n{'=' * 70}")
    print("PHASE 4: CLEANING DENORMALIZED REF ARRAYS")
    print(f"{'=' * 70}")

    updated = await clean_denorm_refs(client, prefix, groups, dry_run)
    print(f"\n  Denorm refs cleaned: {updated}")

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    mode = "DRY RUN" if dry_run else "LIVE"
    print(f"  Mode: {mode}")
    print(f"  Duplicate groups: {stats.total_groups}")
    print(f"  Archived to deleted_objects: {archived}")
    print(f"  Relationships deleted: {result['deleted']}")
    print(f"  Denorm refs cleaned: {updated}")

    if not dry_run:
        print("\n  Remember to flush Redis cache and restart OpenCTI!")

    await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deduplicate country relationships in OpenCTI ES"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only, make no changes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run, force=args.force))
