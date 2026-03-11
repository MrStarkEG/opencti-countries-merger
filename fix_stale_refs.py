"""Fix stale country references left after merging.

Two problems to fix:
1. Inferred relationships with connections pointing to old (merged) country IDs
2. Denormalized ref arrays on entities containing old country IDs

Approach: map each deleted country ID to its current merged-into target,
then redirect all references.

Usage:
    uv run python fix_stale_refs.py --dry-run   # preview only
    uv run python fix_stale_refs.py              # apply (with confirmation)
    uv run python fix_stale_refs.py --force      # apply without confirmation
"""

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.services.country_mapper import CountryMapper


BATCH_SIZE = 500

# Manual overrides for deleted country IDs that auto-resolution can't match.
# Discovered via diagnose_unmapped.py. Format: old_id -> (new_id, new_name)
MANUAL_MAPPINGS: dict[str, tuple[str, str]] = {
    # Netherlands Antilles (old code AN) -> Netherlands
    "08068ab0-c0cf-4595-9151-da1d9f952cf7": (
        "4e073b9b-79dc-4668-9573-c0bee353d3b9", "Netherlands"
    ),
    # Iran, Islamic Republic of -> Iran
    "0ddd2401-c1a9-4e12-938d-de6744ea3428": (
        "fc5ebd36-db64-42d3-b747-8a5af067c315", "Iran"
    ),
    # United States of America (old dup) -> US
    "28402d89-4416-4291-9ae5-31369db2243f": (
        "13f745d8-95af-4978-a6d3-262ba6b834e0", "United States of America"
    ),
    # Iran (old dup) -> Iran
    "38116f95-8189-4697-9a3e-fe9dc8e03785": (
        "fc5ebd36-db64-42d3-b747-8a5af067c315", "Iran"
    ),
    # Germany (old dup) -> Germany
    "6807d047-8eca-4c57-9d92-89b1aa4f1523": (
        "53d122e5-c68a-464d-b5fe-95709063deb9", "Germany"
    ),
    # South Korea (old dup) -> Korea, Republic of
    "6f086273-dc04-4f1c-ab43-d369fe136435": (
        "4dad0306-ead1-4ada-ab44-6f32fbd09d7e", "Korea, Republic of"
    ),
    # Egypt (old dup) -> Egypt
    "a9ba7d1a-0346-4981-8957-d04d54ff828e": (
        "4326d727-5c68-40f3-9746-08ea89e06e68", "Egypt"
    ),
    # Canada (old dup) -> Canada
    "b142aaef-183d-445c-ae27-735387ff472d": (
        "d3c6f763-951d-4dd2-ad8e-d9a7df2be68e", "Canada"
    ),
    # United States (old dup) -> US
    "f4335cdf-7428-432c-8ccc-2839da6ae548": (
        "13f745d8-95af-4978-a6d3-262ba6b834e0", "United States of America"
    ),
    # Madagascar (old dup) -> Madagascar
    "f77607a9-4bfc-4ec4-926a-b253fb002b48": (
        "dfddfd83-444f-47e4-a9a1-6847bd8c2f9f", "Madagascar"
    ),
    # Netherlands (old dup) -> Netherlands
    "f947d768-a3bc-49b9-abfd-b383b6fe72a1": (
        "4e073b9b-79dc-4668-9573-c0bee353d3b9", "Netherlands"
    ),
    # Korea, Republic of (old dup) -> Korea, Republic of
    "f970bbd9-1c96-4e16-8656-ccc37e5f68fe": (
        "4dad0306-ead1-4ada-ab44-6f32fbd09d7e", "Korea, Republic of"
    ),
}

# Old IDs that are junk (not real countries). Their denorm refs will be
# removed entirely rather than redirected.
JUNK_OLD_IDS: set[str] = {
    "33d2ea04-1b4d-43f1-9d7f-b749b784960f",  # QC (Quebec)
    "e3a7109a-a455-4418-897b-c9551fd15a30",  # EU (European Union)
    "959bffa5-4d92-4b22-8c00-b5bce674ce50",  # ?? (unknown junk)
}

# Indices that may contain stale country connections
RELATIONSHIP_INDICES = [
    "stix_core_relationships-*",
    "stix_meta_relationships-*",
    "inferred_relationships-*",
    "stix_sighting_relationships-*",
    "internal_relationships-*",
]

# Denormalized ref fields to check on entities
DENORM_REL_FIELDS = [
    "rel_targets",
    "rel_located-at",
    "rel_object",
    "rel_object-marking",
    "rel_created-by",
    "rel_uses",
    "rel_indicates",
    "rel_based-on",
]

# Entity indices containing denormalized refs
ENTITY_INDICES = [
    "stix_domain_objects-*",
    "stix_cyber_observables-*",
    "stix_core_relationships-*",
    "stix_meta_relationships-*",
    "internal_relationships-*",
    "inferred_relationships-*",
]


@dataclass
class IdMapping:
    """Maps a deleted country ID to its current replacement."""

    old_id: str
    old_name: str
    new_id: str
    new_name: str


@dataclass
class StaleRelHit:
    """A relationship with a stale country reference."""

    rel_id: str
    rel_index: str
    rel_type: str
    old_country_id: str
    old_country_name: str


@dataclass
class FixPlan:
    """Plan for fixing stale references."""

    mappings: list[IdMapping] = field(default_factory=list)
    unmapped: list[tuple[str, str]] = field(default_factory=list)
    stale_rels: list[StaleRelHit] = field(default_factory=list)
    denorm_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class FixResult:
    """Results after applying fixes."""

    rels_redirected: int = 0
    rels_failed: int = 0
    denorm_updated: int = 0
    denorm_failed: int = 0
    errors: list[str] = field(default_factory=list)


# ── Build ID Mapping ──


async def build_id_mapping(
    client: ESClient, prefix: str, mapper: CountryMapper
) -> tuple[list[IdMapping], list[tuple[str, str]]]:
    """Map each deleted country ID to its current country ID."""
    sdo_index = f"{prefix}stix_domain_objects-*"
    deleted_index = f"{prefix}deleted_objects"

    # Fetch current countries: build lookup by name and alias
    current_by_name: dict[str, tuple[str, str]] = {}
    current_by_alias: dict[str, tuple[str, str]] = {}

    body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    async for hit in client.scroll_all(sdo_index, body):
        src = hit["_source"]
        iid = src["internal_id"]
        name = src.get("name", "")
        current_by_name[name.lower()] = (iid, name)
        for alias in src.get("x_opencti_aliases") or []:
            current_by_alias[alias.lower()] = (iid, name)

    # Fetch deleted countries
    deleted_body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    deleted_countries: list[dict] = []
    async for hit in client.scroll_all(deleted_index, deleted_body):
        src = hit["_source"]
        deleted_countries.append(src)

    mappings: list[IdMapping] = []
    unmapped: list[tuple[str, str]] = []

    for dc in deleted_countries:
        old_id = dc["internal_id"]
        old_name = dc.get("name", "?")
        aliases = dc.get("x_opencti_aliases") or []

        # Check manual overrides first
        if old_id in MANUAL_MAPPINGS:
            new_id, new_name = MANUAL_MAPPINGS[old_id]
            mappings.append(IdMapping(
                old_id=old_id,
                old_name=old_name,
                new_id=new_id,
                new_name=new_name,
            ))
            continue

        # Skip known junk
        if old_id in JUNK_OLD_IDS:
            continue

        target = _resolve_target(
            old_name, aliases, current_by_name, current_by_alias, mapper
        )

        if target:
            mappings.append(IdMapping(
                old_id=old_id,
                old_name=old_name,
                new_id=target[0],
                new_name=target[1],
            ))
        else:
            unmapped.append((old_id, old_name))

    return mappings, unmapped


def _resolve_target(
    name: str,
    aliases: list[str],
    by_name: dict[str, tuple[str, str]],
    by_alias: dict[str, tuple[str, str]],
    mapper: CountryMapper,
) -> tuple[str, str] | None:
    """Try to find the current country for a deleted one."""
    # 1. Exact name match
    if name.lower() in by_name:
        return by_name[name.lower()]

    # 2. Check aliases of the deleted country against current names/aliases
    for alias in aliases:
        if alias.lower() in by_alias:
            return by_alias[alias.lower()]
        if alias.lower() in by_name:
            return by_name[alias.lower()]

    # 3. Use the CountryMapper (fuzzy matching + custom aliases)
    resolved = mapper.resolve(name)
    if resolved:
        # resolved is alpha-2 code, look it up in current aliases
        if resolved.lower() in by_alias:
            return by_alias[resolved.lower()]

    # 4. Try resolving from aliases
    for alias in aliases:
        resolved = mapper.resolve(alias)
        if resolved and resolved.lower() in by_alias:
            return by_alias[resolved.lower()]

    return None


# ── Scan Stale Relationships ──


async def scan_stale_rels(
    client: ESClient,
    prefix: str,
    old_ids: set[str],
) -> list[StaleRelHit]:
    """Find relationships with connections pointing to old country IDs."""
    stale: list[StaleRelHit] = []

    for index_suffix in RELATIONSHIP_INDICES:
        index = f"{prefix}{index_suffix}"
        body = {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {
                        "term": {"connections.types.keyword": "Country"}
                    },
                }
            }
        }

        count = await client.count(index, body)
        if count == 0:
            continue

        found = 0
        async for hit in client.scroll_all(index, body):
            src = hit["_source"]
            conns = src.get("connections", [])
            for c in conns:
                if "Country" not in c.get("types", []):
                    continue
                if c["internal_id"] in old_ids:
                    stale.append(StaleRelHit(
                        rel_id=src.get("internal_id", hit["_id"]),
                        rel_index=hit["_index"],
                        rel_type=src.get("relationship_type", "?"),
                        old_country_id=c["internal_id"],
                        old_country_name=c.get("name", "?"),
                    ))
                    found += 1

        if found > 0:
            print(f"    {index_suffix}: {found} stale refs")

    return stale


# ── Scan Denorm Refs ──


async def scan_denorm_refs(
    client: ESClient,
    prefix: str,
    old_ids: list[str],
) -> dict[str, int]:
    """Count entities with stale country IDs in denormalized ref arrays."""
    counts: dict[str, int] = {}

    for rel_field in DENORM_REL_FIELDS:
        field_name = f"{rel_field}.internal_id"
        total = 0
        for chunk_start in range(0, len(old_ids), BATCH_SIZE):
            chunk = old_ids[chunk_start : chunk_start + BATCH_SIZE]
            body = {"query": {"terms": {f"{field_name}.keyword": chunk}}}
            for idx_suffix in ENTITY_INDICES:
                idx = f"{prefix}{idx_suffix}"
                try:
                    count = await client.count(idx, body)
                    total += count
                except Exception:
                    pass

        if total > 0:
            counts[field_name] = total

    return counts


# ── Fix Stale Relationships ──


async def fix_stale_relationships(
    client: ESClient,
    prefix: str,
    stale_rels: list[StaleRelHit],
    mapping: dict[str, IdMapping],
    dry_run: bool,
) -> tuple[int, int, list[str]]:
    """Redirect stale country connections in relationships."""
    # Group by (old_country_id, rel_index) for batch updates
    by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    for sr in stale_rels:
        if sr.old_country_id in mapping:
            by_key[(sr.old_country_id, sr.rel_index)].append(sr.rel_id)

    redirected = 0
    failed = 0
    errors: list[str] = []

    for (old_id, index), rel_ids in by_key.items():
        m = mapping[old_id]
        body = {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {
                        "term": {"connections.internal_id.keyword": old_id}
                    },
                }
            },
            "script": {
                "source": (
                    "for (conn in ctx._source.connections) {"
                    " if (conn.internal_id == params.old_id) {"
                    " conn.internal_id = params.new_id;"
                    " conn.name = params.new_name;"
                    " }"
                    " }"
                ),
                "params": {
                    "old_id": old_id,
                    "new_id": m.new_id,
                    "new_name": m.new_name,
                },
            },
        }

        if dry_run:
            count = await client.count(index, body)
            redirected += count
            if count > 0:
                print(
                    f"  [DRY RUN] Would redirect {count} rels in {index}: "
                    f"{m.old_name} -> {m.new_name}"
                )
        else:
            try:
                resp = await client.update_by_query(index, body)
                updated = resp.get("updated", 0)
                redirected += updated
                if updated > 0:
                    print(
                        f"  Redirected {updated} rels in {index}: "
                        f"{m.old_name} -> {m.new_name}"
                    )
            except Exception as e:
                failed += len(rel_ids)
                errors.append(f"Failed redirect {old_id[:12]}... in {index}: {e}")

    return redirected, failed, errors


# ── Fix Denorm Refs ──


async def fix_denorm_refs(
    client: ESClient,
    prefix: str,
    mapping: dict[str, IdMapping],
    denorm_counts: dict[str, int],
    dry_run: bool,
) -> tuple[int, int, list[str]]:
    """Replace old country IDs with new ones in denormalized ref arrays."""
    old_ids = list(mapping.keys())
    updated_total = 0
    failed_total = 0
    errors: list[str] = []

    for field_name in denorm_counts:
        # For each old_id, replace with new_id in the array
        for old_id, m in mapping.items():
            script = (
                f"def field = ctx._source['{field_name}'];"
                " if (field != null && field instanceof List) {"
                "   boolean changed = false;"
                "   for (int i = 0; i < field.size(); i++) {"
                "     if (field[i] == params.old_id) {"
                "       field[i] = params.new_id;"
                "       changed = true;"
                "     }"
                "   }"
                "   if (!changed) { ctx.op = 'noop'; }"
                " } else { ctx.op = 'noop'; }"
            )

            body = {
                "query": {"term": {f"{field_name}.keyword": old_id}},
                "script": {
                    "source": script,
                    "params": {
                        "old_id": old_id,
                        "new_id": m.new_id,
                    },
                },
            }

            for idx_suffix in ENTITY_INDICES:
                idx = f"{prefix}{idx_suffix}"
                try:
                    if dry_run:
                        count = await client.count(idx, body)
                        if count > 0:
                            updated_total += count
                    else:
                        resp = await client.update_by_query(idx, body)
                        upd = resp.get("updated", 0)
                        updated_total += upd
                except Exception as e:
                    failed_total += 1
                    errors.append(
                        f"Failed denorm fix {field_name} "
                        f"{old_id[:12]}... in {idx}: {e}"
                    )

    return updated_total, failed_total, errors


# ── Remove Junk Denorm Refs ──


async def remove_junk_denorm_refs(
    client: ESClient,
    prefix: str,
    dry_run: bool,
) -> int:
    """Remove denormalized refs pointing to junk IDs (QC, EU, etc.).

    These are not real countries, so we remove their IDs from ref arrays
    rather than redirecting them.
    """
    junk_ids = list(JUNK_OLD_IDS)
    if not junk_ids:
        return 0

    total_removed = 0

    for rel_field in DENORM_REL_FIELDS:
        field_name = f"{rel_field}.internal_id"
        for junk_id in junk_ids:
            script = (
                f"def field = ctx._source['{field_name}'];"
                " if (field != null && field instanceof List) {"
                "   def original = field.size();"
                "   field.removeIf(v -> v == params.junk_id);"
                "   if (field.size() == original) { ctx.op = 'noop'; }"
                " } else { ctx.op = 'noop'; }"
            )

            body = {
                "query": {"term": {f"{field_name}.keyword": junk_id}},
                "script": {
                    "source": script,
                    "params": {"junk_id": junk_id},
                },
            }

            for idx_suffix in ENTITY_INDICES:
                idx = f"{prefix}{idx_suffix}"
                try:
                    if dry_run:
                        count = await client.count(idx, body)
                        if count > 0:
                            print(
                                f"  [DRY RUN] Would remove {junk_id[:12]}... "
                                f"from {field_name} on {count} entities in {idx}"
                            )
                            total_removed += count
                    else:
                        resp = await client.update_by_query(idx, body)
                        upd = resp.get("updated", 0)
                        if upd > 0:
                            print(
                                f"  Removed {junk_id[:12]}... "
                                f"from {field_name} on {upd} entities in {idx}"
                            )
                            total_removed += upd
                except Exception as e:
                    print(
                        f"  WARNING: Failed junk removal "
                        f"{junk_id[:12]}... in {idx}: {e}"
                    )

    return total_removed


# ── Display ──


def display_plan(plan: FixPlan) -> None:
    """Display the fix plan."""
    print(f"\n{'=' * 70}")
    print("FIX PLAN")
    print(f"{'=' * 70}")

    print(f"\n  ID Mappings (deleted -> current): {len(plan.mappings)}")
    for m in plan.mappings:
        print(f"    {m.old_name} ({m.old_id[:16]}...) -> {m.new_name} ({m.new_id[:16]}...)")

    if plan.unmapped:
        print(f"\n  UNMAPPED (no current match found): {len(plan.unmapped)}")
        for old_id, old_name in plan.unmapped:
            print(f"    {old_name} ({old_id[:16]}...)")

    print(f"\n  Stale relationship refs to fix: {len(plan.stale_rels)}")
    if plan.stale_rels:
        by_country = defaultdict(int)
        for sr in plan.stale_rels:
            by_country[sr.old_country_name] += 1
        for name, count in sorted(by_country.items(), key=lambda x: -x[1]):
            print(f"    {name}: {count} rels")

    print(f"\n  Denormalized ref arrays to fix:")
    for field_name, count in sorted(plan.denorm_counts.items()):
        print(f"    {field_name}: {count} entities")

    total_denorm = sum(plan.denorm_counts.values())
    print(f"\n  TOTAL: {len(plan.stale_rels)} rels + {total_denorm} denorm entries")


# ── Main ──


async def main(dry_run: bool, force: bool) -> None:
    """Main entry point."""
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix
    mapper = CountryMapper(fuzzy_threshold=80)

    try:
        health = await client.health_check()
        print(f"ES cluster: {health['cluster_name']} - {health['status']}")
    except Exception as e:
        print(f"Cannot connect to ES: {e}")
        return

    # Phase 1: Build mapping
    print(f"\n{'=' * 70}")
    print("PHASE 1: MAPPING DELETED COUNTRIES TO CURRENT ONES")
    print(f"{'=' * 70}")

    mappings, unmapped = await build_id_mapping(client, prefix, mapper)
    mapping_dict = {m.old_id: m for m in mappings}
    old_ids = set(m.old_id for m in mappings)
    print(f"  Mapped: {len(mappings)}")
    print(f"  Unmapped: {len(unmapped)}")
    print(f"  Junk (will remove refs): {len(JUNK_OLD_IDS)}")

    # Phase 2: Scan for stale refs
    print(f"\n{'=' * 70}")
    print("PHASE 2: SCANNING FOR STALE REFERENCES")
    print(f"{'=' * 70}")

    print("  Scanning relationships...")
    stale_rels = await scan_stale_rels(client, prefix, old_ids)
    print(f"  Total stale relationship refs: {len(stale_rels)}")

    print("\n  Scanning denormalized ref arrays...")
    denorm_counts = await scan_denorm_refs(client, prefix, list(old_ids))
    total_denorm = sum(denorm_counts.values())
    print(f"  Total denorm entries to fix: {total_denorm}")

    plan = FixPlan(
        mappings=mappings,
        unmapped=unmapped,
        stale_rels=stale_rels,
        denorm_counts=denorm_counts,
    )

    if not stale_rels and total_denorm == 0:
        print("\nNo stale references found. Everything is clean!")
        await client.close()
        return

    display_plan(plan)

    # Confirm
    if dry_run:
        print(f"\n{'=' * 70}")
        print("DRY RUN -- no changes will be made")
        print(f"{'=' * 70}")
    elif not force:
        print(f"\nThis will:")
        print(f"  - Redirect {len(stale_rels)} relationship connections to current country IDs")
        print(f"  - Fix {total_denorm} denormalized ref entries on entities")
        print(f"  - NO entities are deleted or modified beyond ref arrays")
        response = input("\nProceed? [y/N]: ")
        if response.lower() != "y":
            print("Aborted.")
            await client.close()
            return

    result = FixResult()

    # Phase 3: Fix relationships
    print(f"\n{'=' * 70}")
    print("PHASE 3: REDIRECTING STALE RELATIONSHIP CONNECTIONS")
    print(f"{'=' * 70}")

    r, f, errs = await fix_stale_relationships(
        client, prefix, stale_rels, mapping_dict, dry_run
    )
    result.rels_redirected = r
    result.rels_failed = f
    result.errors.extend(errs)
    print(f"\n  Redirected: {r}, Failed: {f}")

    # Phase 4: Fix denorm refs (redirect old -> new)
    print(f"\n{'=' * 70}")
    print("PHASE 4: FIXING DENORMALIZED REF ARRAYS (redirect)")
    print(f"{'=' * 70}")

    u, f2, errs2 = await fix_denorm_refs(
        client, prefix, mapping_dict, denorm_counts, dry_run
    )
    result.denorm_updated = u
    result.denorm_failed = f2
    result.errors.extend(errs2)
    print(f"\n  Updated: {u}, Failed: {f2}")

    # Phase 5: Remove junk denorm refs
    print(f"\n{'=' * 70}")
    print("PHASE 5: REMOVING JUNK DENORM REFS (QC, EU, etc.)")
    print(f"{'=' * 70}")

    junk_removed = await remove_junk_denorm_refs(client, prefix, dry_run)
    print(f"\n  Junk refs removed: {junk_removed}")

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    mode = "DRY RUN" if dry_run else "LIVE"
    print(f"  Mode: {mode}")
    print(f"  Relationships redirected: {result.rels_redirected}")
    print(f"  Denorm refs updated: {result.denorm_updated}")
    print(f"  Junk refs removed: {junk_removed}")
    if result.errors:
        print(f"  Errors: {len(result.errors)}")
        for e in result.errors[:10]:
            print(f"    {e}")

    if not dry_run:
        print("\n  Remember to flush Redis cache and restart OpenCTI!")

    await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fix stale country references after merge"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview only, make no changes",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Skip confirmation prompt",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run, force=args.force))
