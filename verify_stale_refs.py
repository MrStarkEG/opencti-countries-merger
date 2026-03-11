"""Verify that fix_stale_refs.py worked: check for remaining stale country refs.

Compares deleted country IDs against current relationship connections and
denormalized ref arrays to see if any stale pointers remain.
"""

import asyncio
from collections import defaultdict

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


RELATIONSHIP_INDICES = [
    "stix_core_relationships-*",
    "stix_meta_relationships-*",
    "inferred_relationships-*",
    "stix_sighting_relationships-*",
    "internal_relationships-*",
]

DENORM_FIELDS = [
    "rel_targets.internal_id",
    "rel_located-at.internal_id",
    "rel_object.internal_id",
    "rel_object-marking.internal_id",
    "rel_created-by.internal_id",
    "rel_uses.internal_id",
    "rel_indicates.internal_id",
    "rel_based-on.internal_id",
]

ENTITY_INDICES = [
    "stix_domain_objects-*",
    "stix_cyber_observables-*",
]

BATCH_SIZE = 500


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix

    # Step 1: Get all current country IDs
    print("Fetching current country IDs...")
    sdo_index = f"{prefix}stix_domain_objects-*"
    current_ids: set[str] = set()
    body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    async for hit in client.scroll_all(sdo_index, body):
        current_ids.add(hit["_source"]["internal_id"])
    print(f"  Current countries: {len(current_ids)}")

    # Step 2: Get all deleted country IDs
    print("Fetching deleted country IDs...")
    deleted_index = f"{prefix}deleted_objects"
    deleted_ids: set[str] = set()
    deleted_names: dict[str, str] = {}
    del_body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    async for hit in client.scroll_all(deleted_index, del_body):
        src = hit["_source"]
        did = src["internal_id"]
        deleted_ids.add(did)
        deleted_names[did] = src.get("name", "?")
    print(f"  Deleted countries: {len(deleted_ids)}")

    # Step 3: Check relationships for stale country connections
    print("\nChecking relationships for stale country connections...")
    stale_rel_counts: dict[str, list[str]] = defaultdict(list)

    for suffix in RELATIONSHIP_INDICES:
        index = f"{prefix}{suffix}"
        query = {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {"term": {"connections.types.keyword": "Country"}},
                }
            }
        }
        try:
            async for hit in client.scroll_all(index, query):
                src = hit["_source"]
                conns = src.get("connections", [])
                for c in conns:
                    if "Country" not in c.get("types", []):
                        continue
                    cid = c["internal_id"]
                    if cid in deleted_ids and cid not in current_ids:
                        name = c.get("name", deleted_names.get(cid, "?"))
                        stale_rel_counts[name].append(src.get("internal_id", "?"))
        except Exception as e:
            print(f"  Error scanning {suffix}: {e}")

    if stale_rel_counts:
        total = sum(len(v) for v in stale_rel_counts.values())
        print(f"  STALE REFS REMAINING: {total}")
        for name, ids in sorted(stale_rel_counts.items(), key=lambda x: -len(x[1])):
            print(f"    {name}: {len(ids)} refs")
    else:
        print("  CLEAN - No stale country refs in relationships")

    # Step 4: Check denorm arrays for stale country IDs
    print("\nChecking denorm ref arrays for stale country IDs...")
    deleted_list = list(deleted_ids - current_ids)
    stale_denorm: dict[str, int] = {}

    for field in DENORM_FIELDS:
        total = 0
        for i in range(0, len(deleted_list), BATCH_SIZE):
            chunk = deleted_list[i : i + BATCH_SIZE]
            query = {"query": {"terms": {f"{field}.keyword": chunk}}}
            for idx_suffix in ENTITY_INDICES:
                idx = f"{prefix}{idx_suffix}"
                try:
                    count = await client.count(idx, query)
                    total += count
                except Exception:
                    pass
        if total > 0:
            stale_denorm[field] = total

    if stale_denorm:
        total = sum(stale_denorm.values())
        print(f"  STALE DENORM REFS REMAINING: {total}")
        for field, count in sorted(stale_denorm.items()):
            print(f"    {field}: {count}")
    else:
        print("  CLEAN - No stale country IDs in denorm arrays")

    # Summary
    rel_total = sum(len(v) for v in stale_rel_counts.values())
    denorm_total = sum(stale_denorm.values()) if stale_denorm else 0
    print(f"\n{'=' * 60}")
    if rel_total == 0 and denorm_total == 0:
        print("ALL CLEAR: No stale country references remain.")
        print("fix_stale_refs.py successfully cleaned everything.")
    else:
        print(f"REMAINING ISSUES: {rel_total} stale rels, {denorm_total} stale denorms")
    print(f"{'=' * 60}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
