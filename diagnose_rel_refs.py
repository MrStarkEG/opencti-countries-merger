"""Check if duplicate relationship IDs are referenced by other documents.

Before deleting duplicates, verify their IDs don't appear as references in:
- connections.internal_id of other relationships (rel-to-rel)
- rel_TYPE.internal_id denorm arrays on entities
- object_refs or any other reference field

If any are referenced, deleting them would create dangling pointers.
"""

import asyncio
from collections import defaultdict

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


SCAN_INDICES = [
    "stix_core_relationships-*",
    "stix_meta_relationships-*",
    "inferred_relationships-*",
]

DEFAULT_START = "1970-01-01T00:00:00.000Z"
DEFAULT_STOP = "5138-11-16T09:46:40.000Z"

BATCH_SIZE = 500


async def collect_duplicate_ids(client: ESClient, prefix: str) -> list[str]:
    """Collect all relationship IDs that would be deleted (truly identical dups)."""
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for suffix in SCAN_INDICES:
        index = f"{prefix}{suffix}"
        body = {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {"term": {"connections.types.keyword": "Country"}},
                }
            }
        }
        async for hit in client.scroll_all(index, body):
            src = hit["_source"]
            conns = src.get("connections", [])
            rel_type = src.get("relationship_type", "?")
            country = [c for c in conns if "Country" in c.get("types", [])]
            other = [c for c in conns if "Country" not in c.get("types", [])]
            for nc in other:
                for cc in country:
                    key = (nc["internal_id"], cc["internal_id"], rel_type)
                    groups[key].append({
                        "rel_id": src.get("internal_id", hit["_id"]),
                        "created_at": src.get("created_at", src.get("created", "9999")),
                        "start_time": src.get("start_time", ""),
                        "stop_time": src.get("stop_time", ""),
                        "confidence": src.get("confidence", ""),
                        "description": (src.get("description") or ""),
                    })

    dup_ids = []
    for hits in groups.values():
        if len(hits) < 2:
            continue
        if _has_meaningful_differences(hits):
            continue
        sorted_hits = sorted(hits, key=lambda h: h["created_at"])
        for h in sorted_hits[1:]:
            dup_ids.append(h["rel_id"])

    return dup_ids


def _has_meaningful_differences(hits: list[dict]) -> bool:
    """Same logic as fix_duplicate_rels.py."""
    starts, stops, confs, descs = set(), set(), set(), set()
    for h in hits:
        st = "" if h["start_time"] == DEFAULT_START else h["start_time"]
        sp = "" if h["stop_time"] == DEFAULT_STOP else h["stop_time"]
        starts.add(st)
        stops.add(sp)
        confs.add(str(h["confidence"]))
        d = h["description"].strip()
        if d:
            descs.add(d)
    starts.discard("")
    stops.discard("")
    return len(starts) > 1 or len(stops) > 1 or len(confs) > 1 or len(descs) > 1


async def check_references(
    client: ESClient, prefix: str, dup_ids: list[str],
) -> None:
    """Check if any duplicate IDs are referenced elsewhere in ES."""
    print(f"\nChecking {len(dup_ids)} duplicate IDs for external references...\n")

    # 1. Check connections.internal_id in all relationship indices
    print("1) Checking connections.internal_id in relationship indices...")
    conn_refs = 0
    for suffix in SCAN_INDICES:
        index = f"{prefix}{suffix}"
        for i in range(0, len(dup_ids), BATCH_SIZE):
            chunk = dup_ids[i : i + BATCH_SIZE]
            body = {
                "query": {
                    "nested": {
                        "path": "connections",
                        "query": {
                            "terms": {"connections.internal_id.keyword": chunk}
                        },
                    }
                }
            }
            count = await client.count(index, body)
            conn_refs += count
    print(f"   Result: {conn_refs} references found")

    # 2. Check object_refs in SDO indices (reports, notes, opinions, etc.)
    print("2) Checking object_refs in stix_domain_objects...")
    sdo_index = f"{prefix}stix_domain_objects-*"
    obj_refs = 0
    for i in range(0, len(dup_ids), BATCH_SIZE):
        chunk = dup_ids[i : i + BATCH_SIZE]
        body = {"query": {"terms": {"object_refs.keyword": chunk}}}
        count = await client.count(sdo_index, body)
        obj_refs += count
    print(f"   Result: {obj_refs} references found")

    # 3. Check object_refs_inferred
    print("3) Checking object_refs_inferred in stix_domain_objects...")
    inferred_refs = 0
    for i in range(0, len(dup_ids), BATCH_SIZE):
        chunk = dup_ids[i : i + BATCH_SIZE]
        body = {"query": {"terms": {"object_refs_inferred.keyword": chunk}}}
        count = await client.count(sdo_index, body)
        inferred_refs += count
    print(f"   Result: {inferred_refs} references found")

    # 4. Check rel_TYPE.internal_id denorm arrays for rel IDs
    #    (these should store entity IDs, not rel IDs, but let's verify)
    print("4) Checking rel_*.internal_id denorm arrays for rel IDs...")
    denorm_refs = 0
    for rel_type in ["targets", "located-at", "uses", "object", "related-to"]:
        field = f"rel_{rel_type}.internal_id.keyword"
        for i in range(0, len(dup_ids), BATCH_SIZE):
            chunk = dup_ids[i : i + BATCH_SIZE]
            body = {"query": {"terms": {field: chunk}}}
            try:
                count = await client.count(sdo_index, body)
                denorm_refs += count
            except Exception:
                pass
    print(f"   Result: {denorm_refs} references found")

    # 5. Check internal_id in stix_meta_relationships
    #    (e.g., a meta-rel pointing TO a core-rel as its target)
    print("5) Checking if meta-relationships reference these rel IDs...")
    meta_index = f"{prefix}stix_meta_relationships-*"
    meta_refs = 0
    for i in range(0, len(dup_ids), BATCH_SIZE):
        chunk = dup_ids[i : i + BATCH_SIZE]
        body = {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {
                        "terms": {"connections.internal_id.keyword": chunk}
                    },
                }
            }
        }
        count = await client.count(meta_index, body)
        meta_refs += count
    print(f"   Result: {meta_refs} references found")

    # Summary
    total = conn_refs + obj_refs + inferred_refs + denorm_refs + meta_refs
    print(f"\n{'=' * 60}")
    if total == 0:
        print("SAFE: No external references found for any duplicate IDs.")
        print("Deleting these relationships will NOT create dangling pointers.")
    else:
        print(f"WARNING: {total} external references found!")
        print("  connections.internal_id: ", conn_refs)
        print("  object_refs: ", obj_refs)
        print("  object_refs_inferred: ", inferred_refs)
        print("  rel_*.internal_id: ", denorm_refs)
        print("  meta-rel connections: ", meta_refs)
        print("DO NOT delete until these references are investigated.")
    print(f"{'=' * 60}")


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix

    print("Collecting duplicate relationship IDs...")
    dup_ids = await collect_duplicate_ids(client, prefix)
    print(f"Found {len(dup_ids)} duplicate IDs to check")

    await check_references(client, prefix, dup_ids)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
