"""Check for relationships pointing to old/deleted country IDs.

Looks for:
1. Relationships where a connection references a country ID that no longer
   exists in stix_domain_objects (the country was merged/deleted).
2. Denormalized ref arrays on entities that reference stale country IDs.
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


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix

    health = await client.health_check()
    print(f"ES cluster: {health['cluster_name']} - {health['status']}")

    # -- 1. Build set of all CURRENT country IDs --
    print(f"\n{'=' * 70}")
    print("1. FETCHING ALL CURRENT COUNTRIES")
    print(f"{'=' * 70}")

    country_index = f"{prefix}stix_domain_objects-*"
    body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    current_countries = {}

    async for hit in client.scroll_all(country_index, body):
        src = hit["_source"]
        iid = src["internal_id"]
        current_countries[iid] = src.get("name", "?")

    print(f"  Current countries: {len(current_countries)}")

    # -- 2. Build set of all DELETED country IDs (from archive) --
    print(f"\n{'=' * 70}")
    print("2. FETCHING DELETED/MERGED COUNTRIES FROM ARCHIVE")
    print(f"{'=' * 70}")

    deleted_index = f"{prefix}deleted_objects"
    deleted_body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    deleted_countries = {}

    try:
        async for hit in client.scroll_all(deleted_index, deleted_body):
            src = hit["_source"]
            iid = src["internal_id"]
            deleted_countries[iid] = src.get("name", "?")
        print(f"  Deleted/merged countries in archive: {len(deleted_countries)}")
        for did, dname in list(deleted_countries.items())[:15]:
            print(f"    {dname} (id={did[:16]}...)")
        if len(deleted_countries) > 15:
            print(f"    ... and {len(deleted_countries) - 15} more")
    except Exception as e:
        print(f"  Could not read deleted_objects: {e}")

    # -- 3. Scan ALL relationship indices for stale country refs --
    print(f"\n{'=' * 70}")
    print("3. SCANNING RELATIONSHIPS FOR STALE COUNTRY REFERENCES")
    print(f"{'=' * 70}")

    # Query: any relationship with a Country-typed connection
    query_body = {
        "query": {
            "nested": {
                "path": "connections",
                "query": {
                    "term": {"connections.types.keyword": "Country"}
                },
            }
        }
    }

    stale_rels = []  # rels pointing to deleted country IDs
    orphan_rels = []  # rels pointing to IDs not found anywhere
    valid_count = 0

    for index_suffix in RELATIONSHIP_INDICES:
        index = f"{prefix}{index_suffix}"
        count = await client.count(index, query_body)
        print(f"\n  Scanning {index_suffix}: {count} rels with Country connections...")

        stale_in_index = 0
        orphan_in_index = 0
        valid_in_index = 0

        async for hit in client.scroll_all(index, query_body):
            src = hit["_source"]
            conns = src.get("connections", [])
            rel_type = src.get("relationship_type", "?")

            for c in conns:
                if "Country" not in c.get("types", []):
                    continue

                country_id = c["internal_id"]
                country_name = c.get("name", "?")

                if country_id in current_countries:
                    valid_in_index += 1
                elif country_id in deleted_countries:
                    stale_in_index += 1
                    stale_rels.append({
                        "rel_id": src.get("internal_id", hit["_id"]),
                        "rel_index": hit["_index"],
                        "rel_type": rel_type,
                        "stale_country_id": country_id,
                        "stale_country_name": country_name,
                        "archived_name": deleted_countries[country_id],
                        "connections": conns,
                    })
                else:
                    orphan_in_index += 1
                    orphan_rels.append({
                        "rel_id": src.get("internal_id", hit["_id"]),
                        "rel_index": hit["_index"],
                        "rel_type": rel_type,
                        "orphan_country_id": country_id,
                        "orphan_country_name": country_name,
                        "connections": conns,
                    })

        valid_count += valid_in_index
        print(f"    Valid: {valid_in_index}")
        print(f"    Stale (points to deleted/merged country): {stale_in_index}")
        print(f"    Orphan (country ID not found anywhere): {orphan_in_index}")

    # -- 4. Report stale refs --
    print(f"\n{'=' * 70}")
    print("4. STALE COUNTRY REFERENCES (pointing to old merged-away IDs)")
    print(f"{'=' * 70}")
    print(f"  Total stale refs: {len(stale_rels)}")

    if stale_rels:
        # Group by stale country
        by_country = defaultdict(list)
        for s in stale_rels:
            by_country[s["stale_country_id"]].append(s)

        print(f"  Unique stale country IDs: {len(by_country)}")
        print(f"\n  Breakdown by stale country:")
        for cid, rels in sorted(by_country.items(), key=lambda x: -len(x[1])):
            # Find the other side of the relationship
            other_names = set()
            for r in rels[:5]:
                for c in r["connections"]:
                    if c["internal_id"] != cid:
                        other_names.add(c.get("name", "?"))
            others = ", ".join(list(other_names)[:3])
            print(
                f"    {rels[0]['archived_name']} "
                f"(old id={cid[:16]}...): "
                f"{len(rels)} stale rels "
                f"(rel_type={rels[0]['rel_type']}, connected to: {others})"
            )

    # -- 5. Report orphan refs --
    print(f"\n{'=' * 70}")
    print("5. ORPHAN COUNTRY REFERENCES (ID not found in current OR deleted)")
    print(f"{'=' * 70}")
    print(f"  Total orphan refs: {len(orphan_rels)}")

    if orphan_rels:
        by_country = defaultdict(list)
        for o in orphan_rels:
            by_country[o["orphan_country_id"]].append(o)

        print(f"  Unique orphan country IDs: {len(by_country)}")
        print(f"\n  Breakdown:")
        for cid, rels in sorted(by_country.items(), key=lambda x: -len(x[1])):
            print(
                f"    name='{rels[0]['orphan_country_name']}' "
                f"(id={cid[:16]}...): "
                f"{len(rels)} orphan rels"
            )

    # -- 6. Check denormalized refs on stix_domain_objects --
    print(f"\n{'=' * 70}")
    print("6. CHECKING DENORMALIZED REFS FOR STALE COUNTRY IDS")
    print(f"{'=' * 70}")

    if deleted_countries:
        deleted_ids = list(deleted_countries.keys())
        # Check if any entity has a deleted country ID in rel_targets.internal_id
        for rel_field in ["rel_targets", "rel_located-at", "rel_object"]:
            field_name = f"{rel_field}.internal_id"
            for chunk_start in range(0, len(deleted_ids), BATCH_SIZE):
                chunk = deleted_ids[chunk_start : chunk_start + BATCH_SIZE]
                body = {
                    "query": {
                        "terms": {f"{field_name}.keyword": chunk}
                    }
                }
                sdo_index = f"{prefix}stix_domain_objects-*"
                count = await client.count(sdo_index, body)
                if count > 0:
                    print(
                        f"  {field_name}: {count} entities still reference "
                        f"deleted country IDs"
                    )

    # -- Summary --
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Current countries: {len(current_countries)}")
    print(f"  Deleted/archived countries: {len(deleted_countries)}")
    print(f"  Valid country refs in relationships: {valid_count}")
    print(f"  Stale refs (to deleted countries): {len(stale_rels)}")
    print(f"  Orphan refs (ID not found anywhere): {len(orphan_rels)}")

    await client.close()


BATCH_SIZE = 500

if __name__ == "__main__":
    asyncio.run(main())
