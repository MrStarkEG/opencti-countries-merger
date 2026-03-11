"""Check what the 15 unmapped deleted countries look like and find their matches."""

import asyncio
from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


UNMAPPED = [
    ("08068ab0-c0cf-45", "AN"),
    ("0ddd2401-c1a9-4e", "Iran, Islamic Republic of"),
    ("28402d89-4416-42", "United States of America"),
    ("33d2ea04-1b4d-43", "QC"),
    ("38116f95-8189-46", "Iran"),
    ("6807d047-8eca-4c", "Germany"),
    ("6f086273-dc04-4f", "South Korea"),
    ("959bffa5-4d92-4b", "??"),
    ("a9ba7d1a-0346-49", "Egypt"),
    ("b142aaef-183d-44", "Canada"),
    ("e3a7109a-a455-44", "EU"),
    ("f4335cdf-7428-43", "United States"),
    ("f77607a9-4bfc-4e", "Madagascar"),
    ("f947d768-a3bc-49", "Netherlands"),
    ("f970bbd9-1c96-4e", "Korea, Republic of"),
]


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix

    sdo_index = f"{prefix}stix_domain_objects-*"
    deleted_index = f"{prefix}deleted_objects"

    # For each unmapped, fetch the full doc from deleted_objects
    # and try to find the matching current country
    print("UNMAPPED DELETED COUNTRIES - FULL DETAILS")
    print("=" * 70)

    for partial_id, name in UNMAPPED:
        # Search in deleted_objects by name
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"entity_type.keyword": "Country"}},
                        {"match": {"name": name}},
                    ]
                }
            }
        }
        resp = await client.search(deleted_index, body, size=5)
        hits = resp["hits"]["hits"]

        print(f"\n  {name} (partial id={partial_id})")
        for hit in hits:
            src = hit["_source"]
            aliases = src.get("x_opencti_aliases") or []
            full_id = src["internal_id"]
            print(f"    Full ID: {full_id}")
            print(f"    Aliases: {aliases}")
            print(f"    i_aliases_ids: {src.get('i_aliases_ids', [])[:3]}")

        # Now search for matching current country
        print(f"    --- Searching current countries for match ---")
        for search_name in [name] + [name.split(",")[0] if "," in name else ""]:
            if not search_name:
                continue
            curr_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"entity_type.keyword": "Country"}},
                            {"match": {"name": search_name}},
                        ]
                    }
                }
            }
            curr_resp = await client.search(sdo_index, curr_body, size=5)
            for ch in curr_resp["hits"]["hits"]:
                cs = ch["_source"]
                print(
                    f"    MATCH: {cs['name']} "
                    f"(id={cs['internal_id'][:16]}...) "
                    f"aliases={cs.get('x_opencti_aliases', [])}"
                )

    # Also check how many stale refs these unmapped IDs have
    print(f"\n{'=' * 70}")
    print("STALE REF COUNTS FOR UNMAPPED IDS")
    print("=" * 70)

    for partial_id, name in UNMAPPED:
        # Get full ID from deleted_objects
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"entity_type.keyword": "Country"}},
                        {"match": {"name": name}},
                    ]
                }
            }
        }
        resp = await client.search(deleted_index, body, size=1)
        if not resp["hits"]["hits"]:
            print(f"  {name}: not found in deleted_objects")
            continue

        full_id = resp["hits"]["hits"][0]["_source"]["internal_id"]

        # Count refs in all rel indices
        total = 0
        for idx_suffix in [
            "stix_core_relationships-*",
            "stix_meta_relationships-*",
            "inferred_relationships-*",
        ]:
            idx = f"{prefix}{idx_suffix}"
            ref_body = {
                "query": {
                    "nested": {
                        "path": "connections",
                        "query": {
                            "term": {"connections.internal_id.keyword": full_id}
                        },
                    }
                }
            }
            count = await client.count(idx, ref_body)
            total += count

        # Count denorm refs
        denorm_total = 0
        for field in ["rel_targets.internal_id", "rel_located-at.internal_id", "rel_object.internal_id"]:
            dn_body = {"query": {"term": {f"{field}.keyword": full_id}}}
            dn_count = await client.count(f"{prefix}stix_domain_objects-*", dn_body)
            denorm_total += dn_count

        print(f"  {name} ({full_id[:16]}...): {total} rel refs, {denorm_total} denorm refs")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
