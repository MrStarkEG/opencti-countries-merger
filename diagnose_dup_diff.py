"""Check if duplicate relationships have meaningful differences.

Are they truly identical, or do they have different start_time, stop_time,
confidence, description, or created_by?
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

# Fields that would make two relationships meaningfully different
DIFF_FIELDS = [
    "start_time",
    "stop_time",
    "confidence",
    "description",
]

# Default "meaningless" values in OpenCTI
DEFAULT_START = "1970-01-01T00:00:00.000Z"
DEFAULT_STOP = "5138-11-16T09:46:40.000Z"


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix

    print("Scanning for duplicates and checking for meaningful differences...")

    # key = (entity_id, country_id, rel_type) -> list of full sources
    groups = defaultdict(list)

    for index_suffix in SCAN_INDICES:
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

        async for hit in client.scroll_all(index, body):
            src = hit["_source"]
            conns = src.get("connections", [])
            rel_type = src.get("relationship_type", "?")

            country_conns = [c for c in conns if "Country" in c.get("types", [])]
            non_country_conns = [c for c in conns if "Country" not in c.get("types", [])]

            for nc in non_country_conns:
                for cc in country_conns:
                    key = (nc["internal_id"], cc["internal_id"], rel_type)
                    groups[key].append({
                        "rel_id": src.get("internal_id", hit["_id"]),
                        "index": hit["_index"],
                        "start_time": src.get("start_time", ""),
                        "stop_time": src.get("stop_time", ""),
                        "confidence": src.get("confidence", ""),
                        "description": (src.get("description") or "")[:100],
                        "created_at": src.get("created_at", src.get("created", "")),
                        "entity_name": nc.get("name", "?"),
                        "country_name": cc.get("name", "?"),
                    })

    # Filter to duplicates only
    dups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"\nTotal duplicate groups: {len(dups)}")

    # Check each group for meaningful differences
    truly_identical = 0
    has_diff_times = 0
    has_diff_confidence = 0
    has_diff_description = 0
    diff_examples = []

    for (eid, cid, rtype), hits in dups.items():
        start_times = set()
        stop_times = set()
        confidences = set()
        descriptions = set()

        for h in hits:
            st = h["start_time"]
            sp = h["stop_time"]
            # Normalize defaults to empty
            if st == DEFAULT_START:
                st = ""
            if sp == DEFAULT_STOP:
                sp = ""
            start_times.add(st)
            stop_times.add(sp)
            confidences.add(str(h["confidence"]))
            desc = h["description"].strip()
            if desc:
                descriptions.add(desc)

        # Remove empty strings for comparison
        start_times.discard("")
        stop_times.discard("")

        has_real_time_diff = len(start_times) > 1 or len(stop_times) > 1
        has_real_conf_diff = len(confidences) > 1
        has_real_desc_diff = len(descriptions) > 1

        if has_real_time_diff:
            has_diff_times += 1
            if len(diff_examples) < 10:
                diff_examples.append({
                    "entity": hits[0]["entity_name"],
                    "country": hits[0]["country_name"],
                    "rel_type": rtype,
                    "count": len(hits),
                    "diff_type": "time",
                    "start_times": start_times,
                    "stop_times": stop_times,
                    "hits": hits,
                })
        elif has_real_desc_diff:
            has_diff_description += 1
        elif has_real_conf_diff:
            has_diff_confidence += 1
        else:
            truly_identical += 1

    print(f"\nResults:")
    print(f"  Truly identical (safe to dedup): {truly_identical}")
    print(f"  Different start/stop times: {has_diff_times}")
    print(f"  Different confidence only: {has_diff_confidence}")
    print(f"  Different description only: {has_diff_description}")

    if diff_examples:
        print(f"\nExamples with different times:")
        for ex in diff_examples:
            print(f"\n  {ex['entity']} -> {ex['country']} ({ex['rel_type']}) x{ex['count']}")
            print(f"    start_times: {ex['start_times']}")
            print(f"    stop_times: {ex['stop_times']}")
            for h in ex["hits"][:4]:
                print(
                    f"      id={h['rel_id'][:12]}... "
                    f"start={h['start_time']} "
                    f"stop={h['stop_time']} "
                    f"conf={h['confidence']} "
                    f"created={h['created_at']}"
                )

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
