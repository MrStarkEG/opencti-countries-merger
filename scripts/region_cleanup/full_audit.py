"""Comprehensive data integrity audit for countries and regions.

Checks:
  1. Duplicate countries (by name and alpha-2 code)
  2. Junk countries still present
  3. Countries missing alpha-2 codes
  4. Duplicate located-at relationships
  5. Sub-region -> macro-region hierarchy
  6. ISO reference countries missing from ES

Usage:
    uv run python full_audit.py
"""

import asyncio
from collections import defaultdict

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.data.iso3166 import ISO_COUNTRIES, ISO_BY_ALPHA2
from opencti_country_merger.data.regions import (
    COUNTRY_TO_SUBREGION,
    SUBREGION_TO_MACRO,
    REGIONS_BY_M49,
    UN_REGIONS,
)
from opencti_country_merger.services.country_mapper import JUNK_COUNTRY_NAMES


def _get_alpha2(aliases: list[str]) -> str | None:
    for a in aliases:
        if len(a) == 2 and a.isalpha() and a.isupper():
            return a
    return None


async def run() -> None:
    client = ESClient(Settings())
    try:
        await client.health_check()
        sdo = f"{client.prefix}stix_domain_objects-*"
        rels = f"{client.prefix}stix_core_relationships-*"

        print("=" * 70)
        print("  COMPREHENSIVE DATA INTEGRITY AUDIT")
        print("=" * 70)

        # Fetch all countries
        body = {"query": {"term": {"entity_type.keyword": "Country"}}}
        resp = await client.search(sdo, body, size=500)
        countries = resp["hits"]["hits"]

        # Fetch all regions
        body = {"query": {"term": {"entity_type.keyword": "Region"}}}
        resp = await client.search(sdo, body, size=200)
        es_regions = resp["hits"]["hits"]

        issues = 0

        # ==========================================================
        # 1. DUPLICATE COUNTRIES
        # ==========================================================
        print("\n[1] DUPLICATE COUNTRIES")
        by_name: dict[str, list] = defaultdict(list)
        by_code: dict[str, list] = defaultdict(list)
        for c in countries:
            src = c["_source"]
            name = src.get("name", "?")
            cid = src["internal_id"]
            aliases = src.get("x_opencti_aliases", []) or []
            alpha2 = _get_alpha2(aliases)
            by_name[name.lower()].append({"name": name, "id": cid, "alpha2": alpha2})
            if alpha2:
                by_code[alpha2].append({"name": name, "id": cid})

        dup_names = {k: v for k, v in by_name.items() if len(v) > 1}
        dup_codes = {k: v for k, v in by_code.items() if len(v) > 1}

        if dup_names:
            print("  DUPLICATE NAMES:")
            for entries in dup_names.values():
                for e in entries:
                    print(f"    {e['name']} ({e['id'][:12]}...) code={e['alpha2']}")
            issues += len(dup_names)
        else:
            print("  No duplicate country names. OK")

        if dup_codes:
            print("  DUPLICATE CODES:")
            for code, entries in dup_codes.items():
                for e in entries:
                    print(f"    {code}: {e['name']} ({e['id'][:12]}...)")
            issues += len(dup_codes)
        else:
            print("  No duplicate country codes. OK")

        # ==========================================================
        # 2. JUNK COUNTRIES
        # ==========================================================
        print("\n[2] JUNK COUNTRIES")
        junk_found = [
            c["_source"].get("name", "")
            for c in countries
            if c["_source"].get("name", "") in JUNK_COUNTRY_NAMES
        ]
        if junk_found:
            print(f"  JUNK STILL IN ES: {junk_found}")
            issues += len(junk_found)
        else:
            print("  No junk countries found. OK")

        # ==========================================================
        # 3. COUNTRIES WITHOUT ALPHA-2
        # ==========================================================
        print("\n[3] COUNTRIES WITHOUT ALPHA-2 CODE")
        no_code = []
        for c in countries:
            src = c["_source"]
            aliases = src.get("x_opencti_aliases", []) or []
            if not _get_alpha2(aliases):
                no_code.append(
                    f"{src.get('name', '?')} ({src['internal_id'][:12]}...) "
                    f"aliases={aliases}"
                )
        if no_code:
            print("  MISSING ALPHA-2:")
            for x in sorted(no_code):
                print(f"    {x}")
            issues += len(no_code)
        else:
            print("  All countries have alpha-2 codes. OK")

        # ==========================================================
        # 4. DUPLICATE located-at RELATIONSHIPS
        # ==========================================================
        print("\n[4] DUPLICATE located-at RELATIONSHIPS")
        dup_rels = []
        for c in countries:
            src = c["_source"]
            cid = src["internal_id"]
            name = src.get("name", "?")
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"relationship_type.keyword": "located-at"}},
                            {"nested": {
                                "path": "connections",
                                "query": {"bool": {"must": [
                                    {"term": {"connections.internal_id.keyword": cid}},
                                    {"term": {"connections.role.keyword": "located-at_from"}},
                                ]}},
                            }},
                        ]
                    }
                }
            }
            resp = await client.search(rels, body, size=20)
            targets = []
            for h in resp["hits"]["hits"]:
                for conn in h["_source"].get("connections", []):
                    if conn.get("role") == "located-at_to":
                        targets.append(conn.get("name", "?"))
            seen: set[str] = set()
            for t in targets:
                if t in seen:
                    dup_rels.append(f"{name} -> {t} (appears multiple times)")
                seen.add(t)

        if dup_rels:
            print("  DUPLICATE RELATIONSHIPS:")
            for x in sorted(dup_rels):
                print(f"    {x}")
            issues += len(dup_rels)
        else:
            print("  No duplicate located-at relationships. OK")

        # ==========================================================
        # 5. SUB-REGION -> MACRO-REGION HIERARCHY
        # ==========================================================
        print("\n[5] SUB-REGION -> MACRO-REGION HIERARCHY")
        region_ids = {
            r["_source"].get("name", ""): r["_source"]["internal_id"]
            for r in es_regions
        }
        missing_hierarchy = []
        for sub_m49, macro_m49 in SUBREGION_TO_MACRO.items():
            sub_entry = REGIONS_BY_M49.get(sub_m49)
            macro_entry = REGIONS_BY_M49.get(macro_m49)
            if not sub_entry or not macro_entry:
                continue
            sub_id = region_ids.get(sub_entry.name)
            macro_id = region_ids.get(macro_entry.name)
            if not sub_id or not macro_id:
                continue
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"relationship_type.keyword": "located-at"}},
                            {"nested": {
                                "path": "connections",
                                "query": {"bool": {"must": [
                                    {"term": {"connections.internal_id.keyword": sub_id}},
                                    {"term": {"connections.role.keyword": "located-at_from"}},
                                ]}},
                            }},
                            {"nested": {
                                "path": "connections",
                                "query": {"bool": {"must": [
                                    {"term": {"connections.internal_id.keyword": macro_id}},
                                    {"term": {"connections.role.keyword": "located-at_to"}},
                                ]}},
                            }},
                        ]
                    }
                }
            }
            cnt = await client.count(rels, body)
            if cnt == 0:
                missing_hierarchy.append(
                    f"{sub_entry.name} ({sub_m49}) -> {macro_entry.name} ({macro_m49})"
                )

        if missing_hierarchy:
            print("  MISSING SUB-REGION -> MACRO-REGION LINKS:")
            for x in sorted(missing_hierarchy):
                print(f"    {x}")
            issues += len(missing_hierarchy)
        else:
            print("  All sub-regions linked to their macro-regions. OK")

        # ==========================================================
        # 6. ISO REFERENCE vs ES
        # ==========================================================
        print("\n[6] ISO REFERENCE vs ES")
        es_codes: set[str] = set()
        for c in countries:
            aliases = c["_source"].get("x_opencti_aliases", []) or []
            a2 = _get_alpha2(aliases)
            if a2:
                es_codes.add(a2)
        ref_codes = {e.alpha_2 for e in ISO_COUNTRIES}
        missing_from_es = ref_codes - es_codes
        extra_in_es = es_codes - ref_codes

        if missing_from_es:
            print("  IN REFERENCE BUT NOT IN ES:")
            for code in sorted(missing_from_es):
                entry = ISO_BY_ALPHA2.get(code)
                print(f"    {code} - {entry.name if entry else '?'}")
            issues += len(missing_from_es)
        else:
            print("  All ISO countries present in ES. OK")

        if extra_in_es:
            print(f"  IN ES BUT NOT IN REFERENCE: {sorted(extra_in_es)}")
            issues += len(extra_in_es)

        # ==========================================================
        # SUMMARY
        # ==========================================================
        print("\n" + "=" * 70)
        if issues == 0:
            print("  ALL CHECKS PASSED. 0 issues found.")
        else:
            print(f"  {issues} ISSUE(S) FOUND -- see details above.")
        print("=" * 70)

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(run())
