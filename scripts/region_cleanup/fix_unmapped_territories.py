"""Create missing located-at relationships for 46 unmapped territories.

These territories exist in ES as Country entities but have no located-at
relationship to their correct UN M49 sub-region and macro-region. This
script creates both links for each territory.

Usage:
    uv run python fix_unmapped_territories.py --dry-run
    uv run python fix_unmapped_territories.py
    uv run python fix_unmapped_territories.py --force
"""

import argparse
import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.data.regions import (
    REGIONS_BY_M49,
    SUBREGION_TO_MACRO,
)


# UN M49 sub-region code for each unmapped territory.
TERRITORY_SUBREGIONS: dict[str, str] = {
    # Caribbean (029)
    "AI": "029", "AW": "029", "BL": "029", "BQ": "029",
    "CW": "029", "GP": "029", "KY": "029", "MF": "029",
    "MQ": "029", "MS": "029", "PR": "029", "SX": "029",
    "TC": "029", "VG": "029", "VI": "029",
    # Northern Europe (154)
    "AX": "154", "FO": "154", "GG": "154", "IM": "154",
    "JE": "154", "SJ": "154",
    # Polynesia (061)
    "AS": "061", "NU": "061", "PN": "061", "TK": "061",
    "UM": "061", "WF": "061",
    # Eastern Africa (014)
    "IO": "014", "RE": "014", "TF": "014", "YT": "014",
    # South America (005)
    "BV": "005", "FK": "005", "GF": "005", "GS": "005",
    # Northern America (021)
    "BM": "021", "PM": "021",
    # Australia and New Zealand (053)
    "HM": "053", "NF": "053",
    # South-eastern Asia (035)
    "CC": "035", "CX": "035",
    # Southern Europe (039)
    "GI": "039", "VA": "039",
    # Western Africa (011)
    "SH": "011",
    # Micronesia (057)
    "MP": "057",
}

# Antarctica has no UN M49 region.
EXCLUDED = {"AQ"}


@dataclass
class EntityHit:
    """Minimal entity from ES."""

    internal_id: str
    index: str
    name: str
    entity_type: str
    parent_types: list[str]
    alpha_2: str


async def find_countries_by_codes(
    client: ESClient, codes: set[str],
) -> dict[str, EntityHit]:
    """Fetch all Country entities and return those matching given alpha-2 codes."""
    sdo_index = f"{client.prefix}stix_domain_objects-*"
    body: dict[str, Any] = {"query": {"term": {"entity_type.keyword": "Country"}}}
    found: dict[str, EntityHit] = {}

    async for hit in client.scroll_all(sdo_index, body):
        src = hit["_source"]
        aliases = src.get("x_opencti_aliases", []) or []
        alpha2 = None
        for a in aliases:
            if len(a) == 2 and a.isalpha() and a.isupper() and a in codes:
                alpha2 = a
                break
        if alpha2:
            found[alpha2] = EntityHit(
                internal_id=src["internal_id"],
                index=hit["_index"],
                name=src.get("name", ""),
                entity_type=src.get("entity_type", ""),
                parent_types=src.get("parent_types", []),
                alpha_2=alpha2,
            )
    return found


async def find_region(
    client: ESClient, name: str,
) -> EntityHit | None:
    """Find a Region entity by exact name."""
    sdo_index = f"{client.prefix}stix_domain_objects-*"
    body: dict[str, Any] = {
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
    return EntityHit(
        internal_id=src["internal_id"],
        index=hits[0]["_index"],
        name=src.get("name", ""),
        entity_type=src.get("entity_type", ""),
        parent_types=src.get("parent_types", []),
        alpha_2="",
    )


async def check_located_at(
    client: ESClient, from_id: str, to_id: str,
) -> bool:
    """Check if a located-at relationship already exists."""
    rels = f"{client.prefix}stix_core_relationships-*"
    body: dict[str, Any] = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"relationship_type.keyword": "located-at"}},
                    {"nested": {
                        "path": "connections",
                        "query": {"bool": {"must": [
                            {"term": {"connections.internal_id.keyword": from_id}},
                            {"term": {"connections.role.keyword": "located-at_from"}},
                        ]}},
                    }},
                    {"nested": {
                        "path": "connections",
                        "query": {"bool": {"must": [
                            {"term": {"connections.internal_id.keyword": to_id}},
                            {"term": {"connections.role.keyword": "located-at_to"}},
                        ]}},
                    }},
                ]
            }
        }
    }
    cnt = await client.count(rels, body)
    return cnt > 0


def build_located_at_doc(
    from_entity: EntityHit, to_entity: EntityHit,
) -> dict[str, Any]:
    """Build a located-at relationship document."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    internal_id = str(uuid.uuid4())

    return {
        "internal_id": internal_id,
        "standard_id": f"relationship--{uuid.uuid4()}",
        "entity_type": "located-at",
        "relationship_type": "located-at",
        "created_at": now,
        "updated_at": now,
        "x_opencti_stix_ids": [],
        "revoked": False,
        "confidence": 100,
        "lang": "en",
        "created": now,
        "modified": now,
        "description": "",
        "start_time": "1970-01-01T00:00:00.000Z",
        "stop_time": "5138-11-16T09:46:40.000Z",
        "id": internal_id,
        "parent_types": [
            "basic-relationship",
            "stix-relationship",
            "stix-core-relationship",
        ],
        "base_type": "RELATION",
        "connections": [
            {
                "internal_id": from_entity.internal_id,
                "name": from_entity.name,
                "types": [from_entity.entity_type] + from_entity.parent_types,
                "role": "located-at_from",
            },
            {
                "internal_id": to_entity.internal_id,
                "name": to_entity.name,
                "types": [to_entity.entity_type] + to_entity.parent_types,
                "role": "located-at_to",
            },
        ],
    }


async def run(dry_run: bool, force: bool) -> None:
    """Main execution flow."""
    settings = Settings()
    client = ESClient(settings)

    try:
        await client.health_check()
        print("Connected to Elasticsearch.\n")

        # --- Find all target territories in ES ---
        target_codes = set(TERRITORY_SUBREGIONS.keys())
        countries = await find_countries_by_codes(client, target_codes)
        print(f"Found {len(countries)}/{len(target_codes)} territories in ES.\n")

        missing_in_es = target_codes - set(countries.keys())
        if missing_in_es:
            print(f"Not found in ES: {', '.join(sorted(missing_in_es))}\n")

        # --- Load all needed regions ---
        region_cache: dict[str, EntityHit] = {}
        needed_m49: set[str] = set()
        for code in countries:
            sub_code = TERRITORY_SUBREGIONS[code]
            macro_code = SUBREGION_TO_MACRO.get(sub_code, "")
            needed_m49.add(sub_code)
            if macro_code:
                needed_m49.add(macro_code)

        for m49 in needed_m49:
            region_entry = REGIONS_BY_M49.get(m49)
            if region_entry:
                hit = await find_region(client, region_entry.name)
                if hit:
                    region_cache[m49] = hit

        print(f"Loaded {len(region_cache)} region(s) from ES.\n")

        # --- Check each territory and build action list ---
        @dataclass
        class CreateAction:
            country_name: str
            alpha_2: str
            region_name: str
            from_entity: EntityHit
            to_entity: EntityHit

        actions: list[CreateAction] = []
        already_linked = 0

        for code in sorted(countries.keys()):
            country = countries[code]
            sub_code = TERRITORY_SUBREGIONS[code]
            macro_code = SUBREGION_TO_MACRO.get(sub_code, "")

            for m49 in [sub_code, macro_code]:
                if not m49 or m49 not in region_cache:
                    continue
                region = region_cache[m49]
                exists = await check_located_at(
                    client, country.internal_id, region.internal_id,
                )
                if exists:
                    already_linked += 1
                else:
                    actions.append(CreateAction(
                        country_name=country.name,
                        alpha_2=code,
                        region_name=region.name,
                        from_entity=country,
                        to_entity=region,
                    ))

        print(f"Already linked: {already_linked}")
        print(f"To create:      {len(actions)}\n")

        if actions:
            print("Relationships to create:")
            for a in actions:
                print(f"  {a.country_name} ({a.alpha_2}) -> {a.region_name}")

        if not actions:
            print("Nothing to do.")
            return

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            answer = input(f"\nCreate {len(actions)} relationship(s)? [y/N] ")
            if answer.strip().lower() != "y":
                print("Aborted.")
                return

        # --- Create relationships ---
        rels_index = f"{client.prefix}stix_core_relationships-000001"
        created = 0
        failed = 0
        for a in actions:
            try:
                doc = build_located_at_doc(a.from_entity, a.to_entity)
                await client.index_document(rels_index, doc["internal_id"], doc)
                created += 1
            except Exception as exc:
                failed += 1
                print(f"  FAILED: {a.country_name} -> {a.region_name}: {exc}")

        print(f"\nCreated: {created}  Failed: {failed}")
        print("Done.")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create missing located-at relationships for unmapped territories.",
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
