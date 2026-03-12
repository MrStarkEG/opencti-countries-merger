"""Create missing sub-region -> macro-region located-at relationships.

All 22 UN M49 sub-regions should be linked to their parent macro-region
via a located-at relationship. This script creates any that are missing.

Usage:
    uv run python fix_region_hierarchy.py --dry-run
    uv run python fix_region_hierarchy.py
    uv run python fix_region_hierarchy.py --force
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
    SUBREGION_TO_MACRO,
    REGIONS_BY_M49,
)


@dataclass
class RegionHit:
    """Minimal region entity from ES."""

    internal_id: str
    index: str
    name: str
    entity_type: str
    parent_types: list[str]


async def find_region(client: ESClient, name: str) -> RegionHit | None:
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
    return RegionHit(
        internal_id=src["internal_id"],
        index=hits[0]["_index"],
        name=src.get("name", ""),
        entity_type=src.get("entity_type", ""),
        parent_types=src.get("parent_types", []),
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
    from_entity: RegionHit, to_entity: RegionHit,
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

        # Load all needed regions from ES
        region_cache: dict[str, RegionHit] = {}
        all_m49_codes = set(SUBREGION_TO_MACRO.keys()) | set(SUBREGION_TO_MACRO.values())
        for m49 in all_m49_codes:
            entry = REGIONS_BY_M49.get(m49)
            if not entry:
                continue
            hit = await find_region(client, entry.name)
            if hit:
                region_cache[m49] = hit

        print(f"Loaded {len(region_cache)} region(s) from ES.\n")

        # Check each sub-region -> macro-region link
        to_create: list[tuple[RegionHit, RegionHit]] = []
        already_linked = 0

        for sub_m49, macro_m49 in sorted(SUBREGION_TO_MACRO.items()):
            sub = region_cache.get(sub_m49)
            macro = region_cache.get(macro_m49)
            if not sub or not macro:
                sub_name = REGIONS_BY_M49.get(sub_m49, sub_m49)
                macro_name = REGIONS_BY_M49.get(macro_m49, macro_m49)
                print(f"  SKIP: {sub_name} or {macro_name} not found in ES")
                continue

            exists = await check_located_at(client, sub.internal_id, macro.internal_id)
            if exists:
                already_linked += 1
            else:
                to_create.append((sub, macro))

        print(f"Already linked: {already_linked}")
        print(f"To create:      {len(to_create)}\n")

        if to_create:
            print("Relationships to create:")
            for sub, macro in to_create:
                print(f"  {sub.name} -> {macro.name}")

        if not to_create:
            print("Nothing to do.")
            return

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            answer = input(f"\nCreate {len(to_create)} relationship(s)? [y/N] ")
            if answer.strip().lower() != "y":
                print("Aborted.")
                return

        # Create relationships
        rels_index = f"{client.prefix}stix_core_relationships-000001"
        created = 0
        failed = 0
        for sub, macro in to_create:
            try:
                doc = build_located_at_doc(sub, macro)
                await client.index_document(rels_index, doc["internal_id"], doc)
                created += 1
                print(f"  Created: {sub.name} -> {macro.name}")
            except Exception as exc:
                failed += 1
                print(f"  FAILED: {sub.name} -> {macro.name}: {exc}")

        print(f"\nCreated: {created}  Failed: {failed}")
        print("Done.")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create missing sub-region -> macro-region hierarchy links.",
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
