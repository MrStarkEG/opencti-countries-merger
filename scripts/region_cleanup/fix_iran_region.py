"""Create missing located-at relationships for Iran -> Western Asia -> Asia.

Iran (IR) was missing from COUNTRY_TO_SUBREGION and has no region links in ES.
This script creates the two located-at relationships:
  1. Iran -> Western Asia (sub-region 145)
  2. Iran -> Asia (macro-region 142)

Usage:
    uv run python fix_iran_region.py --dry-run
    uv run python fix_iran_region.py
    uv run python fix_iran_region.py --force
"""

import argparse
import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


IRAN_NAME = "Iran"
WESTERN_ASIA_NAME = "Western Asia"
ASIA_NAME = "Asia"


@dataclass
class EntityHit:
    """Minimal entity from ES."""

    internal_id: str
    index: str
    name: str
    entity_type: str
    parent_types: list[str]


async def find_entity(
    client: ESClient, name: str, entity_type: str,
) -> EntityHit | None:
    """Search for an entity by exact name and type."""
    sdo_index = f"{client.prefix}stix_domain_objects-*"
    body: dict[str, Any] = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"entity_type.keyword": entity_type}},
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
    )


async def check_located_at(
    client: ESClient, from_id: str, to_id: str,
) -> bool:
    """Check if a located-at relationship already exists between two entities."""
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
    standard_id = f"relationship--{uuid.uuid4()}"

    return {
        "internal_id": internal_id,
        "standard_id": standard_id,
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

        # --- Locate entities ---
        iran = await find_entity(client, IRAN_NAME, "Country")
        western_asia = await find_entity(client, WESTERN_ASIA_NAME, "Region")
        asia = await find_entity(client, ASIA_NAME, "Region")

        if iran is None:
            print(f"Country '{IRAN_NAME}' not found. Aborting.")
            return
        print(f"Country:    '{iran.name}' ({iran.internal_id})")

        if western_asia is None:
            print(f"Region '{WESTERN_ASIA_NAME}' not found. Aborting.")
            return
        print(f"Sub-region: '{western_asia.name}' ({western_asia.internal_id})")

        if asia is None:
            print(f"Region '{ASIA_NAME}' not found. Aborting.")
            return
        print(f"Macro:      '{asia.name}' ({asia.internal_id})")

        # --- Check existing links ---
        has_sub = await check_located_at(client, iran.internal_id, western_asia.internal_id)
        has_macro = await check_located_at(client, iran.internal_id, asia.internal_id)

        print(f"\n'{IRAN_NAME}' -> '{WESTERN_ASIA_NAME}': {'exists' if has_sub else 'MISSING'}")
        print(f"'{IRAN_NAME}' -> '{ASIA_NAME}': {'exists' if has_macro else 'MISSING'}")

        to_create: list[tuple[str, EntityHit, EntityHit]] = []
        if not has_sub:
            to_create.append((WESTERN_ASIA_NAME, iran, western_asia))
        if not has_macro:
            to_create.append((ASIA_NAME, iran, asia))

        if not to_create:
            print("\nAll links already exist. Nothing to do.")
            return

        print(f"\n{len(to_create)} relationship(s) to create.")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            answer = input("\nCreate missing located-at relationships? [y/N] ")
            if answer.strip().lower() != "y":
                print("Aborted.")
                return

        # --- Create relationships ---
        rels_index = f"{client.prefix}stix_core_relationships-000001"
        for region_name, from_e, to_e in to_create:
            doc = build_located_at_doc(from_e, to_e)
            await client.index_document(rels_index, doc["internal_id"], doc)
            print(f"Created: '{IRAN_NAME}' -> '{region_name}' ({doc['internal_id']})")

        print(f"\nDone. Iran is now linked to {WESTERN_ASIA_NAME} and {ASIA_NAME}.")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create missing located-at relationships for Iran.",
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
