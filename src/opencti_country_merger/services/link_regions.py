"""Service for creating located-at relationships between countries and regions."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opencti_country_merger.data.regions import (
    COUNTRY_TO_SUBREGION,
    REGIONS_BY_M49,
    SUBREGION_TO_MACRO,
)
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.models.errors import ElasticsearchError
from opencti_country_merger.services.discovery import CountryEntity


# ------------------------------------------------------------------
# Data models
# ------------------------------------------------------------------


@dataclass
class LinkAction:
    """A located-at relationship to create between a country and a region."""

    country: CountryEntity
    region: CountryEntity
    region_m49: str


@dataclass
class LinkPlan:
    to_create: list[LinkAction] = field(default_factory=list)
    already_linked: int = 0
    unmatched_countries: list[CountryEntity] = field(default_factory=list)
    rel_index: str = ""


@dataclass
class LinkResult:
    created: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


# ------------------------------------------------------------------
# Service
# ------------------------------------------------------------------


class LinkRegionsService:
    """Build and execute a plan to link countries to their UN M49 sub-regions."""

    def __init__(self, client: ESClient) -> None:
        self._client = client
        self._prefix = client.prefix

    async def build_plan(
        self,
        countries: list[CountryEntity],
        regions: list[CountryEntity],
    ) -> LinkPlan:
        plan = LinkPlan()

        # Build region lookup: M49 code → region entity
        region_by_m49: dict[str, CountryEntity] = {}
        for r in regions:
            aliases = r.source.get("x_opencti_aliases") or []
            for alias in aliases:
                if alias in REGIONS_BY_M49:
                    region_by_m49[alias] = r

        # Also index regions by name for M49 lookup
        region_by_name: dict[str, CountryEntity] = {r.name: r for r in regions}
        for entry in REGIONS_BY_M49.values():
            if entry.m49_code not in region_by_m49 and entry.name in region_by_name:
                region_by_m49[entry.m49_code] = region_by_name[entry.name]

        # Determine the relationship index
        plan.rel_index = f"{self._prefix}stix_core_relationships-000001"

        # Resolve each country's alpha-2 from its aliases
        country_alpha2: dict[str, str] = {}
        for c in countries:
            aliases = c.source.get("x_opencti_aliases") or []
            for alias in aliases:
                if len(alias) == 2 and alias.isalpha() and alias.isupper():
                    country_alpha2[c.internal_id] = alias
                    break

        # Batch-check which specific region IDs each country is already linked to
        existing = await self._find_existing_links(countries)

        for country in countries:
            alpha2 = country_alpha2.get(country.internal_id)
            if not alpha2:
                plan.unmatched_countries.append(country)
                continue

            sub_m49 = COUNTRY_TO_SUBREGION.get(alpha2)
            if not sub_m49:
                plan.unmatched_countries.append(country)
                continue

            # Collect target regions: sub-region + macro-region
            target_m49s = [sub_m49]
            macro_m49 = SUBREGION_TO_MACRO.get(sub_m49)
            if macro_m49:
                target_m49s.append(macro_m49)

            linked_region_ids = existing.get(country.internal_id, set())
            any_created = False

            for m49 in target_m49s:
                region = region_by_m49.get(m49)
                if not region:
                    continue
                if region.internal_id in linked_region_ids:
                    plan.already_linked += 1
                    continue
                plan.to_create.append(
                    LinkAction(country=country, region=region, region_m49=m49)
                )
                any_created = True

            if not any_created and not linked_region_ids:
                plan.unmatched_countries.append(country)

        return plan

    async def _find_existing_links(
        self, countries: list[CountryEntity]
    ) -> dict[str, set[str]]:
        """Return mapping of country internal_id → set of region internal_ids
        that already have a located-at relationship."""
        existing: dict[str, set[str]] = {}
        rel_index = f"{self._prefix}stix_core_relationships-*"

        # Scroll all Country → Region located-at relationships
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"relationship_type.keyword": "located-at"}},
                        {
                            "nested": {
                                "path": "connections",
                                "query": {"term": {"connections.types.keyword": "Country"}},
                            }
                        },
                        {
                            "nested": {
                                "path": "connections",
                                "query": {"term": {"connections.types.keyword": "Region"}},
                            }
                        },
                    ]
                }
            }
        }
        async for hit in self._client.scroll_all(rel_index, body):
            conns = hit["_source"]["connections"]
            from_c = next((c for c in conns if c["role"] == "located-at_from"), None)
            to_c = next((c for c in conns if c["role"] == "located-at_to"), None)
            if from_c and to_c:
                existing.setdefault(from_c["internal_id"], set()).add(to_c["internal_id"])

        return existing

    async def execute(self, plan: LinkPlan, dry_run: bool = False) -> LinkResult:
        """Create located-at relationship docs and update denormalized refs."""
        result = LinkResult()

        # Collect denormalized ref updates: entity_id → set of IDs to add
        # Region gets country IDs; Country gets region IDs
        region_refs: dict[str, set[str]] = {}
        country_refs: dict[str, set[str]] = {}
        # Track entity index for updates
        entity_index: dict[str, str] = {}

        for action in plan.to_create:
            try:
                doc = _build_located_at_doc(action.country, action.region)
                if not dry_run:
                    await self._client.index_document(
                        plan.rel_index, doc["internal_id"], doc
                    )
                result.created += 1

                # Queue denormalized ref updates
                rid = action.region.internal_id
                cid = action.country.internal_id
                region_refs.setdefault(rid, set()).add(cid)
                country_refs.setdefault(cid, set()).add(rid)
                entity_index[rid] = action.region.index
                entity_index[cid] = action.country.index
            except ElasticsearchError as exc:
                result.failed += 1
                result.errors.append(
                    f"{action.country.name} -> {action.region.name}: {exc}"
                )

        if dry_run:
            return result

        # Update denormalized rel_located-at.internal_id on regions and countries
        for entity_id, new_ids in {**region_refs, **country_refs}.items():
            try:
                await self._append_denorm_refs(
                    entity_index[entity_id],
                    entity_id,
                    "rel_located-at.internal_id",
                    list(new_ids),
                )
            except ElasticsearchError as exc:
                result.errors.append(f"Denorm update {entity_id[:12]}...: {exc}")

        return result

    async def _append_denorm_refs(
        self,
        index: str,
        entity_id: str,
        field: str,
        new_ids: list[str],
    ) -> None:
        """Append IDs to a denormalized ref array field using a Painless script."""
        body = {
            "query": {"term": {"internal_id.keyword": entity_id}},
            "script": {
                "source": (
                    f"def field = ctx._source['{field}'];"
                    " if (field == null) {"
                    f"  ctx._source['{field}'] = params.new_ids;"
                    " } else {"
                    "  for (id in params.new_ids) {"
                    "    if (!field.contains(id)) { field.add(id); }"
                    "  }"
                    " }"
                ),
                "params": {"new_ids": new_ids},
            },
        }
        await self._client.update_by_query(index, body)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _build_located_at_doc(
    country: CountryEntity, region: CountryEntity
) -> dict:
    """Build a located-at relationship document matching OpenCTI's schema."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    internal_id = str(uuid.uuid4())
    standard_id = f"relationship--{uuid.uuid4()}"

    return {
        "internal_id": internal_id,
        "standard_id": standard_id,
        "entity_type": "located-at",
        "parent_types": [
            "basic-relationship",
            "stix-relationship",
            "stix-core-relationship",
        ],
        "base_type": "RELATION",
        "relationship_type": "located-at",
        "description": "",
        "confidence": 100,
        "revoked": False,
        "lang": "en",
        "start_time": "1970-01-01T00:00:00.000Z",
        "stop_time": "5138-11-16T09:46:40.000Z",
        "created": now,
        "modified": now,
        "created_at": now,
        "updated_at": now,
        "id": internal_id,
        "x_opencti_stix_ids": [],
        "connections": [
            {
                "internal_id": country.internal_id,
                "name": country.name,
                "role": "located-at_from",
                "types": country.source.get(
                    "parent_types", ["Basic-Object", "Stix-Object", "Stix-Core-Object", "Stix-Domain-Object", "Location"]
                ) + [country.source.get("entity_type", "Country")],
            },
            {
                "internal_id": region.internal_id,
                "name": region.name,
                "role": "located-at_to",
                "types": region.source.get(
                    "parent_types", ["Basic-Object", "Stix-Object", "Stix-Core-Object", "Stix-Domain-Object", "Location"]
                ) + [region.source.get("entity_type", "Region")],
            },
        ],
    }
