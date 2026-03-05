"""Service for merging duplicate regions, normalising names, and creating missing ones."""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opencti_country_merger.data.regions import (
    REGIONS_BY_NORM,
    UN_REGIONS,
    RegionEntry,
    _normalize,
)
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.models.errors import ElasticsearchError
from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.merger import MergerService, MergeResult
from opencti_country_merger.services.planner import CountryCluster


# ------------------------------------------------------------------
# Data models
# ------------------------------------------------------------------


@dataclass
class RegionMergeGroup:
    """A group of region entities that normalise to the same key."""

    norm_key: str
    canonical: RegionEntry
    entities: list[CountryEntity]
    rel_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class RegionFixAction:
    """A name / alias update for an existing region entity."""

    entity_id: str
    index: str
    current_name: str
    new_name: str
    new_aliases: list[str]


@dataclass
class RegionCreateAction:
    """A missing region that needs to be created."""

    name: str
    m49_code: str


@dataclass
class RegionPlan:
    merge_groups: list[RegionMergeGroup] = field(default_factory=list)
    fixes: list[RegionFixAction] = field(default_factory=list)
    creates: list[RegionCreateAction] = field(default_factory=list)
    junk: list[CountryEntity] = field(default_factory=list)
    # Template for creating new region docs
    target_index: str = ""
    template_source: dict | None = None

    @property
    def total_merges(self) -> int:
        return sum(
            len(g.entities) - 1 for g in self.merge_groups if len(g.entities) > 1
        )

    @property
    def total_actions(self) -> int:
        return self.total_merges + len(self.fixes) + len(self.creates) + len(self.junk)


@dataclass
class RegionResult:
    merges_ok: int = 0
    merges_failed: int = 0
    fixes_ok: int = 0
    fixes_failed: int = 0
    creates_ok: int = 0
    creates_failed: int = 0
    junk_ok: int = 0
    junk_failed: int = 0
    errors: list[str] = field(default_factory=list)
    merge_results: list[MergeResult] = field(default_factory=list)

    @property
    def total_ok(self) -> int:
        return self.merges_ok + self.fixes_ok + self.creates_ok + self.junk_ok

    @property
    def total_failed(self) -> int:
        return self.merges_failed + self.fixes_failed + self.creates_failed + self.junk_failed


# ------------------------------------------------------------------
# Service
# ------------------------------------------------------------------


class FixRegionsService:
    """Build and execute a plan to deduplicate and normalise Region entities."""

    @staticmethod
    def build_plan(
        entities: list[CountryEntity],
        rel_counts: dict[str, int],
    ) -> RegionPlan:
        plan = RegionPlan()

        # Capture template from first entity
        if entities:
            plan.target_index = entities[0].index
            plan.template_source = entities[0].source

        # Group entities by normalised name
        groups: dict[str, list[CountryEntity]] = defaultdict(list)
        for entity in entities:
            key = _normalize(entity.name)
            groups[key].append(entity)

        # Walk groups and classify
        seen_norm_keys: set[str] = set()
        for key, members in groups.items():
            region = REGIONS_BY_NORM.get(key)
            if region is None:
                # Not in our reference list — junk if 0 relationships
                for m in members:
                    if rel_counts.get(m.internal_id, 0) == 0:
                        plan.junk.append(m)
                    # Entities with relationships but not in ref list are left alone
                continue

            seen_norm_keys.add(key)

            if len(members) > 1:
                # Duplicate group — needs merge
                group = RegionMergeGroup(
                    norm_key=key,
                    canonical=region,
                    entities=members,
                    rel_counts={m.internal_id: rel_counts.get(m.internal_id, 0) for m in members},
                )
                plan.merge_groups.append(group)
            else:
                # Single entity — check if it needs name/alias fix
                entity = members[0]
                desired_aliases = [region.m49_code] if region.m49_code else []
                current_aliases = list(entity.source.get("x_opencti_aliases") or [])
                needs_rename = entity.name != region.name
                needs_alias_fix = current_aliases != desired_aliases

                if needs_rename or needs_alias_fix:
                    plan.fixes.append(
                        RegionFixAction(
                            entity_id=entity.internal_id,
                            index=entity.index,
                            current_name=entity.name,
                            new_name=region.name,
                            new_aliases=desired_aliases,
                        )
                    )

        # Find missing regions (not seen in any group)
        for region in UN_REGIONS:
            if _normalize(region.name) not in seen_norm_keys:
                plan.creates.append(
                    RegionCreateAction(name=region.name, m49_code=region.m49_code)
                )

        return plan

    @staticmethod
    async def execute_merges(
        plan: RegionPlan,
        merger: MergerService,
    ) -> list[MergeResult]:
        """Build CountryCluster objects and delegate to MergerService."""
        results: list[MergeResult] = []
        for group in plan.merge_groups:
            if len(group.entities) < 2:
                continue

            # Sort: most relationships first, earliest created_at as tiebreaker
            sorted_entities = sorted(
                group.entities,
                key=lambda e: (
                    -group.rel_counts.get(e.internal_id, 0),
                    e.source.get("created_at", ""),
                ),
            )
            target = sorted_entities[0]
            sources = sorted_entities[1:]

            cluster = CountryCluster(
                iso_code=group.norm_key,
                country_name=group.canonical.name,
                alpha_3="",
                target_entity=target,
                source_entities=sources,
                target_rel_count=group.rel_counts.get(target.internal_id, 0),
                source_rel_counts={
                    e.internal_id: group.rel_counts.get(e.internal_id, 0)
                    for e in sources
                },
            )
            result = await merger.merge_cluster(cluster)
            results.append(result)
        return results

    @staticmethod
    async def execute_fixes(
        plan: RegionPlan,
        client: ESClient,
    ) -> RegionResult:
        """Apply name + alias updates to existing region entities."""
        result = RegionResult()
        for fix in plan.fixes:
            try:
                fields: dict = {"name": fix.new_name, "x_opencti_aliases": fix.new_aliases}
                await client.update_doc(fix.index, fix.entity_id, fields)
                result.fixes_ok += 1
            except ElasticsearchError as exc:
                result.fixes_failed += 1
                result.errors.append(
                    f"Fix {fix.current_name!r} → {fix.new_name!r}: {exc}"
                )
        return result

    @staticmethod
    async def execute_creates(
        plan: RegionPlan,
        client: ESClient,
    ) -> RegionResult:
        """Create missing region entities."""
        result = RegionResult()
        if not plan.creates or not plan.template_source or not plan.target_index:
            return result

        for action in plan.creates:
            try:
                doc = _build_region_doc(
                    action.name, action.m49_code, plan.template_source
                )
                await client.index_document(plan.target_index, doc["internal_id"], doc)
                result.creates_ok += 1
            except ElasticsearchError as exc:
                result.creates_failed += 1
                result.errors.append(f"Create {action.name!r}: {exc}")
        return result

    @staticmethod
    async def execute_junk(
        plan: RegionPlan,
        merger: MergerService,
    ) -> RegionResult:
        """Delete junk region entities (0 relationships, not in ref list)."""
        result = RegionResult()
        if not plan.junk:
            return result
        junk_results = await merger.delete_junk_entities(plan.junk)
        for jr in junk_results:
            if jr.errors:
                result.junk_failed += 1
                result.errors.extend(jr.errors)
            else:
                result.junk_ok += 1
        return result


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _build_region_doc(name: str, m49_code: str, template: dict) -> dict:
    """Build a minimal Region entity document modelled on *template*."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    internal_id = str(uuid.uuid4())
    standard_id = f"location--{uuid.uuid4()}"

    aliases = [m49_code] if m49_code else []

    doc = {
        # Identity
        "internal_id": internal_id,
        "standard_id": standard_id,
        "entity_type": "Region",
        "parent_types": template.get(
            "parent_types",
            [
                "Basic-Object",
                "Stix-Object",
                "Stix-Core-Object",
                "Stix-Domain-Object",
                "Location",
            ],
        ),
        "base_type": "ENTITY",
        # Content
        "name": name,
        "x_opencti_aliases": aliases,
        "aliases": [],
        "description": "",
        # Timestamps
        "created_at": now,
        "updated_at": now,
        "created": now,
        "modified": now,
        # IDs / stix
        "i_aliases_ids": [standard_id],
        "x_opencti_stix_ids": [],
    }

    if "x_opencti_location_type" in template:
        doc["x_opencti_location_type"] = template["x_opencti_location_type"]

    return doc
