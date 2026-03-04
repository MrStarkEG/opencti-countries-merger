"""Service for normalising country names directly in Elasticsearch."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from opencti_country_merger.data.iso3166 import ISO_BY_ALPHA2, ISO_COUNTRIES
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.models.errors import ElasticsearchError
from opencti_country_merger.services.country_mapper import CountryMapper
from opencti_country_merger.services.discovery import CountryEntity


# ------------------------------------------------------------------
# Data models
# ------------------------------------------------------------------


@dataclass
class RenameAction:
    entity_id: str
    index: str
    alpha_2: str
    current_name: str
    new_name: str


@dataclass
class AliasAction:
    entity_id: str
    index: str
    alpha_2: str
    entity_name: str
    current_aliases: list[str]
    new_aliases: list[str]


@dataclass
class CreateAction:
    alpha_2: str
    name: str
    alpha_3: str


@dataclass
class FixNamesPlan:
    renames: list[RenameAction] = field(default_factory=list)
    alias_replacements: list[AliasAction] = field(default_factory=list)
    creates: list[CreateAction] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    # Concrete index + template source for creating new entities
    target_index: str = ""
    template_source: dict | None = None

    @property
    def total_actions(self) -> int:
        return len(self.renames) + len(self.alias_replacements) + len(self.creates)


@dataclass
class FixNamesResult:
    renames_ok: int = 0
    renames_failed: int = 0
    aliases_ok: int = 0
    aliases_failed: int = 0
    creates_ok: int = 0
    creates_failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_ok(self) -> int:
        return self.renames_ok + self.aliases_ok + self.creates_ok

    @property
    def total_failed(self) -> int:
        return self.renames_failed + self.aliases_failed + self.creates_failed


# ------------------------------------------------------------------
# Service
# ------------------------------------------------------------------


class FixNamesService:
    """Build and execute a plan to normalise country names via ES."""

    def __init__(self, mapper: CountryMapper) -> None:
        self._mapper = mapper

    def build_plan(self, entities: list[CountryEntity]) -> FixNamesPlan:
        """Analyse entities and produce a fix-names plan."""
        plan = FixNamesPlan()

        # Track which ISO codes already have an entity
        matched_codes: dict[str, CountryEntity] = {}

        for entity in entities:
            code = self._mapper.resolve_entity(entity.source)
            if code is None:
                continue

            iso = ISO_BY_ALPHA2.get(code)
            if iso is None:
                continue

            # Handle duplicates: warn and keep first seen
            if code in matched_codes:
                plan.warnings.append(
                    f"Duplicate entity for {code}: "
                    f"{entity.name!r} (id={entity.internal_id[:12]}...) "
                    f"— keeping {matched_codes[code].name!r}"
                )
                continue
            matched_codes[code] = entity

            # Capture first entity as template for creates
            if not plan.target_index:
                plan.target_index = entity.index
                plan.template_source = entity.source

            # Check rename
            if entity.name != iso.name:
                plan.renames.append(
                    RenameAction(
                        entity_id=entity.internal_id,
                        index=entity.index,
                        alpha_2=code,
                        current_name=entity.name,
                        new_name=iso.name,
                    )
                )

            # Check aliases — should be exactly [alpha_2] and nothing else
            existing_aliases = list(
                entity.source.get("x_opencti_aliases") or []
            )
            desired_aliases = [code]
            if existing_aliases != desired_aliases:
                plan.alias_replacements.append(
                    AliasAction(
                        entity_id=entity.internal_id,
                        index=entity.index,
                        alpha_2=code,
                        entity_name=entity.name,
                        current_aliases=existing_aliases,
                        new_aliases=desired_aliases,
                    )
                )

        # Find ISO entries with no matched entity → queue create
        for iso_entry in ISO_COUNTRIES:
            if iso_entry.alpha_2 not in matched_codes:
                plan.creates.append(
                    CreateAction(
                        alpha_2=iso_entry.alpha_2,
                        name=iso_entry.name,
                        alpha_3=iso_entry.alpha_3,
                    )
                )

        return plan

    @staticmethod
    async def execute(plan: FixNamesPlan, client: ESClient) -> FixNamesResult:
        """Execute the plan against Elasticsearch."""
        result = FixNamesResult()

        # --- renames ---
        for action in plan.renames:
            try:
                await client.update_doc(
                    action.index,
                    action.entity_id,
                    {"name": action.new_name},
                )
                result.renames_ok += 1
            except ElasticsearchError as exc:
                result.renames_failed += 1
                result.errors.append(
                    f"Rename {action.alpha_2} "
                    f"({action.current_name!r} → {action.new_name!r}): {exc}"
                )

        # --- alias replacements ---
        for action in plan.alias_replacements:
            try:
                await client.update_doc(
                    action.index,
                    action.entity_id,
                    {"x_opencti_aliases": action.new_aliases},
                )
                result.aliases_ok += 1
            except ElasticsearchError as exc:
                result.aliases_failed += 1
                result.errors.append(
                    f"Set aliases {action.new_aliases!r} on {action.alpha_2}: {exc}"
                )

        # --- creates ---
        if plan.creates and plan.template_source and plan.target_index:
            for action in plan.creates:
                try:
                    doc = _build_country_doc(
                        action.name, action.alpha_2, plan.template_source
                    )
                    await client.index_document(
                        plan.target_index, doc["internal_id"], doc
                    )
                    result.creates_ok += 1
                except ElasticsearchError as exc:
                    result.creates_failed += 1
                    result.errors.append(
                        f"Create {action.alpha_2} ({action.name!r}): {exc}"
                    )

        return result


def _build_country_doc(
    name: str, alpha_2: str, template: dict
) -> dict:
    """Build a minimal Country entity document modelled on *template*."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    internal_id = str(uuid.uuid4())
    standard_id = f"location--{uuid.uuid4()}"

    doc = {
        # Identity
        "internal_id": internal_id,
        "standard_id": standard_id,
        "entity_type": template.get("entity_type", "Country"),
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
        "x_opencti_aliases": [alpha_2],
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

    # Copy location-type field from template if present
    if "x_opencti_location_type" in template:
        doc["x_opencti_location_type"] = template["x_opencti_location_type"]

    return doc
