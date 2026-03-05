"""Auto-discover how country entities are stored in OpenCTI's Elasticsearch."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from opencti_country_merger.es.client import ESClient
from opencti_country_merger.es import queries
from opencti_country_merger.models.errors import DiscoveryError


@dataclass
class DiscoveryResult:
    entity_type: str
    location_filter: str | None
    count: int
    sample_names: list[str]


@dataclass
class CountryEntity:
    internal_id: str
    index: str
    name: str
    source: dict[str, Any] = field(repr=False)


class DiscoveryService:
    """Probes ES to determine how country entities are stored."""

    def __init__(self, client: ESClient) -> None:
        self._client = client
        self._sdo_index = f"{client.prefix}stix_domain_objects-*"
        self._discovery: DiscoveryResult | None = None

    async def discover(self) -> DiscoveryResult:
        """Try progressively looser queries to find country entities."""
        # Strategy 1: entity_type = "Country"
        body = queries.entities_by_type("Country")
        count = await self._client.count(self._sdo_index, body)
        if count > 0:
            samples = await self._sample_names(body)
            self._discovery = DiscoveryResult("Country", None, count, samples)
            return self._discovery

        # Strategy 2: entity_type = "Location" + location_type = "Country"
        body = queries.entities_by_type_and_location("Location", "Country")
        count = await self._client.count(self._sdo_index, body)
        if count > 0:
            samples = await self._sample_names(body)
            self._discovery = DiscoveryResult("Location", "Country", count, samples)
            return self._discovery

        raise DiscoveryError(
            "No country entities found. Tried entity_type='Country' and "
            "entity_type='Location' + x_opencti_location_type='Country'."
        )

    async def fetch_all_countries(self) -> list[CountryEntity]:
        """Fetch every country entity using the previously discovered strategy."""
        if self._discovery is None:
            await self.discover()
        assert self._discovery is not None

        if self._discovery.location_filter:
            body = queries.entities_by_type_and_location(
                self._discovery.entity_type, self._discovery.location_filter
            )
        else:
            body = queries.entities_by_type(self._discovery.entity_type)

        entities: list[CountryEntity] = []
        async for hit in self._client.scroll_all(self._sdo_index, body):
            entities.append(
                CountryEntity(
                    internal_id=hit["_source"]["internal_id"],
                    index=hit["_index"],
                    name=hit["_source"].get("name", ""),
                    source=hit["_source"],
                )
            )
        return entities

    async def fetch_all_regions(self) -> list[CountryEntity]:
        """Fetch every Region entity from the SDO index."""
        body = queries.entities_by_type("Region")
        entities: list[CountryEntity] = []
        async for hit in self._client.scroll_all(self._sdo_index, body):
            entities.append(
                CountryEntity(
                    internal_id=hit["_source"]["internal_id"],
                    index=hit["_index"],
                    name=hit["_source"].get("name", ""),
                    source=hit["_source"],
                )
            )
        return entities

    async def _sample_names(self, body: dict[str, Any], n: int = 5) -> list[str]:
        resp = await self._client.search(self._sdo_index, body, size=n)
        return [h["_source"].get("name", "") for h in resp["hits"]["hits"]]
