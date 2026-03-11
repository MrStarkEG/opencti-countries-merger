"""Find the exact current IDs for the unmapped countries."""

import asyncio
from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient


# Names to search for in current countries
SEARCH_NAMES = [
    "Iran",
    "United States of America",
    "Germany",
    "South Korea",
    "Egypt",
    "Canada",
    "United States",
    "Madagascar",
    "Netherlands",
    "Korea",
]


async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix
    sdo_index = f"{prefix}stix_domain_objects-*"

    # Fetch ALL current countries and search manually
    body = {"query": {"term": {"entity_type.keyword": "Country"}}}
    countries = []
    async for hit in client.scroll_all(sdo_index, body):
        src = hit["_source"]
        countries.append({
            "id": src["internal_id"],
            "name": src.get("name", "?"),
            "aliases": src.get("x_opencti_aliases") or [],
        })

    print(f"Total current countries: {len(countries)}")
    print()

    for search in SEARCH_NAMES:
        print(f"Searching for: '{search}'")
        lower = search.lower()
        for c in countries:
            name_match = lower in c["name"].lower()
            alias_match = any(lower == a.lower() for a in c["aliases"])
            if name_match or alias_match:
                print(f"  -> {c['name']} (id={c['id']}) aliases={c['aliases']}")
        print()

    # Also search by alias codes
    print("Searching by alpha-2 codes:")
    for code in ["IR", "US", "DE", "KR", "EG", "CA", "MG", "NL"]:
        for c in countries:
            if code in c["aliases"]:
                print(f"  {code} -> {c['name']} (id={c['id']})")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
