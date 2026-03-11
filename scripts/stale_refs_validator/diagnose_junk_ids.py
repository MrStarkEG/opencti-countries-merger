"""Find exact IDs for junk entries (QC, EU, ??)."""
import asyncio
from opencti_country_merger.config import Settings
from opencti_country_merger.es.client import ESClient

async def main() -> None:
    settings = Settings()
    client = ESClient(settings)
    prefix = client.prefix
    deleted_index = f"{prefix}deleted_objects"

    for name in ["QC", "EU", "??", "AN"]:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"entity_type.keyword": "Country"}},
                        {"term": {"name.keyword": name}},
                    ]
                }
            }
        }
        resp = await client.search(deleted_index, body, size=5)
        hits = resp["hits"]["hits"]
        if hits:
            for h in hits:
                src = h["_source"]
                print(f"  {name}: id={src['internal_id']} aliases={src.get('x_opencti_aliases', [])}")
        else:
            print(f"  {name}: NOT FOUND in deleted_objects")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
