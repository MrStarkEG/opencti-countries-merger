"""Async Elasticsearch client with concurrency helpers."""

from typing import Any, AsyncGenerator

import asyncio
from elasticsearch import AsyncElasticsearch, NotFoundError

from opencti_country_merger.config import Settings
from opencti_country_merger.models.errors import ElasticsearchError

# Max concurrent ES requests to avoid overwhelming the cluster.
_DEFAULT_CONCURRENCY = 10


class ESClient:
    """Async Elasticsearch client with convenience helpers."""

    def __init__(self, settings: Settings) -> None:
        kwargs: dict[str, Any] = {
            "hosts": [settings.elasticsearch_url],
            "request_timeout": 60,
        }
        if settings.elasticsearch_username:
            kwargs["basic_auth"] = (
                settings.elasticsearch_username,
                settings.elasticsearch_password,
            )
        if not settings.elasticsearch_ssl_verify:
            kwargs["verify_certs"] = False
            kwargs["ssl_show_warn"] = False

        self._es = AsyncElasticsearch(**kwargs)
        self._prefix = settings.elasticsearch_index_prefix
        self._sem = asyncio.Semaphore(_DEFAULT_CONCURRENCY)

    @property
    def prefix(self) -> str:
        return self._prefix

    async def close(self) -> None:
        await self._es.close()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def health_check(self) -> dict[str, Any]:
        try:
            return await self._es.cluster.health()  # type: ignore[return-value]
        except Exception as exc:
            raise ElasticsearchError("health_check", exc) from exc

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(
        self,
        index: str,
        body: dict[str, Any],
        size: int = 10,
    ) -> dict[str, Any]:
        async with self._sem:
            try:
                merged = {**body, "size": size}
                return await self._es.search(index=index, body=merged)  # type: ignore[return-value]
            except Exception as exc:
                raise ElasticsearchError(f"search on {index}", exc) from exc

    async def count(self, index: str, body: dict[str, Any] | None = None) -> int:
        async with self._sem:
            try:
                kwargs: dict[str, Any] = {"index": index}
                if body and "query" in body:
                    kwargs["body"] = {"query": body["query"]}
                resp = await self._es.count(**kwargs)
                return resp["count"]  # type: ignore[return-value]
            except Exception as exc:
                raise ElasticsearchError(f"count on {index}", exc) from exc

    async def scroll_all(
        self,
        index: str,
        body: dict[str, Any],
        size: int = 500,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield every matching document using the search-after API."""
        sort_body = {**body, "sort": [{"internal_id.keyword": "asc"}]}
        while True:
            resp = await self.search(index, sort_body, size=size)
            hits = resp["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                yield hit
            sort_body["search_after"] = hits[-1]["sort"]

    # ------------------------------------------------------------------
    # Multi-search (batch counts)
    # ------------------------------------------------------------------

    async def msearch_counts(
        self, requests: list[tuple[str, dict[str, Any]]]
    ) -> list[int]:
        """Execute multiple count queries in a single ``_msearch`` call.

        *requests* is a list of ``(index, query_body)`` tuples.
        Returns a list of counts in the same order.
        """
        if not requests:
            return []

        body_lines: list[dict[str, Any]] = []
        for index, qbody in requests:
            body_lines.append({"index": index})
            merged = {**qbody, "size": 0}
            if "query" not in merged:
                merged["query"] = {"match_all": {}}
            body_lines.append(merged)

        async with self._sem:
            try:
                resp = await self._es.msearch(body=body_lines)
            except Exception as exc:
                raise ElasticsearchError("msearch_counts", exc) from exc

        counts: list[int] = []
        for r in resp["responses"]:
            if "error" in r:
                counts.append(0)
            else:
                counts.append(r["hits"]["total"]["value"])
        return counts

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def index_document(
        self, index: str, doc_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        async with self._sem:
            try:
                return await self._es.index(index=index, id=doc_id, body=body)  # type: ignore[return-value]
            except Exception as exc:
                raise ElasticsearchError(f"index_document {doc_id}", exc) from exc

    async def delete_document(self, index: str, doc_id: str) -> dict[str, Any] | None:
        async with self._sem:
            try:
                return await self._es.delete(index=index, id=doc_id)  # type: ignore[return-value]
            except NotFoundError:
                return None
            except Exception as exc:
                raise ElasticsearchError(f"delete_document {doc_id}", exc) from exc

    async def update_by_query(
        self,
        index: str,
        body: dict[str, Any],
    ) -> dict[str, int]:
        async with self._sem:
            try:
                resp = await self._es.update_by_query(
                    index=index,
                    body=body,
                    conflicts="proceed",
                    refresh=True,
                )
                return {
                    "updated": resp.get("updated", 0),
                    "total": resp.get("total", 0),
                    "failures": len(resp.get("failures", [])),
                }
            except Exception as exc:
                raise ElasticsearchError(f"update_by_query on {index}", exc) from exc

    async def delete_by_query(
        self,
        index: str,
        body: dict[str, Any],
    ) -> dict[str, int]:
        async with self._sem:
            try:
                resp = await self._es.delete_by_query(
                    index=index,
                    body=body,
                    conflicts="proceed",
                    refresh=True,
                )
                return {
                    "deleted": resp.get("deleted", 0),
                    "total": resp.get("total", 0),
                    "failures": len(resp.get("failures", [])),
                }
            except Exception as exc:
                raise ElasticsearchError(f"delete_by_query on {index}", exc) from exc
