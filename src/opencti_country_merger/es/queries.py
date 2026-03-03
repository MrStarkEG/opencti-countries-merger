"""Elasticsearch query builders matching OpenCTI's data model.

All text fields use the `.keyword` suffix. Connections are stored as nested
objects. Painless scripts follow the exact patterns from the Rust codebase.
"""

from typing import Any


def entity_by_id(internal_id: str) -> dict[str, Any]:
    """Term query on ``internal_id.keyword``."""
    return {"query": {"term": {"internal_id.keyword": internal_id}}}


def entities_by_type(entity_type: str) -> dict[str, Any]:
    """Term query on ``entity_type.keyword``."""
    return {"query": {"term": {"entity_type.keyword": entity_type}}}


def entities_by_type_and_location(
    entity_type: str, location_type: str
) -> dict[str, Any]:
    """Combined term query for entity_type + x_opencti_location_type."""
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"entity_type.keyword": entity_type}},
                    {"term": {"x_opencti_location_type.keyword": location_type}},
                ]
            }
        }
    }


def relationships_by_entity(entity_id: str) -> dict[str, Any]:
    """Nested query on ``connections.internal_id.keyword``."""
    return {
        "query": {
            "nested": {
                "path": "connections",
                "query": {
                    "term": {"connections.internal_id.keyword": entity_id}
                },
            }
        }
    }


def redirect_connections(
    source_id: str,
    target_id: str,
    target_name: str,
    *,
    inferred_only: bool = False,
) -> dict[str, Any]:
    """Update-by-query body that redirects nested connections from source to target.

    When *inferred_only* is ``True`` an additional ``i_inference: true``
    filter is added to the nested query so only inferred relationships are
    affected (Phase 4).
    """
    nested_must: list[dict[str, Any]] = [
        {"term": {"connections.internal_id.keyword": source_id}},
    ]
    if inferred_only:
        nested_must.append({"term": {"i_inference": True}})

    return {
        "query": {
            "nested": {
                "path": "connections",
                "query": {"bool": {"must": nested_must}},
            }
        },
        "script": {
            "source": (
                "for (conn in ctx._source.connections) {"
                " if (conn.internal_id == params.source_id) {"
                " conn.internal_id = params.target_id;"
                " conn.name = params.target_name;"
                " }"
                " }"
            ),
            "params": {
                "source_id": source_id,
                "target_id": target_id,
                "target_name": target_name,
            },
        },
    }


def denormalized_ref_update(
    rel_type: str, source_id: str, target_id: str
) -> dict[str, Any]:
    """Update-by-query that patches ``rel_TYPE.internal_id`` arrays.

    The field name contains a literal dot (e.g. ``rel_uses.internal_id``)
    and must be accessed in Painless via bracket notation.
    """
    field = f"rel_{rel_type}.internal_id"
    return {
        "query": {"term": {f"{field}.keyword": source_id}},
        "script": {
            "source": (
                f"def field = ctx._source['{field}'];"
                " if (field != null) {"
                " for (int i = 0; i < field.size(); i++) {"
                " if (field[i] == params.source_id) {"
                " field[i] = params.target_id;"
                " }"
                " }"
                " }"
            ),
            "params": {
                "source_id": source_id,
                "target_id": target_id,
            },
        },
    }


def relationship_types_query() -> dict[str, Any]:
    """Aggregation to collect distinct ``relationship_type`` values."""
    return {
        "size": 0,
        "aggs": {
            "rel_types": {
                "terms": {"field": "relationship_type.keyword", "size": 500}
            }
        },
    }


def delete_relationships_by_entity(entity_id: str) -> dict[str, Any]:
    """Delete-by-query body that removes all relationships referencing *entity_id*."""
    return {
        "query": {
            "nested": {
                "path": "connections",
                "query": {
                    "term": {"connections.internal_id.keyword": entity_id}
                },
            }
        }
    }
