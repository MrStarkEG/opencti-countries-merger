"""Tests for ES query builders — verify JSON structure matches Rust patterns."""

from opencti_country_merger.es.queries import (
    denormalized_ref_update,
    entities_by_type,
    entities_by_type_and_location,
    entity_by_id,
    redirect_connections,
    relationships_by_entity,
)


class TestEntityById:
    def test_structure(self) -> None:
        q = entity_by_id("abc-123")
        assert q == {
            "query": {"term": {"internal_id.keyword": "abc-123"}}
        }


class TestEntitiesByType:
    def test_structure(self) -> None:
        q = entities_by_type("Country")
        assert q == {
            "query": {"term": {"entity_type.keyword": "Country"}}
        }


class TestEntitiesByTypeAndLocation:
    def test_structure(self) -> None:
        q = entities_by_type_and_location("Location", "Country")
        assert q == {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"entity_type.keyword": "Location"}},
                        {"term": {"x_opencti_location_type.keyword": "Country"}},
                    ]
                }
            }
        }


class TestRelationshipsByEntity:
    def test_nested_query(self) -> None:
        q = relationships_by_entity("entity-456")
        assert q == {
            "query": {
                "nested": {
                    "path": "connections",
                    "query": {
                        "term": {"connections.internal_id.keyword": "entity-456"}
                    },
                }
            }
        }


class TestRedirectConnections:
    def test_basic(self) -> None:
        q = redirect_connections("src-1", "tgt-2", "US")
        assert q["query"]["nested"]["path"] == "connections"
        nested_query = q["query"]["nested"]["query"]
        # Should be a bool.must with a single term
        assert nested_query["bool"]["must"][0] == {
            "term": {"connections.internal_id.keyword": "src-1"}
        }
        assert len(nested_query["bool"]["must"]) == 1
        assert q["script"]["params"]["source_id"] == "src-1"
        assert q["script"]["params"]["target_id"] == "tgt-2"
        assert q["script"]["params"]["target_name"] == "US"
        assert "conn.internal_id = params.target_id" in q["script"]["source"]
        assert "conn.name = params.target_name" in q["script"]["source"]

    def test_inferred_only(self) -> None:
        q = redirect_connections("src-1", "tgt-2", "US", inferred_only=True)
        must_clauses = q["query"]["nested"]["query"]["bool"]["must"]
        assert len(must_clauses) == 2
        assert {"term": {"i_inference": True}} in must_clauses


class TestDenormalizedRefUpdate:
    def test_structure(self) -> None:
        q = denormalized_ref_update("uses", "src-1", "tgt-2")
        # Query uses the dotted field name
        assert q["query"] == {
            "term": {"rel_uses.internal_id.keyword": "src-1"}
        }
        # Script accesses via bracket notation
        assert "ctx._source['rel_uses.internal_id']" in q["script"]["source"]
        assert q["script"]["params"]["source_id"] == "src-1"
        assert q["script"]["params"]["target_id"] == "tgt-2"

    def test_different_rel_type(self) -> None:
        q = denormalized_ref_update("targets", "a", "b")
        assert "rel_targets.internal_id.keyword" in str(q["query"])
        assert "rel_targets.internal_id" in q["script"]["source"]
