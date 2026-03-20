"""Security tests for the Neo4j sink — Cypher injection via identifier fields.

Labels, relationship types, and property key names are interpolated into
Cypher query strings.  These tests verify that:

1. Any identifier containing a backtick is rejected at ``NodeMapping`` /
   ``RelationshipMapping`` construction time (backtick-quoting gives no
   protection when the name itself contains a backtick).
2. Empty identifiers and NUL bytes are rejected.
3. Valid identifiers (including those with spaces, hyphens, Unicode) that
   do NOT contain backticks are accepted.
4. The generated Cypher templates backtick-quote property key names so that
   names with spaces or other non-standard characters remain syntactically
   valid Cypher.
"""

from __future__ import annotations

import pytest
from pycypher.sinks.neo4j import (
    NodeMapping,
    RelationshipMapping,
    _node_merge_cypher,
    _rel_merge_cypher,
)
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BACKTICK_CASES = [
    "`",  # bare backtick
    "test`injection",  # backtick mid-name
    "`leading",  # leading backtick
    "trailing`",  # trailing backtick
    "a`; DETACH DELETE (n) //",  # realistic injection payload
]

_NUL_CASES = [
    "\x00",
    "label\x00name",
]


# ---------------------------------------------------------------------------
# NodeMapping — label validation
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.neo4j


class TestNodeMappingLabelValidation:
    @pytest.mark.parametrize("bad_label", _BACKTICK_CASES)
    def test_label_with_backtick_raises(self, bad_label: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label=bad_label, id_column="id")

    @pytest.mark.parametrize("bad_label", _NUL_CASES)
    def test_label_with_nul_raises(self, bad_label: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label=bad_label, id_column="id")

    def test_empty_label_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="", id_column="id")

    def test_valid_simple_label_accepted(self) -> None:
        m = NodeMapping(label="Person", id_column="id")
        assert m.label == "Person"

    def test_valid_label_with_spaces_accepted(self) -> None:
        # Spaces are safe inside backtick-quoted identifiers
        m = NodeMapping(label="My Person", id_column="id")
        assert m.label == "My Person"

    def test_valid_label_with_unicode_accepted(self) -> None:
        m = NodeMapping(label="Personne_FR", id_column="id")
        assert m.label == "Personne_FR"

    def test_valid_label_with_hyphen_accepted(self) -> None:
        m = NodeMapping(label="person-node", id_column="id")
        assert m.label == "person-node"


# ---------------------------------------------------------------------------
# NodeMapping — id_property validation
# ---------------------------------------------------------------------------


class TestNodeMappingIdPropertyValidation:
    @pytest.mark.parametrize("bad_prop", _BACKTICK_CASES)
    def test_id_property_with_backtick_raises(self, bad_prop: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person", id_column="id", id_property=bad_prop)

    @pytest.mark.parametrize("bad_prop", _NUL_CASES)
    def test_id_property_with_nul_raises(self, bad_prop: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person", id_column="id", id_property=bad_prop)

    def test_empty_id_property_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person", id_column="id", id_property="")

    def test_valid_id_property_accepted(self) -> None:
        m = NodeMapping(
            label="Person", id_column="id", id_property="person_id"
        )
        assert m.id_property == "person_id"

    def test_id_property_with_space_accepted(self) -> None:
        # Space is safe — property key will be backtick-quoted in Cypher
        m = NodeMapping(label="Person", id_column="id", id_property="my id")
        assert m.id_property == "my id"


# ---------------------------------------------------------------------------
# RelationshipMapping — rel_type validation
# ---------------------------------------------------------------------------


class TestRelationshipMappingRelTypeValidation:
    @pytest.mark.parametrize("bad_type", _BACKTICK_CASES)
    def test_rel_type_with_backtick_raises(self, bad_type: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type=bad_type,
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_empty_rel_type_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_valid_rel_type_accepted(self) -> None:
        m = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert m.rel_type == "KNOWS"


# ---------------------------------------------------------------------------
# RelationshipMapping — source_label / target_label validation
# ---------------------------------------------------------------------------


class TestRelationshipMappingLabelValidation:
    @pytest.mark.parametrize("bad_label", _BACKTICK_CASES)
    def test_source_label_with_backtick_raises(self, bad_label: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label=bad_label,
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    @pytest.mark.parametrize("bad_label", _BACKTICK_CASES)
    def test_target_label_with_backtick_raises(self, bad_label: str) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label=bad_label,
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_empty_source_label_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_empty_target_label_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="",
                source_id_column="src",
                target_id_column="tgt",
            )


# ---------------------------------------------------------------------------
# RelationshipMapping — id property validation
# ---------------------------------------------------------------------------


class TestRelationshipMappingIdPropertyValidation:
    @pytest.mark.parametrize("bad_prop", _BACKTICK_CASES)
    def test_source_id_property_with_backtick_raises(
        self, bad_prop: str
    ) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
                source_id_property=bad_prop,
            )

    @pytest.mark.parametrize("bad_prop", _BACKTICK_CASES)
    def test_target_id_property_with_backtick_raises(
        self, bad_prop: str
    ) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
                target_id_property=bad_prop,
            )

    def test_empty_source_id_property_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
                source_id_property="",
            )

    def test_empty_target_id_property_raises(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
                target_id_property="",
            )


# ---------------------------------------------------------------------------
# Generated Cypher — property keys must be backtick-quoted
# ---------------------------------------------------------------------------


class TestCypherTemplatePropertyQuoting:
    def test_node_merge_cypher_quotes_id_property(self) -> None:
        cypher = _node_merge_cypher("Person", "my id")
        assert "`my id`" in cypher

    def test_node_merge_cypher_quotes_simple_id_property(self) -> None:
        cypher = _node_merge_cypher("Person", "id")
        assert "`id`" in cypher

    def test_rel_merge_cypher_quotes_src_id_property(self) -> None:
        cypher = _rel_merge_cypher(
            "Person", "Person", "KNOWS", "person id", "id"
        )
        assert "`person id`" in cypher

    def test_rel_merge_cypher_quotes_tgt_id_property(self) -> None:
        cypher = _rel_merge_cypher(
            "Person", "Person", "KNOWS", "id", "target id"
        )
        assert "`target id`" in cypher

    def test_node_merge_cypher_label_is_backtick_quoted(self) -> None:
        cypher = _node_merge_cypher("My Label", "id")
        assert "`My Label`" in cypher

    def test_rel_merge_cypher_rel_type_is_backtick_quoted(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "MY REL", "id", "id")
        assert "`MY REL`" in cypher
