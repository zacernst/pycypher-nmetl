"""Integration tests for pycypher.sinks.neo4j.

These tests require a live Neo4j instance.  They are skipped automatically
when Neo4j is unreachable or the driver package is absent.

Run against the docker-compose Neo4j service::

    make neo4j-up
    uv run pytest tests/test_neo4j_sink_integration.py -m neo4j -v

Or with the full suite while skipping Neo4j::

    uv run pytest -m "not neo4j"
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.sinks.neo4j import Neo4jSink, NodeMapping, RelationshipMapping

pytestmark = [pytest.mark.neo4j, pytest.mark.integration]


# ===========================================================================
# Helpers
# ===========================================================================


def _count_nodes(neo4j_session: Any, label: str) -> int:  # type: ignore[name-defined]
    """Return the number of nodes with *label* in the database."""
    result = neo4j_session.run(f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
    return result.single()["cnt"]


def _count_rels(neo4j_session: Any, rel_type: str) -> int:  # type: ignore[name-defined]
    """Return the number of relationships of *rel_type* in the database."""
    result = neo4j_session.run(
        f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt",
    )
    return result.single()["cnt"]


def _get_node(
    neo4j_session: Any,
    label: str,
    id_prop: str,
    id_val: Any,
) -> dict:  # type: ignore[name-defined]
    """Return the property dict of a single node identified by *id_val*."""
    result = neo4j_session.run(
        f"MATCH (n:`{label}` {{{id_prop}: $val}}) RETURN n",
        val=id_val,
    )
    record = result.single()
    if record is None:
        return {}
    return dict(record["n"])


def _get_rel(
    neo4j_session: Any,  # type: ignore[name-defined]
    rel_type: str,
    src_id: Any,  # type: ignore[name-defined]
    tgt_id: Any,  # type: ignore[name-defined]
    id_prop: str = "id",
) -> dict:
    """Return the property dict of a single relationship."""
    result = neo4j_session.run(
        f"MATCH (src {{{id_prop}: $src_id}})-[r:`{rel_type}`]->(tgt {{{id_prop}: $tgt_id}}) "
        "RETURN r",
        src_id=src_id,
        tgt_id=tgt_id,
    )
    record = result.single()
    if record is None:
        return {}
    return dict(record["r"])


# ===========================================================================
# Fixtures
# ===========================================================================


from typing import Any


@pytest.fixture
def neo4j_sink(neo4j_session: Any) -> Neo4jSink:
    """A Neo4jSink connected to the test database (graph already wiped)."""
    import os

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "pycypher")
    with Neo4jSink(uri, user, password) as sink:
        yield sink


@pytest.fixture
def persons_df() -> pd.DataFrame:
    """Five-person DataFrame."""
    return pd.DataFrame(
        {
            "pid": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 40, 35, 28],
        },
    )


@pytest.fixture
def rels_df() -> pd.DataFrame:
    """Four-relationship DataFrame (persons must exist first)."""
    return pd.DataFrame(
        {
            "src": [1, 1, 2, 3],
            "tgt": [2, 3, 3, 4],
            "since": [2018, 2019, 2020, 2021],
        },
    )


@pytest.fixture
def node_mapping() -> NodeMapping:
    """NodeMapping for persons_df."""
    return NodeMapping(
        label="Person",
        id_column="pid",
        property_columns={"name": "name", "age": "age"},
    )


@pytest.fixture
def rel_mapping() -> RelationshipMapping:
    """RelationshipMapping for rels_df."""
    return RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        source_id_column="src",
        target_id_column="tgt",
        property_columns={"since": "since"},
    )


# ===========================================================================
# Node write tests
# ===========================================================================


class TestNeo4jSinkNodesIntegration:
    """Integration tests for Neo4jSink.write_nodes."""

    def test_creates_correct_node_count(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        assert _count_nodes(neo4j_session, "Person") == 5

    def test_returns_correct_write_count(
        self,
        neo4j_sink: Neo4jSink,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        count = neo4j_sink.write_nodes(persons_df, node_mapping)
        assert count == 5

    def test_node_properties_are_correct(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        node = _get_node(neo4j_session, "Person", "id", 1)
        assert node["name"] == "Alice"
        assert node["age"] == 30

    def test_all_nodes_have_id_property(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        result = neo4j_session.run(
            "MATCH (n:Person) WHERE n.id IS NULL RETURN count(n) AS cnt",
        )
        assert result.single()["cnt"] == 0

    def test_idempotent_write_node_count_unchanged(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_nodes(persons_df, node_mapping)
        assert _count_nodes(neo4j_session, "Person") == 5

    def test_idempotent_write_properties_unchanged(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_nodes(persons_df, node_mapping)
        node = _get_node(neo4j_session, "Person", "id", 1)
        assert node["name"] == "Alice"

    def test_integer_property_type_preserved(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"pid": [1], "score": [42]})
        neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Item",
                id_column="pid",
                property_columns={"score": "score"},
            ),
        )
        node = _get_node(neo4j_session, "Item", "id", 1)
        assert node["score"] == 42
        assert isinstance(node["score"], int)

    def test_float_property_type_preserved(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"pid": [1], "ratio": [3.14]})
        neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Measure",
                id_column="pid",
                property_columns={"ratio": "ratio"},
            ),
        )
        node = _get_node(neo4j_session, "Measure", "id", 1)
        assert abs(node["ratio"] - 3.14) < 1e-6

    def test_string_property_type_preserved(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"pid": [1], "tag": ["hello"]})
        neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Tag",
                id_column="pid",
                property_columns={"label": "tag"},
            ),
        )
        node = _get_node(neo4j_session, "Tag", "id", 1)
        assert node["label"] == "hello"

    def test_null_property_not_written_to_node(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"pid": [1], "name": ["Alice"], "nickname": [None]})
        neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Person",
                id_column="pid",
                property_columns={"name": "name", "nickname": "nickname"},
            ),
        )
        node = _get_node(neo4j_session, "Person", "id", 1)
        assert "nickname" not in node

    def test_custom_id_property_used_as_merge_key(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"code": ["A", "B"], "value": [1, 2]})
        neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Code",
                id_column="code",
                id_property="code",
                property_columns={"value": "value"},
            ),
        )
        result = neo4j_session.run(
            "MATCH (n:Code {code: 'A'}) RETURN n.value AS v",
        )
        assert result.single()["v"] == 1

    def test_large_dataset_all_rows_written(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        n = 1200  # exceeds default batch_size of 500
        df = pd.DataFrame(
            {
                "pid": range(n),
                "name": [f"Person_{i}" for i in range(n)],
            },
        )
        count = neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="BigPerson",
                id_column="pid",
                property_columns={"name": "name"},
            ),
        )
        assert count == n
        assert _count_nodes(neo4j_session, "BigPerson") == n

    def test_empty_dataframe_writes_nothing(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        empty = pd.DataFrame({"pid": pd.Series([], dtype=int)})
        count = neo4j_sink.write_nodes(
            empty,
            NodeMapping(label="Empty", id_column="pid"),
        )
        assert count == 0
        assert _count_nodes(neo4j_session, "Empty") == 0

    def test_write_nodes_returns_zero_for_all_null_ids(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df = pd.DataFrame({"pid": [None, None], "name": ["A", "B"]})
        count = neo4j_sink.write_nodes(
            df,
            NodeMapping(
                label="Ghost",
                id_column="pid",
                property_columns={"name": "name"},
            ),
        )
        assert count == 0
        assert _count_nodes(neo4j_session, "Ghost") == 0

    def test_update_existing_node_properties(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        df1 = pd.DataFrame({"pid": [1], "name": ["Alice"]})
        df2 = pd.DataFrame({"pid": [1], "name": ["Alicia"]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name"},
        )
        neo4j_sink.write_nodes(df1, mapping)
        neo4j_sink.write_nodes(df2, mapping)
        node = _get_node(neo4j_session, "Person", "id", 1)
        assert node["name"] == "Alicia"
        assert _count_nodes(neo4j_session, "Person") == 1


# ===========================================================================
# Relationship write tests
# ===========================================================================


class TestNeo4jSinkRelationshipsIntegration:
    """Integration tests for Neo4jSink.write_relationships."""

    def test_creates_correct_relationship_count(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        rels_df: pd.DataFrame,
        node_mapping: NodeMapping,
        rel_mapping: RelationshipMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_relationships(rels_df, rel_mapping)
        assert _count_rels(neo4j_session, "KNOWS") == 4

    def test_returns_correct_write_count(
        self,
        neo4j_sink: Neo4jSink,
        persons_df: pd.DataFrame,
        rels_df: pd.DataFrame,
        node_mapping: NodeMapping,
        rel_mapping: RelationshipMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        count = neo4j_sink.write_relationships(rels_df, rel_mapping)
        assert count == 4

    def test_relationship_properties_are_correct(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        rels_df: pd.DataFrame,
        node_mapping: NodeMapping,
        rel_mapping: RelationshipMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_relationships(rels_df, rel_mapping)
        rel = _get_rel(neo4j_session, "KNOWS", 1, 2)
        assert rel["since"] == 2018

    def test_idempotent_relationship_count_unchanged(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        rels_df: pd.DataFrame,
        node_mapping: NodeMapping,
        rel_mapping: RelationshipMapping,
    ) -> None:
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_relationships(rels_df, rel_mapping)
        neo4j_sink.write_relationships(rels_df, rel_mapping)
        assert _count_rels(neo4j_session, "KNOWS") == 4

    def test_no_nodes_means_no_relationships_written(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        rels_df: pd.DataFrame,
        rel_mapping: RelationshipMapping,
    ) -> None:
        # Nodes are absent — MATCH will find nothing, MERGE is never reached
        neo4j_sink.write_relationships(rels_df, rel_mapping)
        assert _count_rels(neo4j_session, "KNOWS") == 0

    def test_relationship_with_no_properties(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        df = pd.DataFrame({"src": [1], "tgt": [2]})
        mapping = RelationshipMapping(
            rel_type="LINKED",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        neo4j_sink.write_nodes(persons_df, node_mapping)
        count = neo4j_sink.write_relationships(df, mapping)
        assert count == 1
        assert _count_rels(neo4j_session, "LINKED") == 1

    def test_null_property_not_written_to_relationship(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
        persons_df: pd.DataFrame,
        node_mapping: NodeMapping,
    ) -> None:
        df = pd.DataFrame({"src": [1], "tgt": [2], "note": [None]})
        mapping = RelationshipMapping(
            rel_type="TAGGED",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            property_columns={"note": "note"},
        )
        neo4j_sink.write_nodes(persons_df, node_mapping)
        neo4j_sink.write_relationships(df, mapping)
        rel = _get_rel(neo4j_session, "TAGGED", 1, 2)
        assert "note" not in rel

    def test_different_source_and_target_labels(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        persons = pd.DataFrame({"pid": [1], "name": ["Alice"]})
        cities = pd.DataFrame({"cid": [10], "city": ["NYC"]})
        rels = pd.DataFrame({"person_id": [1], "city_id": [10]})

        neo4j_sink.write_nodes(
            persons,
            NodeMapping(label="Person", id_column="pid"),
        )
        neo4j_sink.write_nodes(
            cities,
            NodeMapping(label="City", id_column="cid"),
        )
        neo4j_sink.write_relationships(
            rels,
            RelationshipMapping(
                rel_type="LIVES_IN",
                source_label="Person",
                target_label="City",
                source_id_column="person_id",
                target_id_column="city_id",
            ),
        )
        assert _count_rels(neo4j_session, "LIVES_IN") == 1

    def test_large_relationship_dataset(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        n_nodes = 100
        nodes_df = pd.DataFrame(
            {
                "pid": range(n_nodes),
                "name": [f"P{i}" for i in range(n_nodes)],
            },
        )
        # Every adjacent pair
        rels_df = pd.DataFrame(
            {
                "src": range(n_nodes - 1),
                "tgt": range(1, n_nodes),
            },
        )
        neo4j_sink.write_nodes(
            nodes_df,
            NodeMapping(label="Chain", id_column="pid"),
        )
        count = neo4j_sink.write_relationships(
            rels_df,
            RelationshipMapping(
                rel_type="NEXT",
                source_label="Chain",
                target_label="Chain",
                source_id_column="src",
                target_id_column="tgt",
            ),
        )
        assert count == n_nodes - 1
        assert _count_rels(neo4j_session, "NEXT") == n_nodes - 1


# ===========================================================================
# Full pipeline: execute_query → write
# ===========================================================================


class TestNeo4jSinkPipelineIntegration:
    """End-to-end tests: pycypher query → Neo4j sink."""

    def test_execute_query_then_write_nodes(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        """Verify pycypher result lands correctly in Neo4j."""
        import pandas as pd
        from pycypher import ContextBuilder, Star
        from pycypher.relational_models import ID_COLUMN

        raw = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 40],
            },
        )
        context = ContextBuilder().add_entity("Person", raw).build()
        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.__ID__ AS pid, p.name AS name, p.age AS age",
        )

        neo4j_sink.write_nodes(
            result,
            NodeMapping(
                label="Person",
                id_column="pid",
                property_columns={"name": "name", "age": "age"},
            ),
        )

        assert _count_nodes(neo4j_session, "Person") == 3
        node = _get_node(neo4j_session, "Person", "id", 1)
        assert node["name"] == "Alice"
        assert node["age"] == 30

    def test_execute_query_then_write_relationships(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        """Verify relationship results from pycypher land correctly in Neo4j."""
        import pandas as pd
        from pycypher import ContextBuilder, Star
        from pycypher.relational_models import (
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        )

        persons_raw = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
            },
        )
        knows_raw = pd.DataFrame(
            {
                ID_COLUMN: [10, 11],
                RELATIONSHIP_SOURCE_COLUMN: [1, 2],
                RELATIONSHIP_TARGET_COLUMN: [2, 3],
            },
        )

        context = (
            ContextBuilder()
            .add_entity("Person", persons_raw)
            .add_relationship("KNOWS", knows_raw)
            .build()
        )
        star = Star(context=context)

        # Write persons first
        persons_result = star.execute_query(
            "MATCH (p:Person) RETURN p.__ID__ AS pid, p.name AS name",
        )
        neo4j_sink.write_nodes(
            persons_result,
            NodeMapping(label="Person", id_column="pid"),
        )

        # Write relationships
        rels_result = star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.__ID__ AS src_id, b.__ID__ AS tgt_id",
        )
        neo4j_sink.write_relationships(
            rels_result,
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src_id",
                target_id_column="tgt_id",
            ),
        )

        assert _count_nodes(neo4j_session, "Person") == 3
        assert _count_rels(neo4j_session, "KNOWS") == 2

    def test_filtered_query_results_in_partial_write(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        """Only rows matching a WHERE clause should land in Neo4j."""
        import pandas as pd
        from pycypher import ContextBuilder, Star
        from pycypher.relational_models import ID_COLUMN

        raw = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Carol", "Dave"],
                "age": [30, 25, 40, 22],
            },
        )
        context = ContextBuilder().add_entity("Person", raw).build()
        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age >= 30 RETURN p.__ID__ AS pid, p.name AS name",
        )

        neo4j_sink.write_nodes(
            result,
            NodeMapping(label="Adult", id_column="pid"),
        )

        assert _count_nodes(neo4j_session, "Adult") == 2

    def test_aggregated_result_written_as_nodes(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        """Aggregation results (one row per group) can be written as nodes."""
        import pandas as pd
        from pycypher import ContextBuilder, Star
        from pycypher.relational_models import ID_COLUMN

        raw = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "department": ["Eng", "Eng", "HR", "HR"],
                "salary": [100, 120, 80, 90],
            },
        )
        context = ContextBuilder().add_entity("Employee", raw).build()
        star = Star(context=context)
        result = star.execute_query(
            "MATCH (e:Employee) RETURN e.department AS dept, count(e) AS headcount",
        )

        # Use department string as the id
        neo4j_sink.write_nodes(
            result,
            NodeMapping(
                label="Department",
                id_column="dept",
                id_property="name",
                property_columns={"headcount": "headcount"},
            ),
        )

        assert _count_nodes(neo4j_session, "Department") == 2
        dept = _get_node(neo4j_session, "Department", "name", "Eng")
        assert dept["headcount"] == 2

    def test_pipeline_idempotent_on_second_run(
        self,
        neo4j_sink: Neo4jSink,
        neo4j_session: Any,
    ) -> None:
        """Running the full pipeline twice produces no duplicate nodes."""
        import pandas as pd
        from pycypher import ContextBuilder, Star
        from pycypher.relational_models import ID_COLUMN

        raw = pd.DataFrame(
            {
                ID_COLUMN: [1, 2],
                "name": ["Alice", "Bob"],
            },
        )
        context = ContextBuilder().add_entity("Person", raw).build()

        for _ in range(2):
            star = Star(context=context)
            result = star.execute_query(
                "MATCH (p:Person) RETURN p.__ID__ AS pid, p.name AS name",
            )
            neo4j_sink.write_nodes(
                result,
                NodeMapping(
                    label="Person",
                    id_column="pid",
                    property_columns={"name": "name"},
                ),
            )

        assert _count_nodes(neo4j_session, "Person") == 2
