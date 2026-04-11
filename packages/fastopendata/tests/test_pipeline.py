"""Tests for fastopendata.pipeline — GraphPipeline integration with pycypher."""

from __future__ import annotations

import asyncio

import pandas as pd
from fastopendata.pipeline import GraphPipeline
from fastopendata.streaming.core import StreamRecord
from fastopendata.streaming.views import IncrementalView


def _run(coro):
    """Helper to run async code in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Batch ingestion
# ---------------------------------------------------------------------------


class TestBatchIngestion:
    def test_add_entity_dataframe(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        assert "Person" in pipeline.entity_types
        assert pipeline.entity_count("Person") == 2

    def test_add_relationship_dataframe(self) -> None:
        rels = pd.DataFrame({"from_id": [1], "to_id": [2], "since": [2020]})
        pipeline = GraphPipeline().add_relationship_dataframe(
            "KNOWS",
            rels,
            source_col="from_id",
            target_col="to_id",
        )
        assert "KNOWS" in pipeline.relationship_types
        assert pipeline.relationship_count("KNOWS") == 1

    def test_chaining(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        cities = pd.DataFrame({"__ID__": [10, 20], "name": ["NYC", "LA"]})
        lives = pd.DataFrame({"person": [1, 2], "city": [10, 20]})

        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_entity_dataframe("City", cities)
            .add_relationship_dataframe(
                "LIVES_IN",
                lives,
                source_col="person",
                target_col="city",
            )
        )
        assert pipeline.entity_types == ["Person", "City"]
        assert pipeline.relationship_types == ["LIVES_IN"]

    def test_build_context(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["A", "B", "C"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        ctx = pipeline.build_context()
        assert ctx is not None
        assert "Person" in ctx.entity_mapping.mapping

    def test_build_star(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        star = pipeline.build_star()
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 2
        names = set(result["name"])
        assert names == {"Alice", "Bob"}

    def test_build_star_with_relationship(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        knows = pd.DataFrame(
            {"__ID__": [100], "__SOURCE__": [1], "__TARGET__": [2]}
        )
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_relationship_dataframe(
                "KNOWS",
                knows,
                source_col="__SOURCE__",
                target_col="__TARGET__",
            )
        )
        star = pipeline.build_star()
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name",
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Record-level ingestion
# ---------------------------------------------------------------------------


class TestRecordIngestion:
    def test_add_entity_records(self) -> None:
        pipeline = GraphPipeline().add_entity_records(
            "Sensor",
            [
                {"__ID__": 1, "type": "temp", "location": "A"},
                {"__ID__": 2, "type": "humidity", "location": "B"},
            ],
        )
        assert pipeline.entity_count("Sensor") == 2

    def test_add_relationship_records(self) -> None:
        pipeline = GraphPipeline().add_relationship_records(
            "MONITORS",
            [{"sensor": 1, "room": 10}, {"sensor": 2, "room": 20}],
            source_col="sensor",
            target_col="room",
        )
        assert pipeline.relationship_count("MONITORS") == 2

    def test_records_to_query(self) -> None:
        pipeline = GraphPipeline().add_entity_records(
            "Person",
            [
                {"__ID__": 1, "name": "Alice", "age": 30},
                {"__ID__": 2, "name": "Bob", "age": 25},
            ],
        )
        star = pipeline.build_star()
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name",
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Streaming view ingestion
# ---------------------------------------------------------------------------


class TestStreamingViewIngestion:
    def test_add_entity_from_view(self) -> None:
        view = IncrementalView(name="sensors")
        _run(
            view.apply(
                StreamRecord(
                    key="s1",
                    value={"__ID__": 1, "type": "temp"},
                    event_time=1.0,
                ),
            ),
        )
        _run(
            view.apply(
                StreamRecord(
                    key="s2",
                    value={"__ID__": 2, "type": "humidity"},
                    event_time=2.0,
                ),
            ),
        )

        pipeline = GraphPipeline().add_entity_from_view("Sensor", view)
        assert pipeline.entity_count("Sensor") == 2

    def test_view_to_query(self) -> None:
        view = IncrementalView(name="people")
        for i, name in enumerate(["Alice", "Bob", "Carol"]):
            _run(
                view.apply(
                    StreamRecord(
                        key=f"p{i}",
                        value={"__ID__": i + 1, "name": name},
                        event_time=float(i),
                    ),
                ),
            )

        pipeline = GraphPipeline().add_entity_from_view("Person", view)
        star = pipeline.build_star()
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 3

    def test_empty_view(self) -> None:
        view = IncrementalView(name="empty")
        pipeline = GraphPipeline().add_entity_from_view("Empty", view)
        assert pipeline.entity_count("Empty") == 0


# ---------------------------------------------------------------------------
# Stream record ingestion
# ---------------------------------------------------------------------------


class TestStreamRecordIngestion:
    def test_add_entity_from_stream_records(self) -> None:
        records = [
            StreamRecord(
                key="r1", value={"__ID__": 1, "val": 22.5}, event_time=1.0
            ),
            StreamRecord(
                key="r2", value={"__ID__": 2, "val": 18.3}, event_time=2.0
            ),
        ]
        pipeline = GraphPipeline().add_entity_from_stream_records(
            "Reading", records
        )
        assert pipeline.entity_count("Reading") == 2

    def test_stream_records_to_query(self) -> None:
        records = [
            StreamRecord(
                key=f"p{i}",
                value={"__ID__": i + 1, "name": name, "score": score},
                event_time=float(i),
            )
            for i, (name, score) in enumerate(
                [("Alice", 95), ("Bob", 80), ("Carol", 90)],
            )
        ]
        pipeline = GraphPipeline().add_entity_from_stream_records(
            "Student", records
        )
        star = pipeline.build_star()
        result = star.execute_query(
            "MATCH (s:Student) WHERE s.score > 85 RETURN s.name",
        )
        assert len(result) == 2
        names = set(result["name"])
        assert names == {"Alice", "Carol"}

    def test_empty_stream_records(self) -> None:
        pipeline = GraphPipeline().add_entity_from_stream_records("Empty", [])
        assert pipeline.entity_count("Empty") == 0


# ---------------------------------------------------------------------------
# Inspection helpers
# ---------------------------------------------------------------------------


class TestInspection:
    def test_entity_count_unknown_type(self) -> None:
        pipeline = GraphPipeline()
        assert pipeline.entity_count("Unknown") == 0

    def test_relationship_count_unknown_type(self) -> None:
        pipeline = GraphPipeline()
        assert pipeline.relationship_count("Unknown") == 0

    def test_entity_types_empty(self) -> None:
        pipeline = GraphPipeline()
        assert pipeline.entity_types == []

    def test_relationship_types_empty(self) -> None:
        pipeline = GraphPipeline()
        assert pipeline.relationship_types == []


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestSchemaValidation:
    def test_no_registry_no_validation(self) -> None:
        """Without a registry, schema validation is skipped."""
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", df)
        assert pipeline.entity_count("Person") == 2

    def test_registry_registers_schema(self) -> None:
        """With a registry, schemas are automatically registered."""
        from fastopendata.schema_evolution import SchemaRegistry

        registry = SchemaRegistry()
        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"], "age": [30]})
        pipeline = GraphPipeline(schema_registry=registry)
        pipeline.add_entity_dataframe("Person", df)

        schema = registry.get_latest("Person")
        assert schema is not None
        assert len(schema.fields) == 3
        field_names = {f.name for f in schema.fields}
        assert field_names == {"__ID__", "name", "age"}

    def test_compatible_schema_evolution(self) -> None:
        """Adding a nullable column is backward-compatible."""
        from fastopendata.schema_evolution import SchemaRegistry

        registry = SchemaRegistry()
        pipeline = GraphPipeline(schema_registry=registry)

        df1 = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        pipeline.add_entity_dataframe("Person", df1)

        # Add a new nullable column — should be compatible
        df2 = pd.DataFrame(
            {"__ID__": [2], "name": ["Bob"], "email": [None]}
        )
        pipeline.add_entity_dataframe("Person", df2)

        history = registry.get_history("Person")
        assert history is not None
        assert history.version_count == 2

    def test_incompatible_schema_rejected(self) -> None:
        """Removing a required column is backward-incompatible."""
        import pytest
        from fastopendata.schema_evolution import (
            CompatibilityLevel,
            SchemaRegistry,
        )

        registry = SchemaRegistry(compatibility_level=CompatibilityLevel.FULL)
        pipeline = GraphPipeline(schema_registry=registry)

        df1 = pd.DataFrame({"__ID__": [1], "name": ["Alice"], "age": [30]})
        pipeline.add_entity_dataframe("Person", df1)

        # Drop a column — should be incompatible under FULL
        df2 = pd.DataFrame({"__ID__": [2], "name": ["Bob"]})
        with pytest.raises(ValueError, match="incompatible"):
            pipeline.add_entity_dataframe("Person", df2)

    def test_empty_dataframe_skips_validation(self) -> None:
        """Empty DataFrames are not validated."""
        from fastopendata.schema_evolution import SchemaRegistry

        registry = SchemaRegistry()
        pipeline = GraphPipeline(schema_registry=registry)
        pipeline.add_entity_dataframe("Empty", pd.DataFrame())

        assert registry.get_latest("Empty") is None


# ---------------------------------------------------------------------------
# Lineage auto-population
# ---------------------------------------------------------------------------


class TestLineageAutoPopulation:
    """Tests for automatic lineage graph construction during pipeline operations."""

    def test_entity_creates_source_node(self) -> None:
        """Adding an entity DataFrame creates a SOURCE lineage node."""
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", df)

        lineage = pipeline.lineage
        node = lineage.get_node("entity:Person")
        assert node is not None
        assert node.node_type.name == "SOURCE"
        assert node.name == "Person"
        assert node.metadata["rows"] == "2"
        assert node.metadata["columns"] == "2"

    def test_relationship_creates_transform_node(self) -> None:
        """Adding a relationship DataFrame creates a TRANSFORM lineage node."""
        df = pd.DataFrame({
            "src": [1, 2],
            "tgt": [3, 4],
            "weight": [0.5, 0.8],
        })
        pipeline = GraphPipeline()
        pipeline.add_relationship_dataframe(
            "KNOWS", df, source_col="src", target_col="tgt"
        )

        node = pipeline.lineage.get_node("relationship:KNOWS")
        assert node is not None
        assert node.node_type.name == "TRANSFORM"
        assert node.metadata["source_col"] == "src"
        assert node.metadata["target_col"] == "tgt"

    def test_relationship_links_to_existing_entities(self) -> None:
        """Relationship nodes get edges from already-registered entity nodes."""
        people_df = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        rel_df = pd.DataFrame({"src": [1], "tgt": [2]})

        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", people_df)
        pipeline.add_relationship_dataframe(
            "KNOWS", rel_df, source_col="src", target_col="tgt"
        )

        rel_node = pipeline.lineage.get_node("relationship:KNOWS")
        assert rel_node is not None
        parents = pipeline.lineage.get_parents("relationship:KNOWS")
        parent_ids = {p.node_id for p in parents}
        assert "entity:Person" in parent_ids

    def test_build_context_creates_sink_node(self) -> None:
        """build_context() creates a SINK node with edges from all sources."""
        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", df)
        pipeline.build_context()

        sink = pipeline.lineage.get_node("context:pycypher")
        assert sink is not None
        assert sink.node_type.name == "SINK"
        assert sink.metadata["entity_types"] == "1"

        parents = pipeline.lineage.get_parents("context:pycypher")
        parent_ids = {p.node_id for p in parents}
        assert "entity:Person" in parent_ids

    def test_build_context_links_relationships_to_sink(self) -> None:
        """build_context() creates edges from relationship nodes to sink."""
        people_df = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        rel_df = pd.DataFrame({"src": [1], "tgt": [2]})

        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", people_df)
        pipeline.add_relationship_dataframe(
            "KNOWS", rel_df, source_col="src", target_col="tgt"
        )
        pipeline.build_context()

        parents = pipeline.lineage.get_parents("context:pycypher")
        parent_ids = {p.node_id for p in parents}
        assert "entity:Person" in parent_ids
        assert "relationship:KNOWS" in parent_ids

    def test_multiple_entities_all_tracked(self) -> None:
        """Multiple entity types each get their own lineage node."""
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe(
            "Person", pd.DataFrame({"__ID__": [1], "name": ["A"]})
        )
        pipeline.add_entity_dataframe(
            "City", pd.DataFrame({"__ID__": [10], "name": ["NYC"]})
        )

        assert pipeline.lineage.get_node("entity:Person") is not None
        assert pipeline.lineage.get_node("entity:City") is not None
        assert pipeline.lineage.node_count == 2

    def test_build_context_idempotent(self) -> None:
        """Calling build_context() twice doesn't duplicate sink/edges."""
        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Person", df)

        pipeline.build_context()
        edge_count_1 = pipeline.lineage.edge_count

        pipeline.build_context()
        edge_count_2 = pipeline.lineage.edge_count

        assert edge_count_1 == edge_count_2

    def test_lineage_metadata_accuracy(self) -> None:
        """Lineage node metadata accurately reflects DataFrame dimensions."""
        df = pd.DataFrame({
            "__ID__": range(100),
            "name": [f"item_{i}" for i in range(100)],
            "value": [float(i) for i in range(100)],
        })
        pipeline = GraphPipeline()
        pipeline.add_entity_dataframe("Item", df)

        node = pipeline.lineage.get_node("entity:Item")
        assert node is not None
        assert node.metadata["rows"] == "100"
        assert node.metadata["columns"] == "3"

    def test_record_ingestion_populates_lineage(self) -> None:
        """Record-level ingestion (which delegates to add_entity_dataframe) also tracks lineage."""
        pipeline = GraphPipeline()
        pipeline.add_entity_records(
            "Sensor", [{"__ID__": 1, "type": "temp"}, {"__ID__": 2, "type": "humid"}]
        )

        node = pipeline.lineage.get_node("entity:Sensor")
        assert node is not None
        assert node.metadata["rows"] == "2"
