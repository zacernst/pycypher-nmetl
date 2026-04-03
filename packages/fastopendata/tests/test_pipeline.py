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
