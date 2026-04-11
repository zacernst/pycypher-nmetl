"""Tests for load_available_datasets() and GraphPipeline lineage integration.

Covers two first-cycle features that shipped without test coverage:
1. ``load_available_datasets()`` — auto-discovery and CSV loading from config
2. GraphPipeline lineage auto-population during entity/relationship/context operations
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
from fastopendata.pipeline import GraphPipeline, load_available_datasets
from fastopendata.schema_evolution.lineage import LineageNode, NodeType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node_ids(nodes: list[LineageNode]) -> set[str]:
    """Extract node_id strings from a list of LineageNode objects."""
    return {n.node_id for n in nodes}


class _FakeDataset:
    """Minimal stand-in for ``DatasetConfig``."""

    def __init__(
        self,
        output_file: str | None,
        fmt: str = "CSV",
        source: str = "test",
        description: str = "test dataset",
        approx_size: str = "~1 KB",
    ) -> None:
        self.output_file = output_file
        self.format = fmt
        self.source = source
        self.description = description
        self.approx_size = approx_size


class _FakeConfig:
    """Minimal stand-in for ``Config`` used by ``load_available_datasets``."""

    def __init__(self, data_path: Path, datasets: dict[str, _FakeDataset]) -> None:
        self.data_path = data_path
        self.datasets = datasets


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a list of dicts as a CSV file."""
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _patch_config(fake_config: _FakeConfig):
    """Patch the config object used by load_available_datasets."""
    return patch("fastopendata.config.config", fake_config)


# ---------------------------------------------------------------------------
# Tests — load_available_datasets()
# ---------------------------------------------------------------------------


class TestLoadAvailableDatasets:
    """Tests for the ``load_available_datasets()`` function."""

    def test_loads_csv_dataset(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "people.csv", [{"name": "Alice"}, {"name": "Bob"}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"test_people": _FakeDataset(output_file="people.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "TestPeople" in pipeline.entity_types
        assert pipeline.entity_count("TestPeople") == 2

    def test_pascalcase_conversion(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "out.csv", [{"x": 1}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"acs_pums_1yr_persons": _FakeDataset(output_file="out.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "AcsPums1YrPersons" in pipeline.entity_types

    def test_skips_missing_file(self, tmp_path: Path) -> None:
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"gone": _FakeDataset(output_file="does_not_exist.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert pipeline.entity_types == []

    def test_skips_no_output_file(self, tmp_path: Path) -> None:
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"no_output": _FakeDataset(output_file=None)},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert pipeline.entity_types == []

    def test_skips_non_csv_format(self, tmp_path: Path) -> None:
        (tmp_path / "shapes.shp").write_text("not a shapefile")
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={
                "shapes": _FakeDataset(output_file="shapes.shp", fmt="shapefile"),
            },
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert pipeline.entity_types == []

    def test_pbf_csv_format_accepted(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "osm.csv", [{"id": 1, "name": "road"}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={
                "osm_roads": _FakeDataset(output_file="osm.csv", fmt="PBF/CSV"),
            },
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "OsmRoads" in pipeline.entity_types

    def test_max_rows_limits_loading(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "big.csv", [{"val": i} for i in range(100)])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"big_data": _FakeDataset(output_file="big.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path, max_rows=5)
        assert pipeline.entity_count("BigData") == 5

    def test_auto_detects_id_column(self, tmp_path: Path) -> None:
        _write_csv(
            tmp_path / "items.csv",
            [{"__ID__": 1, "name": "A"}, {"__ID__": 2, "name": "B"}],
        )
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"items": _FakeDataset(output_file="items.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        star = pipeline.build_star()
        result = star.execute_query("MATCH (i:Items) RETURN i.name")
        assert len(result) == 2

    def test_multiple_datasets_loaded(self, tmp_path: Path) -> None:
        _write_csv(tmp_path / "a.csv", [{"x": 1}])
        _write_csv(tmp_path / "b.csv", [{"y": 2}, {"y": 3}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={
                "dataset_a": _FakeDataset(output_file="a.csv"),
                "dataset_b": _FakeDataset(output_file="b.csv"),
            },
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert len(pipeline.entity_types) == 2
        assert pipeline.entity_count("DatasetA") == 1
        assert pipeline.entity_count("DatasetB") == 2

    def test_corrupt_csv_skipped_gracefully(self, tmp_path: Path) -> None:
        (tmp_path / "bad.csv").write_bytes(b"\xff\xfe" + b"\x00" * 100)
        _write_csv(tmp_path / "good.csv", [{"val": 1}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={
                "bad_data": _FakeDataset(output_file="bad.csv"),
                "good_data": _FakeDataset(output_file="good.csv"),
            },
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert "GoodData" in pipeline.entity_types

    def test_data_dir_override(self, tmp_path: Path) -> None:
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        _write_csv(custom_dir / "d.csv", [{"v": 1}])
        fake = _FakeConfig(
            data_path=tmp_path,
            datasets={"my_data": _FakeDataset(output_file="d.csv")},
        )
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=custom_dir)
        assert "MyData" in pipeline.entity_types

    def test_empty_config_returns_empty_pipeline(self, tmp_path: Path) -> None:
        fake = _FakeConfig(data_path=tmp_path, datasets={})
        with _patch_config(fake):
            pipeline = load_available_datasets(data_dir=tmp_path)
        assert pipeline.entity_types == []


# ---------------------------------------------------------------------------
# Tests — GraphPipeline lineage auto-population
# ---------------------------------------------------------------------------


class TestPipelineLineageIntegration:
    """Tests verifying lineage is auto-populated during pipeline operations."""

    def test_entity_creates_source_node(self) -> None:
        df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", df)
        node = pipeline.lineage.get_node("entity:Person")
        assert node is not None
        assert node.node_type == NodeType.SOURCE
        assert node.name == "Person"
        assert node.metadata["rows"] == "1"
        assert node.metadata["columns"] == "2"

    def test_relationship_creates_transform_node(self) -> None:
        rels = pd.DataFrame({"src": [1], "tgt": [2]})
        pipeline = GraphPipeline().add_relationship_dataframe(
            "KNOWS", rels, source_col="src", target_col="tgt",
        )
        node = pipeline.lineage.get_node("relationship:KNOWS")
        assert node is not None
        assert node.node_type == NodeType.TRANSFORM
        assert node.metadata["source_col"] == "src"
        assert node.metadata["target_col"] == "tgt"

    def test_relationship_links_to_existing_entities(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        rels = pd.DataFrame({"src": [1], "tgt": [2]})
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_relationship_dataframe(
                "KNOWS", rels, source_col="src", target_col="tgt",
            )
        )
        children_ids = _node_ids(pipeline.lineage.get_children("entity:Person"))
        assert "relationship:KNOWS" in children_ids

    def test_build_context_creates_sink_node(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        pipeline.build_context()
        sink = pipeline.lineage.get_node("context:pycypher")
        assert sink is not None
        assert sink.node_type == NodeType.SINK
        assert sink.metadata["entity_types"] == "1"

    def test_build_context_edges_from_entities(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["A"]})
        cities = pd.DataFrame({"__ID__": [10], "name": ["NYC"]})
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_entity_dataframe("City", cities)
        )
        pipeline.build_context()
        parent_ids = _node_ids(pipeline.lineage.get_parents("context:pycypher"))
        assert "entity:Person" in parent_ids
        assert "entity:City" in parent_ids

    def test_build_context_edges_from_relationships(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        knows = pd.DataFrame({"src": [1], "tgt": [2]})
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_relationship_dataframe(
                "KNOWS", knows, source_col="src", target_col="tgt",
            )
        )
        pipeline.build_context()
        parent_ids = _node_ids(pipeline.lineage.get_parents("context:pycypher"))
        assert "relationship:KNOWS" in parent_ids

    def test_provenance_from_sink_to_sources(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["A"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        pipeline.build_context()
        prov_ids = _node_ids(pipeline.lineage.provenance("context:pycypher"))
        assert "entity:Person" in prov_ids

    def test_impact_analysis_from_source(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["A"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        pipeline.build_context()
        impact_ids = _node_ids(pipeline.lineage.impact_analysis("entity:Person"))
        assert "context:pycypher" in impact_ids

    def test_lineage_node_count(self) -> None:
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        cities = pd.DataFrame({"__ID__": [10], "name": ["NYC"]})
        lives = pd.DataFrame({"person": [1], "city": [10]})
        pipeline = (
            GraphPipeline()
            .add_entity_dataframe("Person", people)
            .add_entity_dataframe("City", cities)
            .add_relationship_dataframe(
                "LIVES_IN", lives, source_col="person", target_col="city",
            )
        )
        pipeline.build_context()
        # 2 entity nodes + 1 relationship node + 1 context sink = 4
        assert pipeline.lineage.node_count == 4

    def test_topological_order(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["A"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        pipeline.build_context()
        order = pipeline.lineage.topological_order()
        order_ids = [n.node_id for n in order]
        source_idx = order_ids.index("entity:Person")
        sink_idx = order_ids.index("context:pycypher")
        assert source_idx < sink_idx

    def test_double_build_context_no_duplicate_sink(self) -> None:
        people = pd.DataFrame({"__ID__": [1], "name": ["A"]})
        pipeline = GraphPipeline().add_entity_dataframe("Person", people)
        pipeline.build_context()
        pipeline.build_context()
        # Should still be exactly 1 entity + 1 sink = 2 nodes
        assert pipeline.lineage.node_count == 2
