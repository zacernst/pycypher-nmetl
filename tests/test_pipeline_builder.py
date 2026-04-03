"""Tests for PipelineBuilder — TUI-friendly high-level pipeline configuration API.

Tests follow strict TDD methodology: written before implementation.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)


# ---------------------------------------------------------------------------
# Import the module under test (will be created)
# ---------------------------------------------------------------------------

from pycypher.ingestion.pipeline_builder import (
    PipelineBuilder,
    PipelineOperation,
    PipelineSnapshot,
    SchemaInfo,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def empty_builder() -> PipelineBuilder:
    """A fresh PipelineBuilder with no configuration."""
    return PipelineBuilder()


@pytest.fixture
def sample_config() -> PipelineConfig:
    """A minimal valid PipelineConfig for testing."""
    return PipelineConfig(
        project=None,
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="people",
                    uri="data/people.csv",
                    entity_type="Person",
                    id_col="person_id",
                ),
            ],
            relationships=[],
        ),
        queries=[
            QueryConfig(
                id="q1",
                inline="MATCH (p:Person) RETURN p.name AS name",
            ),
        ],
        output=[],
    )


@pytest.fixture
def builder_with_entity(empty_builder: PipelineBuilder) -> PipelineBuilder:
    """A builder with one entity source added."""
    empty_builder.add_entity_source(
        source_id="people",
        uri="data/people.csv",
        entity_type="Person",
        id_col="person_id",
    )
    return empty_builder


# ===========================================================================
# PipelineBuilder construction
# ===========================================================================


class TestPipelineBuilderConstruction:
    def test_empty_builder_creates_valid_config(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """A fresh builder holds a valid (empty) PipelineConfig."""
        config = empty_builder.get_config()
        assert isinstance(config, PipelineConfig)
        assert config.sources.entities == []
        assert config.sources.relationships == []
        assert config.queries == []
        assert config.output == []

    def test_from_config(self, sample_config: PipelineConfig) -> None:
        """PipelineBuilder.from_config() loads an existing configuration."""
        builder = PipelineBuilder.from_config(sample_config)
        config = builder.get_config()
        assert len(config.sources.entities) == 1
        assert config.sources.entities[0].id == "people"

    def test_from_config_is_deep_copy(
        self, sample_config: PipelineConfig
    ) -> None:
        """from_config() does not share state with the original config."""
        builder = PipelineBuilder.from_config(sample_config)
        builder.add_entity_source(
            source_id="orders",
            uri="data/orders.csv",
            entity_type="Order",
        )
        # Original config should be unmodified
        assert len(sample_config.sources.entities) == 1


# ===========================================================================
# Entity source operations
# ===========================================================================


class TestEntitySourceOperations:
    def test_add_entity_source(self, empty_builder: PipelineBuilder) -> None:
        """add_entity_source() adds an entity to the config."""
        empty_builder.add_entity_source(
            source_id="people",
            uri="data/people.csv",
            entity_type="Person",
            id_col="person_id",
        )
        config = empty_builder.get_config()
        assert len(config.sources.entities) == 1
        assert config.sources.entities[0].id == "people"
        assert config.sources.entities[0].entity_type == "Person"
        assert config.sources.entities[0].id_col == "person_id"

    def test_remove_entity_source(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """remove_entity_source() removes by source id."""
        builder_with_entity.remove_entity_source("people")
        config = builder_with_entity.get_config()
        assert len(config.sources.entities) == 0

    def test_remove_nonexistent_entity_raises(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """Removing a non-existent entity source raises KeyError."""
        with pytest.raises(KeyError, match="no_such_source"):
            empty_builder.remove_entity_source("no_such_source")

    def test_update_entity_source(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """update_entity_source() modifies fields of an existing source."""
        builder_with_entity.update_entity_source(
            "people", uri="data/people_v2.csv"
        )
        config = builder_with_entity.get_config()
        assert config.sources.entities[0].uri == "data/people_v2.csv"
        # Unchanged fields preserved
        assert config.sources.entities[0].entity_type == "Person"

    def test_duplicate_entity_source_id_raises(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """Adding a source with a duplicate id raises ValueError."""
        with pytest.raises(ValueError, match="people"):
            builder_with_entity.add_entity_source(
                source_id="people",
                uri="data/other.csv",
                entity_type="Other",
            )

    def test_list_entity_sources(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """list_entity_sources() returns summary dicts."""
        sources = builder_with_entity.list_entity_sources()
        assert len(sources) == 1
        assert sources[0]["id"] == "people"
        assert sources[0]["entity_type"] == "Person"
        assert sources[0]["uri"] == "data/people.csv"


# ===========================================================================
# Relationship source operations
# ===========================================================================


class TestRelationshipSourceOperations:
    def test_add_relationship_source(
        self, empty_builder: PipelineBuilder
    ) -> None:
        empty_builder.add_relationship_source(
            source_id="knows",
            uri="data/knows.csv",
            relationship_type="KNOWS",
            source_col="from_id",
            target_col="to_id",
        )
        config = empty_builder.get_config()
        assert len(config.sources.relationships) == 1
        assert config.sources.relationships[0].relationship_type == "KNOWS"

    def test_remove_relationship_source(
        self, empty_builder: PipelineBuilder
    ) -> None:
        empty_builder.add_relationship_source(
            source_id="knows",
            uri="data/knows.csv",
            relationship_type="KNOWS",
            source_col="from_id",
            target_col="to_id",
        )
        empty_builder.remove_relationship_source("knows")
        config = empty_builder.get_config()
        assert len(config.sources.relationships) == 0

    def test_list_relationship_sources(
        self, empty_builder: PipelineBuilder
    ) -> None:
        empty_builder.add_relationship_source(
            source_id="knows",
            uri="data/knows.csv",
            relationship_type="KNOWS",
            source_col="from_id",
            target_col="to_id",
        )
        sources = empty_builder.list_relationship_sources()
        assert len(sources) == 1
        assert sources[0]["relationship_type"] == "KNOWS"


# ===========================================================================
# Query operations
# ===========================================================================


class TestQueryOperations:
    def test_add_inline_query(self, empty_builder: PipelineBuilder) -> None:
        empty_builder.add_query(
            query_id="q1",
            inline="MATCH (p:Person) RETURN p.name",
        )
        config = empty_builder.get_config()
        assert len(config.queries) == 1
        assert config.queries[0].id == "q1"
        assert config.queries[0].inline == "MATCH (p:Person) RETURN p.name"

    def test_add_file_query(self, empty_builder: PipelineBuilder) -> None:
        empty_builder.add_query(
            query_id="q2",
            source="queries/analytics.cypher",
        )
        config = empty_builder.get_config()
        assert config.queries[0].source == "queries/analytics.cypher"

    def test_remove_query(self, empty_builder: PipelineBuilder) -> None:
        empty_builder.add_query(query_id="q1", inline="MATCH (n) RETURN n")
        empty_builder.remove_query("q1")
        assert len(empty_builder.get_config().queries) == 0

    def test_duplicate_query_id_raises(
        self, empty_builder: PipelineBuilder
    ) -> None:
        empty_builder.add_query(query_id="q1", inline="MATCH (n) RETURN n")
        with pytest.raises(ValueError, match="q1"):
            empty_builder.add_query(
                query_id="q1", inline="MATCH (m) RETURN m"
            )


# ===========================================================================
# Output operations
# ===========================================================================


class TestOutputOperations:
    def test_add_output(self, empty_builder: PipelineBuilder) -> None:
        empty_builder.add_query(query_id="q1", inline="MATCH (n) RETURN n")
        empty_builder.add_output(query_id="q1", uri="output/results.csv")
        config = empty_builder.get_config()
        assert len(config.output) == 1
        assert config.output[0].query_id == "q1"

    def test_remove_output(self, empty_builder: PipelineBuilder) -> None:
        empty_builder.add_query(query_id="q1", inline="MATCH (n) RETURN n")
        empty_builder.add_output(query_id="q1", uri="output/results.csv")
        empty_builder.remove_output("q1", "output/results.csv")
        assert len(empty_builder.get_config().output) == 0


# ===========================================================================
# Undo / redo
# ===========================================================================


class TestUndoRedo:
    def test_undo_add_entity(self, empty_builder: PipelineBuilder) -> None:
        """Undo reverts the most recent add_entity_source."""
        empty_builder.add_entity_source(
            source_id="people",
            uri="data/people.csv",
            entity_type="Person",
        )
        assert len(empty_builder.get_config().sources.entities) == 1
        empty_builder.undo()
        assert len(empty_builder.get_config().sources.entities) == 0

    def test_redo_after_undo(self, empty_builder: PipelineBuilder) -> None:
        """Redo re-applies an undone operation."""
        empty_builder.add_entity_source(
            source_id="people",
            uri="data/people.csv",
            entity_type="Person",
        )
        empty_builder.undo()
        assert len(empty_builder.get_config().sources.entities) == 0
        empty_builder.redo()
        assert len(empty_builder.get_config().sources.entities) == 1

    def test_undo_nothing_raises(self, empty_builder: PipelineBuilder) -> None:
        """Undo on an empty history raises IndexError."""
        with pytest.raises(IndexError):
            empty_builder.undo()

    def test_redo_nothing_raises(self, empty_builder: PipelineBuilder) -> None:
        """Redo with no undone operations raises IndexError."""
        with pytest.raises(IndexError):
            empty_builder.redo()

    def test_new_operation_clears_redo_stack(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """A new operation after undo clears the redo stack."""
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        empty_builder.undo()
        # New operation should clear redo
        empty_builder.add_entity_source(
            source_id="b", uri="data/b.csv", entity_type="B"
        )
        with pytest.raises(IndexError):
            empty_builder.redo()

    def test_multiple_undo(self, empty_builder: PipelineBuilder) -> None:
        """Multiple undos revert operations in reverse order."""
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        empty_builder.add_entity_source(
            source_id="b", uri="data/b.csv", entity_type="B"
        )
        assert len(empty_builder.get_config().sources.entities) == 2
        empty_builder.undo()
        assert len(empty_builder.get_config().sources.entities) == 1
        assert empty_builder.get_config().sources.entities[0].id == "a"
        empty_builder.undo()
        assert len(empty_builder.get_config().sources.entities) == 0

    def test_undo_remove_restores_entity(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """Undoing a remove_entity_source restores the removed entity."""
        builder_with_entity.remove_entity_source("people")
        assert len(builder_with_entity.get_config().sources.entities) == 0
        builder_with_entity.undo()
        assert len(builder_with_entity.get_config().sources.entities) == 1
        assert (
            builder_with_entity.get_config().sources.entities[0].id == "people"
        )

    def test_can_undo_and_can_redo(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """can_undo() and can_redo() reflect stack state."""
        assert not empty_builder.can_undo()
        assert not empty_builder.can_redo()
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        assert empty_builder.can_undo()
        assert not empty_builder.can_redo()
        empty_builder.undo()
        assert not empty_builder.can_undo()
        assert empty_builder.can_redo()


# ===========================================================================
# Snapshot / diff
# ===========================================================================


class TestSnapshotAndDiff:
    def test_snapshot_captures_state(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """snapshot() returns a PipelineSnapshot of current state."""
        snap = builder_with_entity.snapshot()
        assert isinstance(snap, PipelineSnapshot)
        assert len(snap.config.sources.entities) == 1

    def test_diff_detects_added_entity(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """diff() detects entities added between two snapshots."""
        snap_before = empty_builder.snapshot()
        empty_builder.add_entity_source(
            source_id="people",
            uri="data/people.csv",
            entity_type="Person",
        )
        diff = empty_builder.diff(snap_before)
        assert len(diff["added_entities"]) == 1
        assert diff["added_entities"][0] == "people"

    def test_diff_detects_removed_entity(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """diff() detects entities removed between two snapshots."""
        snap_before = builder_with_entity.snapshot()
        builder_with_entity.remove_entity_source("people")
        diff = builder_with_entity.diff(snap_before)
        assert len(diff["removed_entities"]) == 1
        assert diff["removed_entities"][0] == "people"

    def test_diff_no_changes(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """diff() returns empty lists when nothing changed."""
        snap = builder_with_entity.snapshot()
        diff = builder_with_entity.diff(snap)
        assert diff["added_entities"] == []
        assert diff["removed_entities"] == []
        assert diff["added_relationships"] == []
        assert diff["removed_relationships"] == []
        assert diff["added_queries"] == []
        assert diff["removed_queries"] == []

    def test_restore_from_snapshot(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """restore() reverts builder state to a previous snapshot."""
        snap = builder_with_entity.snapshot()
        builder_with_entity.add_entity_source(
            source_id="orders",
            uri="data/orders.csv",
            entity_type="Order",
        )
        assert len(builder_with_entity.get_config().sources.entities) == 2
        builder_with_entity.restore(snap)
        assert len(builder_with_entity.get_config().sources.entities) == 1


# ===========================================================================
# Validation / preview
# ===========================================================================


class TestValidationAndPreview:
    def test_validate_empty_config(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """An empty config is valid (no sources required)."""
        errors = empty_builder.validate()
        assert errors == []

    def test_validate_detects_dangling_output(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """validate() flags outputs referencing non-existent queries."""
        # Bypass normal add_output to create an invalid state
        empty_builder._config.output.append(
            OutputConfig(query_id="nonexistent", uri="out.csv")
        )
        errors = empty_builder.validate()
        assert any("nonexistent" in e for e in errors)

    def test_is_dirty_after_modification(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """is_dirty() returns True after a modification."""
        assert not empty_builder.is_dirty()
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        assert empty_builder.is_dirty()

    def test_mark_clean(self, empty_builder: PipelineBuilder) -> None:
        """mark_clean() resets the dirty flag (e.g., after save)."""
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        assert empty_builder.is_dirty()
        empty_builder.mark_clean()
        assert not empty_builder.is_dirty()


# ===========================================================================
# YAML serialization round-trip
# ===========================================================================


class TestYamlRoundTrip:
    def test_to_yaml_string(
        self, builder_with_entity: PipelineBuilder
    ) -> None:
        """to_yaml() produces a valid YAML string."""
        yaml_str = builder_with_entity.to_yaml()
        assert "Person" in yaml_str
        assert "people.csv" in yaml_str

    def test_round_trip_yaml(
        self, builder_with_entity: PipelineBuilder, tmp_path: Path
    ) -> None:
        """save() and from_yaml_file() produce equivalent configs."""
        yaml_path = tmp_path / "pipeline.yaml"
        builder_with_entity.save(yaml_path)
        loaded = PipelineBuilder.from_yaml_file(yaml_path)
        assert (
            loaded.get_config().sources.entities[0].id
            == builder_with_entity.get_config().sources.entities[0].id
        )


# ===========================================================================
# Operation history
# ===========================================================================


class TestOperationHistory:
    def test_history_records_operations(
        self, empty_builder: PipelineBuilder
    ) -> None:
        """history() returns list of performed operations."""
        empty_builder.add_entity_source(
            source_id="a", uri="data/a.csv", entity_type="A"
        )
        empty_builder.add_query(query_id="q1", inline="MATCH (n) RETURN n")
        history = empty_builder.history()
        assert len(history) == 2
        assert history[0].operation_type == "add_entity_source"
        assert history[1].operation_type == "add_query"
