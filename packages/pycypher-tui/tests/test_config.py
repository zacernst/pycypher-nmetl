"""Tests for the TUI configuration management system.

Covers:
- TUI pipeline config manager (atomic save, backup, validation integration)
- Validation caching for real-time TUI feedback
- Built-in pipeline templates
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from pycypher.ingestion.config import PipelineConfig
from pycypher.ingestion.validation import ErrorCategory, ValidationResult

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import (
    PipelineTemplate,
    get_template,
    list_templates,
)
from pycypher_tui.config.validation import CachedValidator


# ===========================================================================
# ConfigManager — atomic save, backup, validation integration
# ===========================================================================


class TestConfigManager:
    """Tests for the TUI configuration manager."""

    def test_create_empty(self):
        mgr = ConfigManager()
        assert mgr.is_empty()
        assert not mgr.is_dirty()

    def test_create_from_yaml(self, tmp_path: Path):
        cfg_file = tmp_path / "pipeline.yaml"
        cfg_file.write_text(
            yaml.dump({
                "version": "1.0",
                "sources": {
                    "entities": [
                        {"id": "p", "uri": "file:///p.csv", "entity_type": "Person"}
                    ]
                },
            }),
            encoding="utf-8",
        )
        mgr = ConfigManager.from_file(cfg_file)
        assert not mgr.is_empty()
        assert mgr.get_config().sources.entities[0].id == "p"

    def test_save_creates_file(self, tmp_path: Path):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        path = tmp_path / "out.yaml"
        mgr.save(path)
        assert path.exists()
        loaded = yaml.safe_load(path.read_text())
        assert loaded["version"] == "1.0"
        assert not mgr.is_dirty()

    def test_save_creates_backup(self, tmp_path: Path):
        path = tmp_path / "pipeline.yaml"
        path.write_text("version: '1.0'\n", encoding="utf-8")

        mgr = ConfigManager.from_file(path)
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        mgr.save(path)

        backup = path.with_suffix(".yaml.bak")
        assert backup.exists()
        assert backup.read_text() == "version: '1.0'\n"

    def test_atomic_save_does_not_corrupt_on_success(self, tmp_path: Path):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        path = tmp_path / "out.yaml"
        mgr.save(path)

        loaded = yaml.safe_load(path.read_text())
        assert loaded["sources"]["entities"][0]["id"] == "p"

    def test_dirty_tracking(self):
        mgr = ConfigManager()
        assert not mgr.is_dirty()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        assert mgr.is_dirty()

    def test_undo_redo_passthrough(self):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        assert len(mgr.get_config().sources.entities) == 1
        mgr.undo()
        assert len(mgr.get_config().sources.entities) == 0
        mgr.redo()
        assert len(mgr.get_config().sources.entities) == 1

    def test_validate_returns_structured_result(self):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("q1", "file:///out.csv")
        result = mgr.validate()
        assert isinstance(result, ValidationResult)
        assert result.is_valid

    def test_validate_catches_semantic_errors(self):
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("nonexistent", "file:///out.csv")
        result = mgr.validate()
        assert not result.is_valid
        assert any(e.category == ErrorCategory.SEMANTIC for e in result.errors)

    def test_add_relationship_source(self):
        mgr = ConfigManager()
        mgr.add_relationship_source(
            "rel", "file:///r.csv", "KNOWS", "src", "tgt"
        )
        assert len(mgr.get_config().sources.relationships) == 1

    def test_remove_entity_source(self):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        mgr.remove_entity_source("p")
        assert len(mgr.get_config().sources.entities) == 0

    def test_remove_query(self):
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.remove_query("q1")
        assert len(mgr.get_config().queries) == 0

    def test_history(self):
        mgr = ConfigManager()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        hist = mgr.history()
        assert len(hist) == 2

    def test_snapshot_and_diff(self):
        mgr = ConfigManager()
        snap = mgr.snapshot()
        mgr.add_entity_source("p", "file:///p.csv", "Person")
        diff = mgr.diff(snap)
        assert "p" in diff["added_entities"]


# ===========================================================================
# CachedValidator — validation caching for TUI real-time feedback
# ===========================================================================


class TestCachedValidator:
    """Tests for validation caching."""

    def test_validate_returns_result(self):
        cfg = PipelineConfig(
            version="1.0",
            queries=[],
            output=[],
        )
        validator = CachedValidator()
        result = validator.validate(cfg)
        assert isinstance(result, ValidationResult)
        assert result.is_valid

    def test_cache_returns_same_result_for_same_config(self):
        cfg = PipelineConfig(version="1.0")
        validator = CachedValidator()
        r1 = validator.validate(cfg)
        r2 = validator.validate(cfg)
        assert r1 is r2  # same object from cache

    def test_cache_invalidated_on_config_change(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            QueryConfig,
            OutputConfig,
            SourcesConfig,
        )

        cfg1 = PipelineConfig(version="1.0")
        cfg2 = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="p", uri="file:///p.csv", entity_type="Person"
                    )
                ]
            ),
        )
        validator = CachedValidator()
        r1 = validator.validate(cfg1)
        r2 = validator.validate(cfg2)
        assert r1 is not r2

    def test_validate_field_incremental(self):
        validator = CachedValidator()
        result = validator.validate_field("version", "1.0")
        assert result.is_valid

    def test_validate_field_invalid(self):
        validator = CachedValidator()
        result = validator.validate_field("version", "bad")
        assert not result.is_valid

    def test_clear_cache(self):
        cfg = PipelineConfig(version="1.0")
        validator = CachedValidator()
        r1 = validator.validate(cfg)
        validator.clear_cache()
        r2 = validator.validate(cfg)
        assert r1 is not r2


# ===========================================================================
# Pipeline Templates
# ===========================================================================


class TestTemplates:
    """Tests for built-in pipeline templates."""

    def test_list_templates_returns_nonempty(self):
        templates = list_templates()
        assert len(templates) >= 3
        assert all(isinstance(t, PipelineTemplate) for t in templates)

    def test_each_template_has_required_fields(self):
        for t in list_templates():
            assert t.name
            assert t.description
            assert t.category

    def test_get_template_by_name(self):
        templates = list_templates()
        first = templates[0]
        found = get_template(first.name)
        assert found is not None
        assert found.name == first.name

    def test_get_nonexistent_template_returns_none(self):
        assert get_template("nonexistent_template_xyz") is None

    def test_csv_analytics_template_exists(self):
        t = get_template("csv_analytics")
        assert t is not None
        assert "csv" in t.name.lower() or "csv" in t.description.lower()

    def test_template_instantiate_returns_valid_config(self):
        t = get_template("csv_analytics")
        assert t is not None
        config = t.instantiate(
            project_name="test_project",
            data_dir="data",
        )
        assert isinstance(config, PipelineConfig)
        assert config.project is not None
        assert config.project.name == "test_project"
        assert len(config.sources.entities) > 0

    def test_ecommerce_template(self):
        t = get_template("ecommerce_pipeline")
        assert t is not None
        config = t.instantiate(
            project_name="shop",
            data_dir="data",
        )
        assert len(config.sources.entities) >= 2
        assert len(config.queries) >= 1

    def test_social_network_template(self):
        t = get_template("social_network")
        assert t is not None
        config = t.instantiate(
            project_name="social",
            data_dir="data",
        )
        assert len(config.sources.relationships) >= 1

    def test_template_instantiate_with_custom_params(self):
        t = get_template("csv_analytics")
        assert t is not None
        config = t.instantiate(
            project_name="custom",
            data_dir="/custom/path",
        )
        # URI should contain the custom data directory
        assert any(
            "/custom/path" in s.uri
            for s in config.sources.entities
        )
