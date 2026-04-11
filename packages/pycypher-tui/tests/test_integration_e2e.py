"""End-to-end integration tests for the TUI pipeline configuration.

Tests complete workflows: template instantiation → config modification →
validation → screen display, verifying cross-component integration.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml
from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import (
    PipelineTemplate,
    get_template,
    list_templates,
)
from pycypher_tui.config.validation import CachedValidator
from pycypher_tui.screens.data_sources import DataSourcesScreen, SourceItem
from pycypher_tui.screens.pipeline_overview import (
    PipelineOverviewScreen,
    SectionInfo,
)
from pycypher_tui.screens.pipeline_testing import (
    ExecutionPlan,
    ExecutionStep,
    StepStatus,
    build_execution_plan,
    run_dry_execution,
)
from pycypher_tui.screens.relationships import (
    RelationshipItem,
    RelationshipScreen,
)
from pycypher_tui.screens.template_browser import (
    TemplateBrowserScreen,
    TemplateSummary,
    summarise_template,
)

# ---------------------------------------------------------------------------
# Workflow: Template → ConfigManager → Screen display
# ---------------------------------------------------------------------------


class TestTemplateToConfigWorkflow:
    """Test creating a pipeline from a template and verifying through screens."""

    def test_csv_analytics_creates_valid_config(self):
        """Template → ConfigManager → PipelineConfig round-trip."""
        t = get_template("csv_analytics")
        config = t.instantiate(project_name="test", data_dir="data")

        mgr = ConfigManager.from_config(config)
        assert not mgr.is_empty()

        retrieved = mgr.get_config()
        assert retrieved.project.name == "test"
        assert len(retrieved.sources.entities) == 2
        assert len(retrieved.queries) == 1
        assert len(retrieved.output) == 1

    def test_ecommerce_template_has_relationships_and_entities(self):
        """E-commerce template contains both entity and relationship sources."""
        t = get_template("ecommerce_pipeline")
        config = t.instantiate(project_name="shop", data_dir="/tmp/shop")

        mgr = ConfigManager.from_config(config)
        retrieved = mgr.get_config()

        assert len(retrieved.sources.entities) >= 2
        assert len(retrieved.sources.relationships) >= 1
        assert len(retrieved.queries) >= 2
        assert len(retrieved.output) >= 1

    def test_social_network_template_overview_sections(self):
        """Social network template populates all 5 overview sections."""
        t = get_template("social_network")
        config = t.instantiate()

        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []

        sections = overview._build_section_list(config)
        assert len(sections) == 6  # data_model + 5 pipeline sections (including query_lineage)

        entity_sec = sections[1]
        assert entity_sec.key == "entity_sources"
        assert entity_sec.item_count >= 1
        assert entity_sec.status == "configured"

        rel_sec = sections[2]
        assert rel_sec.key == "relationship_sources"
        assert rel_sec.item_count >= 1

        query_sec = sections[3]
        assert query_sec.key == "queries"
        assert query_sec.item_count >= 1

    def test_all_templates_produce_nonempty_overview(self):
        """Every built-in template produces at least one configured section."""
        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []

        for template in list_templates():
            config = template.instantiate()
            sections = overview._build_section_list(config)
            configured = [s for s in sections if s.status == "configured"]
            assert len(configured) >= 1, f"Template {template.name} has no configured sections"


# ---------------------------------------------------------------------------
# Workflow: ConfigManager CRUD → Screen reflection
# ---------------------------------------------------------------------------


class TestConfigManagerCRUDWorkflow:
    """Test that ConfigManager modifications reflect in screen logic."""

    def test_add_entity_reflected_in_overview(self):
        """Adding an entity source increases overview entity section count."""
        mgr = ConfigManager()
        mgr.add_entity_source("people", "data/people.csv", "Person", id_col="id")

        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []

        sections = overview._build_section_list(mgr.get_config())
        entity_section = sections[1]  # index 1 after data_model
        assert entity_section.item_count == 1
        assert entity_section.status == "configured"

    def test_add_multiple_entities_and_query(self):
        """Building a config incrementally produces valid overview."""
        mgr = ConfigManager()
        mgr.add_entity_source("customers", "data/customers.csv", "Customer", id_col="cid")
        mgr.add_entity_source("products", "data/products.csv", "Product", id_col="pid")
        mgr.add_query("q1", inline="MATCH (c:Customer) RETURN c", description="All customers")

        config = mgr.get_config()
        assert len(config.sources.entities) == 2
        assert len(config.queries) == 1

        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []

        sections = overview._build_section_list(config)
        assert sections[1].item_count == 2  # entities (index 1, after data_model)
        assert sections[3].item_count == 1  # queries (index 3, after data_model, entities, relationships)

    def test_add_relationship_reflected_in_relationship_screen(self):
        """Adding a relationship source is visible through RelationshipScreen._build_relationship_list."""
        mgr = ConfigManager()
        mgr.add_entity_source("people", "data/people.csv", "Person", id_col="person_id")
        mgr.add_relationship_source(
            "follows",
            "data/follows.csv",
            "FOLLOWS",
            source_col="person_id",
            target_col="person_id",
        )

        rel_screen = RelationshipScreen.__new__(RelationshipScreen)
        rel_screen._cursor = 0
        rel_screen._items = []
        rel_screen._pending_keys = []
        rel_screen._entity_types = {}

        items = rel_screen._build_relationship_list(mgr.get_config())
        assert len(items) == 1
        assert items[0].relationship_type == "FOLLOWS"
        assert items[0].source_entity == "Person"
        assert items[0].target_entity == "Person"

    def test_data_sources_screen_shows_both_types(self):
        """DataSourcesScreen._extract_sources returns both entity and relationship sources."""
        mgr = ConfigManager()
        mgr.add_entity_source("people", "data/people.csv", "Person", id_col="id")
        mgr.add_relationship_source(
            "follows",
            "data/follows.csv",
            "FOLLOWS",
            source_col="follower_id",
            target_col="followed_id",
        )

        ds_screen = DataSourcesScreen.__new__(DataSourcesScreen)
        ds_screen._cursor = 0
        ds_screen._sources = []
        ds_screen._filter_mode = "all"
        ds_screen._pending_keys = []
        ds_screen._search_pattern = ""

        config = mgr.get_config()
        sources = ds_screen._extract_sources(config)
        assert len(sources) == 2

        entity_sources = [s for s in sources if s.source_type == "entity"]
        rel_sources = [s for s in sources if s.source_type == "relationship"]
        assert len(entity_sources) == 1
        assert len(rel_sources) == 1

    def test_data_sources_filter_modes(self):
        """DataSourcesScreen filtering by entity/relationship works correctly."""
        mgr = ConfigManager()
        mgr.add_entity_source("e1", "data/e1.csv", "Type1", id_col="id")
        mgr.add_entity_source("e2", "data/e2.csv", "Type2", id_col="id")
        mgr.add_relationship_source(
            "r1", "data/r1.csv", "REL1",
            source_col="a", target_col="b",
        )

        ds_screen = DataSourcesScreen.__new__(DataSourcesScreen)
        ds_screen._cursor = 0
        ds_screen._sources = []
        ds_screen._pending_keys = []
        ds_screen._search_pattern = ""

        all_sources = ds_screen._extract_sources(mgr.get_config())

        # All filter
        ds_screen._filter_mode = "all"
        assert len(all_sources) == 3

        # Entity filter
        entities = [s for s in all_sources if s.source_type == "entity"]
        assert len(entities) == 2

        # Relationship filter
        rels = [s for s in all_sources if s.source_type == "relationship"]
        assert len(rels) == 1


# ---------------------------------------------------------------------------
# Workflow: Config save/load round-trip
# ---------------------------------------------------------------------------


class TestConfigPersistenceWorkflow:
    """Test saving and loading configs through ConfigManager."""

    def test_save_and_reload(self, tmp_path):
        """Config survives save/load round-trip."""
        mgr = ConfigManager()
        mgr.add_entity_source("people", "data/people.csv", "Person", id_col="id")
        mgr.add_query("q1", inline="MATCH (n:Person) RETURN n")

        filepath = tmp_path / "pipeline.yaml"
        mgr.save(str(filepath))

        loaded = ConfigManager.from_file(str(filepath))
        config = loaded.get_config()
        assert len(config.sources.entities) == 1
        assert config.sources.entities[0].entity_type == "Person"
        assert len(config.queries) == 1

    def test_template_save_and_reload(self, tmp_path):
        """Template-generated config survives save/load."""
        t = get_template("ecommerce_pipeline")
        config = t.instantiate(project_name="shop", data_dir="data")

        mgr = ConfigManager.from_config(config)
        filepath = tmp_path / "ecommerce.yaml"
        mgr.save(str(filepath))

        loaded = ConfigManager.from_file(str(filepath))
        lconfig = loaded.get_config()
        assert lconfig.project.name == "shop"
        assert len(lconfig.sources.entities) == len(config.sources.entities)
        assert len(lconfig.sources.relationships) == len(config.sources.relationships)
        assert len(lconfig.queries) == len(config.queries)

    def test_undo_redo_workflow(self):
        """ConfigManager undo/redo preserves state correctly."""
        mgr = ConfigManager()
        mgr.add_entity_source("e1", "data/e1.csv", "Entity1", id_col="id")
        assert len(mgr.get_config().sources.entities) == 1

        mgr.add_entity_source("e2", "data/e2.csv", "Entity2", id_col="id")
        assert len(mgr.get_config().sources.entities) == 2

        mgr.undo()
        assert len(mgr.get_config().sources.entities) == 1

        mgr.redo()
        assert len(mgr.get_config().sources.entities) == 2

    def test_modify_after_template_load(self, tmp_path):
        """Can modify config after loading from template."""
        t = get_template("csv_analytics")
        config = t.instantiate()

        mgr = ConfigManager.from_config(config)
        original_count = len(mgr.get_config().sources.entities)

        mgr.add_entity_source("extra", "data/extra.csv", "Extra", id_col="id")
        assert len(mgr.get_config().sources.entities) == original_count + 1


# ---------------------------------------------------------------------------
# Workflow: Validation integration
# ---------------------------------------------------------------------------


class TestValidationWorkflow:
    """Test validation across config and screen components."""

    def test_validator_on_valid_config(self):
        """CachedValidator passes valid config."""
        t = get_template("csv_analytics")
        config = t.instantiate()
        mgr = ConfigManager.from_config(config)

        validator = CachedValidator()
        result = validator.validate(mgr.get_config())
        # Valid configs should have no errors (warnings may exist)
        assert result.errors == []

    def test_validator_on_empty_config(self):
        """CachedValidator handles empty config."""
        mgr = ConfigManager()
        validator = CachedValidator()
        result = validator.validate(mgr.get_config())
        # Empty config may have warnings but shouldn't crash
        assert result is not None

    def test_relationship_validation_integrity(self):
        """Relationship screen validation detects missing entities."""
        mgr = ConfigManager()
        # Add relationship without entities
        mgr.add_relationship_source(
            "follows",
            "data/follows.csv",
            "FOLLOWS",
            source_col="a",
            target_col="b",
        )

        rel_screen = RelationshipScreen.__new__(RelationshipScreen)
        rel_screen._cursor = 0
        rel_screen._items = []
        rel_screen._pending_keys = []
        rel_screen._entity_types = {}

        items = rel_screen._build_relationship_list(mgr.get_config())
        assert len(items) == 1
        assert items[0].status == "warning"
        assert any("No entity sources" in m for m in items[0].validation_messages)

    def test_relationship_validation_with_entities(self):
        """Relationship screen shows valid when entities match."""
        mgr = ConfigManager()
        mgr.add_entity_source("people", "data/people.csv", "Person", id_col="person_id")
        mgr.add_entity_source("companies", "data/co.csv", "Company", id_col="company_id")
        mgr.add_relationship_source(
            "works_at",
            "data/works_at.csv",
            "WORKS_AT",
            source_col="person_id",
            target_col="company_id",
        )

        rel_screen = RelationshipScreen.__new__(RelationshipScreen)
        rel_screen._cursor = 0
        rel_screen._items = []
        rel_screen._pending_keys = []
        rel_screen._entity_types = {}

        items = rel_screen._build_relationship_list(mgr.get_config())
        assert items[0].status == "valid"
        assert items[0].source_entity == "Person"
        assert items[0].target_entity == "Company"


# ---------------------------------------------------------------------------
# Workflow: Template browser → selection → config creation
# ---------------------------------------------------------------------------


class TestTemplateBrowserWorkflow:
    """Test browsing templates and creating configs from them."""

    def test_all_templates_appear_in_browser(self):
        """All registered templates are summarised for the browser."""
        templates = list_templates()
        summaries = [summarise_template(t) for t in templates]
        assert len(summaries) == len(templates)
        assert all(isinstance(s, TemplateSummary) for s in summaries)

    def test_selected_template_creates_config(self):
        """Selecting a template name can create a config."""
        # Simulate user selecting a template in the browser
        browser = TemplateBrowserScreen.__new__(TemplateBrowserScreen)
        browser._cursor = 0
        browser._items = [summarise_template(t) for t in list_templates()]
        browser._pending_keys = []
        browser._category_filter = None

        selected = browser.current_template
        assert selected is not None

        # Use the name to get the actual template and instantiate
        template = get_template(selected.name)
        assert template is not None

        config = template.instantiate(project_name="from_browser")
        assert isinstance(config, PipelineConfig)
        assert config.project.name == "from_browser"

    def test_category_filter_reduces_list(self):
        """Category filtering in browser reduces visible templates."""
        all_summaries = [summarise_template(t) for t in list_templates()]
        categories = {s.category for s in all_summaries}

        for cat in categories:
            filtered = [s for s in all_summaries if s.category == cat]
            assert len(filtered) < len(all_summaries) or len(categories) == 1


# ---------------------------------------------------------------------------
# Workflow: Pipeline execution plan
# ---------------------------------------------------------------------------


class TestPipelineExecutionWorkflow:
    """Test building and running execution plans."""

    def test_build_plan_from_template_config(self):
        """Execution plan built from template config has expected steps."""
        t = get_template("csv_analytics")
        config = t.instantiate()
        mgr = ConfigManager.from_config(config)

        plan = build_execution_plan(mgr)
        assert isinstance(plan, ExecutionPlan)
        assert len(plan.steps) > 0

    def test_build_plan_from_ecommerce(self):
        """E-commerce template plan includes load and query steps."""
        t = get_template("ecommerce_pipeline")
        config = t.instantiate()
        mgr = ConfigManager.from_config(config)

        plan = build_execution_plan(mgr)
        step_types = {s.step_type for s in plan.steps}
        assert "load" in step_types or "query" in step_types

    def test_dry_execution_on_valid_config(self):
        """Dry execution runs without crashing on valid template config."""
        t = get_template("csv_analytics")
        config = t.instantiate()
        mgr = ConfigManager.from_config(config)

        plan = build_execution_plan(mgr)
        result = run_dry_execution(mgr)
        assert isinstance(result, ExecutionPlan)
        # All steps should have been processed (success or error)
        for step in result.steps:
            assert step.status != StepStatus.RUNNING

    def test_empty_config_plan(self):
        """Empty config produces empty or minimal plan."""
        mgr = ConfigManager()
        plan = build_execution_plan(mgr)
        assert isinstance(plan, ExecutionPlan)


# ---------------------------------------------------------------------------
# Cross-screen navigation consistency
# ---------------------------------------------------------------------------


class TestCrossScreenConsistency:
    """Test that config state is consistent across different screen views."""

    def test_overview_and_data_sources_agree_on_count(self):
        """Overview entity count matches data sources entity filter count."""
        t = get_template("ecommerce_pipeline")
        config = t.instantiate()

        # Overview section count
        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []
        sections = overview._build_section_list(config)
        entity_count = sections[1].item_count  # index 1 after data_model
        rel_count = sections[2].item_count  # index 2 after data_model, entities

        # Data sources extraction
        ds_screen = DataSourcesScreen.__new__(DataSourcesScreen)
        ds_screen._cursor = 0
        ds_screen._sources = []
        ds_screen._filter_mode = "all"
        ds_screen._pending_keys = []
        ds_screen._search_pattern = ""
        sources = ds_screen._extract_sources(config)

        ds_entities = [s for s in sources if s.source_type == "entity"]
        ds_rels = [s for s in sources if s.source_type == "relationship"]

        assert entity_count == len(ds_entities)
        assert rel_count == len(ds_rels)

    def test_overview_and_relationship_screen_agree(self):
        """Overview relationship count matches relationship screen."""
        t = get_template("social_network")
        config = t.instantiate()

        # Overview
        overview = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        overview._cursor = 0
        overview._sections = []
        overview._pending_keys = []
        sections = overview._build_section_list(config)
        rel_count = sections[2].item_count  # index 2 after data_model, entities

        # Relationship screen
        rel_screen = RelationshipScreen.__new__(RelationshipScreen)
        rel_screen._cursor = 0
        rel_screen._items = []
        rel_screen._pending_keys = []
        rel_screen._entity_types = {}
        items = rel_screen._build_relationship_list(config)

        assert rel_count == len(items)

    def test_template_summary_matches_actual_config(self):
        """Template summary counts match actual instantiated config."""
        for template in list_templates():
            summary = summarise_template(template)
            config = template.instantiate()

            assert summary.entity_count == len(config.sources.entities)
            assert summary.relationship_count == len(config.sources.relationships)
            assert summary.query_count == len(config.queries)
            assert summary.output_count == len(config.output)


# ---------------------------------------------------------------------------
# PyCypher config integration
# ---------------------------------------------------------------------------


class TestPyCypherConfigIntegration:
    """Test that TUI config objects properly integrate with PyCypher types."""

    def test_entity_source_config_fields(self):
        """EntitySourceConfig created through ConfigManager has all required fields."""
        mgr = ConfigManager()
        mgr.add_entity_source(
            "people",
            "data/people.csv",
            "Person",
            id_col="person_id",
        )
        entity = mgr.get_config().sources.entities[0]
        assert isinstance(entity, EntitySourceConfig)
        assert entity.id == "people"
        assert entity.uri == "data/people.csv"
        assert entity.entity_type == "Person"
        assert entity.id_col == "person_id"

    def test_relationship_source_config_fields(self):
        """RelationshipSourceConfig has all required fields."""
        mgr = ConfigManager()
        mgr.add_relationship_source(
            "follows",
            "data/follows.csv",
            "FOLLOWS",
            source_col="follower_id",
            target_col="followed_id",
        )
        rel = mgr.get_config().sources.relationships[0]
        assert isinstance(rel, RelationshipSourceConfig)
        assert rel.id == "follows"
        assert rel.relationship_type == "FOLLOWS"
        assert rel.source_col == "follower_id"
        assert rel.target_col == "followed_id"

    def test_query_config_fields(self):
        """QueryConfig created through ConfigManager has expected structure."""
        mgr = ConfigManager()
        mgr.add_query(
            "q1",
            inline="MATCH (n:Person) RETURN n.name",
            description="Get person names",
        )
        query = mgr.get_config().queries[0]
        assert isinstance(query, QueryConfig)
        assert query.id == "q1"
        assert "Person" in query.inline
        assert query.description == "Get person names"

    def test_output_config_fields(self):
        """OutputConfig created through ConfigManager has expected structure."""
        mgr = ConfigManager()
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("q1", "output/results.csv")
        output = mgr.get_config().output[0]
        assert isinstance(output, OutputConfig)
        assert output.query_id == "q1"
        assert output.uri == "output/results.csv"

    def test_config_serialization_roundtrip(self, tmp_path):
        """PipelineConfig serializes to YAML and deserializes correctly."""
        mgr = ConfigManager()
        mgr.add_entity_source("e1", "data/e1.csv", "Type1", id_col="id")
        mgr.add_relationship_source(
            "r1", "data/r1.csv", "REL1",
            source_col="a", target_col="b",
        )
        mgr.add_query("q1", inline="MATCH (n) RETURN n")
        mgr.add_output("q1", "out.csv")

        filepath = tmp_path / "test.yaml"
        mgr.save(str(filepath))

        content = filepath.read_text()
        assert "Type1" in content
        assert "REL1" in content

        loaded = ConfigManager.from_file(str(filepath))
        lconfig = loaded.get_config()
        assert len(lconfig.sources.entities) == 1
        assert len(lconfig.sources.relationships) == 1
        assert len(lconfig.queries) == 1
        assert len(lconfig.output) == 1
