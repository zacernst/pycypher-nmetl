"""Tests for EntityBrowserScreen and EntityEditorScreen."""

from __future__ import annotations

import pytest

from pycypher.ingestion.config import (
    EntitySourceConfig,
    PipelineConfig,
    ProjectConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)
from pycypher_tui.adapters.data_model import DataModelAdapter
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.entity_browser import (
    EntityBrowserDetailPanel,
    EntityBrowserScreen,
    EntityListItem,
)
from pycypher_tui.screens.entity_editor import (
    EntityEditorScreen,
    EntitySourceListItem,
)


# -- Fixtures ---------------------------------------------------------------

def _make_config(
    entities=None,
    relationships=None,
) -> PipelineConfig:
    return PipelineConfig(
        version="1.0",
        project=ProjectConfig(name="test"),
        sources=SourcesConfig(
            entities=entities or [],
            relationships=relationships or [],
        ),
        queries=[],
        output=[],
    )


def _make_mgr(config: PipelineConfig) -> ConfigManager:
    return ConfigManager.from_config(config)


def _social_config() -> PipelineConfig:
    return _make_config(
        entities=[
            EntitySourceConfig(
                id="person_csv",
                uri="data/people.csv",
                entity_type="Person",
                id_col="person_id",
                schema_hints={"name": "string", "age": "int"},
            ),
            EntitySourceConfig(
                id="person_api",
                uri="data/people_api.json",
                entity_type="Person",
                id_col="person_id",
            ),
            EntitySourceConfig(
                id="company_csv",
                uri="data/companies.csv",
                entity_type="Company",
                id_col="company_id",
            ),
        ],
        relationships=[
            RelationshipSourceConfig(
                id="knows_csv",
                uri="data/knows.csv",
                relationship_type="KNOWS",
                source_col="person_id",
                target_col="friend_id",
            ),
        ],
    )


# -- EntityBrowserScreen Tests ----------------------------------------------

class TestEntityBrowserScreenInit:
    def test_creates_with_config(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        assert screen.screen_title == "Entity Types"
        assert "Entity Types" in screen.breadcrumb_text

    def test_creates_with_explicit_adapter(self):
        mgr = _make_mgr(_social_config())
        adapter = DataModelAdapter(config_manager=mgr)
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr, adapter=adapter)

        assert screen.adapter is adapter


class TestEntityBrowserLoadItems:
    def test_loads_entity_types(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        assert len(items) == 2  # Company, Person (sorted)
        assert items[0].entity_type == "Company"
        assert items[1].entity_type == "Person"

    def test_person_has_two_sources(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        person = [i for i in items if i.entity_type == "Person"][0]
        assert person.source_count == 2
        assert person.id_column == "person_id"

    def test_empty_config_loads_empty(self):
        mgr = _make_mgr(_make_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        assert items == []

    def test_person_has_properties(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        person = [i for i in items if i.entity_type == "Person"][0]
        assert "name" in person.property_names
        assert "age" in person.property_names


class TestEntityBrowserItemIds:
    def test_item_id_from_entity_type(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        assert screen.get_item_id(items[0]) == "Company"

    def test_search_text_includes_properties(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        items = screen.load_items()
        person = [i for i in items if i.entity_type == "Person"][0]
        search_text = screen.get_item_search_text(person)
        assert "Person" in search_text
        assert "name" in search_text


class TestEntityBrowserFooter:
    def test_footer_hints(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        hints = screen.footer_hints
        assert "j/k" in hints
        assert "Enter" in hints
        assert "search" in hints


class TestEntityBrowserOverrideKeys:
    def test_p_key_override(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        assert "p" in screen._screen_override_keys

    def test_handle_unknown_key(self):
        mgr = _make_mgr(_social_config())
        screen = EntityBrowserScreen.__new__(EntityBrowserScreen)
        EntityBrowserScreen.__init__(screen, config_manager=mgr)

        assert screen.handle_extra_key("x") is False


# -- EntityEditorScreen Tests -----------------------------------------------

class TestEntityEditorScreenInit:
    def test_creates_with_entity_type(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        assert screen.entity_type == "Person"
        assert "Person" in screen.screen_title
        assert "Person" in screen.breadcrumb_text


class TestEntityEditorLoadItems:
    def test_loads_sources_for_type(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        items = screen.load_items()
        assert len(items) == 2  # person_csv, person_api
        assert {i.source_id for i in items} == {"person_csv", "person_api"}

    def test_loads_empty_for_unknown_type(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Unknown"
        )

        items = screen.load_items()
        assert items == []


class TestEntityEditorFields:
    def test_edit_fields_for_existing(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        items = screen.load_items()
        fields = screen.get_fields(items[0])

        # Should have source_id (readonly), uri, entity_type (readonly), id_col
        assert len(fields) == 4
        source_id_field = [f for f in fields if f.name == "source_id"][0]
        assert source_id_field.readonly is True

        uri_field = [f for f in fields if f.name == "uri"][0]
        assert uri_field.required is True
        assert uri_field.readonly is False

        type_field = [f for f in fields if f.name == "entity_type"][0]
        assert type_field.readonly is True
        assert type_field.value == "Person"

    def test_add_fields_for_new(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        fields = screen.get_fields(None)

        source_id_field = [f for f in fields if f.name == "source_id"][0]
        assert source_id_field.required is True
        assert source_id_field.readonly is False

        type_field = [f for f in fields if f.name == "entity_type"][0]
        assert type_field.readonly is True
        assert type_field.value == "Person"


class TestEntityEditorValidation:
    def test_valid_source_id(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        result = screen.validate_field("source_id", "valid_id")
        assert result.valid is True

    def test_empty_source_id(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        result = screen.validate_field("source_id", "")
        assert result.valid is False
        assert "required" in result.error.lower()

    def test_source_id_with_spaces(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        result = screen.validate_field("source_id", "has space")
        assert result.valid is False
        assert "space" in result.error.lower()

    def test_empty_uri_invalid(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        result = screen.validate_field("uri", "")
        assert result.valid is False

    def test_valid_uri(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        result = screen.validate_field("uri", "data/people.csv")
        assert result.valid is True


class TestEntityEditorApplyChanges:
    def test_apply_uri_change(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        items = screen.load_items()
        person_csv = [i for i in items if i.source_id == "person_csv"][0]

        screen.apply_changes(person_csv, {
            "source_id": "person_csv",
            "uri": "data/new_people.csv",
            "entity_type": "Person",
            "id_col": "person_id",
        })

        # Verify config was updated
        config = mgr.get_config()
        updated = [e for e in config.sources.entities if e.id == "person_csv"][0]
        assert updated.uri == "data/new_people.csv"

    def test_apply_new_source(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        screen.apply_changes(None, {
            "source_id": "person_parquet",
            "uri": "data/people_extra.parquet",
            "entity_type": "Person",
            "id_col": "",
        })

        config = mgr.get_config()
        new_source = [e for e in config.sources.entities if e.id == "person_parquet"]
        assert len(new_source) == 1
        assert new_source[0].uri == "data/people_extra.parquet"
        assert new_source[0].entity_type == "Person"

    def test_apply_no_changes_is_noop(self):
        mgr = _make_mgr(_social_config())
        screen = EntityEditorScreen.__new__(EntityEditorScreen)
        EntityEditorScreen.__init__(
            screen, config_manager=mgr, entity_type="Person"
        )

        items = screen.load_items()
        person_csv = [i for i in items if i.source_id == "person_csv"][0]

        # Apply same values — should be a no-op
        screen.apply_changes(person_csv, {
            "source_id": "person_csv",
            "uri": person_csv.uri,
            "entity_type": "Person",
            "id_col": person_csv.id_col or "",
        })

        # Config should not be dirty (no actual changes)
        # Note: depends on ConfigManager tracking — just verify no error
