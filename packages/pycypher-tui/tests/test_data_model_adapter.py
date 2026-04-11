"""Tests for DataModelAdapter and view models."""

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
from pycypher_tui.adapters.view_models import (
    ColumnMappingViewModel,
    EntityDetailViewModel,
    EntitySourceViewModel,
    EntityViewModel,
    ModelStatsViewModel,
    PropertyViewModel,
    RelationshipDetailViewModel,
    RelationshipSourceViewModel,
    RelationshipViewModel,
    SourceMappingViewModel,
    ValidationIssue,
)
from pycypher_tui.config.pipeline import ConfigManager

# -- Fixtures ---------------------------------------------------------------

def _make_config(
    entities: list[EntitySourceConfig] | None = None,
    relationships: list[RelationshipSourceConfig] | None = None,
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


def _make_adapter(config: PipelineConfig) -> DataModelAdapter:
    mgr = ConfigManager.from_config(config)
    return DataModelAdapter(config_manager=mgr)


# -- View Model Immutability -----------------------------------------------

class TestViewModelImmutability:
    def test_entity_view_model_frozen(self):
        vm = EntityViewModel(
            entity_type="Person",
            source_count=1,
            property_names=("name", "age"),
            id_column="id",
        )
        with pytest.raises(AttributeError):
            vm.entity_type = "Other"  # type: ignore[misc]

    def test_relationship_view_model_frozen(self):
        vm = RelationshipViewModel(
            relationship_type="KNOWS",
            source_entity="Person",
            target_entity="Person",
            source_count=1,
            column_mappings=(),
            validation_status="valid",
        )
        with pytest.raises(AttributeError):
            vm.relationship_type = "Other"  # type: ignore[misc]

    def test_validation_issue_frozen(self):
        issue = ValidationIssue(level="error", message="test")
        with pytest.raises(AttributeError):
            issue.level = "warning"  # type: ignore[misc]

    def test_model_stats_frozen(self):
        stats = ModelStatsViewModel(
            entity_type_count=3,
            relationship_type_count=2,
            total_source_count=5,
        )
        with pytest.raises(AttributeError):
            stats.entity_type_count = 10  # type: ignore[misc]

    def test_source_mapping_frozen(self):
        vm = SourceMappingViewModel(
            source_id="s1",
            uri="data.csv",
            maps_to="Person",
            mapping_type="entity",
            status="connected",
        )
        with pytest.raises(AttributeError):
            vm.status = "error"  # type: ignore[misc]


# -- Entity Access ----------------------------------------------------------

class TestEntityAccess:
    def test_empty_config_returns_empty(self):
        adapter = _make_adapter(_make_config())
        assert adapter.entity_types() == []

    def test_single_entity_type(self):
        config = _make_config(entities=[
            EntitySourceConfig(
                id="person_csv",
                uri="data/people.csv",
                entity_type="Person",
                id_col="person_id",
            ),
        ])
        adapter = _make_adapter(config)
        result = adapter.entity_types()

        assert len(result) == 1
        assert result[0].entity_type == "Person"
        assert result[0].source_count == 1
        assert result[0].id_column == "person_id"
        assert result[0].row_count is None  # no index manager

    def test_multiple_sources_same_type(self):
        config = _make_config(entities=[
            EntitySourceConfig(id="p1", uri="a.csv", entity_type="Person"),
            EntitySourceConfig(id="p2", uri="b.csv", entity_type="Person"),
        ])
        adapter = _make_adapter(config)
        result = adapter.entity_types()

        assert len(result) == 1
        assert result[0].source_count == 2

    def test_multiple_entity_types_sorted(self):
        config = _make_config(entities=[
            EntitySourceConfig(id="c1", uri="c.csv", entity_type="Company"),
            EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
            EntitySourceConfig(id="a1", uri="a.csv", entity_type="Address"),
        ])
        adapter = _make_adapter(config)
        result = adapter.entity_types()

        assert len(result) == 3
        assert [r.entity_type for r in result] == ["Address", "Company", "Person"]

    def test_entity_with_schema_hints(self):
        config = _make_config(entities=[
            EntitySourceConfig(
                id="p1",
                uri="p.csv",
                entity_type="Person",
                id_col="id",
                schema_hints={"name": "string", "age": "int"},
            ),
        ])
        adapter = _make_adapter(config)
        result = adapter.entity_types()

        assert set(result[0].property_names) == {"age", "name"}

    def test_entity_detail(self):
        config = _make_config(entities=[
            EntitySourceConfig(
                id="p1",
                uri="data/people.csv",
                entity_type="Person",
                id_col="person_id",
                schema_hints={"name": "string"},
            ),
        ])
        adapter = _make_adapter(config)
        detail = adapter.entity_detail("Person")

        assert detail.entity_type == "Person"
        assert len(detail.sources) == 1
        assert detail.sources[0].source_id == "p1"
        assert detail.sources[0].uri == "data/people.csv"
        assert len(detail.properties) == 1
        assert detail.properties[0].name == "name"
        assert detail.properties[0].dtype == "string"

    def test_entity_detail_nonexistent_type(self):
        adapter = _make_adapter(_make_config())
        detail = adapter.entity_detail("NonExistent")
        assert detail.entity_type == "NonExistent"
        assert len(detail.sources) == 0


# -- Relationship Access ----------------------------------------------------

class TestRelationshipAccess:
    def test_empty_config_returns_empty(self):
        adapter = _make_adapter(_make_config())
        assert adapter.relationship_types() == []

    def test_single_relationship(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person", id_col="person_id"),
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
        adapter = _make_adapter(config)
        result = adapter.relationship_types()

        assert len(result) == 1
        assert result[0].relationship_type == "KNOWS"
        assert result[0].source_count == 1
        assert result[0].source_entity == "Person"  # resolved via person_id
        assert result[0].target_entity is None  # friend_id doesn't match

    def test_relationship_endpoint_resolution(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person", id_col="person_id"),
                EntitySourceConfig(id="c1", uri="c.csv", entity_type="Company", id_col="company_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="works_at",
                    uri="w.csv",
                    relationship_type="WORKS_AT",
                    source_col="person_id",
                    target_col="company_id",
                ),
            ],
        )
        adapter = _make_adapter(config)
        result = adapter.relationship_types()

        assert result[0].source_entity == "Person"
        assert result[0].target_entity == "Company"

    def test_relationship_detail(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person", id_col="pid"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1",
                    uri="r.csv",
                    relationship_type="KNOWS",
                    source_col="pid",
                    target_col="friend_pid",
                ),
            ],
        )
        adapter = _make_adapter(config)
        detail = adapter.relationship_detail("KNOWS")

        assert detail.relationship_type == "KNOWS"
        assert len(detail.sources) == 1
        assert detail.sources[0].source_col == "pid"
        assert len(detail.column_mappings) == 1

    def test_relationship_validation_no_entities(self):
        config = _make_config(
            relationships=[
                RelationshipSourceConfig(
                    id="r1",
                    uri="r.csv",
                    relationship_type="KNOWS",
                    source_col="from_id",
                    target_col="to_id",
                ),
            ],
        )
        adapter = _make_adapter(config)
        result = adapter.relationship_types()

        assert result[0].validation_status == "warning"


# -- Source Mappings --------------------------------------------------------

class TestSourceMappings:
    def test_entity_and_relationship_sources(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="a", target_col="b",
                ),
            ],
        )
        adapter = _make_adapter(config)
        result = adapter.source_mappings()

        assert len(result) == 2
        entity_mapping = [m for m in result if m.mapping_type == "entity"][0]
        rel_mapping = [m for m in result if m.mapping_type == "relationship"][0]

        assert entity_mapping.source_id == "p1"
        assert entity_mapping.maps_to == "Person"
        assert entity_mapping.status == "connected"
        assert rel_mapping.source_id == "r1"
        assert rel_mapping.maps_to == "KNOWS"


# -- Statistics -------------------------------------------------------------

class TestModelStatistics:
    def test_basic_stats(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
                EntitySourceConfig(id="c1", uri="c.csv", entity_type="Company"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="a", target_col="b",
                ),
            ],
        )
        adapter = _make_adapter(config)
        stats = adapter.model_statistics()

        assert stats.entity_type_count == 2
        assert stats.relationship_type_count == 1
        assert stats.total_source_count == 3
        assert stats.total_entity_rows is None  # no index manager

    def test_empty_stats(self):
        adapter = _make_adapter(_make_config())
        stats = adapter.model_statistics()

        assert stats.entity_type_count == 0
        assert stats.relationship_type_count == 0
        assert stats.total_source_count == 0


# -- Cache Management -------------------------------------------------------

class TestCacheManagement:
    def test_cache_returns_same_object(self):
        config = _make_config(entities=[
            EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
        ])
        adapter = _make_adapter(config)

        first = adapter.entity_types()
        second = adapter.entity_types()
        assert first is second

    def test_refresh_invalidates_cache(self):
        config = _make_config(entities=[
            EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
        ])
        adapter = _make_adapter(config)

        first = adapter.entity_types()
        adapter.refresh()
        second = adapter.entity_types()

        assert first is not second
        # Same content
        assert first == second

    def test_refresh_invalidates_all_caches(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="a", target_col="b",
                ),
            ],
        )
        adapter = _make_adapter(config)

        e1 = adapter.entity_types()
        r1 = adapter.relationship_types()
        s1 = adapter.source_mappings()
        m1 = adapter.model_statistics()

        adapter.refresh()

        e2 = adapter.entity_types()
        r2 = adapter.relationship_types()
        s2 = adapter.source_mappings()
        m2 = adapter.model_statistics()

        assert e1 is not e2
        assert r1 is not r2
        assert s1 is not s2
        assert m1 is not m2


# -- Validation -------------------------------------------------------------

class TestValidation:
    def test_entity_no_sources_for_type(self):
        """Entity detail for a type with no matching sources reports error."""
        config = _make_config(entities=[
            EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person"),
        ])
        adapter = _make_adapter(config)
        # Query a type that doesn't exist
        detail = adapter.entity_detail("NonExistent")

        assert len(detail.validation_issues) > 0
        assert any(i.level == "error" for i in detail.validation_issues)

    def test_relationship_unresolved_columns(self):
        config = _make_config(
            entities=[
                EntitySourceConfig(id="p1", uri="p.csv", entity_type="Person", id_col="person_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="unknown_col", target_col="also_unknown",
                ),
            ],
        )
        adapter = _make_adapter(config)
        detail = adapter.relationship_detail("KNOWS")

        warnings = [i for i in detail.validation_issues if i.level == "warning"]
        assert len(warnings) >= 2  # both columns unresolved

    def test_relationship_no_sources_for_type(self):
        """Relationship detail for a type with no matching sources reports error."""
        config = _make_config(
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="a", target_col="b",
                ),
            ],
        )
        adapter = _make_adapter(config)
        # Query a type that doesn't exist
        detail = adapter.relationship_detail("NONEXISTENT")

        errors = [i for i in detail.validation_issues if i.level == "error"]
        assert len(errors) >= 1
