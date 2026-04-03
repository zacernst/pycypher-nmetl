"""Quality gate tests for Task #15: DataModelAdapter facade and VimEditableScreen.

These tests validate the foundation layer that Tasks #16-18 depend on.
They verify contracts, integration behavior, performance, and edge cases
beyond the basic unit tests in test_data_model_adapter.py and test_editable_base.py.

Test Categories:
1. DataModelAdapter contract tests (API guarantees for downstream screens)
2. View model dataclass contract verification (all 11 types)
3. Cache invalidation performance benchmarks
4. VimEditableScreen behavioral tests (field lifecycle, dirty state, undo)
5. Integration tests (adapter + editable screen interaction)
6. Edge case and error resilience tests
"""

from __future__ import annotations

import time
from unittest.mock import Mock

import pytest

from pycypher.ingestion.config import (
    EntitySourceConfig,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
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
from pycypher_tui.screens.editable_base import (
    EditableField,
    FieldValidationResult,
    VimEditableScreen,
)
from pycypher_tui.screens.base import BaseDetailPanel, BaseListItem


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(
    entities: list[EntitySourceConfig] | None = None,
    relationships: list[RelationshipSourceConfig] | None = None,
    queries: list[QueryConfig] | None = None,
) -> PipelineConfig:
    return PipelineConfig(
        project=ProjectConfig(name="quality_gate"),
        sources=SourcesConfig(
            entities=entities or [],
            relationships=relationships or [],
        ),
        queries=queries or [],
        output=[],
    )


def _adapter(config: PipelineConfig, index_mgr=None) -> DataModelAdapter:
    mgr = ConfigManager.from_config(config)
    return DataModelAdapter(config_manager=mgr, index_manager=index_mgr)


def _ecommerce_config() -> PipelineConfig:
    from pycypher_tui.config.templates import get_template
    t = get_template("ecommerce_pipeline")
    return t.instantiate(project_name="test", data_dir="data")


def _large_config(n_entities=50, n_rels=30, n_queries=0, n_outputs=0) -> PipelineConfig:
    return _config(
        entities=[
            EntitySourceConfig(
                id=f"e{i}", uri=f"f:///e{i}.csv", entity_type=f"Type{i}",
                id_col=f"id_{i}"
            )
            for i in range(n_entities)
        ],
        relationships=[
            RelationshipSourceConfig(
                id=f"r{i}", uri=f"f:///r{i}.csv", relationship_type=f"REL{i}",
                source_col=f"src_{i}", target_col=f"tgt_{i}"
            )
            for i in range(n_rels)
        ],
        queries=[QueryConfig(id=f"q{i}", inline=f"MATCH (n) RETURN n LIMIT {i}") for i in range(n_queries)],
    )


# ===========================================================================
# 1. DataModelAdapter API contract tests
# ===========================================================================


class TestAdapterAPIContracts:
    """Verify API contracts that downstream screens (Tasks #16-18) depend on."""

    def test_entity_types_returns_list_of_entity_view_models(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.entity_types()
        assert isinstance(result, list)
        assert all(isinstance(vm, EntityViewModel) for vm in result)

    def test_relationship_types_returns_list_of_relationship_view_models(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.relationship_types()
        assert isinstance(result, list)
        assert all(isinstance(vm, RelationshipViewModel) for vm in result)

    def test_source_mappings_returns_list_of_source_mapping_view_models(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.source_mappings()
        assert isinstance(result, list)
        assert all(isinstance(vm, SourceMappingViewModel) for vm in result)

    def test_model_statistics_returns_model_stats_view_model(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.model_statistics()
        assert isinstance(result, ModelStatsViewModel)

    def test_entity_detail_returns_entity_detail_view_model(self):
        adapter = _adapter(_ecommerce_config())
        types = adapter.entity_types()
        if types:
            detail = adapter.entity_detail(types[0].entity_type)
            assert isinstance(detail, EntityDetailViewModel)

    def test_relationship_detail_returns_relationship_detail_view_model(self):
        adapter = _adapter(_ecommerce_config())
        types = adapter.relationship_types()
        if types:
            detail = adapter.relationship_detail(types[0].relationship_type)
            assert isinstance(detail, RelationshipDetailViewModel)

    def test_entity_types_sorted_alphabetically(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.entity_types()
        names = [vm.entity_type for vm in result]
        assert names == sorted(names)

    def test_relationship_types_sorted_alphabetically(self):
        adapter = _adapter(_ecommerce_config())
        result = adapter.relationship_types()
        names = [vm.relationship_type for vm in result]
        assert names == sorted(names)

    def test_empty_config_returns_empty_lists(self):
        adapter = _adapter(_config())
        assert adapter.entity_types() == []
        assert adapter.relationship_types() == []
        assert adapter.source_mappings() == []

    def test_empty_config_stats_all_zero(self):
        adapter = _adapter(_config())
        stats = adapter.model_statistics()
        assert stats.entity_type_count == 0
        assert stats.relationship_type_count == 0
        assert stats.total_source_count == 0

    def test_config_only_mode_row_counts_none(self):
        """Without index_manager, row counts degrade to None."""
        adapter = _adapter(_ecommerce_config())
        for vm in adapter.entity_types():
            assert vm.row_count is None
        stats = adapter.model_statistics()
        assert stats.total_entity_rows is None


# ===========================================================================
# 2. View model dataclass contract verification
# ===========================================================================


class TestViewModelContracts:
    """Verify all 11 view model types satisfy their contracts."""

    def test_all_view_models_are_frozen(self):
        """Every view model must be immutable (frozen dataclass)."""
        frozen_types = [
            ValidationIssue, PropertyViewModel, EntitySourceViewModel,
            EntityViewModel, EntityDetailViewModel, ColumnMappingViewModel,
            RelationshipSourceViewModel, RelationshipViewModel,
            RelationshipDetailViewModel, SourceMappingViewModel,
            ModelStatsViewModel,
        ]
        for cls in frozen_types:
            instance = cls.__new__(cls)
            # The frozen flag is set in __dataclass_params__
            assert cls.__dataclass_params__.frozen, f"{cls.__name__} is not frozen"

    def test_validation_issue_required_fields(self):
        issue = ValidationIssue(level="error", message="test")
        assert issue.level == "error"
        assert issue.message == "test"
        assert issue.field is None
        assert issue.fix_hint is None

    def test_validation_issue_with_all_fields(self):
        issue = ValidationIssue(
            level="warning", message="missing col",
            field="source_col", fix_hint="Add source_col"
        )
        assert issue.field == "source_col"
        assert issue.fix_hint == "Add source_col"

    def test_entity_view_model_property_names_are_tuple(self):
        vm = EntityViewModel(
            entity_type="Person", source_count=1,
            property_names=("name", "age"), id_column="id"
        )
        assert isinstance(vm.property_names, tuple)

    def test_relationship_view_model_column_mappings_are_tuple(self):
        vm = RelationshipViewModel(
            relationship_type="KNOWS",
            source_entity="Person", target_entity="Person",
            source_count=1,
            column_mappings=(ColumnMappingViewModel(
                source_col="pid", target_col="fid"
            ),),
            validation_status="valid",
        )
        assert isinstance(vm.column_mappings, tuple)

    def test_entity_detail_sources_are_tuple(self):
        detail = EntityDetailViewModel(
            entity_type="Person",
            sources=(EntitySourceViewModel(
                source_id="p1", uri="p.csv",
                entity_type="Person", id_col="id", query=None,
            ),),
            properties=(),
            validation_issues=(),
        )
        assert isinstance(detail.sources, tuple)

    def test_model_stats_default_optional_fields(self):
        stats = ModelStatsViewModel(
            entity_type_count=1,
            relationship_type_count=1,
            total_source_count=2,
        )
        assert stats.total_entity_rows is None
        assert stats.total_relationship_rows is None
        assert stats.query_count == 0
        assert stats.output_count == 0


# ===========================================================================
# 3. Cache invalidation performance benchmarks
# ===========================================================================


class TestCachePerformance:
    """Verify cache refresh is fast enough for interactive use."""

    def test_refresh_is_constant_time(self):
        """refresh() should be O(1) — just null out pointers."""
        adapter = _adapter(_large_config())
        # Populate caches
        adapter.entity_types()
        adapter.relationship_types()
        adapter.source_mappings()
        adapter.model_statistics()

        start = time.perf_counter()
        for _ in range(10_000):
            adapter.refresh()
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5  # 10k refreshes in <0.5s

    def test_cache_hit_faster_than_miss(self):
        """Cached access should be faster than initial build."""
        adapter = _adapter(_large_config(n_entities=100, n_rels=50))

        # Cold
        start = time.perf_counter()
        adapter.entity_types()
        cold = time.perf_counter() - start

        # Hot
        start = time.perf_counter()
        adapter.entity_types()
        hot = time.perf_counter() - start

        assert hot < cold  # cached should be faster

    def test_full_refresh_rebuild_cycle_under_50ms(self):
        """Full cache refresh + rebuild should be under 50ms for medium configs."""
        adapter = _adapter(_large_config(n_entities=50, n_rels=30))
        adapter.entity_types()  # warm up

        start = time.perf_counter()
        adapter.refresh()
        adapter.entity_types()
        adapter.relationship_types()
        adapter.source_mappings()
        adapter.model_statistics()
        elapsed = time.perf_counter() - start

        assert elapsed < 0.05  # 50ms budget

    def test_large_config_entity_types_under_100ms(self):
        """200 entity types should build in <100ms."""
        adapter = _adapter(_large_config(n_entities=200, n_rels=0))

        start = time.perf_counter()
        result = adapter.entity_types()
        elapsed = time.perf_counter() - start

        assert len(result) == 200
        assert elapsed < 0.1


# ===========================================================================
# 4. Validation logic contract tests
# ===========================================================================


class TestValidationContracts:
    """Verify validation logic produces correct issues."""

    def test_entity_nonexistent_type_produces_error(self):
        """Querying a nonexistent entity type produces validation error."""
        config = _config(entities=[
            EntitySourceConfig(id="e1", uri="a.csv", entity_type="Person"),
        ])
        adapter = _adapter(config)
        detail = adapter.entity_detail("NonExistent")
        errors = [i for i in detail.validation_issues if i.level == "error"]
        assert len(errors) >= 1
        assert any("No data sources" in i.message for i in errors)

    def test_entity_duplicate_source_id_rejected_at_config_level(self):
        """Duplicate source IDs are caught by pydantic validation."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="Duplicate source id"):
            _config(entities=[
                EntitySourceConfig(id="same_id", uri="a.csv", entity_type="Person"),
                EntitySourceConfig(id="same_id", uri="b.csv", entity_type="Person"),
            ])

    def test_entity_with_valid_uri_has_no_uri_errors(self):
        config = _config(entities=[
            EntitySourceConfig(id="e1", uri="data/people.csv", entity_type="Person"),
        ])
        adapter = _adapter(config)
        detail = adapter.entity_detail("Person")
        uri_errors = [i for i in detail.validation_issues if i.field == "uri"]
        assert len(uri_errors) == 0

    def test_relationship_unresolved_source_col_produces_warning(self):
        config = _config(
            entities=[
                EntitySourceConfig(id="e1", uri="e.csv", entity_type="Person", id_col="person_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="unknown_col", target_col="person_id",
                ),
            ],
        )
        adapter = _adapter(config)
        detail = adapter.relationship_detail("KNOWS")
        warnings = [i for i in detail.validation_issues if i.level == "warning"]
        assert any("unknown_col" in i.message for i in warnings)

    def test_relationship_no_entities_produces_warning(self):
        config = _config(relationships=[
            RelationshipSourceConfig(
                id="r1", uri="r.csv", relationship_type="KNOWS",
                source_col="a", target_col="b",
            ),
        ])
        adapter = _adapter(config)
        types = adapter.relationship_types()
        assert types[0].validation_status == "warning"

    def test_validation_issue_supports_fix_hint_field(self):
        """ValidationIssue dataclass supports fix_hint for remediation guidance."""
        issue = ValidationIssue(
            level="error", message="URI missing",
            field="uri", fix_hint="Provide a valid file path",
        )
        assert issue.fix_hint == "Provide a valid file path"
        assert issue.field == "uri"

    def test_fully_valid_config_has_no_issues(self):
        config = _config(
            entities=[
                EntitySourceConfig(id="e1", uri="a.csv", entity_type="Person", id_col="pid"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="pid", target_col="pid",
                ),
            ],
        )
        adapter = _adapter(config)
        e_detail = adapter.entity_detail("Person")
        r_detail = adapter.relationship_detail("KNOWS")
        assert len(e_detail.validation_issues) == 0
        assert all(i.level != "error" for i in r_detail.validation_issues)


# ===========================================================================
# 5. Relationship endpoint resolution tests
# ===========================================================================


class TestEndpointResolution:
    """Verify relationship source/target entity resolution."""

    def test_resolves_both_endpoints(self):
        config = _config(
            entities=[
                EntitySourceConfig(id="p", uri="p.csv", entity_type="Person", id_col="person_id"),
                EntitySourceConfig(id="c", uri="c.csv", entity_type="Company", id_col="company_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="WORKS_AT",
                    source_col="person_id", target_col="company_id",
                ),
            ],
        )
        adapter = _adapter(config)
        result = adapter.relationship_types()
        assert result[0].source_entity == "Person"
        assert result[0].target_entity == "Company"

    def test_unresolvable_endpoint_is_none(self):
        config = _config(
            entities=[
                EntitySourceConfig(id="p", uri="p.csv", entity_type="Person", id_col="person_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="KNOWS",
                    source_col="person_id", target_col="friend_id",
                ),
            ],
        )
        adapter = _adapter(config)
        result = adapter.relationship_types()
        assert result[0].source_entity == "Person"
        assert result[0].target_entity is None

    def test_self_referential_relationship(self):
        config = _config(
            entities=[
                EntitySourceConfig(id="p", uri="p.csv", entity_type="Person", id_col="person_id"),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="r1", uri="r.csv", relationship_type="FOLLOWS",
                    source_col="person_id", target_col="person_id",
                ),
            ],
        )
        adapter = _adapter(config)
        result = adapter.relationship_types()
        assert result[0].source_entity == "Person"
        assert result[0].target_entity == "Person"


# ===========================================================================
# 6. Statistics contract tests
# ===========================================================================


class TestStatisticsContracts:
    """Verify statistics reflect config accurately."""

    def test_stats_count_queries(self):
        config = _large_config(n_entities=3, n_rels=2, n_queries=5)
        adapter = _adapter(config)
        stats = adapter.model_statistics()
        assert stats.query_count == 5
        assert stats.output_count == 0  # no outputs in test config

    def test_stats_count_unique_types_not_sources(self):
        """Multiple sources for same type should count as 1 type."""
        config = _config(entities=[
            EntitySourceConfig(id="e1", uri="a.csv", entity_type="Person"),
            EntitySourceConfig(id="e2", uri="b.csv", entity_type="Person"),
            EntitySourceConfig(id="e3", uri="c.csv", entity_type="Company"),
        ])
        adapter = _adapter(config)
        stats = adapter.model_statistics()
        assert stats.entity_type_count == 2  # Person, Company
        assert stats.total_source_count == 3  # 3 sources

    def test_stats_without_index_manager(self):
        adapter = _adapter(_ecommerce_config())
        stats = adapter.model_statistics()
        assert stats.total_entity_rows is None
        assert stats.total_relationship_rows is None

    def test_stats_with_mock_index_manager(self):
        """Verify graceful index manager integration."""
        mock_idx = Mock()
        mock_idx._label = {"Person": [1, 2, 3]}
        mock_idx._adjacency = {}

        config = _config(entities=[
            EntitySourceConfig(id="e1", uri="p.csv", entity_type="Person", id_col="pid"),
        ])
        adapter = _adapter(config, index_mgr=mock_idx)
        types = adapter.entity_types()
        assert types[0].row_count == 3
        assert types[0].has_index is True


# ===========================================================================
# 7. VimEditableScreen contract tests
# ===========================================================================


class _TestItem:
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class _TestListItem(BaseListItem[_TestItem]):
    pass


class _ConcreteEditableScreen(VimEditableScreen[_TestItem]):
    """Minimal concrete subclass for quality gate testing."""

    @property
    def screen_title(self) -> str:
        return "QG Editor"

    @property
    def breadcrumb_text(self) -> str:
        return "Test > QG Editor"

    @property
    def footer_hints(self) -> str:
        return "Tab:next  Escape:cancel"

    def load_items(self) -> list[_TestItem]:
        return [_TestItem("a", "1"), _TestItem("b", "2")]

    def create_list_item(self, item, item_id):
        return _TestListItem(id=item_id)

    def create_detail_panel(self):
        return BaseDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item):
        pass

    def get_item_id(self, item):
        return item.name

    def on_edit(self, item):
        self.start_editing(item)

    def on_add(self):
        self.start_editing(None)

    async def on_delete(self, item):
        pass

    def get_fields(self, item):
        if item is None:
            return [
                EditableField(name="name", label="Name", required=True),
                EditableField(name="value", label="Value"),
            ]
        return [
            EditableField(name="name", label="Name", value=item.name, readonly=True),
            EditableField(name="value", label="Value", value=item.value),
        ]

    def validate_field(self, name, value):
        if name == "name" and not value.strip():
            return FieldValidationResult(valid=False, error="Required")
        return FieldValidationResult(valid=True)

    def apply_changes(self, item, field_values):
        if item:
            item.value = field_values.get("value", item.value)


class TestVimEditableScreenContracts:
    """Verify VimEditableScreen contracts for downstream editor screens."""

    def _make_screen(self) -> _ConcreteEditableScreen:
        mgr = ConfigManager.from_config(_config())
        screen = _ConcreteEditableScreen.__new__(_ConcreteEditableScreen)
        VimEditableScreen.__init__(screen, config_manager=mgr)
        return screen

    def test_start_editing_captures_initial_values(self):
        screen = self._make_screen()
        item = _TestItem("x", "42")
        screen.start_editing(item)
        assert screen._initial_values == {"name": "x", "value": "42"}

    def test_start_editing_none_for_new_item(self):
        screen = self._make_screen()
        screen.start_editing(None)
        assert screen._editing_item is None
        assert len(screen._fields) == 2
        assert screen._fields[0].required is True

    def test_field_cursor_starts_at_zero(self):
        screen = self._make_screen()
        screen.start_editing(_TestItem("a", "1"))
        assert screen._field_cursor == 0

    def test_fields_populated_with_correct_count(self):
        screen = self._make_screen()
        screen.start_editing(_TestItem("a", "1"))
        assert len(screen._fields) == 2
        assert screen._fields[0].name == "name"
        assert screen._fields[1].name == "value"

    def test_editing_item_stored(self):
        screen = self._make_screen()
        item = _TestItem("a", "1")
        screen.start_editing(item)
        assert screen._editing_item is item

    def test_field_values_match_item(self):
        screen = self._make_screen()
        screen.start_editing(_TestItem("x", "42"))
        assert screen._fields[0].value == "x"
        assert screen._fields[1].value == "42"

    def test_readonly_fields_skipped_on_editing(self):
        screen = self._make_screen()
        item = _TestItem("x", "42")
        screen.start_editing(item)
        # First field is readonly, editing should set fields correctly
        readonly_fields = [f for f in screen._fields if f.readonly]
        assert len(readonly_fields) == 1
        assert readonly_fields[0].name == "name"

    def test_validate_field_rejects_empty_required(self):
        """validate_field correctly rejects empty required fields."""
        screen = self._make_screen()
        result = screen.validate_field("name", "")
        assert result.valid is False
        assert result.error is not None

    def test_validate_field_accepts_valid_value(self):
        """validate_field correctly accepts valid values."""
        screen = self._make_screen()
        result = screen.validate_field("name", "valid_name")
        assert result.valid is True

    def test_validate_field_accepts_optional_empty(self):
        """validate_field accepts empty value for non-required fields."""
        screen = self._make_screen()
        result = screen.validate_field("value", "")
        assert result.valid is True

    def test_screen_override_keys_include_form_keys(self):
        screen = self._make_screen()
        keys = screen._screen_override_keys
        assert "tab" in keys
        assert "shift+tab" in keys
        assert "escape" in keys

    def test_handle_extra_key_tab_returns_true(self):
        screen = self._make_screen()
        assert screen.handle_extra_key("tab") is True

    def test_handle_extra_key_unknown_returns_false(self):
        screen = self._make_screen()
        assert screen.handle_extra_key("z") is False

    def test_undo_redo_safe_when_empty(self):
        screen = self._make_screen()
        # Should not raise
        screen.undo()
        screen.redo()


# ===========================================================================
# 8. Adapter + index manager graceful degradation
# ===========================================================================


class TestGracefulDegradation:
    """Verify adapter works correctly with and without index manager."""

    def test_no_index_manager_entity_has_index_false(self):
        adapter = _adapter(_ecommerce_config())
        for vm in adapter.entity_types():
            assert vm.has_index is False

    def test_broken_index_manager_does_not_crash(self):
        """Adapter should handle index_manager that raises exceptions."""
        broken_idx = Mock()
        type(broken_idx)._label = property(lambda self: (_ for _ in ()).throw(RuntimeError("broken")))

        config = _config(entities=[
            EntitySourceConfig(id="e1", uri="p.csv", entity_type="Person"),
        ])
        adapter = _adapter(config, index_mgr=broken_idx)
        # Should not crash, just return has_index=False
        result = adapter.entity_types()
        assert len(result) == 1
        assert result[0].has_index is False

    def test_detail_for_nonexistent_type_returns_empty(self):
        adapter = _adapter(_ecommerce_config())
        detail = adapter.entity_detail("DoesNotExist")
        assert detail.entity_type == "DoesNotExist"
        assert len(detail.sources) == 0

    def test_relationship_detail_nonexistent_type(self):
        adapter = _adapter(_ecommerce_config())
        detail = adapter.relationship_detail("NONEXISTENT")
        assert detail.relationship_type == "NONEXISTENT"
        assert len(detail.sources) == 0
