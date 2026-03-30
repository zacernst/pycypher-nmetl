"""Tests for schema evolution: schema diffing, compatibility, merging, registry, lineage."""

from __future__ import annotations

import pytest
from fastopendata.schema_evolution.lineage import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
)
from fastopendata.schema_evolution.registry import SchemaRegistry
from fastopendata.schema_evolution.schema import (
    CompatibilityChecker,
    CompatibilityLevel,
    ConflictStrategy,
    DiffType,
    FieldSchema,
    FieldType,
    SchemaDiff,
    SchemaMerger,
    TableSchema,
)

# ---------------------------------------------------------------------------
# FieldSchema
# ---------------------------------------------------------------------------


class TestFieldSchema:
    def test_same_type_compatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.STRING)
        b = FieldSchema(name="x", field_type=FieldType.STRING)
        assert a.is_compatible_with(b)

    def test_int_to_float_compatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.INTEGER)
        b = FieldSchema(name="x", field_type=FieldType.FLOAT)
        assert a.is_compatible_with(b)

    def test_int_to_string_compatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.INTEGER)
        b = FieldSchema(name="x", field_type=FieldType.STRING)
        assert a.is_compatible_with(b)

    def test_string_to_int_incompatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.STRING)
        b = FieldSchema(name="x", field_type=FieldType.INTEGER)
        assert not a.is_compatible_with(b)

    def test_date_to_timestamp_compatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.DATE)
        b = FieldSchema(name="x", field_type=FieldType.TIMESTAMP)
        assert a.is_compatible_with(b)

    def test_boolean_to_string_compatible(self) -> None:
        a = FieldSchema(name="x", field_type=FieldType.BOOLEAN)
        b = FieldSchema(name="x", field_type=FieldType.STRING)
        assert a.is_compatible_with(b)


# ---------------------------------------------------------------------------
# TableSchema
# ---------------------------------------------------------------------------


class TestTableSchema:
    def _make_schema(self, version: int = 1) -> TableSchema:
        return TableSchema(
            name="users",
            fields=(
                FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
                FieldSchema(name="name", field_type=FieldType.STRING),
            ),
            version=version,
        )

    def test_field_names(self) -> None:
        s = self._make_schema()
        assert s.field_names == ["id", "name"]

    def test_get_field(self) -> None:
        s = self._make_schema()
        assert s.get_field("id") is not None
        assert s.get_field("missing") is None

    def test_field_map(self) -> None:
        s = self._make_schema()
        fm = s.field_map()
        assert "id" in fm
        assert "name" in fm

    def test_add_field(self) -> None:
        s = self._make_schema()
        new = s.add_field(FieldSchema(name="email", field_type=FieldType.STRING))
        assert len(new.fields) == 3
        assert new.version == 2
        assert "email" in new.field_names

    def test_remove_field(self) -> None:
        s = self._make_schema()
        new = s.remove_field("name")
        assert len(new.fields) == 1
        assert new.version == 2
        assert "name" not in new.field_names


# ---------------------------------------------------------------------------
# SchemaDiff
# ---------------------------------------------------------------------------


class TestSchemaDiff:
    def test_no_changes(self) -> None:
        s = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
            version=1,
        )
        diff = SchemaDiff.compute(s, s)
        assert not diff.has_changes

    def test_field_added(self) -> None:
        old = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
        )
        new = old.add_field(FieldSchema(name="y", field_type=FieldType.INTEGER))
        diff = SchemaDiff.compute(old, new)
        assert diff.has_changes
        assert diff.added_fields == ["y"]

    def test_field_removed(self) -> None:
        old = TableSchema(
            name="t",
            fields=(
                FieldSchema(name="x", field_type=FieldType.STRING),
                FieldSchema(name="y", field_type=FieldType.INTEGER),
            ),
        )
        new = old.remove_field("y")
        diff = SchemaDiff.compute(old, new)
        assert diff.removed_fields == ["y"]

    def test_type_changed(self) -> None:
        old = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        new = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
            version=2,
        )
        diff = SchemaDiff.compute(old, new)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].field_name == "x"

    def test_nullability_changed(self) -> None:
        old = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING, nullable=True),),
        )
        new = TableSchema(
            name="t",
            fields=(
                FieldSchema(name="x", field_type=FieldType.STRING, nullable=False),
            ),
            version=2,
        )
        diff = SchemaDiff.compute(old, new)
        assert any(e.diff_type == DiffType.NULLABILITY_CHANGED for e in diff.entries)


# ---------------------------------------------------------------------------
# CompatibilityChecker
# ---------------------------------------------------------------------------


class TestCompatibilityChecker:
    def _base(self) -> TableSchema:
        return TableSchema(
            name="t",
            fields=(
                FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
                FieldSchema(name="name", field_type=FieldType.STRING, nullable=True),
            ),
        )

    def test_none_level_always_compatible(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = TableSchema(name="t", fields=(), version=2)  # remove everything
        result = checker.check(old, new, CompatibilityLevel.NONE)
        assert result.compatible

    def test_backward_add_nullable_field(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=True),
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert result.compatible

    def test_backward_add_nonnullable_without_default_fails(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=False),
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert not result.compatible
        assert any("non-nullable" in v for v in result.violations)

    def test_backward_add_nonnullable_with_default_ok(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.add_field(
            FieldSchema(
                name="email",
                field_type=FieldType.STRING,
                nullable=False,
                default="",
            ),
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert result.compatible

    def test_backward_remove_nonnullable_fails(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.remove_field("id")  # id is non-nullable
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert not result.compatible

    def test_backward_remove_nullable_ok(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.remove_field("name")  # name is nullable
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert result.compatible

    def test_backward_safe_type_widening(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        # Widen id from INTEGER → FLOAT (safe)
        new = TableSchema(
            name="t",
            fields=(
                FieldSchema(name="id", field_type=FieldType.FLOAT, nullable=False),
                FieldSchema(name="name", field_type=FieldType.STRING, nullable=True),
            ),
            version=2,
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert result.compatible

    def test_backward_unsafe_type_narrowing(self) -> None:
        checker = CompatibilityChecker()
        old = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
        )
        new = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
            version=2,
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert not result.compatible

    def test_backward_nullable_to_nonnullable_fails(self) -> None:
        checker = CompatibilityChecker()
        old = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING, nullable=True),),
        )
        new = TableSchema(
            name="t",
            fields=(
                FieldSchema(name="x", field_type=FieldType.STRING, nullable=False),
            ),
            version=2,
        )
        result = checker.check(old, new, CompatibilityLevel.BACKWARD)
        assert not result.compatible

    def test_forward_add_nonnullable_fails(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        new = old.add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=False),
        )
        result = checker.check(old, new, CompatibilityLevel.FORWARD)
        assert not result.compatible

    def test_full_requires_both(self) -> None:
        checker = CompatibilityChecker()
        old = self._base()
        # Add nullable field — backward OK, forward OK (old readers can ignore it)
        new = old.add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=True),
        )
        result = checker.check(old, new, CompatibilityLevel.FULL)
        assert result.compatible


# ---------------------------------------------------------------------------
# SchemaMerger
# ---------------------------------------------------------------------------


class TestSchemaMerger:
    def test_merge_disjoint(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="a", field_type=FieldType.STRING),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="b", field_type=FieldType.INTEGER),),
        )
        merged = SchemaMerger().merge(left, right)
        assert set(merged.field_names) == {"a", "b"}
        # Fields unique to one side become nullable in merged
        assert merged.get_field("a").nullable is True  # type: ignore[union-attr]
        assert merged.get_field("b").nullable is True  # type: ignore[union-attr]

    def test_merge_same_type(self) -> None:
        left = TableSchema(
            name="t",
            fields=(
                FieldSchema(name="x", field_type=FieldType.STRING, nullable=False),
            ),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING, nullable=True),),
        )
        merged = SchemaMerger().merge(left, right)
        assert merged.get_field("x").nullable is True  # type: ignore[union-attr]

    def test_merge_widen_type(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.FLOAT),),
        )
        merged = SchemaMerger(ConflictStrategy.WIDEN).merge(left, right)
        assert merged.get_field("x").field_type == FieldType.FLOAT  # type: ignore[union-attr]

    def test_merge_prefer_left(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
        )
        merged = SchemaMerger(ConflictStrategy.PREFER_LEFT).merge(left, right)
        assert merged.get_field("x").field_type == FieldType.INTEGER  # type: ignore[union-attr]

    def test_merge_prefer_right(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.STRING),),
        )
        merged = SchemaMerger(ConflictStrategy.PREFER_RIGHT).merge(left, right)
        assert merged.get_field("x").field_type == FieldType.STRING  # type: ignore[union-attr]

    def test_merge_fail_strategy_raises(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.BOOLEAN),),
        )
        with pytest.raises(ValueError, match="Type conflict"):
            SchemaMerger(ConflictStrategy.FAIL).merge(left, right)

    def test_merge_no_widening_path_falls_to_string(self) -> None:
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.BINARY),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.ARRAY),),
        )
        merged = SchemaMerger(ConflictStrategy.WIDEN).merge(left, right)
        assert merged.get_field("x").field_type == FieldType.STRING  # type: ignore[union-attr]

    def test_merge_widen_symmetric(self) -> None:
        """WIDEN should produce the same wider type regardless of left/right order."""
        a = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        b = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.FLOAT),),
        )
        merger = SchemaMerger(ConflictStrategy.WIDEN)
        # INT→FLOAT widening: should pick FLOAT regardless of argument order
        merged_ab = merger.merge(a, b)
        merged_ba = merger.merge(b, a)
        assert merged_ab.get_field("x").field_type == FieldType.FLOAT  # type: ignore[union-attr]
        assert merged_ba.get_field("x").field_type == FieldType.FLOAT  # type: ignore[union-attr]

    def test_merge_prefer_right_ignores_widening_rules(self) -> None:
        """PREFER_RIGHT should always pick right's type, even if not a widening."""
        left = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.FLOAT),),
        )
        right = TableSchema(
            name="t",
            fields=(FieldSchema(name="x", field_type=FieldType.INTEGER),),
        )
        merged = SchemaMerger(ConflictStrategy.PREFER_RIGHT).merge(left, right)
        # FLOAT→INTEGER is narrowing, but PREFER_RIGHT should pick INTEGER
        assert merged.get_field("x").field_type == FieldType.INTEGER  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# SchemaRegistry
# ---------------------------------------------------------------------------


class TestSchemaRegistry:
    def _v1(self) -> TableSchema:
        return TableSchema(
            name="users",
            fields=(
                FieldSchema(name="id", field_type=FieldType.INTEGER, nullable=False),
                FieldSchema(name="name", field_type=FieldType.STRING),
            ),
            version=1,
        )

    def test_register_first_version(self) -> None:
        reg = SchemaRegistry()
        result = reg.register(self._v1())
        assert result.compatible
        assert reg.get_latest("users") is not None

    def test_register_compatible_evolution(self) -> None:
        reg = SchemaRegistry(CompatibilityLevel.BACKWARD)
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=True),
        )
        result = reg.register(v2)
        assert result.compatible
        assert reg.get_latest("users").version == 2  # type: ignore[union-attr]

    def test_register_incompatible_blocked(self) -> None:
        reg = SchemaRegistry(CompatibilityLevel.BACKWARD)
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING, nullable=False),
        )
        result = reg.register(v2)
        assert not result.compatible
        # Should still be on v1
        assert reg.get_latest("users").version == 1  # type: ignore[union-attr]

    def test_get_version(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING),
        )
        reg.register(v2)
        assert reg.get_version("users", 1) is not None
        assert reg.get_version("users", 2) is not None
        assert reg.get_version("users", 3) is None

    def test_diff_between_versions(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING),
        )
        reg.register(v2)
        diff = reg.diff("users", 1, 2)
        assert diff is not None
        assert diff.added_fields == ["email"]

    def test_rollback(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING),
        )
        reg.register(v2)
        rolled = reg.rollback("users", 1)
        assert rolled is not None
        assert reg.get_latest("users").version == 1  # type: ignore[union-attr]

    def test_rollback_nonexistent_version(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        assert reg.rollback("users", 99) is None

    def test_table_names(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        reg.register(
            TableSchema(
                name="orders",
                fields=(FieldSchema(name="id", field_type=FieldType.INTEGER),),
            ),
        )
        assert set(reg.table_names) == {"users", "orders"}

    def test_check_compatibility_dry_run(self) -> None:
        reg = SchemaRegistry(CompatibilityLevel.BACKWARD)
        reg.register(self._v1())
        bad = self._v1().add_field(
            FieldSchema(name="x", field_type=FieldType.STRING, nullable=False),
        )
        result = reg.check_compatibility("users", bad)
        assert not result.compatible
        # Schema should NOT have been registered
        assert reg.get_latest("users").version == 1  # type: ignore[union-attr]

    def test_set_compatibility_level(self) -> None:
        reg = SchemaRegistry(CompatibilityLevel.BACKWARD)
        reg.set_compatibility_level(CompatibilityLevel.NONE)
        assert reg.compatibility_level == CompatibilityLevel.NONE

    def test_history(self) -> None:
        reg = SchemaRegistry()
        reg.register(self._v1())
        v2 = self._v1().add_field(
            FieldSchema(name="email", field_type=FieldType.STRING),
        )
        reg.register(v2)
        history = reg.get_history("users")
        assert history is not None
        assert history.version_count == 2


# ---------------------------------------------------------------------------
# LineageGraph
# ---------------------------------------------------------------------------


class TestLineageGraph:
    def _build_pipeline(self) -> LineageGraph:
        """Build: source_a → transform → sink, source_b → transform."""
        g = LineageGraph()
        g.add_node(
            LineageNode("src_a", NodeType.SOURCE, "Census Data", schema_name="census"),
        )
        g.add_node(LineageNode("src_b", NodeType.SOURCE, "OSM Data", schema_name="osm"))
        g.add_node(LineageNode("xform", NodeType.TRANSFORM, "Join & Filter"))
        g.add_node(
            LineageNode("sink", NodeType.SINK, "Output Table", schema_name="output"),
        )
        g.add_edge(LineageEdge("src_a", "xform", transformation="inner join"))
        g.add_edge(LineageEdge("src_b", "xform", transformation="inner join"))
        g.add_edge(LineageEdge("xform", "sink", transformation="write parquet"))
        return g

    def test_node_count(self) -> None:
        g = self._build_pipeline()
        assert g.node_count == 4

    def test_edge_count(self) -> None:
        g = self._build_pipeline()
        assert g.edge_count == 3

    def test_get_children(self) -> None:
        g = self._build_pipeline()
        children = g.get_children("xform")
        assert len(children) == 1
        assert children[0].node_id == "sink"

    def test_get_parents(self) -> None:
        g = self._build_pipeline()
        parents = g.get_parents("xform")
        assert len(parents) == 2
        parent_ids = {p.node_id for p in parents}
        assert parent_ids == {"src_a", "src_b"}

    def test_impact_analysis(self) -> None:
        g = self._build_pipeline()
        impact = g.impact_analysis("src_a")
        impact_ids = {n.node_id for n in impact}
        assert impact_ids == {"xform", "sink"}

    def test_provenance(self) -> None:
        g = self._build_pipeline()
        prov = g.provenance("sink")
        prov_ids = {n.node_id for n in prov}
        assert prov_ids == {"xform", "src_a", "src_b"}

    def test_topological_order(self) -> None:
        g = self._build_pipeline()
        order = g.topological_order()
        ids = [n.node_id for n in order]
        # Sources before transform before sink
        assert ids.index("src_a") < ids.index("xform")
        assert ids.index("src_b") < ids.index("xform")
        assert ids.index("xform") < ids.index("sink")

    def test_sources(self) -> None:
        g = self._build_pipeline()
        srcs = g.sources()
        assert {s.node_id for s in srcs} == {"src_a", "src_b"}

    def test_sinks(self) -> None:
        g = self._build_pipeline()
        snks = g.sinks()
        assert len(snks) == 1
        assert snks[0].node_id == "sink"

    def test_add_edge_invalid_source_raises(self) -> None:
        g = LineageGraph()
        g.add_node(LineageNode("a", NodeType.SOURCE, "A"))
        with pytest.raises(ValueError, match="Source node"):
            g.add_edge(LineageEdge("missing", "a"))

    def test_add_edge_invalid_target_raises(self) -> None:
        g = LineageGraph()
        g.add_node(LineageNode("a", NodeType.SOURCE, "A"))
        with pytest.raises(ValueError, match="Target node"):
            g.add_edge(LineageEdge("a", "missing"))

    def test_cycle_detection(self) -> None:
        g = LineageGraph()
        g.add_node(LineageNode("a", NodeType.TRANSFORM, "A"))
        g.add_node(LineageNode("b", NodeType.TRANSFORM, "B"))
        g.add_edge(LineageEdge("a", "b"))
        g.add_edge(LineageEdge("b", "a"))
        with pytest.raises(ValueError, match="cycle"):
            g.topological_order()

    def test_empty_graph(self) -> None:
        g = LineageGraph()
        assert g.node_count == 0
        assert g.topological_order() == []
        assert g.sources() == []
        assert g.sinks() == []

    def test_impact_of_leaf_is_empty(self) -> None:
        g = self._build_pipeline()
        assert g.impact_analysis("sink") == []

    def test_provenance_of_root_is_empty(self) -> None:
        g = self._build_pipeline()
        assert g.provenance("src_a") == []
