"""Unit tests for BindingFrame — Phase 1 of the IR refactor.

Every test in this file operates on the BindingFrame data structure directly,
without touching star.py or the legacy Relation pipeline.  The goal is to
verify the contract of the new abstraction before wiring it into the execution
engine.

Dataset used throughout:

    Person: Alice (id=1, age=30), Bob (id=2, age=25), Carol (id=3, age=35)
    KNOWS edges: Alice→Bob (id=10), Bob→Carol (id=11), Alice→Carol (id=12)
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_evaluator import BindingExpressionEvaluator
from pycypher.binding_frame import (
    BindingFilter,
    BindingFrame,
    EntityScan,
    RelationshipScan,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

# ===========================================================================
# Shared fixtures
# ===========================================================================


@pytest.fixture
def ctx() -> Context:
    """Context with Person nodes and KNOWS edges."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 1],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3],
        }
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_col=RELATIONSHIP_SOURCE_COLUMN,
        target_col=RELATIONSHIP_TARGET_COLUMN,
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


@pytest.fixture
def single_var_frame(ctx: Context) -> BindingFrame:
    """BindingFrame with one variable 'p' bound to all three Person IDs."""
    return BindingFrame(
        bindings=pd.DataFrame({"p": [1, 2, 3]}),
        type_registry={"p": "Person"},
        context=ctx,
    )


@pytest.fixture
def two_var_frame(ctx: Context) -> BindingFrame:
    """BindingFrame with 'p' and 'q' — both Person, representing KNOWS pairs."""
    return BindingFrame(
        bindings=pd.DataFrame({"p": [1, 1, 2], "q": [2, 3, 3]}),
        type_registry={"p": "Person", "q": "Person"},
        context=ctx,
    )


# ===========================================================================
# Construction and introspection
# ===========================================================================


class TestConstruction:
    def test_len(self, single_var_frame: BindingFrame) -> None:
        assert len(single_var_frame) == 3

    def test_var_names_single(self, single_var_frame: BindingFrame) -> None:
        assert single_var_frame.var_names == ["p"]

    def test_var_names_two(self, two_var_frame: BindingFrame) -> None:
        assert set(two_var_frame.var_names) == {"p", "q"}

    def test_entity_type(self, single_var_frame: BindingFrame) -> None:
        assert single_var_frame.entity_type("p") == "Person"

    def test_entity_type_missing_raises(
        self, single_var_frame: BindingFrame
    ) -> None:
        with pytest.raises(KeyError):
            single_var_frame.entity_type("z")


# ===========================================================================
# get_property
# ===========================================================================


class TestGetProperty:
    def test_returns_series_of_correct_length(
        self, single_var_frame: BindingFrame
    ) -> None:
        result = single_var_frame.get_property("p", "name")
        assert len(result) == 3

    def test_correct_values_single_var(
        self, single_var_frame: BindingFrame
    ) -> None:
        result = single_var_frame.get_property("p", "name")
        assert list(result) == ["Alice", "Bob", "Carol"]

    def test_integer_property(self, single_var_frame: BindingFrame) -> None:
        result = single_var_frame.get_property("p", "age")
        assert list(result) == [30, 25, 35]

    def test_two_same_type_vars_independent(
        self, two_var_frame: BindingFrame
    ) -> None:
        """p and q are both Person — their properties must be independent."""
        p_names = two_var_frame.get_property("p", "name")
        q_names = two_var_frame.get_property("q", "name")

        # p binding is [1, 1, 2] → Alice, Alice, Bob
        assert list(p_names) == ["Alice", "Alice", "Bob"]
        # q binding is [2, 3, 3] → Bob, Carol, Carol
        assert list(q_names) == ["Bob", "Carol", "Carol"]

    def test_p_and_q_names_differ(self, two_var_frame: BindingFrame) -> None:
        """Verify p.name != q.name for each row (no cross-contamination)."""
        p_names = two_var_frame.get_property("p", "name")
        q_names = two_var_frame.get_property("q", "name")
        for pn, qn in zip(p_names, q_names):
            assert pn != qn

    def test_unknown_variable_raises(
        self, single_var_frame: BindingFrame
    ) -> None:
        from pycypher.exceptions import VariableNotFoundError

        with pytest.raises(
            VariableNotFoundError, match="Variable 'z' is not defined"
        ):
            single_var_frame.get_property("z", "name")

    def test_unknown_property_returns_null_series(
        self, single_var_frame: BindingFrame
    ) -> None:
        """Per Cypher semantics, accessing a nonexistent property returns null."""
        import pandas as pd

        result = single_var_frame.get_property("p", "salary")
        assert isinstance(result, pd.Series)
        assert len(result) == len(single_var_frame)
        for val in result:
            assert val is None or val is pd.NA or pd.isna(val)


# ===========================================================================
# filter
# ===========================================================================


class TestFilter:
    def test_filter_by_age(self, single_var_frame: BindingFrame) -> None:
        ages = single_var_frame.get_property("p", "age")
        filtered = single_var_frame.filter(ages >= 30)
        # Alice (30) and Carol (35) qualify; Bob (25) does not
        assert len(filtered) == 2
        names = filtered.get_property("p", "name")
        assert set(names) == {"Alice", "Carol"}

    def test_filter_to_one_row(self, single_var_frame: BindingFrame) -> None:
        names = single_var_frame.get_property("p", "name")
        filtered = single_var_frame.filter(names == "Bob")
        assert len(filtered) == 1
        assert filtered.bindings["p"].iloc[0] == 2  # Bob's ID

    def test_filter_preserves_type_registry(
        self, single_var_frame: BindingFrame
    ) -> None:
        mask = single_var_frame.get_property("p", "age") > 0
        filtered = single_var_frame.filter(mask)
        assert filtered.type_registry == single_var_frame.type_registry

    def test_filter_empty_result(self, single_var_frame: BindingFrame) -> None:
        names = single_var_frame.get_property("p", "name")
        filtered = single_var_frame.filter(names == "Nobody")
        assert len(filtered) == 0

    def test_filter_two_var_frame(self, two_var_frame: BindingFrame) -> None:
        """Filter rows where p is Alice (id=1)."""
        mask = two_var_frame.bindings["p"] == 1
        filtered = two_var_frame.filter(mask)
        assert len(filtered) == 2  # Alice→Bob and Alice→Carol


# ===========================================================================
# join
# ===========================================================================


class TestJoin:
    def test_join_node_to_relationship(self, ctx: Context) -> None:
        """Join person scan to KNOWS source column."""
        person_frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        # Relationship scan: _src_r is the source-node ID
        rel_frame = BindingFrame(
            bindings=pd.DataFrame(
                {
                    "r": [10, 11, 12],
                    "_src_r": [1, 2, 1],
                    "_tgt_r": [2, 3, 3],
                }
            ),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        joined = person_frame.join(rel_frame, left_col="p", right_col="_src_r")

        # Only rows where p ID matches _src_r
        assert len(joined) == 3  # all three edges have a matching source
        assert "p" in joined.var_names
        assert "r" in joined.var_names
        assert "_tgt_r" in joined.var_names
        # The redundant _src_r column should be gone
        assert "_src_r" not in joined.var_names

    def test_join_result_row_count(self, ctx: Context) -> None:
        """Joining on a single shared ID gives one row per match."""
        p_frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),  # only Alice
            type_registry={"p": "Person"},
            context=ctx,
        )
        rel_frame = BindingFrame(
            bindings=pd.DataFrame(
                {
                    "r": [10, 11, 12],
                    "_src_r": [1, 2, 1],
                    "_tgt_r": [2, 3, 3],
                }
            ),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        joined = p_frame.join(rel_frame, left_col="p", right_col="_src_r")
        # Alice is source of edges 10 and 12
        assert len(joined) == 2

    def test_join_merges_type_registries(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1, 3], "_src_q": [4, 5]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        joined = left.join(right, left_col="p", right_col="q")
        assert "p" in joined.type_registry
        assert "q" in joined.type_registry

    def test_join_missing_left_col_raises(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        with pytest.raises(ValueError, match="is not defined"):
            left.join(right, left_col="z", right_col="q")

    def test_join_missing_right_col_raises(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        with pytest.raises(ValueError, match="is not defined"):
            left.join(right, left_col="p", right_col="z")


# ===========================================================================
# project
# ===========================================================================


class TestProject:
    def test_project_single_column(
        self, single_var_frame: BindingFrame
    ) -> None:
        names = single_var_frame.get_property("p", "name")
        result = single_var_frame.project({"name": names})
        assert list(result.columns) == ["name"]
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_project_multiple_columns(
        self, single_var_frame: BindingFrame
    ) -> None:
        names = single_var_frame.get_property("p", "name")
        ages = single_var_frame.get_property("p", "age")
        result = single_var_frame.project({"name": names, "age": ages})
        assert set(result.columns) == {"name", "age"}
        assert len(result) == 3

    def test_project_returns_plain_dataframe(
        self, single_var_frame: BindingFrame
    ) -> None:
        names = single_var_frame.get_property("p", "name")
        result = single_var_frame.project({"name": names})
        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, BindingFrame)

    def test_project_two_same_type_vars(
        self, two_var_frame: BindingFrame
    ) -> None:
        """Project p.name and q.name as independent columns."""
        p_names = two_var_frame.get_property("p", "name")
        q_names = two_var_frame.get_property("q", "name")
        result = two_var_frame.project({"src": p_names, "tgt": q_names})
        assert list(result["src"]) == ["Alice", "Alice", "Bob"]
        assert list(result["tgt"]) == ["Bob", "Carol", "Carol"]


# ===========================================================================
# mutate
# ===========================================================================


class TestMutate:
    def test_mutate_adds_new_property(self, ctx: Context) -> None:
        """SET p.status = 'active' should appear in subsequent get_property."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        new_values = pd.Series(["active", "active", "active"])
        bf.mutate("p", "status", new_values)

        result = bf.get_property("p", "status")
        assert list(result) == ["active", "active", "active"]

    def test_mutate_overwrites_existing_property(self, ctx: Context) -> None:
        """SET p.age = 99 should overwrite the existing age column."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        bf.mutate("p", "age", pd.Series([99, 99, 99]))
        result = bf.get_property("p", "age")
        assert list(result) == [99, 99, 99]

    def test_mutate_partial_frame_leaves_others_unchanged(
        self, ctx: Context
    ) -> None:
        """Mutating a subset of rows should not corrupt the rest."""
        # Frame with only Alice
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        bf.mutate("p", "age", pd.Series([99]))

        # Full frame should reflect Alice's new age but Bob and Carol unchanged
        full_bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,  # same context, same mutated source_obj
        )
        ages = full_bf.get_property("p", "age")
        assert ages.iloc[0] == 99  # Alice updated
        assert ages.iloc[1] == 25  # Bob unchanged
        assert ages.iloc[2] == 35  # Carol unchanged

    def test_mutate_unknown_variable_raises(self, ctx: Context) -> None:
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        with pytest.raises(ValueError, match="'z' is not defined"):
            bf.mutate("z", "age", pd.Series([1]))


# ===========================================================================
# Anonymous variable convention
# ===========================================================================


class TestAnonymousVariable:
    """Verify that synthetic names for anonymous relationships work identically
    to named variables — as long as the caller uses the same name consistently.

    In the execution engine (Phase 5), anonymous relationship patterns will
    generate synthetic names like '_anon_0'.  These tests confirm BindingFrame
    itself imposes no restrictions on variable names.
    """

    def test_synthetic_name_works_for_get_property(self, ctx: Context) -> None:
        """An internal '_anon_0' column resolves properties normally."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        # Synthetic name still resolves if added to bindings and type_registry
        bf2 = BindingFrame(
            bindings=pd.DataFrame({"_anon_0": [1, 2, 3]}),
            type_registry={"_anon_0": "Person"},
            context=ctx,
        )
        result = bf2.get_property("_anon_0", "name")
        assert list(result) == ["Alice", "Bob", "Carol"]

    def test_synthetic_name_absent_from_project(self, ctx: Context) -> None:
        """Synthetic internal columns need not appear in the projected output."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 1, 2], "_anon_0": [10, 12, 11]}),
            type_registry={"p": "Person", "_anon_0": "KNOWS"},
            context=ctx,
        )
        # Project only the user-visible variable
        names = bf.get_property("p", "name")
        result = bf.project({"name": names})
        assert "_anon_0" not in result.columns
        assert list(result.columns) == ["name"]


# ===========================================================================
# Phase 2 — EntityScan / RelationshipScan / BindingFrame.rename
# ===========================================================================


class TestEntityScan:
    """EntityScan.scan() must return a BindingFrame of all entity IDs."""

    def test_scan_row_count(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        assert len(bf) == 3

    def test_scan_column_name(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        assert bf.var_names == ["p"]

    def test_scan_type_registry(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        assert bf.entity_type("p") == "Person"

    def test_scan_id_values(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        assert set(bf.bindings["p"]) == {1, 2, 3}

    def test_scan_different_var_name(self, ctx: Context) -> None:
        bf = EntityScan("Person", "actor").scan(ctx)
        assert "actor" in bf.bindings.columns
        assert bf.entity_type("actor") == "Person"

    def test_scan_get_property(self, ctx: Context) -> None:
        """Properties can be resolved immediately after a scan."""
        bf = EntityScan("Person", "p").scan(ctx)
        names = bf.get_property("p", "name")
        assert set(names) == {"Alice", "Bob", "Carol"}

    def test_scan_context_is_set(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        assert bf.context is ctx


class TestRelationshipScan:
    """RelationshipScan.scan() must return a BindingFrame with rel + src + tgt columns."""

    def test_scan_row_count(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert len(bf) == 3

    def test_scan_column_names(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert set(bf.var_names) == {"r", "_src_r", "_tgt_r"}

    def test_src_col_property(self) -> None:
        rs = RelationshipScan("KNOWS", "r")
        assert rs.src_col == "_src_r"

    def test_tgt_col_property(self) -> None:
        rs = RelationshipScan("KNOWS", "r")
        assert rs.tgt_col == "_tgt_r"

    def test_scan_rel_ids(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert set(bf.bindings["r"]) == {10, 11, 12}

    def test_scan_src_ids(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert set(bf.bindings["_src_r"]) == {1, 2}

    def test_scan_tgt_ids(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert set(bf.bindings["_tgt_r"]) == {2, 3}

    def test_scan_type_registry_contains_only_rel_var(
        self, ctx: Context
    ) -> None:
        """Structural columns _src_ and _tgt_ are NOT in the type registry."""
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert set(bf.type_registry.keys()) == {"r"}
        assert bf.entity_type("r") == "KNOWS"

    def test_scan_anonymous_rel(self, ctx: Context) -> None:
        """Synthetic names work identically to user-provided names."""
        bf = RelationshipScan("KNOWS", "_anon_0").scan(ctx)
        assert "_anon_0" in bf.bindings.columns
        assert "_src__anon_0" in bf.bindings.columns
        assert "_tgt__anon_0" in bf.bindings.columns
        assert bf.entity_type("_anon_0") == "KNOWS"

    def test_scan_context_is_set(self, ctx: Context) -> None:
        bf = RelationshipScan("KNOWS", "r").scan(ctx)
        assert bf.context is ctx

    def test_scan_then_join_entity(self, ctx: Context) -> None:
        """Full traversal: Person → KNOWS → Person using scans + join + rename."""
        p_bf = EntityScan("Person", "p").scan(ctx)
        r_bf = RelationshipScan("KNOWS", "r").scan(ctx)

        # Join p on _src_r to get (p, r, _tgt_r) rows
        joined = p_bf.join(r_bf, left_col="p", right_col="_src_r")
        assert "p" in joined.var_names
        assert "r" in joined.var_names
        assert "_tgt_r" in joined.var_names
        # Each of Alice's 2 edges + Bob's 1 edge = 3 rows
        assert len(joined) == 3

        # Promote _tgt_r to q (Person)
        q_bf = joined.rename("_tgt_r", "q", new_type="Person")
        assert "q" in q_bf.var_names
        assert "_tgt_r" not in q_bf.var_names
        assert q_bf.entity_type("q") == "Person"

        # Resolve names for both endpoints
        p_names = q_bf.get_property("p", "name")
        q_names = q_bf.get_property("q", "name")
        pairs = set(zip(p_names, q_names))
        assert pairs == {
            ("Alice", "Bob"),
            ("Alice", "Carol"),
            ("Bob", "Carol"),
        }


class TestRename:
    """BindingFrame.rename() must rename a column and update the type registry."""

    def test_rename_column_appears(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        bf2 = bf.rename("p", "actor")
        assert "actor" in bf2.var_names
        assert "p" not in bf2.var_names

    def test_rename_moves_registry_entry(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        bf2 = bf.rename("p", "actor")
        assert bf2.entity_type("actor") == "Person"
        assert "p" not in bf2.type_registry

    def test_rename_with_new_type(self, ctx: Context) -> None:
        r_bf = RelationshipScan("KNOWS", "r").scan(ctx)
        # Promote _tgt_r to q with explicit type
        q_bf = r_bf.rename("_tgt_r", "q", new_type="Person")
        assert q_bf.entity_type("q") == "Person"

    def test_rename_non_registry_column(self, ctx: Context) -> None:
        """Structural columns not in registry are renamed without error."""
        r_bf = RelationshipScan("KNOWS", "r").scan(ctx)
        # _src_r is NOT in type_registry; renaming it is still valid
        r_bf2 = r_bf.rename("_src_r", "src_node")
        assert "src_node" in r_bf2.var_names
        assert "_src_r" not in r_bf2.var_names

    def test_rename_missing_column_raises(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        with pytest.raises(ValueError, match="'z' is not defined"):
            bf.rename("z", "new_name")

    def test_rename_values_unchanged(self, ctx: Context) -> None:
        bf = EntityScan("Person", "p").scan(ctx)
        original_ids = set(bf.bindings["p"])
        bf2 = bf.rename("p", "actor")
        assert set(bf2.bindings["actor"]) == original_ids

    def test_rename_original_frame_unchanged(self, ctx: Context) -> None:
        """rename() must return a new frame without mutating the original."""
        bf = EntityScan("Person", "p").scan(ctx)
        bf.rename("p", "actor")
        assert "p" in bf.var_names
        assert "actor" not in bf.var_names


# ===========================================================================
# Phase 3 — BindingFilter
# ===========================================================================


class TestBindingFilter:
    """BindingFilter.apply() must filter using AST boolean expressions."""

    def test_filter_by_equality(self, ctx: Context) -> None:
        """WHERE p.name = 'Alice' keeps one row."""
        from pycypher.ast_models import (
            Comparison,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        bf = EntityScan("Person", "p").scan(ctx)
        predicate = Comparison(
            operator="=",
            left=PropertyLookup(
                expression=Variable(name="p"), property="name"
            ),
            right=StringLiteral(value="Alice"),
        )
        filtered = BindingFilter(predicate=predicate).apply(bf)
        assert len(filtered) == 1
        names = filtered.get_property("p", "name")
        assert list(names) == ["Alice"]

    def test_filter_by_comparison_gt(self, ctx: Context) -> None:
        """WHERE p.age > 25 keeps Alice (30) and Carol (35)."""
        from pycypher.ast_models import (
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        bf = EntityScan("Person", "p").scan(ctx)
        predicate = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="p"), property="age"),
            right=IntegerLiteral(value=25),
        )
        filtered = BindingFilter(predicate=predicate).apply(bf)
        assert len(filtered) == 2
        names = set(filtered.get_property("p", "name"))
        assert names == {"Alice", "Carol"}

    def test_filter_by_and(self, ctx: Context) -> None:
        """WHERE p.age > 25 AND p.age < 35 keeps only Alice (30)."""
        from pycypher.ast_models import (
            And,
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        bf = EntityScan("Person", "p").scan(ctx)
        predicate = And(
            operands=[
                Comparison(
                    operator=">",
                    left=PropertyLookup(
                        expression=Variable(name="p"), property="age"
                    ),
                    right=IntegerLiteral(value=25),
                ),
                Comparison(
                    operator="<",
                    left=PropertyLookup(
                        expression=Variable(name="p"), property="age"
                    ),
                    right=IntegerLiteral(value=35),
                ),
            ]
        )
        filtered = BindingFilter(predicate=predicate).apply(bf)
        assert len(filtered) == 1
        assert list(filtered.get_property("p", "name")) == ["Alice"]

    def test_filter_nothing_matches(self, ctx: Context) -> None:
        """WHERE p.age > 100 returns empty frame."""
        from pycypher.ast_models import (
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        bf = EntityScan("Person", "p").scan(ctx)
        predicate = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="p"), property="age"),
            right=IntegerLiteral(value=100),
        )
        filtered = BindingFilter(predicate=predicate).apply(bf)
        assert len(filtered) == 0

    def test_filter_all_match(self, ctx: Context) -> None:
        """WHERE p.age > 0 keeps all rows."""
        from pycypher.ast_models import (
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        bf = EntityScan("Person", "p").scan(ctx)
        predicate = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="p"), property="age"),
            right=IntegerLiteral(value=0),
        )
        filtered = BindingFilter(predicate=predicate).apply(bf)
        assert len(filtered) == 3

    def test_filter_after_join(self, ctx: Context) -> None:
        """WHERE p.name = 'Alice' applied after traversal keeps Alice's edges."""
        from pycypher.ast_models import (
            Comparison,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        p_bf = EntityScan("Person", "p").scan(ctx)
        r_bf = RelationshipScan("KNOWS", "r").scan(ctx)
        joined = p_bf.join(r_bf, "p", "_src_r").rename(
            "_tgt_r", "q", new_type="Person"
        )

        predicate = Comparison(
            operator="=",
            left=PropertyLookup(
                expression=Variable(name="p"), property="name"
            ),
            right=StringLiteral(value="Alice"),
        )
        filtered = BindingFilter(predicate=predicate).apply(joined)
        # Alice has 2 outgoing KNOWS edges
        assert len(filtered) == 2
        assert all(filtered.get_property("p", "name") == "Alice")


# ===========================================================================
# Phase 4 — BindingExpressionEvaluator
# ===========================================================================


class TestBindingExpressionEvaluator:
    """BindingExpressionEvaluator must evaluate all expression types vectorially."""

    @pytest.fixture
    def bf(self, ctx: Context) -> BindingFrame:
        return EntityScan("Person", "p").scan(ctx)

    # Literals

    def test_integer_literal(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import IntegerLiteral

        result = BindingExpressionEvaluator(bf).evaluate(
            IntegerLiteral(value=42)
        )
        assert list(result) == [42, 42, 42]

    def test_string_literal(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import StringLiteral

        result = BindingExpressionEvaluator(bf).evaluate(
            StringLiteral(value="hi")
        )
        assert list(result) == ["hi", "hi", "hi"]

    def test_null_literal(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import NullLiteral

        result = BindingExpressionEvaluator(bf).evaluate(NullLiteral())
        assert result.isna().all()

    # Property lookup

    def test_property_lookup(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import PropertyLookup, Variable

        result = BindingExpressionEvaluator(bf).evaluate(
            PropertyLookup(expression=Variable(name="p"), property="name")
        )
        assert set(result) == {"Alice", "Bob", "Carol"}

    def test_property_lookup_unknown_variable_raises(
        self, bf: BindingFrame
    ) -> None:
        from pycypher.ast_models import PropertyLookup, Variable

        with pytest.raises(ValueError):
            BindingExpressionEvaluator(bf).evaluate(
                PropertyLookup(expression=Variable(name="z"), property="name")
            )

    # Arithmetic

    def test_arithmetic_add(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            Arithmetic,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            Arithmetic(
                operator="+",
                left=PropertyLookup(
                    expression=Variable(name="p"), property="age"
                ),
                right=IntegerLiteral(value=10),
            )
        )
        assert set(result) == {40, 35, 45}

    # Comparison

    def test_comparison_eq(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            Comparison,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            Comparison(
                operator="=",
                left=PropertyLookup(
                    expression=Variable(name="p"), property="name"
                ),
                right=StringLiteral(value="Alice"),
            )
        )
        assert list(result) == [True, False, False]

    # Null check

    def test_null_check_is_null(self, ctx: Context) -> None:
        from pycypher.ast_models import NullCheck, PropertyLookup, Variable

        # Add a row with null name for testing
        person_df = ctx.entity_mapping["Person"].source_obj.copy()
        person_df.loc[len(person_df)] = {ID_COLUMN: 99, "name": None, "age": 0}
        ctx.entity_mapping.mapping["Person"].source_obj = person_df

        bf = EntityScan("Person", "p").scan(ctx)
        result = BindingExpressionEvaluator(bf).evaluate(
            NullCheck(
                operator="IS NULL",
                operand=PropertyLookup(
                    expression=Variable(name="p"), property="name"
                ),
            )
        )
        assert result.sum() == 1

    # String predicates

    def test_string_starts_with(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            PropertyLookup,
            StringLiteral,
            StringPredicate,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            StringPredicate(
                operator="STARTS WITH",
                left=PropertyLookup(
                    expression=Variable(name="p"), property="name"
                ),
                right=StringLiteral(value="A"),
            )
        )
        assert list(result) == [True, False, False]

    def test_string_contains(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            PropertyLookup,
            StringLiteral,
            StringPredicate,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            StringPredicate(
                operator="CONTAINS",
                left=PropertyLookup(
                    expression=Variable(name="p"), property="name"
                ),
                right=StringLiteral(value="o"),
            )
        )
        # Bob and Carol contain 'o'
        assert result.sum() == 2

    # Boolean logic

    def test_and(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            And,
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            And(
                operands=[
                    Comparison(
                        operator=">",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=24),
                    ),
                    Comparison(
                        operator="<",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=31),
                    ),
                ]
            )
        )
        # Bob (25) and Alice (30) match; Carol (35) does not
        assert result.sum() == 2

    def test_not(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            Comparison,
            Not,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        result = BindingExpressionEvaluator(bf).evaluate(
            Not(
                operand=Comparison(
                    operator="=",
                    left=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    right=StringLiteral(value="Alice"),
                )
            )
        )
        assert list(result) == [False, True, True]

    # CASE expression (Phase 4 key feature)

    def test_case_searched_two_branches(self, bf: BindingFrame) -> None:
        """CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END."""
        from pycypher.ast_models import (
            CaseExpression,
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            StringLiteral,
            Variable,
            WhenClause,
        )

        expr = CaseExpression(
            when_clauses=[
                WhenClause(
                    condition=Comparison(
                        operator=">=",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=30),
                    ),
                    result=StringLiteral(value="senior"),
                )
            ],
            else_expr=StringLiteral(value="junior"),
        )
        result = BindingExpressionEvaluator(bf).evaluate(expr)
        # Alice=30 (senior), Bob=25 (junior), Carol=35 (senior)
        assert list(result) == ["senior", "junior", "senior"]

    def test_case_multiple_whens_first_wins(self, bf: BindingFrame) -> None:
        """First matching WHEN clause wins — A ≥30, B ≥25, else C."""
        from pycypher.ast_models import (
            CaseExpression,
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            StringLiteral,
            Variable,
            WhenClause,
        )

        expr = CaseExpression(
            when_clauses=[
                WhenClause(
                    condition=Comparison(
                        operator=">=",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=30),
                    ),
                    result=StringLiteral(value="A"),
                ),
                WhenClause(
                    condition=Comparison(
                        operator=">=",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=25),
                    ),
                    result=StringLiteral(value="B"),
                ),
            ],
            else_expr=StringLiteral(value="C"),
        )
        result = BindingExpressionEvaluator(bf).evaluate(expr)
        # Alice=30 → A (≥30 wins over ≥25), Bob=25 → B, Carol=35 → A
        assert list(result) == ["A", "B", "A"]

    def test_case_no_else_returns_none(self, bf: BindingFrame) -> None:
        """CASE with no ELSE and no matching WHEN returns None."""
        from pycypher.ast_models import (
            CaseExpression,
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            StringLiteral,
            Variable,
            WhenClause,
        )

        expr = CaseExpression(
            when_clauses=[
                WhenClause(
                    condition=Comparison(
                        operator=">",
                        left=PropertyLookup(
                            expression=Variable(name="p"), property="age"
                        ),
                        right=IntegerLiteral(value=100),
                    ),
                    result=StringLiteral(value="impossible"),
                )
            ],
            else_expr=None,
        )
        result = BindingExpressionEvaluator(bf).evaluate(expr)
        assert result.isna().all()

    # Scalar functions

    def test_scalar_function_toupper(self, bf: BindingFrame) -> None:
        from pycypher.ast_models import (
            FunctionInvocation,
            PropertyLookup,
            Variable,
        )

        expr = FunctionInvocation(
            name="toUpper",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    )
                ]
            },
        )
        result = BindingExpressionEvaluator(bf).evaluate(expr)
        assert set(result) == {"ALICE", "BOB", "CAROL"}


# ===========================================================================
# Query atomicity — shadow write semantics (Architect Loop 2)
# ===========================================================================


class TestQueryAtomicity:
    """SET mutations must be atomic: committed on success, rolled back on failure.

    These tests were written *before* the implementation (TDD red phase).
    """

    def test_failed_query_does_not_mutate_context(self, ctx: Context) -> None:
        """A query that raises after a SET must not persist the mutation."""
        from pycypher.star import Star

        star = Star(context=ctx)
        original_names: list = list(
            ctx.entity_mapping["Person"].source_obj["name"]
        )

        # The RETURN references a non-existent variable; the query will fail.
        with pytest.raises(Exception):
            star.execute_query(
                "MATCH (p:Person) SET p.name = 'Changed' RETURN nosuchvar.foo AS x"
            )

        current_names: list = list(
            ctx.entity_mapping["Person"].source_obj["name"]
        )
        assert current_names == original_names, (
            "Rolled-back mutation must not persist to context"
        )

    def test_successful_query_persists_mutation(self, ctx: Context) -> None:
        """A successful SET query must persist changes for subsequent queries."""
        from pycypher.star import Star

        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) SET p.score = 99 RETURN p.name AS name"
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.score AS score")
        assert (result["score"] == 99).all()

    def test_same_literal_set_twice_is_idempotent(self, ctx: Context) -> None:
        """SET p.prop = 'fixed' run twice must produce the same result."""
        from pycypher.star import Star

        star = Star(context=ctx)
        star.execute_query(
            "MATCH (p:Person) SET p.tag = 'hello' RETURN p.name AS n"
        )
        result1 = list(
            star.execute_query("MATCH (p:Person) RETURN p.tag AS tag")["tag"]
        )
        star.execute_query(
            "MATCH (p:Person) SET p.tag = 'hello' RETURN p.name AS n"
        )
        result2 = list(
            star.execute_query("MATCH (p:Person) RETURN p.tag AS tag")["tag"]
        )
        assert result1 == result2

    def test_context_begin_rollback_clears_shadow(self, ctx: Context) -> None:
        """Context.begin_query() / rollback_query() must leave source_obj intact."""
        entity_table = ctx.entity_mapping["Person"]
        original_df_id = id(entity_table.source_obj)

        ctx.begin_query()
        ctx.rollback_query()

        assert id(entity_table.source_obj) == original_df_id, (
            "rollback_query must not replace source_obj"
        )


# ===========================================================================
# Coverage expansion — uncovered code paths
# ===========================================================================


class TestSourceToPandas:
    """Tests for the _source_to_pandas helper function."""

    def test_pandas_dataframe_passthrough(self) -> None:
        """A pd.DataFrame is returned unchanged."""
        from pycypher.binding_frame import _source_to_pandas

        df = pd.DataFrame({"a": [1, 2]})
        result = _source_to_pandas(df)
        assert result is df

    def test_pyarrow_table_conversion(self) -> None:
        """A pyarrow.Table is converted to pd.DataFrame."""
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        from pycypher.binding_frame import _source_to_pandas

        table = pa.table({"x": [10, 20], "y": ["a", "b"]})
        result = _source_to_pandas(table)
        assert isinstance(result, pd.DataFrame)
        assert list(result["x"]) == [10, 20]
        assert list(result["y"]) == ["a", "b"]


class TestLeftJoin:
    """Tests for BindingFrame.left_join (OPTIONAL MATCH semantics)."""

    def test_left_join_preserves_unmatched_rows(self, ctx: Context) -> None:
        """Rows in left frame with no match get NaN for right columns."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        # Only Alice (1) and Bob (2) have matches
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1, 2], "val": [100, 200]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        result = left.left_join(right, left_col="p", right_col="q")
        assert len(result) == 3  # Carol preserved with NaN
        assert "val" in result.var_names

    def test_left_join_all_matched(self, ctx: Context) -> None:
        """When all left rows match, behaves like inner join."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1, 2], "val": [10, 20]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        result = left.left_join(right, left_col="p", right_col="q")
        assert len(result) == 2

    def test_left_join_merges_type_registries(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"r": [10], "_src_r": [1]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        result = left.left_join(right, left_col="p", right_col="_src_r")
        assert "p" in result.type_registry
        assert "r" in result.type_registry

    def test_left_join_missing_left_col_raises(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        with pytest.raises(ValueError, match="is not defined"):
            left.left_join(right, left_col="z", right_col="q")

    def test_left_join_missing_right_col_raises(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [1]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        with pytest.raises(ValueError, match="is not defined"):
            left.left_join(right, left_col="p", right_col="z")


class TestCrossJoin:
    """Tests for BindingFrame.cross_join (Cartesian product)."""

    def test_cross_join_row_count(self, ctx: Context) -> None:
        """3 × 2 = 6 rows."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [10, 20]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        result = left.cross_join(right)
        assert len(result) == 6

    def test_cross_join_merges_registries(self, ctx: Context) -> None:
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": [10]}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        result = left.cross_join(right)
        assert "p" in result.type_registry
        assert "q" in result.type_registry

    def test_cross_join_exceeds_limit_raises(self, ctx: Context) -> None:
        """Cross join above MAX_CROSS_JOIN_ROWS raises MemoryError."""
        import pycypher.binding_frame as bf_mod

        original = bf_mod.MAX_CROSS_JOIN_ROWS
        try:
            bf_mod.MAX_CROSS_JOIN_ROWS = 5  # Set very low limit
            left = BindingFrame(
                bindings=pd.DataFrame({"p": [1, 2, 3]}),
                type_registry={"p": "Person"},
                context=ctx,
            )
            right = BindingFrame(
                bindings=pd.DataFrame({"q": [10, 20]}),
                type_registry={"q": "Person"},
                context=ctx,
            )
            with pytest.raises(MemoryError, match="safety limit"):
                left.cross_join(right)
        finally:
            bf_mod.MAX_CROSS_JOIN_ROWS = original

    def test_cross_join_empty_frame(self, ctx: Context) -> None:
        """Cross join with empty frame produces 0 rows."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"q": pd.Series([], dtype=int)}),
            type_registry={"q": "Person"},
            context=ctx,
        )
        result = left.cross_join(right)
        assert len(result) == 0


class TestCleanupMerged:
    """Tests for BindingFrame._cleanup_merged static method."""

    def test_drops_right_col_when_different(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = BindingFrame._cleanup_merged(df, left_col="a", right_col="b")
        assert "a" in result.columns
        assert "b" not in result.columns

    def test_keeps_shared_col(self) -> None:
        df = pd.DataFrame({"a": [1], "c": [3]})
        result = BindingFrame._cleanup_merged(df, left_col="a", right_col="a")
        assert "a" in result.columns

    def test_drops_right_suffix_columns(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "b_right": [3]})
        result = BindingFrame._cleanup_merged(df, left_col="a", right_col="a")
        assert "b_right" not in result.columns
        assert "b" in result.columns

    def test_cross_join_mode_no_keys(self) -> None:
        df = pd.DataFrame({"x": [1], "y": [2]})
        result = BindingFrame._cleanup_merged(df)
        assert list(result.columns) == ["x", "y"]


class TestInferEntityType:
    """Tests for BindingFrame._infer_entity_type (unlabeled nodes)."""

    def test_infer_existing_type(self, ctx: Context) -> None:
        """IDs from Person table should be inferred as Person."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"q": [1, 2, 3]}),
            type_registry={},  # No type registered for q
            context=ctx,
        )
        assert bf._infer_entity_type("q") == "Person"

    def test_infer_returns_none_for_unknown_ids(self, ctx: Context) -> None:
        """IDs not in any entity table return None."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"q": [999, 998]}),
            type_registry={},
            context=ctx,
        )
        assert bf._infer_entity_type("q") is None

    def test_infer_returns_none_for_empty_frame(self, ctx: Context) -> None:
        bf = BindingFrame(
            bindings=pd.DataFrame({"q": pd.Series([], dtype=int)}),
            type_registry={},
            context=ctx,
        )
        assert bf._infer_entity_type("q") is None

    def test_infer_returns_none_for_missing_col(self, ctx: Context) -> None:
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={},
            context=ctx,
        )
        assert bf._infer_entity_type("nonexistent") is None


class TestGetPropertyEdgeCases:
    """Edge cases for get_property not covered by basic tests."""

    def test_variable_not_in_registry_but_inferable(
        self, ctx: Context
    ) -> None:
        """Variable without type_registry entry should be auto-detected."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"q": [1, 2, 3]}),
            type_registry={},  # No entry for q
            context=ctx,
        )
        result = bf.get_property("q", "name")
        assert set(result) == {"Alice", "Bob", "Carol"}

    def test_variable_not_inferable_raises(self, ctx: Context) -> None:
        """Variable with unknown IDs and no type_registry entry raises."""
        from pycypher.exceptions import VariableTypeMismatchError

        bf = BindingFrame(
            bindings=pd.DataFrame({"q": [999]}),
            type_registry={},
            context=ctx,
        )
        with pytest.raises(VariableTypeMismatchError):
            bf.get_property("q", "name")

    def test_empty_frame_not_in_registry_returns_empty(
        self, ctx: Context
    ) -> None:
        """Empty frame with unregistered variable returns empty series."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"q": pd.Series([], dtype=int)}),
            type_registry={},
            context=ctx,
        )
        result = bf.get_property("q", "name")
        assert len(result) == 0

    def test_entity_type_not_in_mapping_returns_null(
        self, ctx: Context
    ) -> None:
        """Entity type registered but not in entity_mapping returns null series."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2]}),
            type_registry={"x": "NonExistentType"},
            context=ctx,
        )
        result = bf.get_property("x", "name")
        assert len(result) == 2
        assert all(v is None for v in result)

    def test_relationship_property_lookup(self, ctx: Context) -> None:
        """get_property on a relationship variable fetches from relationship table."""
        # Add a 'weight' property to KNOWS relationships
        knows_df = ctx.relationship_mapping["KNOWS"].source_obj
        knows_df["weight"] = [1.0, 2.0, 3.0]
        ctx.relationship_mapping.mapping["KNOWS"].column_names.append("weight")

        bf = BindingFrame(
            bindings=pd.DataFrame({"r": [10, 11, 12]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        result = bf.get_property("r", "weight")
        assert list(result) == [1.0, 2.0, 3.0]

    def test_multitype_variable_property_lookup(self, ctx: Context) -> None:
        """__MULTI__ type variable does per-row lookup across entity tables."""
        # Add a second entity type
        animal_df = pd.DataFrame(
            {
                ID_COLUMN: [100, 101],
                "name": ["Rex", "Spot"],
            }
        )
        animal_table = EntityTable(
            entity_type="Animal",
            identifier="Animal",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=animal_df,
        )
        ctx.entity_mapping.mapping["Animal"] = animal_table

        bf = BindingFrame(
            bindings=pd.DataFrame({"n": [1, 100, 2, 101]}),
            type_registry={"n": "__MULTI__"},
            context=ctx,
        )
        result = bf.get_property("n", "name")
        assert list(result) == ["Alice", "Rex", "Bob", "Spot"]


class TestMutateRelationship:
    """Tests for mutating relationship properties via BindingFrame.mutate."""

    def test_mutate_relationship_property(self, ctx: Context) -> None:
        """SET r.weight = value should update relationship table."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"r": [10, 11, 12]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        bf.mutate("r", "weight", pd.Series([1.0, 2.0, 3.0]))

        # Verify the property was set
        result = bf.get_property("r", "weight")
        assert list(result) == [1.0, 2.0, 3.0]

    def test_mutate_entity_type_not_in_mapping_raises(
        self, ctx: Context
    ) -> None:
        """Mutating with a type not in any mapping raises ValueError."""
        bf = BindingFrame(
            bindings=pd.DataFrame({"x": [1]}),
            type_registry={"x": "FakeType"},
            context=ctx,
        )
        with pytest.raises((ValueError, KeyError)):
            bf.mutate("x", "prop", pd.Series([42]))


class TestRelationshipScanPushdown:
    """Tests for predicate pushdown in RelationshipScan."""

    def test_scan_with_source_ids(self, ctx: Context) -> None:
        """Filtering by source IDs should reduce results."""
        source_ids = pd.Series([1])  # Only Alice
        bf = RelationshipScan("KNOWS", "r").scan(ctx, source_ids=source_ids)
        # Alice is source of edges 10, 12
        assert len(bf) == 2

    def test_scan_with_target_ids(self, ctx: Context) -> None:
        """Filtering by target IDs should reduce results."""
        target_ids = pd.Series([3])  # Only Carol
        bf = RelationshipScan("KNOWS", "r").scan(ctx, target_ids=target_ids)
        # Carol is target of edges 11, 12
        assert len(bf) == 2

    def test_scan_with_both_filters(self, ctx: Context) -> None:
        """Filtering by both source and target narrows further."""
        source_ids = pd.Series([1])  # Alice
        target_ids = pd.Series([2])  # Bob
        bf = RelationshipScan("KNOWS", "r").scan(
            ctx,
            source_ids=source_ids,
            target_ids=target_ids,
        )
        # Alice→Bob is edge 10
        assert len(bf) == 1
        assert bf.bindings["r"].iloc[0] == 10

    def test_scan_no_matches(self, ctx: Context) -> None:
        """Filtering that matches nothing returns empty frame."""
        source_ids = pd.Series([999])
        bf = RelationshipScan("KNOWS", "r").scan(ctx, source_ids=source_ids)
        assert len(bf) == 0


class TestEntityScanErrors:
    """Error handling for EntityScan and RelationshipScan."""

    def test_entity_scan_missing_type_raises(self, ctx: Context) -> None:
        """Scanning a nonexistent entity type raises GraphTypeNotFoundError."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            EntityScan("NonExistent", "n").scan(ctx)

    def test_relationship_scan_missing_type_raises(self, ctx: Context) -> None:
        """Scanning a nonexistent relationship type raises GraphTypeNotFoundError."""
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            RelationshipScan("FAKE_REL", "r").scan(ctx)
