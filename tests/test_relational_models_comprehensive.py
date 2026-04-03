"""Comprehensive tests for relational_models.py — EntityTable, RelationshipTable, Context.

These are the foundational data structures for all query execution.
Tests cover construction, mutation transactions, shadow writes, savepoints,
procedure registry, and edge cases.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    ProcedureRegistry,
    RegisteredFunction,
    RelationshipMapping,
    RelationshipTable,
    flatten,
)

# ===========================================================================
# Helper fixtures
# ===========================================================================


@pytest.fixture
def person_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )


@pytest.fixture
def company_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [10, 20],
            "company_name": ["Acme", "Globex"],
        },
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [100, 101],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2],
            RELATIONSHIP_TARGET_COLUMN: [2, 3],
            "since": [2020, 2021],
        },
    )


@pytest.fixture
def person_table(person_df: pd.DataFrame) -> EntityTable:
    return EntityTable.from_dataframe("Person", person_df)


@pytest.fixture
def company_table(company_df: pd.DataFrame) -> EntityTable:
    return EntityTable.from_dataframe("Company", company_df)


@pytest.fixture
def knows_table(knows_df: pd.DataFrame) -> RelationshipTable:
    return RelationshipTable.from_dataframe("KNOWS", knows_df)


@pytest.fixture
def context(
    person_table: EntityTable,
    company_table: EntityTable,
    knows_table: RelationshipTable,
) -> Context:
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "Company": company_table},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


# ===========================================================================
# flatten utility
# ===========================================================================


class TestFlatten:
    def test_flat_list(self) -> None:
        assert flatten([1, 2, 3]) == [1, 2, 3]

    def test_nested_list(self) -> None:
        assert flatten([1, [2, 3], [4, [5]]]) == [1, 2, 3, 4, 5]

    def test_empty(self) -> None:
        assert flatten([]) == []


# ===========================================================================
# EntityMapping / RelationshipMapping
# ===========================================================================


class TestEntityMapping:
    def test_getitem(self, person_table: EntityTable) -> None:
        mapping = EntityMapping(mapping={"Person": person_table})
        assert mapping["Person"] is person_table

    def test_getitem_missing_raises(self) -> None:
        mapping = EntityMapping(mapping={})
        with pytest.raises(KeyError):
            mapping["Missing"]


class TestRelationshipMapping:
    def test_getitem(self, knows_table: RelationshipTable) -> None:
        mapping = RelationshipMapping(mapping={"KNOWS": knows_table})
        assert mapping["KNOWS"] is knows_table

    def test_getitem_missing_raises(self) -> None:
        mapping = RelationshipMapping(mapping={})
        with pytest.raises(KeyError):
            mapping["MISSING"]


# ===========================================================================
# RegisteredFunction
# ===========================================================================


class TestRegisteredFunction:
    def test_call_passthrough(self) -> None:
        fn = RegisteredFunction(
            name="double",
            implementation=lambda x: x * 2,
            arity=1,
        )
        assert fn(5) == 10

    def test_arity_mismatch_raises(self) -> None:
        fn = RegisteredFunction(
            name="double",
            implementation=lambda x: x * 2,
            arity=1,
        )
        with pytest.raises(Exception):  # FunctionArgumentError
            fn(1, 2)

    def test_zero_arity_accepts_any(self) -> None:
        fn = RegisteredFunction(
            name="anything",
            implementation=lambda *args: sum(args),
            arity=0,
        )
        assert fn(1, 2, 3) == 6


# ===========================================================================
# ProcedureRegistry
# ===========================================================================


class TestProcedureRegistry:
    def test_builtin_db_labels(self, context: Context) -> None:
        registry = ProcedureRegistry()
        result = registry.execute("db.labels", context, [])
        labels = [r["label"] for r in result]
        assert "Company" in labels
        assert "Person" in labels

    def test_builtin_db_relationship_types(self, context: Context) -> None:
        registry = ProcedureRegistry()
        result = registry.execute("db.relationshipTypes", context, [])
        types = [r["relationshipType"] for r in result]
        assert "KNOWS" in types

    def test_builtin_db_property_keys(self, context: Context) -> None:
        registry = ProcedureRegistry()
        result = registry.execute("db.propertyKeys", context, [])
        keys = [r["propertyKey"] for r in result]
        assert "name" in keys
        assert "age" in keys
        assert "company_name" in keys
        # Internal columns should be excluded
        assert ID_COLUMN not in keys

    def test_register_custom_procedure(self, context: Context) -> None:
        registry = ProcedureRegistry()

        @registry.register("my.proc")
        def my_proc(ctx: Context, args: list) -> list[dict]:
            return [{"value": 42}]

        result = registry.execute("my.proc", context, [])
        assert result == [{"value": 42}]

    def test_execute_unknown_raises(self, context: Context) -> None:
        registry = ProcedureRegistry()
        with pytest.raises(ValueError, match="Unknown procedure"):
            registry.execute("nonexistent.proc", context, [])

    def test_case_insensitive_lookup(self, context: Context) -> None:
        registry = ProcedureRegistry()
        # Built-ins should work case-insensitively
        result = registry.execute("DB.LABELS", context, [])
        assert len(result) > 0


# ===========================================================================
# EntityTable
# ===========================================================================


class TestEntityTable:
    def test_from_dataframe(self, person_df: pd.DataFrame) -> None:
        table = EntityTable.from_dataframe("Person", person_df)
        assert table.entity_type == "Person"
        assert ID_COLUMN in table.column_names
        assert "name" in table.attribute_map
        assert "age" in table.attribute_map

    def test_from_dataframe_custom_id_col(self) -> None:
        df = pd.DataFrame({"person_id": [1, 2], "name": ["Alice", "Bob"]})
        table = EntityTable.from_dataframe("Person", df, id_col="person_id")
        assert ID_COLUMN in table.source_obj.columns
        assert "person_id" not in table.source_obj.columns

    def test_from_dataframe_missing_id_raises(self) -> None:
        df = pd.DataFrame({"name": ["Alice"]})
        with pytest.raises(ValueError, match="__ID__"):
            EntityTable.from_dataframe("Person", df)

    def test_from_dataframe_bad_id_col_raises(self) -> None:
        df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"]})
        with pytest.raises(ValueError, match="not found"):
            EntityTable.from_dataframe("Person", df, id_col="nonexistent")

    def test_to_pandas_prefixes(self, person_table: EntityTable) -> None:
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        df = person_table.to_pandas(ctx)
        assert f"Person__{ID_COLUMN}" in df.columns
        assert "Person__name" in df.columns
        assert "Person__age" in df.columns


# ===========================================================================
# RelationshipTable
# ===========================================================================


class TestRelationshipTable:
    def test_from_dataframe(self, knows_df: pd.DataFrame) -> None:
        table = RelationshipTable.from_dataframe("KNOWS", knows_df)
        assert table.relationship_type == "KNOWS"
        assert ID_COLUMN in table.column_names

    def test_to_pandas_prefixes(
        self,
        knows_table: RelationshipTable,
        context: Context,
    ) -> None:
        df = knows_table.to_pandas(context)
        assert f"KNOWS__{ID_COLUMN}" in df.columns
        assert f"KNOWS__{RELATIONSHIP_SOURCE_COLUMN}" in df.columns
        assert f"KNOWS__{RELATIONSHIP_TARGET_COLUMN}" in df.columns


# ===========================================================================
# Context — construction and properties
# ===========================================================================


class TestContextConstruction:
    def test_default_backend_is_pandas(self, context: Context) -> None:
        assert context.backend_name == "pandas"

    def test_repr_shows_entities(self, context: Context) -> None:
        r = repr(context)
        assert "Person" in r
        assert "Company" in r
        assert "KNOWS" in r

    def test_empty_context(self) -> None:
        ctx = Context()
        assert ctx.backend_name == "pandas"
        assert repr(ctx).startswith("Context(")

    def test_entity_lookup(self, context: Context) -> None:
        table = context.entity_mapping["Person"]
        assert table.entity_type == "Person"

    def test_relationship_lookup(self, context: Context) -> None:
        table = context.relationship_mapping["KNOWS"]
        assert table.relationship_type == "KNOWS"


# ===========================================================================
# Context — transaction lifecycle (begin/commit/rollback)
# ===========================================================================


class TestContextTransactions:
    def test_begin_clears_shadow(self, context: Context) -> None:
        context.begin_query()
        assert context._shadow == {}
        assert context._shadow_rels == {}

    def test_commit_promotes_entity_shadow(self, context: Context) -> None:
        context.begin_query()
        # Simulate a SET mutation via shadow
        original_df = context.entity_mapping["Person"].source_obj
        shadow_df = original_df.copy()
        shadow_df.loc[shadow_df["name"] == "Alice", "age"] = 99
        context._shadow["Person"] = shadow_df
        context.commit_query()

        # Verify the canonical table was updated
        updated = context.entity_mapping["Person"].source_obj
        assert updated.loc[updated["name"] == "Alice", "age"].iloc[0] == 99

    def test_commit_new_entity_type(self, context: Context) -> None:
        context.begin_query()
        new_df = pd.DataFrame({ID_COLUMN: [100], "flavor": ["vanilla"]})
        context._shadow["IceCream"] = new_df
        context.commit_query()

        # New entity type should be registered
        assert "IceCream" in context.entity_mapping.mapping

    def test_commit_increments_data_epoch(self, context: Context) -> None:
        epoch_before = context._data_epoch
        context.begin_query()
        context._shadow["Person"] = context.entity_mapping[
            "Person"
        ].source_obj.copy()
        context.commit_query()
        assert context._data_epoch == epoch_before + 1

    def test_commit_no_mutations_preserves_epoch(
        self, context: Context
    ) -> None:
        epoch_before = context._data_epoch
        context.begin_query()
        context.commit_query()  # No shadow writes
        assert context._data_epoch == epoch_before

    def test_rollback_discards_shadow(self, context: Context) -> None:
        context.begin_query()
        original_df = context.entity_mapping["Person"].source_obj
        shadow_df = original_df.copy()
        shadow_df.loc[shadow_df["name"] == "Alice", "age"] = 99
        context._shadow["Person"] = shadow_df
        context.rollback_query()

        # Canonical table should be unchanged
        current = context.entity_mapping["Person"].source_obj
        assert current.loc[current["name"] == "Alice", "age"].iloc[0] == 30

    def test_rollback_clears_shadow(self, context: Context) -> None:
        context.begin_query()
        context._shadow["Person"] = pd.DataFrame()
        context.rollback_query()
        assert context._shadow == {}


# ===========================================================================
# Context — savepoints
# ===========================================================================


class TestContextSavepoints:
    def test_savepoint_captures_snapshot(self, context: Context) -> None:
        context.begin_query()
        context._shadow["Person"] = context.entity_mapping[
            "Person"
        ].source_obj.copy()
        sp = context.savepoint()
        assert "Person" in sp["entities"]

    def test_restore_savepoint(self, context: Context) -> None:
        context.begin_query()
        original = context.entity_mapping["Person"].source_obj.copy()
        context._shadow["Person"] = original
        sp = context.savepoint()

        # Mutate further
        mutated = original.copy()
        mutated.loc[0, "name"] = "MUTATED"
        context._shadow["Person"] = mutated

        # Restore
        context.restore_savepoint(sp)
        assert context._shadow["Person"].loc[0, "name"] != "MUTATED"

    def test_savepoint_is_independent_copy(self, context: Context) -> None:
        context.begin_query()
        context._shadow["Person"] = context.entity_mapping[
            "Person"
        ].source_obj.copy()
        sp = context.savepoint()

        # Modifying the snapshot should not affect the shadow
        sp["entities"]["Person"].loc[0, "name"] = "HACKED"
        assert context._shadow["Person"].loc[0, "name"] != "HACKED"


# ===========================================================================
# Context — timeout management
# ===========================================================================


class TestContextTimeout:
    def test_set_deadline(self, context: Context) -> None:
        context.set_deadline(10.0)
        assert context._query_deadline is not None
        assert context._query_timeout_seconds == 10.0

    def test_set_deadline_none(self, context: Context) -> None:
        context.set_deadline(None)
        assert context._query_deadline is None

    def test_check_timeout_no_deadline(self, context: Context) -> None:
        context.set_deadline(None)
        context.check_timeout()  # Should not raise

    def test_check_timeout_not_expired(self, context: Context) -> None:
        context.set_deadline(60.0)  # 60 seconds from now
        context.check_timeout()  # Should not raise

    def test_check_timeout_expired_raises(self, context: Context) -> None:
        from pycypher.exceptions import QueryTimeoutError

        context.set_deadline(0.001)  # 1ms timeout
        time.sleep(0.05)  # Wait for expiry (generous for CI)
        with pytest.raises(QueryTimeoutError):
            context.check_timeout()

    def test_clear_deadline(self, context: Context) -> None:
        context.set_deadline(10.0)
        context.clear_deadline()
        assert context._query_deadline is None
        assert context._query_timeout_seconds is None


# ===========================================================================
# Context — custom function registration
# ===========================================================================


class TestContextCypherFunctions:
    def test_register_function_decorator(self, context: Context) -> None:
        @context.cypher_function
        def my_func(x: int) -> int:
            return x + 1

        assert "my_func" in context.cypher_functions
        assert context.cypher_functions["my_func"](5) == 6

    def test_registered_function_arity(self, context: Context) -> None:
        @context.cypher_function
        def add(a: int, b: int) -> int:
            return a + b

        rf = context.cypher_functions["add"]
        assert rf.arity == 2
        assert rf(3, 4) == 7


# ===========================================================================
# Context — relationship shadow writes
# ===========================================================================


class TestContextRelationshipShadow:
    def test_commit_relationship_shadow(self, context: Context) -> None:
        context.begin_query()
        new_rel = pd.DataFrame(
            {
                ID_COLUMN: [200],
                RELATIONSHIP_SOURCE_COLUMN: [1],
                RELATIONSHIP_TARGET_COLUMN: [3],
            },
        )
        context._shadow_rels["LIKES"] = new_rel
        context.commit_query()

        assert "LIKES" in context.relationship_mapping.mapping

    def test_rollback_relationship_shadow(self, context: Context) -> None:
        context.begin_query()
        context._shadow_rels["LIKES"] = pd.DataFrame()
        context.rollback_query()
        assert "LIKES" not in context.relationship_mapping.mapping
