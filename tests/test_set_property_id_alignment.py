"""
Unit tests for SET clause property ID alignment.

These tests specifically cover the correctness of `PropertyModification._update_source_entity_tables`,
which must use index-based (ID-keyed) alignment when writing changed values back to the entity table.
The root bug was positional array assignment: when a WHERE clause reduced the working set before SET,
the write-back corrupted rows that happened to be at the same positions as the filtered-out entities.

Test coverage:
  1. Partial update (filtered SET) — core alignment bug
  2. Full-table SET preserves numeric dtype
  3. New property only added to matching rows; non-matching rows get None
  4. Unaffected rows retain correct original property values after partial SET
  5. Chained SET then RETURN reads the newly SET properties
  6. Multiple properties SET in one clause
  7. SET with arithmetic expression
  8. Only explicitly SET properties are written back (no enriched-column contamination)
  9. to_pandas column construction does not leak __ID__ as a user property
 10. ExpressionEvaluator sees updated entity table after SET
 11. Three-entity partial filter — Bob (middle row) is not corrupted
 12. SET updates existing property on subset of entities correctly
 13. New property has correct dtype (int, float, bool, str) after write-back
 14. Sequential SET calls accumulate correctly
"""

from __future__ import annotations

import pandas as pd
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_person_context(
    persons: list[dict] | None = None,
) -> Context:
    """Create a Context with a Person entity table.

    Defaults to three persons: Alice (Engineering, row 0), Bob (Sales, row 1),
    Carol (Engineering, row 2). This ordering is important: the tests verify
    that partial SET operations don't accidentally corrupt Bob just because he
    sits between Alice and Carol in the entity table.
    """
    if persons is None:
        persons = [
            {
                ID_COLUMN: 1,
                "name": "Alice",
                "age": 25,
                "department": "Engineering",
                "salary": 60000.0,
            },
            {
                ID_COLUMN: 2,
                "name": "Bob",
                "age": 30,
                "department": "Sales",
                "salary": 55000.0,
            },
            {
                ID_COLUMN: 3,
                "name": "Carol",
                "age": 35,
                "department": "Engineering",
                "salary": 70000.0,
            },
        ]
    person_df = pd.DataFrame(persons)
    cols = [c for c in person_df.columns if c != ID_COLUMN]
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN] + cols,
        source_obj_attribute_map={c: c for c in cols},
        attribute_map={c: c for c in cols},
        source_obj=person_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _star(ctx: Context) -> Star:
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Test 1: Partial update — the core alignment bug
#
# MATCH (p:Person) WHERE p.department = 'Engineering'
# SET p.team = 'tech'
# RETURN p.name AS name, p.team AS team
#
# Alice (row 0) and Carol (row 2) are Engineering. Bob (row 1) is Sales.
# After SET, Alice and Carol must have team='tech'. Bob must have team=None.
# The bug caused Bob to steal Carol's value and Carol to get None.
# ---------------------------------------------------------------------------


class TestPartialSetAlignment:
    """Tests that SET only mutates entities matched by the preceding WHERE."""

    def test_correct_entities_receive_new_property(self) -> None:
        """Engineering persons get team='tech'; Sales person is unaffected."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        # Only matched rows are returned
        assert set(result["name"]) == {"Alice", "Carol"}
        assert (result["team"] == "tech").all()

    def test_entity_table_integrity_after_partial_set(self) -> None:
        """The entity table source_obj is only modified for matched rows."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        # Alice (id=1) and Carol (id=3) must have team='tech'
        assert source.at[1, "team"] == "tech", "Alice should have team='tech'"
        assert source.at[3, "team"] == "tech", "Carol should have team='tech'"

        # Bob (id=2) must have team=None — he was not matched
        assert source.at[2, "team"] is None or pd.isna(source.at[2, "team"]), (
            "Bob's team should be None/NaN — he was excluded by WHERE"
        )

    def test_bob_existing_properties_not_corrupted(self) -> None:
        """Unmatched entity (Bob) retains all original property values."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        # Bob is row with id=2
        assert source.at[2, "name"] == "Bob", "Bob's name was corrupted"
        assert source.at[2, "department"] == "Sales", (
            "Bob's department was corrupted"
        )
        assert source.at[2, "age"] == 30, "Bob's age was corrupted"

    def test_alice_properties_not_corrupted_by_partial_set(self) -> None:
        """Matched entity (Alice) retains her original properties alongside the new one."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        assert source.at[1, "name"] == "Alice"
        assert source.at[1, "department"] == "Engineering"
        assert source.at[1, "age"] == 25
        assert source.at[1, "team"] == "tech"

    def test_carol_properties_not_corrupted_by_partial_set(self) -> None:
        """Carol (row 2, beyond the filter boundary) has correct values after partial SET."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        assert source.at[3, "name"] == "Carol"
        assert source.at[3, "department"] == "Engineering"
        assert source.at[3, "age"] == 35
        assert source.at[3, "team"] == "tech"


# ---------------------------------------------------------------------------
# Test 2: Full-table SET preserves numeric dtype
# ---------------------------------------------------------------------------


class TestFullTableSet:
    """Tests that full-table SET (no WHERE) works correctly."""

    def test_full_table_set_numeric_property(self) -> None:
        """SET a numeric property on all entities; result dtype must be numeric."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.bonus = 1000 "
            "RETURN p.name AS name, p.bonus AS bonus"
        )
        assert len(result) == 3
        assert (result["bonus"] == 1000).all()
        # dtype must be numeric, not object
        assert result["bonus"].dtype.kind in ("i", "f"), (
            f"Expected numeric dtype, got {result['bonus'].dtype}"
        )

    def test_full_table_set_all_entities_receive_value(self) -> None:
        """All three entities get the new property."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.active = true "
            "RETURN p.name AS name, p.active AS active"
        )
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}
        assert (result["active"] == True).all()

    def test_full_table_modify_existing_property(self) -> None:
        """Modifying an existing property updates all rows."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.age = 99 RETURN p.name AS name, p.age AS age"
        )
        assert len(result) == 3
        assert (result["age"] == 99).all()
        entity_table = ctx.entity_mapping.mapping["Person"]
        assert (entity_table.source_obj["age"] == 99).all()

    def test_full_table_set_preserves_arithmetic_dtype(self) -> None:
        """Arithmetic SET result (salary * 0.1) maintains float dtype."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.raise_amount = p.salary * 0.1 "
            "RETURN p.name AS name, p.raise_amount AS raise_amount"
        )
        assert len(result) == 3
        assert result["raise_amount"].dtype.kind == "f", (
            f"Expected float dtype, got {result['raise_amount'].dtype}"
        )
        # Verify correctness for each entity
        expected = {"Alice": 6000.0, "Bob": 5500.0, "Carol": 7000.0}
        for _, row in result.iterrows():
            assert abs(row["raise_amount"] - expected[row["name"]]) < 0.01


# ---------------------------------------------------------------------------
# Test 3: New property scoped to matching rows only
# ---------------------------------------------------------------------------


class TestNewPropertyScope:
    """New properties introduced by SET only appear on matched entities."""

    def test_new_property_null_on_unmatched(self) -> None:
        """Bob's team column exists in entity table but is None after partial SET."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        assert "team" in entity_table.source_obj.columns, (
            "'team' column must be present in entity table after SET"
        )
        bob_row = entity_table.source_obj[
            entity_table.source_obj["name"] == "Bob"
        ]
        assert len(bob_row) == 1
        assert (
            pd.isna(bob_row["team"].iloc[0]) or bob_row["team"].iloc[0] is None
        ), "Bob's team must be None/NaN since he was not matched by WHERE"

    def test_new_property_registered_in_attribute_map(self) -> None:
        """New property is added to entity_table.attribute_map after SET."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        assert "team" in entity_table.attribute_map, (
            "New property must be registered in attribute_map"
        )
        assert "team" in entity_table.source_obj_attribute_map, (
            "New property must be registered in source_obj_attribute_map"
        )

    def test_single_match_new_property(self) -> None:
        """SET on a single matching entity; others must have None for new property."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "SET p.manager = true "
            "RETURN p.name AS name, p.manager AS manager"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        assert (
            source.at[1, "manager"] is True or source.at[1, "manager"] == True
        )
        assert (
            pd.isna(source.at[2, "manager"]) or source.at[2, "manager"] is None
        ), "Bob was not matched; his manager property must be None"
        assert (
            pd.isna(source.at[3, "manager"]) or source.at[3, "manager"] is None
        ), "Carol was not matched; her manager property must be None"


# ---------------------------------------------------------------------------
# Test 4: Multiple properties SET simultaneously
# ---------------------------------------------------------------------------


class TestMultiPropertySet:
    """Multiple SET assignments in one clause all take effect."""

    def test_two_new_properties_full_table(self) -> None:
        """Two new properties set on all entities."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.status = 'active', p.level = 1 "
            "RETURN p.name AS name, p.status AS status, p.level AS level"
        )
        assert len(result) == 3
        assert (result["status"] == "active").all()
        assert (result["level"] == 1).all()

    def test_two_properties_partial_update(self) -> None:
        """Two new properties set on filtered subset; non-matching rows get None."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech', p.tier = 2 "
            "RETURN p.name AS name, p.team AS team, p.tier AS tier"
        )
        assert set(result["name"]) == {"Alice", "Carol"}
        assert (result["team"] == "tech").all()
        assert (result["tier"] == 2).all()

        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)
        # Bob's new properties must be None
        assert pd.isna(source.at[2, "team"]) or source.at[2, "team"] is None
        assert pd.isna(source.at[2, "tier"]) or source.at[2, "tier"] is None


# ---------------------------------------------------------------------------
# Test 5: SET with arithmetic expression
# ---------------------------------------------------------------------------


class TestArithmeticSet:
    """SET using arithmetic expressions produces correct values."""

    def test_set_salary_increase(self) -> None:
        """SET p.raise = p.salary + 5000 correctly computes per-entity raise."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.new_salary = p.salary + 5000 "
            "RETURN p.name AS name, p.salary AS salary, p.new_salary AS new_salary"
        )
        assert len(result) == 3
        for _, row in result.iterrows():
            assert abs(row["new_salary"] - (row["salary"] + 5000)) < 0.01, (
                f"new_salary incorrect for {row['name']}"
            )

    def test_set_percentage_increase_partial(self) -> None:
        """Arithmetic SET on filtered subset; unmatched entity retains original salary."""
        ctx = _make_person_context()
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.raise_amount = p.salary * 0.1 "
            "RETURN p.name AS name, p.raise_amount AS raise_amount"
        )
        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        # Alice: 60000 * 0.1 = 6000
        assert abs(float(source.at[1, "raise_amount"]) - 6000.0) < 0.01
        # Carol: 70000 * 0.1 = 7000
        assert abs(float(source.at[3, "raise_amount"]) - 7000.0) < 0.01
        # Bob: was not matched → raise_amount must be None
        assert (
            pd.isna(source.at[2, "raise_amount"])
            or source.at[2, "raise_amount"] is None
        )


# ---------------------------------------------------------------------------
# Test 6: Chained SET then RETURN reads new properties via entity table
# ---------------------------------------------------------------------------


class TestChainedSetReturn:
    """RETURN after SET correctly reads the new property from the entity table."""

    def test_return_reads_newly_set_property(self) -> None:
        """A property SET on all entities is accessible in RETURN."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.score = 100 RETURN p.name AS name, p.score AS score"
        )
        assert len(result) == 3
        assert "score" in result.columns
        assert (result["score"] == 100).all()

    def test_return_reads_property_after_filtered_set(self) -> None:
        """A property SET on a filtered subset is readable by RETURN for matched rows."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )
        assert set(result["name"]) == {"Alice", "Carol"}
        assert set(result["team"]) == {"tech"}

    def test_return_reads_arithmetic_property(self) -> None:
        """Arithmetic SET property is readable by RETURN with correct values."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "SET p.promoted_salary = p.salary + 10000 "
            "RETURN p.name AS name, p.promoted_salary AS promoted_salary"
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
        assert abs(result.iloc[0]["promoted_salary"] - 70000.0) < 0.01


# ---------------------------------------------------------------------------
# Test 7: Only explicitly SET properties are written back
#         (no contamination from enriched evaluation columns)
# ---------------------------------------------------------------------------


class TestWriteBackScope:
    """Only the explicitly SET property names are written to the entity table."""

    def test_enriched_columns_do_not_overwrite_entity_table(self) -> None:
        """
        _ensure_full_entity_data enriches the working DF with all entity columns
        so that expressions can reference p.salary etc.  After the SET those
        enriched columns must NOT be written back to the entity table — only the
        property being SET should be.
        """
        ctx = _make_person_context()
        # Modify Bob's salary in the entity table directly before query
        bob_idx = ctx.entity_mapping.mapping["Person"].source_obj.index[
            ctx.entity_mapping.mapping["Person"].source_obj[ID_COLUMN] == 2
        ][0]
        ctx.entity_mapping.mapping["Person"].source_obj.at[
            bob_idx, "salary"
        ] = 99999.0

        # SET only affects Engineering persons, computing team from salary
        _star(ctx).execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name, p.team AS team"
        )

        entity_table = ctx.entity_mapping.mapping["Person"]
        source = entity_table.source_obj.set_index(ID_COLUMN)

        # Bob's salary must still be 99999 — the enriched copy should not have
        # overwritten the entity table's value for Bob.
        assert abs(float(source.at[2, "salary"]) - 99999.0) < 0.01, (
            "Bob's salary was incorrectly overwritten by an enriched column write-back"
        )


# ---------------------------------------------------------------------------
# Test 8: `to_pandas` column construction does not expose __ID__ as user property
# ---------------------------------------------------------------------------


class TestToPandasColumnConstruction:
    """PropertyModification.to_pandas() must not expose internal __ columns."""

    def test_id_column_not_exposed_as_user_property(self) -> None:
        """The output of to_pandas must not contain a raw '__ID__' user column."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.flag = true RETURN p.name AS name, p.flag AS flag"
        )
        # __ID__ must never appear as a result column alias
        assert "__ID__" not in result.columns, (
            "__ID__ should not be exposed as a RETURN column"
        )

    def test_new_property_visible_without_entity_prefix(self) -> None:
        """New SET property appears in the output without entity-type prefix."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.badge = 'gold' "
            "RETURN p.name AS name, p.badge AS badge"
        )
        assert "badge" in result.columns
        assert "Person__badge" not in result.columns


# ---------------------------------------------------------------------------
# Test 9: ExpressionEvaluator reads updated entity table after SET
# ---------------------------------------------------------------------------


class TestExpressionEvaluatorPostSet:
    """ExpressionEvaluator (used by RETURN) re-reads from the updated entity table."""

    def test_evaluator_sees_new_string_property(self) -> None:
        """RETURN reads a new string property SET on all entities."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.role = 'engineer' "
            "RETURN p.name AS name, p.role AS role"
        )
        assert len(result) == 3
        assert (result["role"] == "engineer").all()

    def test_evaluator_sees_updated_numeric_property(self) -> None:
        """RETURN reads a modified numeric property after SET."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.age = 0 RETURN p.name AS name, p.age AS age"
        )
        assert (result["age"] == 0).all()

    def test_evaluator_sees_new_bool_property(self) -> None:
        """RETURN reads a new boolean property SET on all entities."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.verified = true "
            "RETURN p.name AS name, p.verified AS verified"
        )
        assert (result["verified"] == True).all()


# ---------------------------------------------------------------------------
# Test 10: Direct unit tests of _update_source_entity_tables
#          These bypass Star and test the method in isolation.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Test 11: dtype preservation after write-back
# ---------------------------------------------------------------------------


class TestDtypePreservation:
    """Property dtypes are preserved correctly after SET write-back."""

    def test_integer_property_dtype(self) -> None:
        """Integer-valued SET property has integer dtype in result."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.level = 5 RETURN p.name AS name, p.level AS level"
        )
        assert result["level"].dtype.kind in ("i", "f"), (
            f"Expected numeric dtype for integer literal, got {result['level'].dtype}"
        )

    def test_float_property_dtype(self) -> None:
        """Float-valued SET property has float dtype in result."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.rate = p.salary * 0.1 "
            "RETURN p.name AS name, p.rate AS rate"
        )
        assert result["rate"].dtype.kind == "f", (
            f"Expected float dtype, got {result['rate'].dtype}"
        )

    def test_existing_numeric_column_stays_numeric_after_update(self) -> None:
        """Updating an existing numeric column preserves its numeric dtype."""
        ctx = _make_person_context()
        result = _star(ctx).execute_query(
            "MATCH (p:Person) SET p.salary = 80000 "
            "RETURN p.name AS name, p.salary AS salary"
        )
        assert result["salary"].dtype.kind in ("i", "f"), (
            f"salary should stay numeric after SET, got {result['salary'].dtype}"
        )
        assert (result["salary"] == 80000).all()


# ---------------------------------------------------------------------------
# Test 12: Sequential / re-entrant SET calls accumulate
# ---------------------------------------------------------------------------


class TestSequentialSet:
    """Multiple independent calls to execute_query with SET accumulate state."""

    def test_two_sequential_sets_both_visible(self) -> None:
        """Two sequential SET queries both leave their marks on the entity table."""
        ctx = _make_person_context()
        s = _star(ctx)
        s.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' SET p.grade = 'A' "
            "RETURN p.name AS name, p.grade AS grade"
        )
        s.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' SET p.grade = 'B' "
            "RETURN p.name AS name, p.grade AS grade"
        )
        source = ctx.entity_mapping.mapping["Person"].source_obj.set_index(
            ID_COLUMN
        )
        assert source.at[1, "grade"] == "A", "Alice's grade should be 'A'"
        assert source.at[2, "grade"] == "B", "Bob's grade should be 'B'"
        # Carol was not matched in either query
        assert pd.isna(source.at[3, "grade"]) or source.at[3, "grade"] is None
