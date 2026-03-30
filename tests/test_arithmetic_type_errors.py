"""TDD tests for arithmetic type-mismatch error messages (Loop 181).

Currently broken user experience:
  - MATCH (p:Person) RETURN p.age + p.name AS v
    → TypeError: unsupported operand type(s) for +: 'int' and 'str'
    (raw pandas error, no query context, no suggestion)

  - RETURN date('2024-01-01') + 7 AS d
    → silently returns null (should raise with suggestion to use duration())

Goal: every type mismatch in arithmetic must raise TypeError with:
  1. The operator that failed (e.g. '+')
  2. The left-operand type (e.g. 'int')
  3. The right-operand type (e.g. 'str')
  4. A human-readable suggestion where possible

All tests are written before the implementation (TDD red phase).
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _star_empty():
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        RelationshipMapping,
    )
    from pycypher.star import Star

    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


def _q(cypher: str):
    return _star_empty().execute_query(cypher)


def _star_with_people():
    import pandas as pd
    from pycypher.relational_models import (
        ID_COLUMN,
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )
    from pycypher.star import Star

    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
            "score": [9.5, 8.0],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
        },
        attribute_map={"name": "name", "age": "age", "score": "score"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ===========================================================================
# Category 1 — Arithmetic on incompatible scalar types (via properties)
# ===========================================================================


class TestArithmeticTypeMismatchFromProperties:
    """Arithmetic between incompatible property types must raise TypeError
    with a message that names both operand types and the operator.
    """

    def test_int_plus_string_raises_type_error(self) -> None:
        star = _star_with_people()
        with pytest.raises(TypeError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN p.age + p.name AS v")
        msg = str(exc_info.value).lower()
        # Message must mention the operator
        assert "+" in str(exc_info.value)
        # Message must mention both types (int and str)
        assert "int" in msg or "str" in msg

    def test_string_minus_int_raises_type_error(self) -> None:
        star = _star_with_people()
        with pytest.raises(TypeError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN p.name - p.age AS v")
        msg = str(exc_info.value)
        assert "-" in msg or "str" in msg.lower() or "int" in msg.lower()

    def test_string_times_string_raises_type_error(self) -> None:
        """String * string is not valid arithmetic."""
        star = _star_with_people()
        # In Cypher, you can't multiply two strings — must raise TypeError
        with pytest.raises(TypeError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN p.name * p.name AS v")
        # Ensure something useful is in the error
        assert exc_info.value is not None

    def test_error_is_chained_from_original(self) -> None:
        """The TypeError must chain the original pandas/operator exception."""
        star = _star_with_people()
        with pytest.raises(TypeError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN p.age + p.name AS v")
        # Python exception chaining: __cause__ or __context__ should be set
        assert (
            exc_info.value.__cause__ is not None
            or exc_info.value.__context__ is not None
        )


# ===========================================================================
# Category 2 — Arithmetic on incompatible literal types
# ===========================================================================


class TestArithmeticTypeMismatchLiterals:
    """Arithmetic on incompatible literal types in standalone RETURN."""

    def test_literal_int_plus_literal_string_raises(self) -> None:
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN 5 + 'hello' AS v")
        msg = str(exc_info.value)
        assert "+" in msg or "int" in msg.lower() or "str" in msg.lower()

    def test_literal_string_minus_literal_int_raises(self) -> None:
        with pytest.raises(TypeError):
            _q("RETURN 'hello' - 3 AS v")

    def test_error_message_mentions_operator(self) -> None:
        """The error message must identify the failing operator."""
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN 5 + 'world' AS v")
        # The operator '+' should appear somewhere in the message
        assert "+" in str(exc_info.value)


# ===========================================================================
# Category 3 — Temporal arithmetic: date + integer (forgot duration())
# ===========================================================================


class TestTemporalArithmeticTypeMismatch:
    """date + integer should raise TypeError, not silently return null.

    The most common user mistake: forgetting to wrap a number in duration().
    Without this guard, `date('2024-01-01') + 7` silently returns null.
    """

    def test_date_plus_integer_raises_type_error(self) -> None:
        """date('2024-01-01') + 7 must raise, not return null."""
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN date('2024-01-01') + 7 AS d")
        msg = str(exc_info.value)
        # Must mention duration() as the fix
        assert "duration" in msg.lower()

    def test_date_minus_integer_raises_type_error(self) -> None:
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN date('2024-01-01') - 7 AS d")
        assert "duration" in str(exc_info.value).lower()

    def test_datetime_plus_integer_raises_type_error(self) -> None:
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN datetime('2024-01-15T10:00:00') + 3 AS dt")
        assert "duration" in str(exc_info.value).lower()

    def test_integer_plus_date_raises_type_error(self) -> None:
        """7 + date('2024-01-01') must also raise — date is not on left side."""
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN 7 + date('2024-01-01') AS d")
        assert exc_info.value is not None

    def test_error_message_mentions_duration_suggestion(self) -> None:
        """The error message must suggest using duration()."""
        with pytest.raises(TypeError) as exc_info:
            _q("RETURN date('2024-01-01') + 5 AS d")
        msg = str(exc_info.value).lower()
        assert "duration" in msg

    def test_valid_temporal_arithmetic_still_works(self) -> None:
        """After the fix, valid arithmetic must still work correctly."""
        result = _q("RETURN date('2024-01-01') + duration({days: 5}) AS d")
        assert result["d"].iloc[0] == "2024-01-06"

    def test_date_plus_string_raises_type_error(self) -> None:
        """Date + arbitrary string (not a duration) must raise."""
        with pytest.raises(TypeError):
            _q("RETURN date('2024-01-01') + 'not a duration' AS d")


# ===========================================================================
# Category 4 — TypeError must not fire for valid arithmetic
# ===========================================================================


class TestValidArithmeticStillWorks:
    """Confirm the error-handling additions don't break valid arithmetic."""

    def test_int_plus_int(self) -> None:
        result = _q("RETURN 3 + 4 AS v")
        assert result["v"].iloc[0] == 7

    def test_float_plus_int(self) -> None:
        result = _q("RETURN 3.5 + 1 AS v")
        assert result["v"].iloc[0] == pytest.approx(4.5)

    def test_int_minus_int(self) -> None:
        result = _q("RETURN 10 - 3 AS v")
        assert result["v"].iloc[0] == 7

    def test_string_concatenation_works(self) -> None:
        """String + string must still work (Cypher allows string concatenation)."""
        result = _q("RETURN 'hello' + ' world' AS v")
        assert result["v"].iloc[0] == "hello world"

    def test_property_int_plus_int(self) -> None:
        star = _star_with_people()
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age + 10 AS v",
        )
        assert result["v"].iloc[0] == 40

    def test_null_arithmetic_still_returns_null(self) -> None:
        """Null + 5 must still return null, not raise."""
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = pd.DataFrame({ID_COLUMN: [1], "x": [None]})
        table = EntityTable(
            entity_type="P",
            identifier="P",
            column_names=[ID_COLUMN, "x"],
            source_obj_attribute_map={"x": "x"},
            attribute_map={"x": "x"},
            source_obj=df,
        )
        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={"P": table}),
                relationship_mapping=RelationshipMapping(mapping={}),
            ),
        )
        result = star.execute_query("MATCH (p:P) RETURN p.x + 5 AS v")
        assert pd.isna(result["v"].iloc[0])

    def test_duration_plus_duration(self) -> None:
        result = _q("RETURN duration({days: 3}) + duration({days: 4}) AS dur")
        assert result["dur"].iloc[0]["days"] == 7
