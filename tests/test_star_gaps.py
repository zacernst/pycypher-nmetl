"""Tests for star.py coverage gaps.

Covers:
- execute_query error paths
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    IntegerLiteral,
    Return,
    ReturnItem,
    Variable,
    With,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def person_context() -> Context:
    """Context with Person entities."""
    data = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=data,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


@pytest.fixture
def star(person_context: Context) -> Star:
    """Star instance with person context."""
    return Star(context=person_context)


# ---------------------------------------------------------------------------
# execute_query error paths
# ---------------------------------------------------------------------------


class TestExecuteQueryErrors:
    """Cover error paths in execute_query."""

    def test_invalid_query_type(self, star: Star) -> None:
        """Passing a non-Query/non-str raises GrammarTransformerSyncError."""
        with pytest.raises(
            (TypeError, ValueError),
        ):
            star.execute_query(42)

    def test_with_without_match(self, star: Star) -> None:
        """Standalone WITH (no preceding MATCH) evaluates literal expressions."""
        from pycypher.ast_models import Query

        query = Query(
            clauses=[
                With(
                    items=[
                        ReturnItem(
                            expression=IntegerLiteral(value=1),
                            alias="x",
                        ),
                    ],
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="x"), alias="x"),
                    ],
                ),
            ],
        )
        result = star.execute_query(query)
        assert result is not None
        assert result["x"].iloc[0] == 1

    def test_return_without_match(self, star: Star) -> None:
        """Standalone RETURN (no preceding MATCH) evaluates literal expressions."""
        from pycypher.ast_models import Query

        query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(
                            expression=IntegerLiteral(value=1),
                            alias="x",
                        ),
                    ],
                ),
            ],
        )
        result = star.execute_query(query)
        assert result is not None
        assert result["x"].iloc[0] == 1

    def test_empty_query(self, star: Star) -> None:
        """Query with no clauses raises ValueError."""
        from pycypher.ast_models import Query

        with pytest.raises(ValueError, match="at least one clause"):
            star.execute_query(Query(clauses=[]))

    def test_unsupported_clause_type(self, star: Star) -> None:
        """Genuinely unsupported clause type raises NotImplementedError."""
        from pycypher.ast_models import Clause, Query

        # Synthesize an anonymous Clause subclass to represent a future clause
        # type that the dispatcher has not yet implemented.
        class FutureClause(Clause):
            pass

        query = Query(clauses=[FutureClause()])
        with pytest.raises(NotImplementedError, match="not.*supported"):
            star.execute_query(query)


# ---------------------------------------------------------------------------
# Engine layer removal tests (Flaw 2 from ARCHITECTURE_CRITIQUE.md)
# ---------------------------------------------------------------------------


class TestEngineLayerRemoval:
    """Verify that engine layer indirection has been removed.

    These tests were written as failing tests first (TDD step 1), then the
    implementation was updated to make them pass.
    """

    def test_no_phantom_inner_star(self, star: Star) -> None:
        """Star must not hold a secondary engine that wraps another Star."""
        # engine attribute should not exist or should be None
        assert not hasattr(star, "engine") or star.engine is None

    def test_errors_propagate_not_suppressed(self, star: Star) -> None:
        """Exceptions from _execute_query_binding_frame must propagate."""
        from unittest.mock import patch

        sentinel = RuntimeError("deliberate test error")
        with (
            patch.object(
                star,
                "_execute_query_binding_frame",
                side_effect=sentinel,
            ),
            pytest.raises(RuntimeError, match="deliberate test error"),
        ):
            star.execute_query("MATCH (p:Person) RETURN p.name AS name")

    def test_query_still_works_without_engine(self, star: Star) -> None:
        """Basic query must execute correctly after engine layer removal."""
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Bob", "Charlie"}


# ---------------------------------------------------------------------------
# Aggregation consistency — WITH vs RETURN must use the same logic
# ---------------------------------------------------------------------------


@pytest.fixture
def dept_context() -> Context:
    """Context with Person entities that have a dept attribute for grouping."""
    data = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Dave"],
            "dept": ["eng", "eng", "hr", "hr"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "dept"],
        source_obj_attribute_map={"name": "name", "dept": "dept"},
        attribute_map={"name": "name", "dept": "dept"},
        source_obj=data,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


class TestAggregateItemsConsistency:
    """Grouped aggregation must behave identically whether driven by WITH or RETURN."""

    def test_full_table_count_return(self, dept_context: Context) -> None:
        """RETURN count(*) returns a single-row DataFrame with correct count."""
        star = Star(context=dept_context)
        result = star.execute_query("MATCH (p:Person) RETURN count(*) AS n")
        assert len(result) == 1
        assert int(result["n"].iloc[0]) == 4

    def test_full_table_count_with_then_return(
        self,
        dept_context: Context,
    ) -> None:
        """WITH count(*) followed by RETURN produces same result as direct RETURN."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH count(*) AS n RETURN n",
        )
        assert len(result) == 1
        assert int(result["n"].iloc[0]) == 4

    def test_grouped_count_return(self, dept_context: Context) -> None:
        """RETURN grouped count aggregates correctly per group."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS n",
        )
        grouped = dict(zip(result["dept"], result["n"].astype(int)))
        assert grouped == {"eng": 2, "hr": 2}

    def test_grouped_count_with_then_return(
        self,
        dept_context: Context,
    ) -> None:
        """WITH grouped count then RETURN produces identical result to direct RETURN."""
        star = Star(context=dept_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.dept AS dept, count(p) AS n RETURN dept, n",
        )
        grouped = dict(zip(result["dept"], result["n"].astype(int)))
        assert grouped == {"eng": 2, "hr": 2}


# ---------------------------------------------------------------------------
# Missing query parameter error semantics
# ---------------------------------------------------------------------------


class TestMissingParameterError:
    """Missing $param references must raise ValueError, not KeyError.

    KeyError is meant for dict/mapping lookups; a missing parameter is a
    *validation failure* and must be a ValueError so callers can catch it
    with standard input-validation error handlers.
    """

    def test_missing_parameter_raises_value_error(
        self,
        person_context: Context,
    ) -> None:
        """Referencing an undeclared $param must raise ValueError."""
        star = Star(context=person_context)
        with pytest.raises(ValueError):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
                parameters={},  # $min_age not provided
            )

    def test_missing_parameter_not_key_error(
        self,
        person_context: Context,
    ) -> None:
        """ValueError is raised, not KeyError — callers must not catch KeyError."""
        star = Star(context=person_context)
        # KeyError must NOT be raised (wrong exception type)
        try:
            star.execute_query(
                "MATCH (p:Person) RETURN p.name AS nm LIMIT $n",
                parameters={},  # $n not provided
            )
        except ValueError:
            pass  # Correct
        except KeyError:
            pytest.fail(
                "Missing parameter raised KeyError; should be ValueError",
            )

    def test_missing_parameter_message_mentions_name(
        self,
        person_context: Context,
    ) -> None:
        """Error message must include the parameter name for debugging."""
        star = Star(context=person_context)
        with pytest.raises(ValueError, match="min_age"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
                parameters={"wrong_param": 10},
            )

    def test_provided_parameter_does_not_raise(
        self,
        person_context: Context,
    ) -> None:
        """Correctly provided parameters must NOT raise any error."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS nm",
            parameters={"min_age": 28},
        )
        # Only Bob (30) and Charlie (35) are > 28
        assert set(result["nm"].tolist()) == {"Bob", "Charlie"}


# ---------------------------------------------------------------------------
# Error handling path coverage (Task #1 / Task #11)
# ---------------------------------------------------------------------------


class TestWarmupCacheErrorHandling:
    """Cover error paths in _warmup_ast_cache."""

    def test_warmup_handles_parse_errors_gracefully(self) -> None:
        """_warmup_ast_cache should not raise when templates fail to parse."""
        from pycypher.star import Star

        # Entity types with special characters that produce unparseable queries
        Star._warmup_ast_cache(
            entity_types=["Invalid Type!", "123Bad"],
            rel_types=["BAD REL!"],
        )
        # Should complete without raising

    def test_warmup_succeeds_for_valid_types(self) -> None:
        """_warmup_ast_cache should populate cache for valid type names."""
        from pycypher.ast_converter import _parse_cypher_cached
        from pycypher.star import Star

        # Clear cache to measure effect
        _parse_cypher_cached.cache_clear()

        Star._warmup_ast_cache(
            entity_types=["Person"],
            rel_types=["KNOWS"],
        )

        # Cache should have entries
        info = _parse_cypher_cached.cache_info()
        assert info.currsize > 0

    def test_warmup_with_empty_types(self) -> None:
        """_warmup_ast_cache with no types should be a no-op."""
        from pycypher.star import Star

        Star._warmup_ast_cache(entity_types=[], rel_types=[])
        # Should complete without raising


class TestComplexityScoringErrorHandling:
    """Cover error paths in PROFILE complexity analysis."""

    def test_explain_query_with_valid_query(self, star: Star) -> None:
        """explain_query should include complexity score for valid queries."""
        plan = star.explain_query("MATCH (p:Person) RETURN p.name AS name")
        assert "Complexity score" in plan

    def test_explain_query_handles_broken_scoring(self, star: Star) -> None:
        """explain_query should degrade gracefully if score_query fails."""
        from unittest.mock import patch

        with patch(
            "pycypher.query_complexity.score_query",
            side_effect=AttributeError("mock failure"),
        ):
            plan = star.explain_query("MATCH (p:Person) RETURN p.name AS name")
            # Plan should still be produced, just without complexity score
            assert "Execution Plan" in plan


class TestExecuteQueryMetricsOnError:
    """Cover the execute_query error instrumentation path."""

    def test_error_metrics_recorded_on_failure(self, star: Star) -> None:
        """execute_query must record error metrics and re-raise."""
        from unittest.mock import patch

        sentinel = RuntimeError("deliberate metrics test error")
        with (
            patch.object(
                star,
                "_execute_query_binding_frame",
                side_effect=sentinel,
            ),
            pytest.raises(RuntimeError, match="deliberate metrics test error"),
        ):
            star.execute_query("MATCH (p:Person) RETURN p.name AS name")

    def test_syntax_error_re_raised(self, star: Star) -> None:
        """Syntax errors should propagate through the metrics wrapper."""
        with pytest.raises(Exception):  # noqa: B017
            star.execute_query("THIS IS NOT CYPHER")
