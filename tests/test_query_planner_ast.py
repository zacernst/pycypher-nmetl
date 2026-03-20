"""TDD tests for AST-aware QueryPlanner analysis — Phase 2.1.

Validates that ``QueryPlanAnalyzer`` can walk a Cypher AST and Context to:
1. Extract table statistics (entity/relationship row counts)
2. Estimate cardinality through the clause pipeline
3. Estimate peak memory requirements
4. Identify filter-pushdown opportunities
5. Select join strategies based on actual table sizes
6. Produce a human-readable plan summary

These tests build on the existing ``QueryPlanner`` (join/agg strategy
selection) by adding AST-level awareness.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Comparison,
    IntegerLiteral,
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
)
from pycypher.query_planner import JoinStrategy, QueryPlanAnalyzer
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_context(
    n_persons: int = 4,
    n_rels: int = 3,
) -> Context:
    """Build a Context with configurable table sizes."""
    persons = pd.DataFrame(
        {
            ID_COLUMN: range(n_persons),
            "name": [f"person_{i}" for i in range(n_persons)],
            "age": [20 + i % 60 for i in range(n_persons)],
            "dept": [
                ["eng", "mktg", "sales"][i % 3] for i in range(n_persons)
            ],
        },
    )
    knows = pd.DataFrame(
        {
            ID_COLUMN: range(n_rels),
            "__SOURCE__": [i % max(n_persons, 1) for i in range(n_rels)],
            "__TARGET__": [(i + 1) % max(n_persons, 1) for i in range(n_rels)],
        },
    )
    person_table = EntityTable.from_dataframe("Person", persons)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


@pytest.fixture()
def small_ctx() -> Context:
    return _make_context(n_persons=4, n_rels=3)


@pytest.fixture()
def large_ctx() -> Context:
    return _make_context(n_persons=100_000, n_rels=500_000)


# ---------------------------------------------------------------------------
# AST helper builders
# ---------------------------------------------------------------------------


def _simple_scan_query() -> Query:
    """MATCH (p:Person) RETURN p.name AS name"""
    return Query(
        clauses=[
            Match(
                pattern=Pattern(
                    paths=[
                        PatternPath(
                            elements=[
                                NodePattern(
                                    variable=Variable(name="p"),
                                    labels=["Person"],
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            Return(
                items=[
                    ReturnItem(
                        expression=PropertyLookup(
                            expression=Variable(name="p"),
                            property="name",
                        ),
                        alias="name",
                    ),
                ],
            ),
        ],
    )


def _filtered_scan_query() -> Query:
    """MATCH (p:Person) WHERE p.age > 27 RETURN p.name AS name"""
    return Query(
        clauses=[
            Match(
                pattern=Pattern(
                    paths=[
                        PatternPath(
                            elements=[
                                NodePattern(
                                    variable=Variable(name="p"),
                                    labels=["Person"],
                                ),
                            ],
                        ),
                    ],
                ),
                where=Comparison(
                    operator=">",
                    left=PropertyLookup(
                        expression=Variable(name="p"),
                        property="age",
                    ),
                    right=IntegerLiteral(value=27),
                ),
            ),
            Return(
                items=[
                    ReturnItem(
                        expression=PropertyLookup(
                            expression=Variable(name="p"),
                            property="name",
                        ),
                        alias="name",
                    ),
                ],
            ),
        ],
    )


def _relationship_query() -> Query:
    """MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"""
    return Query(
        clauses=[
            Match(
                pattern=Pattern(
                    paths=[
                        PatternPath(
                            elements=[
                                NodePattern(
                                    variable=Variable(name="a"),
                                    labels=["Person"],
                                ),
                                RelationshipPattern(
                                    labels=["KNOWS"],
                                    direction=RelationshipDirection.RIGHT,
                                ),
                                NodePattern(
                                    variable=Variable(name="b"),
                                    labels=["Person"],
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            Return(
                items=[
                    ReturnItem(
                        expression=PropertyLookup(
                            expression=Variable(name="a"),
                            property="name",
                        ),
                        alias="src",
                    ),
                    ReturnItem(
                        expression=PropertyLookup(
                            expression=Variable(name="b"),
                            property="name",
                        ),
                        alias="tgt",
                    ),
                ],
            ),
        ],
    )


def _filtered_relationship_query() -> Query:
    """MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN b.name AS friend"""
    return Query(
        clauses=[
            Match(
                pattern=Pattern(
                    paths=[
                        PatternPath(
                            elements=[
                                NodePattern(
                                    variable=Variable(name="a"),
                                    labels=["Person"],
                                ),
                                RelationshipPattern(
                                    labels=["KNOWS"],
                                    direction=RelationshipDirection.RIGHT,
                                ),
                                NodePattern(
                                    variable=Variable(name="b"),
                                    labels=["Person"],
                                ),
                            ],
                        ),
                    ],
                ),
                where=Comparison(
                    operator="=",
                    left=PropertyLookup(
                        expression=Variable(name="a"),
                        property="name",
                    ),
                    right=IntegerLiteral(value=0),  # stand-in
                ),
            ),
            Return(
                items=[
                    ReturnItem(
                        expression=PropertyLookup(
                            expression=Variable(name="b"),
                            property="name",
                        ),
                        alias="friend",
                    ),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Construction and basic analysis
# ---------------------------------------------------------------------------


class TestAnalyzerConstruction:
    """Verify QueryPlanAnalyzer initialises correctly."""

    def test_create(self, small_ctx: Context) -> None:
        """Can construct analyzer with AST and Context."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert analyzer.query is not None
        assert analyzer.context is small_ctx

    def test_analyze_returns_result(self, small_ctx: Context) -> None:
        """analyze() returns an AnalysisResult."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        result = analyzer.analyze()
        assert result is not None


# ---------------------------------------------------------------------------
# Table statistics extraction
# ---------------------------------------------------------------------------


class TestTableStatistics:
    """Verify extraction of table sizes from Context."""

    def test_entity_row_count(self, small_ctx: Context) -> None:
        """Reports correct entity row count."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert analyzer.entity_row_count("Person") == 4

    def test_relationship_row_count(self, small_ctx: Context) -> None:
        """Reports correct relationship row count."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert analyzer.relationship_row_count("KNOWS") == 3

    def test_unknown_entity(self, small_ctx: Context) -> None:
        """Unknown entity type returns 0."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert analyzer.entity_row_count("Company") == 0

    def test_unknown_relationship(self, small_ctx: Context) -> None:
        """Unknown relationship type returns 0."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert analyzer.relationship_row_count("WORKS_AT") == 0

    def test_large_context(self, large_ctx: Context) -> None:
        """Large context reports correct counts."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), large_ctx)
        assert analyzer.entity_row_count("Person") == 100_000
        assert analyzer.relationship_row_count("KNOWS") == 500_000


# ---------------------------------------------------------------------------
# Cardinality estimation
# ---------------------------------------------------------------------------


class TestCardinalityEstimation:
    """Verify cardinality estimation through the clause pipeline."""

    def test_simple_scan(self, small_ctx: Context) -> None:
        """MATCH (p:Person) estimates entity count rows."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        # First clause (MATCH) should estimate 4 rows
        assert result.clause_cardinalities[0] == 4

    def test_filtered_scan_reduces(self, small_ctx: Context) -> None:
        """WHERE clause reduces cardinality estimate."""
        result = QueryPlanAnalyzer(_filtered_scan_query(), small_ctx).analyze()
        # Filtered should estimate <= entity count
        assert result.clause_cardinalities[0] <= 4

    def test_relationship_cardinality(self, small_ctx: Context) -> None:
        """Relationship pattern estimates from relationship count."""
        result = QueryPlanAnalyzer(_relationship_query(), small_ctx).analyze()
        assert result.clause_cardinalities[0] > 0

    def test_large_scan(self, large_ctx: Context) -> None:
        """Large entity table produces large cardinality."""
        result = QueryPlanAnalyzer(_simple_scan_query(), large_ctx).analyze()
        assert result.clause_cardinalities[0] == 100_000

    def test_return_preserves_cardinality(self, small_ctx: Context) -> None:
        """RETURN clause doesn't change cardinality (without LIMIT)."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        # MATCH produces 4, RETURN passes through
        assert len(result.clause_cardinalities) == 2
        assert result.clause_cardinalities[1] == result.clause_cardinalities[0]


# ---------------------------------------------------------------------------
# Memory estimation
# ---------------------------------------------------------------------------


class TestMemoryEstimation:
    """Verify memory estimation."""

    def test_positive_estimate(self, small_ctx: Context) -> None:
        """Any query produces positive memory estimate."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        assert result.estimated_peak_bytes > 0

    def test_relationship_higher_than_scan(self, small_ctx: Context) -> None:
        """Relationship query estimates >= simple scan."""
        scan_result = QueryPlanAnalyzer(
            _simple_scan_query(), small_ctx
        ).analyze()
        rel_result = QueryPlanAnalyzer(
            _relationship_query(), small_ctx
        ).analyze()
        assert (
            rel_result.estimated_peak_bytes >= scan_result.estimated_peak_bytes
        )

    def test_large_context_higher(self, large_ctx: Context) -> None:
        """Large tables produce large memory estimates."""
        result = QueryPlanAnalyzer(_simple_scan_query(), large_ctx).analyze()
        assert result.estimated_peak_bytes > 100_000

    def test_budget_check(self, small_ctx: Context) -> None:
        """Can check if query exceeds a given memory budget."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        assert result.exceeds_budget(budget_bytes=1_000_000_000) is False
        assert result.exceeds_budget(budget_bytes=1) is True


# ---------------------------------------------------------------------------
# Join strategy selection (AST-aware)
# ---------------------------------------------------------------------------


class TestJoinStrategyFromAST:
    """Verify join strategy selection from AST analysis."""

    def test_relationship_query_produces_join_plan(
        self, small_ctx: Context
    ) -> None:
        """Relationship pattern triggers join plan."""
        result = QueryPlanAnalyzer(_relationship_query(), small_ctx).analyze()
        assert len(result.join_plans) > 0

    def test_simple_scan_no_join(self, small_ctx: Context) -> None:
        """Simple scan produces no join plans."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        assert len(result.join_plans) == 0

    def test_small_tables_strategy(self, small_ctx: Context) -> None:
        """Small tables get broadcast or hash join."""
        result = QueryPlanAnalyzer(_relationship_query(), small_ctx).analyze()
        for jp in result.join_plans:
            assert jp.strategy in (JoinStrategy.BROADCAST, JoinStrategy.HASH)

    def test_large_tables_strategy(self, large_ctx: Context) -> None:
        """Large tables produce a valid strategy."""
        result = QueryPlanAnalyzer(_relationship_query(), large_ctx).analyze()
        for jp in result.join_plans:
            assert isinstance(jp.strategy, JoinStrategy)


# ---------------------------------------------------------------------------
# Filter pushdown detection
# ---------------------------------------------------------------------------


class TestFilterPushdownDetection:
    """Verify identification of filter-pushdown opportunities."""

    def test_no_pushdown_for_simple_scan(self, small_ctx: Context) -> None:
        """Simple scan has no pushdown opportunity."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        assert result.has_pushdown_opportunities is False

    def test_pushdown_for_filtered_relationship(
        self, small_ctx: Context
    ) -> None:
        """WHERE on source node in relationship pattern is a pushdown opportunity."""
        result = QueryPlanAnalyzer(
            _filtered_relationship_query(),
            small_ctx,
        ).analyze()
        assert result.has_pushdown_opportunities is True

    def test_pushdown_variables(self, small_ctx: Context) -> None:
        """Pushdown report identifies which variables can be filtered early."""
        result = QueryPlanAnalyzer(
            _filtered_relationship_query(),
            small_ctx,
        ).analyze()
        # WHERE a.name = ... can push filter on 'a' before joining with KNOWS
        pushdown_vars = {p.variable for p in result.pushdown_opportunities}
        assert "a" in pushdown_vars


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------


class TestPlanSummary:
    """Verify human-readable plan summary."""

    def test_summary_is_string(self, small_ctx: Context) -> None:
        """summary() returns a non-empty string."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        s = result.summary()
        assert isinstance(s, str)
        assert len(s) > 0

    def test_summary_includes_cardinality(self, small_ctx: Context) -> None:
        """Summary mentions cardinality estimates."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        s = result.summary()
        assert "4" in s  # 4 persons

    def test_summary_includes_memory(self, small_ctx: Context) -> None:
        """Summary mentions memory estimate."""
        result = QueryPlanAnalyzer(_simple_scan_query(), small_ctx).analyze()
        s = result.summary()
        assert "memory" in s.lower() or "bytes" in s.lower()
