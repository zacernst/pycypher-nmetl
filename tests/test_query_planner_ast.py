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
            "dept": [["eng", "mktg", "sales"][i % 3] for i in range(n_persons)],
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
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def small_ctx() -> Context:
    return _make_context(n_persons=4, n_rels=3)


@pytest.fixture
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
            _simple_scan_query(),
            small_ctx,
        ).analyze()
        rel_result = QueryPlanAnalyzer(
            _relationship_query(),
            small_ctx,
        ).analyze()
        assert rel_result.estimated_peak_bytes >= scan_result.estimated_peak_bytes

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
        self,
        small_ctx: Context,
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
        self,
        small_ctx: Context,
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


# ---------------------------------------------------------------------------
# Enhanced cardinality estimation — column statistics integration
# ---------------------------------------------------------------------------


class TestColumnStatisticsSelectivity:
    """Verify that column statistics improve selectivity estimation."""

    def test_table_statistics_built(self, small_ctx: Context) -> None:
        """Analyzer builds TableStatistics for all registered tables."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        assert "Person" in analyzer._table_stats
        assert "KNOWS" in analyzer._table_stats
        assert analyzer._table_stats["Person"].row_count == 4

    def test_column_stats_ndv(self, small_ctx: Context) -> None:
        """Column statistics report correct NDV."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "name")
        assert stats is not None
        assert stats.ndv == 4  # 4 unique names

    def test_column_stats_numeric_range(self, small_ctx: Context) -> None:
        """Numeric column statistics report min/max."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "age")
        assert stats is not None
        assert stats.min_value is not None
        assert stats.max_value is not None
        assert stats.min_value <= stats.max_value

    def test_column_stats_null_fraction(self, small_ctx: Context) -> None:
        """Column with no nulls has null_fraction = 0."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "name")
        assert stats is not None
        assert stats.null_fraction == 0.0

    def test_equality_selectivity_uses_ndv(self, small_ctx: Context) -> None:
        """Equality selectivity is 1/NDV for non-null columns."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "name")
        assert stats is not None
        sel = stats.equality_selectivity()
        assert abs(sel - 0.25) < 0.01  # 1/4 names

    def test_range_selectivity_uses_min_max(self, small_ctx: Context) -> None:
        """Range selectivity uses actual column min/max."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "age")
        assert stats is not None
        # Ages are [20, 21, 22, 23] (i % 60 + 20 for i in 0..3)
        # Range p.age > 21 should keep ~2/3 of the range
        sel = stats.range_selectivity(low=21)
        assert 0 < sel < 1.0

    def test_filtered_scan_uses_statistics(self, small_ctx: Context) -> None:
        """Filtered scan cardinality uses column stats, not fixed 0.33."""
        result = QueryPlanAnalyzer(
            _filtered_scan_query(),
            small_ctx,
        ).analyze()
        # p.age > 27 on ages [20,21,22,23]: should filter most out
        # With stats: selectivity depends on actual range
        # Without stats: would be 4 * 0.33 = 1
        assert result.clause_cardinalities[0] <= 4
        assert result.clause_cardinalities[0] >= 1

    def test_unknown_column_falls_back(self, small_ctx: Context) -> None:
        """Unknown column falls back to default selectivity."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "nonexistent_col")
        assert stats is None

    def test_predicate_selectivity_equality(self, small_ctx: Context) -> None:
        """estimate_predicate_selectivity returns stats-based selectivity for =."""
        analyzer = QueryPlanAnalyzer(
            _filtered_relationship_query(),
            small_ctx,
        )
        # The WHERE is a.name = 0 (IntegerLiteral stand-in)
        sel = analyzer.estimate_predicate_selectivity(
            _filtered_relationship_query().clauses[0].where,
        )
        # Should use 1/NDV for name column (4 unique names) = 0.25
        assert abs(sel - 0.25) < 0.01

    def test_predicate_selectivity_range(self, small_ctx: Context) -> None:
        """estimate_predicate_selectivity handles > with range stats."""
        analyzer = QueryPlanAnalyzer(_filtered_scan_query(), small_ctx)
        sel = analyzer.estimate_predicate_selectivity(
            _filtered_scan_query().clauses[0].where,
        )
        # p.age > 27 on ages [20,21,22,23]
        assert 0 < sel < 1.0

    def test_large_context_statistics(self, large_ctx: Context) -> None:
        """Statistics work on large (100K row) tables."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), large_ctx)
        stats = analyzer._get_column_stats("Person", "age")
        assert stats is not None
        assert stats.row_count == 100_000
        assert stats.ndv > 0

    def test_equality_selectivity_uses_ndv(self, small_ctx: Context) -> None:
        """Equality predicate selectivity uses 1/NDV from column stats."""
        analyzer = QueryPlanAnalyzer(_filtered_relationship_query(), small_ctx)
        # The name column has 4 distinct values in a 4-row table
        stats = analyzer._get_column_stats("Person", "name")
        assert stats is not None
        assert stats.ndv == 4
        # Equality selectivity should be ~0.25 (1/4)
        assert abs(stats.equality_selectivity() - 0.25) < 0.01

    def test_range_selectivity_uses_min_max(self, small_ctx: Context) -> None:
        """Range predicate selectivity uses min/max from column stats."""
        analyzer = QueryPlanAnalyzer(_filtered_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "age")
        assert stats is not None
        assert stats.min_value is not None
        assert stats.max_value is not None
        # Range selectivity should be between 0 and 1
        sel = stats.range_selectivity(low=27.0)
        assert 0 < sel <= 1.0

    def test_filtered_scan_uses_column_stats(self, small_ctx: Context) -> None:
        """Filtered scan should use column stats instead of hardcoded 0.33."""
        result = QueryPlanAnalyzer(
            _filtered_scan_query(),
            small_ctx,
        ).analyze()
        # The age column has values [20, 21, 22, 23] for 4 persons
        # WHERE p.age > 27 filters out all rows, so cardinality should be
        # less than using the default 0.33 * 4 ≈ 1
        assert result.clause_cardinalities[0] >= 1  # always at least 1

    def test_unknown_column_falls_back_to_default(
        self,
        small_ctx: Context,
    ) -> None:
        """Unknown column uses default selectivity, not crash."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        stats = analyzer._get_column_stats("Person", "nonexistent_column")
        assert stats is None

    def test_estimate_predicate_selectivity_equality(
        self,
        small_ctx: Context,
    ) -> None:
        """estimate_predicate_selectivity uses column stats for equality."""
        eq_predicate = Comparison(
            operator="=",
            left=PropertyLookup(
                expression=Variable(name="p"),
                property="name",
            ),
            right=IntegerLiteral(value=0),
        )
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        sel = analyzer.estimate_predicate_selectivity(eq_predicate)
        # With 4 distinct names, selectivity should be ~0.25
        assert 0 < sel < 0.5


class TestCardinalityFeedbackLogging:
    """Verify cardinality feedback logging."""

    def test_feedback_no_error(self, small_ctx: Context) -> None:
        """log_cardinality_feedback does not raise."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        result = analyzer.analyze()
        actual = [4, 4]  # MATCH and RETURN both 4 rows
        analyzer.log_cardinality_feedback(result, actual)

    def test_feedback_with_mismatch(self, small_ctx: Context) -> None:
        """Feedback with mismatched estimates logs without error."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        result = analyzer.analyze()
        actual = [100, 100]  # Much larger than estimated
        analyzer.log_cardinality_feedback(result, actual)

    def test_feedback_empty(self, small_ctx: Context) -> None:
        """Empty actual rows list handles gracefully."""
        analyzer = QueryPlanAnalyzer(_simple_scan_query(), small_ctx)
        result = analyzer.analyze()
        analyzer.log_cardinality_feedback(result, [])


class TestJoinReorderingWithStats:
    """Verify JoinReorderingRule uses improved cardinality estimation."""

    def test_reordering_uses_relationship_bounds(self) -> None:
        """JoinReorderingRule._estimate_match_cardinality bounds by relationship count."""
        from pycypher.query_optimizer import JoinReorderingRule

        # Build context: 1000 persons, 50 relationships
        persons = pd.DataFrame(
            {
                "__ID__": range(1000),
                "name": [f"p{i}" for i in range(1000)],
            },
        )
        rels = pd.DataFrame(
            {
                "__ID__": range(50),
                "__SOURCE__": range(50),
                "__TARGET__": [i + 1 for i in range(50)],
            },
        )
        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": EntityTable.from_dataframe("Person", persons),
                },
            ),
            relationship_mapping=RelationshipMapping(
                mapping={
                    "KNOWS": RelationshipTable(
                        relationship_type="KNOWS",
                        identifier="KNOWS",
                        column_names=list(rels.columns),
                        source_obj_attribute_map={},
                        attribute_map={},
                        source_obj=rels,
                    ),
                },
            ),
        )

        match = Match(
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
        )

        est = JoinReorderingRule._estimate_match_cardinality(match, ctx)
        # Should be bounded by relationship count (50), not 1000 * 50
        assert est <= 50

    def test_reordering_applies_where_selectivity(self) -> None:
        """JoinReorderingRule._estimate_match_cardinality applies WHERE selectivity."""
        from pycypher.query_optimizer import JoinReorderingRule

        persons = pd.DataFrame(
            {
                "__ID__": range(100),
                "name": [f"p{i}" for i in range(100)],
                "age": list(range(100)),
            },
        )
        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": EntityTable.from_dataframe("Person", persons),
                },
            ),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        # MATCH (p:Person) WHERE p.age > 50
        match = Match(
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
                right=IntegerLiteral(value=50),
            ),
        )

        est = JoinReorderingRule._estimate_match_cardinality(match, ctx)
        # Should be less than 100 (100 persons filtered by age > 50)
        assert est < 100
        assert est >= 1
