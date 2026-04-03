"""Tests for QueryPlanAnalyzer integration into the execution path.

Validates that:
1. QueryPlanAnalyzer is invoked during query execution
2. It produces correct cardinality estimates for different query patterns
3. Memory budget warnings are emitted for large queries
4. Pushdown opportunities are detected for filtered relationship queries
5. The integration doesn't break existing query execution

Run with:
    uv run pytest tests/test_query_planner_integration.py -v
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
from pycypher import Star
from pycypher.query_planner import JoinStrategy, QueryPlanAnalyzer
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def social_context() -> Context:
    """Context with Person entities and KNOWS relationships."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 32],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102, 103],
            "__SOURCE__": [1, 1, 2, 3],
            "__TARGET__": [2, 3, 4, 5],
        },
    )
    person_table = EntityTable.from_dataframe("Person", person_df)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def social_star(social_context: Context) -> Star:
    """Star with social graph context."""
    return Star(context=social_context)


@pytest.fixture
def person_only_context() -> Context:
    """Context with just Person entities, no relationships."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        },
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": EntityTable.from_dataframe("Person", person_df),
            },
        ),
    )


# ---------------------------------------------------------------------------
# QueryPlanAnalyzer unit tests
# ---------------------------------------------------------------------------


class TestQueryPlanAnalyzerCardinality:
    """Cardinality estimation for different query patterns."""

    def test_simple_node_scan_cardinality(
        self,
        social_context: Context,
    ) -> None:
        """MATCH (p:Person) — cardinality should equal entity count."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        # 5 Person rows, RETURN preserves cardinality
        assert result.clause_cardinalities[0] == 5

    def test_relationship_scan_cardinality(
        self,
        social_context: Context,
    ) -> None:
        """MATCH (a)-[:KNOWS]->(b) — cardinality bounded by relationship count."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        # MATCH cardinality bounded by min(Person×KNOWS, KNOWS) = 4
        assert result.clause_cardinalities[0] == 4

    def test_filtered_scan_reduces_cardinality(
        self,
        social_context: Context,
    ) -> None:
        """WHERE clause applies selectivity factor to cardinality estimate."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        # 5 rows × 0.33 selectivity → ~1-2 rows
        assert result.clause_cardinalities[0] < 5


class TestQueryPlanAnalyzerJoinStrategy:
    """Join strategy selection for relationship queries."""

    def test_small_dataset_uses_broadcast_join(
        self,
        social_context: Context,
    ) -> None:
        """Small datasets should recommend broadcast join."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        assert len(result.join_plans) > 0
        # Both sides < 10K rows → broadcast join
        assert result.join_plans[0].strategy == JoinStrategy.BROADCAST

    def test_join_plan_has_valid_fields(self, social_context: Context) -> None:
        """Join plans should have populated fields."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        jp = result.join_plans[0]
        assert jp.left_name == "Person"
        assert jp.right_name == "KNOWS"
        assert jp.join_key == "__ID__"
        assert jp.estimated_rows >= 0
        assert jp.estimated_memory_bytes >= 0
        assert jp.notes  # non-empty explanation


class TestQueryPlanAnalyzerPushdown:
    """Pushdown opportunity detection."""

    def test_where_on_node_variable_detects_pushdown(
        self,
        social_context: Context,
    ) -> None:
        """WHERE on a node variable in a relationship pattern = pushdown opportunity."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 30 RETURN a.name AS name",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        assert result.has_pushdown_opportunities
        pushdown_vars = {p.variable for p in result.pushdown_opportunities}
        assert "a" in pushdown_vars

    def test_no_pushdown_for_simple_node_scan(
        self,
        person_only_context: Context,
    ) -> None:
        """Simple node scan with no relationship — no pushdown needed."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name",
        )
        result = QueryPlanAnalyzer(query, person_only_context).analyze()
        assert not result.has_pushdown_opportunities


class TestQueryPlanAnalyzerMemory:
    """Memory estimation and budget checking."""

    def test_small_query_within_budget(self, social_context: Context) -> None:
        """Small query should be within 2GB budget."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        assert not result.exceeds_budget(budget_bytes=2 * 1024 * 1024 * 1024)

    def test_analysis_result_summary_is_readable(
        self,
        social_context: Context,
    ) -> None:
        """summary() should produce non-empty human-readable text."""
        from pycypher.ast_models import ASTConverter

        query = ASTConverter().from_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a",
        )
        result = QueryPlanAnalyzer(query, social_context).analyze()
        summary = result.summary()
        assert "Query Plan Analysis" in summary
        assert "Estimated peak memory" in summary


# ---------------------------------------------------------------------------
# Integration with Star execution
# ---------------------------------------------------------------------------


class TestQueryPlannerExecutionIntegration:
    """Verify the planner is called during Star.execute_query and doesn't break execution."""

    def test_simple_query_still_works(self, social_star: Star) -> None:
        """Basic query execution still produces correct results after planner integration."""
        result = social_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        assert len(result) == 5
        assert set(result["name"]) == {"Alice", "Bob", "Carol", "Dave", "Eve"}

    def test_relationship_query_still_works(self, social_star: Star) -> None:
        """Relationship query with planner integration produces correct results."""
        result = social_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS source, b.name AS target",
        )
        assert len(result) == 4

    def test_filtered_query_still_works(self, social_star: Star) -> None:
        """Filtered query with planner produces correct results."""
        result = social_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name",
        )
        names = set(result["name"])
        assert names == {"Carol", "Eve"}

    def test_planner_logs_join_strategy(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Planner should log join strategy for relationship queries."""
        with caplog.at_level(logging.DEBUG, logger="shared.logger"):
            social_star.execute_query(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS source",
            )
        # Check that query planner logging occurred
        planner_logs = [
            r for r in caplog.records if "query planner: join" in r.message
        ]
        assert len(planner_logs) > 0
        assert "broadcast" in planner_logs[0].message.lower()

    def test_planner_logs_pushdown_opportunity(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Planner should log pushdown opportunities for filtered relationship queries."""
        with caplog.at_level(logging.DEBUG, logger="shared.logger"):
            social_star.execute_query(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "WHERE a.age > 30 RETURN a.name AS name",
            )
        pushdown_logs = [
            r for r in caplog.records if "pushdown opportunity" in r.message
        ]
        assert len(pushdown_logs) > 0
