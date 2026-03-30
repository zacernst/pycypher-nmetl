"""Test to reproduce the mutable context issue.

This test demonstrates the problem where running the same query twice
produces different results due to context mutations.
"""

import pandas as pd
import pytest
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestMutableContextIssue:
    """Test that reproduces the mutable context problem."""

    @pytest.fixture
    def star_with_modifiable_data(self) -> Star:
        """Create a Star with data that can be modified."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
                "active": [True, True, False],
            },
        )
        table = EntityTable.from_dataframe("Person", person_df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
        )
        return Star(context=context)

    def test_mutation_affects_subsequent_queries(
        self,
        star_with_modifiable_data: Star,
    ) -> None:
        """Test that demonstrates the mutable context issue."""
        # First query: SET Alice's age to 40
        result1 = star_with_modifiable_data.execute_query(
            "MATCH (p:Person {name: 'Alice'}) SET p.age = 40 RETURN p.age AS age",
        )
        assert result1.iloc[0]["age"] == 40

        # Second query: Check Alice's age again (should this be 30 or 40?)
        result2 = star_with_modifiable_data.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age",
        )

        # The PROBLEM: Alice's age is now 40, even though we want isolation
        # This demonstrates that the context was mutated by the first query
        actual_age = result2.iloc[0]["age"]
        print(f"Alice's age after mutation: {actual_age}")

        # Document the current behavior (this is the bug):
        # The age persists across queries because mutations write directly to source_obj
        assert actual_age == 40  # Current buggy behavior

        # What we WANT is for each query to see the original state:
        # assert actual_age == 30  # Desired behavior (isolation)

    def test_same_query_produces_different_results(
        self,
        star_with_modifiable_data: Star,
    ) -> None:
        """Test that the same query produces different results on repeated execution."""
        query = "MATCH (p:Person) SET p.visited = true RETURN count(p) AS count"

        # Run the query twice
        result1 = star_with_modifiable_data.execute_query(query)
        result2 = star_with_modifiable_data.execute_query(query)

        # Both should return the same count
        count1 = result1.iloc[0]["count"]
        count2 = result2.iloc[0]["count"]

        assert count1 == count2  # This should pass (same query, same result)
        assert count1 == 3  # Should be 3 people

        # But let's check if the 'visited' property was actually added to context
        # This demonstrates the mutation persistence problem
        check_result = star_with_modifiable_data.execute_query(
            "MATCH (p:Person) WHERE p.visited = true RETURN count(p) AS visited_count",
        )

        visited_count = check_result.iloc[0]["visited_count"]
        print(f"Number of people with visited=true: {visited_count}")

        # This demonstrates the issue: the visited property persists in context
        # After running the SET query, all people have visited=true permanently
        assert visited_count == 3  # Current buggy behavior - mutation persists

    def test_context_state_leakage_between_queries(
        self,
        star_with_modifiable_data: Star,
    ) -> None:
        """Test that context state leaks between different queries."""
        # Query 1: Add a new property to all people
        star_with_modifiable_data.execute_query(
            "MATCH (p:Person) SET p.processed = true RETURN count(p) AS count",
        )

        # Query 2: Check if the property exists (it shouldn't in isolated execution)
        result = star_with_modifiable_data.execute_query(
            "MATCH (p:Person) WHERE p.processed IS NOT NULL RETURN count(p) AS processed_count",
        )

        processed_count = result.iloc[0]["processed_count"]

        # The PROBLEM: This returns 3 because the context was mutated
        # In proper isolation, this should return 0
        assert processed_count == 3  # Current buggy behavior

        # What we want: assert processed_count == 0  # Desired isolated behavior

    def test_shadow_write_system_exists_but_bypassed(
        self,
        star_with_modifiable_data: Star,
    ) -> None:
        """Test that shadow write system exists but is being bypassed."""
        # Check that the context has shadow write infrastructure
        context = star_with_modifiable_data.context

        # These methods should exist
        assert hasattr(context, "begin_query")
        assert hasattr(context, "commit_query")
        assert hasattr(context, "rollback_query")

        # After begin_query, shadow should be initialized
        context.begin_query()
        assert hasattr(context, "_shadow")
        assert hasattr(context, "_shadow_rels")
        assert context._shadow == {}  # Empty but not None
        assert context._shadow_rels == {}  # Empty but not None

        # The problem is in mutate() method when shadow is empty dict
        # but the fallback path checks `if shadow is None:` instead of `if not shadow:`
