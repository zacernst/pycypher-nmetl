"""TDD tests for Architecture Loop 283 - Collection Expression Evaluator Extraction.

This module provides comprehensive test coverage for collection operations before
extraction from BindingExpressionEvaluator god object into CollectionExpressionEvaluator.

Collection Operations to Extract (775 lines total):
1. _eval_list_comprehension (87 lines) - List comprehension evaluation
2. _eval_quantifier (92 lines) - ANY/ALL/NONE quantifier logic
3. _eval_reduce (205 lines) - REDUCE expression evaluation
4. _eval_pattern_comprehension (212 lines) - Pattern comprehension logic
5. _eval_slicing (39 lines) - List/string slicing operations
6. _eval_property_lookup (56 lines) - Property access (e.g., n.name)
7. _eval_map_literal (26 lines) - Map literal construction
8. _eval_map_projection (58 lines) - Map projection operations

Run with:
    uv run pytest tests/test_architecture_loop_283_collection_evaluator_tdd.py -v
"""

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)


class TestListComprehensionEvaluation:
    """Test list comprehension evaluation functionality."""

    @pytest.fixture
    def list_context(self) -> Context:
        """Create context with list data for comprehension testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "numbers": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            },
        )

        table = EntityTable.from_dataframe("Person", df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_list_comprehension_basic(self, list_context: Context) -> None:
        """Test basic list comprehension evaluation."""
        star = Star(context=list_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN [x IN p.numbers | x * 2] AS doubled",
        )

        # Should return doubled values for each list
        assert len(result) == 3
        expected_values = [[2, 4, 6], [8, 10, 12], [14, 16, 18]]

        for i, row in result.iterrows():
            assert row["doubled"] == expected_values[i]

    def test_list_comprehension_with_where(
        self,
        list_context: Context,
    ) -> None:
        """Test list comprehension with WHERE clause filtering."""
        star = Star(context=list_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN [x IN p.numbers WHERE x > 2 | x] AS filtered",
        )

        # Should filter values > 2
        expected_values = [[3], [4, 5, 6], [7, 8, 9]]

        for i, row in result.iterrows():
            assert row["filtered"] == expected_values[i]

    def test_list_comprehension_nested_property(self) -> None:
        """Test list comprehension accessing nested properties."""
        # Create context with nested structure
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2],
                "name": ["Group1", "Group2"],
                "items": [
                    [{"value": 10}, {"value": 20}],
                    [{"value": 30}, {"value": 40}],
                ],
            },
        )

        table = EntityTable.from_dataframe("Group", df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"Group": table}),
        )

        star = Star(context=context)
        result = star.execute_query(
            "MATCH (g:Group) RETURN [item IN g.items | item.value] AS values",
        )

        # Should extract value from each nested item
        expected_values = [[10, 20], [30, 40]]
        for i, row in result.iterrows():
            assert row["values"] == expected_values[i]


class TestQuantifierEvaluation:
    """Test quantifier (ANY/ALL/NONE) evaluation functionality."""

    @pytest.fixture
    def quantifier_context(self) -> Context:
        """Create context for quantifier testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Carol", "Dave"],
                "scores": [
                    [90, 85, 88],
                    [75, 80, 85],
                    [95, 92, 98],
                    [60, 65, 70],
                ],
            },
        )

        table = EntityTable.from_dataframe("Student", df)
        return Context(
            entity_mapping=EntityMapping(mapping={"Student": table}),
        )

    def test_any_quantifier(self, quantifier_context: Context) -> None:
        """Test ANY quantifier evaluation."""
        star = Star(context=quantifier_context)
        result = star.execute_query(
            "MATCH (s:Student) RETURN s.name AS name, ANY(score IN s.scores WHERE score > 90) AS has_high_score",
        )

        # Alice: [90, 85, 88] - no score > 90 = false
        # Bob: [75, 80, 85] - no score > 90 = false
        # Carol: [95, 92, 98] - has scores > 90 = true
        # Dave: [60, 65, 70] - no score > 90 = false

        expected = [False, False, True, False]
        for i, (_, row) in enumerate(result.iterrows()):
            assert row["has_high_score"] == expected[i]

    def test_all_quantifier(self, quantifier_context: Context) -> None:
        """Test ALL quantifier evaluation."""
        star = Star(context=quantifier_context)
        result = star.execute_query(
            "MATCH (s:Student) RETURN s.name AS name, ALL(score IN s.scores WHERE score >= 75) AS all_passing",
        )

        # Alice: [90, 85, 88] - all >= 75 = true
        # Bob: [75, 80, 85] - all >= 75 = true
        # Carol: [95, 92, 98] - all >= 75 = true
        # Dave: [60, 65, 70] - not all >= 75 = false

        expected = [True, True, True, False]
        for i, (_, row) in enumerate(result.iterrows()):
            assert row["all_passing"] == expected[i]

    def test_none_quantifier(self, quantifier_context: Context) -> None:
        """Test NONE quantifier evaluation."""
        star = Star(context=quantifier_context)
        result = star.execute_query(
            "MATCH (s:Student) RETURN s.name AS name, NONE(score IN s.scores WHERE score < 60) AS none_failing",
        )

        # All students have scores >= 60, so none have scores < 60
        expected = [True, True, True, True]
        for i, (_, row) in enumerate(result.iterrows()):
            assert row["none_failing"] == expected[i]


class TestReduceEvaluation:
    """Test REDUCE expression evaluation functionality."""

    @pytest.fixture
    def reduce_context(self) -> Context:
        """Create context for REDUCE testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "values": [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
            },
        )

        table = EntityTable.from_dataframe("Person", df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_reduce_sum(self, reduce_context: Context) -> None:
        """Test REDUCE for summing list values."""
        star = Star(context=reduce_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, "
            "REDUCE(total = 0, value IN p.values | total + value) AS sum",
        )

        # Alice: 1+2+3 = 6
        # Bob: 4+5 = 9
        # Carol: 6+7+8+9 = 30
        expected_sums = [6, 9, 30]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["sum"] == expected_sums[i]

    def test_reduce_accumulate_string(self, reduce_context: Context) -> None:
        """Test REDUCE for string accumulation."""
        star = Star(context=reduce_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, "
            "REDUCE(acc = '', value IN p.values | acc + toString(value) + ',') AS concatenated",
        )

        # Should concatenate all values with commas
        expected = ["1,2,3,", "4,5,", "6,7,8,9,"]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["concatenated"] == expected[i]

    def test_reduce_max_value(self, reduce_context: Context) -> None:
        """Test REDUCE for finding maximum value."""
        star = Star(context=reduce_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, "
            "REDUCE(max_val = 0, value IN p.values | "
            "CASE WHEN value > max_val THEN value ELSE max_val END) AS max_value",
        )

        # Alice: max(1,2,3) = 3
        # Bob: max(4,5) = 5
        # Carol: max(6,7,8,9) = 9
        expected_max = [3, 5, 9]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["max_value"] == expected_max[i]


class TestPatternComprehensionEvaluation:
    """Test pattern comprehension evaluation functionality."""

    @pytest.fixture
    def pattern_context(self) -> Context:
        """Create context with relationships for pattern comprehension."""
        # Create people
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Carol", "Dave"],
                "age": [25, 30, 35, 40],
            },
        )

        # Create relationships
        friend_df = pd.DataFrame(
            {
                ID_COLUMN: [101, 102, 103, 104],
                "__SOURCE__": [1, 1, 2, 3],
                "__TARGET__": [2, 3, 4, 1],
                "since": [2020, 2018, 2019, 2021],
            },
        )

        person_table = EntityTable.from_dataframe("Person", person_df)
        friend_table = EntityTable.from_dataframe("FRIEND", friend_df)

        return Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"FRIEND": friend_table},
            ),
        )

    def test_pattern_comprehension_basic(
        self,
        pattern_context: Context,
    ) -> None:
        """Test basic pattern comprehension evaluation."""
        star = Star(context=pattern_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, "
            "[(p)-[:FRIEND]->(friend) | friend.name] AS friend_names",
        )

        # Alice friends: Bob, Carol
        # Bob friends: Dave
        # Carol friends: Alice
        # Dave friends: none
        expected_friends = {
            "Alice": ["Bob", "Carol"],
            "Bob": ["Dave"],
            "Carol": ["Alice"],
            "Dave": [],
        }

        for _, row in result.iterrows():
            name = row["name"]
            friends = row["friend_names"] or []
            assert sorted(friends) == sorted(expected_friends[name])

    def test_pattern_comprehension_with_where(
        self,
        pattern_context: Context,
    ) -> None:
        """Test pattern comprehension with WHERE filtering."""
        star = Star(context=pattern_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, "
            "[(p)-[:FRIEND]->(friend) WHERE friend.age > 30 | friend.name] AS older_friends",
        )

        # Filter friends where age > 30
        # Alice friends: Bob(30), Carol(35) -> Carol only
        # Bob friends: Dave(40) -> Dave
        # Carol friends: Alice(25) -> none
        # Dave friends: none -> none
        expected = {
            "Alice": ["Carol"],
            "Bob": ["Dave"],
            "Carol": [],
            "Dave": [],
        }

        for _, row in result.iterrows():
            name = row["name"]
            older_friends = row["older_friends"] or []
            assert sorted(older_friends) == sorted(expected[name])


class TestSlicingEvaluation:
    """Test list/string slicing evaluation functionality."""

    @pytest.fixture
    def slicing_context(self) -> Context:
        """Create context for slicing testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "text": ["Hello World", "Python Code", "Data Science"],
                "numbers": [
                    [1, 2, 3, 4, 5],
                    [10, 20, 30],
                    [100, 200, 300, 400],
                ],
            },
        )

        table = EntityTable.from_dataframe("Person", df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_list_slicing_basic(self, slicing_context: Context) -> None:
        """Test basic list slicing operations."""
        star = Star(context=slicing_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.numbers[1..3] AS sliced",
        )

        # Alice: [1,2,3,4,5][1..3] = [2,3] (exclusive end)
        # Bob: [10,20,30][1..3] = [20,30]
        # Carol: [100,200,300,400][1..3] = [200,300]
        expected = [[2, 3], [20, 30], [200, 300]]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["sliced"] == expected[i]

    def test_string_slicing(self, slicing_context: Context) -> None:
        """Test string slicing operations."""
        star = Star(context=slicing_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.text[0..5] AS prefix",
        )

        # Should get first 5 characters of each text (exclusive end)
        expected = ["Hello", "Pytho", "Data "]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["prefix"] == expected[i]

    def test_negative_indexing_slicing(self, slicing_context: Context) -> None:
        """Test slicing with negative indices."""
        star = Star(context=slicing_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.numbers[-2..] AS last_two",
        )

        # Get last 2 elements from each list
        expected = [[4, 5], [20, 30], [300, 400]]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["last_two"] == expected[i]


class TestPropertyLookupEvaluation:
    """Test property lookup evaluation functionality."""

    @pytest.fixture
    def property_context(self) -> Context:
        """Create context for property testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
                "address": [
                    {"street": "123 Main St", "city": "NYC"},
                    {"street": "456 Oak Ave", "city": "LA"},
                    {"street": "789 Pine Rd", "city": "Chicago"},
                ],
            },
        )

        table = EntityTable.from_dataframe("Person", df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_simple_property_lookup(self, property_context: Context) -> None:
        """Test basic property access."""
        star = Star(context=property_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
        )

        expected_names = ["Alice", "Bob", "Carol"]
        expected_ages = [25, 30, 35]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["name"] == expected_names[i]
            assert row["age"] == expected_ages[i]

    def test_nested_property_lookup(self, property_context: Context) -> None:
        """Test nested property access."""
        star = Star(context=property_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.address.city AS city",
        )

        expected_cities = ["NYC", "LA", "Chicago"]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["city"] == expected_cities[i]

    def test_missing_property_lookup(self, property_context: Context) -> None:
        """Test property lookup for non-existent properties."""
        star = Star(context=property_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.nonexistent AS missing",
        )

        # Missing properties should return null
        for _, row in result.iterrows():
            assert pd.isna(row["missing"])


class TestMapOperationsEvaluation:
    """Test map literal and projection evaluation functionality."""

    @pytest.fixture
    def map_context(self) -> Context:
        """Create context for map operations testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
                "salary": [50000, 75000, 100000],
            },
        )

        table = EntityTable.from_dataframe("Person", df)
        return Context(entity_mapping=EntityMapping(mapping={"Person": table}))

    def test_map_literal_creation(self, map_context: Context) -> None:
        """Test map literal construction."""
        star = Star(context=map_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN {name: p.name, age: p.age, employed: true} AS person_map",
        )

        expected_maps = [
            {"name": "Alice", "age": 25, "employed": True},
            {"name": "Bob", "age": 30, "employed": True},
            {"name": "Carol", "age": 35, "employed": True},
        ]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["person_map"] == expected_maps[i]

    def test_map_projection_basic(self, map_context: Context) -> None:
        """Test basic map projection operations."""
        star = Star(context=map_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p{name, age} AS projected_person",
        )

        # Should project only specified properties
        expected = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Carol", "age": 35},
        ]

        for i, (_, row) in enumerate(result.iterrows()):
            projected = row["projected_person"]
            assert projected["name"] == expected[i]["name"]
            assert projected["age"] == expected[i]["age"]
            assert (
                "salary" not in projected
            )  # Should not include unprojected properties

    def test_map_projection_with_computed_values(
        self,
        map_context: Context,
    ) -> None:
        """Test map projection with computed property values."""
        star = Star(context=map_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p{name, double_age: p.age * 2} AS computed_map",
        )

        expected = [
            {"name": "Alice", "double_age": 50},
            {"name": "Bob", "double_age": 60},
            {"name": "Carol", "double_age": 70},
        ]

        for i, (_, row) in enumerate(result.iterrows()):
            computed = row["computed_map"]
            assert computed["name"] == expected[i]["name"]
            assert computed["double_age"] == expected[i]["double_age"]


class TestCollectionEvaluatorIntegration:
    """Test integration scenarios combining multiple collection operations."""

    @pytest.fixture
    def integration_context(self) -> Context:
        """Create rich context for integration testing."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "skills": [
                    ["Python", "SQL", "Data Analysis"],
                    ["Java", "Spring", "Microservices"],
                    ["R", "Statistics", "Machine Learning"],
                ],
                "projects": [
                    [
                        {"name": "Dashboard", "hours": 40},
                        {"name": "API", "hours": 30},
                    ],
                    [{"name": "Backend", "hours": 50}],
                    [
                        {"name": "Analysis", "hours": 35},
                        {"name": "Model", "hours": 45},
                    ],
                ],
            },
        )

        person_table = EntityTable.from_dataframe("Person", person_df)
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )

    def test_complex_collection_query(
        self,
        integration_context: Context,
    ) -> None:
        """Test complex query combining multiple collection operations."""
        star = Star(context=integration_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "p.skills[0..2] AS top_skills, "  # Slicing
            "[skill IN p.skills WHERE skill CONTAINS 'a' | skill] AS skills_with_a, "  # List comprehension with WHERE
            "REDUCE(total = 0, proj IN p.projects | total + proj.hours) AS total_hours ",  # Reduce
        )

        expected_results = [
            {
                "name": "Alice",
                "top_skills": ["Python", "SQL"],
                "skills_with_a": [
                    "Data Analysis",
                ],  # Only "Data Analysis" contains 'a'
                "total_hours": 70,  # 40 + 30
            },
            {
                "name": "Bob",
                "top_skills": ["Java", "Spring"],
                "skills_with_a": ["Java"],  # Only "Java" contains 'a'
                "total_hours": 50,  # 50
            },
            {
                "name": "Carol",
                "top_skills": ["R", "Statistics"],
                "skills_with_a": [
                    "Statistics",
                    "Machine Learning",
                ],  # "Statistics" and "Machine Learning" both contain 'a'
                "total_hours": 80,  # 35 + 45
            },
        ]

        for i, (_, row) in enumerate(result.iterrows()):
            expected = expected_results[i]
            assert row["name"] == expected["name"]
            assert row["top_skills"] == expected["top_skills"]
            assert row["skills_with_a"] == expected["skills_with_a"]
            assert row["total_hours"] == expected["total_hours"]

    def test_nested_collection_operations(
        self,
        integration_context: Context,
    ) -> None:
        """Test nested collection operations (comprehension within comprehension)."""
        star = Star(context=integration_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "[proj IN p.projects | {name: proj.name, category: "
            "CASE WHEN proj.hours > 40 THEN 'large' ELSE 'small' END}] AS categorized_projects",
        )

        expected = [
            {
                "name": "Alice",
                "categorized_projects": [
                    {"name": "Dashboard", "category": "small"},  # 40 not > 40
                    {"name": "API", "category": "small"},  # 30 not > 40
                ],
            },
            {
                "name": "Bob",
                "categorized_projects": [
                    {"name": "Backend", "category": "large"},  # 50 > 40
                ],
            },
            {
                "name": "Carol",
                "categorized_projects": [
                    {"name": "Analysis", "category": "small"},  # 35 not > 40
                    {"name": "Model", "category": "large"},  # 45 > 40
                ],
            },
        ]

        for i, (_, row) in enumerate(result.iterrows()):
            assert row["name"] == expected[i]["name"]
            assert (
                row["categorized_projects"]
                == expected[i]["categorized_projects"]
            )

    def test_performance_large_collection_operations(self) -> None:
        """Test collection operations performance with larger datasets."""
        # Create larger dataset for performance validation
        import time

        large_df = pd.DataFrame(
            {
                ID_COLUMN: list(range(1, 101)),  # 100 people
                "name": [f"Person{i}" for i in range(1, 101)],
                "values": [
                    list(range(i, i + 50)) for i in range(1, 101)
                ],  # 50 values each
            },
        )

        table = EntityTable.from_dataframe("Person", large_df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
        )
        star = Star(context=context)

        # Test list comprehension performance
        start_time = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN count([x IN p.values WHERE x % 2 = 0 | x]) AS even_count",
        )
        end_time = time.perf_counter()

        # Should complete in reasonable time (< 5 seconds for 100 people × 50 values)
        execution_time = end_time - start_time
        assert execution_time < perf_threshold(5.0), (
            f"Large collection operation took {execution_time:.2f}s"
        )

        # Should have correct result (count of filtered even numbers)
        assert result["even_count"].iloc[0] > 0
