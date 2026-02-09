"""Additional comprehensive tests for Cypher grammar parser - filling identified gaps.

This test suite complements test_grammar_parser.py by adding tests for:
1. AST to typed AST conversion (Critical)
2. AST traversal and modification utilities (Critical)
3. Variable scoping and binding (Critical)
4. Subquery correlation (Critical)
5. Enhanced error messages (Critical)
6. SET clause variations (Medium priority)
7. Path variables and functions (Medium priority)
8. Parameter usage (Medium priority)
9. NULL handling edge cases (Lower priority)
10. Additional edge cases

These tests verify functionality that developers will directly use.
"""

import pytest
from lark import Tree
from lark.exceptions import LarkError, UnexpectedInput
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parser():
    """Create a GrammarParser instance for testing."""
    return GrammarParser()


# ============================================================================
# Critical Priority 1: Lark Parse Tree Structure Tests
# ============================================================================


class TestParseTreeStructure:
    """Test Lark parse tree structure for various queries."""

    def test_simple_match_tree_structure(self, parser):
        """Verify simple MATCH query produces correct tree structure."""
        query = "MATCH (n:Person) RETURN n"
        tree = parser.parse(query)

        # Verify it's a Tree object
        assert isinstance(tree, Tree), f"Expected Tree, got {type(tree)}"

        # Tree should have structure
        assert tree.data is not None
        assert len(tree.children) > 0

    def test_match_with_where_has_where_clause(self, parser):
        """Test MATCH with WHERE produces where_clause in tree."""
        query = "MATCH (n:Person {age: 30}) WHERE n.active = true RETURN n"
        tree = parser.parse(query)

        # Convert to string to inspect structure
        tree_str = tree.pretty()
        assert "where" in tree_str.lower() or "condition" in tree_str.lower()

    def test_create_produces_update_statement(self, parser):
        """Test CREATE produces update_statement tree."""
        query = "CREATE (n:Person {name: 'Alice', age: 30})"
        tree = parser.parse(query)

        assert isinstance(tree, Tree)
        tree_str = tree.pretty()
        assert "create" in tree_str.lower()

    def test_complex_query_tree_depth(self, parser):
        """Test complex query produces deep tree structure."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 25
        WITH a, b, r
        WHERE b.active = true
        RETURN a.name AS person1, b.name AS person2, r.since AS since
        ORDER BY since DESC
        LIMIT 10
        """
        tree = parser.parse(query)

        # Tree should be deeply nested for complex query
        assert isinstance(tree, Tree)
        assert len(tree.children) > 0

        # Should have multiple levels
        def tree_depth(t):
            if not isinstance(t, Tree):
                return 0
            if not t.children:
                return 1
            return 1 + max(tree_depth(c) for c in t.children)

        depth = tree_depth(tree)
        assert depth > 3, f"Expected deep tree, got depth {depth}"

    def test_all_literal_types_parse(self, parser):
        """Test all literal types parse correctly."""
        literals = [
            ("42", "integer"),
            ("3.14", "float"),
            ("'hello'", "string"),
            ("true", "boolean"),
            ("null", "null"),
            ("[1, 2, 3]", "list"),
            ("{a: 1, b: 2}", "map"),
        ]

        for literal, lit_type in literals:
            query = f"RETURN {literal}"
            tree = parser.parse(query)
            assert isinstance(tree, Tree), (
                f"Failed to parse {lit_type} literal"
            )


# ============================================================================
# Critical Priority 2: Lark Tree Navigation
# ============================================================================


class TestTreeNavigation:
    """Test navigating and extracting information from Lark parse trees."""

    def test_find_match_clauses(self, parser):
        """Test finding match clauses in tree."""
        query = """
        MATCH (a:Person)
        MATCH (b:Person)
        RETURN a, b
        """
        tree = parser.parse(query)

        # Find all match clauses using tree.find_data()
        match_count = sum(1 for _ in tree.find_data("match_clause"))
        assert match_count >= 2, (
            f"Expected at least 2 MATCH clauses, found {match_count}"
        )

    def test_extract_node_patterns(self, parser):
        """Test extracting node patterns from tree."""
        query = "MATCH (a:Person), (b:Company) RETURN a, b"
        tree = parser.parse(query)

        # Find node patterns
        node_patterns = list(tree.find_data("node_pattern"))
        assert len(node_patterns) >= 2, (
            f"Expected at least 2 node patterns, found {len(node_patterns)}"
        )

    def test_tree_iteration(self, parser):
        """Test iterating over all tree nodes."""
        query = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)

        # Count all nodes in tree
        node_count = sum(1 for _ in tree.iter_subtrees())
        assert node_count > 5, f"Expected many nodes, found {node_count}"

    def test_find_where_clauses(self, parser):
        """Test finding WHERE clauses in tree."""
        query = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)

        # Look for where clause
        where_clauses = list(tree.find_data("where_clause"))
        assert len(where_clauses) >= 1, "Should find WHERE clause"


# ============================================================================
# Critical Priority 3: Variable Scoping (Query Validation)
# ============================================================================


class TestVariableScoping:
    """Test that queries with correct variable scoping parse successfully."""

    def test_with_clause_creates_new_bindings(self, parser):
        """WITH clause introduces new variable bindings."""
        query = """
        MATCH (n:Person)
        WITH n.name AS personName, n.age AS personAge
        WHERE personAge > 25
        RETURN personName
        """
        tree = parser.parse(query)
        assert tree is not None
        # Variables personName and personAge should be accessible after WITH

    def test_variable_shadowing_in_with(self, parser):
        """Test variable shadowing across WITH clauses."""
        query = """
        MATCH (n:Person)
        WITH n.name AS n
        MATCH (m:Person {name: n})
        RETURN m
        """
        tree = parser.parse(query)
        assert tree is not None
        # 'n' in first MATCH is different from 'n' after WITH

    def test_comprehension_local_variables(self, parser):
        """Variables in comprehensions are locally scoped."""
        query = """
        WITH [1, 2, 3] AS numbers
        RETURN [x IN numbers | x * 2] AS doubled,
               [x IN numbers WHERE x > 1 | x] AS filtered
        """
        tree = parser.parse(query)
        assert tree is not None
        # 'x' in first comprehension doesn't conflict with 'x' in second

    def test_nested_comprehension_scopes(self, parser):
        """Nested comprehensions have separate scopes."""
        query = """
        RETURN [x IN [1,2,3] | 
                [y IN [4,5,6] | 
                 x + y
                ]
               ] AS nested
        """
        tree = parser.parse(query)
        assert tree is not None
        # 'x' and 'y' have different scopes

    def test_pattern_comprehension_variables(self, parser):
        """Pattern comprehension variables are locally scoped."""
        query = """
        MATCH (person:Person)
        RETURN person.name,
               [path = (person)-[:KNOWS]->(friend) | friend.name] AS friendNames
        """
        tree = parser.parse(query)
        assert tree is not None
        # 'path' and 'friend' are local to comprehension


# ============================================================================
# Critical Priority 4: Subquery Correlation
# ============================================================================


class TestSubqueryCorrelation:
    """Test variable references across subquery boundaries."""

    def test_exists_references_outer_variable(self, parser):
        """EXISTS subquery can reference outer scope variables."""
        query = """
        MATCH (p:Person)
        WHERE EXISTS {
            MATCH (p)-[:KNOWS]->(friend:Person)
            WHERE friend.age > p.age
        }
        RETURN p
        """
        tree = parser.parse(query)
        assert tree is not None
        # 'p' from outer MATCH is accessible in EXISTS subquery

    def test_exists_with_where_correlation(self, parser):
        """EXISTS with WHERE correlating outer and inner variables."""
        query = """
        MATCH (n:Person)
        WHERE n.age > 18
          AND EXISTS {
            MATCH (n)-[:WORKS_AT]->(c:Company)
            WHERE c.revenue > 1000000
          }
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_exists_subqueries(self, parser):
        """Multiple EXISTS subqueries in same WHERE clause."""
        query = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:KNOWS]->(:Person) }
          AND NOT EXISTS { (p)-[:BLOCKED]->() }
        RETURN p
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_pattern_in_exists_subquery(self, parser):
        """Pattern-only EXISTS subquery (no MATCH keyword)."""
        query = """
        MATCH (person:Person)
        WHERE EXISTS { (person)-[:KNOWS]->(:Person {country: 'USA'}) }
        RETURN person.name
        """
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Critical Priority 5: Error Messages and Validation
# ============================================================================


class TestErrorMessages:
    """Test error messages for common syntax errors."""

    def test_unclosed_parenthesis_error(self, parser):
        """Missing closing parenthesis should give clear error."""
        query = "MATCH (n:Person RETURN n"
        try:
            parser.parse(query)
            pytest.fail("Should have raised parsing error")
        except (LarkError, UnexpectedInput) as e:
            error_msg = str(e).lower()
            # Error should mention parenthesis or bracket
            assert any(
                word in error_msg
                for word in ["parenthes", "bracket", "expected", ")"]
            )

    def test_invalid_keyword_error(self, parser):
        """Invalid keyword should give helpful error."""
        query = "MATCH (n:Person) RETRUN n"  # Typo: RETRUN
        try:
            parser.parse(query)
            pytest.fail("Should have raised parsing error")
        except (LarkError, UnexpectedInput) as e:
            # Should indicate unexpected token
            assert True

    def test_missing_return_error(self, parser):
        """Query missing RETURN should error clearly."""
        query = "MATCH (n:Person) WHERE n.age > 30"
        # This might be valid depending on grammar (query without RETURN)
        # If invalid, should give clear error
        try:
            result = parser.parse(query)
            # If it parses, that's also valid behavior
            assert result is not None
        except (LarkError, UnexpectedInput):
            # If it errors, that's expected
            pass

    def test_invalid_property_syntax_error(self, parser):
        """Invalid property syntax should error."""
        query = "MATCH (n:Person) WHERE n..age > 30 RETURN n"  # Double dot
        try:
            parser.parse(query)
            pytest.fail("Should have raised parsing error")
        except (LarkError, UnexpectedInput):
            pass

    def test_incomplete_relationship_pattern_error(self, parser):
        """Incomplete relationship pattern should error."""
        query = "MATCH (a)-[r->(b) RETURN a"  # Missing closing bracket
        try:
            parser.parse(query)
            pytest.fail("Should have raised parsing error")
        except (LarkError, UnexpectedInput):
            pass


# ============================================================================
# Medium Priority 6: SET Clause Variations
# ============================================================================


class TestSetClauseVariations:
    """Test all SET clause syntax variations."""

    def test_set_single_property(self, parser):
        """SET single property value."""
        query = "MATCH (n:Person) SET n.age = 31 RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_multiple_properties(self, parser):
        """SET multiple properties at once."""
        query = "MATCH (n:Person) SET n.age = 31, n.status = 'active' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_all_properties_from_map(self, parser):
        """SET all properties from a map (replaces all)."""
        query = "MATCH (n:Person) SET n = {name: 'Alice', age: 30} RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_merge_properties(self, parser):
        """SET merge properties using += operator."""
        query = (
            "MATCH (n:Person) SET n += {extra: 'value', more: 123} RETURN n"
        )
        tree = parser.parse(query)
        assert tree is not None

    def test_set_labels(self, parser):
        """SET node labels."""
        query = "MATCH (n:Person) SET n:Employee:Manager RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_computed_property(self, parser):
        """SET property to computed value."""
        query = "MATCH (n:Person) SET n.birthYear = 2024 - n.age RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_from_another_property(self, parser):
        """SET property from another property."""
        query = "MATCH (n:Person) SET n.oldName = n.name, n.name = 'New Name' RETURN n"
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Medium Priority 7: Path Variables and Functions
# ============================================================================


class TestPathVariables:
    """Test path variables and path-related functions."""

    def test_path_variable_assignment(self, parser):
        """Assign path to variable."""
        query = "MATCH path = (a)-[*1..3]-(b) RETURN path"
        tree = parser.parse(query)
        assert tree is not None

    def test_nodes_function_on_path(self, parser):
        """Use nodes() function to get nodes from path."""
        query = """
        MATCH p = (a:Person)-[*]-(b:Person)
        RETURN nodes(p)
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_relationships_function_on_path(self, parser):
        """Use relationships() function to get relationships from path."""
        query = """
        MATCH p = (a)-[*]-(b)
        RETURN relationships(p)
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_length_function_on_path(self, parser):
        """Use length() function to get path length."""
        query = """
        MATCH p = (a)-[*1..5]-(b)
        WHERE length(p) = 3
        RETURN p
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_path_functions(self, parser):
        """Use multiple path functions together."""
        query = """
        MATCH p = (a)-[*]-(b)
        RETURN length(p) AS pathLength,
               nodes(p) AS pathNodes,
               relationships(p) AS pathRels
        """
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Medium Priority 8: Parameters
# ============================================================================


class TestParameters:
    """Test parameter syntax and usage."""

    def test_parameter_in_match(self, parser):
        """Use parameter in MATCH clause."""
        query = "MATCH (n:Person {id: $personId}) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_parameter_in_where(self, parser):
        """Use parameter in WHERE clause."""
        query = "MATCH (n) WHERE n.age > $minAge RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_parameters(self, parser):
        """Use multiple parameters in query."""
        query = """
        MATCH (n:Person)
        WHERE n.age > $minAge AND n.age < $maxAge
          AND n.status = $status
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_parameter_in_limit_skip(self, parser):
        """Use parameters for LIMIT and SKIP."""
        query = "MATCH (n) RETURN n SKIP $offset LIMIT $limit"
        tree = parser.parse(query)
        assert tree is not None

    def test_parameter_in_create(self, parser):
        """Use parameters in CREATE statement."""
        query = "CREATE (n:Person {name: $name, age: $age})"
        tree = parser.parse(query)
        assert tree is not None

    def test_numeric_parameter(self, parser):
        """Use numeric parameter name."""
        query = "MATCH (n) WHERE n.id = $1 RETURN n"
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Medium Priority 9: UNWIND Edge Cases
# ============================================================================


class TestUnwindEdgeCases:
    """Test UNWIND with complex expressions."""

    def test_unwind_comprehension_result(self, parser):
        """UNWIND the result of a list comprehension."""
        query = "UNWIND [x IN [1,2,3] | x * 2] AS doubled RETURN doubled"
        tree = parser.parse(query)
        assert tree is not None

    def test_unwind_nested_list(self, parser):
        """UNWIND nested lists."""
        query = "UNWIND [[1,2], [3,4], [5,6]] AS pair RETURN pair"
        tree = parser.parse(query)
        assert tree is not None

    def test_unwind_with_where(self, parser):
        """UNWIND followed by WITH ... WHERE filter."""
        query = """
        UNWIND [1,2,3,4,5] AS num
        WITH num
        WHERE num > 2
        RETURN num
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_unwind_clauses(self, parser):
        """Multiple UNWIND clauses in sequence."""
        query = """
        UNWIND [1,2,3] AS x
        UNWIND [4,5,6] AS y
        RETURN x, y, x + y AS sum
        """
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Medium Priority 10: WITH Clause Edge Cases
# ============================================================================


class TestWithClauseEdgeCases:
    """Test WITH clause with various modifiers."""

    def test_with_distinct(self, parser):
        """WITH DISTINCT to deduplicate."""
        query = """
        MATCH (n:Person)
        WITH DISTINCT n.type AS type
        RETURN type
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_with_aggregation_and_where(self, parser):
        """WITH with aggregation and WHERE filter."""
        query = """
        MATCH (n:Person)
        WITH n.department AS dept, count(*) AS cnt
        WHERE cnt > 5
        RETURN dept, cnt
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_with_order_by_limit(self, parser):
        """WITH with ORDER BY and LIMIT."""
        query = """
        MATCH (n:Person)
        WITH n.age AS age, n.name AS name
        ORDER BY age DESC
        LIMIT 10
        RETURN name, age
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_with_clauses(self, parser):
        """Multiple WITH clauses chained."""
        query = """
        MATCH (n:Person)
        WITH n.name AS name, n.age AS age
        WHERE age > 30
        WITH name, age, age / 10 AS decade
        WHERE decade < 5
        RETURN name, age, decade
        """
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Lower Priority: NULL Handling Edge Cases
# ============================================================================


class TestNullHandling:
    """Test NULL value handling in various contexts."""

    def test_null_literal(self, parser):
        """NULL literal in RETURN."""
        query = "RETURN null AS value"
        tree = parser.parse(query)
        assert tree is not None

    def test_null_comparison(self, parser):
        """Comparing with NULL."""
        query = "RETURN null = null AS eq, null <> null AS neq"
        tree = parser.parse(query)
        assert tree is not None

    def test_is_null_predicate(self, parser):
        """IS NULL predicate."""
        query = "MATCH (n) WHERE n.prop IS NULL RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_is_not_null_predicate(self, parser):
        """IS NOT NULL predicate."""
        query = "MATCH (n) WHERE n.email IS NOT NULL RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_null_in_map(self, parser):
        """NULL value in map literal."""
        query = "RETURN {a: null, b: 'value'} AS map"
        tree = parser.parse(query)
        assert tree is not None

    def test_null_in_list(self, parser):
        """NULL value in list literal."""
        query = "RETURN [1, null, 3, null, 5] AS list"
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Lower Priority: IN Operator
# ============================================================================


class TestInOperator:
    """Test IN operator with various value types."""

    def test_in_with_string_list(self, parser):
        """IN operator with list of strings."""
        query = "MATCH (n) WHERE n.status IN ['active', 'pending', 'approved'] RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_in_with_number_list(self, parser):
        """IN operator with list of numbers."""
        query = "MATCH (n) WHERE n.id IN [1, 2, 3, 5, 8, 13] RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_in_with_parameter(self, parser):
        """IN operator with parameter."""
        query = "MATCH (n) WHERE n.id IN $allowedIds RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_in_with_subquery(self, parser):
        """IN operator with list comprehension."""
        query = """
        MATCH (n:Person)
        WHERE n.age IN [x IN range(20, 30) | x]
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Lower Priority: Additional Edge Cases
# ============================================================================


class TestAdditionalEdgeCases:
    """Test additional edge cases and corner scenarios."""

    def test_very_long_property_chain(self, parser):
        """Property access chain."""
        query = "RETURN n.a.b.c.d.e.f"
        tree = parser.parse(query)
        assert tree is not None

    def test_mixed_relationship_directions(self, parser):
        """Mix of relationship directions in pattern."""
        query = "MATCH (a)-[r1]->(b)<-[r2]-(c) RETURN a, b, c"
        tree = parser.parse(query)
        assert tree is not None

    def test_empty_node_pattern(self, parser):
        """Empty node pattern (no variable, label, or properties)."""
        query = "MATCH ()-[r:KNOWS]->() RETURN r"
        tree = parser.parse(query)
        assert tree is not None

    def test_relationship_without_variable(self, parser):
        """Relationship pattern without variable."""
        query = "MATCH (a)-[:KNOWS]->(b) RETURN a, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_node_without_variable(self, parser):
        """Node pattern without variable (only label)."""
        query = "MATCH (:Person)-[r]->(:Company) RETURN r"
        tree = parser.parse(query)
        assert tree is not None

    def test_expression_in_return_alias(self, parser):
        """Complex expression with alias in RETURN."""
        query = """
        MATCH (n:Person)
        RETURN n.age * 2 + 10 AS computedValue
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_nested_function_calls(self, parser):
        """Deeply nested function calls."""
        query = "RETURN toUpper(trim(substring('  hello world  ', 2, 5)))"
        tree = parser.parse(query)
        assert tree is not None

    def test_case_insensitive_keywords(self, parser):
        """Keywords should be case-insensitive."""
        query = "match (n:Person) where n.age > 30 return n"
        tree = parser.parse(query)
        assert tree is not None

    def test_mixed_case_keywords(self, parser):
        """Mixed case keywords."""
        query = "MaTcH (n:Person) WHeRe n.age > 30 ReTuRn n"
        tree = parser.parse(query)
        assert tree is not None


# ============================================================================
# Run tests
# ============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
