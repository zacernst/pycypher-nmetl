"""Tests for the grammar_parser module.

This module contains unit tests for the openCypher grammar parser implementation.
"""

import pytest
from pycypher.grammar_parser import GrammarParser
from lark.exceptions import LarkError


@pytest.fixture
def parser():
    """Create a GrammarParser instance for testing."""
    return GrammarParser()


class TestBasicParsing:
    """Test basic parsing functionality."""

    def test_simple_match_return(self, parser):
        """Test parsing a simple MATCH ... RETURN query."""
        query = "MATCH (n) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_match_with_label(self, parser):
        """Test parsing MATCH with node label."""
        query = "MATCH (n:Person) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_match_with_properties(self, parser):
        """Test parsing MATCH with node properties."""
        query = "MATCH (n:Person {name: 'Alice'}) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_match_with_relationship(self, parser):
        """Test parsing MATCH with relationships."""
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_match_with_where(self, parser):
        """Test parsing MATCH with WHERE clause."""
        query = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)
        assert tree is not None


class TestCreateStatements:
    """Test CREATE statement parsing."""

    def test_create_simple_node(self, parser):
        """Test parsing CREATE with simple node."""
        query = "CREATE (n:Person)"
        tree = parser.parse(query)
        assert tree is not None

    def test_create_node_with_properties(self, parser):
        """Test parsing CREATE with node properties."""
        query = "CREATE (n:Person {name: 'Alice', age: 30})"
        tree = parser.parse(query)
        assert tree is not None

    def test_create_with_relationship(self, parser):
        """Test parsing CREATE with relationship."""
        query = "CREATE (a:Person)-[:KNOWS]->(b:Person)"
        tree = parser.parse(query)
        assert tree is not None


class TestReturnStatements:
    """Test RETURN statement parsing."""

    def test_return_all(self, parser):
        """Test parsing RETURN *."""
        query = "MATCH (n) RETURN *"
        tree = parser.parse(query)
        assert tree is not None

    def test_return_with_alias(self, parser):
        """Test parsing RETURN with alias."""
        query = "MATCH (n:Person) RETURN n.name AS personName"
        tree = parser.parse(query)
        assert tree is not None

    def test_return_distinct(self, parser):
        """Test parsing RETURN DISTINCT."""
        query = "MATCH (n:Person) RETURN DISTINCT n.age"
        tree = parser.parse(query)
        assert tree is not None

    def test_return_with_order_by(self, parser):
        """Test parsing RETURN with ORDER BY."""
        query = "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC"
        tree = parser.parse(query)
        assert tree is not None

    def test_return_with_limit(self, parser):
        """Test parsing RETURN with LIMIT."""
        query = "MATCH (n:Person) RETURN n LIMIT 10"
        tree = parser.parse(query)
        assert tree is not None

    def test_return_with_skip_and_limit(self, parser):
        """Test parsing RETURN with SKIP and LIMIT."""
        query = "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10"
        tree = parser.parse(query)
        assert tree is not None


class TestDataUpdateStatements:
    """Test data update statement parsing."""

    def test_delete_statement(self, parser):
        """Test parsing DELETE statement."""
        query = "MATCH (n:Person) DELETE n"
        tree = parser.parse(query)
        assert tree is not None

    def test_detach_delete_statement(self, parser):
        """Test parsing DETACH DELETE statement."""
        query = "MATCH (n:Person) DETACH DELETE n"
        tree = parser.parse(query)
        assert tree is not None

    def test_set_property(self, parser):
        """Test parsing SET statement."""
        query = "MATCH (n:Person) SET n.age = 31"
        tree = parser.parse(query)
        assert tree is not None

    def test_remove_property(self, parser):
        """Test parsing REMOVE statement."""
        query = "MATCH (n:Person) REMOVE n.age"
        tree = parser.parse(query)
        assert tree is not None

    def test_merge_statement(self, parser):
        """Test parsing MERGE statement."""
        query = "MERGE (n:Person {name: 'Alice'})"
        tree = parser.parse(query)
        assert tree is not None


class TestComplexQueries:
    """Test more complex query patterns."""

    def test_multiple_match_clauses(self, parser):
        """Test parsing query with multiple MATCH clauses."""
        query = """
        MATCH (a:Person)
        MATCH (b:Person)
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN a, b
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_optional_match(self, parser):
        """Test parsing OPTIONAL MATCH."""
        query = "OPTIONAL MATCH (n:Person) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_with_clause(self, parser):
        """Test parsing WITH clause."""
        query = """
        MATCH (n:Person)
        WITH n.name AS name, n.age AS age
        RETURN name, age
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_unwind_statement(self, parser):
        """Test parsing UNWIND statement."""
        query = "UNWIND [1, 2, 3] AS x RETURN x"
        tree = parser.parse(query)
        assert tree is not None


class TestLiterals:
    """Test literal value parsing."""

    def test_integer_literal(self, parser):
        """Test parsing integer literals."""
        query = "RETURN 42"
        tree = parser.parse(query)
        assert tree is not None

    def test_float_literal(self, parser):
        """Test parsing float literals."""
        query = "RETURN 3.14"
        tree = parser.parse(query)
        assert tree is not None

    def test_string_literal_single_quotes(self, parser):
        """Test parsing string literals with single quotes."""
        query = "RETURN 'Hello, World!'"
        tree = parser.parse(query)
        assert tree is not None

    def test_string_literal_double_quotes(self, parser):
        """Test parsing string literals with double quotes."""
        query = 'RETURN "Hello, World!"'
        tree = parser.parse(query)
        assert tree is not None

    def test_boolean_literal_true(self, parser):
        """Test parsing boolean literal TRUE."""
        query = "RETURN TRUE"
        tree = parser.parse(query)
        assert tree is not None

    def test_boolean_literal_false(self, parser):
        """Test parsing boolean literal FALSE."""
        query = "RETURN FALSE"
        tree = parser.parse(query)
        assert tree is not None

    def test_null_literal(self, parser):
        """Test parsing NULL literal."""
        query = "RETURN NULL"
        tree = parser.parse(query)
        assert tree is not None

    def test_list_literal(self, parser):
        """Test parsing list literals."""
        query = "RETURN [1, 2, 3, 4, 5]"
        tree = parser.parse(query)
        assert tree is not None

    def test_map_literal(self, parser):
        """Test parsing map literals."""
        query = "RETURN {name: 'Alice', age: 30}"
        tree = parser.parse(query)
        assert tree is not None


class TestExpressions:
    """Test expression parsing."""

    def test_arithmetic_addition(self, parser):
        """Test parsing arithmetic addition."""
        query = "RETURN 1 + 2"
        tree = parser.parse(query)
        assert tree is not None

    def test_arithmetic_multiplication(self, parser):
        """Test parsing arithmetic multiplication."""
        query = "RETURN 3 * 4"
        tree = parser.parse(query)
        assert tree is not None

    def test_comparison_equals(self, parser):
        """Test parsing equality comparison."""
        query = "MATCH (n) WHERE n.age = 30 RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_comparison_greater_than(self, parser):
        """Test parsing greater than comparison."""
        query = "MATCH (n) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_boolean_and(self, parser):
        """Test parsing AND operator."""
        query = "MATCH (n) WHERE n.age > 30 AND n.name = 'Alice' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_boolean_or(self, parser):
        """Test parsing OR operator."""
        query = "MATCH (n) WHERE n.age > 30 OR n.name = 'Alice' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_boolean_not(self, parser):
        """Test parsing NOT operator."""
        query = "MATCH (n) WHERE NOT n.active RETURN n"
        tree = parser.parse(query)
        assert tree is not None


class TestFunctions:
    """Test function invocation parsing."""

    def test_count_star(self, parser):
        """Test parsing COUNT(*)."""
        query = "MATCH (n) RETURN COUNT(*)"
        tree = parser.parse(query)
        assert tree is not None

    def test_function_with_arguments(self, parser):
        """Test parsing function with arguments."""
        query = "RETURN toUpper('hello')"
        tree = parser.parse(query)
        assert tree is not None

    def test_aggregation_function(self, parser):
        """Test parsing aggregation function."""
        query = "MATCH (n:Person) RETURN avg(n.age)"
        tree = parser.parse(query)
        assert tree is not None


class TestValidation:
    """Test query validation functionality."""

    def test_validate_valid_query(self, parser):
        """Test that valid queries are validated correctly."""
        query = "MATCH (n) RETURN n"
        assert parser.validate(query) is True

    def test_validate_invalid_query(self, parser):
        """Test that invalid queries are rejected."""
        query = "MATCH (n RETURN n"  # Missing closing parenthesis
        assert parser.validate(query) is False


class TestComments:
    """Test comment handling."""

    def test_single_line_comment(self, parser):
        """Test parsing query with single-line comment."""
        query = """
        // This is a comment
        MATCH (n) RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multi_line_comment(self, parser):
        """Test parsing query with multi-line comment."""
        query = """
        /* This is a
           multi-line comment */
        MATCH (n) RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None


class TestAdvancedPatterns:
    """Test advanced pattern matching features."""

    def test_variable_length_relationship(self, parser):
        """Test parsing variable-length relationships."""
        query = "MATCH (a)-[r*1..5]->(b) RETURN a, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_variable_length_unbounded(self, parser):
        """Test parsing unbounded variable-length relationships."""
        query = "MATCH (a)-[r*]->(b) RETURN a, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_bidirectional_relationship(self, parser):
        """Test parsing bidirectional relationships."""
        query = "MATCH (a)-[r]-(b) RETURN a, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_labels(self, parser):
        """Test parsing nodes with multiple labels."""
        query = "MATCH (n:Person:Employee) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_label_or_expression(self, parser):
        """Test parsing label OR expressions."""
        query = "MATCH (n:Person|Employee) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_label_not_expression(self, parser):
        """Test parsing label NOT expressions."""
        query = "MATCH (n:!Person) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_label_wildcard(self, parser):
        """Test parsing label wildcard."""
        query = "MATCH (n:%) RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_relationship_type_or(self, parser):
        """Test parsing relationship type with OR using |: syntax."""
        query = "MATCH (a)-[r:KNOWS|:LIKES]->(b) RETURN a, b"
        tree = parser.parse(query)
        assert tree is not None

    def test_shortest_path(self, parser):
        """Test parsing SHORTESTPATH function."""
        query = "MATCH p = shortestPath((a:Person)-[*]-(b:Person)) RETURN p"
        tree = parser.parse(query)
        assert tree is not None

    def test_all_shortest_paths(self, parser):
        """Test parsing ALLSHORTESTPATHS function."""
        query = "MATCH p = allShortestPaths((a)-[*]-(b)) RETURN p"
        tree = parser.parse(query)
        assert tree is not None


class TestExistsSubqueries:
    """Test EXISTS subquery parsing."""

    def test_exists_pattern(self, parser):
        """Test EXISTS with pattern."""
        query = "MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->(:Person) } RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_exists_match_where(self, parser):
        """Test EXISTS with MATCH and WHERE."""
        query = """
        MATCH (n:Person)
        WHERE EXISTS {
            MATCH (n)-[:KNOWS]->(friend)
            WHERE friend.age > 30
        }
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    # Note: COUNT subquery is not yet fully supported in the grammar
    # def test_count_subquery(self, parser):
    #     """Test COUNT subquery."""
    #     query = """
    #     MATCH (n:Person)
    #     WHERE COUNT { (n)-[:KNOWS]->() } > 5
    #     RETURN n
    #     """
    #     tree = parser.parse(query)
    #     assert tree is not None


class TestComprehensions:
    """Test list and pattern comprehension parsing."""

    def test_list_comprehension_simple(self, parser):
        """Test simple list comprehension."""
        query = "RETURN [x IN [1,2,3] | x * 2]"
        tree = parser.parse(query)
        assert tree is not None

    def test_list_comprehension_with_where(self, parser):
        """Test list comprehension with WHERE clause."""
        query = "RETURN [x IN [1,2,3,4,5] WHERE x > 2 | x * 2]"
        tree = parser.parse(query)
        assert tree is not None

    def test_pattern_comprehension(self, parser):
        """Test pattern comprehension."""
        query = """
        MATCH (person:Person)
        RETURN [path = (person)-[:KNOWS]->(friend) | friend.name]
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_nested_comprehension(self, parser):
        """Test nested list comprehension."""
        query = "RETURN [x IN [1,2,3] | [y IN [4,5,6] | x + y]]"
        tree = parser.parse(query)
        assert tree is not None


class TestMapProjections:
    """Test map projection parsing."""

    def test_map_projection_simple(self, parser):
        """Test simple map projection."""
        query = "MATCH (n:Person) RETURN n{.name, .age}"
        tree = parser.parse(query)
        assert tree is not None

    def test_map_projection_computed(self, parser):
        """Test map projection with computed properties."""
        query = "MATCH (n:Person) RETURN n{.name, birthYear: 2024 - n.age}"
        tree = parser.parse(query)
        assert tree is not None

    def test_map_projection_all_properties(self, parser):
        """Test map projection with all properties."""
        query = "MATCH (n:Person) RETURN n{.*}"
        tree = parser.parse(query)
        assert tree is not None

    def test_map_projection_mixed(self, parser):
        """Test map projection with mixed syntax."""
        query = "MATCH (n:Person) RETURN n{.*, computed: n.age * 2}"
        tree = parser.parse(query)
        assert tree is not None


class TestQuantifiers:
    """Test quantifier expression parsing."""

    def test_all_quantifier(self, parser):
        """Test ALL quantifier."""
        query = "RETURN ALL(x IN [1,2,3] WHERE x > 0)"
        tree = parser.parse(query)
        assert tree is not None

    def test_any_quantifier(self, parser):
        """Test ANY quantifier."""
        query = "RETURN ANY(x IN [1,2,3] WHERE x > 2)"
        tree = parser.parse(query)
        assert tree is not None

    def test_none_quantifier(self, parser):
        """Test NONE quantifier."""
        query = "RETURN NONE(x IN [1,2,3] WHERE x < 0)"
        tree = parser.parse(query)
        assert tree is not None

    def test_single_quantifier(self, parser):
        """Test SINGLE quantifier."""
        query = "RETURN SINGLE(x IN [1,2,3] WHERE x = 2)"
        tree = parser.parse(query)
        assert tree is not None


class TestReduceExpressions:
    """Test REDUCE expression parsing."""

    def test_reduce_simple(self, parser):
        """Test simple REDUCE expression."""
        query = "RETURN REDUCE(sum = 0, x IN [1,2,3] | sum + x)"
        tree = parser.parse(query)
        assert tree is not None

    def test_reduce_complex(self, parser):
        """Test complex REDUCE expression."""
        query = """
        MATCH p = (a)-[*]->(b)
        RETURN REDUCE(totalCost = 0, r IN relationships(p) | totalCost + r.cost)
        """
        tree = parser.parse(query)
        assert tree is not None


class TestCaseExpressions:
    """Test CASE expression parsing."""

    def test_case_simple(self, parser):
        """Test simple CASE expression."""
        query = """
        MATCH (n:Person)
        RETURN CASE n.gender
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'Other'
        END
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_case_searched(self, parser):
        """Test searched CASE expression."""
        query = """
        MATCH (n:Person)
        RETURN CASE
            WHEN n.age < 18 THEN 'Minor'
            WHEN n.age < 65 THEN 'Adult'
            ELSE 'Senior'
        END AS ageGroup
        """
        tree = parser.parse(query)
        assert tree is not None


class TestStringPredicates:
    """Test string predicate parsing."""

    def test_starts_with(self, parser):
        """Test STARTS WITH predicate."""
        query = "MATCH (n:Person) WHERE n.name STARTS WITH 'Al' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_ends_with(self, parser):
        """Test ENDS WITH predicate."""
        query = "MATCH (n:Person) WHERE n.name ENDS WITH 'ice' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_contains(self, parser):
        """Test CONTAINS predicate."""
        query = "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_regex_match(self, parser):
        """Test regex match with =~."""
        query = "MATCH (n:Person) WHERE n.email =~ '.*@example\\.com' RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_is_null(self, parser):
        """Test IS NULL predicate."""
        query = "MATCH (n:Person) WHERE n.middleName IS NULL RETURN n"
        tree = parser.parse(query)
        assert tree is not None

    def test_is_not_null(self, parser):
        """Test IS NOT NULL predicate."""
        query = "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n"
        tree = parser.parse(query)
        assert tree is not None
    
    def test_is_null_in_complex_expression(self, parser):
        """Test IS NULL in complex expression."""
        query = "MATCH (n) WHERE n.prop1 IS NULL OR n.prop2 IS NOT NULL RETURN n"
        tree = parser.parse(query)
        assert tree is not None


class TestAdvancedLiterals:
    """Test advanced literal parsing."""

    def test_hexadecimal(self, parser):
        """Test hexadecimal literal."""
        query = "RETURN 0x1A2B"
        tree = parser.parse(query)
        assert tree is not None

    def test_octal(self, parser):
        """Test octal literal."""
        query = "RETURN 0o755"
        tree = parser.parse(query)
        assert tree is not None

    def test_scientific_notation(self, parser):
        """Test scientific notation."""
        query = "RETURN 1.5e10"
        tree = parser.parse(query)
        assert tree is not None

    def test_infinity(self, parser):
        """Test INF literal."""
        query = "RETURN INF"
        tree = parser.parse(query)
        assert tree is not None

    def test_nan(self, parser):
        """Test NaN literal."""
        query = "RETURN NaN"
        tree = parser.parse(query)
        assert tree is not None


class TestArrayOperations:
    """Test array operation parsing."""

    def test_array_indexing(self, parser):
        """Test array indexing."""
        query = "RETURN [1,2,3,4,5][0]"
        tree = parser.parse(query)
        assert tree is not None

    def test_array_slicing(self, parser):
        """Test array slicing."""
        query = "RETURN [1,2,3,4,5][1..3]"
        tree = parser.parse(query)
        assert tree is not None

    def test_array_slicing_open_ended(self, parser):
        """Test open-ended array slicing."""
        query = "RETURN [1,2,3,4,5][2..]"
        tree = parser.parse(query)
        assert tree is not None

    def test_string_indexing(self, parser):
        """Test string indexing."""
        query = "RETURN 'hello'[0]"
        tree = parser.parse(query)
        assert tree is not None


class TestCallStatements:
    """Test CALL statement parsing."""

    def test_call_simple(self, parser):
        """Test simple CALL statement."""
        query = "CALL db.labels()"
        tree = parser.parse(query)
        assert tree is not None

    def test_call_with_yield(self, parser):
        """Test CALL with YIELD."""
        query = "CALL db.labels() YIELD label"
        tree = parser.parse(query)
        assert tree is not None

    def test_call_with_yield_where(self, parser):
        """Test CALL with YIELD and WHERE."""
        query = """
        CALL db.labels() YIELD label
        WHERE label STARTS WITH 'Person'
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_standalone_call(self, parser):
        """Test standalone CALL without YIELD."""
        query = "CALL dbms.clearQueryCaches()"
        tree = parser.parse(query)
        assert tree is not None


class TestAdvancedFunctions:
    """Test advanced function parsing."""

    def test_namespaced_function(self, parser):
        """Test namespaced function."""
        query = "RETURN apoc.text.join(['hello', 'world'], ' ')"
        tree = parser.parse(query)
        assert tree is not None

    def test_power_operator(self, parser):
        """Test power operator (^)."""
        query = "RETURN 2 ^ 8"
        tree = parser.parse(query)
        assert tree is not None

    def test_nested_function_calls(self, parser):
        """Test nested function calls."""
        query = "RETURN toUpper(substring('hello world', 0, 5))"
        tree = parser.parse(query)
        assert tree is not None


class TestComplexQueriesAdvanced:
    """Test very complex query patterns combining multiple advanced features."""

    def test_complex_aggregation(self, parser):
        """Test complex aggregation with grouping."""
        query = """
        MATCH (person:Person)-[:WORKS_AT]->(company:Company)
        WITH company.name AS companyName, 
             avg(person.salary) AS avgSalary,
             count(person) AS employeeCount
        WHERE avgSalary > 50000
        RETURN companyName, avgSalary, employeeCount
        ORDER BY avgSalary DESC
        LIMIT 10
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_pattern_with_exists(self, parser):
        """Test complex pattern matching with EXISTS."""
        query = """
        MATCH (person:Person)
        WHERE person.age > 25
          AND EXISTS { (person)-[:KNOWS]->(:Person {country: 'USA'}) }
          AND NOT EXISTS { (person)-[:BLOCKED]->() }
        RETURN person.name, person.age
        ORDER BY person.age DESC
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_union_query(self, parser):
        """Test UNION query."""
        query = """
        MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name
        UNION
        MATCH (p:Person) WHERE p.age > 60 RETURN p.name AS name
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_create_pattern(self, parser):
        """Test complex CREATE with multiple patterns."""
        query = """
        CREATE (alice:Person:Employee {name: 'Alice', age: 30}),
               (bob:Person {name: 'Bob', age: 35}),
               (company:Company {name: 'TechCorp'}),
               (alice)-[:WORKS_AT {since: 2020}]->(company),
               (bob)-[:WORKS_AT {since: 2018}]->(company),
               (alice)-[:KNOWS {since: 2019}]->(bob)
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_merge_with_on(self, parser):
        """Test MERGE with ON CREATE and ON MATCH."""
        query = """
        MERGE (person:Person {id: 123})
        ON CREATE SET person.created = timestamp(), person.visits = 1
        ON MATCH SET person.visits = person.visits + 1, person.lastSeen = timestamp()
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_very_complex_graph_traversal(self, parser):
        """Test very complex graph traversal with multiple features."""
        query = """
        MATCH path = (start:Person {name: 'Alice'})-[rels:KNOWS*1..5]->(end:Person)
        WHERE ALL(r IN rels WHERE r.trust > 0.5)
          AND NONE(n IN nodes(path)[1..-1] WHERE n.blocked = true)
          AND EXISTS { (end)-[:LIVES_IN]->(:City {name: 'Boston'}) }
        WITH end, 
             [r IN rels | r.trust] AS trustScores,
             length(path) AS pathLength,
             REDUCE(totalTrust = 1.0, r IN rels | totalTrust * r.trust) AS trustProduct
        WHERE trustProduct > 0.1
        RETURN end.name AS person,
               pathLength,
               trustProduct,
               end{.age, .occupation, .email} AS details
        ORDER BY trustProduct DESC, pathLength ASC
        LIMIT 20
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_data_transformation(self, parser):
        """Test complex data transformation with comprehensions."""
        query = """
        MATCH (person:Person)
        OPTIONAL MATCH (person)-[:KNOWS]->(friend:Person)
        OPTIONAL MATCH (person)-[:WORKS_AT]->(job)
        WITH person,
             [f IN collect(friend) WHERE f.age > 25 | f{.name, .age}] AS adultFriends,
             count(job) AS jobCount
        WHERE size(adultFriends) > 2
        RETURN person.name,
               adultFriends,
               jobCount,
               CASE
                   WHEN jobCount = 0 THEN 'Unemployed'
                   WHEN jobCount = 1 THEN 'Single Job'
                   ELSE 'Multiple Jobs'
               END AS employmentStatus
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_complex_recommendation_query(self, parser):
        """Test recommendation algorithm query."""
        query = """
        MATCH (user:Person {id: $userId})-[:LIKES]->(item:Product)
        MATCH (item)<-[:LIKES]-(other:Person)-[:LIKES]->(recommendation:Product)
        WHERE NOT EXISTS { (user)-[:LIKES]->(recommendation) }
          AND recommendation.available = true
        WITH recommendation,
             COUNT(DISTINCT other) AS commonUsers,
             AVG(other.trustScore) AS avgTrustScore,
             COLLECT(DISTINCT item.category) AS likedCategories
        WHERE commonUsers >= 3
          AND ANY(cat IN recommendation.categories WHERE cat IN likedCategories)
        RETURN recommendation.name,
               recommendation.price,
               commonUsers,
               avgTrustScore,
               recommendation.rating AS productRating,
               (commonUsers * avgTrustScore * recommendation.rating) AS score
        ORDER BY score DESC
        LIMIT 10
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_multi_hop_pattern_with_quantifiers(self, parser):
        """Test multi-hop pattern with quantifiers and comprehensions."""
        query = """
        MATCH (company:Company)<-[:WORKS_AT]-(employee:Person)
        WHERE company.industry = 'Technology'
          AND SINGLE(skill IN employee.skills WHERE skill = 'Python')
        WITH company,
             [p IN COLLECT(employee) WHERE p.experience > 5 | 
                 p{.name, .title, yearsExp: p.experience}] AS seniorDevs
        WHERE size(seniorDevs) >= 10
        RETURN company.name,
               size(seniorDevs) AS seniorCount,
               seniorDevs[0..5] AS topFive
        """
        tree = parser.parse(query)
        assert tree is not None


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_list(self, parser):
        """Test empty list literal."""
        query = "RETURN []"
        tree = parser.parse(query)
        assert tree is not None

    def test_empty_map(self, parser):
        """Test empty map literal."""
        query = "RETURN {}"
        tree = parser.parse(query)
        assert tree is not None

    def test_nested_lists(self, parser):
        """Test deeply nested lists."""
        query = "RETURN [[1, 2], [3, [4, 5]], [[[6]]]]"
        tree = parser.parse(query)
        assert tree is not None

    def test_nested_maps(self, parser):
        """Test nested maps."""
        query = "RETURN {outer: {inner: {deep: 'value'}}}"
        tree = parser.parse(query)
        assert tree is not None

    def test_property_chain(self, parser):
        """Test long property chain."""
        query = "RETURN n.address.city.name"
        tree = parser.parse(query)
        assert tree is not None

    def test_multiple_predicates(self, parser):
        """Test multiple WHERE predicates."""
        query = """
        MATCH (n:Person)
        WHERE n.age > 18
          AND n.age < 65
          AND n.active = true
          AND n.country IN ['USA', 'UK', 'Canada']
          AND n.salary >= 50000
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None
    
    def test_multiple_predicates_with_null_checks(self, parser):
        """Test multiple WHERE predicates including IS NULL checks."""
        query = """
        MATCH (n:Person)
        WHERE n.age > 18
          AND n.email IS NOT NULL
          AND n.middleName IS NULL
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_escaped_strings(self, parser):
        """Test escaped strings."""
        query = """RETURN 'It\\'s a beautiful day'"""
        tree = parser.parse(query)
        assert tree is not None

    def test_unicode_in_strings(self, parser):
        """Test unicode characters in strings."""
        query = "RETURN 'Hello 世界 🌍'"
        tree = parser.parse(query)
        assert tree is not None


class TestPerformanceQueries:
    """Test queries that might stress the parser."""

    def test_many_return_items(self, parser):
        """Test RETURN with many items."""
        query = """
        MATCH (n:Person)
        RETURN n.name, n.age, n.email, n.phone, n.address, 
               n.city, n.state, n.zip, n.country, n.occupation,
               n.salary, n.department, n.manager, n.startDate, n.active
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_many_where_conditions(self, parser):
        """Test WHERE with many OR conditions."""
        query = """
        MATCH (n:Person)
        WHERE n.id = 1 OR n.id = 2 OR n.id = 3 OR n.id = 4 OR n.id = 5
           OR n.id = 6 OR n.id = 7 OR n.id = 8 OR n.id = 9 OR n.id = 10
        RETURN n
        """
        tree = parser.parse(query)
        assert tree is not None

    def test_deeply_nested_expression(self, parser):
        """Test deeply nested arithmetic expression."""
        query = "RETURN ((((1 + 2) * 3) - 4) / 5) ^ 6"
        tree = parser.parse(query)
        assert tree is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
