"""Extended tests for grammar_parser module - covering missing test coverage.

This module contains additional unit tests for GrammarParser methods and transformer
edge cases that were not covered in test_grammar_parser.py.
"""

import pytest
import tempfile
import os
import sys
from io import StringIO
from pathlib import Path
from pycypher.grammar_parser import GrammarParser, main
from lark.exceptions import LarkError, UnexpectedInput


@pytest.fixture
def parser():
    """Create a GrammarParser instance for testing."""
    return GrammarParser()


class TestValidateMethod:
    """Test the validate() method with various edge cases."""

    def test_validate_valid_query(self, parser):
        """Test validation with valid query."""
        assert parser.validate("MATCH (n) RETURN n") is True

    def test_validate_invalid_query(self, parser):
        """Test validation with invalid query."""
        assert parser.validate("MATCH (n RETURN n") is False

    def test_validate_empty_string(self, parser):
        """Test validation with empty string."""
        assert parser.validate("") is False

    def test_validate_whitespace_only(self, parser):
        """Test validation with whitespace-only string."""
        assert parser.validate("   \n\t  ") is False

    def test_validate_unclosed_string_single_quote(self, parser):
        """Test validation with unclosed single-quoted string."""
        assert parser.validate("RETURN 'hello") is False

    def test_validate_unclosed_string_double_quote(self, parser):
        """Test validation with unclosed double-quoted string."""
        assert parser.validate('RETURN "hello') is False

    def test_validate_unclosed_comment_multiline(self, parser):
        """Test validation with unclosed multiline comment."""
        assert parser.validate("/* This is a comment RETURN 42") is False

    def test_validate_incomplete_statement(self, parser):
        """Test validation with incomplete statement."""
        assert parser.validate("MATCH (n) WHERE") is False

    def test_validate_multiple_statements_union(self, parser):
        """Test validation with multiple UNION statements."""
        query = """
        MATCH (n:Person) RETURN n
        UNION
        MATCH (m:Employee) RETURN m
        """
        assert parser.validate(query) is True

    def test_validate_complex_valid_query(self, parser):
        """Test validation with complex but valid query."""
        query = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.age > 25 AND f.age < 50
        WITH p, f, COUNT(*) as connectionCount
        ORDER BY connectionCount DESC
        LIMIT 10
        RETURN p.name, f.name, connectionCount
        """
        assert parser.validate(query) is True


class TestFileIO:
    """Test file I/O methods: parse_file() and parse_file_to_ast()."""

    def test_parse_file_valid(self, parser):
        """Test parsing from a valid file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cypher', delete=False) as f:
            f.write("MATCH (n:Person) RETURN n")
            temp_path = f.name

        try:
            tree = parser.parse_file(temp_path)
            assert tree is not None
        finally:
            os.unlink(temp_path)

    def test_parse_file_to_ast_valid(self, parser):
        """Test parsing file directly to AST."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cypher', delete=False) as f:
            f.write("MATCH (n:Person {name: 'Alice'}) RETURN n.age")
            temp_path = f.name

        try:
            ast = parser.parse_file_to_ast(temp_path)
            assert ast is not None
            assert isinstance(ast, dict)
            assert ast.get("type") == "Query"
        finally:
            os.unlink(temp_path)

    def test_parse_file_not_found(self, parser):
        """Test parsing non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parser.parse_file("/nonexistent/path/query.cypher")

    def test_parse_file_empty_file(self, parser):
        """Test parsing empty file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cypher', delete=False) as f:
            temp_path = f.name

        try:
            with pytest.raises(LarkError):
                parser.parse_file(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_file_with_path_object(self, parser):
        """Test parsing with Path object."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cypher', delete=False) as f:
            f.write("RETURN 42")
            temp_path = Path(f.name)

        try:
            tree = parser.parse_file(temp_path)
            assert tree is not None
        finally:
            temp_path.unlink()

    def test_parse_file_multiline_query(self, parser):
        """Test parsing file with multi-line query."""
        query = """
        // This is a comment
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name, p.age
        ORDER BY p.age DESC
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cypher', delete=False) as f:
            f.write(query)
            temp_path = f.name

        try:
            tree = parser.parse_file(temp_path)
            assert tree is not None
        finally:
            os.unlink(temp_path)


class TestTransformerEdgeCases:
    """Test edge cases in transformer methods."""

    def test_parameter_numeric(self, parser):
        """Test numeric parameter like $0, $1."""
        query = "RETURN $1"
        ast = parser.parse_to_ast(query)
        assert ast is not None
        # Should parse without error

    def test_parameter_named(self, parser):
        """Test named parameter like $name."""
        query = "RETURN $userName, $userAge"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_variable_name_with_backticks(self, parser):
        """Test variable name with backticks for reserved words."""
        query = "MATCH (`match`:Person) RETURN `match`.name"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_variable_name_with_special_chars(self, parser):
        """Test variable name with special characters in backticks."""
        query = "MATCH (`my-variable`:Person) RETURN `my-variable`"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_path_length_exact(self, parser):
        """Test exact path length [*5]."""
        query = "MATCH (a)-[*5]->(b) RETURN a, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_path_length_minimum(self, parser):
        """Test minimum path length [*3..]."""
        query = "MATCH (a)-[*3..]->(b) RETURN a, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_path_length_maximum(self, parser):
        """Test maximum path length [*..7]."""
        query = "MATCH (a)-[*..7]->(b) RETURN a, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_path_length_unbounded(self, parser):
        """Test unbounded path length [*]."""
        query = "MATCH (a)-[*]->(b) RETURN a, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_label_or_expression(self, parser):
        """Test label OR expression like :Person|Employee."""
        query = "MATCH (n:Person|Employee) RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_predicate_starts_with(self, parser):
        """Test STARTS WITH string predicate."""
        query = "MATCH (n) WHERE n.name STARTS WITH 'Al' RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_predicate_ends_with(self, parser):
        """Test ENDS WITH string predicate."""
        query = "MATCH (n) WHERE n.email ENDS WITH '@example.com' RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_predicate_contains(self, parser):
        """Test CONTAINS string predicate."""
        query = "MATCH (n) WHERE n.description CONTAINS 'important' RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_predicate_regex(self, parser):
        """Test regex string predicate =~."""
        query = "MATCH (n) WHERE n.email =~ '.*@example\\.com' RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_predicate_in_operator(self, parser):
        """Test IN operator."""
        query = "MATCH (n) WHERE n.status IN ['active', 'pending'] RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_null_check_is_null(self, parser):
        """Test IS NULL operator."""
        query = "MATCH (n) WHERE n.deletedAt IS NULL RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_null_check_is_not_null(self, parser):
        """Test IS NOT NULL operator."""
        query = "MATCH (n) WHERE n.email IS NOT NULL RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_function_no_arguments(self, parser):
        """Test function with no arguments like timestamp()."""
        query = "RETURN timestamp()"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_function_namespaced(self, parser):
        """Test namespaced function like apoc.text.join()."""
        query = "RETURN apoc.text.join(['a', 'b'], ', ')"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_function_nested(self, parser):
        """Test nested function calls."""
        query = "RETURN toUpper(substring('hello', 0, 3))"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_function_distinct_arguments(self, parser):
        """Test function with DISTINCT modifier."""
        query = "MATCH (n) RETURN count(DISTINCT n.category)"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestCaseExpressionEdgeCases:
    """Test CASE expression edge cases."""

    def test_searched_case_no_else(self, parser):
        """Test CASE without ELSE clause."""
        query = "RETURN CASE WHEN 1 > 2 THEN 'yes' WHEN 2 > 1 THEN 'no' END"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_simple_case_with_multiple_when(self, parser):
        """Test simple CASE with multiple WHEN clauses."""
        query = """
        RETURN CASE n.status
            WHEN 'active' THEN 1
            WHEN 'pending' THEN 2
            WHEN 'inactive' THEN 3
            ELSE 0
        END
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_case_nested(self, parser):
        """Test nested CASE expressions."""
        query = """
        RETURN CASE WHEN n.age > 18
            THEN CASE WHEN n.age < 65 THEN 'adult' ELSE 'senior' END
            ELSE 'minor'
        END
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_case_with_complex_expression(self, parser):
        """Test CASE with complex expressions in WHEN."""
        query = """
        RETURN CASE
            WHEN n.score > 90 AND n.attendance > 0.8 THEN 'A'
            WHEN n.score > 80 OR n.bonus > 10 THEN 'B'
            ELSE 'C'
        END
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestComprehensionEdgeCases:
    """Test list and pattern comprehension edge cases."""

    def test_list_comprehension_no_filter(self, parser):
        """Test list comprehension without WHERE clause."""
        query = "RETURN [x IN [1, 2, 3] | x * 2]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_comprehension_no_projection(self, parser):
        """Test list comprehension without projection (|)."""
        query = "RETURN [x IN [1, 2, 3] WHERE x > 1]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_comprehension_complex_expression(self, parser):
        """Test list comprehension with complex projection."""
        query = "RETURN [x IN [1, 2, 3] | {value: x, squared: x * x}]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_pattern_comprehension_basic(self, parser):
        """Test basic pattern comprehension."""
        query = "MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f) | f.name]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_pattern_comprehension_with_where(self, parser):
        """Test pattern comprehension with WHERE clause."""
        query = """
        MATCH (p:Person)
        RETURN [(p)-[:KNOWS]->(f) WHERE f.age > 30 | f.name]
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_pattern_comprehension_complex_projection(self, parser):
        """Test pattern comprehension with complex projection."""
        query = """
        MATCH (p:Person)
        RETURN [(p)-[r:KNOWS]->(f) | {name: f.name, since: r.since}]
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestQuantifierEdgeCases:
    """Test quantifier expression edge cases."""

    def test_quantifier_all(self, parser):
        """Test ALL quantifier."""
        query = "RETURN ALL(x IN [1, 2, 3] WHERE x > 0)"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_quantifier_any(self, parser):
        """Test ANY quantifier."""
        query = "RETURN ANY(x IN [1, 2, 3] WHERE x > 2)"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_quantifier_none(self, parser):
        """Test NONE quantifier."""
        query = "RETURN NONE(x IN [1, 2, 3] WHERE x < 0)"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_quantifier_single(self, parser):
        """Test SINGLE quantifier."""
        query = "RETURN SINGLE(x IN [1, 2, 3] WHERE x = 2)"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_quantifier_complex_predicate(self, parser):
        """Test quantifier with complex WHERE predicate."""
        query = "RETURN ALL(x IN [1, 2, 3] WHERE x > 0 AND x < 10)"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestReduceExpression:
    """Test REDUCE expression."""

    def test_reduce_basic(self, parser):
        """Test basic REDUCE expression."""
        query = "RETURN REDUCE(sum = 0, x IN [1, 2, 3] | sum + x)"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_reduce_complex_expression(self, parser):
        """Test REDUCE with complex expression."""
        query = """
        RETURN REDUCE(product = 1, x IN [2, 3, 4] | product * x)
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_reduce_with_string_accumulator(self, parser):
        """Test REDUCE with string accumulator."""
        query = """
        RETURN REDUCE(str = '', x IN ['a', 'b', 'c'] | str + x)
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestMapProjection:
    """Test map projection edge cases."""

    def test_map_projection_basic(self, parser):
        """Test basic map projection."""
        query = "MATCH (n:Person) RETURN n{.name, .age}"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_map_projection_all_properties(self, parser):
        """Test map projection with .* (all properties)."""
        query = "MATCH (n:Person) RETURN n{.*}"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_map_projection_mixed(self, parser):
        """Test map projection with mix of .* and specific properties."""
        query = "MATCH (n:Person) RETURN n{.*, computed: n.age * 2}"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_map_projection_computed_only(self, parser):
        """Test map projection with only computed properties."""
        query = "MATCH (n:Person) RETURN n{fullName: n.firstName + ' ' + n.lastName}"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestNumberLiterals:
    """Test number literal edge cases."""

    def test_number_hex(self, parser):
        """Test hexadecimal number literal."""
        query = "RETURN 0xFF"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_octal(self, parser):
        """Test octal number literal."""
        query = "RETURN 0o77"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_scientific_notation(self, parser):
        """Test scientific notation."""
        query = "RETURN 1.5e10"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_infinity(self, parser):
        """Test infinity literal."""
        query = "RETURN Infinity"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_negative_infinity(self, parser):
        """Test negative infinity literal."""
        query = "RETURN -Infinity"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_nan(self, parser):
        """Test NaN literal."""
        query = "RETURN NaN"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_number_with_underscores(self, parser):
        """Test number with underscores for readability."""
        query = "RETURN 1_000_000"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestStringLiterals:
    """Test string literal edge cases."""

    def test_string_escaped_quotes_single(self, parser):
        """Test string with escaped single quotes."""
        query = "RETURN 'It\\'s a test'"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_escaped_quotes_double(self, parser):
        """Test string with escaped double quotes."""
        query = 'RETURN "He said \\"hello\\""'
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_escaped_newline(self, parser):
        """Test string with escaped newline."""
        query = "RETURN 'Line 1\\nLine 2'"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_escaped_tab(self, parser):
        """Test string with escaped tab."""
        query = "RETURN 'Column1\\tColumn2'"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_string_empty(self, parser):
        """Test empty string."""
        query = "RETURN ''"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestExistsExpression:
    """Test EXISTS expression."""

    def test_exists_simple_pattern(self, parser):
        """Test EXISTS with simple pattern."""
        query = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_exists_with_match(self, parser):
        """Test EXISTS with explicit MATCH."""
        query = """
        MATCH (p:Person)
        WHERE EXISTS { MATCH (p)-[:KNOWS]->(f) WHERE f.age > 30 }
        RETURN p
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_exists_complex_subquery(self, parser):
        """Test EXISTS with complex subquery."""
        query = """
        MATCH (p:Person)
        WHERE EXISTS {
            MATCH (p)-[:WORKS_AT]->(c:Company)
            WHERE c.revenue > 1000000
        }
        RETURN p
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestMergeStatement:
    """Test MERGE statement edge cases."""

    def test_merge_on_create(self, parser):
        """Test MERGE with ON CREATE."""
        query = """
        MERGE (n:Person {id: 123})
        ON CREATE SET n.created = timestamp()
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_merge_on_match(self, parser):
        """Test MERGE with ON MATCH."""
        query = """
        MERGE (n:Person {id: 123})
        ON MATCH SET n.lastSeen = timestamp()
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_merge_on_create_and_match(self, parser):
        """Test MERGE with both ON CREATE and ON MATCH."""
        query = """
        MERGE (n:Person {id: 123})
        ON CREATE SET n.created = timestamp()
        ON MATCH SET n.updated = timestamp()
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestSetStatementVariations:
    """Test SET statement variations."""

    def test_set_multiple_labels(self, parser):
        """Test SET with multiple labels."""
        query = "MATCH (n) SET n:Person:Employee"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_set_all_properties(self, parser):
        """Test SET replacing all properties."""
        query = "MATCH (n) SET n = {name: 'Alice', age: 30}"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_set_add_properties(self, parser):
        """Test SET adding properties with +=."""
        query = "MATCH (n) SET n += {email: 'alice@example.com'}"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_set_multiple_operations(self, parser):
        """Test SET with multiple operations."""
        query = """
        MATCH (n:Person)
        SET n.age = 31, n.updated = timestamp(), n:Active
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestRemoveStatement:
    """Test REMOVE statement edge cases."""

    def test_remove_multiple_labels(self, parser):
        """Test REMOVE with multiple labels."""
        query = "MATCH (n) REMOVE n:Inactive:Pending"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_remove_mixed_properties_and_labels(self, parser):
        """Test REMOVE with both properties and labels."""
        query = "MATCH (n) REMOVE n.temporary, n:Draft"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestCallStatement:
    """Test CALL statement."""

    def test_call_simple_procedure(self, parser):
        """Test simple CALL statement."""
        query = "CALL db.labels()"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_call_with_yield(self, parser):
        """Test CALL with YIELD."""
        query = "CALL db.labels() YIELD label"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_call_yield_with_alias(self, parser):
        """Test CALL YIELD with alias."""
        query = "CALL db.labels() YIELD label AS labelName"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_call_yield_multiple_fields(self, parser):
        """Test CALL YIELD with multiple fields."""
        query = """
        CALL db.relationshipTypes()
        YIELD relationshipType AS type
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_call_in_query(self, parser):
        """Test CALL within larger query."""
        query = """
        CALL db.labels() YIELD label
        RETURN label
        ORDER BY label
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestUnwindStatement:
    """Test UNWIND statement edge cases."""

    def test_unwind_simple_list(self, parser):
        """Test UNWIND with simple list."""
        query = "UNWIND [1, 2, 3] AS num RETURN num"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_unwind_nested_list(self, parser):
        """Test UNWIND with nested list."""
        query = "UNWIND [[1, 2], [3, 4]] AS pair RETURN pair"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_unwind_parameter(self, parser):
        """Test UNWIND with parameter."""
        query = "UNWIND $items AS item RETURN item"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestWithClause:
    """Test WITH clause edge cases."""

    def test_with_distinct(self, parser):
        """Test WITH DISTINCT."""
        query = """
        MATCH (n:Person)
        WITH DISTINCT n.category AS category
        RETURN category
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_with_aggregation(self, parser):
        """Test WITH with aggregation."""
        query = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        WHERE population > 1000
        RETURN c.name, population
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_with_order_skip_limit(self, parser):
        """Test WITH with ORDER BY, SKIP, and LIMIT."""
        query = """
        MATCH (n:Person)
        WITH n
        ORDER BY n.age DESC
        SKIP 10
        LIMIT 5
        RETURN n.name
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestComplexPatterns:
    """Test complex pattern matching."""

    def test_multiple_relationship_types(self, parser):
        """Test relationship with multiple types."""
        query = "MATCH (a)-[:KNOWS|:LIKES|:FOLLOWS]->(b) RETURN a, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_relationship_with_properties_and_range(self, parser):
        """Test relationship with both properties and variable length."""
        query = """
        MATCH (a)-[r:KNOWS {since: 2020} *1..3]->(b)
        RETURN a, b
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_shortest_path(self, parser):
        """Test SHORTESTPATH function."""
        query = """
        MATCH p = shortestPath((a:Person)-[*]-(b:Person))
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN p
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_all_shortest_paths(self, parser):
        """Test ALLSHORTESTPATHS function."""
        query = """
        MATCH p = allShortestPaths((a:Person)-[*]-(b:Person))
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN p
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_named_path(self, parser):
        """Test named path pattern."""
        query = """
        MATCH p = (a:Person)-[:KNOWS*]->(b:Person)
        RETURN p, length(p)
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_node_inline_where(self, parser):
        """Test node pattern with inline WHERE."""
        query = "MATCH (n WHERE n.age > 30) RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_relationship_inline_where(self, parser):
        """Test relationship pattern with inline WHERE."""
        query = "MATCH (a)-[r WHERE r.since > 2020]->(b) RETURN a, r, b"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestArithmeticExpressions:
    """Test arithmetic expression edge cases."""

    def test_power_operator(self, parser):
        """Test power operator ^."""
        query = "RETURN 2 ^ 8"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_modulo_operator(self, parser):
        """Test modulo operator %."""
        query = "RETURN 10 % 3"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_unary_minus(self, parser):
        """Test unary minus."""
        query = "RETURN -42"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_unary_plus(self, parser):
        """Test unary plus."""
        query = "RETURN +42"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_complex_arithmetic(self, parser):
        """Test complex arithmetic expression."""
        query = "RETURN (2 + 3) * 4 - 10 / 2 + 2 ^ 3"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestPropertyAndIndexAccess:
    """Test property and index access."""

    def test_nested_property_access(self, parser):
        """Test nested property access."""
        query = "RETURN n.address.city"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_index_access(self, parser):
        """Test list index access."""
        query = "RETURN [1, 2, 3][0]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_slicing_full(self, parser):
        """Test list slicing with both bounds."""
        query = "RETURN [1, 2, 3, 4, 5][1..3]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_slicing_from_start(self, parser):
        """Test list slicing from start."""
        query = "RETURN [1, 2, 3, 4, 5][..3]"
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_list_slicing_to_end(self, parser):
        """Test list slicing to end."""
        query = "RETURN [1, 2, 3, 4, 5][2..]"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestComments:
    """Test comment handling."""

    def test_single_line_comment(self, parser):
        """Test single-line comment //."""
        query = """
        // This is a comment
        MATCH (n) RETURN n
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_multiline_comment(self, parser):
        """Test multi-line comment /* */."""
        query = """
        /* This is a
           multi-line
           comment */
        MATCH (n) RETURN n
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_comment_inline(self, parser):
        """Test inline comment."""
        query = "MATCH (n) /* get all nodes */ RETURN n"
        ast = parser.parse_to_ast(query)
        assert ast is not None


class TestUnionStatements:
    """Test UNION statements."""

    def test_union_basic(self, parser):
        """Test basic UNION."""
        query = """
        MATCH (n:Person) RETURN n.name AS name
        UNION
        MATCH (c:Company) RETURN c.name AS name
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_union_all(self, parser):
        """Test UNION ALL (keeps duplicates)."""
        query = """
        MATCH (n:Person) RETURN n.name AS name
        UNION ALL
        MATCH (c:Company) RETURN c.name AS name
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None

    def test_multiple_unions(self, parser):
        """Test multiple UNION operations."""
        query = """
        MATCH (p:Person) RETURN p.name AS name
        UNION
        MATCH (c:Company) RETURN c.name AS name
        UNION
        MATCH (ct:City) RETURN ct.name AS name
        """
        ast = parser.parse_to_ast(query)
        assert ast is not None
