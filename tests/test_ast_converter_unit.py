"""Unit tests for ASTConverter — dict-based AST to Pydantic model conversion.

Exercises the converter via from_cypher() and direct convert() calls to cover
the ~60 conversion methods and edge cases.
"""

from __future__ import annotations

from typing import Any

import pytest
from pycypher.ast_converter import ASTConverter, _friendly_parse_error
from pycypher.ast_models import (
    And,
    Arithmetic,
    BooleanLiteral,
    Call,
    CaseExpression,
    Clause,
    Comparison,
    CountStar,
    Create,
    Delete,
    Exists,
    FloatLiteral,
    Foreach,
    FunctionInvocation,
    IndexLookup,
    IntegerLiteral,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    MapLiteral,
    MapProjection,
    Match,
    Merge,
    NodePattern,
    Not,
    NullCheck,
    NullLiteral,
    Or,
    OrderByItem,
    PathLength,
    Pattern,
    PatternComprehension,
    PatternPath,
    PropertyLookup,
    Quantifier,
    Query,
    Reduce,
    RelationshipDirection,
    RelationshipPattern,
    Remove,
    Return,
    ReturnAll,
    ReturnItem,
    Set,
    SetItem,
    Slicing,
    StringLiteral,
    StringPredicate,
    Unary,
    UnionQuery,
    Unwind,
    Variable,
    WhenClause,
    With,
    Xor,
    YieldItem,
)

# ===========================================================================
# from_cypher: end-to-end parsing tests
# ===========================================================================


class TestFromCypher:
    """Tests that exercise the full pipeline: Cypher string -> typed AST."""

    def test_simple_match_return(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n")
        assert isinstance(ast, Query)
        assert len(ast.clauses) == 2
        assert isinstance(ast.clauses[0], Match)
        assert isinstance(ast.clauses[1], Return)

    def test_match_with_where(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause, Match)
        assert match_clause.where is not None

    def test_match_relationship(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
        )
        assert isinstance(ast, Query)
        match_clause = ast.clauses[0]
        assert isinstance(match_clause, Match)
        paths = match_clause.pattern.paths
        assert len(paths) >= 1
        # Should have 3 elements: node-rel-node
        elements = paths[0].elements
        assert len(elements) == 3
        assert isinstance(elements[0], NodePattern)
        assert isinstance(elements[1], RelationshipPattern)
        assert isinstance(elements[2], NodePattern)

    def test_left_direction_relationship(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a"
        )
        match = ast.clauses[0]
        rel = match.pattern.paths[0].elements[1]
        assert isinstance(rel, RelationshipPattern)
        assert rel.direction == RelationshipDirection.LEFT

    def test_undirected_relationship(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a"
        )
        match = ast.clauses[0]
        rel = match.pattern.paths[0].elements[1]
        assert isinstance(rel, RelationshipPattern)
        assert rel.direction == RelationshipDirection.UNDIRECTED

    def test_variable_length_path(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b"
        )
        match = ast.clauses[0]
        rel = match.pattern.paths[0].elements[1]
        assert isinstance(rel, RelationshipPattern)
        assert rel.length is not None
        assert isinstance(rel.length, PathLength)
        assert rel.length.min == 1
        assert rel.length.max == 3

    def test_return_with_alias(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name AS personName"
        )
        ret = ast.clauses[1]
        assert isinstance(ret, Return)
        assert len(ret.items) >= 1

    def test_return_star(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN *")
        ret = ast.clauses[1]
        assert isinstance(ret, Return)

    def test_with_clause(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WITH n.name AS name RETURN name"
        )
        assert isinstance(ast, Query)
        assert any(isinstance(c, With) for c in ast.clauses)

    def test_create_clause(self) -> None:
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice', age: 30})"
        )
        assert isinstance(ast, Query)
        assert any(isinstance(c, Create) for c in ast.clauses)

    def test_merge_clause(self) -> None:
        ast = ASTConverter.from_cypher("MERGE (n:Person {name: 'Alice'})")
        assert isinstance(ast, Query)
        assert any(isinstance(c, Merge) for c in ast.clauses)

    def test_delete_clause(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) DELETE n")
        assert any(isinstance(c, Delete) for c in ast.clauses)

    def test_set_clause(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) SET n.age = 31")
        assert any(isinstance(c, Set) for c in ast.clauses)

    def test_remove_clause(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) REMOVE n.age")
        assert any(isinstance(c, Remove) for c in ast.clauses)

    def test_unwind(self) -> None:
        ast = ASTConverter.from_cypher("UNWIND [1, 2, 3] AS x RETURN x")
        assert any(isinstance(c, Unwind) for c in ast.clauses)

    def test_order_by(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name"
        )
        ret = [c for c in ast.clauses if isinstance(c, Return)][0]
        assert ret.order_by is not None

    def test_limit_skip(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10"
        )
        ret = [c for c in ast.clauses if isinstance(c, Return)][0]
        assert ret.limit is not None
        assert ret.skip is not None

    def test_distinct(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN DISTINCT n.name"
        )
        ret = [c for c in ast.clauses if isinstance(c, Return)][0]
        assert ret.distinct is True

    def test_union(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name "
            "UNION "
            "MATCH (m:Person) RETURN m.name"
        )
        assert isinstance(ast, (UnionQuery, Query))

    def test_optional_match(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person) "
            "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) "
            "RETURN a, b"
        )
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        assert any(m.optional for m in matches)


# ===========================================================================
# Expression parsing tests
# ===========================================================================


class TestExpressionParsing:
    """Tests for expression-level AST conversion."""

    def test_comparison_operators(self) -> None:
        for op in ["=", "<>", "<", ">", "<=", ">="]:
            ast = ASTConverter.from_cypher(
                f"MATCH (n:Person) WHERE n.age {op} 30 RETURN n"
            )
            match_clause = ast.clauses[0]
            assert match_clause.where is not None

    def test_boolean_and(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age > 20 AND n.age < 40 RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, And)

    def test_boolean_or(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age < 20 OR n.age > 40 RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, Or)

    def test_boolean_xor(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age < 20 XOR n.age > 40 RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, Xor)

    def test_not(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE NOT n.age > 30 RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, Not)

    def test_null_check_is_null(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age IS NULL RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, NullCheck)

    def test_null_check_is_not_null(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, NullCheck)

    def test_string_predicate_starts_with(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, StringPredicate)

    def test_string_predicate_contains(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, StringPredicate)

    def test_string_predicate_ends_with(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n"
        )
        match_clause = ast.clauses[0]
        assert isinstance(match_clause.where, StringPredicate)

    def test_arithmetic_operations(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.age + 1")
        ret = ast.clauses[1]
        assert isinstance(ret, Return)

    def test_unary_minus(self) -> None:
        ast = ASTConverter.from_cypher("RETURN -42")
        assert isinstance(ast, Query)

    def test_function_invocation(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN count(n)")
        assert isinstance(ast, Query)

    def test_function_invocation_distinct(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN count(DISTINCT n)"
        )
        assert isinstance(ast, Query)

    def test_property_lookup(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")
        ret = ast.clauses[1]
        assert isinstance(ret, Return)

    def test_list_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN [1, 2, 3]")
        assert isinstance(ast, Query)

    def test_map_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN {a: 1, b: 2}")
        assert isinstance(ast, Query)

    def test_null_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN null")
        assert isinstance(ast, Query)

    def test_string_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN 'hello'")
        assert isinstance(ast, Query)

    def test_integer_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN 42")
        assert isinstance(ast, Query)

    def test_float_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN 3.14")
        assert isinstance(ast, Query)

    def test_boolean_literal(self) -> None:
        ast = ASTConverter.from_cypher("RETURN true")
        assert isinstance(ast, Query)

    def test_case_expression(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'old' ELSE 'young' END"
        )
        assert isinstance(ast, Query)

    def test_exists_subquery(self) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n"
        )
        assert isinstance(ast, Query)

    def test_list_comprehension(self) -> None:
        ast = ASTConverter.from_cypher(
            "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2]"
        )
        assert isinstance(ast, Query)

    def test_index_access(self) -> None:
        ast = ASTConverter.from_cypher("WITH [1, 2, 3] AS list RETURN list[0]")
        assert isinstance(ast, Query)

    def test_slice_access(self) -> None:
        ast = ASTConverter.from_cypher(
            "WITH [1, 2, 3, 4, 5] AS list RETURN list[1..3]"
        )
        assert isinstance(ast, Query)

    def test_count_star(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN count(*)")
        assert isinstance(ast, Query)

    def test_label_predicate(self) -> None:
        ast = ASTConverter.from_cypher("MATCH (n) WHERE n:Person RETURN n")
        match_clause = ast.clauses[0]
        assert match_clause.where is not None


# ===========================================================================
# Direct convert() method tests
# ===========================================================================


class TestConvertMethod:
    """Direct tests for ASTConverter.convert()."""

    def test_convert_none_returns_none(self) -> None:
        c = ASTConverter()
        assert c.convert(None) is None

    def test_convert_int_returns_integer_literal(self) -> None:
        c = ASTConverter()
        result = c.convert(42)
        assert isinstance(result, IntegerLiteral)
        assert result.value == 42

    def test_convert_float_returns_float_literal(self) -> None:
        c = ASTConverter()
        result = c.convert(3.14)
        assert isinstance(result, FloatLiteral)
        assert result.value == 3.14

    def test_convert_bool_returns_boolean_literal(self) -> None:
        c = ASTConverter()
        result = c.convert(True)
        assert isinstance(result, BooleanLiteral)
        assert result.value is True

    def test_convert_string_returns_variable(self) -> None:
        c = ASTConverter()
        result = c.convert("x")
        assert isinstance(result, Variable)
        assert result.name == "x"

    def test_convert_empty_string_returns_none_or_empty(self) -> None:
        c = ASTConverter()
        result = c.convert("")
        # Empty string case: match str() if result -> False for empty
        assert result == ""

    def test_convert_empty_dict_returns_none(self) -> None:
        c = ASTConverter()
        result = c.convert({})
        assert result is None

    def test_convert_dict_with_unknown_type_warns(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "UnknownNodeType12345"})
        # Should return None and log warning
        assert result is None

    def test_convert_query_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "Query",
                "clauses": [],
            }
        )
        assert isinstance(result, Query)

    def test_convert_string_literal_dict(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "StringLiteral", "value": "hello"})
        assert isinstance(result, StringLiteral)
        assert result.value == "hello"

    def test_convert_integer_literal_dict(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "IntegerLiteral", "value": 42})
        assert isinstance(result, IntegerLiteral)
        assert result.value == 42

    def test_convert_null_literal_dict(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "NullLiteral"})
        assert isinstance(result, NullLiteral)

    def test_convert_boolean_literal_dict(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "BooleanLiteral", "value": True})
        assert isinstance(result, BooleanLiteral)

    def test_convert_comparison_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "Comparison",
                "operator": "=",
                "left": {"type": "Variable", "name": "x"},
                "right": {"type": "IntegerLiteral", "value": 1},
            }
        )
        assert isinstance(result, Comparison)

    def test_convert_not_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "Not",
                "expression": {
                    "type": "BooleanLiteral",
                    "value": True,
                },
            }
        )
        assert isinstance(result, Not)

    def test_convert_and_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "And",
                "left": {"type": "BooleanLiteral", "value": True},
                "right": {"type": "BooleanLiteral", "value": False},
            }
        )
        assert isinstance(result, And)

    def test_convert_or_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "Or",
                "left": {"type": "BooleanLiteral", "value": True},
                "right": {"type": "BooleanLiteral", "value": False},
            }
        )
        assert isinstance(result, Or)

    def test_convert_variable_dict(self) -> None:
        c = ASTConverter()
        result = c.convert({"type": "Variable", "name": "n"})
        assert isinstance(result, Variable)
        assert result.name == "n"

    def test_convert_return_item_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "ReturnItem",
                "expression": {"type": "Variable", "name": "n"},
                "alias": "result",
            }
        )
        assert isinstance(result, ReturnItem)

    def test_convert_node_pattern_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "NodePattern",
                "variable": "n",
                "labels": ["Person"],
            }
        )
        assert isinstance(result, NodePattern)

    def test_convert_path_length_dict(self) -> None:
        c = ASTConverter()
        result = c.convert(
            {
                "type": "PathLength",
                "min": 1,
                "max": 3,
                "unbounded": False,
            }
        )
        assert isinstance(result, PathLength)
        assert result.min == 1
        assert result.max == 3


# ===========================================================================
# _convert_primitive tests
# ===========================================================================


class TestConvertPrimitive:
    """Tests for the _convert_primitive method."""

    def test_none(self) -> None:
        c = ASTConverter()
        assert c._convert_primitive(None) is None

    def test_bool(self) -> None:
        c = ASTConverter()
        assert c._convert_primitive(True) is True
        assert c._convert_primitive(False) is False

    def test_int(self) -> None:
        c = ASTConverter()
        assert c._convert_primitive(42) == 42

    def test_float(self) -> None:
        c = ASTConverter()
        assert c._convert_primitive(3.14) == 3.14

    def test_string(self) -> None:
        c = ASTConverter()
        assert c._convert_primitive("hello") == "hello"

    def test_list_returns_list_literal(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive([1, 2, 3])
        assert isinstance(result, ListLiteral)

    def test_empty_list_returns_list_literal(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive([])
        assert isinstance(result, ListLiteral)

    def test_dict_with_type_returns_none(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive({"type": "Something"})
        assert result is None

    def test_empty_dict_returns_empty_dict(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive({})
        assert result == {}

    def test_plain_dict_returns_map_literal(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive({"a": 1, "b": 2})
        assert isinstance(result, MapLiteral)

    def test_unknown_type_returns_none(self) -> None:
        c = ASTConverter()
        result = c._convert_primitive(object())
        assert result is None


# ===========================================================================
# _friendly_parse_error tests
# ===========================================================================


class TestFriendlyParseError:
    """Tests for _friendly_parse_error helper."""

    def test_missing_closing_paren(self) -> None:
        """Detects unbalanced parentheses."""

        class FakeExc(Exception):
            pass

        result = _friendly_parse_error(FakeExc("err"), "MATCH (n:Person")
        assert "Missing" in result and ")" in result

    def test_extra_closing_paren(self) -> None:
        """Detects extra closing parentheses."""

        class FakeExc(Exception):
            pass

        result = _friendly_parse_error(FakeExc("err"), "MATCH (n:Person))")
        assert "Extra" in result and ")" in result

    def test_missing_closing_bracket(self) -> None:
        """Detects unbalanced brackets."""

        class FakeExc(Exception):
            pass

        result = _friendly_parse_error(FakeExc("err"), "MATCH (n)-[r:KNOWS")
        assert "Missing" in result and "]" in result

    def test_no_hints_fallback(self) -> None:
        """Falls back to raw message when no hints detected."""

        class FakeExc(Exception):
            pass

        result = _friendly_parse_error(
            FakeExc("raw error"), "MATCH (n) RETURN n"
        )
        assert result == "raw error"

    def test_misspelled_keyword_suggestion(self) -> None:
        """Suggests correct keyword for misspellings."""

        class FakeExc(Exception):
            line = 1
            column = 1
            expected = {"MATCH", "RETURN", "WITH", "CREATE"}

        result = _friendly_parse_error(FakeExc("err"), "MATC (n) RETURN n")
        assert "MATCH" in result

    def test_no_close_match_no_suggestion(self) -> None:
        """No suggestion for totally different words."""

        class FakeExc(Exception):
            line = 1
            column = 1
            expected = {"MATCH", "RETURN"}

        result = _friendly_parse_error(FakeExc("err"), "ZZZZZ (n) RETURN n")
        # Should not suggest anything (cutoff 0.6 won't match)
        assert "Did you mean" not in result


# ===========================================================================
# Error handling tests
# ===========================================================================


class TestErrorHandling:
    """Tests for error paths in AST conversion."""

    def test_invalid_syntax_raises(self) -> None:
        """Completely invalid syntax raises an error."""
        from pycypher.exceptions import ASTConversionError

        with pytest.raises(ASTConversionError):
            ASTConverter.from_cypher("THIS IS NOT CYPHER AT ALL")

    def test_empty_query_raises(self) -> None:
        """Empty query string raises."""
        from pycypher.exceptions import ASTConversionError

        with pytest.raises((ASTConversionError, ValueError)):
            ASTConverter.from_cypher("")

    def test_generic_fallback_for_unknown_type(self) -> None:
        """Generic fallback handles unknown AST node types gracefully."""
        c = ASTConverter()
        # Variable is a valid ast type, should work via generic
        result = c._convert_generic(
            {"type": "Variable", "name": "x"}, "Variable"
        )
        assert isinstance(result, Variable)

    def test_generic_fallback_invalid_args(self) -> None:
        """Generic fallback raises ASTConversionError on bad args."""
        from pycypher.exceptions import ASTConversionError

        c = ASTConverter()
        # Pass a required argument with wrong type to trigger validation error
        with pytest.raises(ASTConversionError):
            c._convert_generic(
                {
                    "type": "Comparison",
                    "operator": 999,
                    "left": "bad",
                    "right": "bad",
                },
                "Comparison",
            )


# ===========================================================================
# Caching tests
# ===========================================================================


class TestCaching:
    """Tests for LRU cache behavior."""

    def test_same_query_returns_same_object(self) -> None:
        """Cached parsing returns identical object."""
        q = "MATCH (n:Person) RETURN n.name"
        ast1 = ASTConverter.from_cypher(q)
        ast2 = ASTConverter.from_cypher(q)
        assert ast1 is ast2

    def test_different_queries_return_different_objects(self) -> None:
        """Different queries produce different ASTs."""
        ast1 = ASTConverter.from_cypher("MATCH (n:Person) RETURN n")
        ast2 = ASTConverter.from_cypher("MATCH (n:Animal) RETURN n")
        assert ast1 is not ast2
