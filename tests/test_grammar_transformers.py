"""Unit tests for pycypher.grammar_transformers.

Tests the four specialized AST transformer classes (LiteralTransformer,
ExpressionTransformer, PatternTransformer, StatementTransformer) and the
CompositeTransformer delegation mechanism.
"""

from __future__ import annotations

import math

import pytest
from pycypher.grammar_transformers import (
    CompositeTransformer,
    ExpressionTransformer,
    LiteralTransformer,
    PatternTransformer,
    StatementTransformer,
)


# ---------------------------------------------------------------------------
# LiteralTransformer
# ---------------------------------------------------------------------------


class TestLiteralTransformer:
    """Tests for LiteralTransformer methods."""

    def setup_method(self) -> None:
        self.t = LiteralTransformer()

    # -- number_literal --

    def test_number_literal_returns_first_arg(self) -> None:
        assert self.t.number_literal([42]) == 42

    def test_number_literal_empty_args(self) -> None:
        assert self.t.number_literal([]) == 0

    # -- signed_number --

    def test_signed_number_integer(self) -> None:
        assert self.t.signed_number(["123"]) == 123

    def test_signed_number_negative_integer(self) -> None:
        assert self.t.signed_number(["-42"]) == -42

    def test_signed_number_float_dot(self) -> None:
        assert self.t.signed_number(["3.14"]) == pytest.approx(3.14)

    def test_signed_number_float_exponent(self) -> None:
        assert self.t.signed_number(["1e10"]) == pytest.approx(1e10)

    def test_signed_number_float_E_upper(self) -> None:
        assert self.t.signed_number(["2.5E3"]) == pytest.approx(2500.0)

    def test_signed_number_float_suffix_f(self) -> None:
        assert self.t.signed_number(["1.5f"]) == pytest.approx(1.5)

    def test_signed_number_float_suffix_d(self) -> None:
        assert self.t.signed_number(["2.0d"]) == pytest.approx(2.0)

    def test_signed_number_float_suffix_F(self) -> None:
        assert self.t.signed_number(["1.5F"]) == pytest.approx(1.5)

    def test_signed_number_float_suffix_D(self) -> None:
        assert self.t.signed_number(["2.0D"]) == pytest.approx(2.0)

    def test_signed_number_underscores(self) -> None:
        assert self.t.signed_number(["1_000_000"]) == 1_000_000

    def test_signed_number_inf(self) -> None:
        assert self.t.signed_number(["inf"]) == float("inf")

    def test_signed_number_negative_inf(self) -> None:
        assert self.t.signed_number(["-inf"]) == float("-inf")

    def test_signed_number_nan(self) -> None:
        result = self.t.signed_number(["nan"])
        assert math.isnan(result)

    def test_signed_number_unparseable_returns_string(self) -> None:
        result = self.t.signed_number(["not_a_number"])
        assert result == "not_a_number"

    # -- unsigned_number --

    def test_unsigned_number_integer(self) -> None:
        assert self.t.unsigned_number(["456"]) == 456

    def test_unsigned_number_float(self) -> None:
        assert self.t.unsigned_number(["7.89"]) == pytest.approx(7.89)

    def test_unsigned_number_hex(self) -> None:
        assert self.t.unsigned_number(["0xFF"]) == 255

    def test_unsigned_number_hex_upper(self) -> None:
        assert self.t.unsigned_number(["0XAB"]) == 0xAB

    def test_unsigned_number_octal(self) -> None:
        assert self.t.unsigned_number(["0o17"]) == 15

    def test_unsigned_number_octal_upper(self) -> None:
        assert self.t.unsigned_number(["0O10"]) == 8

    def test_unsigned_number_exponent(self) -> None:
        assert self.t.unsigned_number(["5e2"]) == pytest.approx(500.0)

    def test_unsigned_number_suffix_f(self) -> None:
        assert self.t.unsigned_number(["3.0f"]) == pytest.approx(3.0)

    def test_unsigned_number_underscores(self) -> None:
        assert self.t.unsigned_number(["1_000"]) == 1000

    def test_unsigned_number_inf(self) -> None:
        assert self.t.unsigned_number(["inf"]) == float("inf")

    def test_unsigned_number_nan(self) -> None:
        result = self.t.unsigned_number(["nan"])
        assert math.isnan(result)

    def test_unsigned_number_unparseable_returns_string(self) -> None:
        result = self.t.unsigned_number(["xyz"])
        assert result == "xyz"

    # -- string_literal --

    def test_string_literal_single_quotes(self) -> None:
        result = self.t.string_literal(["'hello'"])
        assert result == {"type": "StringLiteral", "value": "hello"}

    def test_string_literal_double_quotes(self) -> None:
        result = self.t.string_literal(['"world"'])
        assert result == {"type": "StringLiteral", "value": "world"}

    def test_string_literal_escape_newline(self) -> None:
        result = self.t.string_literal(["'line1\\nline2'"])
        assert result["value"] == "line1\nline2"

    def test_string_literal_escape_tab(self) -> None:
        result = self.t.string_literal(["'col1\\tcol2'"])
        assert result["value"] == "col1\tcol2"

    def test_string_literal_escape_carriage_return(self) -> None:
        result = self.t.string_literal(["'a\\rb'"])
        assert result["value"] == "a\rb"

    def test_string_literal_escape_backslash(self) -> None:
        result = self.t.string_literal(["'path\\\\dir'"])
        assert result["value"] == "path\\dir"

    def test_string_literal_escape_single_quote(self) -> None:
        result = self.t.string_literal(["'it\\'s'"])
        assert result["value"] == "it's"

    def test_string_literal_escape_double_quote(self) -> None:
        result = self.t.string_literal(["'say\\\"hi\\\"'"])
        assert result["value"] == 'say"hi"'

    def test_string_literal_no_quotes(self) -> None:
        result = self.t.string_literal(["plain"])
        assert result == {"type": "StringLiteral", "value": "plain"}

    # -- boolean literals --

    def test_true(self) -> None:
        assert self.t.true([]) is True

    def test_false(self) -> None:
        assert self.t.false([]) is False


# ---------------------------------------------------------------------------
# ExpressionTransformer
# ---------------------------------------------------------------------------


class TestExpressionTransformer:
    """Tests for ExpressionTransformer methods."""

    def setup_method(self) -> None:
        self.t = ExpressionTransformer()

    def test_add_op(self) -> None:
        assert self.t.add_op(["+"]) == "+"

    def test_add_op_minus(self) -> None:
        assert self.t.add_op(["-"]) == "-"

    def test_mult_op_multiply(self) -> None:
        assert self.t.mult_op(["*"]) == "*"

    def test_mult_op_divide(self) -> None:
        assert self.t.mult_op(["/"]) == "/"

    def test_mult_op_modulo(self) -> None:
        assert self.t.mult_op(["%"]) == "%"

    def test_pow_op(self) -> None:
        assert self.t.pow_op(["^"]) == "^"

    def test_unary_op(self) -> None:
        assert self.t.unary_op(["-"]) == "-"

    def test_unary_op_empty(self) -> None:
        assert self.t.unary_op([]) == "-"

    def test_property_lookup(self) -> None:
        result = self.t.property_lookup(["name"])
        assert result == {"type": "PropertyLookup", "property": "name"}

    def test_property_lookup_empty(self) -> None:
        result = self.t.property_lookup([])
        assert result == {"type": "PropertyLookup", "property": None}


# ---------------------------------------------------------------------------
# PatternTransformer
# ---------------------------------------------------------------------------


class TestPatternTransformer:
    """Tests for PatternTransformer methods."""

    def setup_method(self) -> None:
        self.t = PatternTransformer()

    def test_property_list_single_pair(self) -> None:
        pairs = [{"key": "name", "value": "Alice"}]
        result = self.t.property_list(pairs)
        assert result == {"props": {"name": "Alice"}}

    def test_property_list_multiple_pairs(self) -> None:
        pairs = [
            {"key": "name", "value": "Bob"},
            {"key": "age", "value": 30},
        ]
        result = self.t.property_list(pairs)
        assert result == {"props": {"name": "Bob", "age": 30}}

    def test_property_list_empty(self) -> None:
        result = self.t.property_list([])
        assert result == {"props": {}}

    def test_property_list_ignores_non_dict_args(self) -> None:
        result = self.t.property_list(["not_a_dict", 42])
        assert result == {"props": {}}

    def test_property_list_ignores_dict_without_key(self) -> None:
        result = self.t.property_list([{"value": "orphan"}])
        assert result == {"props": {}}

    def test_property_key_value(self) -> None:
        result = self.t.property_key_value(["name", "Alice"])
        assert result == {"key": "name", "value": "Alice"}

    def test_property_key_value_single_arg(self) -> None:
        result = self.t.property_key_value(["name"])
        assert result == {"key": "name", "value": None}

    def test_property_key_value_empty(self) -> None:
        result = self.t.property_key_value([])
        assert result == {"key": "", "value": None}

    def test_property_name(self) -> None:
        assert self.t.property_name(["age"]) == "age"

    def test_property_name_empty(self) -> None:
        assert self.t.property_name([]) == ""


# ---------------------------------------------------------------------------
# StatementTransformer
# ---------------------------------------------------------------------------


class TestStatementTransformer:
    """Tests for StatementTransformer methods."""

    def setup_method(self) -> None:
        self.t = StatementTransformer()

    # -- cypher_query --

    def test_cypher_query_simple(self) -> None:
        clauses = [{"type": "MatchClause"}, {"type": "ReturnStatement"}]
        result = self.t.cypher_query(clauses)
        assert result["type"] == "Query"
        assert result["statements"] == clauses

    def test_cypher_query_union(self) -> None:
        union_list = {
            "type": "UnionStatementList",
            "stmts": [{"type": "A"}, {"type": "B"}],
            "all_flags": [True],
        }
        result = self.t.cypher_query([union_list])
        assert result["type"] == "UnionQuery"
        assert result["stmts"] == [{"type": "A"}, {"type": "B"}]
        assert result["all_flags"] == [True]

    def test_cypher_query_empty(self) -> None:
        result = self.t.cypher_query([])
        assert result["type"] == "Query"
        assert result["statements"] == []

    # -- statement_list --

    def test_statement_list_single(self) -> None:
        stmts = [{"type": "MatchClause"}]
        result = self.t.statement_list(stmts)
        assert result == [{"type": "MatchClause"}]

    def test_statement_list_with_union(self) -> None:
        args = [
            {"type": "QueryStatement", "clauses": []},
            {"type": "UnionOp", "all": True},
            {"type": "QueryStatement", "clauses": []},
        ]
        result = self.t.statement_list(args)
        assert result["type"] == "UnionStatementList"
        assert len(result["stmts"]) == 2
        assert result["all_flags"] == [True]

    def test_statement_list_union_not_all(self) -> None:
        args = [
            {"type": "QueryStatement"},
            {"type": "UnionOp", "all": False},
            {"type": "QueryStatement"},
        ]
        result = self.t.statement_list(args)
        assert result["all_flags"] == [False]

    # -- union_op --

    def test_union_op_all(self) -> None:
        result = self.t.union_op([True])
        assert result == {"type": "UnionOp", "all": True}

    def test_union_op_plain(self) -> None:
        result = self.t.union_op([])
        assert result == {"type": "UnionOp", "all": False}

    # -- query_statement --

    def test_query_statement(self) -> None:
        clauses = [{"type": "MatchClause"}, {"type": "ReturnStatement"}]
        result = self.t.query_statement(clauses)
        assert result == {"type": "QueryStatement", "clauses": clauses}

    # -- statement --

    def test_statement_returns_first_arg(self) -> None:
        result = self.t.statement([{"type": "MatchClause"}])
        assert result == {"type": "MatchClause"}

    def test_statement_empty(self) -> None:
        assert self.t.statement([]) is None

    # -- match_clause --

    def test_match_clause_basic(self) -> None:
        pattern = {"type": "Pattern", "nodes": []}
        result = self.t.match_clause([pattern])
        assert result["type"] == "MatchClause"
        assert result["pattern"] == pattern
        assert result["optional"] is False
        assert "where" not in result

    def test_match_clause_with_where(self) -> None:
        pattern = {"type": "Pattern", "nodes": []}
        where = {"type": "WhereClause", "condition": "x > 1"}
        result = self.t.match_clause([pattern, where])
        assert result["pattern"] == pattern
        assert result["where"] == where

    def test_match_clause_optional(self) -> None:
        pattern = {"type": "Pattern", "nodes": []}
        optional = {"type": "OptionalKeyword"}
        result = self.t.match_clause([optional, pattern])
        assert result["optional"] is True

    def test_match_clause_no_pattern(self) -> None:
        result = self.t.match_clause([])
        assert result["pattern"] is None
        assert result["optional"] is False

    # -- return_clause --

    def test_return_clause_basic(self) -> None:
        body = [{"type": "ReturnItem", "expr": "n"}]
        result = self.t.return_clause([body])
        assert result["type"] == "ReturnStatement"
        assert result["distinct"] is False
        assert result["body"]["type"] == "ReturnBody"
        assert result["order"] is None
        assert result["skip"] is None
        assert result["limit"] is None

    def test_return_clause_star(self) -> None:
        result = self.t.return_clause(["*"])
        assert result["body"] == "*"

    def test_return_clause_distinct_string(self) -> None:
        result = self.t.return_clause(["DISTINCT", [{"expr": "n"}]])
        assert result["distinct"] is True

    def test_return_clause_with_order_skip_limit(self) -> None:
        body = {"type": "ReturnBody", "items": []}
        order = {"type": "OrderClause", "items": []}
        skip = {"type": "SkipClause", "value": 5}
        limit = {"type": "LimitClause", "value": 10}
        result = self.t.return_clause([body, order, skip, limit])
        assert result["body"] == body
        assert result["order"] == order
        assert result["skip"] == skip
        assert result["limit"] == limit

    # -- optional_keyword --

    def test_optional_keyword(self) -> None:
        result = self.t.optional_keyword([])
        assert result == {"type": "OptionalKeyword"}

    # -- where_clause --

    def test_where_clause(self) -> None:
        result = self.t.where_clause(["x > 1"])
        assert result == {"type": "WhereClause", "condition": "x > 1"}

    def test_where_clause_empty(self) -> None:
        result = self.t.where_clause([])
        assert result == {"type": "WhereClause", "condition": None}

    # -- order_clause --

    def test_order_clause(self) -> None:
        items_dict = {"items": [{"expr": "n.name", "asc": True}]}
        result = self.t.order_clause([items_dict])
        assert result["type"] == "OrderClause"
        assert result["items"] == [{"expr": "n.name", "asc": True}]

    def test_order_clause_empty(self) -> None:
        result = self.t.order_clause([])
        assert result["type"] == "OrderClause"
        assert result["items"] == []

    # -- skip_clause --

    def test_skip_clause(self) -> None:
        result = self.t.skip_clause([10])
        assert result == {"type": "SkipClause", "value": 10}

    def test_skip_clause_empty(self) -> None:
        result = self.t.skip_clause([])
        assert result == {"type": "SkipClause", "value": None}

    # -- limit_clause --

    def test_limit_clause(self) -> None:
        result = self.t.limit_clause([25])
        assert result == {"type": "LimitClause", "value": 25}

    def test_limit_clause_empty(self) -> None:
        result = self.t.limit_clause([])
        assert result == {"type": "LimitClause", "value": None}


# ---------------------------------------------------------------------------
# CompositeTransformer
# ---------------------------------------------------------------------------


class TestCompositeTransformer:
    """Tests for CompositeTransformer delegation."""

    def setup_method(self) -> None:
        self.t = CompositeTransformer()

    def test_delegates_to_literal_transformer(self) -> None:
        assert self.t.true([]) is True
        assert self.t.false([]) is False

    def test_delegates_to_expression_transformer(self) -> None:
        result = self.t.property_lookup(["name"])
        assert result == {"type": "PropertyLookup", "property": "name"}

    def test_delegates_to_pattern_transformer(self) -> None:
        result = self.t.property_name(["age"])
        assert result == "age"

    def test_delegates_to_statement_transformer(self) -> None:
        result = self.t.where_clause(["x > 1"])
        assert result["type"] == "WhereClause"

    def test_raises_attribute_error_for_unknown_method(self) -> None:
        with pytest.raises(AttributeError, match="No transformer handles"):
            self.t.nonexistent_method([])

    def test_fallback_transformer(self) -> None:

        class FakeTransformer:
            def custom_rule(self, args: list) -> str:
                return "fallback_result"

        self.t.set_fallback_transformer(FakeTransformer())
        assert self.t.custom_rule([]) == "fallback_result"

    def test_fallback_not_used_when_specialized_handles(self) -> None:

        class FakeTransformer:
            def true(self, args: list) -> str:
                return "wrong"

        self.t.set_fallback_transformer(FakeTransformer())
        # Specialized LiteralTransformer should take priority
        assert self.t.true([]) is True

    def test_ambig_single_arg(self) -> None:
        assert self.t._ambig(["only"]) == "only"

    def test_ambig_multiple_args(self) -> None:
        assert self.t._ambig(["first", "second"]) == "first"

    def test_no_fallback_raises_attribute_error(self) -> None:
        assert self.t._fallback_transformer is None
        with pytest.raises(AttributeError):
            self.t.totally_unknown_method([])
