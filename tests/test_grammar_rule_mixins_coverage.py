"""Coverage-gap tests for pycypher.grammar_rule_mixins.

Targets uncovered lines/branches by parsing Cypher queries that exercise
specific mixin transformer methods (LiteralRulesMixin, ExpressionRulesMixin,
PatternRulesMixin, ClauseRulesMixin, FunctionRulesMixin).

Missing lines targeted: 60, 75-90, 105-124, 142-147, 151, 155, 322-334,
359, 377, 394, 459, 528-532, 557-601, 605-625, 693, 730, 768, 807, 847,
918, 1092-1098, 1135, 1757, 1792, 1810, 2000, 2114, 2271, 2307, 2464-2465,
2612-2625, 2656, 2694-2698, 2712, 2730, 2762, 2778-2815, 2835-2848,
2967-2979, 2994, 3024-3035, 3081-3092, 3227-3270, 3341-3381, 3755-3768,
3806-3838, 3861, 3930, 3949-3953, 4030, 4051, 4066.
"""

from __future__ import annotations

import math
from typing import Any

import pytest
from pycypher.grammar_rule_mixins import (
    ClauseRulesMixin,
    ExpressionRulesMixin,
    FunctionRulesMixin,
    LiteralRulesMixin,
    PatternRulesMixin,
)

# ---------------------------------------------------------------------------
# Helper: parse Cypher via grammar_parser
# ---------------------------------------------------------------------------


def _parse(query: str) -> Any:
    """Parse a Cypher query and return the raw AST dict."""
    from pycypher.grammar_parser import get_default_parser

    parser = get_default_parser()
    return parser.parse_to_ast(query)


# ---------------------------------------------------------------------------
# LiteralRulesMixin — number / string / boolean literals
# ---------------------------------------------------------------------------


class TestLiteralRulesMixinDirect:
    """Direct calls to mixin methods for branch coverage."""

    def _mixin(self) -> LiteralRulesMixin:
        return LiteralRulesMixin()

    # number_literal: empty args fallback (line 60)
    def test_number_literal_empty_args(self) -> None:
        result = self._mixin().number_literal([])
        assert result == 0

    def test_number_literal_passthrough(self) -> None:
        result = self._mixin().number_literal([42])
        assert result == 42

    # signed_number: various branches (lines 75-90)
    def test_signed_number_integer(self) -> None:
        result = self._mixin().signed_number(["-42"])
        assert result == -42

    def test_signed_number_float_dot(self) -> None:
        result = self._mixin().signed_number(["3.14"])
        assert result == pytest.approx(3.14)

    def test_signed_number_scientific(self) -> None:
        result = self._mixin().signed_number(["2.5e10"])
        assert result == pytest.approx(2.5e10)

    def test_signed_number_float_suffix_f(self) -> None:
        result = self._mixin().signed_number(["1.5f"])
        assert result == pytest.approx(1.5)

    def test_signed_number_float_suffix_d(self) -> None:
        result = self._mixin().signed_number(["2.5d"])
        assert result == pytest.approx(2.5)

    def test_signed_number_underscore(self) -> None:
        result = self._mixin().signed_number(["1_000"])
        assert result == 1000

    def test_signed_number_inf(self) -> None:
        result = self._mixin().signed_number(["INF"])
        assert result == float("inf")

    def test_signed_number_neg_inf(self) -> None:
        result = self._mixin().signed_number(["-INF"])
        assert result == float("-inf")

    def test_signed_number_nan(self) -> None:
        result = self._mixin().signed_number(["NaN"])
        assert math.isnan(result)

    def test_signed_number_unparseable(self) -> None:
        result = self._mixin().signed_number(["not_a_number"])
        assert result == "not_a_number"

    # unsigned_number: hex, octal, special (lines 105-124)
    def test_unsigned_number_hex(self) -> None:
        # 0x1A has no 'e'/'f'/'d' so it hits the hex branch
        result = self._mixin().unsigned_number(["0x1A"])
        assert result == 26

    def test_unsigned_number_octal(self) -> None:
        result = self._mixin().unsigned_number(["0o17"])
        assert result == 15

    def test_unsigned_number_float(self) -> None:
        result = self._mixin().unsigned_number(["2.5"])
        assert result == pytest.approx(2.5)

    def test_unsigned_number_inf(self) -> None:
        result = self._mixin().unsigned_number(["Infinity"])
        assert result == float("inf")

    def test_unsigned_number_nan(self) -> None:
        result = self._mixin().unsigned_number(["NaN"])
        assert math.isnan(result)

    def test_unsigned_number_unparseable(self) -> None:
        result = self._mixin().unsigned_number(["xyz"])
        assert result == "xyz"

    # string_literal (lines 142-147)
    def test_string_literal_single_quoted(self) -> None:
        result = self._mixin().string_literal(["'hello'"])
        assert result == {"type": "StringLiteral", "value": "hello"}

    def test_string_literal_double_quoted(self) -> None:
        result = self._mixin().string_literal(['"world"'])
        assert result == {"type": "StringLiteral", "value": "world"}

    def test_string_literal_escapes(self) -> None:
        result = self._mixin().string_literal(["'a\\nb\\tc'"])
        assert result["value"] == "a\nb\tc"

    # boolean literals (lines 151, 155)
    def test_true_literal(self) -> None:
        assert self._mixin().true([]) is True

    def test_false_literal(self) -> None:
        assert self._mixin().false([]) is False


# ---------------------------------------------------------------------------
# ExpressionRulesMixin — operators and expressions
# ---------------------------------------------------------------------------


class TestExpressionRulesMixinDirect:
    """Direct calls to expression mixin methods."""

    def _mixin(self) -> ExpressionRulesMixin:
        return ExpressionRulesMixin()

    # Operator extractors (lines 322-334)
    def test_add_op(self) -> None:
        assert self._mixin().add_op(["+"]) == "+"

    def test_mult_op(self) -> None:
        assert self._mixin().mult_op(["*"]) == "*"

    def test_pow_op(self) -> None:
        assert self._mixin().pow_op(["^"]) == "^"

    def test_unary_op(self) -> None:
        assert self._mixin().unary_op(["-"]) == "-"

    # Boolean expressions pass-through (lines 359, 377, 394)
    def test_or_expression_single(self) -> None:
        assert self._mixin().or_expression(["x"]) == "x"

    def test_or_expression_multiple(self) -> None:
        result = self._mixin().or_expression(["x", "y"])
        assert result == {"type": "Or", "operands": ["x", "y"]}

    def test_xor_expression_single(self) -> None:
        assert self._mixin().xor_expression(["x"]) == "x"

    def test_xor_expression_multiple(self) -> None:
        result = self._mixin().xor_expression(["a", "b"])
        assert result == {"type": "Xor", "operands": ["a", "b"]}

    def test_and_expression_single(self) -> None:
        assert self._mixin().and_expression(["x"]) == "x"

    def test_and_expression_multiple(self) -> None:
        result = self._mixin().and_expression(["a", "b", "c"])
        assert result == {"type": "And", "operands": ["a", "b", "c"]}

    # String predicate expression (lines 557-569)
    def test_string_predicate_expression_single(self) -> None:
        assert self._mixin().string_predicate_expression(["val"]) == "val"

    def test_string_predicate_expression_with_op(self) -> None:
        result = self._mixin().string_predicate_expression(
            ["left", "CONTAINS", "right"],
        )
        assert result["type"] == "StringPredicate"
        assert result["operator"] == "CONTAINS"
        assert result["left"] == "left"
        assert result["right"] == "right"

    # String predicate operators (lines 605-625)
    def test_starts_with_op(self) -> None:
        assert self._mixin().starts_with_op([]) == "STARTS WITH"

    def test_ends_with_op(self) -> None:
        assert self._mixin().ends_with_op([]) == "ENDS WITH"

    def test_contains_op(self) -> None:
        assert self._mixin().contains_op([]) == "CONTAINS"

    def test_regex_match_op(self) -> None:
        assert self._mixin().regex_match_op([]) == "=~"

    def test_in_op(self) -> None:
        assert self._mixin().in_op([]) == "IN"

    # Arithmetic expressions (lines 693, 730, 768)
    def test_add_expression_single(self) -> None:
        assert self._mixin().add_expression(["x"]) == "x"

    def test_add_expression_multi(self) -> None:
        result = self._mixin().add_expression([1, "+", 2])
        assert result["type"] == "Arithmetic"
        assert result["operator"] == "+"

    def test_mult_expression_single(self) -> None:
        assert self._mixin().mult_expression(["x"]) == "x"

    def test_mult_expression_multi(self) -> None:
        result = self._mixin().mult_expression([3, "*", 4])
        assert result["type"] == "Arithmetic"
        assert result["operator"] == "*"

    def test_power_expression_single(self) -> None:
        assert self._mixin().power_expression(["x"]) == "x"

    def test_power_expression_multi(self) -> None:
        result = self._mixin().power_expression([2, "^", 3])
        assert result["type"] == "Arithmetic"
        assert result["operator"] == "^"

    # Unary expression (lines 806-810)
    def test_unary_expression_single(self) -> None:
        assert self._mixin().unary_expression(["val"]) == "val"

    def test_unary_expression_with_op(self) -> None:
        result = self._mixin().unary_expression(["-", 5])
        assert result == {"type": "Unary", "operator": "-", "operand": 5}


# ---------------------------------------------------------------------------
# ExpressionRulesMixin — string_predicate_op (lines 589-601)
# ---------------------------------------------------------------------------


class TestStringPredicateOp:
    """Cover the Token-handling logic in string_predicate_op."""

    def _mixin(self) -> ExpressionRulesMixin:
        return ExpressionRulesMixin()

    def test_plain_string_arg(self) -> None:
        result = self._mixin().string_predicate_op(["contains"])
        assert result == "CONTAINS"

    def test_token_with_value(self) -> None:
        from lark import Token

        tok = Token("STARTS_KEYWORD", "STARTS")
        result = self._mixin().string_predicate_op([tok, Token("WITH_KEYWORD", "WITH")])
        assert result == "STARTS WITH"

    def test_empty_value_uses_type(self) -> None:
        from lark import Token

        tok = Token("IN_KEYWORD", "")
        result = self._mixin().string_predicate_op([tok])
        assert result == "IN_KEYWORD"


# ---------------------------------------------------------------------------
# PatternRulesMixin — path length range (lines 2612-2656)
# ---------------------------------------------------------------------------


class TestPatternRulesMixinDirect:
    """Direct calls to PatternRulesMixin methods."""

    def _mixin(self) -> PatternRulesMixin:
        return PatternRulesMixin()

    def test_path_length_range_fixed(self) -> None:
        result = self._mixin().path_length_range(["3"])
        assert result == {"fixed": 3}

    def test_path_length_range_bounded(self) -> None:
        result = self._mixin().path_length_range(["2", "5"])
        assert result == {"min": 2, "max": 5}

    def test_path_length_range_unbounded(self) -> None:
        result = self._mixin().path_length_range([])
        assert result == {"unbounded": True}

    # path_length (lines 2592-2631)
    def test_path_length_with_range_dict(self) -> None:
        result = self._mixin().path_length([{"fixed": 3}])
        length = result["length"]
        assert length["min"] == 3
        assert length["max"] == 3

    def test_path_length_with_min_max_dict(self) -> None:
        result = self._mixin().path_length([{"min": 1, "max": 5}])
        length = result["length"]
        assert length["min"] == 1
        assert length["max"] == 5

    def test_path_length_with_unbounded_dict(self) -> None:
        result = self._mixin().path_length([{"unbounded": True}])
        length = result["length"]
        assert length["unbounded"] is True
        assert length["min"] == 1  # default lower bound

    def test_path_length_with_int(self) -> None:
        result = self._mixin().path_length([5])
        length = result["length"]
        assert length["min"] == 5
        assert length["max"] == 5

    def test_path_length_with_none(self) -> None:
        result = self._mixin().path_length([None])
        length = result["length"]
        assert length["unbounded"] is True
        assert length["min"] == 1

    def test_path_length_fallback_string(self) -> None:
        """Fallback coercion from string token (lines 2620-2625)."""
        result = self._mixin().path_length(["3"])
        length = result["length"]
        assert length["min"] == 3
        assert length["max"] == 3

    def test_path_length_fallback_unparseable(self) -> None:
        """Non-numeric fallback sets None."""
        result = self._mixin().path_length(["abc"])
        length = result["length"]
        assert length["min"] is None
        assert length["max"] is None


# ---------------------------------------------------------------------------
# ClauseRulesMixin — direct calls
# ---------------------------------------------------------------------------


class TestClauseRulesMixinDirect:
    """Direct calls to ClauseRulesMixin methods."""

    def _mixin(self) -> ClauseRulesMixin:
        return ClauseRulesMixin()

    # cypher_query / statement / statement_list (lines 2778-2815)
    def test_cypher_query_single_statement(self) -> None:
        result = self._mixin().cypher_query(
            [{"type": "QueryStatement", "clauses": [], "return": None}],
        )
        assert result["type"] == "Query"

    def test_statement_list_no_union(self) -> None:
        stmt = {"type": "QueryStatement", "clauses": [], "return": None}
        result = self._mixin().statement_list([stmt])
        assert isinstance(result, list)
        assert len(result) == 1

    def test_statement_list_with_union(self) -> None:
        stmt1 = {"type": "QueryStatement", "clauses": [], "return": None}
        union_op = {"type": "UnionOp", "all": False}
        stmt2 = {"type": "QueryStatement", "clauses": [], "return": None}
        result = self._mixin().statement_list([stmt1, union_op, stmt2])
        assert result["type"] == "UnionStatementList"
        assert len(result["stmts"]) == 2
        assert result["all_flags"] == [False]

    def test_cypher_query_union(self) -> None:
        union_list = {
            "type": "UnionStatementList",
            "stmts": [1, 2],
            "all_flags": [True],
        }
        result = self._mixin().cypher_query([union_list])
        assert result["type"] == "UnionQuery"

    # query_statement (lines 2835-2848)
    def test_query_statement_no_return(self) -> None:
        clause = {"type": "MatchClause", "pattern": None}
        result = self._mixin().query_statement([clause])
        assert result["type"] == "QueryStatement"
        assert result["return"] is None

    def test_query_statement_with_return(self) -> None:
        clause = {"type": "MatchClause", "pattern": None}
        ret = {"type": "ReturnStatement", "body": None}
        result = self._mixin().query_statement([clause, ret])
        assert result["type"] == "QueryStatement"
        assert result["return"]["type"] == "ReturnStatement"

    # statement pass-through (line 2994)
    def test_statement_passthrough(self) -> None:
        result = self._mixin().statement([{"type": "QueryStatement"}])
        assert result["type"] == "QueryStatement"

    def test_statement_empty(self) -> None:
        result = self._mixin().statement([])
        assert result is None

    # _ambig (lines 2967-2979)
    def test_ambig_empty(self) -> None:
        result = self._mixin()._ambig([])
        assert result is None

    def test_ambig_prefers_dict(self) -> None:
        result = self._mixin()._ambig(["plain", {"type": "Not"}, {"type": "Other"}])
        assert result == {"type": "Not"}

    def test_ambig_dict_no_not(self) -> None:
        result = self._mixin()._ambig(["plain", {"type": "Other"}])
        assert result == {"type": "Other"}

    def test_ambig_no_dict(self) -> None:
        result = self._mixin()._ambig(["hello", 42])
        assert result == "hello"

    # match_clause _is_optional (lines 3227-3270)
    def test_match_clause_not_optional(self) -> None:
        pattern = {"type": "Pattern", "paths": []}
        result = self._mixin().match_clause([pattern])
        assert result["type"] == "MatchClause"
        assert result["optional"] is False

    def test_match_clause_optional_string(self) -> None:
        pattern = {"type": "Pattern", "paths": []}
        result = self._mixin().match_clause(["OPTIONAL", pattern])
        assert result["optional"] is True

    def test_match_clause_with_where(self) -> None:
        pattern = {"type": "Pattern", "paths": []}
        where = {"type": "WhereClause", "expression": "x > 0"}
        result = self._mixin().match_clause([pattern, where])
        assert result["where"] == where

    # merge_action (lines 3334-3381)
    def test_merge_action_with_string_create(self) -> None:
        set_c = {"type": "SetClause", "items": []}
        result = self._mixin().merge_action(["ON", "CREATE", set_c])
        assert result["type"] == "MergeAction"
        assert result["on"] == "create"
        assert result["set"] == set_c

    def test_merge_action_with_string_match(self) -> None:
        set_c = {"type": "SetClause", "items": []}
        result = self._mixin().merge_action(["ON", "MATCH", set_c])
        assert result["on"] == "match"

    def test_merge_action_with_token(self) -> None:
        from lark import Token

        set_c = {"type": "SetClause", "items": []}
        result = self._mixin().merge_action(
            [
                Token("ON_KEYWORD", "ON"),
                Token("CREATE_KEYWORD", "CREATE"),
                set_c,
            ],
        )
        assert result["on"] == "create"

    def test_merge_action_no_type_defaults_create(self) -> None:
        """When on_type is None, defaults to 'create' (line 3365)."""
        set_c = {"type": "SetClause", "items": []}
        result = self._mixin().merge_action([set_c])
        assert result["on"] == "create"

    def test_merge_action_type_direct(self) -> None:
        result = self._mixin().merge_action_type(["match"])
        assert result == "MATCH"

    def test_merge_action_type_empty(self) -> None:
        result = self._mixin().merge_action_type([])
        assert result == ""

    def test_merge_action_type_token(self) -> None:
        from lark import Token

        result = self._mixin().merge_action_type([Token("CREATE_KEYWORD", "CREATE")])
        assert result == "CREATE"

    # with_clause (lines 3742-3778)
    def test_with_clause_basic(self) -> None:
        items = [{"type": "ReturnItem", "expression": "x", "alias": "y"}]
        result = self._mixin().with_clause([items])
        assert result["type"] == "WithClause"
        assert result["items"] == items
        assert result["distinct"] is False

    def test_with_clause_distinct(self) -> None:
        result = self._mixin().with_clause(["DISTINCT", []])
        assert result["distinct"] is True

    def test_with_clause_star(self) -> None:
        result = self._mixin().with_clause(["*"])
        assert result["items"] == "*"

    def test_with_clause_with_where_order(self) -> None:
        where = {"type": "WhereClause", "expression": "x > 0"}
        order = {"type": "OrderClause", "items": []}
        skip = {"type": "SkipClause", "value": 5}
        limit = {"type": "LimitClause", "value": 10}
        body = {"type": "ReturnBody", "items": []}
        result = self._mixin().with_clause([body, where, order, skip, limit])
        assert result["where"] == where
        assert result["order"] == order
        assert result["skip"] == skip
        assert result["limit"] == limit

    # return_clause (lines 3806-3838)
    def test_return_clause_simple(self) -> None:
        body = {"type": "ReturnBody", "items": [{"expr": "x"}]}
        result = self._mixin().return_clause([body])
        assert result["type"] == "ReturnStatement"
        assert result["body"] == body
        assert result["distinct"] is False

    def test_return_clause_distinct_string(self) -> None:
        result = self._mixin().return_clause(["DISTINCT", [{"expr": "x"}]])
        assert result["distinct"] is True

    def test_return_clause_star(self) -> None:
        result = self._mixin().return_clause(["*"])
        assert result["body"] == "*"

    def test_return_clause_with_order_skip_limit(self) -> None:
        body = {"type": "ReturnBody", "items": []}
        order = {"type": "OrderClause", "items": []}
        skip = {"type": "SkipClause", "value": 5}
        limit = {"type": "LimitClause", "value": 10}
        result = self._mixin().return_clause([body, order, skip, limit])
        assert result["order"] == order
        assert result["skip"] == skip
        assert result["limit"] == limit

    # distinct_keyword (line 3783)
    def test_distinct_keyword(self) -> None:
        assert self._mixin().distinct_keyword([]) == "DISTINCT"

    # order_clause / order_items / order_item / order_direction (lines 3949-4032)
    def test_order_clause(self) -> None:
        items_dict = {"items": [{"type": "OrderByItem"}]}
        result = self._mixin().order_clause([items_dict])
        assert result["type"] == "OrderClause"
        assert len(result["items"]) == 1

    def test_order_clause_empty(self) -> None:
        result = self._mixin().order_clause([])
        assert result["type"] == "OrderClause"
        assert result["items"] == []

    def test_order_items(self) -> None:
        result = self._mixin().order_items(["a", "b"])
        assert result == {"items": ["a", "b"]}

    def test_order_item_simple(self) -> None:
        result = self._mixin().order_item(["expr"])
        assert result["type"] == "OrderByItem"
        assert result["expression"] == "expr"
        assert result["ascending"] is True

    def test_order_item_desc(self) -> None:
        result = self._mixin().order_item(["expr", "desc"])
        assert result["ascending"] is False

    def test_order_item_with_nulls(self) -> None:
        nulls = {"type": "nulls_placement", "placement": "first"}
        result = self._mixin().order_item(["expr", "asc", nulls])
        assert result["nulls_placement"] == "first"

    def test_order_direction_asc(self) -> None:
        assert self._mixin().order_direction(["ASC"]) == "asc"

    def test_order_direction_desc(self) -> None:
        assert self._mixin().order_direction(["DESC"]) == "desc"

    def test_order_direction_empty(self) -> None:
        assert self._mixin().order_direction([]) == "asc"

    # skip/limit (lines 4051, 4066)
    def test_skip_clause(self) -> None:
        result = self._mixin().skip_clause([10])
        assert result == {"type": "SkipClause", "value": 10}

    def test_limit_clause(self) -> None:
        result = self._mixin().limit_clause([5])
        assert result == {"type": "LimitClause", "value": 5}

    # nulls_placement (line 3979)
    def test_nulls_placement_first(self) -> None:
        result = self._mixin().nulls_placement(["FIRST"])
        assert result["placement"] == "first"

    def test_nulls_placement_last(self) -> None:
        result = self._mixin().nulls_placement(["LAST"])
        assert result["placement"] == "last"

    # create_clause (line 3295)
    def test_create_clause(self) -> None:
        pattern = {"type": "Pattern", "paths": []}
        result = self._mixin().create_clause([pattern])
        assert result["type"] == "CreateClause"
        assert result["pattern"] == pattern

    # call_statement (lines 3013-3048)
    def test_call_statement_basic(self) -> None:
        result = self._mixin().call_statement(["db.info"])
        assert result["type"] == "Call"
        assert result["procedure_name"] == "db.info"

    def test_call_statement_with_args_list(self) -> None:
        result = self._mixin().call_statement(["db.func", ["arg1", "arg2"]])
        assert result["arguments"] == ["arg1", "arg2"]

    def test_call_statement_with_yield(self) -> None:
        yield_c = {"type": "YieldClause", "items": ["x", "y"], "where": None}
        result = self._mixin().call_statement(["db.func", yield_c])
        assert result["yield_items"] == ["x", "y"]

    def test_call_statement_with_none_args(self) -> None:
        result = self._mixin().call_statement(["db.func", None])
        assert result["arguments"] == []

    def test_call_statement_with_single_arg(self) -> None:
        result = self._mixin().call_statement(["db.func", "single_arg"])
        assert result["arguments"] == ["single_arg"]


# ---------------------------------------------------------------------------
# FunctionRulesMixin — pattern_comp_variable, pattern_filter, exists_content
# ---------------------------------------------------------------------------


class TestFunctionRulesMixinDirect:
    """Direct calls to FunctionRulesMixin methods."""

    def _mixin(self) -> FunctionRulesMixin:
        return FunctionRulesMixin()

    # pattern_comp_variable (line 1792)
    def test_pattern_comp_variable(self) -> None:
        assert self._mixin().pattern_comp_variable(["p"]) == "p"

    def test_pattern_comp_variable_empty(self) -> None:
        assert self._mixin().pattern_comp_variable([]) is None


class TestExistsContentDirect:
    """Direct calls to ExpressionRulesMixin.exists_content."""

    def _mixin(self) -> ExpressionRulesMixin:
        return ExpressionRulesMixin()

    # exists_content (lines 1134-1146)
    def test_exists_content_empty(self) -> None:
        assert self._mixin().exists_content([]) is None

    def test_exists_content_match_clause(self) -> None:
        clause = {"type": "MatchClause", "pattern": None}
        result = self._mixin().exists_content([clause])
        assert result["type"] == "ExistsSubquery"

    def test_exists_content_pattern(self) -> None:
        pattern = {"type": "PatternElement", "parts": []}
        result = self._mixin().exists_content([pattern])
        assert result == pattern


# ---------------------------------------------------------------------------
# Integration: parse real queries to exercise transformer methods
# ---------------------------------------------------------------------------


class TestGrammarParserIntegration:
    """Parse Cypher queries that exercise specific mixin code paths."""

    def test_xor_expression(self) -> None:
        """XOR expression triggers xor_expression method."""
        ast = _parse("MATCH (n) WHERE n.a XOR n.b RETURN n")
        assert ast is not None

    def test_string_contains_predicate(self) -> None:
        """CONTAINS triggers string_predicate_expression."""
        ast = _parse("MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n")
        assert ast is not None

    def test_starts_with_predicate(self) -> None:
        """STARTS WITH triggers starts_with_op."""
        ast = _parse("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n")
        assert ast is not None

    def test_ends_with_predicate(self) -> None:
        """ENDS WITH triggers ends_with_op."""
        ast = _parse("MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n")
        assert ast is not None

    def test_regex_predicate(self) -> None:
        """=~ triggers regex_match_op."""
        ast = _parse("MATCH (n:Person) WHERE n.name =~ '.*lice' RETURN n")
        assert ast is not None

    def test_optional_match(self) -> None:
        """OPTIONAL MATCH triggers match_clause with optional=True."""
        ast = _parse("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m")
        assert ast is not None

    def test_merge_on_create_set(self) -> None:
        """MERGE ON CREATE SET triggers merge_action."""
        ast = _parse(
            "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 RETURN n",
        )
        assert ast is not None

    def test_union_query(self) -> None:
        """UNION triggers statement_list union path."""
        ast = _parse(
            "MATCH (n:Person) RETURN n.name UNION MATCH (m:Person) RETURN m.name",
        )
        assert ast is not None

    def test_union_all_query(self) -> None:
        """UNION ALL triggers union with all=True."""
        ast = _parse(
            "MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Person) RETURN m.name",
        )
        assert ast is not None

    def test_with_distinct(self) -> None:
        """WITH DISTINCT triggers with_clause distinct path."""
        ast = _parse("MATCH (n:Person) WITH DISTINCT n.name AS name RETURN name")
        assert ast is not None

    def test_return_distinct(self) -> None:
        """RETURN DISTINCT triggers return_clause distinct path."""
        ast = _parse("MATCH (n:Person) RETURN DISTINCT n.name")
        assert ast is not None

    def test_order_by_desc(self) -> None:
        """ORDER BY DESC triggers order_direction."""
        ast = _parse("MATCH (n:Person) RETURN n.name ORDER BY n.name DESC")
        assert ast is not None

    def test_skip_and_limit(self) -> None:
        """SKIP/LIMIT triggers skip_clause/limit_clause."""
        ast = _parse("MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 10")
        assert ast is not None

    def test_variable_length_path(self) -> None:
        """Variable-length path triggers variable_length."""
        ast = _parse("MATCH (a)-[*1..3]->(b) RETURN a, b")
        assert ast is not None

    def test_unbounded_variable_length(self) -> None:
        """Unbounded variable-length path."""
        ast = _parse("MATCH (a)-[*]->(b) RETURN a, b")
        assert ast is not None

    def test_create_clause_parse(self) -> None:
        """CREATE triggers create_clause."""
        ast = _parse("CREATE (n:Person {name: 'Alice'})")
        assert ast is not None

    def test_call_statement_parse(self) -> None:
        """CALL triggers call_statement."""
        ast = _parse("CALL db.info()")
        assert ast is not None

    def test_exists_subquery(self) -> None:
        """EXISTS subquery triggers exists_pattern."""
        ast = _parse("MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n")
        assert ast is not None

    def test_arithmetic_operations(self) -> None:
        """Mixed arithmetic triggers add/mult/power expressions."""
        ast = _parse("RETURN 1 + 2 * 3 ^ 4")
        assert ast is not None

    def test_unary_minus(self) -> None:
        """Unary minus triggers unary_expression."""
        ast = _parse("RETURN -5")
        assert ast is not None

    def test_boolean_and_or(self) -> None:
        """Combined AND/OR triggers both expression types."""
        ast = _parse("MATCH (n) WHERE n.a AND n.b OR n.c RETURN n")
        assert ast is not None

    def test_not_expression(self) -> None:
        """NOT triggers not_expression."""
        ast = _parse("MATCH (n) WHERE NOT n.active RETURN n")
        assert ast is not None

    def test_in_list(self) -> None:
        """IN triggers in_op."""
        ast = _parse("MATCH (n) WHERE n.age IN [25, 30, 35] RETURN n")
        assert ast is not None

    def test_string_literal_in_query(self) -> None:
        """String literal exercises string_literal method."""
        ast = _parse("RETURN 'hello world'")
        assert ast is not None

    def test_boolean_literals_in_query(self) -> None:
        """TRUE/FALSE exercise true/false methods."""
        ast = _parse("RETURN true, false")
        assert ast is not None

    def test_with_where_order_skip_limit(self) -> None:
        """WITH with all modifiers."""
        ast = _parse(
            "MATCH (n:Person) "
            "WITH n.name AS name, n.age AS age "
            "WHERE age > 25 "
            "ORDER BY name "
            "SKIP 1 "
            "LIMIT 5 "
            "RETURN name",
        )
        assert ast is not None
