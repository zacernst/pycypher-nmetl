"""Coverage-gap tests for pycypher.ast_converter.

Targets uncovered lines by calling ASTConverter.convert() with crafted
dict inputs and by testing _friendly_parse_error and _parse_cypher_cached
error paths.

Missing lines targeted: 137, 176, 230-233, 289-294, 345, 354, 477-479,
503-514, 557-558, 566-567, 587, 633-634, 638-642, 647-650, 661-664,
668-671, 685, 718, 742-745, 749-752, 765, 769-772, 776-779, 792-797,
817, 821-826, 837, 843-851, 859-860, 867-868, 872-873, 906-907, 944-945,
949-950, 975-976, 986, 1047, 1054-1065, 1074, 1078-1086, 1120-1133,
1149-1155, 1293, 1387, 1407, 1453-1455, 1527, 1571, 1606-1607, 1665, 1836.
"""

from __future__ import annotations

from typing import Any

import pytest
from pycypher.ast_converter import (
    ASTConverter,
    _friendly_parse_error,
    _parse_cypher_cached,
)
from pycypher.ast_models import (
    BooleanLiteral,
    Create,
    Delete,
    FloatLiteral,
    Foreach,
    IntegerLiteral,
    Merge,
    PatternPath,
    Query,
    Remove,
    RemoveItem,
    Return,
    ReturnAll,
    Set,
    SetItem,
    Unwind,
    Variable,
    With,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conv(node: dict[str, Any] | Any) -> Any:
    """Convert a dict AST node using ASTConverter."""
    return ASTConverter().convert(node)


# ---------------------------------------------------------------------------
# convert() primitive wrapping branches
# ---------------------------------------------------------------------------


class TestConvertPrimitives:
    """Cover primitive value wrapping in convert()."""

    def test_none_returns_none(self) -> None:
        assert _conv(None) is None

    def test_string_becomes_variable(self) -> None:
        result = _conv("name")
        assert isinstance(result, Variable)
        assert result.name == "name"

    def test_empty_string_returns_none_or_empty(self) -> None:
        # Empty string => not truthy, match case falls through
        result = _conv("")
        assert result is None or result == ""

    def test_bool_becomes_boolean_literal(self) -> None:
        assert isinstance(_conv(True), BooleanLiteral)
        assert _conv(True).value is True

    def test_int_becomes_integer_literal(self) -> None:
        result = _conv(42)
        assert isinstance(result, IntegerLiteral)
        assert result.value == 42

    def test_float_becomes_float_literal(self) -> None:
        result = _conv(3.14)
        assert isinstance(result, FloatLiteral)

    def test_empty_dict_returns_none(self) -> None:
        assert _conv({}) is None

    def test_dict_without_type_converts_as_primitive(self) -> None:
        result = _conv({"key": "val"})
        # Non-typed dict goes through _convert_primitive
        assert result is not None


# ---------------------------------------------------------------------------
# convert() Lark Tree fallback (lines 289-294)
# ---------------------------------------------------------------------------


class TestConvertLarkTree:
    """Cover the Tree detection branch."""

    def test_tree_with_children(self) -> None:
        """Lark Tree with children recurses into first child."""

        class FakeTree:
            children = ["hello"]

        result = _conv(FakeTree())
        assert isinstance(result, Variable)
        assert result.name == "hello"

    def test_tree_empty_children(self) -> None:
        """Lark Tree with empty children returns None."""

        class FakeTree:
            children = []

        result = _conv(FakeTree())
        assert result is None


# ---------------------------------------------------------------------------
# _convert_generic: dict-in-list and primitive-in-list (lines 345, 354)
# ---------------------------------------------------------------------------


class TestConvertGeneric:
    """Cover _convert_generic fallback paths."""

    def test_generic_with_known_ast_type(self) -> None:
        """A dict with type matching an AST class uses generic converter."""
        # CountStar has no specific _convert_ method — goes through generic
        result = _conv({"type": "CountStar"})
        assert result is not None

    def test_generic_with_list_containing_typed_dicts(self) -> None:
        """List items that are typed dicts get converted."""
        node = {
            "type": "ListLiteral",
            "elements": [{"type": "IntegerLiteral", "value": 1}],
        }
        result = _conv(node)
        assert result is not None

    def test_generic_with_list_containing_plain_dicts(self) -> None:
        """List items that are untyped dicts go through _convert_primitive."""
        node = {
            "type": "ListLiteral",
            "elements": [{"key": "val"}],
        }
        result = _conv(node)
        assert result is not None


# ---------------------------------------------------------------------------
# QueryStatement / UpdateStatement (lines 477-479, 503-514)
# ---------------------------------------------------------------------------


class TestConvertStatements:
    """Cover QueryStatement and UpdateStatement conversion."""

    def test_query_statement_with_return(self) -> None:
        """QueryStatement with return clause (lines 477-479)."""
        node = {
            "type": "QueryStatement",
            "clauses": [
                {
                    "type": "MatchClause",
                    "pattern": {"type": "Pattern", "paths": []},
                    "where": None,
                    "optional": False,
                },
            ],
            "return": {
                "type": "ReturnStatement",
                "distinct": False,
                "body": {
                    "type": "ReturnBody",
                    "items": [
                        {
                            "type": "ReturnItem",
                            "expression": "n",
                            "alias": None,
                        }
                    ],
                },
                "order": None,
                "skip": None,
                "limit": None,
            },
        }
        result = _conv(node)
        assert isinstance(result, Query)

    def test_update_statement_legacy_path(self) -> None:
        """UpdateStatement without 'clauses' key uses legacy prefix/updates path (lines 503-514)."""
        node = {
            "type": "UpdateStatement",
            "prefix": [
                {
                    "type": "MatchClause",
                    "pattern": {"type": "Pattern", "paths": []},
                    "where": None,
                    "optional": False,
                },
            ],
            "updates": [
                {"type": "SetClause", "items": []},
            ],
            "return": {
                "type": "ReturnStatement",
                "distinct": False,
                "body": {"type": "ReturnBody", "items": []},
                "order": None,
                "skip": None,
                "limit": None,
            },
        }
        result = _conv(node)
        assert isinstance(result, Query)

    def test_update_statement_ordered_clauses(self) -> None:
        """UpdateStatement with 'clauses' key (ordered list)."""
        node = {
            "type": "UpdateStatement",
            "clauses": [
                {
                    "type": "MatchClause",
                    "pattern": {"type": "Pattern", "paths": []},
                    "where": None,
                    "optional": False,
                },
                {"type": "SetClause", "items": []},
            ],
        }
        result = _conv(node)
        assert isinstance(result, Query)


# ---------------------------------------------------------------------------
# MergeClause: on_create/on_match SetItem case (lines 557-558, 566-567)
# ---------------------------------------------------------------------------


class TestConvertMergeClause:
    """Cover MergeClause on_create/on_match SetItem branches."""

    def test_merge_on_create_with_set(self) -> None:
        node = {
            "type": "MergeClause",
            "pattern": {"type": "Pattern", "paths": []},
            "actions": [
                {
                    "on": "create",
                    "set": {
                        "type": "SetClause",
                        "items": [
                            {
                                "type": "SetProperty",
                                "variable": "n",
                                "property": "age",
                                "value": 30,
                            },
                        ],
                    },
                },
            ],
        }
        result = _conv(node)
        assert isinstance(result, Merge)

    def test_merge_on_match_with_set(self) -> None:
        node = {
            "type": "MergeClause",
            "pattern": {"type": "Pattern", "paths": []},
            "actions": [
                {
                    "on": "match",
                    "set": {
                        "type": "SetClause",
                        "items": [
                            {
                                "type": "SetProperty",
                                "variable": "n",
                                "property": "age",
                                "value": 30,
                            },
                        ],
                    },
                },
            ],
        }
        result = _conv(node)
        assert isinstance(result, Merge)


# ---------------------------------------------------------------------------
# ReturnStatement limit/skip branches (lines 633-671)
# ---------------------------------------------------------------------------


class TestConvertReturnStatement:
    """Cover ReturnStatement limit/skip edge cases."""

    def test_limit_with_tree_object(self) -> None:
        """Limit with a Tree-like value (lines 633-634)."""

        class FakeTree:
            children = [5]

        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": FakeTree()},
        }
        result = _conv(node)
        assert isinstance(result, Return)

    def test_limit_with_value_attr(self) -> None:
        """Limit with a Token-like value having .value (lines 638-642)."""

        class FakeToken:
            value = "10"

        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": FakeToken()},
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.limit == 10

    def test_limit_with_unconvertible_value_attr(self) -> None:
        """Token-like .value that can't be int (lines 640-642)."""

        class FakeToken:
            value = "not_a_number"

        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": FakeToken()},
        }
        result = _conv(node)
        assert isinstance(result, Return)

    def test_limit_with_dict_expression(self) -> None:
        """Limit with a dict expression (lines 643-645)."""
        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {
                "type": "LimitClause",
                "value": {"type": "Parameter", "name": "lim"},
            },
        }
        result = _conv(node)
        assert isinstance(result, Return)

    def test_limit_with_string_fallback(self) -> None:
        """Limit with a string that can be int (lines 647-650)."""
        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": "7"},
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.limit == 7

    def test_limit_with_unconvertible_string(self) -> None:
        """String that can't be int (lines 649-650)."""
        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": "abc"},
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.limit is None

    def test_skip_with_value_attr(self) -> None:
        """Skip with Token-like .value (lines 661-664)."""

        class FakeToken:
            value = "3"

        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": {"type": "SkipClause", "value": FakeToken()},
            "limit": None,
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.skip == 3

    def test_skip_with_string_fallback(self) -> None:
        """Skip with string (lines 668-671)."""
        node = {
            "type": "ReturnStatement",
            "distinct": False,
            "body": {"type": "ReturnBody", "items": []},
            "order": None,
            "skip": {"type": "SkipClause", "value": "5"},
            "limit": None,
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.skip == 5


# ---------------------------------------------------------------------------
# _convert_Return, _convert_ReturnAll, _convert_With (lines 792-833)
# ---------------------------------------------------------------------------


class TestConvertReturnAndWith:
    """Cover the _convert_Return, _convert_ReturnAll, _convert_With branches."""

    def test_convert_return_with_order_by(self) -> None:
        """_convert_Return with order_by list (lines 792-797)."""
        node = {
            "type": "Return",
            "distinct": True,
            "items": [
                {"type": "ReturnItem", "expression": "n", "alias": None}
            ],
            "order_by": [
                {"type": "OrderByItem", "expression": "n", "ascending": True}
            ],
        }
        result = _conv(node)
        assert isinstance(result, Return)
        assert result.distinct is True

    def test_convert_return_all(self) -> None:
        """_convert_ReturnAll (line 817)."""
        result = _conv({"type": "ReturnAll"})
        assert isinstance(result, ReturnAll)

    def test_convert_with_order_by(self) -> None:
        """_convert_With with order_by (lines 821-826)."""
        node = {
            "type": "With",
            "distinct": False,
            "items": [],
            "order_by": [
                {"type": "OrderByItem", "expression": "n", "ascending": True}
            ],
        }
        result = _conv(node)
        assert isinstance(result, With)


# ---------------------------------------------------------------------------
# _convert_Merge, _convert_Delete, _convert_Set etc. (lines 837-976)
# ---------------------------------------------------------------------------


class TestConvertCRUDNodes:
    """Cover _convert_Merge, _convert_Delete, _convert_Set, _convert_Remove."""

    def test_convert_create(self) -> None:
        result = _conv(
            {"type": "Create", "pattern": {"type": "Pattern", "paths": []}}
        )
        assert isinstance(result, Create)

    def test_convert_merge_with_on_create(self) -> None:
        """_convert_Merge with on_create (lines 843-851)."""
        node = {
            "type": "Merge",
            "pattern": {"type": "Pattern", "paths": []},
            "on_create": [
                {
                    "type": "SetProperty",
                    "variable": "n",
                    "property": "age",
                    "value": 30,
                },
            ],
        }
        result = _conv(node)
        assert isinstance(result, Merge)

    def test_convert_merge_with_on_match(self) -> None:
        """_convert_Merge with on_match (lines 847-851)."""
        node = {
            "type": "Merge",
            "pattern": {"type": "Pattern", "paths": []},
            "on_match": [
                {
                    "type": "SetProperty",
                    "variable": "n",
                    "property": "age",
                    "value": 30,
                },
            ],
        }
        result = _conv(node)
        assert isinstance(result, Merge)

    def test_convert_delete(self) -> None:
        """_convert_Delete (lines 859-860)."""
        node = {"type": "Delete", "detach": True, "expressions": ["n"]}
        result = _conv(node)
        assert isinstance(result, Delete)

    def test_convert_set(self) -> None:
        """_convert_Set (lines 867-868)."""
        node = {"type": "Set", "items": []}
        result = _conv(node)
        assert isinstance(result, Set)

    def test_convert_set_item(self) -> None:
        """_convert_SetItem (lines 872-873)."""
        node = {
            "type": "SetItem",
            "variable": "n",
            "property": "age",
            "expression": 30,
        }
        result = _conv(node)
        assert isinstance(result, SetItem)

    def test_convert_set_labels(self) -> None:
        """_convert_SetLabels (lines 906-907)."""
        node = {
            "type": "SetLabels",
            "variable": "n",
            "labels": {"name": "Person"},
        }
        result = _conv(node)
        assert isinstance(result, SetItem)

    def test_convert_set_all_properties(self) -> None:
        """_convert_SetAllProperties (line 918)."""
        node = {
            "type": "SetAllProperties",
            "variable": "n",
            "value": {"type": "MapLiteral", "value": {}, "entries": {}},
        }
        result = _conv(node)
        assert isinstance(result, SetItem)
        assert result.property == "*"

    def test_convert_remove(self) -> None:
        """_convert_Remove (lines 944-945)."""
        node = {"type": "Remove", "items": []}
        result = _conv(node)
        assert isinstance(result, Remove)

    def test_convert_remove_item(self) -> None:
        """_convert_RemoveItem (lines 949-950)."""
        node = {"type": "RemoveItem", "variable": "n", "property": "age"}
        result = _conv(node)
        assert isinstance(result, RemoveItem)

    def test_convert_remove_labels(self) -> None:
        """_convert_RemoveLabels (lines 975-976)."""
        node = {
            "type": "RemoveLabels",
            "variable": "n",
            "labels": {"name": "Admin"},
        }
        result = _conv(node)
        assert isinstance(result, RemoveItem)

    def test_convert_unwind(self) -> None:
        """_convert_Unwind (line 986)."""
        node = {
            "type": "Unwind",
            "expression": {"type": "ListLiteral", "elements": []},
            "alias": "x",
        }
        result = _conv(node)
        assert isinstance(result, Unwind)


# ---------------------------------------------------------------------------
# _normalize_procedure_name (lines 1047-1065)
# ---------------------------------------------------------------------------


class TestNormalizeProcedureName:
    """Cover _normalize_procedure_name branches."""

    def test_none(self) -> None:
        result = ASTConverter()._normalize_procedure_name(None)
        assert result is None

    def test_dict_with_namespace_and_name(self) -> None:
        result = ASTConverter()._normalize_procedure_name(
            {"namespace": "db", "name": "info"},
        )
        assert result == "db.info"

    def test_dict_with_name_only(self) -> None:
        result = ASTConverter()._normalize_procedure_name({"name": "info"})
        assert result == "info"

    def test_dict_with_namespace_only(self) -> None:
        result = ASTConverter()._normalize_procedure_name({"namespace": "db"})
        assert result == "db"

    def test_dict_empty(self) -> None:
        result = ASTConverter()._normalize_procedure_name({})
        assert result is None

    def test_list_of_parts(self) -> None:
        result = ASTConverter()._normalize_procedure_name(
            ["db", "schema", "info"]
        )
        assert result == "db.schema.info"

    def test_string(self) -> None:
        result = ASTConverter()._normalize_procedure_name("db.info")
        assert result == "db.info"


# ---------------------------------------------------------------------------
# Pattern / PathPattern / ShortestPath (lines 1074-1155)
# ---------------------------------------------------------------------------


class TestConvertPatternPaths:
    """Cover Pattern dict-in-list, PathPattern ShortestPath, etc."""

    def test_pattern_with_none_path(self) -> None:
        """Pattern with a None path (line 1074)."""
        node = {"type": "Pattern", "paths": [None]}
        result = _conv(node)
        assert result is not None

    def test_pattern_with_unconverted_dict(self) -> None:
        """Pattern path that is a dict needing conversion (lines 1078-1086)."""
        node = {
            "type": "Pattern",
            "paths": [
                {
                    "type": "PathPattern",
                    "variable": None,
                    "element": {"type": "PatternElement", "parts": []},
                },
            ],
        }
        result = _conv(node)
        assert result is not None

    def test_shortest_path_conversion(self) -> None:
        """_convert_ShortestPath (lines 1149-1155)."""
        node = {
            "type": "ShortestPath",
            "all": True,
            "parts": [],
        }
        result = _conv(node)
        assert isinstance(result, PatternPath)
        assert result.shortest_path_mode == "all"

    def test_shortest_path_one(self) -> None:
        node = {
            "type": "ShortestPath",
            "all": False,
            "parts": [],
        }
        result = _conv(node)
        assert isinstance(result, PatternPath)
        assert result.shortest_path_mode == "one"

    def test_path_pattern_with_shortest_path_element(self) -> None:
        """PathPattern whose element is ShortestPath (lines 1120-1133)."""
        node = {
            "type": "PathPattern",
            "variable": None,
            "element": {
                "type": "ShortestPath",
                "all": False,
                "parts": [],
            },
        }
        result = _conv(node)
        assert isinstance(result, PatternPath)
        assert result.shortest_path_mode == "one"

    def test_path_pattern_with_non_pattern_element(self) -> None:
        """PathPattern with a generic element (lines 1130-1133)."""
        node = {
            "type": "PathPattern",
            "variable": None,
            "element": {
                "type": "NodePattern",
                "variable": "n",
                "labels": ["Person"],
            },
        }
        result = _conv(node)
        assert isinstance(result, PatternPath)


# ---------------------------------------------------------------------------
# WithClause limit/skip branches (lines 742-779)
# ---------------------------------------------------------------------------


class TestConvertWithClause:
    """Cover WithClause limit/skip edge cases."""

    def test_with_skip_token_like(self) -> None:
        """WithClause skip with .value attribute (lines 742-745)."""

        class FakeToken:
            value = "2"

        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": None,
            "order": None,
            "skip": {"type": "SkipClause", "value": FakeToken()},
            "limit": None,
        }
        result = _conv(node)
        assert isinstance(result, With)
        assert result.skip == 2

    def test_with_skip_string_fallback(self) -> None:
        """WithClause skip with string (lines 749-752)."""
        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": None,
            "order": None,
            "skip": {"type": "SkipClause", "value": "3"},
            "limit": None,
        }
        result = _conv(node)
        assert isinstance(result, With)
        assert result.skip == 3

    def test_with_limit_tree_like(self) -> None:
        """WithClause limit with Tree-like value (lines 765)."""

        class FakeTree:
            children = [10]

        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": None,
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": FakeTree()},
        }
        result = _conv(node)
        assert isinstance(result, With)

    def test_with_limit_token_like(self) -> None:
        """WithClause limit with .value (lines 769-772)."""

        class FakeToken:
            value = "5"

        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": None,
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": FakeToken()},
        }
        result = _conv(node)
        assert isinstance(result, With)
        assert result.limit == 5

    def test_with_limit_string_fallback(self) -> None:
        """WithClause limit with plain string (lines 776-779)."""
        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": None,
            "order": None,
            "skip": None,
            "limit": {"type": "LimitClause", "value": "8"},
        }
        result = _conv(node)
        assert isinstance(result, With)
        assert result.limit == 8

    def test_with_where_condition_key(self) -> None:
        """WithClause where using 'condition' key (line 718)."""
        node = {
            "type": "WithClause",
            "distinct": False,
            "items": [],
            "where": {
                "condition": {
                    "type": "Comparison",
                    "operator": ">",
                    "left": "n",
                    "right": 5,
                },
            },
            "order": None,
            "skip": None,
            "limit": None,
        }
        result = _conv(node)
        assert isinstance(result, With)


# ---------------------------------------------------------------------------
# _friendly_parse_error (lines 137, 176)
# ---------------------------------------------------------------------------


class TestFriendlyParseError:
    """Cover _friendly_parse_error branches."""

    def test_missing_closing_brace(self) -> None:
        """Missing brace hint (line 176)."""

        class FakeExc(Exception):
            pass

        exc = FakeExc("Unexpected")
        exc.line = None
        exc.col = None
        result = _friendly_parse_error(exc, "MATCH (n {name: 'Alice'")
        assert "Missing" in result or "Unexpected" in result

    def test_close_match_suggestion(self) -> None:
        """Close-match keyword suggestion (line 137)."""

        class FakeExc(Exception):
            pass

        exc = FakeExc("Unexpected")
        exc.line = 1
        exc.column = 1
        exc.expected = {
            "MATCH",
            "RETURN",
            "WITH",
            "WHERE",
            "CREATE",
            "DELETE",
            "SET",
            "REMOVE",
        }
        result = _friendly_parse_error(exc, "METCH (n) RETURN n")
        assert "MATCH" in result or "Syntax error" in result


# ---------------------------------------------------------------------------
# _parse_cypher_cached error paths (lines 230-233)
# ---------------------------------------------------------------------------


class TestParseCypherCachedErrors:
    """Cover error handling in _parse_cypher_cached."""

    def test_syntax_error_raises_ast_conversion_error(self) -> None:
        from pycypher.exceptions import ASTConversionError

        with pytest.raises(ASTConversionError):
            _parse_cypher_cached("MATCH (n RETURN")

    def test_valid_query_parses(self) -> None:
        result = _parse_cypher_cached("MATCH (n:Person) RETURN n")
        assert result is not None


# ---------------------------------------------------------------------------
# ForeachClause (line 1024)
# ---------------------------------------------------------------------------


class TestConvertForeachClause:
    """Cover _convert_ForeachClause."""

    def test_foreach_clause(self) -> None:
        node = {
            "type": "ForeachClause",
            "variable": "x",
            "list_expression": {
                "type": "ListLiteral",
                "elements": [
                    {"type": "IntegerLiteral", "value": 1},
                    {"type": "IntegerLiteral", "value": 2},
                ],
            },
            "clauses": [
                {"type": "SetClause", "items": []},
            ],
        }
        result = _conv(node)
        assert isinstance(result, Foreach)
