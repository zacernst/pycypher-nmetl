"""openCypher Grammar Parser using Lark.

This module implements a parser for the openCypher query language based on the
grammar specification in grammar.bnf. It uses the Lark parsing library to parse
Cypher queries into abstract syntax trees (ASTs).

The parser uses a CompositeTransformer architecture with specialized transformers
(LiteralTransformer, ExpressionTransformer, PatternTransformer, StatementTransformer)
that follow the Single Responsibility Principle, replacing the previous monolithic
CypherASTTransformer design.

This is an alternative implementation to cypher_parser.py, providing a more
comprehensive grammar coverage directly based on the BNF specification.

Example:
    >>> from pycypher.grammar_parser import GrammarParser
    >>> parser = GrammarParser()
    >>> query = "MATCH (n:Person {name: 'Alice'}) RETURN n"
    >>> tree = parser.parse(query)
    >>> print(tree.pretty())

"""

import argparse
import collections
import functools
import sys
import threading
import warnings
from pathlib import Path
from typing import Any

from lark import Lark, Transformer, Tree
from shared.logger import LOGGER

from pycypher.grammar_rule_mixins import (
    ClauseRulesMixin,
    ExpressionRulesMixin,
    FunctionRulesMixin,
    LiteralRulesMixin,
    PatternRulesMixin,
)

# Import specialized transformers
from pycypher.grammar_transformers import CompositeTransformer

__all__ = [
    "GrammarParser",
    "get_default_parser",
]

# Complete Lark grammar for openCypher
# Based on the official openCypher grammar specification
CYPHER_GRAMMAR = r"""
?start: cypher_query

//============================================================================
// Top-level query structure
//============================================================================

cypher_query: statement_list

// union_all_marker must be a named rule (not anonymous terminal) so the
// transformer can detect whether UNION was followed by ALL or not.
union_all_marker: "ALL"i
union_op: "UNION"i union_all_marker?
statement_list: statement (union_op statement)*

statement: query_statement
         | update_statement
         | call_statement

query_statement: read_clause* return_clause

read_clause: match_clause
           | unwind_clause
           | with_clause
           | call_clause

update_statement: (match_clause | unwind_clause | with_clause | call_clause)* update_clause+ ((with_clause | match_clause | unwind_clause | call_clause)* update_clause+)* (with_clause | match_clause | unwind_clause | call_clause)* return_clause?

update_clause: create_clause
             | merge_clause
             | delete_clause
             | set_clause
             | remove_clause
             | foreach_clause

//============================================================================
// FOREACH clause
//============================================================================

foreach_clause: "FOREACH"i "(" variable_name "IN"i expression "|" update_clause+ ")"

//============================================================================
// CALL statement (procedure calls)
//============================================================================

call_statement: "CALL"i procedure_reference explicit_args? yield_clause?

call_clause: "CALL"i procedure_reference explicit_args yield_clause?

procedure_reference: function_name

explicit_args: "(" (expression ("," expression)*)? ")"

yield_clause: "YIELD"i (yield_items | "*") where_clause?

yield_items: yield_item ("," yield_item)*

yield_item: field_name ("AS"i variable_name)?

field_name: IDENTIFIER

//============================================================================
// MATCH clause
//============================================================================

match_clause: optional_keyword? "MATCH"i pattern where_clause?

optional_keyword: "OPTIONAL"i

//============================================================================
// CREATE clause
//============================================================================

create_clause: "CREATE"i pattern

//============================================================================
// MERGE clause
//============================================================================

merge_clause: "MERGE"i pattern merge_action*

merge_action: "ON"i merge_action_type set_clause

merge_action_type: "MATCH"i  -> merge_action_match
                 | "CREATE"i -> merge_action_create

//============================================================================
// DELETE clause
//============================================================================

delete_clause: detach_keyword? "DELETE"i delete_items

?detach_keyword: "DETACH"i


delete_items: expression ("," expression)*

//============================================================================
// SET clause
//============================================================================

set_clause: "SET"i set_items

set_items: set_item ("," set_item)*

set_item: set_property_item
        | set_labels_item
        | set_all_properties_item
        | add_all_properties_item

set_property_item: variable_name property_lookup "=" expression

set_labels_item: variable_name node_labels

set_all_properties_item: variable_name "=" expression

add_all_properties_item: variable_name "+=" expression

//============================================================================
// REMOVE clause
//============================================================================

remove_clause: "REMOVE"i remove_items

remove_items: remove_item ("," remove_item)*

remove_item: remove_property_item
           | remove_labels_item

remove_property_item: variable_name property_lookup

remove_labels_item: variable_name node_labels

//============================================================================
// UNWIND clause
//============================================================================

unwind_clause: "UNWIND"i expression "AS"i variable_name

//============================================================================
// WITH clause
//============================================================================

with_clause: "WITH"i distinct_keyword? return_body where_clause? order_clause? skip_clause? limit_clause?

//============================================================================
// RETURN clause
//============================================================================

return_clause: "RETURN"i distinct_keyword? return_body order_clause? skip_clause? limit_clause?

distinct_keyword: "DISTINCT"i


return_body: return_items | "*"

return_items: return_item ("," return_item)*

return_item: expression ("AS"i return_alias)?

return_alias: IDENTIFIER

//============================================================================
// WHERE clause
//============================================================================

where_clause: "WHERE"i expression

//============================================================================
// ORDER BY clause
//============================================================================

order_clause: "ORDER"i "BY"i order_items

order_items: order_item ("," order_item)*

order_item: expression order_direction? nulls_placement?

order_direction: ASC_KEYWORD | ASCENDING_KEYWORD | DESC_KEYWORD | DESCENDING_KEYWORD

nulls_placement: "NULLS"i NULLS_FIRST_KEYWORD
              | "NULLS"i NULLS_LAST_KEYWORD

//============================================================================
// SKIP and LIMIT
//============================================================================

skip_clause: "SKIP"i expression

limit_clause: "LIMIT"i expression

//============================================================================
// Pattern matching
//============================================================================

pattern: path_pattern ("," path_pattern)*

path_pattern: (variable_name "=")? pattern_element

pattern_element: node_pattern (relationship_pattern node_pattern)*
               | "(" pattern_element ")"
               | shortest_path

shortest_path: ("SHORTESTPATH"i | "ALLSHORTESTPATHS"i) "(" node_pattern relationship_pattern node_pattern ")"

//============================================================================
// Node pattern
//============================================================================

node_pattern: "(" node_pattern_filler? ")"

node_pattern_filler: variable_name? node_labels? node_properties? node_where?

node_labels: label_expression+

label_expression: ":" label_term
                | "IS"i label_term

label_term: label_factor ("|" label_factor)*

label_factor: "!"? label_primary

label_primary: label_name
             | "(" label_term ")"
             | "%"

label_name: IDENTIFIER

node_properties: properties | node_where

node_where: "WHERE"i expression

//============================================================================
// Relationship pattern
//============================================================================

relationship_pattern: full_rel_left
                    | full_rel_right
                    | full_rel_any

full_rel_left: "<-" rel_detail? "-"

full_rel_right: "-" rel_detail? "->"

full_rel_any: "-" rel_detail? "-"

rel_detail: "[" rel_filler? "]"

rel_filler: variable_name? rel_types? rel_properties? path_length? rel_where?

rel_types: ":" rel_type (("|:" | "|") rel_type)*

rel_type: IDENTIFIER

rel_properties: properties

rel_where: "WHERE"i expression

path_length: "*" path_length_range?

path_length_range: UNSIGNED_INT ".." UNSIGNED_INT
                 | ".." UNSIGNED_INT
                 | UNSIGNED_INT ".."
                 | UNSIGNED_INT

//============================================================================
// Properties
//============================================================================

properties: "{" property_list? "}"

property_list: property_key_value ("," property_key_value)*

property_key_value: property_name ":" expression

property_name: IDENTIFIER

//============================================================================
// Expressions
//============================================================================

?expression: or_expression

?or_expression: xor_expression ("OR"i xor_expression)*

?xor_expression: and_expression ("XOR"i and_expression)*

?and_expression: not_expression ("AND"i not_expression)*

not_expression: NOT_KEYWORD* comparison_expression

?comparison_expression: label_predicate_expression (COMP_OP label_predicate_expression)*

// Label predicate: n:Person  (true if node n has label Person)
// Multiple labels: n:Person:Employee  (AND — all labels must match)
label_predicate_expression: null_predicate_expression (":" label_name)*

?null_predicate_expression: string_predicate_expression null_check_op?

null_check_op: "IS"i "NOT"i "NULL"i  -> is_not_null
             | "IS"i "NULL"i         -> is_null

?string_predicate_expression: add_expression (string_predicate_op add_expression)*

string_predicate_op: "STARTS"i "WITH"i -> starts_with_op
                   | "ENDS"i "WITH"i   -> ends_with_op
                   | "CONTAINS"i        -> contains_op
                   | "=~"               -> regex_match_op
                   | "NOT"i "IN"i       -> not_in_op
                   | "IN"i              -> in_op

?add_expression: mult_expression (add_op mult_expression)*
!add_op: "+" | "-"

?mult_expression: power_expression (mult_op power_expression)*
!mult_op: "*" | "/" | "%"

?power_expression: unary_expression (pow_op unary_expression)*
!pow_op: "^"

?unary_expression: unary_op unary_expression
                 | postfix_expression
!unary_op: "+" | "-"

?postfix_expression: atom_expression postfix_op*

postfix_op: property_lookup
          | index_lookup
          | slicing

property_lookup: "." property_name

index_lookup: "[" expression "]"

slicing: "[" slice_start ".." slice_end "]"
slice_start: expression |
slice_end: expression |

?atom_expression: literal
                | parameter
                | variable_name
                | count_star
                | function_invocation
                | exists_expression
                | inline_pattern_predicate
                | list_comprehension
                | pattern_comprehension
                | case_expression
                | reduce_expression
                | quantifier_expression
                | map_projection
                | "(" expression ")"

// Inline pattern predicate: (a)-[:R]->(b) shorthand for EXISTS { (a)-[:R]->(b) }
// Requires at least one relationship hop (+) to distinguish from "(expression)".
inline_pattern_predicate: node_pattern (relationship_pattern node_pattern)+

//============================================================================
// Count star
//============================================================================

count_star: "COUNT"i "(" "*" ")"

//============================================================================
// EXISTS expression
//============================================================================

exists_expression: "EXISTS"i "{" exists_content "}"

exists_content: (match_clause | unwind_clause | with_clause)* return_clause?
              | pattern where_clause?

//============================================================================
// Function invocation
//============================================================================

function_invocation: function_name "(" function_args? ")"

function_args: distinct_keyword? function_arg_list

function_arg_list: expression ("," expression)*

function_name: namespace_name? function_simple_name

namespace_name: IDENTIFIER ("." IDENTIFIER)* "."

function_simple_name: IDENTIFIER | NULLS_FIRST_KEYWORD | NULLS_LAST_KEYWORD

//============================================================================
// Case expression
//============================================================================

case_expression: simple_case | searched_case

simple_case: "CASE"i expression simple_when+ else_clause? "END"i

searched_case: "CASE"i searched_when+ else_clause? "END"i

simple_when: "WHEN"i when_operands "THEN"i expression

searched_when: "WHEN"i expression "THEN"i expression

when_operands: expression ("," expression)*

else_clause: "ELSE"i expression

//============================================================================
// List comprehension
//============================================================================

list_comprehension: "[" list_variable "IN"i expression list_filter? list_projection? "]"

list_variable: variable_name

list_filter: "WHERE"i expression

list_projection: "|" expression

//============================================================================
// Pattern comprehension
//============================================================================

pattern_comprehension: "[" pattern_comp_variable? pattern_element pattern_filter? pattern_projection "]"

pattern_comp_variable: variable_name "="

pattern_filter: "WHERE"i expression

pattern_projection: "|" expression

//============================================================================
// Reduce expression
//============================================================================

reduce_expression: "REDUCE"i "(" reduce_accumulator "," reduce_variable "IN"i expression "|" expression ")"

reduce_accumulator: variable_name "=" expression

reduce_variable: variable_name

//============================================================================
// Quantifier expressions (ALL, ANY, SINGLE, NONE)
//============================================================================

quantifier_expression: quantifier "(" quantifier_variable "IN"i expression "WHERE"i expression ")"

QUANTIFIER_KW: "ALL"i | "ANY"i | "SINGLE"i | "NONE"i
quantifier: QUANTIFIER_KW

quantifier_variable: variable_name

//============================================================================
// Map projection
//============================================================================

map_projection: variable_name "{" map_elements? "}"

map_elements: map_element ("," map_element)*

map_element: property_name ":" expression
           | property_name
           | "." "*"
           | "." property_name
           | variable_name

//============================================================================
// Literals
//============================================================================

?literal: number_literal
        | string_literal
        | boolean_literal
        | null_literal
        | list_literal
        | map_literal

number_literal: signed_number
              | unsigned_number

signed_number: SIGNED_INT
             | SIGNED_FLOAT
             | SIGNED_INFINITY
             | SIGNED_NAN

unsigned_number: UNSIGNED_INT
               | UNSIGNED_FLOAT
               | UNSIGNED_INFINITY
               | UNSIGNED_NAN

string_literal: STRING

boolean_literal: "TRUE"i  -> true
               | "FALSE"i -> false

null_literal: "NULL"i

list_literal: "[" list_elements? "]"

list_elements: expression ("," expression)*

map_literal: "{" map_entries? "}"

map_entries: map_entry ("," map_entry)*

map_entry: property_name ":" expression

//============================================================================
// Parameter
//============================================================================

parameter: "$" parameter_name

parameter_name: IDENTIFIER
              | UNSIGNED_INT

//============================================================================
// Variable name
//============================================================================

variable_name: IDENTIFIER | NULLS_FIRST_KEYWORD | NULLS_LAST_KEYWORD

//============================================================================
// Terminals
//============================================================================

// Comparison operators (higher priority to match multi-char operators first)
COMP_OP: "<=" | ">=" | "<>" | "=" | "<" | ">"

// Identifiers (regular or backtick-quoted)
IDENTIFIER: REGULAR_IDENTIFIER
          | ESCAPED_IDENTIFIER

NOT_KEYWORD.2: "NOT"i  // Higher priority than IDENTIFIER
ASC_KEYWORD.2: "ASC"i
ASCENDING_KEYWORD.2: "ASCENDING"i
DESC_KEYWORD.2: "DESC"i
DESCENDING_KEYWORD.2: "DESCENDING"i
NULLS_FIRST_KEYWORD.2: "FIRST"i
NULLS_LAST_KEYWORD.2: "LAST"i
REGULAR_IDENTIFIER: /[a-zA-Z_][a-zA-Z0-9_]*/

ESCAPED_IDENTIFIER: /`[^`]+`/

// Unsigned integers
UNSIGNED_INT: /\d+/
            | /\d+(_\d+)+/
            | /0[xX][0-9a-fA-F]+/
            | /0[xX][0-9a-fA-F]+(_[0-9a-fA-F]+)+/
            | /0[oO][0-7]+/
            | /0[oO][0-7]+(_[0-7]+)+/

// Signed integers
SIGNED_INT: /[+-]?\d+/
          | /[+-]?\d+(_\d+)+/

// Unsigned floats
UNSIGNED_FLOAT: /\d+\.\d+([eE][+-]?\d+)?/
              | /\d+[eE][+-]?\d+/
              | /\.\d+([eE][+-]?\d+)?/
              | /\d+\.\d+([eE][+-]?\d+)?[fFdD]/
              | /\d+[eE][+-]?\d+[fFdD]/
              | /\.\d+([eE][+-]?\d+)?[fFdD]/

// Signed floats
SIGNED_FLOAT: /[+-]?\d+\.\d+([eE][+-]?\d+)?/
            | /[+-]?\d+[eE][+-]?\d+/
            | /[+-]?\.\d+([eE][+-]?\d+)?/
            | /[+-]?\d+\.\d+([eE][+-]?\d+)?[fFdD]/
            | /[+-]?\d+[eE][+-]?\d+[fFdD]/
            | /[+-]?\.\d+([eE][+-]?\d+)?[fFdD]/

// Infinity and NaN
UNSIGNED_INFINITY: "INF"i | "INFINITY"i
SIGNED_INFINITY: /[+-]?(INF|INFINITY)/i
UNSIGNED_NAN: "NAN"i
SIGNED_NAN: /[+-]?NAN/i

// Strings (single or double quoted)
STRING: /'([^'\\\n]|\\.)*'/
      | /"([^"\\\n]|\\.)*"/

// Whitespace and comments
%import common.WS
%ignore WS

COMMENT: "//" /[^\n]*/
       | "/*" /(.|\n)*?/ "*/"
%ignore COMMENT
"""


class CypherASTTransformer(
    ClauseRulesMixin,
    PatternRulesMixin,
    FunctionRulesMixin,
    ExpressionRulesMixin,
    LiteralRulesMixin,
    Transformer,
):
    """Transform the parse tree into a comprehensive AST structure.

    Literal, parameter, and variable-name rules are inherited from
    :class:`~pycypher.grammar_rule_mixins.LiteralRulesMixin`.

    .. deprecated::
        This monolithic transformer is being replaced by specialized
        transformers in CompositeTransformer. Use CompositeTransformer instead.

    This transformer converts Lark's parse tree into a clean abstract
    syntax tree that covers the complete openCypher specification.

    The transformer implements the visitor pattern, with each method corresponding
    to a grammar rule. Methods are called automatically by Lark during tree traversal,
    receiving the child nodes as arguments and returning transformed AST nodes.

    The transformation process:
    1. Parse tree nodes are visited bottom-up (leaves first)
    2. Each transformer method processes its children (already transformed)
    3. Methods construct typed AST dictionaries with semantic information
    4. The final result is a complete AST ready for type checking and analysis
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise the deprecated transformer, emitting a deprecation warning.

        .. deprecated:: 0.0.19
            Use :class:`CompositeTransformer` instead.  Will be removed in 0.1.0.
        """
        from shared.deprecation import emit_deprecation

        emit_deprecation(
            "CypherASTTransformer",
            since="0.0.19",
            removed_in="0.1.0",
            alternative="CompositeTransformer",
        )
        super().__init__(*args, **kwargs)

    # ========================================================================
    # All clause, statement, and pattern methods are inherited from
    # ClauseRulesMixin, PatternRulesMixin, FunctionRulesMixin,
    # ExpressionRulesMixin, and LiteralRulesMixin
    # (see grammar_rule_mixins.py).
    # ========================================================================


_MAX_QUERY_LOG_LEN = 200


def _enforce_query_size_limit(query: str) -> None:
    """Reject queries that exceed the configured byte-size limit.

    Args:
        query: The raw Cypher query string.

    Raises:
        SecurityError: If the query exceeds ``MAX_QUERY_SIZE_BYTES``.

    """
    from pycypher.config import MAX_QUERY_SIZE_BYTES
    from pycypher.exceptions import SecurityError

    size = len(query.encode("utf-8"))
    if size > MAX_QUERY_SIZE_BYTES:
        msg = (
            f"Query size ({size:,} bytes) exceeds limit "
            f"({MAX_QUERY_SIZE_BYTES:,} bytes). "
            f"Adjust PYCYPHER_MAX_QUERY_SIZE_BYTES to increase."
        )
        raise SecurityError(msg)


def _log_parse_failure(query: str, exc: Exception) -> None:
    """Emit a structured WARNING log for a parse failure.

    Args:
        query: The original Cypher query string.
        exc: The Lark UnexpectedInput exception.
    """
    snippet = query[:_MAX_QUERY_LOG_LEN]
    if len(query) > _MAX_QUERY_LOG_LEN:
        snippet += "..."

    line = getattr(exc, "line", None)
    col = getattr(exc, "column", None)

    expected = getattr(exc, "expected", None)
    expected_str = ""
    if expected:
        clean = sorted(
            {t.strip("\"'") for t in expected if not t.startswith("_")},
        )
        if clean:
            expected_str = ", ".join(clean[:10])
            if len(clean) > 10:
                expected_str += f" (+{len(clean) - 10} more)"

    LOGGER.warning(
        "Parse failure: line=%s col=%s expected=[%s] query=%r",
        line,
        col,
        expected_str,
        snippet,
    )


class GrammarParser:
    """Parser for openCypher queries based on the BNF grammar specification.

    This class provides a high-level interface for parsing Cypher queries
    into abstract syntax trees (ASTs) using the Lark parsing library.

    Attributes:
        parser: The Lark parser instance.
        transformer: The AST transformer instance.

    Example:
        >>> parser = GrammarParser()
        >>> query = "MATCH (n:Person) RETURN n.name"
        >>> tree = parser.parse(query)
        >>> ast = parser.parse_to_ast(query)

    """

    parser: Lark
    transformer: CypherASTTransformer | CompositeTransformer

    # Class-level Lark instance cache keyed by debug flag.  Building the
    # Earley parser from the grammar string is the most expensive step
    # (~96 ms).  By caching at the class level, all ``GrammarParser``
    # instances with the same *debug* flag share a single ``Lark`` object,
    # avoiding redundant grammar compilation.
    _lark_cache: dict[bool, Lark] = {}
    _lark_cache_lock: threading.Lock = threading.Lock()
    _lark_cache_hits: int = 0
    _lark_cache_misses: int = 0

    def __init__(self, debug: bool = False) -> None:
        """Initialize the grammar parser.

        Args:
            debug: If True, enable debug mode for more verbose parsing errors.

        """
        with GrammarParser._lark_cache_lock:
            if debug not in GrammarParser._lark_cache:
                GrammarParser._lark_cache[debug] = Lark(
                    CYPHER_GRAMMAR,
                    parser="earley",  # Use Earley parser for better ambiguity handling
                    debug=debug,
                    maybe_placeholders=True,
                    ambiguity="explicit",  # Handle ambiguous parses explicitly
                )
                GrammarParser._lark_cache_misses += 1
                LOGGER.debug(
                    "Lark parser cache MISS (debug=%s); compiled new parser",
                    debug,
                )
            else:
                GrammarParser._lark_cache_hits += 1
                LOGGER.debug(
                    "Lark parser cache HIT (debug=%s)",
                    debug,
                )
            self.parser = GrammarParser._lark_cache[debug]
        self.transformer = CompositeTransformer()
        # Set up fallback to original transformer for unmigrated methods
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.transformer.set_fallback_transformer(CypherASTTransformer())
        # LRU cache for parsed ASTs — avoids re-parsing identical queries.
        # Size is capped to prevent unbounded memory growth from unique queries.
        from pycypher.config import AST_CACHE_MAX_ENTRIES

        self._ast_cache_max: int = AST_CACHE_MAX_ENTRIES
        self._ast_cache: collections.OrderedDict[str, dict[str, Any]] = (
            collections.OrderedDict()
        )
        self._ast_cache_hits: int = 0
        self._ast_cache_misses: int = 0
        self._ast_cache_evictions: int = 0

    def parse(self, query: str) -> Tree:
        """Parse a Cypher query into a parse tree.

        Args:
            query: The Cypher query string to parse.

        Returns:
            Tree: The Lark parse tree.

        Raises:
            CypherSyntaxError: If the query has syntax errors.
            SecurityError: If the query exceeds the size limit.

        """
        from lark.exceptions import UnexpectedInput

        from pycypher.exceptions import CypherSyntaxError

        _enforce_query_size_limit(query)
        try:
            return self.parser.parse(query)
        except UnexpectedInput as exc:
            _log_parse_failure(query, exc)
            raise CypherSyntaxError(query, exc) from exc

    def parse_to_ast(self, query: str) -> dict[str, Any]:
        """Parse a Cypher query into an AST.

        Results are cached by query string so that repeated parsing of the
        same query (common in ETL pipelines) returns instantly.

        Args:
            query: The Cypher query string to parse.

        Returns:
            Dict: The abstract syntax tree as a dictionary.

        Raises:
            CypherSyntaxError: If the query has syntax errors.

        """
        if query in self._ast_cache:
            # Move to end (most recently used) and return.
            self._ast_cache.move_to_end(query)
            self._ast_cache_hits += 1
            LOGGER.debug(
                "AST cache HIT (%d/%d, %.0f%% hit rate)",
                self._ast_cache_hits,
                self._ast_cache_hits + self._ast_cache_misses,
                100.0
                * self._ast_cache_hits
                / (self._ast_cache_hits + self._ast_cache_misses),
            )
            return self._ast_cache[query]
        self._ast_cache_misses += 1
        LOGGER.debug(
            "AST cache MISS (%d/%d entries cached)",
            len(self._ast_cache),
            self._ast_cache_max,
        )
        tree = self.parse(query)
        result = self.transformer.transform(tree)
        self._ast_cache[query] = result
        # Evict least-recently-used entries when the cache exceeds its limit.
        while len(self._ast_cache) > self._ast_cache_max:
            self._ast_cache.popitem(last=False)
            self._ast_cache_evictions += 1
        return result

    @property
    def cache_stats(self) -> dict[str, Any]:
        """Return cache hit/miss statistics for this parser instance.

        Returns:
            Dict with keys: ast_hits, ast_misses, ast_size, ast_max_size,
            ast_evictions, ast_hit_rate, lark_hits, lark_misses.

        """
        total = self._ast_cache_hits + self._ast_cache_misses
        return {
            "ast_hits": self._ast_cache_hits,
            "ast_misses": self._ast_cache_misses,
            "ast_size": len(self._ast_cache),
            "ast_max_size": self._ast_cache_max,
            "ast_evictions": self._ast_cache_evictions,
            "ast_hit_rate": (
                self._ast_cache_hits / total if total > 0 else 0.0
            ),
            "lark_hits": GrammarParser._lark_cache_hits,
            "lark_misses": GrammarParser._lark_cache_misses,
        }

    def parse_file(self, filepath: str | Path) -> Tree:
        """Parse a Cypher query from a file.

        Args:
            filepath: Path to the file containing the Cypher query.

        Returns:
            Tree: The Lark parse tree.

        Raises:
            FileNotFoundError: If *filepath* does not exist.
            IsADirectoryError: If *filepath* is a directory.
            ValueError: If the resolved path is not a regular file.

        """
        resolved = Path(filepath).resolve(strict=True)
        if not resolved.is_file():
            msg = f"Not a regular file: {resolved}"
            raise ValueError(msg)
        with open(resolved, encoding="utf-8") as f:
            return self.parse(f.read())

    def parse_file_to_ast(self, filepath: str | Path) -> dict[str, Any]:
        """Parse a Cypher query from a file into an AST.

        Args:
            filepath: Path to the file containing the Cypher query.

        Returns:
            Dict: The abstract syntax tree as a dictionary.

        """
        tree = self.parse_file(filepath)
        return self.transformer.transform(tree)

    def validate(self, query: str) -> bool:
        """Validate a Cypher query without returning the parse tree.

        Returns ``True`` when *query* is syntactically valid Cypher.
        Returns ``False`` only for genuine Lark parse errors
        (:exc:`~lark.exceptions.UnexpectedInput` and its subclasses:
        :exc:`~lark.exceptions.UnexpectedToken`,
        :exc:`~lark.exceptions.UnexpectedCharacters`,
        :exc:`~lark.exceptions.UnexpectedEOF`).

        Any *other* exception (e.g. a :exc:`~lark.exceptions.VisitError`
        wrapping an internal transformer bug, or a plain
        :exc:`AttributeError`) is **not** caught here and propagates to the
        caller.  This ensures that internal bugs surface immediately rather
        than being silently masked as "invalid query" responses.

        Args:
            query: The Cypher query string to validate.

        Returns:
            bool: True if the query is syntactically valid, False if it
            contains a genuine parse error.

        Raises:
            lark.exceptions.VisitError: If the grammar transformer raises
                an internal error while processing the parse tree.
            Exception: Any other non-parse exception raised during parsing.

        """
        from lark.exceptions import UnexpectedInput

        from pycypher.exceptions import CypherSyntaxError

        try:
            self.parse(query)
            return True
        except (UnexpectedInput, CypherSyntaxError):
            return False


@functools.cache
def get_default_parser(*, debug: bool = False) -> GrammarParser:
    """Return a cached ``GrammarParser`` instance.

    The Lark grammar compilation step inside ``GrammarParser.__init__`` is
    expensive (~0.175 s per call).  This factory builds the parser exactly
    once per unique ``debug`` flag value and returns the same object on every
    subsequent call, eliminating per-query re-instantiation overhead.

    Args:
        debug: Passed through to ``GrammarParser.__init__``.  Separate
            instances are cached for ``debug=True`` and ``debug=False``.

    Returns:
        GrammarParser: A fully-initialised, reusable parser instance.

    """
    return GrammarParser(debug=debug)


def main() -> None:
    """Command-line interface for the grammar parser."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Parse openCypher queries using the BNF grammar",
    )
    parser.add_argument("query", nargs="?", help="Cypher query to parse")
    parser.add_argument("-f", "--file", help="File containing Cypher query")
    parser.add_argument(
        "-a",
        "--ast",
        action="store_true",
        help="Output AST instead of parse tree",
    )
    parser.add_argument(
        "-j",
        "--json",
        action="store_true",
        help="Output as JSON",
    )
    parser.add_argument(
        "-v",
        "--validate",
        action="store_true",
        help="Only validate, don't output",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    args: argparse.Namespace = parser.parse_args()

    cypher_parser: GrammarParser = GrammarParser(debug=args.debug)

    # Get query from file or argument
    query: str
    if args.file:
        resolved = Path(args.file).resolve(strict=True)
        if not resolved.is_file():
            print(f"Error: not a regular file: {resolved}", file=sys.stderr)
            sys.exit(1)
        with open(resolved, encoding="utf-8") as f:
            query = f.read()
    elif args.query:
        query = args.query
    else:
        # Read from stdin with size limit to prevent memory exhaustion.
        from pycypher.config import MAX_QUERY_SIZE_BYTES

        query = sys.stdin.read(MAX_QUERY_SIZE_BYTES + 1)
        if len(query) > MAX_QUERY_SIZE_BYTES:
            print(
                f"Error: stdin exceeds {MAX_QUERY_SIZE_BYTES:,} byte limit "
                f"(set PYCYPHER_MAX_QUERY_SIZE_BYTES to increase)",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        if args.validate:
            is_valid = cypher_parser.validate(query)
            if is_valid:
                print("Valid")
                sys.exit(0)
            else:
                print("Invalid")
                sys.exit(1)

        if args.ast:
            ast_result = cypher_parser.parse_to_ast(query)
            if args.json:
                import json

                print(json.dumps(ast_result, indent=2))
            else:
                print(ast_result)

        else:
            parse_tree = cypher_parser.parse(query)
            if args.json:
                # Convert tree to dict for JSON serialization
                import json

                tree_dict = parse_tree.pretty()
                print(json.dumps({"parse_tree": tree_dict}))
            else:
                print(parse_tree.pretty())

    except FileNotFoundError as e:
        print(f"Error: file not found: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if args.debug:
            raise
        from lark.exceptions import UnexpectedInput

        if isinstance(e, UnexpectedInput):
            print(f"Syntax error: {e}", file=sys.stderr)
        else:
            print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
