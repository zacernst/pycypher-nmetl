"""openCypher Grammar Parser using Lark

This module implements a parser for the openCypher query language based on the
grammar specification in grammar.bnf. It uses the Lark parsing library to parse
Cypher queries into abstract syntax trees (ASTs).

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
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lark import Lark, Transformer, Tree

# Complete Lark grammar for openCypher
# Based on the official openCypher grammar specification
CYPHER_GRAMMAR = r"""
?start: cypher_query

//============================================================================
// Top-level query structure
//============================================================================

cypher_query: statement_list

statement_list: statement ("UNION"i "ALL"i? statement)*

statement: query_statement
         | update_statement
         | call_statement

query_statement: read_clause* return_clause

read_clause: match_clause
           | unwind_clause
           | with_clause
           | call_clause

update_statement: (match_clause | unwind_clause | with_clause)* update_clause+ return_clause?

update_clause: create_clause
             | merge_clause
             | delete_clause
             | set_clause
             | remove_clause

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

match_clause: "OPTIONAL"i? "MATCH"i pattern where_clause?

//============================================================================
// CREATE clause  
//============================================================================

create_clause: "CREATE"i pattern

//============================================================================
// MERGE clause
//============================================================================

merge_clause: "MERGE"i pattern merge_action*

merge_action: "ON"i ("MATCH"i | "CREATE"i) set_clause

//============================================================================
// DELETE clause
//============================================================================

delete_clause: ("DETACH"i)? "DELETE"i delete_items

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

with_clause: "WITH"i ("DISTINCT"i)? return_body where_clause? order_clause? skip_clause? limit_clause?

//============================================================================
// RETURN clause
//============================================================================

return_clause: "RETURN"i ("DISTINCT"i)? return_body order_clause? skip_clause? limit_clause?

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

order_item: expression order_direction?

order_direction: "ASC"i | "ASCENDING"i | "DESC"i | "DESCENDING"i

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

rel_types: ":" rel_type ("|:" rel_type)*

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

?comparison_expression: null_predicate_expression (COMP_OP null_predicate_expression)*

?null_predicate_expression: string_predicate_expression null_check_op?

null_check_op: "IS"i "NOT"i "NULL"i  -> is_not_null
             | "IS"i "NULL"i         -> is_null

?string_predicate_expression: add_expression (string_predicate_op add_expression)*

string_predicate_op: "STARTS"i "WITH"i
                   | "ENDS"i "WITH"i
                   | "CONTAINS"i
                   | "=~"
                   | "IN"i

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

slicing: "[" expression? ".." expression? "]"

?atom_expression: literal
                | parameter
                | variable_name
                | count_star
                | function_invocation
                | exists_expression
                | list_comprehension
                | pattern_comprehension
                | case_expression
                | reduce_expression
                | quantifier_expression
                | map_projection
                | "(" expression ")"

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

function_args: ("DISTINCT"i)? function_arg_list

function_arg_list: expression ("," expression)*

function_name: namespace_name? function_simple_name

namespace_name: IDENTIFIER ("." IDENTIFIER)* "."

function_simple_name: IDENTIFIER

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

quantifier: "ALL"i | "ANY"i | "SINGLE"i | "NONE"i

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

variable_name: IDENTIFIER

//============================================================================
// Terminals
//============================================================================

// Comparison operators (higher priority to match multi-char operators first)
COMP_OP: "<=" | ">=" | "<>" | "=" | "<" | ">"

// Identifiers (regular or backtick-quoted)
IDENTIFIER: REGULAR_IDENTIFIER
          | ESCAPED_IDENTIFIER

NOT_KEYWORD.2: "NOT"i  // Higher priority than IDENTIFIER
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


class CypherASTTransformer(Transformer):
    """Transform the parse tree into a comprehensive AST structure.

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

    # ========================================================================
    # Top-level query structure
    # ========================================================================

    def add_op(self, args: List[Any]) -> str:
        """Extract operator from add_op rule."""
        return str(args[0])

    def mult_op(self, args: List[Any]) -> str:
        """Extract operator from mult_op rule."""
        return str(args[0])

    def pow_op(self, args: List[Any]) -> str:
        """Extract operator from pow_op rule."""
        return str(args[0])

    def unary_op(self, args: List[Any]) -> str:
        """Extract operator from unary_op rule."""
        return str(args[0])

    def cypher_query(self, args: List[Any]) -> Dict[str, Any]:
        """Transform the root query node.

        This is the entry point for all Cypher queries. It wraps all statements
        in a Query container, which is necessary for handling multi-statement
        queries (e.g., multiple queries joined by UNION).

        Args:
            args: List of statement nodes from the statement_list rule.

        Returns:
            Dict with type "Query" containing all statements in the query.
        """
        return {"type": "Query", "statements": args}

    def statement_list(self, args: List[Any]) -> List[Any]:
        """Transform a list of statements.

        Statements can be connected via UNION or UNION ALL operators.
        This method normalizes the list structure for easier processing.

        Args:
            args: Individual statement nodes.

        Returns:
            List of statement dictionaries.
        """
        return list(args)

    def query_statement(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a read-only query statement (MATCH...RETURN).

        Query statements consist of read clauses (MATCH, UNWIND, WITH) followed
        by an optional RETURN clause. This separation is necessary for the AST
        to distinguish between read-only queries and update operations.

        Args:
            args: Mix of read clause nodes and an optional ReturnStatement.

        Returns:
            Dict with type "QueryStatement" containing separated clauses and return.
        """
        read_clauses = [
            a
            for a in args
            if not isinstance(a, dict) or a.get("type") != "ReturnStatement"
        ]
        return_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "ReturnStatement"
            ),
            None,
        )
        return {
            "type": "QueryStatement",
            "clauses": read_clauses,
            "return": return_clause,
        }

    def update_statement(self, args: List[Any]) -> Dict[str, Any]:
        """Transform an update statement (CREATE/MERGE/DELETE/SET/REMOVE).

        Update statements can have prefix clauses (MATCH/WITH for context),
        one or more update operations, and an optional RETURN clause.
        Proper categorization is necessary for execution planning and validation.

        Args:
            args: Mix of prefix clauses, update clauses, and optional return.

        Returns:
            Dict with type "UpdateStatement" containing categorized clauses.
        """
        prefix_clauses = []
        update_clauses = []
        return_clause = None

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnStatement":
                    return_clause = arg
                elif arg.get("type") in [
                    "CreateClause",
                    "MergeClause",
                    "DeleteClause",
                    "SetClause",
                    "RemoveClause",
                ]:
                    update_clauses.append(arg)
                else:
                    prefix_clauses.append(arg)

        return {
            "type": "UpdateStatement",
            "prefix": prefix_clauses,
            "updates": update_clauses,
            "return": return_clause,
        }

    def update_clause(self, args: List[Any]) -> Optional[Any]:
        """Pass through update clauses without modification.

        This method exists because the grammar has an update_clause rule that
        acts as a union of different update types. Pass-through is necessary
        to avoid adding unnecessary wrapper nodes in the AST.

        Args:
            args: Single update clause node (CREATE/MERGE/DELETE/SET/REMOVE).

        Returns:
            The update clause node unchanged.
        """
        return args[0] if args else None

    def read_clause(self, args: List[Any]) -> Optional[Any]:
        """Pass through read clauses without modification.

        This method exists because the grammar has a read_clause rule that
        acts as a union of different read types. Pass-through is necessary
        to avoid adding unnecessary wrapper nodes in the AST.

        Args:
            args: Single read clause node (MATCH/UNWIND/WITH/CALL).

        Returns:
            The read clause node unchanged.
        """
        return args[0] if args else None

    def _ambig(self, args: List[Any]) -> Any:
        """Handle ambiguous parses by selecting the most specific interpretation.

        The Earley parser can produce multiple valid parse trees for ambiguous
        grammar rules. This method implements a disambiguation strategy that
        prefers structured semantic nodes over primitive values, ensuring the
        AST contains the most useful representation for type checking.

        Priority order:
        1. Not expression nodes (highest priority for negation)
        2. Other structured dictionary nodes (semantic information)
        3. Primitive values (fallback)

        This is necessary to resolve conflicts like "NOT" being both a keyword
        and a potential identifier, or property lookups vs. simple variables.

        Args:
            args: List of alternative parse results for the same input.

        Returns:
            The most semantically rich parse result.
        """
        if not args:
            return None
        # Prefer dicts (structured data) over primitives
        structured = [a for a in args if isinstance(a, dict)]
        if structured:
            # Prefer nodes with 'Not' type (for NOT expressions)
            not_nodes = [a for a in structured if a.get("type") == "Not"]
            if not_nodes:
                return not_nodes[0]
            # Otherwise return first structured node
            return structured[0]
        # Return first argument if no structured data
        return args[0]

    def statement(self, args: List[Any]) -> Optional[Any]:
        """Pass through statement nodes.

        Statements can be query statements, update statements, or call statements.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single statement node.

        Returns:
            The statement node unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # CALL statement
    # ========================================================================

    def call_statement(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a standalone CALL statement for procedure invocation.

        CALL statements invoke stored procedures, which can have arguments and
        yield results. This is necessary for extending Cypher with custom logic.

        Args:
            args: [procedure_reference, optional explicit_args, optional yield_clause]

        Returns:
            Dict with type "CallStatement" containing procedure info, args, and yield.
        """
        return {
            "type": "CallStatement",
            "procedure": args[0] if args else None,
            "args": args[1] if len(args) > 1 else None,
            "yield": args[2] if len(args) > 2 else None,
        }

    def call_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a CALL clause within a larger query.

        Call clauses can appear as read clauses, allowing procedure results to
        be used in subsequent query parts. Different type from call_statement
        is necessary to distinguish standalone vs. embedded calls.

        Args:
            args: [procedure_reference, explicit_args, yield_clause]

        Returns:
            Dict with type "CallClause" containing procedure info, args, and yield.
        """
        return {
            "type": "CallClause",
            "procedure": args[0] if args else None,
            "args": args[1] if len(args) > 1 else None,
            "yield": args[2] if len(args) > 2 else None,
        }

    def procedure_reference(self, args: List[Any]) -> Optional[Any]:
        """Extract procedure name reference.

        Procedures are referenced by their function name, which may include
        namespace qualification. Pass-through is necessary to avoid wrapping.

        Args:
            args: Function name (possibly namespaced).

        Returns:
            Procedure name string or dict.
        """
        return args[0] if args else None

    def explicit_args(self, args: List[Any]) -> List[Any]:
        """Transform explicit procedure arguments list.

        Procedures can accept arguments just like functions. Converting to
        a list is necessary for consistent handling of variadic arguments.

        Args:
            args: Expression nodes for each argument.

        Returns:
            List of argument expressions.
        """
        return list(args) if args else []

    def yield_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a YIELD clause that selects procedure output fields.

        YIELD specifies which fields from procedure results to expose, with
        optional WHERE filtering. This structure is necessary for controlling
        procedure output visibility.

        Args:
            args: [yield_items or "*", optional where_clause]

        Returns:
            Dict with type "YieldClause" containing items and optional where filter.
        """
        items = args[0] if args else None
        where = args[1] if len(args) > 1 else None
        return {"type": "YieldClause", "items": items, "where": where}

    def yield_items(self, args: List[Any]) -> List[Any]:
        """Transform list of yielded fields.

        Multiple fields can be yielded from a procedure. List normalization
        is necessary for consistent iteration during execution.

        Args:
            args: Individual yield_item nodes.

        Returns:
            List of yield item dictionaries.
        """
        return list(args) if args else []

    def yield_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single yielded field with optional alias.

        Fields can be renamed using AS. Separating field and alias is
        necessary for proper symbol table construction.

        Args:
            args: [field_name] or [field_name, alias]

        Returns:
            Dict with field name and optional alias.
        """
        if len(args) == 1:
            return {"field": args[0]}
        return {"field": args[0], "alias": args[1]}

    def field_name(self, args: List[Any]) -> str:
        """Extract field name identifier.

        Field names in YIELD clauses reference procedure output columns.
        Stripping backticks is necessary to normalize identifier representation.

        Args:
            args: IDENTIFIER token.

        Returns:
            Field name string with backticks removed.
        """
        return str(args[0]).strip("`")

    # ========================================================================
    # MATCH clause
    # ========================================================================

    def match_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a MATCH clause for pattern matching.

        MATCH finds existing graph patterns. The OPTIONAL modifier makes the match
        non-failing (like SQL LEFT JOIN). WHERE filters matched patterns.
        Separating these components is necessary for query optimization and execution.

        Args:
            args: Mix of OPTIONAL keyword, pattern, and optional where_clause.

        Returns:
            Dict with type "MatchClause" containing optional flag, pattern, and where.
        """
        optional = any(
            str(a).upper() == "OPTIONAL" for a in args if isinstance(a, str)
        )
        pattern = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Pattern"
            ),
            None,
        )
        where = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "WhereClause"
            ),
            None,
        )
        return {
            "type": "MatchClause",
            "optional": optional,
            "pattern": pattern,
            "where": where,
        }

    # ========================================================================
    # CREATE clause
    # ========================================================================

    def create_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a CREATE clause for creating new graph elements.

        CREATE adds new nodes and relationships to the graph based on a pattern.
        Wrapping the pattern in a typed node is necessary to distinguish CREATE
        from MATCH and other pattern-using clauses during execution.

        Args:
            args: Pattern to create.

        Returns:
            Dict with type "CreateClause" containing the creation pattern.
        """
        return {"type": "CreateClause", "pattern": args[0] if args else None}

    # ========================================================================
    # MERGE clause
    # ========================================================================

    def merge_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a MERGE clause for create-or-match operations.

        MERGE ensures a pattern exists, creating it if necessary. This is atomic
        and prevents duplicates. ON MATCH/CREATE actions allow different behavior
        based on whether the pattern existed. This structure is necessary for
        conditional update logic.

        Args:
            args: [pattern, optional merge_action nodes]

        Returns:
            Dict with type "MergeClause" containing pattern and conditional actions.
        """
        pattern = args[0] if args else None
        actions = args[1:] if len(args) > 1 else []
        return {"type": "MergeClause", "pattern": pattern, "actions": actions}

    def merge_action(self, args: List[Any]) -> Dict[str, Any]:
        """Transform ON MATCH or ON CREATE action within MERGE.

        These actions execute conditionally based on whether MERGE found or
        created the pattern. Distinguishing the trigger type is necessary for
        correct execution semantics.

        Args:
            args: ["MATCH" or "CREATE" keyword, set_clause]

        Returns:
            Dict with type "MergeAction" specifying trigger type and SET operation.
        """
        on_type = (
            "match"
            if any(
                str(a).upper() == "MATCH" for a in args if isinstance(a, str)
            )
            else "create"
        )
        set_clause = next((a for a in args if isinstance(a, dict)), None)
        return {"type": "MergeAction", "on": on_type, "set": set_clause}

    # ========================================================================
    # DELETE clause
    # ========================================================================

    def delete_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a DELETE clause for removing graph elements.

        DELETE removes nodes and relationships. DETACH DELETE also removes
        relationships connected to deleted nodes, preventing orphaned edges.
        This distinction is necessary for safe cascading deletion.

        Args:
            args: [optional "DETACH" keyword, delete_items]

        Returns:
            Dict with type "DeleteClause" containing detach flag and items to delete.
        """
        detach = any(
            str(a).upper() == "DETACH" for a in args if isinstance(a, str)
        )
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {
            "type": "DeleteClause",
            "detach": detach,
            "items": items.get("items", []),
        }

    def delete_items(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform comma-separated list of expressions to delete.

        Multiple items can be deleted in one clause. Wrapping in a dict is
        necessary to distinguish the list from other argument types.

        Args:
            args: Expression nodes identifying items to delete.

        Returns:
            Dict containing list of items.
        """
        return {"items": list(args)}

    # ========================================================================
    # SET clause
    # ========================================================================

    def set_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a SET clause for updating graph properties.

        SET modifies node/relationship properties and labels. Multiple set
        operations can be combined in one clause. Extracting the items list
        is necessary for execution planning.

        Args:
            args: set_items wrapper containing list of set operations.

        Returns:
            Dict with type "SetClause" containing list of set operations.
        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "SetClause", "items": items.get("items", [])}

    def set_items(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform comma-separated list of SET operations.

        Wrapping in a dict is necessary to pass the list through the
        transformer chain without flattening.

        Args:
            args: Individual set_item nodes.

        Returns:
            Dict containing list of set operations.
        """
        return {"items": list(args)}

    def set_item(self, args: List[Any]) -> Optional[Any]:
        """Pass through individual SET operation.

        The grammar has set_item as a union of different set types.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single set operation (property/labels/all properties/add properties).

        Returns:
            The set operation node unchanged.
        """
        return args[0] if args else None

    def set_property_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single property assignment (e.g., SET n.age = 30).

        Property updates are the most common SET operation. Separating variable,
        property name, and value is necessary for validation and execution.

        Args:
            args: [variable_name, property_lookup, expression]

        Returns:
            Dict with type "SetProperty" containing variable, property, and value.
        """
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        value = args[2] if len(args) > 2 else None
        return {
            "type": "SetProperty",
            "variable": variable,
            "property": prop,
            "value": value,
        }

    def set_labels_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform label assignment (e.g., SET n:Person:Employee).

        Labels are added to nodes for categorization. Separate handling is
        necessary because labels are not stored as properties.

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "SetLabels" containing variable and label list.
        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "SetLabels", "variable": variable, "labels": labels}

    def set_all_properties_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform property map replacement (e.g., SET n = {name: 'Alice'}).

        This replaces ALL properties on a node/relationship with a new map.
        Distinct type is necessary to warn users about potential data loss.

        Args:
            args: [variable_name, expression]

        Returns:
            Dict with type "SetAllProperties" for complete property replacement.
        """
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {
            "type": "SetAllProperties",
            "variable": variable,
            "value": value,
        }

    def add_all_properties_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform property map merge (e.g., SET n += {age: 30}).

        This merges new properties with existing ones without removing others.
        Distinct type from SetAllProperties is necessary for different semantics.

        Args:
            args: [variable_name, expression]

        Returns:
            Dict with type "AddAllProperties" for additive property merge.
        """
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {
            "type": "AddAllProperties",
            "variable": variable,
            "value": value,
        }

    # ========================================================================
    # REMOVE clause
    # ========================================================================

    def remove_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a REMOVE clause for deleting properties or labels.

        REMOVE deletes properties or labels without deleting the node/relationship
        itself. This is distinct from DELETE which removes entire elements.

        Args:
            args: remove_items wrapper containing list of remove operations.

        Returns:
            Dict with type "RemoveClause" containing list of remove operations.
        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "RemoveClause", "items": items.get("items", [])}

    def remove_items(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform comma-separated list of REMOVE operations.

        Wrapping in a dict is necessary to pass the list through the
        transformer chain.

        Args:
            args: Individual remove_item nodes.

        Returns:
            Dict containing list of remove operations.
        """
        return {"items": list(args)}

    def remove_item(self, args: List[Any]) -> Optional[Any]:
        """Pass through individual REMOVE operation.

        The grammar has remove_item as a union of property and label removal.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single remove operation (property or labels).

        Returns:
            The remove operation node unchanged.
        """
        return args[0] if args else None

    def remove_property_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform property removal (e.g., REMOVE n.age).

        Removes a single property from a node/relationship. Separating variable
        and property is necessary for validation.

        Args:
            args: [variable_name, property_lookup]

        Returns:
            Dict with type "RemoveProperty" containing variable and property name.
        """
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        return {
            "type": "RemoveProperty",
            "variable": variable,
            "property": prop,
        }

    def remove_labels_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform label removal (e.g., REMOVE n:Person).

        Removes labels from a node. Separate handling from properties is
        necessary because labels are metadata, not property values.

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "RemoveLabels" containing variable and labels to remove.
        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "RemoveLabels", "variable": variable, "labels": labels}

    # ========================================================================
    # UNWIND clause
    # ========================================================================

    def unwind_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform an UNWIND clause for list expansion.

        UNWIND expands a list into individual rows, creating a new variable for
        each element. This is necessary for processing collections in Cypher,
        similar to SQL's UNNEST or CROSS JOIN LATERAL.

        Args:
            args: [expression (the list), variable_name (for each element)]

        Returns:
            Dict with type "UnwindClause" containing source expression and variable.
        """
        expr = args[0] if args else None
        var = args[1] if len(args) > 1 else None
        return {"type": "UnwindClause", "expression": expr, "variable": var}

    # ========================================================================
    # WITH clause
    # ========================================================================

    def with_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a WITH clause for query chaining and variable passing.

        WITH acts like a pipe operator, passing selected variables to the next
        query part while filtering, sorting, and limiting. This is necessary for
        multi-stage queries where intermediate results need transformation.

        Unlike RETURN (which ends a query), WITH continues processing. The DISTINCT,
        WHERE, ORDER BY, SKIP, and LIMIT modifiers control what gets passed forward.

        Args:
            args: Mix of DISTINCT keyword, return body, and optional clauses.

        Returns:
            Dict with type "WithClause" containing all components for variable passing.
        """
        distinct = any(
            str(a).upper() == "DISTINCT" for a in args if isinstance(a, str)
        )
        items = []
        where = None
        order = None
        skip = None
        limit = None

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnBody":
                    # Extract items from the return body
                    items = arg.get("items", [])
                elif arg.get("type") == "WhereClause":
                    where = arg
                elif arg.get("type") == "OrderClause":
                    order = arg
                elif arg.get("type") == "SkipClause":
                    skip = arg
                elif arg.get("type") == "LimitClause":
                    limit = arg
            elif isinstance(arg, list) and not items:
                # return_body returns a list of items directly
                items = arg
            elif arg == "*":
                items = "*"

        return {
            "type": "WithClause",
            "distinct": distinct,
            "items": items,
            "where": where,
            "order": order,
            "skip": skip,
            "limit": limit,
        }

    # ========================================================================
    # RETURN clause
    # ========================================================================

    def return_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a RETURN clause for query output specification.

        RETURN determines what a query outputs, similar to SQL SELECT. It can
        return specific expressions (with aliases), or * for all variables.
        DISTINCT, ORDER BY, SKIP, and LIMIT control the result set.

        This structure is necessary to separate output specification from ordering
        and pagination, enabling query optimization and execution planning.

        Args:
            args: Mix of DISTINCT keyword, return body/items/*, and optional clauses.

        Returns:
            Dict with type "ReturnStatement" containing all output specifications.
        """
        distinct = any(
            str(a).upper() == "DISTINCT" for a in args if isinstance(a, str)
        )
        body = None
        order = None
        skip = None
        limit = None

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "OrderClause":
                    order = arg
                elif arg.get("type") == "SkipClause":
                    skip = arg
                elif arg.get("type") == "LimitClause":
                    limit = arg
                elif body is None:
                    body = arg
            elif isinstance(arg, list) and body is None:
                # return_body returns a list of items
                body = {"type": "ReturnBody", "items": arg}
            elif arg == "*":
                body = "*"

        return {
            "type": "ReturnStatement",
            "distinct": distinct,
            "body": body,
            "order": order,
            "skip": skip,
            "limit": limit,
        }

    def return_body(self, args: List[Any]) -> Union[str, List[Any]]:
        """Extract the body of a RETURN clause (items or *).

        This handles the special case of RETURN * vs. RETURN item1, item2.
        Pass-through is necessary to avoid double-wrapping the items list.

        Args:
            args: Either "*" string or return_items list.

        Returns:
            Either "*" string or list of return items.
        """
        if args and args[0] == "*":
            return "*"
        # args[0] is already a list from return_items
        return args[0] if args else []

    def return_items(self, args: List[Any]) -> List[Any]:
        """Transform comma-separated list of return items.

        Normalizing to a list is necessary for consistent iteration during
        output construction.

        Args:
            args: Individual return_item nodes.

        Returns:
            List of return item dictionaries.
        """
        return list(args) if args else []

    def return_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single return item with optional alias.

        Return items can be aliased using AS (e.g., RETURN n.name AS fullName).
        Wrapping with type "ReturnItem" is necessary for type checking to
        distinguish expressions from their return metadata.

        Args:
            args: [expression] or [expression, alias]

        Returns:
            Dict with type "ReturnItem" containing expression and optional alias.
        """
        if len(args) == 1:
            return {"type": "ReturnItem", "expression": args[0], "alias": None}
        return {"type": "ReturnItem", "expression": args[0], "alias": args[1]}

    def return_alias(self, args: List[Any]) -> str:
        """Extract return item alias identifier.

        Aliases define the output column names. Stripping backticks is necessary
        to normalize identifier representation.

        Args:
            args: IDENTIFIER token.

        Returns:
            Alias string with backticks removed.
        """
        return str(args[0]).strip("`")

    # ========================================================================
    # WHERE clause
    # ========================================================================

    def where_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a WHERE clause for filtering.

        WHERE filters graph patterns (in MATCH) or intermediate results (in WITH).
        Wrapping the condition is necessary to attach it to the appropriate clause.

        Args:
            args: Single boolean expression for the filter condition.

        Returns:
            Dict with type "WhereClause" containing the filter expression.
        """
        return {"type": "WhereClause", "condition": args[0] if args else None}

    # ========================================================================
    # ORDER BY clause
    # ========================================================================

    def order_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform an ORDER BY clause for result sorting.

        ORDER BY sorts results by one or more expressions, each with a direction.
        This structure is necessary for execution planning and index utilization.

        Args:
            args: order_items wrapper containing list of sort specifications.

        Returns:
            Dict with type "OrderClause" containing list of order items.
        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "OrderClause", "items": items.get("items", [])}

    def order_items(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform comma-separated list of ORDER BY items.

        Wrapping in a dict is necessary to pass the list through the transformer.

        Args:
            args: Individual order_item nodes.

        Returns:
            Dict containing list of order specifications.
        """
        return {"items": list(args)}

    def order_item(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single ORDER BY item with optional direction.

        Each item specifies an expression and sort direction (ASC/DESC).
        Default is ascending. Separating these is necessary for sort planning.

        Args:
            args: [expression, optional direction]

        Returns:
            Dict with expression and direction ("asc" or "desc").
        """
        expr = args[0] if args else None
        direction = args[1] if len(args) > 1 else "asc"
        return {"expression": expr, "direction": direction}

    def order_direction(self, args: List[Any]) -> str:
        """Normalize ORDER BY direction keywords.

        Supports ASC/ASCENDING and DESC/DESCENDING. Normalization is necessary
        for consistent execution regardless of which keyword form is used.

        Args:
            args: Direction keyword token (optional).

        Returns:
            Normalized direction string: "asc" or "desc".
        """
        if not args:
            return "asc"
        d = str(args[0]).upper()
        return "desc" if d in ["DESC", "DESCENDING"] else "asc"

    # ========================================================================
    # SKIP and LIMIT
    # ========================================================================

    def skip_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a SKIP clause for result pagination.

        SKIP skips the first N results, enabling pagination. The expression
        is evaluated at runtime, allowing parameterized pagination.

        Args:
            args: Expression evaluating to number of rows to skip.

        Returns:
            Dict with type "SkipClause" containing skip count expression.
        """
        return {"type": "SkipClause", "value": args[0] if args else None}

    def limit_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a LIMIT clause for result set size restriction.

        LIMIT restricts output to N results. Combined with SKIP, this enables
        efficient pagination. Expression evaluation is necessary for parameterization.

        Args:
            args: Expression evaluating to maximum number of rows to return.

        Returns:
            Dict with type "LimitClause" containing limit count expression.
        """
        return {"type": "LimitClause", "value": args[0] if args else None}

    # ========================================================================
    # Pattern matching
    # ========================================================================

    def pattern(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a graph pattern (one or more paths).

        Patterns describe graph structures to match or create. Multiple comma-separated
        paths can be specified in one pattern (e.g., (a)-[]->(b), (c)-[]->(d)).

        This wrapper is necessary to distinguish pattern collections from individual
        paths during matching and creation operations.

        Args:
            args: List of path_pattern nodes.

        Returns:
            Dict with type "Pattern" containing list of paths.
        """
        return {"type": "Pattern", "paths": list(args)}

    def path_pattern(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single path pattern with optional variable assignment.

        Paths can be assigned to variables (e.g., p = (a)-[]->(b)) for later reference.
        This is necessary for algorithms that operate on entire paths rather than
        individual nodes/relationships.

        Args:
            args: Optional variable name followed by pattern_element.

        Returns:
            Dict with type "PathPattern" containing optional variable and element.
        """
        variable = None
        element = None
        for arg in args:
            if isinstance(arg, str) and variable is None:
                variable = arg
            else:
                element = arg
        return {
            "type": "PathPattern",
            "variable": variable,
            "element": element,
        }

    def pattern_element(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a pattern element (sequence of nodes and relationships).

        Pattern elements describe connected graph structures, alternating between
        nodes and relationships. The parts list maintains order, which is necessary
        for directional relationship matching.

        Args:
            args: Alternating node_pattern and relationship_pattern nodes.

        Returns:
            Dict with type "PatternElement" containing ordered list of parts.
        """
        return {"type": "PatternElement", "parts": list(args)}

    def shortest_path(self, args: List[Any]) -> Dict[str, Any]:
        """Transform SHORTESTPATH or ALLSHORTESTPATHS function.

        Shortest path functions find minimal-length paths between nodes.
        ALLSHORTESTPATHS finds all paths with minimal length. This distinction
        is necessary for different algorithmic execution strategies.

        Args:
            args: Mix of function name keyword and pattern parts.

        Returns:
            Dict with type "ShortestPath" containing all flag and pattern parts.
        """
        all_shortest = any(
            "ALL" in str(a).upper() for a in args if isinstance(a, str)
        )
        nodes_and_rel = [a for a in args if isinstance(a, dict)]
        return {
            "type": "ShortestPath",
            "all": all_shortest,
            "parts": nodes_and_rel,
        }

    # ========================================================================
    # Node pattern
    # ========================================================================

    def node_pattern(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a node pattern (node in parentheses).

        Node patterns describe nodes to match or create, with optional variable,
        labels, properties, and WHERE clause. Parentheses syntax () is required.

        The filler dict is merged into the node pattern to avoid extra nesting.
        This flattening is necessary for simpler AST traversal.

        Args:
            args: Optional node_pattern_filler dict with node components.

        Returns:
            Dict with type "NodePattern" containing variable, labels, properties, where.
        """
        filler = args[0] if args else {}
        return (
            {"type": "NodePattern", **filler}
            if isinstance(filler, dict)
            else {"type": "NodePattern", "filler": filler}
        )

    def node_pattern_filler(self, args: List[Any]) -> Dict[str, Any]:
        """Extract components from inside node parentheses.

        Node patterns can contain: variable, labels, properties, and WHERE.
        These components are parsed separately and need to be combined into
        a single dict for the node pattern. Type detection is necessary to
        distinguish properties from other structured components.

        Args:
            args: Mix of variable name string and component dicts (labels/properties/where).

        Returns:
            Dict containing all node components with appropriate keys.
        """
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                # Check if this is a labels or where dict (has known keys)
                if "labels" in arg or "where" in arg:
                    filler.update(arg)
                # Check if this is a properties object (no special keys)
                elif "type" not in arg and arg:
                    # This is a properties dict - store it as 'properties'
                    filler["properties"] = arg
                else:
                    # Other structured objects
                    filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def node_labels(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform node label expressions.

        Nodes can have multiple labels (e.g., :Person:Employee). Labels can also
        use boolean logic (e.g., :Person|Employee for OR). Wrapping in a dict is
        necessary to distinguish labels from other node components.

        Args:
            args: List of label_expression nodes.

        Returns:
            Dict with "labels" key containing list of label expressions.
        """
        return {"labels": list(args)}

    def label_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform a single label expression.

        Labels can be prefixed with : or IS. For simple cases, just return the
        label. Complex cases with multiple parts need wrapping.

        Args:
            args: Label term(s) from the expression.

        Returns:
            Single label term or dict with type "LabelExpression" for complex cases.
        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelExpression", "parts": list(args)}

    def label_term(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform label OR expressions (e.g., Person|Employee).

        Label terms can use | for alternation (match either label). Single labels
        pass through; multiple labels are wrapped in a LabelOr node.

        Args:
            args: Label factor nodes separated by |.

        Returns:
            Single factor or dict with type "LabelOr" for multiple factors.
        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelOr", "terms": list(args)}

    def label_factor(self, args: List[Any]) -> Optional[Any]:
        """Pass through label factors (possibly negated with !).

        Label factors can have ! negation prefix. Pass-through avoids extra wrapping.

        Args:
            args: Single label_primary node.

        Returns:
            The label primary unchanged.
        """
        return args[0] if args else None

    def label_primary(self, args: List[Any]) -> Optional[Any]:
        """Pass through primary label expressions.

        Primary labels are either names, parenthesized expressions, or % (any label).
        Pass-through avoids unnecessary nesting.

        Args:
            args: Label name or grouped expression.

        Returns:
            The label value unchanged.
        """
        return args[0] if args else None

    def label_name(self, args: List[Any]) -> str:
        """Extract label name identifier.

        Label names can have leading : from grammar rules. Stripping both : and
        backticks is necessary for normalized label comparison.

        Args:
            args: IDENTIFIER token possibly with leading :.

        Returns:
            Label name string with : and backticks removed.
        """
        return str(args[0]).lstrip(":").strip("`")

    def node_properties(self, args: List[Any]) -> Optional[Any]:
        """Pass through node properties or WHERE clause.

        Node properties can be specified as a map literal or extracted via WHERE.
        Pass-through avoids extra wrapping.

        Args:
            args: Properties dict or WHERE clause.

        Returns:
            The properties/where node unchanged.
        """
        return args[0] if args else None

    def node_where(self, args: List[Any]) -> Dict[str, Any]:
        """Transform inline WHERE clause within node pattern.

        WHERE can filter node properties inline (e.g., (n WHERE n.age > 30)).
        Wrapping in a dict is necessary to distinguish from property maps.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.
        """
        return {"where": args[0] if args else None}

    # ========================================================================
    # Relationship pattern
    # ========================================================================

    def relationship_pattern(self, args: List[Any]) -> Optional[Any]:
        """Pass through relationship patterns.

        Relationships are parsed by direction-specific rules (left/right/both/any).
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single relationship node from directional rules.

        Returns:
            The relationship pattern unchanged.
        """
        return args[0] if args else None

    def full_rel_left(self, args: List[Any]) -> Dict[str, Any]:
        """Transform left-pointing relationship (<--).

        Left direction means the relationship points from right to left in the
        pattern. This distinction is necessary for directed graph traversal.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "left", and details.
        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "left", **detail}

    def full_rel_right(self, args: List[Any]) -> Dict[str, Any]:
        """Transform right-pointing relationship (-->).

        Right direction means the relationship points from left to right in the
        pattern. This is the most common relationship direction.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "right", and details.
        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "right", **detail}

    def full_rel_both(self, args: List[Any]) -> Dict[str, Any]:
        """Transform bidirectional relationship (<-->).

        Both direction means the relationship can be traversed in either direction.
        This is uncommon but supported for specific use cases.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "both", and details.
        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "both", **detail}

    def full_rel_any(self, args: List[Any]) -> Dict[str, Any]:
        """Transform undirected relationship (---).

        Any direction means the relationship can point either way. This is useful
        for matching symmetric relationships where direction doesn't matter.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "any", and details.
        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "any", **detail}

    def rel_detail(self, args: List[Any]) -> Dict[str, Any]:
        """Extract details from inside relationship brackets [...].

        Pass-through is necessary to avoid adding wrapper nodes.

        Args:
            args: rel_filler dict with relationship components.

        Returns:
            The filler dict unchanged, or empty dict if no details.
        """
        return args[0] if args else {}

    def rel_filler(self, args: List[Any]) -> Dict[str, Any]:
        """Extract components from inside relationship brackets.

        Relationships can have: variable, types, properties, path length, and WHERE.
        These are combined into a single dict. This merging is necessary to
        flatten the AST structure.

        Args:
            args: Mix of variable name string and component dicts.

        Returns:
            Dict containing all relationship components.
        """
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def rel_types(self, args: List[Any]) -> Dict[str, List[Any]]:
        """Transform relationship type constraints.

        Relationships can be constrained to one or more types (e.g., [:KNOWS|:LIKES]).
        Wrapping in a dict is necessary to distinguish from other components.

        Args:
            args: List of relationship type names.

        Returns:
            Dict with "types" key containing list of type names.
        """
        return {"types": list(args)}

    def rel_type(self, args: List[Any]) -> str:
        """Extract relationship type name.

        Type names are identifiers. Stripping backticks is necessary for
        normalized type comparison.

        Args:
            args: IDENTIFIER token.

        Returns:
            Type name string with backticks removed.
        """
        return str(args[0]).strip("`")

    def rel_properties(self, args: List[Any]) -> Dict[str, Any]:
        """Transform relationship property constraints.

        Relationships can have property filters just like nodes. Wrapping in a
        dict distinguishes properties from other components.

        Args:
            args: Properties map.

        Returns:
            Dict with "properties" key containing the property map.
        """
        return {"properties": args[0] if args else None}

    def rel_where(self, args: List[Any]) -> Dict[str, Any]:
        """Transform inline WHERE clause within relationship pattern.

        WHERE can filter relationship properties (e.g., [:KNOWS WHERE r.since > 2020]).
        Wrapping in a dict distinguishes from property maps.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.
        """
        return {"where": args[0] if args else None}

    def path_length(self, args: List[Any]) -> Dict[str, Any]:
        """Transform variable-length path specification (*).

        Variable-length paths match multiple hops (e.g., *1..3 or * for unlimited).
        This is necessary for traversing graph paths of unknown length.

        Args:
            args: Optional path_length_range specification.

        Returns:
            Dict with "pathLength" key containing range spec or True for unlimited.
        """
        range_spec = args[0] if args else None
        return {"pathLength": range_spec}

    def path_length_range(
        self, args: List[Any]
    ) -> Union[int, Dict[str, Optional[int]]]:
        """Transform path length range specification.

        Ranges can be: exact (5), minimum (5..), maximum (..5), or bounded (5..10).
        These distinctions are necessary for path matching algorithms.

        Args:
            args: One or two integer arguments for range bounds.

        Returns:
            Dict with "fixed", "min"/"max", or "unbounded" keys.
        """
        if len(args) == 1:
            return {"fixed": int(str(args[0]))}
        elif len(args) == 2:
            return {
                "min": int(str(args[0])) if args[0] else None,
                "max": int(str(args[1])) if args[1] else None,
            }
        return {"unbounded": True}

    # ========================================================================
    # Properties
    # ========================================================================

    def properties(self, args: List[Any]) -> Dict[str, Any]:
        """Extract property map from curly braces {...}.

        Property maps are key-value pairs for node/relationship properties.
        Extracting the inner dict is necessary to unwrap the intermediate structure.

        Args:
            args: property_list wrapper containing props dict.

        Returns:
            Dict mapping property names to values.
        """
        props = next(
            (a for a in args if isinstance(a, dict) and "props" in str(a)),
            {"props": {}},
        )
        return props.get("props", {})

    def property_list(self, args: List[Any]) -> Dict[str, Dict[str, Any]]:
        """Transform comma-separated property key-value pairs.

        Combines individual property assignments into a single map. Wrapping
        in a dict with "props" key is necessary to pass through transformer.

        Args:
            args: List of property_key_value dicts.

        Returns:
            Dict with "props" key containing merged property map.
        """
        result = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"props": result}

    def property_key_value(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single property assignment (key: value).

        Separating key and value is necessary for validation and execution.

        Args:
            args: [property_name, expression]

        Returns:
            Dict with "key" and "value" for the property assignment.
        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    def property_name(self, args: List[Any]) -> str:
        """Extract property name identifier.

        Property names are identifiers. Stripping backticks is necessary for
        normalized property name comparison.

        Args:
            args: IDENTIFIER token.

        Returns:
            Property name string with backticks removed.
        """
        return str(args[0]).strip("`")

    # ========================================================================
    # Expressions
    # ========================================================================

    def or_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform OR boolean expression with short-circuit evaluation.

        OR has lowest precedence among boolean operators. Multiple OR operations
        are collected into a single node for easier optimization. Single operands
        pass through to avoid unnecessary wrapping.

        Args:
            args: One or more XOR expression operands.

        Returns:
            Single operand, or dict with type "Or" containing all operands.
        """
        if len(args) == 1:
            return args[0]
        return {"type": "Or", "operands": list(args)}

    def xor_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform XOR (exclusive OR) boolean expression.

        XOR returns true only if operands differ. Multiple XORs are collected
        for easier analysis. This is less common than AND/OR but necessary for
        complete boolean logic support.

        Args:
            args: One or more AND expression operands.

        Returns:
            Single operand, or dict with type "Xor" containing all operands.
        """
        if len(args) == 1:
            return args[0]
        return {"type": "Xor", "operands": list(args)}

    def and_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform AND boolean expression with short-circuit evaluation.

        AND has higher precedence than OR/XOR. Multiple ANDs are collected into
        a single node for easier optimization and execution planning.

        Args:
            args: One or more NOT expression operands.

        Returns:
            Single operand, or dict with type "And" containing all operands.
        """
        if len(args) == 1:
            return args[0]
        return {"type": "And", "operands": list(args)}

    def not_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform NOT expression with proper boolean negation handling.

        NOT is a unary boolean operator that negates its operand. Multiple NOTs
        can be chained (e.g., NOT NOT x), so we count them and apply modulo 2
        logic: odd count = negate, even count = no-op (double negation cancels).

        The NOT_KEYWORD terminal has higher priority than IDENTIFIER in the grammar,
        ensuring "NOT" is parsed as a keyword rather than a variable name. These
        terminals are passed as Lark Token objects, so we filter them separately
        from the expression being negated.

        This careful handling is necessary because:
        1. NOT can be ambiguous with variable names in some contexts
        2. Terminal priorities must be explicit to avoid parse errors
        3. Multiple negations need semantic simplification

        Args:
            args: Mix of NOT_KEYWORD Token objects and the comparison expression.

        Returns:
            The expression unchanged (even NOTs), or wrapped in Not node (odd NOTs).
        """
        # NOT_KEYWORD terminals will be passed as Token objects
        from lark import Token

        not_count = sum(
            1 for a in args if isinstance(a, Token) and a.type == "NOT_KEYWORD"
        )
        # Expression is the non-Token arg
        expr = next((a for a in args if not isinstance(a, Token)), None)

        if not_count == 0:
            return expr
        elif not_count % 2 == 1:
            return {"type": "Not", "operand": expr}
        else:
            return expr

    def comparison_expression(
        self, args: List[Any]
    ) -> Union[Any, Dict[str, Any]]:
        """Transform comparison expression with operators like =, <>, <, >, <=, >=.

        Comparison expressions allow comparing values for equality or ordering.
        Multiple comparisons can be chained (e.g., a < b < c), which is transformed
        into nested comparison nodes. This structure is necessary for type checking
        and ensuring all operands are comparable.

        Single operands pass through to avoid unnecessary wrapping. This is necessary
        for efficient AST traversal and avoiding deep nesting for simple expressions.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Comparison"
            containing operator, left operand, and right operand for each comparison.
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Comparison",
                "operator": str(op),
                "left": result,
                "right": right,
            }
        return result

    def null_predicate_expression(
        self, args: List[Any]
    ) -> Union[Any, Dict[str, Any]]:
        """Transform IS NULL or IS NOT NULL predicate expressions.

        Null predicates check whether an expression evaluates to NULL, which is
        necessary because NULL cannot be compared with = or <> in SQL/Cypher
        semantics (NULL = NULL is false, not true). IS NULL and IS NOT NULL
        are the only correct ways to test for null values.

        This method wraps the expression in a NullCheck node only when a null
        operator is present. Otherwise, it passes through the expression unchanged
        to avoid unnecessary wrapper nodes in the AST.

        Args:
            args: [expression] or [expression, null_check_operator].

        Returns:
            Expression unchanged if no null check, or dict with type "NullCheck"
            containing the operator ("IS NULL" or "IS NOT NULL") and operand.
        """
        expr = args[0] if args else None
        if len(args) > 1:
            # Has a null check
            op_type = args[1]
            return {"type": "NullCheck", "operator": op_type, "operand": expr}
        return expr

    def null_check_op(self, args: List[Any]) -> None:
        # This will be called by the is_null or is_not_null aliases
        return None

    def string_predicate_expression(
        self, args: List[Any]
    ) -> Union[Any, Dict[str, Any]]:
        """Transform string predicate expressions (STARTS WITH, ENDS WITH, CONTAINS, =~, IN).

        String predicates provide specialized string matching operations that are more
        efficient and expressive than using regular expressions for common patterns.
        The IN operator tests set membership. These operations are necessary for
        text search, filtering, and pattern matching in graph queries.

        Multiple string predicates can be chained, creating nested nodes for
        complex string filtering logic. Single operands pass through to avoid
        unnecessary AST depth.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "StringPredicate"
            containing operator, left operand, and right operand for each predicate.
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "StringPredicate",
                "operator": str(op),
                "left": result,
                "right": right,
            }
        return result

    def string_predicate_op(self, args: List[Any]) -> str:
        """Extract and normalize string predicate operator keywords.

        String predicate operators can be multi-word keywords (STARTS WITH, ENDS WITH)
        or single tokens (CONTAINS, IN, =~). This method joins multi-word operators
        with spaces and uppercases them for consistent AST representation.

        Normalization to uppercase is necessary because Cypher is case-insensitive
        for keywords, and consistent casing enables reliable pattern matching and
        operator dispatch during query execution.

        Args:
            args: One or more keyword tokens forming the operator.

        Returns:
            Space-separated, uppercased operator string (e.g., "STARTS WITH").
        """
        return " ".join(str(a).upper() for a in args)

    def is_null(self, args: List[Any]) -> str:
        """Return the IS NULL operator constant.

        This method is called when the grammar matches the IS NULL pattern.
        It returns a consistent string representation that can be used by
        null_predicate_expression to create the appropriate NullCheck node.

        Returning a constant string is necessary because the grammar rule produces
        this as an alias for null_check_op, and the parent expression handler needs
        a standardized operator value to construct the AST node.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NULL".
        """
        return "IS NULL"

    def is_not_null(self, args: List[Any]) -> str:
        """Return the IS NOT NULL operator constant.

        This method is called when the grammar matches the IS NOT NULL pattern.
        It returns a consistent string representation that can be used by
        null_predicate_expression to create the appropriate NullCheck node.

        Returning a constant string is necessary because the grammar rule produces
        this as an alias for null_check_op, and the parent expression handler needs
        a standardized operator value to construct the AST node. The distinction
        from IS NULL is critical for correct null checking semantics.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NOT NULL".
        """
        return "IS NOT NULL"

    def add_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform addition and subtraction arithmetic expressions.

        Addition and subtraction have equal precedence and associate left-to-right.
        This method builds a left-associative tree of Arithmetic nodes, which is
        necessary for correct evaluation order (e.g., a - b + c = (a - b) + c).

        Multiple operations are chained by iterating through operator-operand pairs
        and nesting the previous result as the left operand. This structure enables
        type checking to verify that all operands are numeric, and allows optimization
        passes to simplify constant expressions.

        Single operands pass through unchanged to avoid wrapping simple values in
        unnecessary AST nodes, improving efficiency for common cases like literals
        or variables without arithmetic operations.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator ("+" or "-"), left operand, and right operand.
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "+"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def mult_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform multiplication, division, and modulo arithmetic expressions.

        Multiplication, division, and modulo have equal precedence, higher than
        addition/subtraction, and associate left-to-right. This method builds
        a left-associative tree to preserve evaluation order (e.g., a / b * c = (a / b) * c).

        Higher precedence than addition is enforced by the grammar structure where
        mult_expression is a child of add_expression. This precedence hierarchy is
        necessary to correctly parse expressions like 2 + 3 * 4 as 2 + (3 * 4).

        Division by zero and modulo by zero are runtime errors that cannot be
        detected during parsing, so the AST structure allows these operations and
        defers validation to execution time or static analysis passes.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator ("*", "/", or "%"), left operand, and right operand.
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "*"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def power_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform exponentiation (power) arithmetic expressions using ^ operator.

        Exponentiation has the highest precedence among arithmetic operators and
        associates left-to-right (though mathematically it's often right-associative,
        Cypher follows left-to-right). This method builds a left-associative tree
        for expressions like a ^ b ^ c, evaluating as (a ^ b) ^ c.

        Higher precedence than multiplication is enforced by the grammar where
        power_expression is a child of mult_expression. This ensures 2 * 3 ^ 4
        is correctly parsed as 2 * (3 ^ 4), not (2 * 3) ^ 4.

        The ^ operator can produce very large numbers or complex mathematical edge
        cases (negative base with fractional exponent), so validation and runtime
        overflow handling may be needed during execution.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator "^", left operand (base), and right operand (exponent).
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = "^"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def unary_expression(self, args: List[Any]) -> Union[Any, Dict[str, Any]]:
        """Transform unary plus (+) and minus (-) expressions.

        Unary operators apply to a single operand and have the highest precedence
        among all operators (including exponentiation). The unary minus negates a
        value (e.g., -5, -x), while unary plus is a no-op that promotes to numeric
        type (e.g., +"5" might convert a string to number in some contexts).

        This method creates a Unary node only when a sign operator is present.
        Without a unary operator, it passes through the postfix expression unchanged,
        avoiding unnecessary wrapping. This is necessary for efficient AST structure
        since most expressions don't have unary operators.

        Multiple unary operators can be chained (e.g., --5, +-3), though this is
        rare in practice. The current implementation handles this by nesting Unary
        nodes, though optimization passes could simplify these (-- becomes identity).

        Args:
            args: [operator, operand] for unary expressions, or [operand] without operator.

        Returns:
            Operand unchanged if no operator, or dict with type "Unary" containing
            operator ("+" or "-") and the operand expression.
        """
        if len(args) == 1:
            return args[0]
        sign = str(args[0])
        operand = args[1] if len(args) > 1 else None
        return {"type": "Unary", "operator": sign, "operand": operand}

    def postfix_expression(
        self, args: List[Any]
    ) -> Union[Any, Dict[str, Any]]:
        """Transform postfix expressions (property access, indexing, slicing).

        Postfix operators apply after an expression and associate left-to-right,
        building a chain of access operations. Examples:
        - Property access: person.name
        - Index access: list[0]
        - Slicing: list[1..3]
        - Chained: person.addresses[0].city

        This method iteratively applies postfix operations to build a left-associative
        tree. Each operation uses the previous result as its base object. This
        structure is necessary for:
        1. Type checking - verifying each intermediate result supports the next operation
        2. Execution planning - determining optimal access paths
        3. Null safety - detecting where null pointer exceptions could occur

        The transformation converts intermediate PropertyLookup/IndexLookup/Slicing
        nodes into final PropertyAccess/IndexAccess/Slice nodes that include both
        the object being accessed and the accessor (property name, index, or range).

        Single operands without postfix operations pass through unchanged.

        Args:
            args: [atom_expression, postfix_op1, postfix_op2, ...].

        Returns:
            Single atom unchanged, or nested access nodes (PropertyAccess, IndexAccess,
            Slice) forming a left-to-right chain of operations.
        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for op in args[1:]:
            if isinstance(op, dict) and op.get("type") == "PropertyLookup":
                result = {
                    "type": "PropertyAccess",
                    "object": result,
                    "property": op.get("property"),
                }
            elif isinstance(op, dict) and op.get("type") == "IndexLookup":
                result = {
                    "type": "IndexAccess",
                    "object": result,
                    "index": op.get("index"),
                }
            elif isinstance(op, dict) and op.get("type") == "Slicing":
                result = {
                    "type": "Slice",
                    "object": result,
                    "from": op.get("from"),
                    "to": op.get("to"),
                }
        return result

    def postfix_op(self, args: List[Any]) -> Optional[Any]:
        """Pass through postfix operator nodes without modification.

        The grammar defines postfix_op as a union of property_lookup, index_lookup,
        and slicing. This method acts as a simple pass-through to avoid adding
        unnecessary wrapper nodes in the AST.

        Pass-through is necessary because the actual semantic transformation happens
        in postfix_expression, which combines the operator with its target object.
        This separation of parsing (recognizing the operator) from transformation
        (building the access node) keeps the grammar clean and the AST well-structured.

        Args:
            args: Single postfix operation node (PropertyLookup, IndexLookup, or Slicing).

        Returns:
            The postfix operation unchanged.
        """
        return args[0] if args else None

    def property_lookup(self, args: List[Any]) -> Dict[str, Any]:
        """Transform property lookup syntax (.property_name) into intermediate node.

        Property lookup accesses a named property on a node, relationship, or map.
        Example: person.name, edge.weight, config.timeout

        This method creates an intermediate PropertyLookup node containing just the
        property name. The actual PropertyAccess node (which includes the object
        being accessed) is created by postfix_expression when it combines this with
        the target object.

        This two-step transformation is necessary because:
        1. The grammar parses .name separately from the object
        2. Multiple property accesses can chain: obj.prop1.prop2
        3. postfix_expression needs to build the chain left-to-right

        The property name comes from the property_name rule, which has already
        stripped backticks and normalized the identifier.

        Args:
            args: Property name string from property_name rule.

        Returns:
            Dict with type "PropertyLookup" and the property name.
        """
        return {
            "type": "PropertyLookup",
            "property": args[0] if args else None,
        }

    def index_lookup(self, args: List[Any]) -> Dict[str, Any]:
        """Transform index lookup syntax ([index]) into intermediate node.

        Index lookup accesses an element by position in a list or by key in a map.
        Examples: list[0], map['key'], items[i+1]

        The index expression is evaluated at runtime and can be:
        - Integer for list access (0-based indexing)
        - String for map key access
        - Any expression that evaluates to an appropriate index type

        This method creates an intermediate IndexLookup node containing just the
        index expression. The actual IndexAccess node (which includes the collection
        being indexed) is created by postfix_expression when combining with the target.

        The two-step approach is necessary for the same reasons as property_lookup:
        enabling chained access operations and left-to-right evaluation.

        Negative indices and out-of-bounds access are runtime errors that cannot
        be detected during parsing, so validation is deferred to execution time.

        Args:
            args: Index expression that evaluates to the position/key.

        Returns:
            Dict with type "IndexLookup" and the index expression.
        """
        return {"type": "IndexLookup", "index": args[0] if args else None}

    def slicing(self, args: List[Any]) -> Dict[str, Any]:
        """Transform list slicing syntax ([from..to]) into intermediate node.

        Slicing extracts a sub-list from a list using range notation. Examples:
        - list[1..3] - elements at indices 1 and 2 (end exclusive)
        - list[..5] - first 5 elements (indices 0-4)
        - list[2..] - from index 2 to end
        - list[..] - entire list (copy)

        Both from and to expressions are optional. When omitted:
        - Missing from defaults to start of list (0)
        - Missing to defaults to end of list
        - Both missing creates a copy of the entire list

        This method creates an intermediate Slicing node with the range bounds.
        The actual Slice node (including the list being sliced) is created by
        postfix_expression. This separation is necessary for supporting chained
        operations like list[1..3][0] (get first element of a slice).

        Negative indices, reverse ranges, and out-of-bounds handling are runtime
        behaviors that vary by implementation and cannot be validated during parsing.

        Args:
            args: [from_expr, to_expr] where either can be None for open ranges.

        Returns:
            Dict with type "Slicing" containing from and to range expressions (may be None).
        """
        from_expr = args[0] if args and args[0] is not None else None
        to_expr = args[1] if len(args) > 1 and args[1] is not None else None
        return {"type": "Slicing", "from": from_expr, "to": to_expr}

    # ========================================================================
    # Count star
    # ========================================================================

    def count_star(self, args: List[Any]) -> Dict[str, str]:
        """Transform COUNT(*) aggregate function into a special AST node.

        COUNT(*) counts all rows/matches, including duplicates and null values.
        This is different from COUNT(expression) which excludes nulls. The special
        handling is necessary because * is not a regular expression - it's a syntactic
        marker meaning "count everything."

        This method creates a dedicated CountStar node rather than treating it as
        a regular function invocation. This distinction is important for:
        1. Query optimization - COUNT(*) can often be computed more efficiently
        2. Type checking - CountStar always returns an integer, no expression to validate
        3. Execution planning - Some databases have optimized COUNT(*) implementations

        COUNT(*) is typically used in aggregate queries with GROUP BY or as a simple
        row count: MATCH (n:Person) RETURN COUNT(*)

        Args:
            args: Not used (COUNT(*) has no arguments, * is syntactic).

        Returns:
            Dict with type "CountStar" indicating a count-all operation.
        """
        return {"type": "CountStar"}

    # ========================================================================
    # EXISTS expression
    # ========================================================================

    def exists_expression(self, args: List[Any]) -> Dict[str, Any]:
        """Transform EXISTS { ... } subquery expression.

        EXISTS evaluates to true if the subquery returns any results, false otherwise.
        This is essential for pattern existence checks without needing to collect
        actual matched data. Example:

        MATCH (person:Person)
        WHERE EXISTS { MATCH (person)-[:KNOWS]->(friend) }
        RETURN person

        The subquery can contain:
        - Pattern matching: EXISTS { (a)-[:KNOWS]->(b) }
        - Full queries: EXISTS { MATCH (n) WHERE n.age > 30 RETURN n }

        EXISTS is necessary for efficient existence checks because:
        1. It short-circuits on first match (doesn't need to find all results)
        2. It doesn't materialize data (no memory overhead for large result sets)
        3. It can use specialized indexes for existence tests

        This is analogous to SQL's EXISTS (SELECT ...) but uses Cypher pattern syntax.

        Args:
            args: Single exists_content node containing the subquery specification.

        Returns:
            Dict with type "Exists" containing the subquery content.
        """
        content = args[0] if args else None
        return {"type": "Exists", "content": content}

    def exists_content(self, args: List[Any]) -> Optional[Any]:
        """Extract the content of an EXISTS subquery.

        EXISTS content can be either:
        1. A simple pattern with optional WHERE clause (implicit match)
        2. A full query with MATCH/UNWIND/WITH clauses and optional RETURN

        This method passes through the parsed content without modification, as the
        structure has already been built by the appropriate clause transformers.
        Pass-through is necessary to avoid double-wrapping the subquery.

        The grammar allows both forms to provide flexibility:
        - Simple: EXISTS { (a)-[:KNOWS]->(b) WHERE b.age > 30 }
        - Full: EXISTS { MATCH (a)-[:KNOWS]->(b) WHERE b.age > 30 RETURN b }

        Args:
            args: Parsed subquery content (pattern or query clauses).

        Returns:
            The subquery content unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # Function invocation
    # ========================================================================

    def function_invocation(self, args: List[Any]) -> Dict[str, Any]:
        """Transform function invocation (built-in or user-defined functions).

        Functions are called with parentheses syntax: function_name(arg1, arg2, ...).
        Functions can be:
        - Built-in: count(), sum(), avg(), min(), max(), collect(), etc.
        - User-defined: custom functions registered in the database
        - Namespaced: db.labels(), apoc.create.node(), etc.

        This method creates a FunctionInvocation node containing:
        1. Function name (may include namespace for qualified names)
        2. Arguments (list of expressions, may include DISTINCT flag)

        The separation of name and arguments is necessary for:
        - Function resolution (finding the right function implementation)
        - Type checking (validating argument types match function signature)
        - Query optimization (some functions can be pre-computed or optimized)

        The "unknown" default for missing names handles edge cases in malformed queries
        and provides a fallback for error reporting.

        Args:
            args: [function_name, function_args] where args may be None for no arguments.

        Returns:
            Dict with type "FunctionInvocation" containing name and arguments.
        """
        name = args[0] if args else "unknown"
        func_args = args[1] if len(args) > 1 else None
        return {
            "type": "FunctionInvocation",
            "name": name,
            "arguments": func_args,
        }

    def function_args(
        self, args: List[Any]
    ) -> Dict[str, Union[bool, List[Any]]]:
        """Transform function arguments with optional DISTINCT modifier.

        Function arguments can have a DISTINCT modifier for aggregation functions:
        - COUNT(DISTINCT n.name) - counts unique values only
        - COLLECT(DISTINCT n.label) - collects unique values into a list

        The DISTINCT modifier is only meaningful for certain aggregate functions
        (COUNT, COLLECT, SUM, AVG), but the parser allows it on any function.
        Semantic validation of appropriate DISTINCT usage happens during type checking.

        This method extracts:
        1. distinct flag - whether DISTINCT keyword is present
        2. arguments list - the actual expression arguments

        Separating these components is necessary because the execution behavior
        differs significantly: DISTINCT requires deduplication logic which affects
        performance and memory usage.

        Args:
            args: Mix of "DISTINCT" keyword string and function_arg_list.

        Returns:
            Dict with "distinct" boolean flag and "arguments" list.
        """
        distinct = any(
            str(a).upper() == "DISTINCT" for a in args if isinstance(a, str)
        )
        arg_list = next((a for a in args if isinstance(a, list)), [])
        return {"distinct": distinct, "arguments": arg_list}

    def function_arg_list(self, args: List[Any]) -> List[Any]:
        """Transform comma-separated function argument expressions into a list.

        Function arguments are arbitrary expressions that can include:
        - Literals: sum(1, 2, 3)
        - Variables: max(n.age, m.age)
        - Nested function calls: round(avg(n.score), 2)
        - Complex expressions: count(n.x + n.y * 2)

        Converting to a list is necessary for:
        1. Consistent iteration during execution
        2. Arity checking (validating correct number of arguments)
        3. Type checking each argument against function signature

        Empty argument lists are represented as [] rather than None, which simplifies
        downstream code that needs to iterate over arguments (no null checks needed).

        Args:
            args: Individual expression nodes for each argument.

        Returns:
            List of argument expressions (empty list if no arguments).
        """
        return list(args)

    def function_name(self, args: List[Any]) -> Union[str, Dict[str, str]]:
        """Transform function name with optional namespace qualification.

        Function names can be simple (sum, count) or namespaced (db.labels, apoc.create.node).
        Namespaces organize functions into logical groups and prevent naming conflicts:
        - db.* - database introspection functions
        - apoc.* - APOC procedure library (third-party)
        - custom.* - user-defined namespaces

        This method returns:
        - Simple string for unqualified names: "count"
        - Dict with namespace and name for qualified names: {namespace: "db", name: "labels"}

        The distinction is necessary for:
        1. Function resolution - different namespaces may have same function name
        2. Permission checking - namespaced functions may have different access controls
        3. Error reporting - qualified names provide better context in error messages

        The "unknown" fallback handles malformed queries gracefully.

        Args:
            args: [namespace_name, simple_name] or just [simple_name].

        Returns:
            Simple name string, or dict with namespace and name for qualified functions.
        """
        namespace = args[0] if len(args) > 1 else None
        simple_name = args[-1] if args else "unknown"
        return (
            {"namespace": namespace, "name": simple_name}
            if namespace
            else simple_name
        )

    def namespace_name(self, args: List[Any]) -> str:
        """Transform namespace path into a dot-separated string.

        Namespaces can be multi-level: db.schema.nodeTypeProperties
        The grammar parses these as multiple identifiers separated by dots.
        This method joins them with dots and strips backticks from each part.

        Joining is necessary to create a canonical namespace string for function
        lookup. Stripping backticks normalizes identifiers (backticks allow special
        characters but aren't part of the actual name).

        Example: `my-custom`.`my-function` becomes "my-custom.my-function"

        Args:
            args: List of identifier tokens forming the namespace path.

        Returns:
            Dot-separated namespace string with backticks removed.
        """
        return ".".join(str(a).strip("`") for a in args)

    def function_simple_name(self, args: List[Any]) -> str:
        """Extract the unqualified function name identifier.

        The simple name is the final component of a potentially namespaced function.
        For example, in db.labels(), "labels" is the simple name.

        Stripping backticks is necessary to normalize identifier representation.
        Backticks allow identifiers with special characters or reserved words,
        but the backticks themselves are not part of the semantic name.

        Converting to string handles both Token objects from the parser and any
        other string-like representations.

        Args:
            args: Single identifier token for the function name.

        Returns:
            Function name as a string with backticks removed.
        """
        return str(args[0]).strip("`")

    # ========================================================================
    # Case expression
    # ========================================================================

    def case_expression(self, args: List[Any]) -> Optional[Any]:
        """Transform CASE expression (simple or searched form).

        CASE expressions provide conditional logic similar to if-then-else or switch
        statements. There are two forms:

        1. Simple CASE - compares one expression against multiple values:
           CASE n.status WHEN 'active' THEN 1 WHEN 'pending' THEN 0 ELSE -1 END

        2. Searched CASE - evaluates multiple boolean conditions:
           CASE WHEN n.age < 18 THEN 'minor' WHEN n.age < 65 THEN 'adult' ELSE 'senior' END

        This method acts as a pass-through because the grammar has already dispatched
        to the appropriate specific handler (simple_case or searched_case). Pass-through
        is necessary to avoid adding unnecessary wrapper nodes in the AST.

        CASE expressions are essential for data transformation and conditional logic
        within queries, enabling computed columns and complex filtering.

        Args:
            args: Single node (SimpleCase or SearchedCase) from the specific rule.

        Returns:
            The SimpleCase or SearchedCase node unchanged.
        """
        return args[0] if args else None

    def simple_case(self, args: List[Any]) -> Dict[str, Any]:
        """Transform simple CASE expression that matches an operand against values.

        Simple CASE syntax: CASE expression WHEN value1 THEN result1 [WHEN ...] [ELSE default] END

        The operand expression is evaluated once, then compared against each WHEN value
        sequentially until a match is found. The corresponding THEN result is returned.
        If no WHEN matches, the ELSE value is returned (or NULL if no ELSE clause).

        This is analogous to a switch statement in programming languages. It's more
        concise than searched CASE when you're comparing one expression against
        multiple constant values.

        The structure separates:
        - operand: the expression being compared (evaluated once)
        - when clauses: list of value-result pairs (evaluated sequentially)
        - else clause: default result if no matches (optional)

        This separation is necessary for:
        1. Optimized execution (operand evaluated only once)
        2. Type checking (all WHEN values must be comparable to operand)
        3. Short-circuit evaluation (stop at first match)

        Args:
            args: [operand_expression, when_clause1, when_clause2, ..., optional_else_clause].

        Returns:
            Dict with type "SimpleCase" containing operand, when clauses list, and optional else.
        """
        operand = args[0] if args else None
        when_clauses = [
            a
            for a in args[1:]
            if isinstance(a, dict) and a.get("type") == "SimpleWhen"
        ]
        else_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Else"
            ),
            None,
        )
        return {
            "type": "SimpleCase",
            "operand": operand,
            "when": when_clauses,
            "else": else_clause,
        }

    def searched_case(self, args: List[Any]) -> Dict[str, Any]:
        """Transform searched CASE expression that evaluates boolean conditions.

        Searched CASE syntax: CASE WHEN condition1 THEN result1 [WHEN ...] [ELSE default] END

        Each WHEN clause contains a boolean condition that is evaluated sequentially.
        The first condition that evaluates to true determines the result. If no
        conditions are true, the ELSE value is returned (or NULL if no ELSE clause).

        This is analogous to if-else-if chains in programming languages. It's more
        flexible than simple CASE because each condition can be a completely different
        boolean expression (not just equality tests).

        The structure contains:
        - when clauses: list of condition-result pairs (evaluated sequentially)
        - else clause: default result if no conditions are true (optional)

        This separation is necessary for:
        1. Short-circuit evaluation (stop at first true condition)
        2. Type checking (all THEN results should have compatible types)
        3. Optimization (conditions can be reordered if independent)

        Args:
            args: [when_clause1, when_clause2, ..., optional_else_clause].

        Returns:
            Dict with type "SearchedCase" containing when clauses list and optional else.
        """
        when_clauses = [
            a
            for a in args
            if isinstance(a, dict) and a.get("type") == "SearchedWhen"
        ]
        else_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Else"
            ),
            None,
        )
        return {
            "type": "SearchedCase",
            "when": when_clauses,
            "else": else_clause,
        }

    def simple_when(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a WHEN clause in a simple CASE expression.

        Simple WHEN syntax: WHEN value1, value2, ... THEN result

        In simple CASE, a WHEN clause can match multiple values (comma-separated).
        The operand is compared against each value, and if any match, the result
        is returned. This is a shorthand for multiple WHEN clauses with the same result.

        Example:
        CASE n.status
          WHEN 'active', 'verified' THEN 'good'
          WHEN 'pending', 'new' THEN 'waiting'
        END

        The structure contains:
        - operands: list of values to compare against (evaluated left to right)
        - result: expression to return if any operand matches

        This separation is necessary for:
        1. Efficient matching (can use IN-style lookups for multiple values)
        2. Type checking (all operands must be comparable to CASE operand)
        3. Execution planning (can optimize multiple equality tests)

        Args:
            args: [when_operands_list, result_expression].

        Returns:
            Dict with type "SimpleWhen" containing operands list and result expression.
        """
        operands = args[0] if args else []
        result = args[1] if len(args) > 1 else None
        return {"type": "SimpleWhen", "operands": operands, "result": result}

    def searched_when(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a WHEN clause in a searched CASE expression.

        Searched WHEN syntax: WHEN condition THEN result

        In searched CASE, each WHEN clause has a boolean condition that is
        evaluated independently. The first WHEN with a true condition determines
        the result. This provides maximum flexibility for conditional logic.

        Example:
        CASE
          WHEN n.age < 18 THEN 'minor'
          WHEN n.age >= 65 THEN 'senior'
          WHEN n.employed = true THEN 'working adult'
          ELSE 'adult'
        END

        The structure contains:
        - condition: boolean expression to evaluate
        - result: expression to return if condition is true

        This separation is necessary for:
        1. Short-circuit evaluation (stop evaluating after first true)
        2. Independent condition evaluation (each can access different variables)
        3. Type checking (condition must be boolean, result type must match other WHENs)

        Args:
            args: [condition_expression, result_expression].

        Returns:
            Dict with type "SearchedWhen" containing condition and result expressions.
        """
        condition = args[0] if args else None
        result = args[1] if len(args) > 1 else None
        return {
            "type": "SearchedWhen",
            "condition": condition,
            "result": result,
        }

    def when_operands(self, args: List[Any]) -> List[Any]:
        """Transform comma-separated operands in a simple CASE WHEN clause.

        Multiple operands allow matching against any of several values in one WHEN.
        This is a convenience feature that reduces verbosity when multiple values
        should produce the same result.

        Converting to a list is necessary for:
        1. Iteration during execution (test each value for match)
        2. Type checking (all values must be comparable to CASE operand)
        3. Optimization (can use set-based lookup for many values)

        Args:
            args: Individual expression nodes for each value to test.

        Returns:
            List of operand expressions.
        """
        return list(args)

    def else_clause(self, args: List[Any]) -> Dict[str, Any]:
        """Transform the ELSE clause in a CASE expression.

        The ELSE clause provides a default value when no WHEN conditions match.
        If ELSE is omitted and no WHEN matches, the result is NULL.

        Wrapping in a typed dict is necessary to distinguish the ELSE value from
        regular expressions during AST traversal. The "Else" type marker helps
        the transformer identify and extract this special clause.

        The ELSE value can be any expression, including:
        - Literals: ELSE 'unknown'
        - Variables: ELSE n.default_value
        - Nested expressions: ELSE CASE ... END (nested CASE)

        Args:
            args: Single expression for the default value.

        Returns:
            Dict with type "Else" containing the default value expression.
        """
        return {"type": "Else", "value": args[0] if args else None}

    # ========================================================================
    # List comprehension
    # ========================================================================

    def list_comprehension(self, args: List[Any]) -> Dict[str, Any]:
        """Transform list comprehension expression for filtering and mapping lists.

        List comprehension syntax: [variable IN list WHERE condition | projection]

        List comprehensions provide a concise way to transform lists by:
        1. Iterating over elements (variable IN list)
        2. Optionally filtering (WHERE condition)
        3. Optionally transforming (| projection)

        Examples:
        - [x IN [1,2,3] WHERE x > 1] -> [2, 3]  (filter only)
        - [x IN [1,2,3] | x * 2] -> [2, 4, 6]  (map only)
        - [x IN range(1,5) WHERE x % 2 = 0 | x^2] -> [4, 16]  (filter and map)

        This is similar to list comprehensions in Python/JavaScript and provides
        functional programming capabilities within Cypher. It's necessary for:
        - Data transformation without external functions
        - Inline filtering of collected results
        - Building computed lists in RETURN clauses

        The structure contains:
        - variable: iteration variable name
        - in: source list expression
        - where: optional filter condition
        - projection: optional transformation expression

        Args:
            args: [variable, source_list, optional_filter, optional_projection].

        Returns:
            Dict with type "ListComprehension" containing all components.
        """
        variable = args[0] if args else None
        source = args[1] if len(args) > 1 else None
        filter_expr = args[2] if len(args) > 2 else None
        projection = args[3] if len(args) > 3 else None
        return {
            "type": "ListComprehension",
            "variable": variable,
            "in": source,
            "where": filter_expr,
            "projection": projection,
        }

    def list_variable(self, args: List[Any]) -> Optional[Any]:
        """Extract the iteration variable name from a list comprehension.

        The list variable is the identifier used to reference each element during
        iteration. It's scoped to the comprehension and shadows any outer variable
        with the same name.

        Pass-through is necessary because variable_name has already normalized the
        identifier (stripped backticks, etc.), and we don't want to add extra wrapping.

        Args:
            args: Variable name string from variable_name rule.

        Returns:
            Variable name unchanged.
        """
        return args[0] if args else None

    def list_filter(self, args: List[Any]) -> Optional[Any]:
        """Extract the WHERE filter expression from a list comprehension.

        The filter expression determines which elements from the source list are
        included in the result. It's a boolean expression that can reference the
        iteration variable and any outer scope variables.

        Pass-through is necessary to avoid double-wrapping the expression.

        Args:
            args: Boolean filter expression.

        Returns:
            Filter expression unchanged.
        """
        return args[0] if args else None

    def list_projection(self, args: List[Any]) -> Optional[Any]:
        """Extract the projection expression from a list comprehension.

        The projection (after |) transforms each element before adding it to the
        result list. Without a projection, elements are included as-is (identity mapping).

        The projection expression can reference the iteration variable and perform
        any computation: arithmetic, string operations, property access, etc.

        Pass-through is necessary to avoid double-wrapping the expression.

        Args:
            args: Projection expression to transform each element.

        Returns:
            Projection expression unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # Pattern comprehension
    # ========================================================================

    def pattern_comprehension(self, args: List[Any]) -> Dict[str, Any]:
        """Transform pattern comprehension for collecting results from pattern matching.

        Pattern comprehension syntax: [path_var = pattern WHERE condition | projection]

        Pattern comprehensions match a graph pattern multiple times and collect
        results into a list. This is essential for inline subqueries without OPTIONAL MATCH.

        Examples:
        - [(person)-[:KNOWS]->(friend) | friend.name]
          Collects names of all friends
        - [p = (a)-[:KNOWS*1..3]->(b) WHERE b.age > 30 | length(p)]
          Collects path lengths to people over 30 within 3 hops

        The structure contains:
        - variable: optional variable for the entire path
        - pattern: graph pattern to match repeatedly
        - where: optional filter on matched patterns
        - projection: expression to collect (required, unlike list comprehension)

        This is necessary for:
        - Collecting related data without explicit MATCH/COLLECT
        - Nested pattern matching within expressions
        - Building complex aggregations inline

        Pattern comprehensions are more powerful than list comprehensions because
        they can match graph structures, not just iterate over lists.

        Args:
            args: Mix of optional variable, pattern element, optional where, and projection.

        Returns:
            Dict with type "PatternComprehension" containing all components.
        """
        variable = None
        pattern = None
        filter_expr = None
        projection = None

        for arg in args:
            if isinstance(arg, str) and variable is None:
                variable = arg
            elif isinstance(arg, dict):
                if arg.get("type") == "PatternElement" and pattern is None:
                    pattern = arg
                elif "where" in str(arg).lower() and filter_expr is None:
                    filter_expr = arg
                elif projection is None:
                    projection = arg

        return {
            "type": "PatternComprehension",
            "variable": variable,
            "pattern": pattern,
            "where": filter_expr,
            "projection": projection,
        }

    def pattern_comp_variable(self, args: List[Any]) -> Optional[Any]:
        """Extract the optional path variable from a pattern comprehension.

        The path variable captures the entire matched path, which can be useful
        for computing path properties (length, nodes, relationships) in the projection.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.
        """
        return args[0] if args else None

    def pattern_filter(self, args: List[Any]) -> Optional[Any]:
        """Extract the WHERE filter from a pattern comprehension.

        The filter expression determines which matched patterns are included in
        the collected results. It can reference variables bound in the pattern.

        Pass-through is necessary to avoid double-wrapping.

        Args:
            args: Boolean filter expression.

        Returns:
            Filter expression unchanged.
        """
        return args[0] if args else None

    def pattern_projection(self, args: List[Any]) -> Optional[Any]:
        """Extract the projection expression from a pattern comprehension.

        The projection specifies what to collect from each matched pattern.
        It's required (unlike list comprehension where it's optional) because
        collecting the entire pattern match isn't meaningful by default.

        Pass-through is necessary to avoid double-wrapping.

        Args:
            args: Projection expression.

        Returns:
            Projection expression unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # Reduce expression
    # ========================================================================

    def reduce_expression(self, args: List[Any]) -> Dict[str, Any]:
        """Transform REDUCE expression for list aggregation with accumulator.

        REDUCE syntax: REDUCE(accumulator = initial, variable IN list | step_expression)

        REDUCE is a functional programming construct that aggregates a list into
        a single value by applying a step expression iteratively. It's analogous
        to fold/reduce in functional languages.

        Example:
        REDUCE(sum = 0, x IN [1,2,3,4,5] | sum + x) -> 15
        REDUCE(product = 1, x IN [1,2,3,4] | product * x) -> 24
        REDUCE(max = -999999, x IN numbers | CASE WHEN x > max THEN x ELSE max END)

        The structure contains:
        - accumulator: {variable: name, init: initial_value} for the aggregated result
        - variable: iteration variable name for each list element
        - in: source list expression
        - step: expression that computes next accumulator value (can reference both variables)

        This is necessary for:
        - Custom aggregation logic not provided by built-in functions
        - Stateful list processing (each step can use previous result)
        - Complex computations that require iteration context

        The accumulator is updated in each iteration: accumulator = step_expression,
        where step_expression can reference both the current accumulator value and
        the current list element.

        Args:
            args: [accumulator_dict, iteration_variable, source_list, step_expression].

        Returns:
            Dict with type "Reduce" containing accumulator, variable, source, and step.
        """
        accumulator = args[0] if args else None
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        step = args[3] if len(args) > 3 else None
        return {
            "type": "Reduce",
            "accumulator": accumulator,
            "variable": variable,
            "in": source,
            "step": step,
        }

    def reduce_accumulator(self, args: List[Any]) -> Dict[str, Any]:
        """Transform the accumulator declaration in a REDUCE expression.

        Accumulator syntax: variable_name = initial_expression

        The accumulator holds the running result during iteration. It's initialized
        before the first iteration and updated after each step. The final accumulator
        value becomes the result of the entire REDUCE expression.

        The structure contains:
        - variable: accumulator variable name
        - init: initial value expression (evaluated once before iteration)

        Separating variable and initialization is necessary for:
        1. Scoping - accumulator variable is local to REDUCE
        2. Type inference - initial value determines accumulator type
        3. Execution - initialization happens exactly once

        Args:
            args: [variable_name, initialization_expression].

        Returns:
            Dict with variable name and init expression.
        """
        variable = args[0] if args else None
        init = args[1] if len(args) > 1 else None
        return {"variable": variable, "init": init}

    def reduce_variable(self, args: List[Any]) -> Optional[Any]:
        """Extract the iteration variable from a REDUCE expression.

        The iteration variable represents each element from the source list during
        iteration. It's scoped to the REDUCE expression and can be referenced in
        the step expression.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # Quantifier expressions
    # ========================================================================

    def quantifier_expression(self, args: List[Any]) -> Dict[str, Any]:
        """Transform quantifier expressions (ALL, ANY, SINGLE, NONE) for predicate testing.

        Quantifier syntax: QUANTIFIER(variable IN list WHERE predicate)

        Quantifiers test whether a predicate holds for list elements:
        - ALL: true if predicate is true for every element (universal quantification)
        - ANY: true if predicate is true for at least one element (existential quantification)
        - SINGLE: true if predicate is true for exactly one element
        - NONE: true if predicate is false for all elements (negation of ANY)

        Examples:
        - ALL(x IN [2,4,6,8] WHERE x % 2 = 0) -> true
        - ANY(x IN [1,3,5,6] WHERE x % 2 = 0) -> true
        - SINGLE(x IN [1,2,3,4] WHERE x > 3) -> true
        - NONE(x IN [1,3,5,7] WHERE x % 2 = 0) -> true

        These are essential for:
        - Collection validation (checking constraints on all/some elements)
        - Existence tests (ANY is more efficient than collecting and checking length)
        - Uniqueness checking (SINGLE ensures exactly one match)

        The structure contains:
        - quantifier: which quantifier (ALL/ANY/SINGLE/NONE)
        - variable: iteration variable name
        - in: source list expression
        - where: predicate to test for each element

        This is similar to SQL's ALL/ANY operators and mathematical quantifiers (∀, ∃).

        Args:
            args: [quantifier_keyword, variable, source_list, predicate_expression].

        Returns:
            Dict with type "Quantifier" containing quantifier type, variable, source, and predicate.
        """
        quantifier = args[0] if args else "ALL"
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        predicate = args[3] if len(args) > 3 else None
        return {
            "type": "Quantifier",
            "quantifier": quantifier,
            "variable": variable,
            "in": source,
            "where": predicate,
        }

    def quantifier(self, args: List[Any]) -> str:
        """Extract and normalize the quantifier keyword (ALL, ANY, SINGLE, NONE).

        Quantifier keywords are case-insensitive in Cypher, so normalization to
        uppercase is necessary for consistent matching during execution.

        The default of "ALL" handles edge cases (though grammatically, a quantifier
        keyword is required).

        Args:
            args: Quantifier keyword token.

        Returns:
            Uppercased quantifier string: "ALL", "ANY", "SINGLE", or "NONE".
        """
        if not args:
            return "ALL"
        return str(args[0]).upper()

    def quantifier_variable(self, args: List[Any]) -> Optional[Any]:
        """Extract the iteration variable from a quantifier expression.

        The iteration variable represents each element being tested against the
        predicate. It's scoped to the quantifier expression.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.
        """
        return args[0] if args else None

    # ========================================================================
    # Map projection
    # ========================================================================

    def map_projection(self, args: List[Any]) -> Dict[str, Any]:
        """Transform map projection for selecting/transforming object properties.

        Map projection syntax: variable { property1, .property2, property3: expression, ...}

        Map projections create new maps by selecting and optionally transforming
        properties from a node, relationship, or map. This is essential for:
        - Shaping output data (selecting only needed properties)
        - Property transformation (renaming, computing derived values)
        - Creating anonymous objects in RETURN clauses

        Examples:
        - person { .name, .age } -> {name: person.name, age: person.age}
        - person { .*, age: person.age + 1 } -> all properties plus computed age
        - node { id: id(node), labels: labels(node) } -> custom object

        Elements can be:
        - Property selector: .name (copies property with same name)
        - Computed property: name: expression (evaluates expression for value)
        - Variable: other_var (includes all properties from other_var)
        - Wildcard: .* (includes all properties from base variable)

        The structure contains:
        - variable: base object to project from
        - elements: list of property selections/transformations

        This is similar to JavaScript object destructuring and provides a declarative
        way to shape data without manual property copying.

        Args:
            args: [variable_name, list_of_map_elements].

        Returns:
            Dict with type "MapProjection" containing variable and elements list.
        """
        variable = args[0] if args else None
        elements = args[1] if len(args) > 1 else []
        return {
            "type": "MapProjection",
            "variable": variable,
            "elements": elements,
        }

    def map_elements(self, args: List[Any]) -> List[Any]:
        """Transform comma-separated map projection elements into a list.

        Map elements define what properties to include in the projected map.
        Converting to a list is necessary for iteration during map construction.

        Empty elements list creates an empty map {}.

        Args:
            args: Individual map_element nodes.

        Returns:
            List of map element specifications (empty list if no elements).
        """
        return list(args) if args else []

    def map_element(
        self, args: List[Any]
    ) -> Union[Dict[str, Any], Optional[Any]]:
        """Transform a single map projection element.

        Map elements have different forms:
        1. Selector (string): property name or .* wildcard -> {"selector": name}
        2. Computed property (2 args): name: expression -> {"property": name, "value": expr}
        3. Pass-through: other forms parsed by grammar

        The different representations are necessary for execution to distinguish:
        - Which properties to copy (selectors)
        - Which properties to compute (computed)
        - Special operations (wildcard, variable inclusion)

        Args:
            args: Either [selector_string] or [property_name, value_expression] or other.

        Returns:
            Dict with appropriate structure for the element type.
        """
        match args:
            case [str() as selector]:
                return {"selector": selector}
            case [property_name, value]:
                return {"property": property_name, "value": value}
            case [single_arg]:
                return single_arg
            case _:
                return None

    # ========================================================================
    # Literals
    # ========================================================================

    def number_literal(self, args: List[Any]) -> Union[int, float]:
        """Transform number literals (integers and floats) into Python values.

        Numbers can be signed or unsigned. The grammar has already separated these
        cases, and the specific handlers (signed_number, unsigned_number) have
        converted string tokens to Python numeric types.

        Pass-through is necessary because the actual conversion happens in the
        specific number type handlers. This method just routes between them.

        Args:
            args: Single numeric value from signed_number or unsigned_number.

        Returns:
            Python int or float value, or 0 as fallback.
        """
        return args[0] if args else 0

    def signed_number(self, args: List[Any]) -> Union[int, float, str]:
        """Transform signed number literals into Python int or float values.

        Signed numbers can have + or - prefix and support:
        - Integers: -42, +100, -0x2A (hex), -0o52 (octal)
        - Floats: -3.14, +2.5e10, -1.5E-3
        - Special values: -INF, +INFINITY, -NAN

        The conversion process:
        1. Extract string representation from token
        2. Detect type (int vs float) by looking for . or e/E
        3. Strip format suffixes (f/F/d/D for floats)
        4. Parse to Python numeric type
        5. Handle special values (infinity, NaN)

        Underscores in numbers (1_000_000) are stripped for readability support.

        This conversion is necessary to:
        - Provide native Python values for arithmetic operations
        - Preserve numeric precision (int vs float)
        - Support special IEEE 754 values

        Args:
            args: Single signed number token.

        Returns:
            Python int, float, or special float value (inf/-inf/nan).
        """
        s = str(args[0])
        try:
            if (
                "." in s
                or "e" in s.lower()
                or "f" in s.lower()
                or "d" in s.lower()
            ):
                return float(s.rstrip("fFdD"))
            return int(s.replace("_", ""))
        except ValueError:
            if "inf" in s.lower():
                return float("inf") if s[0] != "-" else float("-inf")
            if "nan" in s.lower():
                return float("nan")
            return s

    def unsigned_number(self, args: List[Any]) -> Union[int, float, str]:
        """Transform unsigned number literals into Python int or float values.

        Unsigned numbers support the same formats as signed numbers but without
        the +/- prefix:
        - Integers: 42, 0x2A (hex), 0o52 (octal)
        - Floats: 3.14, 2.5e10, 1.5E-3
        - Special values: INF, INFINITY, NAN

        The conversion logic handles:
        - Hexadecimal (0x prefix): parsed with base 16
        - Octal (0o prefix): parsed with base 8, skip first 2 chars
        - Decimal: default base 10
        - Underscores: removed for readability (1_000_000)
        - Float format suffixes: stripped (f/F/d/D)

        This method parallels signed_number but handles only positive values.
        The separation is necessary because the grammar distinguishes them.

        Args:
            args: Single unsigned number token.

        Returns:
            Python int, float, or special float value (inf/nan).
        """
        s = str(args[0])
        try:
            if (
                "." in s
                or "e" in s.lower()
                or "f" in s.lower()
                or "d" in s.lower()
            ):
                return float(s.rstrip("fFdD"))
            if s.startswith("0x") or s.startswith("0X"):
                return int(s.replace("_", ""), 16)
            if s.startswith("0o") or s.startswith("0O"):
                return int(s[2:].replace("_", ""), 8)
            return int(s.replace("_", ""))
        except ValueError:
            if "inf" in s.lower():
                return float("inf")
            if "nan" in s.lower():
                return float("nan")
            return s

    def string_literal(self, args: List[Any]) -> str:
        """Transform string literals into Python string values.

        String literals in Cypher can be enclosed in single or double quotes:
        - 'Hello World'
        - "Hello World"

        This method processes:
        1. Quote removal (first and last character)
        2. Escape sequence handling:
           - \\n -> newline
           - \\t -> tab
           - \\r -> carriage return
           - \\\\ -> backslash
           - \\' -> single quote
           - \\" -> double quote

        The escape sequence processing is necessary to:
        - Support multi-line strings
        - Allow quotes within strings
        - Enable special characters in text

        More complex escape sequences (\\uXXXX for Unicode) could be added
        but are not currently implemented.

        Args:
            args: Single string token with quotes.

        Returns:
            Python string with quotes removed and escape sequences processed.
        """
        s = str(args[0])
        # Remove quotes and handle escape sequences
        if s.startswith("'") or s.startswith('"'):
            s = s[1:-1]
        # Basic escape sequence handling
        s = s.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
        s = s.replace("\\\\", "\\").replace("\\'", "'").replace('\\"', '"')
        return s

    def true(self, args: List[Any]) -> bool:
        """Transform the TRUE boolean literal keyword into Python True.

        Cypher's TRUE keyword is case-insensitive and represents the boolean
        true value. Converting to Python's True is necessary for:
        - Native boolean operations in the AST
        - Correct evaluation in boolean expressions
        - Type checking (distinguishing boolean from string "true")

        Args:
            args: Not used (TRUE is a keyword, not parameterized).

        Returns:
            Python boolean True.
        """
        return True

    def false(self, args: List[Any]) -> bool:
        """Transform the FALSE boolean literal keyword into Python False.

        Cypher's FALSE keyword is case-insensitive and represents the boolean
        false value. Converting to Python's False is necessary for:
        - Native boolean operations in the AST
        - Correct evaluation in boolean expressions
        - Type checking (distinguishing boolean from string "false")

        Args:
            args: Not used (FALSE is a keyword, not parameterized).

        Returns:
            Python boolean False.
        """
        return False

    def null_literal(self, args: List[Any]) -> None:
        """Transform the NULL literal keyword into Python None.

        Cypher's NULL keyword represents the absence of a value, similar to SQL NULL.
        Converting to Python's None is necessary for:
        - Representing missing/unknown values in the AST
        - Null propagation in expressions (NULL + anything = NULL)
        - Three-valued logic in boolean expressions (true/false/NULL)

        NULL has special semantics:
        - NULL = NULL is false (not true)
        - IS NULL and IS NOT NULL are the only null tests
        - Most operations with NULL produce NULL

        Args:
            args: Not used (NULL is a keyword, not parameterized).

        Returns:
            Python None.
        """
        return None

    def list_literal(self, args: List[Any]) -> List[Any]:
        """Transform list literal syntax [...] into Python list.

        List literals create ordered collections of values:
        - [1, 2, 3] -> integer list
        - ['a', 'b', 'c'] -> string list
        - [1, 'mixed', true, null] -> mixed-type list
        - [] -> empty list

        Lists in Cypher:
        - Are ordered (preserve insertion order)
        - Can contain mixed types
        - Support indexing list[0], slicing list[1..3]
        - Can be nested [[1,2], [3,4]]

        This method extracts the list of element expressions from the intermediate
        list_elements node. The default empty list handles [...] with no elements.

        Args:
            args: list_elements node containing list of element expressions.

        Returns:
            Python list of element values (empty list if no elements).
        """
        elements = next((a for a in args if isinstance(a, list)), [])
        return elements

    def list_elements(self, args: List[Any]) -> List[Any]:
        """Transform comma-separated list elements into Python list.

        List elements are arbitrary expressions that are evaluated to produce
        the list values. Each element can be:
        - Literals: [1, 'hello', true]
        - Variables: [n.name, m.age]
        - Expressions: [x*2, y+1, f(z)]
        - Nested lists: [[1,2], [3,4]]

        Converting to Python list is necessary for:
        - Standard Python list operations during execution
        - Consistent representation with Cypher list semantics
        - Easy iteration and indexing

        Args:
            args: Individual element expression nodes.

        Returns:
            Python list of element expressions (empty list if no elements).
        """
        return list(args) if args else []

    def map_literal(self, args: List[Any]) -> Dict[str, Any]:
        """Transform map literal syntax {...} into Python dict.

        Map literals create key-value collections (like JSON objects):
        - {name: 'Alice', age: 30} -> property map
        - {x: 1, y: 2, z: 3} -> coordinate map
        - {} -> empty map

        Maps in Cypher:
        - Have string keys (property names)
        - Can have any value types (including nested maps/lists)
        - Are used for node/relationship properties
        - Support property access via . or [] syntax

        This method extracts the dict of entries from the intermediate map_entries
        node. The default empty dict handles {} with no entries.

        Args:
            args: map_entries node containing dict of key-value pairs.

        Returns:
            Python dict mapping property names to values (empty dict if no entries).
        """
        entries = next(
            (a for a in args if isinstance(a, dict) and "entries" in str(a)),
            {"entries": {}},
        )
        return entries.get("entries", {})

    def map_entries(self, args: List[Any]) -> Dict[str, Dict[str, Any]]:
        """Transform comma-separated map entries into a dict wrapped in metadata.

        Map entries are key-value pairs where:
        - Keys are property names (identifiers)
        - Values are arbitrary expressions

        This method collects all entries into a single dict by iterating over
        the parsed entry nodes and extracting their key-value pairs.

        Wrapping in {"entries": result} is necessary to:
        - Pass the dict through the transformer without it being confused with
          other dict nodes in the AST
        - Provide a marker that this is an entries collection, not a semantic node

        Args:
            args: Individual map_entry nodes with key and value.

        Returns:
            Dict with "entries" key containing the collected key-value map.
        """
        result: dict[str, Any] = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"entries": result}

    def map_entry(self, args: List[Any]) -> Dict[str, Any]:
        """Transform a single map entry (key: value) into a key-value dict.

        Map entries define property bindings in map literals. The key is always
        a property name (identifier), and the value can be any expression.

        Examples:
        - name: 'Alice' -> {"key": "name", "value": "Alice"}
        - age: n.age + 1 -> {"key": "age", "value": <expression>}
        - active: true -> {"key": "active", "value": True}

        Structuring as {"key": ..., "value": ...} is necessary for map_entries
        to collect all entries into a final dict. This intermediate representation
        separates the parsing structure from the final map literal value.

        Args:
            args: [property_name, value_expression].

        Returns:
            Dict with "key" (property name string) and "value" (expression).
        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    # ========================================================================
    # Parameter
    # ========================================================================

    def parameter(self, args: List[Any]) -> Dict[str, Any]:
        """Transform parameter reference syntax ($param) into a Parameter node.

        Parameters are placeholders for values supplied at query execution time.
        Syntax: $paramName or $0, $1, $2 (positional)

        Examples:
        - MATCH (n:Person {name: $name}) RETURN n
        - CREATE (n:Person {age: $0})

        Parameters are essential for:
        - Query reuse (same query structure, different values)
        - SQL injection prevention (values never interpreted as code)
        - Query plan caching (parameterized queries can share plans)
        - Batch operations (execute same query with different parameters)

        Parameters can be:
        - Named: $name, $age, $customParam
        - Positional: $0, $1, $2, ...

        Creating a Parameter node with type marker is necessary for:
        1. Distinguishing parameters from regular variables
        2. Parameter binding during execution
        3. Type checking and validation
        4. Query plan optimization

        The $ prefix is removed by the grammar; only the name/index is captured.

        Args:
            args: Parameter name (string identifier or integer index).

        Returns:
            Dict with type "Parameter" and the parameter name.
        """
        name = args[0] if args else None
        return {"type": "Parameter", "name": name}

    def parameter_name(self, args: List[Any]) -> Union[int, str]:
        """Extract and normalize parameter name or index.

        Parameter names can be:
        - Identifiers: myParam, user_name, Age
        - Numeric indices: 0, 1, 2, 42

        This method attempts to parse numeric indices as integers, falling back
        to string identifiers. Integer indices are used for positional parameters
        in some query APIs.

        Stripping backticks is necessary for identifiers that use special characters
        or reserved words: `my-param`, `case`

        The conversion is necessary for:
        - Type-appropriate parameter lookup (int vs string keys)
        - Parameter map indexing during execution
        - Error reporting with correct parameter references

        Args:
            args: Parameter name token (identifier or number).

        Returns:
            Integer for numeric indices, or string for named parameters (backticks removed).
        """
        s = str(args[0])
        try:
            return int(s)
        except ValueError:
            return s.strip("`")

    # ========================================================================
    # Variable name
    # ========================================================================

    def variable_name(self, args: List[Any]) -> str:
        """Extract and normalize variable identifier names.

        Variable names are used throughout Cypher to bind and reference values:
        - Pattern variables: MATCH (n:Person) - 'n' is a variable
        - Relationship variables: MATCH (a)-[r:KNOWS]->(b) - 'r' is a variable
        - Aliases: RETURN n.name AS fullName - 'fullName' is a variable
        - Iteration variables: [x IN list WHERE x > 0] - 'x' is a variable

        Variables can be:
        - Regular identifiers: name, age, person, x1
        - Escaped identifiers (backtick-quoted): `my-var`, `case`, `Person Name`

        Stripping backticks is necessary because:
        1. Backticks are syntax for escaping, not part of the semantic name
        2. Allows reserved words as identifiers: `match`, `return`
        3. Allows special characters: `my-variable`, `user.name`
        4. Normalized names are needed for symbol table lookup

        Converting to string handles Token objects from the parser.

        Args:
            args: Single identifier token (may have backticks).

        Returns:
            Variable name as a string with backticks removed.
        """
        return str(args[0]).strip("`")


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
    transformer: "CypherASTTransformer"

    def __init__(self, debug: bool = False) -> None:
        """Initialize the grammar parser.

        Args:
            debug: If True, enable debug mode for more verbose parsing errors.
        """
        self.parser = Lark(
            CYPHER_GRAMMAR,
            parser="earley",  # Use Earley parser for better ambiguity handling
            debug=debug,
            maybe_placeholders=True,
            ambiguity="explicit",  # Handle ambiguous parses explicitly
        )
        self.transformer = CypherASTTransformer()

    def parse(self, query: str) -> Tree:
        """Parse a Cypher query into a parse tree.

        Args:
            query: The Cypher query string to parse.

        Returns:
            Tree: The Lark parse tree.

        Raises:
            lark.exceptions.LarkError: If the query has syntax errors.
        """
        return self.parser.parse(query)

    def parse_to_ast(self, query: str) -> Dict[str, Any]:
        """Parse a Cypher query into an AST.

        Args:
            query: The Cypher query string to parse.

        Returns:
            Dict: The abstract syntax tree as a dictionary.

        Raises:
            lark.exceptions.LarkError: If the query has syntax errors.
        """
        tree = self.parse(query)
        return self.transformer.transform(tree)

    def parse_file(self, filepath: Union[str, Path]) -> Tree:
        """Parse a Cypher query from a file.

        Args:
            filepath: Path to the file containing the Cypher query.

        Returns:
            Tree: The Lark parse tree.
        """
        with open(filepath, "r") as f:
            return self.parse(f.read())

    def parse_file_to_ast(self, filepath: Union[str, Path]) -> Dict[str, Any]:
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

        Args:
            query: The Cypher query string to validate.

        Returns:
            bool: True if the query is valid, False otherwise.
        """
        try:
            self.parse(query)
            return True
        except Exception:
            return False


def main() -> None:
    """Command-line interface for the grammar parser."""

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Parse openCypher queries using the BNF grammar"
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
        "-j", "--json", action="store_true", help="Output as JSON"
    )
    parser.add_argument(
        "-v",
        "--validate",
        action="store_true",
        help="Only validate, don't output",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug mode"
    )

    args: argparse.Namespace = parser.parse_args()

    cypher_parser: GrammarParser = GrammarParser(debug=args.debug)

    # Get query from file or argument
    query: str
    if args.file:
        with open(args.file, "r") as f:
            query = f.read()
    elif args.query:
        query = args.query
    else:
        # Read from stdin
        query = sys.stdin.read()

    try:
        if args.validate:
            is_valid = cypher_parser.validate(query)
            print("Valid" if is_valid else "Invalid")
            sys.exit(0 if is_valid else 1)

        if args.ast:
            result = cypher_parser.parse_to_ast(query)
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                import pprint

                pprint.pprint(result)
        else:
            tree = cypher_parser.parse(query)
            if args.json:
                # Convert tree to dict for JSON serialization
                print(json.dumps(tree.pretty(), indent=2))
            else:
                print(tree.pretty())

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.debug:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
