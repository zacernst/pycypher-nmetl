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

from lark import Lark, Transformer, v_args, Tree, Token
from typing import Any, List, Dict, Optional, Union
from pathlib import Path
import json


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
                    | full_rel_both
                    | full_rel_any

full_rel_left: "<-" rel_detail? "-"

full_rel_right: "-" rel_detail? "->"

full_rel_both: "<-" rel_detail? "->"

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

?not_expression: ("NOT"i)* comparison_expression

?comparison_expression: null_predicate_expression (comparison_op null_predicate_expression)*

?null_predicate_expression: string_predicate_expression null_check_op?

null_check_op: "IS"i "NOT"i "NULL"i  -> is_not_null
             | "IS"i "NULL"i         -> is_null

?string_predicate_expression: add_expression (string_predicate_op add_expression)*

comparison_op: "=" | "<>" | "<" | ">" | "<=" | ">="

string_predicate_op: "STARTS"i "WITH"i
                   | "ENDS"i "WITH"i
                   | "CONTAINS"i
                   | "=~"
                   | "IN"i

?add_expression: mult_expression (("+"|"-") mult_expression)*

?mult_expression: power_expression (("*"|"/"|"%") power_expression)*

?power_expression: unary_expression ("^" unary_expression)*

?unary_expression: ("+"|"-") unary_expression
                 | postfix_expression

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

// Identifiers (regular or backtick-quoted)
IDENTIFIER: REGULAR_IDENTIFIER
          | ESCAPED_IDENTIFIER

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
STRING: /'([^'\\]|\\.)*'/
      | /"([^"\\]|\\.)*"/

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
    """

    # ========================================================================
    # Top-level query structure
    # ========================================================================
    
    def cypher_query(self, args):
        return {"type": "Query", "statements": args}

    def statement_list(self, args):
        return list(args)

    def query_statement(self, args):
        read_clauses = [a for a in args if not isinstance(a, dict) or a.get("type") != "ReturnStatement"]
        return_clause = next((a for a in args if isinstance(a, dict) and a.get("type") == "ReturnStatement"), None)
        return {"type": "QueryStatement", "clauses": read_clauses, "return": return_clause}

    def update_statement(self, args):
        prefix_clauses = []
        update_clauses = []
        return_clause = None
        
        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnStatement":
                    return_clause = arg
                elif arg.get("type") in ["CreateClause", "MergeClause", "DeleteClause", "SetClause", "RemoveClause"]:
                    update_clauses.append(arg)
                else:
                    prefix_clauses.append(arg)
                    
        return {"type": "UpdateStatement", "prefix": prefix_clauses, "updates": update_clauses, "return": return_clause}
    
    def update_clause(self, args):
        """Pass through the update clause (create/merge/delete/set/remove)."""
        return args[0] if args else None

    def statement(self, args):
        return args[0] if args else None

    # ========================================================================
    # CALL statement
    # ========================================================================
    
    def call_statement(self, args):
        return {"type": "CallStatement", "procedure": args[0] if args else None, 
                "args": args[1] if len(args) > 1 else None, "yield": args[2] if len(args) > 2 else None}

    def call_clause(self, args):
        return {"type": "CallClause", "procedure": args[0] if args else None,
                "args": args[1] if len(args) > 1 else None, "yield": args[2] if len(args) > 2 else None}

    def procedure_reference(self, args):
        return args[0] if args else None

    def explicit_args(self, args):
        return list(args) if args else []

    def yield_clause(self, args):
        items = args[0] if args else None
        where = args[1] if len(args) > 1 else None
        return {"type": "YieldClause", "items": items, "where": where}

    def yield_items(self, args):
        return list(args) if args else []

    def yield_item(self, args):
        if len(args) == 1:
            return {"field": args[0]}
        return {"field": args[0], "alias": args[1]}

    def field_name(self, args):
        return str(args[0]).strip('`')

    # ========================================================================
    # MATCH clause
    # ========================================================================
    
    def match_clause(self, args):
        optional = any(str(a).upper() == "OPTIONAL" for a in args if isinstance(a, str))
        pattern = next((a for a in args if isinstance(a, dict) and a.get("type") == "Pattern"), None)
        where = next((a for a in args if isinstance(a, dict) and a.get("type") == "WhereClause"), None)
        return {"type": "MatchClause", "optional": optional, "pattern": pattern, "where": where}

    # ========================================================================
    # CREATE clause
    # ========================================================================
    
    def create_clause(self, args):
        return {"type": "CreateClause", "pattern": args[0] if args else None}

    # ========================================================================
    # MERGE clause
    # ========================================================================
    
    def merge_clause(self, args):
        pattern = args[0] if args else None
        actions = args[1:] if len(args) > 1 else []
        return {"type": "MergeClause", "pattern": pattern, "actions": actions}

    def merge_action(self, args):
        on_type = "match" if any(str(a).upper() == "MATCH" for a in args if isinstance(a, str)) else "create"
        set_clause = next((a for a in args if isinstance(a, dict)), None)
        return {"type": "MergeAction", "on": on_type, "set": set_clause}

    # ========================================================================
    # DELETE clause
    # ========================================================================
    
    def delete_clause(self, args):
        detach = any(str(a).upper() == "DETACH" for a in args if isinstance(a, str))
        items = next((a for a in args if isinstance(a, dict) and "items" in str(a)), {"items": []})
        return {"type": "DeleteClause", "detach": detach, "items": items.get("items", [])}

    def delete_items(self, args):
        return {"items": list(args)}

    # ========================================================================
    # SET clause
    # ========================================================================
    
    def set_clause(self, args):
        items = next((a for a in args if isinstance(a, dict) and "items" in str(a)), {"items": []})
        return {"type": "SetClause", "items": items.get("items", [])}

    def set_items(self, args):
        return {"items": list(args)}

    def set_item(self, args):
        return args[0] if args else None

    def set_property_item(self, args):
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        value = args[2] if len(args) > 2 else None
        return {"type": "SetProperty", "variable": variable, "property": prop, "value": value}

    def set_labels_item(self, args):
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "SetLabels", "variable": variable, "labels": labels}

    def set_all_properties_item(self, args):
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {"type": "SetAllProperties", "variable": variable, "value": value}

    def add_all_properties_item(self, args):
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {"type": "AddAllProperties", "variable": variable, "value": value}

    # ========================================================================
    # REMOVE clause
    # ========================================================================
    
    def remove_clause(self, args):
        items = next((a for a in args if isinstance(a, dict) and "items" in str(a)), {"items": []})
        return {"type": "RemoveClause", "items": items.get("items", [])}

    def remove_items(self, args):
        return {"items": list(args)}

    def remove_item(self, args):
        return args[0] if args else None

    def remove_property_item(self, args):
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        return {"type": "RemoveProperty", "variable": variable, "property": prop}

    def remove_labels_item(self, args):
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "RemoveLabels", "variable": variable, "labels": labels}

    # ========================================================================
    # UNWIND clause
    # ========================================================================
    
    def unwind_clause(self, args):
        expr = args[0] if args else None
        var = args[1] if len(args) > 1 else None
        return {"type": "UnwindClause", "expression": expr, "variable": var}

    # ========================================================================
    # WITH clause
    # ========================================================================
    
    def with_clause(self, args):
        distinct = any(str(a).upper() == "DISTINCT" for a in args if isinstance(a, str))
        body = None
        where = None
        order = None
        skip = None
        limit = None
        
        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "WhereClause":
                    where = arg
                elif arg.get("type") == "OrderClause":
                    order = arg
                elif arg.get("type") == "SkipClause":
                    skip = arg
                elif arg.get("type") == "LimitClause":
                    limit = arg
                elif body is None:
                    body = arg
                    
        return {"type": "WithClause", "distinct": distinct, "body": body, "where": where, 
                "order": order, "skip": skip, "limit": limit}

    # ========================================================================
    # RETURN clause
    # ========================================================================
    
    def return_clause(self, args):
        distinct = any(str(a).upper() == "DISTINCT" for a in args if isinstance(a, str))
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
        
        return {"type": "ReturnStatement", "distinct": distinct, "body": body,
                "order": order, "skip": skip, "limit": limit}

    def return_body(self, args):
        if args and args[0] == "*":
            return "*"
        return list(args) if args else []

    def return_items(self, args):
        return list(args) if args else []

    def return_item(self, args):
        if len(args) == 1:
            return {"expression": args[0]}
        return {"expression": args[0], "alias": args[1]}

    def return_alias(self, args):
        return str(args[0]).strip('`')

    # ========================================================================
    # WHERE clause
    # ========================================================================
    
    def where_clause(self, args):
        return {"type": "WhereClause", "condition": args[0] if args else None}

    # ========================================================================
    # ORDER BY clause
    # ========================================================================
    
    def order_clause(self, args):
        items = next((a for a in args if isinstance(a, dict) and "items" in str(a)), {"items": []})
        return {"type": "OrderClause", "items": items.get("items", [])}

    def order_items(self, args):
        return {"items": list(args)}

    def order_item(self, args):
        expr = args[0] if args else None
        direction = args[1] if len(args) > 1 else "asc"
        return {"expression": expr, "direction": direction}

    def order_direction(self, args):
        d = str(args[0]).upper()
        return "desc" if d in ["DESC", "DESCENDING"] else "asc"

    # ========================================================================
    # SKIP and LIMIT
    # ========================================================================
    
    def skip_clause(self, args):
        return {"type": "SkipClause", "value": args[0] if args else None}

    def limit_clause(self, args):
        return {"type": "LimitClause", "value": args[0] if args else None}

    # ========================================================================
    # Pattern matching
    # ========================================================================
    
    def pattern(self, args):
        return {"type": "Pattern", "paths": list(args)}

    def path_pattern(self, args):
        variable = None
        element = None
        for arg in args:
            if isinstance(arg, str) and variable is None:
                variable = arg
            else:
                element = arg
        return {"type": "PathPattern", "variable": variable, "element": element}

    def pattern_element(self, args):
        return {"type": "PatternElement", "parts": list(args)}

    def shortest_path(self, args):
        all_shortest = any("ALL" in str(a).upper() for a in args if isinstance(a, str))
        nodes_and_rel = [a for a in args if isinstance(a, dict)]
        return {"type": "ShortestPath", "all": all_shortest, "parts": nodes_and_rel}

    # ========================================================================
    # Node pattern
    # ========================================================================
    
    def node_pattern(self, args):
        filler = args[0] if args else {}
        return {"type": "NodePattern", **filler} if isinstance(filler, dict) else {"type": "NodePattern", "filler": filler}

    def node_pattern_filler(self, args):
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                # Check if this is a labels or where dict (has known keys)
                if 'labels' in arg or 'where' in arg:
                    filler.update(arg)
                # Check if this is a properties object (no special keys)
                elif 'type' not in arg and arg:
                    # This is a properties dict - store it as 'properties'
                    filler["properties"] = arg
                else:
                    # Other structured objects
                    filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def node_labels(self, args):
        return {"labels": list(args)}

    def label_expression(self, args):
        if len(args) == 1:
            return args[0]
        return {"type": "LabelExpression", "parts": list(args)}

    def label_term(self, args):
        if len(args) == 1:
            return args[0]
        return {"type": "LabelOr", "terms": list(args)}

    def label_factor(self, args):
        return args[0] if args else None

    def label_primary(self, args):
        return args[0] if args else None

    def label_name(self, args):
        return str(args[0]).lstrip(":").strip('`')

    def node_properties(self, args):
        return args[0] if args else None

    def node_where(self, args):
        return {"where": args[0] if args else None}

    # ========================================================================
    # Relationship pattern
    # ========================================================================
    
    def relationship_pattern(self, args):
        return args[0] if args else None

    def full_rel_left(self, args):
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "left", **detail}

    def full_rel_right(self, args):
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "right", **detail}

    def full_rel_both(self, args):
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "both", **detail}

    def full_rel_any(self, args):
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "any", **detail}

    def rel_detail(self, args):
        return args[0] if args else {}

    def rel_filler(self, args):
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def rel_types(self, args):
        return {"types": list(args)}

    def rel_type(self, args):
        return str(args[0]).strip('`')

    def rel_properties(self, args):
        return {"properties": args[0] if args else None}

    def rel_where(self, args):
        return {"where": args[0] if args else None}

    def path_length(self, args):
        range_spec = args[0] if args else None
        return {"pathLength": range_spec}

    def path_length_range(self, args):
        if len(args) == 1:
            return {"fixed": int(str(args[0]))}
        elif len(args) == 2:
            return {"min": int(str(args[0])) if args[0] else None, "max": int(str(args[1])) if args[1] else None}
        return {"unbounded": True}

    # ========================================================================
    # Properties
    # ========================================================================
    
    def properties(self, args):
        props = next((a for a in args if isinstance(a, dict) and "props" in str(a)), {"props": {}})
        return props.get("props", {})

    def property_list(self, args):
        result = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"props": result}

    def property_key_value(self, args):
        return {"key": str(args[0]), "value": args[1] if len(args) > 1 else None}

    def property_name(self, args):
        return str(args[0]).strip('`')

    # ========================================================================
    # Expressions
    # ========================================================================
    
    def or_expression(self, args):
        if len(args) == 1:
            return args[0]
        return {"type": "Or", "operands": list(args)}

    def xor_expression(self, args):
        if len(args) == 1:
            return args[0]
        return {"type": "Xor", "operands": list(args)}

    def and_expression(self, args):
        if len(args) == 1:
            return args[0]
        return {"type": "And", "operands": list(args)}

    def not_expression(self, args):
        # Count NOTs
        not_count = sum(1 for a in args if isinstance(a, str) and str(a).upper() == "NOT")
        expr = next((a for a in args if not (isinstance(a, str) and str(a).upper() == "NOT")), None)
        
        if not_count == 0:
            return expr
        elif not_count % 2 == 1:
            return {"type": "Not", "operand": expr}
        else:
            return expr

    def comparison_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i+1] if i+1 < len(args) else None
            result = {"type": "Comparison", "operator": str(op), "left": result, "right": right}
        return result

    def null_predicate_expression(self, args):
        expr = args[0] if args else None
        if len(args) > 1:
            # Has a null check
            op_type = args[1]
            return {"type": "NullCheck", "operator": op_type, "operand": expr}
        return expr

    def null_check_op(self, args):
        # This will be called by the is_null or is_not_null aliases
        return None

    def string_predicate_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i+1] if i+1 < len(args) else None
            result = {"type": "StringPredicate", "operator": str(op), "left": result, "right": right}
        return result

    def comparison_op(self, args):
        if not args:
            return "="
        return str(args[0])

    def string_predicate_op(self, args):
        return " ".join(str(a).upper() for a in args)

    def is_null(self, args):
        return "IS NULL"

    def is_not_null(self, args):
        return "IS NOT NULL"

    def add_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "+"
            right = args[i+1] if i+1 < len(args) else None
            result = {"type": "Arithmetic", "operator": op, "left": result, "right": right}
        return result

    def mult_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "*"
            right = args[i+1] if i+1 < len(args) else None
            result = {"type": "Arithmetic", "operator": op, "left": result, "right": right}
        return result

    def power_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = "^"
            right = args[i+1] if i+1 < len(args) else None
            result = {"type": "Arithmetic", "operator": op, "left": result, "right": right}
        return result

    def unary_expression(self, args):
        if len(args) == 1:
            return args[0]
        sign = str(args[0])
        operand = args[1] if len(args) > 1 else None
        return {"type": "Unary", "operator": sign, "operand": operand}

    def postfix_expression(self, args):
        if len(args) == 1:
            return args[0]
        result = args[0]
        for op in args[1:]:
            if isinstance(op, dict) and op.get("type") == "PropertyLookup":
                result = {"type": "PropertyAccess", "object": result, "property": op.get("property")}
            elif isinstance(op, dict) and op.get("type") == "IndexLookup":
                result = {"type": "IndexAccess", "object": result, "index": op.get("index")}
            elif isinstance(op, dict) and op.get("type") == "Slicing":
                result = {"type": "Slice", "object": result, "from": op.get("from"), "to": op.get("to")}
        return result

    def postfix_op(self, args):
        return args[0] if args else None

    def property_lookup(self, args):
        return {"type": "PropertyLookup", "property": args[0] if args else None}

    def index_lookup(self, args):
        return {"type": "IndexLookup", "index": args[0] if args else None}

    def slicing(self, args):
        from_expr = args[0] if args and args[0] is not None else None
        to_expr = args[1] if len(args) > 1 and args[1] is not None else None
        return {"type": "Slicing", "from": from_expr, "to": to_expr}

    # ========================================================================
    # Count star
    # ========================================================================
    
    def count_star(self, args):
        return {"type": "CountStar"}

    # ========================================================================
    # EXISTS expression
    # ========================================================================
    
    def exists_expression(self, args):
        content = args[0] if args else None
        return {"type": "Exists", "content": content}

    def exists_content(self, args):
        return args[0] if args else None

    # ========================================================================
    # Function invocation
    # ========================================================================
    
    def function_invocation(self, args):
        name = args[0] if args else "unknown"
        func_args = args[1] if len(args) > 1 else None
        return {"type": "FunctionInvocation", "name": name, "arguments": func_args}

    def function_args(self, args):
        distinct = any(str(a).upper() == "DISTINCT" for a in args if isinstance(a, str))
        arg_list = next((a for a in args if isinstance(a, list)), [])
        return {"distinct": distinct, "arguments": arg_list}

    def function_arg_list(self, args):
        return list(args)

    def function_name(self, args):
        namespace = args[0] if len(args) > 1 else None
        simple_name = args[-1] if args else "unknown"
        return {"namespace": namespace, "name": simple_name} if namespace else simple_name

    def namespace_name(self, args):
        return ".".join(str(a).strip('`') for a in args)

    def function_simple_name(self, args):
        return str(args[0]).strip('`')

    # ========================================================================
    # Case expression
    # ========================================================================
    
    def case_expression(self, args):
        return args[0] if args else None

    def simple_case(self, args):
        operand = args[0] if args else None
        when_clauses = [a for a in args[1:] if isinstance(a, dict) and a.get("type") == "SimpleWhen"]
        else_clause = next((a for a in args if isinstance(a, dict) and a.get("type") == "Else"), None)
        return {"type": "SimpleCase", "operand": operand, "when": when_clauses, "else": else_clause}

    def searched_case(self, args):
        when_clauses = [a for a in args if isinstance(a, dict) and a.get("type") == "SearchedWhen"]
        else_clause = next((a for a in args if isinstance(a, dict) and a.get("type") == "Else"), None)
        return {"type": "SearchedCase", "when": when_clauses, "else": else_clause}

    def simple_when(self, args):
        operands = args[0] if args else []
        result = args[1] if len(args) > 1 else None
        return {"type": "SimpleWhen", "operands": operands, "result": result}

    def searched_when(self, args):
        condition = args[0] if args else None
        result = args[1] if len(args) > 1 else None
        return {"type": "SearchedWhen", "condition": condition, "result": result}

    def when_operands(self, args):
        return list(args)

    def else_clause(self, args):
        return {"type": "Else", "value": args[0] if args else None}

    # ========================================================================
    # List comprehension
    # ========================================================================
    
    def list_comprehension(self, args):
        variable = args[0] if args else None
        source = args[1] if len(args) > 1 else None
        filter_expr = args[2] if len(args) > 2 else None
        projection = args[3] if len(args) > 3 else None
        return {"type": "ListComprehension", "variable": variable, "in": source, 
                "where": filter_expr, "projection": projection}

    def list_variable(self, args):
        return args[0] if args else None

    def list_filter(self, args):
        return args[0] if args else None

    def list_projection(self, args):
        return args[0] if args else None

    # ========================================================================
    # Pattern comprehension
    # ========================================================================
    
    def pattern_comprehension(self, args):
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
                    
        return {"type": "PatternComprehension", "variable": variable, "pattern": pattern,
                "where": filter_expr, "projection": projection}

    def pattern_comp_variable(self, args):
        return args[0] if args else None

    def pattern_filter(self, args):
        return args[0] if args else None

    def pattern_projection(self, args):
        return args[0] if args else None

    # ========================================================================
    # Reduce expression
    # ========================================================================
    
    def reduce_expression(self, args):
        accumulator = args[0] if args else None
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        step = args[3] if len(args) > 3 else None
        return {"type": "Reduce", "accumulator": accumulator, "variable": variable,
                "in": source, "step": step}

    def reduce_accumulator(self, args):
        variable = args[0] if args else None
        init = args[1] if len(args) > 1 else None
        return {"variable": variable, "init": init}

    def reduce_variable(self, args):
        return args[0] if args else None

    # ========================================================================
    # Quantifier expressions
    # ========================================================================
    
    def quantifier_expression(self, args):
        quantifier = args[0] if args else "ALL"
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        predicate = args[3] if len(args) > 3 else None
        return {"type": "Quantifier", "quantifier": quantifier, "variable": variable,
                "in": source, "where": predicate}

    def quantifier(self, args):
        if not args:
            return "ALL"
        return str(args[0]).upper()

    def quantifier_variable(self, args):
        return args[0] if args else None

    # ========================================================================
    # Map projection
    # ========================================================================
    
    def map_projection(self, args):
        variable = args[0] if args else None
        elements = args[1] if len(args) > 1 else []
        return {"type": "MapProjection", "variable": variable, "elements": elements}

    def map_elements(self, args):
        return list(args) if args else []

    def map_element(self, args):
        if len(args) == 1 and isinstance(args[0], str):
            return {"selector": args[0]}
        elif len(args) == 2:
            return {"property": args[0], "value": args[1]}
        return args[0] if args else None

    # ========================================================================
    # Literals
    # ========================================================================
    
    def number_literal(self, args):
        return args[0] if args else 0

    def signed_number(self, args):
        s = str(args[0])
        try:
            if '.' in s or 'e' in s.lower() or 'f' in s.lower() or 'd' in s.lower():
                return float(s.rstrip('fFdD'))
            return int(s.replace('_', ''))
        except ValueError:
            if 'inf' in s.lower():
                return float('inf') if s[0] != '-' else float('-inf')
            if 'nan' in s.lower():
                return float('nan')
            return s

    def unsigned_number(self, args):
        s = str(args[0])
        try:
            if '.' in s or 'e' in s.lower() or 'f' in s.lower() or 'd' in s.lower():
                return float(s.rstrip('fFdD'))
            if s.startswith('0x') or s.startswith('0X'):
                return int(s.replace('_', ''), 16)
            if s.startswith('0o') or s.startswith('0O'):
                return int(s[2:].replace('_', ''), 8)
            return int(s.replace('_', ''))
        except ValueError:
            if 'inf' in s.lower():
                return float('inf')
            if 'nan' in s.lower():
                return float('nan')
            return s

    def string_literal(self, args):
        s = str(args[0])
        # Remove quotes and handle escape sequences
        if s.startswith("'") or s.startswith('"'):
            s = s[1:-1]
        # Basic escape sequence handling
        s = s.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
        s = s.replace('\\\\', '\\').replace("\\'", "'").replace('\\"', '"')
        return s

    def true(self, args):
        return True

    def false(self, args):
        return False

    def null_literal(self, args):
        return None

    def list_literal(self, args):
        elements = next((a for a in args if isinstance(a, list)), [])
        return elements

    def list_elements(self, args):
        return list(args) if args else []

    def map_literal(self, args):
        entries = next((a for a in args if isinstance(a, dict) and "entries" in str(a)), {"entries": {}})
        return entries.get("entries", {})

    def map_entries(self, args):
        result = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"entries": result}

    def map_entry(self, args):
        return {"key": str(args[0]), "value": args[1] if len(args) > 1 else None}

    # ========================================================================
    # Parameter
    # ========================================================================
    
    def parameter(self, args):
        name = args[0] if args else None
        return {"type": "Parameter", "name": name}

    def parameter_name(self, args):
        s = str(args[0])
        try:
            return int(s)
        except ValueError:
            return s.strip('`')

    # ========================================================================
    # Variable name
    # ========================================================================
    
    def variable_name(self, args):
        return str(args[0]).strip('`')


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

    def __init__(self, debug: bool = False):
        """Initialize the grammar parser.
        
        Args:
            debug: If True, enable debug mode for more verbose parsing errors.
        """
        self.parser = Lark(
            CYPHER_GRAMMAR,
            parser='earley',  # Use Earley parser for better ambiguity handling
            debug=debug,
            maybe_placeholders=True,
            ambiguity='explicit'  # Handle ambiguous parses explicitly
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
        with open(filepath, 'r') as f:
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


def main():
    """Command-line interface for the grammar parser."""
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Parse openCypher queries using the BNF grammar')
    parser.add_argument('query', nargs='?', help='Cypher query to parse')
    parser.add_argument('-f', '--file', help='File containing Cypher query')
    parser.add_argument('-a', '--ast', action='store_true', help='Output AST instead of parse tree')
    parser.add_argument('-j', '--json', action='store_true', help='Output as JSON')
    parser.add_argument('-v', '--validate', action='store_true', help='Only validate, don\'t output')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    cypher_parser = GrammarParser(debug=args.debug)

    # Get query from file or argument
    if args.file:
        with open(args.file, 'r') as f:
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
