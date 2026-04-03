"""Mixin for Cypher clause and statement structure grammar rules.

Handles top-level query structure, UNION, MATCH, CREATE, MERGE, DELETE,
SET, REMOVE, FOREACH, UNWIND, CALL/YIELD, RETURN, WITH, WHERE,
ORDER BY, SKIP, and LIMIT.
"""

from __future__ import annotations

from typing import Any

from lark import Token, Tree


class ClauseRulesMixin:
    """Grammar rule methods for Cypher clauses and statement structure.

    Handles:
    - Top-level query structure (cypher_query, statement_list, query_statement)
    - UNION operations
    - Read clauses (MATCH, OPTIONAL MATCH, UNWIND)
    - Write clauses (CREATE, MERGE, DELETE, SET, REMOVE, FOREACH)
    - CALL/YIELD for procedure invocation
    - RETURN and WITH clauses (items, aliases, DISTINCT)
    - WHERE clause
    - ORDER BY, SKIP, LIMIT modifiers
    """

    def union_all_marker(self, args: list[Any]) -> bool:
        """Return True to signal that this UNION has the ALL qualifier."""
        return True

    def union_op(self, args: list[Any]) -> dict[str, Any]:
        """Transform a UNION [ALL] connector between two query statements.

        Args:
            args: ``[True]`` when the operator is ``UNION ALL``; ``[]`` when
                it is plain ``UNION``.

        Returns:
            Dict with ``type='UnionOp'`` and ``all`` flag.

        """
        return {"type": "UnionOp", "all": bool(args)}

    def cypher_query(self, args: list[Any]) -> dict[str, Any]:
        """Transform the root query node.

        Args:
            args: List of statement nodes from the statement_list rule.

        Returns:
            Dict with type "Query" or "UnionQuery" containing all statements.

        """
        sl = args[0] if args else []
        if isinstance(sl, dict) and sl.get("type") == "UnionStatementList":
            return {
                "type": "UnionQuery",
                "stmts": sl["stmts"],
                "all_flags": sl["all_flags"],
            }
        return {"type": "Query", "statements": args}

    def statement_list(self, args: list[Any]) -> Any:
        """Transform a list of statements, preserving UNION connectors.

        Args:
            args: Alternating statement nodes and UnionOp dicts.

        Returns:
            A plain list (single statement) or a UnionStatementList dict.

        """
        # Separate statements from union operator markers
        stmts = [
            a
            for a in args
            if not (isinstance(a, dict) and a.get("type") == "UnionOp")
        ]
        ops = [
            a
            for a in args
            if isinstance(a, dict) and a.get("type") == "UnionOp"
        ]

        if not ops:
            # Single statement — return as plain list (legacy behaviour)
            return list(args)

        # Build a structured union list.  `ops[i]` connects `stmts[i]` and
        # `stmts[i+1]`.  Store one all_flag per *connection* (N-1 elements).
        return {
            "type": "UnionStatementList",
            "stmts": stmts,
            "all_flags": [op["all"] for op in ops],
        }

    def query_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a read-only query statement (MATCH...RETURN).

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

    def update_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform an update statement (CREATE/MERGE/DELETE/SET/REMOVE).

        Args:
            args: Mix of prefix clauses, update clauses, and optional return.

        Returns:
            Dict with type "UpdateStatement" containing ordered clauses list
            plus legacy ``prefix``, ``updates``, and ``return`` keys.

        """
        _UPDATE_TYPES = frozenset(
            {
                "CreateClause",
                "MergeClause",
                "DeleteClause",
                "SetClause",
                "RemoveClause",
            },
        )

        prefix_clauses = []
        update_clauses = []
        return_clause = None
        ordered_clauses: list[Any] = []

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnStatement":
                    return_clause = arg
                elif arg.get("type") in _UPDATE_TYPES:
                    update_clauses.append(arg)
                    ordered_clauses.append(arg)
                else:
                    prefix_clauses.append(arg)
                    ordered_clauses.append(arg)

        if return_clause is not None:
            ordered_clauses.append(return_clause)

        return {
            "type": "UpdateStatement",
            "prefix": prefix_clauses,
            "updates": update_clauses,
            "return": return_clause,
            "clauses": ordered_clauses,
        }

    def update_clause(self, args: list[Any]) -> Any | None:
        """Pass through update clauses without modification.

        Args:
            args: Single update clause node (CREATE/MERGE/DELETE/SET/REMOVE).

        Returns:
            The update clause node unchanged.

        """
        return args[0] if args else None

    def read_clause(self, args: list[Any]) -> Any | None:
        """Pass through read clauses without modification.

        Args:
            args: Single read clause node (MATCH/UNWIND/WITH/CALL).

        Returns:
            The read clause node unchanged.

        """
        return args[0] if args else None

    def _ambig(self, args: list[Any]) -> Any:
        """Handle ambiguous parses by selecting the most specific interpretation.

        Priority order:
        1. Not expression nodes (highest priority for negation)
        2. Other structured dictionary nodes (semantic information)
        3. Primitive values (fallback)

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

    def statement(self, args: list[Any]) -> Any | None:
        """Pass through statement nodes.

        Args:
            args: Single statement node.

        Returns:
            The statement node unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # CALL statement
    # ========================================================================

    def call_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a standalone CALL statement for procedure invocation.

        Args:
            args: [procedure_reference, optional explicit_args, optional yield_clause]

        Returns:
            Dict with type "CallStatement" containing procedure info, args, and yield.

        """
        procedure = args[0] if args else None
        remaining = list(args[1:]) if len(args) > 1 else []

        arguments: list[Any] = []
        yield_clause = None

        if remaining:
            potential_args = remaining[0]
            if isinstance(potential_args, list):
                arguments = potential_args
                remaining = remaining[1:]
            elif (
                isinstance(potential_args, dict)
                and potential_args.get("type") == "YieldClause"
            ):
                yield_clause = potential_args
                remaining = remaining[1:]
            elif potential_args is None:
                arguments = []
                remaining = remaining[1:]
            else:
                arguments = [potential_args]
                remaining = remaining[1:]

        if remaining and yield_clause is None:
            yield_clause = remaining[0]

        yield_items: list[Any] = []
        where = None
        if isinstance(yield_clause, dict):
            items = yield_clause.get("items")
            if isinstance(items, list):
                yield_items = items
            where = yield_clause.get("where")

        return {
            "type": "Call",
            "procedure_name": procedure,
            "arguments": arguments,
            "yield_items": yield_items,
            "where": where,
        }

    def call_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a CALL clause within a larger query.

        Args:
            args: [procedure_reference, explicit_args, yield_clause]

        Returns:
            Dict with type "CallClause" containing procedure info, args, and yield.

        """
        procedure = args[0] if args else None
        remaining = list(args[1:]) if len(args) > 1 else []

        arguments: list[Any] = []
        yield_clause = None

        if remaining:
            potential_args = remaining[0]
            if isinstance(potential_args, list):
                arguments = potential_args
                remaining = remaining[1:]
            elif (
                isinstance(potential_args, dict)
                and potential_args.get("type") == "YieldClause"
            ):
                yield_clause = potential_args
                remaining = remaining[1:]
            elif potential_args is None:
                arguments = []
                remaining = remaining[1:]
            else:
                arguments = [potential_args]
                remaining = remaining[1:]

        if remaining and yield_clause is None:
            yield_clause = remaining[0]

        yield_items: list[Any] = []
        where = None
        if isinstance(yield_clause, dict):
            items = yield_clause.get("items")
            if isinstance(items, list):
                yield_items = items
            where = yield_clause.get("where")

        return {
            "type": "Call",
            "procedure_name": procedure,
            "arguments": arguments,
            "yield_items": yield_items,
            "where": where,
        }

    def procedure_reference(self, args: list[Any]) -> Any | None:
        """Extract procedure name reference.

        Args:
            args: Function name (possibly namespaced).

        Returns:
            Procedure name string or dict.

        """
        return args[0] if args else None

    def explicit_args(self, args: list[Any]) -> list[Any]:
        """Transform explicit procedure arguments list.

        Args:
            args: Expression nodes for each argument.

        Returns:
            List of argument expressions.

        """
        return list(args) if args else []

    def yield_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a YIELD clause that selects procedure output fields.

        Args:
            args: [yield_items or "*", optional where_clause]

        Returns:
            Dict with type "YieldClause" containing items and optional where filter.

        """
        items = args[0] if args else None
        where = args[1] if len(args) > 1 else None
        return {"type": "YieldClause", "items": items, "where": where}

    def yield_items(self, args: list[Any]) -> list[Any]:
        """Transform list of yielded fields.

        Args:
            args: Individual yield_item nodes.

        Returns:
            List of yield item dictionaries.

        """
        return list(args) if args else []

    def yield_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single yielded field with optional alias.

        Args:
            args: [field_name] or [field_name, alias]

        Returns:
            Dict with field name and optional alias.

        """
        variable = args[0] if args else None
        alias = args[1] if len(args) > 1 else None
        return {"type": "YieldItem", "variable": variable, "alias": alias}

    def field_name(self, args: list[Any]) -> str:
        """Extract field name identifier.

        Args:
            args: IDENTIFIER token.

        Returns:
            Field name string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # MATCH clause
    # ========================================================================

    def match_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a MATCH clause for pattern matching.

        Args:
            args: Mix of OPTIONAL keyword, pattern, and optional where_clause.

        Returns:
            Dict with type "MatchClause" containing optional flag, pattern, and where.

        """

        def _is_optional(value: Any) -> bool:
            """Return ``True`` if *value* represents the ``OPTIONAL`` keyword.

            Recursively checks strings, Lark :class:`Token`/:class:`Tree`
            nodes, dicts, and sequences to detect the OPTIONAL modifier in
            any parse-tree representation.
            """
            if isinstance(value, str):
                return value.upper() == "OPTIONAL"
            if isinstance(value, Token):
                token_value = str(getattr(value, "value", "") or "").upper()
                token_type = str(getattr(value, "type", "") or "").upper()
                return (
                    token_value == "OPTIONAL"
                    or token_type == "OPTIONAL_KEYWORD"
                )
            if isinstance(value, Tree):
                if getattr(value, "data", "") == "optional_keyword":
                    return True
                return any(_is_optional(child) for child in value.children)
            if isinstance(value, dict):
                return any(_is_optional(child) for child in value.values())
            if isinstance(value, (list, tuple)):
                return any(_is_optional(child) for child in value)
            return False

        optional = any(_is_optional(arg) for arg in args)
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

    def create_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a CREATE clause for creating new graph elements.

        Args:
            args: Pattern to create.

        Returns:
            Dict with type "CreateClause" containing the creation pattern.

        """
        return {"type": "CreateClause", "pattern": args[0] if args else None}

    # ========================================================================
    # MERGE clause
    # ========================================================================

    def merge_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a MERGE clause for create-or-match operations.

        Args:
            args: [pattern, optional merge_action nodes]

        Returns:
            Dict with type "MergeClause" containing pattern and conditional actions.

        """
        pattern = args[0] if args else None
        actions = args[1:] if len(args) > 1 else []
        return {"type": "MergeClause", "pattern": pattern, "actions": actions}

    def merge_action(self, args: list[Any]) -> dict[str, Any]:
        """Transform ON MATCH or ON CREATE action within MERGE.

        Args:
            args: ["MATCH" or "CREATE" keyword, set_clause]

        Returns:
            Dict with type "MergeAction" specifying trigger type and SET operation.

        """
        on_type: str | None = None
        set_clause = None

        for arg in args:
            if isinstance(arg, str):
                upper = arg.upper()
                if upper == "ON":
                    continue
                if upper in {"MATCH", "CREATE"}:
                    on_type = upper.lower()
                    continue
            if isinstance(arg, Token):
                token_value = str(getattr(arg, "value", "") or "").upper()
                token_type = str(getattr(arg, "type", "") or "").upper()
                candidate = token_value or token_type
                if candidate == "ON":
                    continue
                if candidate in {
                    "MATCH",
                    "CREATE",
                    "CREATE_KEYWORD",
                    "MATCH_KEYWORD",
                }:
                    on_type = (
                        "MATCH" if "MATCH" in candidate else "CREATE"
                    ).lower()
                    continue
            if isinstance(arg, dict):
                set_clause = arg

        if on_type is None:
            on_type = "create"

        return {"type": "MergeAction", "on": on_type, "set": set_clause}

    def merge_action_type(self, args: list[Any]) -> str:
        """Normalize merge action type tokens to keyword strings."""
        if not args:
            return ""
        token = args[0]
        if isinstance(token, str):
            return token.upper()
        if isinstance(token, Token):
            value = str(getattr(token, "value", "") or "").upper()
            if value:
                return value
            return str(getattr(token, "type", "") or "").upper()
        return str(token).upper()

    def merge_action_match(self, _args: list[Any]) -> str:
        """Return normalized MATCH keyword for merge action."""
        return "MATCH"

    def merge_action_create(self, _args: list[Any]) -> str:
        """Return normalized CREATE keyword for merge action."""
        return "CREATE"

    # ========================================================================
    # DELETE clause
    # ========================================================================

    def detach_keyword(self, args: list[Any]) -> str:
        """Transform detach_keyword rule."""
        return "DETACH"

    def delete_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a DELETE clause for removing graph elements.

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

    def delete_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of expressions to delete.

        Args:
            args: Expression nodes identifying items to delete.

        Returns:
            Dict containing list of items.

        """
        return {"items": list(args)}

    # ========================================================================
    # SET clause
    # ========================================================================

    def set_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a SET clause for updating graph properties.

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

    def set_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of SET operations.

        Args:
            args: Individual set_item nodes.

        Returns:
            Dict containing list of set operations.

        """
        return {"items": list(args)}

    def set_item(self, args: list[Any]) -> Any | None:
        """Pass through individual SET operation.

        Args:
            args: Single set operation (property/labels/all properties/add properties).

        Returns:
            The set operation node unchanged.

        """
        return args[0] if args else None

    def set_property_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single property assignment (e.g., SET n.age = 30).

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

    def set_labels_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform label assignment (e.g., SET n:Person:Employee).

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "SetLabels" containing variable and label list.

        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "SetLabels", "variable": variable, "labels": labels}

    def set_all_properties_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property map replacement (e.g., SET n = {name: 'Alice'}).

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

    def add_all_properties_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property map merge (e.g., SET n += {age: 30}).

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

    def remove_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a REMOVE clause for deleting properties or labels.

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

    def remove_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of REMOVE operations.

        Args:
            args: Individual remove_item nodes.

        Returns:
            Dict containing list of remove operations.

        """
        return {"items": list(args)}

    def remove_item(self, args: list[Any]) -> Any | None:
        """Pass through individual REMOVE operation.

        Args:
            args: Single remove operation (property or labels).

        Returns:
            The remove operation node unchanged.

        """
        return args[0] if args else None

    def remove_property_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property removal (e.g., REMOVE n.age).

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

    def remove_labels_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform label removal (e.g., REMOVE n:Person).

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "RemoveLabels" containing variable and labels to remove.

        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "RemoveLabels", "variable": variable, "labels": labels}

    # ========================================================================
    # FOREACH clause
    # ========================================================================

    def foreach_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a FOREACH clause for iterative list mutation.

        Args:
            args: [variable_name (str), list_expression, update_clause, ...]

        Returns:
            Dict with type "ForeachClause" containing ``variable``,
            ``list_expression``, and ``clauses`` (list of update clause dicts).

        """
        variable = args[0] if args else None
        list_expr = args[1] if len(args) > 1 else None
        clauses = list(args[2:]) if len(args) > 2 else []
        return {
            "type": "ForeachClause",
            "variable": variable,
            "list_expression": list_expr,
            "clauses": clauses,
        }

    # ========================================================================
    # UNWIND clause
    # ========================================================================

    def unwind_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform an UNWIND clause for list expansion.

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

    def with_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WITH clause for query chaining and variable passing.

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
    def distinct_keyword(self, args: list[Any]) -> str:
        """Transform distinct_keyword rule."""
        return "DISTINCT"

    # RETURN clause
    # ========================================================================

    def return_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a RETURN clause for query output specification.

        Args:
            args: Mix of DISTINCT keyword, return body/items/``*``, and optional clauses.

        Returns:
            Dict with type "ReturnStatement" containing all output specifications.

        """
        # LOGGER.debug(f"DEBUG return_clause args: {args} types: {[type(a) for a in args]}")
        distinct = False
        for a in args:
            if (hasattr(a, "type") and a.type == "DISTINCT") or (
                isinstance(a, str) and a.upper() == "DISTINCT"
            ):
                distinct = True
            elif hasattr(a, "value") and str(a.value).upper() == "DISTINCT":
                distinct = True

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

    def return_body(self, args: list[Any]) -> str | list[Any]:
        r"""Extract the body of a RETURN clause (items or \*).

        Args:
            args: Either ``"*"`` string or return_items list.

        Returns:
            Either ``"*"`` string or list of return items.

        """
        if args and args[0] == "*":
            return "*"
        # args[0] is already a list from return_items
        return args[0] if args else []

    def return_items(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated list of return items.

        Args:
            args: Individual return_item nodes.

        Returns:
            List of return item dictionaries.

        """
        return list(args) if args else []

    def return_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single return item with optional alias.

        Args:
            args: [expression] or [expression, alias]

        Returns:
            Dict with type "ReturnItem" containing expression and optional alias.

        """
        if len(args) == 1:
            return {"type": "ReturnItem", "expression": args[0], "alias": None}
        return {"type": "ReturnItem", "expression": args[0], "alias": args[1]}

    def return_alias(self, args: list[Any]) -> str:
        """Extract return item alias identifier.

        Args:
            args: IDENTIFIER token.

        Returns:
            Alias string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # WHERE clause
    # ========================================================================

    def where_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHERE clause for filtering.

        Args:
            args: Single boolean expression for the filter condition.

        Returns:
            Dict with type "WhereClause" containing the filter expression.

        """
        return {"type": "WhereClause", "condition": args[0] if args else None}

    # ========================================================================
    # ORDER BY clause
    # ========================================================================

    def order_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform an ORDER BY clause for result sorting.

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

    def order_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of ORDER BY items.

        Args:
            args: Individual order_item nodes.

        Returns:
            Dict containing list of order specifications.

        """
        return {"items": list(args)}

    def nulls_placement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a NULLS FIRST or NULLS LAST clause.

        Args:
            args: [NULLS_FIRST_KEYWORD | NULLS_LAST_KEYWORD token]

        Returns:
            Dict with placement field ("first" or "last").

        """
        token = args[0] if args else None
        placement = "first" if str(token).upper() == "FIRST" else "last"
        return {"type": "nulls_placement", "placement": placement}

    def order_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single ORDER BY item with optional direction and nulls placement.

        Args:
            args: [expression, optional direction string, optional nulls_placement dict]

        Returns:
            Dict with expression, direction, and nulls_placement fields.

        """
        expr = args[0] if args else None
        direction = None
        nulls_kw = None

        for arg in args[1:]:
            if isinstance(arg, dict) and arg.get("type") == "nulls_placement":
                nulls_kw = arg["placement"]
            elif isinstance(arg, str):
                direction = arg

        direction_value = direction or "asc"
        ascending = direction_value not in {"desc", "descending"}

        return {
            "type": "OrderByItem",
            "expression": expr,
            "ascending": ascending,
            "nulls_placement": nulls_kw,
        }

    def order_direction(self, args: list[Any]) -> str:
        """Normalize ORDER BY direction keywords.

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

    def skip_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a SKIP clause for result pagination.

        Args:
            args: Expression evaluating to number of rows to skip.

        Returns:
            Dict with type "SkipClause" containing skip count expression.

        """
        return {"type": "SkipClause", "value": args[0] if args else None}

    def limit_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a LIMIT clause for result set size restriction.

        Args:
            args: Expression evaluating to maximum number of rows to return.

        Returns:
            Dict with type "LimitClause" containing limit count expression.

        """
        return {"type": "LimitClause", "value": args[0] if args else None}
