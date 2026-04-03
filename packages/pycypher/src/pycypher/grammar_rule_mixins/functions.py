"""Mixin for function invocation and complex expression grammar rules.

Handles function invocation, CASE expressions, list comprehension,
pattern comprehension, REDUCE, quantifier expressions, and map projection.
"""

from __future__ import annotations

from typing import Any


class FunctionRulesMixin:
    """Grammar rule methods for function invocations and complex expressions.

    Handles:
    - Function invocation (built-in, user-defined, namespaced)
    - Function arguments and DISTINCT modifier
    - CASE expressions (simple and searched forms)
    - List comprehension (variable IN list WHERE cond | projection)
    - Pattern comprehension (graph pattern matching into lists)
    - REDUCE expression (fold/reduce over lists)
    - Quantifier expressions (ALL, ANY, SINGLE, NONE)
    - Map projection (property selection and transformation)
    """

    def function_invocation(self, args: list[Any]) -> dict[str, Any]:
        """Transform function invocation (built-in or user-defined functions).

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
        self,
        args: list[Any],
    ) -> dict[str, bool | list[Any]]:
        """Transform function arguments with optional DISTINCT modifier.

        Args:
            args: Mix of "DISTINCT" keyword string and function_arg_list.

        Returns:
            Dict with "distinct" boolean flag and "arguments" list.

        """
        # After the grammar change, distinct_keyword produces the string "DISTINCT"
        # as a named rule result.  Also accept bare Token/str for backwards compat.
        distinct = any(
            (isinstance(a, str) and a.upper() == "DISTINCT") for a in args
        )
        arg_list = next((a for a in args if isinstance(a, list)), [])
        return {"distinct": distinct, "arguments": arg_list}

    def function_arg_list(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated function argument expressions into a list.

        Args:
            args: Individual expression nodes for each argument.

        Returns:
            List of argument expressions (empty list if no arguments).

        """
        return list(args)

    def function_name(self, args: list[Any]) -> str | dict[str, str]:
        """Transform function name with optional namespace qualification.

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

    def namespace_name(self, args: list[Any]) -> str:
        """Transform namespace path into a dot-separated string.

        Args:
            args: List of identifier tokens forming the namespace path.

        Returns:
            Dot-separated namespace string with backticks removed.

        """
        return ".".join(str(a).strip("`") for a in args)

    def function_simple_name(self, args: list[Any]) -> str:
        """Extract the unqualified function name identifier.

        Args:
            args: Single identifier token for the function name.

        Returns:
            Function name as a string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # Case expression
    # ========================================================================

    def case_expression(self, args: list[Any]) -> Any | None:
        """Transform CASE expression (simple or searched form).

        Pass-through because the grammar has already dispatched to the
        appropriate specific handler (simple_case or searched_case).

        Args:
            args: Single node (SimpleCase or SearchedCase) from the specific rule.

        Returns:
            The SimpleCase or SearchedCase node unchanged.

        """
        return args[0] if args else None

    def simple_case(self, args: list[Any]) -> dict[str, Any]:
        """Transform simple CASE expression that matches an operand against values.

        Simple CASE syntax: CASE expression WHEN value1 THEN result1 [WHEN ...] [ELSE default] END

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

    def searched_case(self, args: list[Any]) -> dict[str, Any]:
        """Transform searched CASE expression that evaluates boolean conditions.

        Searched CASE syntax: CASE WHEN condition1 THEN result1 [WHEN ...] [ELSE default] END

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

    def simple_when(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHEN clause in a simple CASE expression.

        Args:
            args: [when_operands_list, result_expression].

        Returns:
            Dict with type "SimpleWhen" containing operands list and result expression.

        """
        operands = args[0] if args else []
        result = args[1] if len(args) > 1 else None
        return {"type": "SimpleWhen", "operands": operands, "result": result}

    def searched_when(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHEN clause in a searched CASE expression.

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

    def when_operands(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated operands in a simple CASE WHEN clause.

        Args:
            args: Individual expression nodes for each value to test.

        Returns:
            List of operand expressions.

        """
        return list(args)

    def else_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform the ELSE clause in a CASE expression.

        Args:
            args: Single expression for the default value.

        Returns:
            Dict with type "Else" containing the default value expression.

        """
        return {"type": "Else", "value": args[0] if args else None}

    # ========================================================================
    # List comprehension
    # ========================================================================

    def list_comprehension(self, args: list[Any]) -> dict[str, Any]:
        """Transform list comprehension expression for filtering and mapping lists.

        List comprehension syntax: [variable IN list WHERE condition | projection]

        Args:
            args: [variable, source_list, optional_filter, optional_projection].

        Returns:
            Dict with type "ListComprehension" containing all components.

        """
        variable = args[0] if args else None
        source = args[1] if len(args) > 1 else None

        # list_filter and list_projection are both optional and both produce
        # expression dicts.  list_filter wraps its result as {"_lc_filter": e}
        # and list_projection as {"_lc_projection": e} so we can tell them
        # apart even when only one is present.
        filter_expr = None
        projection = None
        for arg in args[2:]:
            if isinstance(arg, dict) and "_lc_filter" in arg:
                filter_expr = arg["_lc_filter"]
            elif isinstance(arg, dict) and "_lc_projection" in arg:
                projection = arg["_lc_projection"]

        return {
            "type": "ListComprehension",
            "variable": variable,
            "in": source,
            "where": filter_expr,
            "projection": projection,
        }

    def list_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable name from a list comprehension.

        Args:
            args: Variable name string from variable_name rule.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    def list_filter(self, args: list[Any]) -> Any | None:
        """Extract the WHERE filter expression from a list comprehension.

        Wraps the expression in a sentinel dict so that
        :meth:`list_comprehension` can distinguish a bare filter from a bare
        projection when only one of the two optional clauses is present.

        Args:
            args: Boolean filter expression.

        Returns:
            ``{"_lc_filter": expr}`` sentinel dict.

        """
        return {"_lc_filter": args[0]} if args else None

    def list_projection(self, args: list[Any]) -> Any | None:
        """Extract the projection expression from a list comprehension.

        Wraps the expression in a sentinel dict so that
        :meth:`list_comprehension` can distinguish a bare projection from a
        bare filter when only one of the two optional clauses is present.

        Args:
            args: Projection expression to transform each element.

        Returns:
            ``{"_lc_projection": expr}`` sentinel dict.

        """
        return {"_lc_projection": args[0]} if args else None

    # ========================================================================
    # Pattern comprehension
    # ========================================================================

    def pattern_comprehension(self, args: list[Any]) -> dict[str, Any]:
        """Transform pattern comprehension for collecting results from pattern matching.

        Pattern comprehension syntax: [path_var = pattern WHERE condition | projection]

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
                elif (
                    arg.get("type") == "PatternFilter" and filter_expr is None
                ):
                    # Tagged sentinel from pattern_filter — extract inner expression
                    filter_expr = arg.get("expression")
                elif projection is None:
                    projection = arg

        return {
            "type": "PatternComprehension",
            "variable": variable,
            "pattern": pattern,
            "where": filter_expr,
            "projection": projection,
        }

    def pattern_comp_variable(self, args: list[Any]) -> Any | None:
        """Extract the optional path variable from a pattern comprehension.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    def pattern_filter(self, args: list[Any]) -> Any | None:
        """Extract the WHERE filter from a pattern comprehension.

        Wraps the expression in a ``{"type": "PatternFilter", "expression": ...}``
        sentinel so that ``pattern_comprehension`` can distinguish the WHERE
        expression from the projection expression when both are dicts.

        Args:
            args: Boolean filter expression.

        Returns:
            Tagged dict ``{"type": "PatternFilter", "expression": expr}`` or
            ``None`` if no args.

        """
        if not args:
            return None
        return {"type": "PatternFilter", "expression": args[0]}

    def pattern_projection(self, args: list[Any]) -> Any | None:
        """Extract the projection expression from a pattern comprehension.

        Args:
            args: Projection expression.

        Returns:
            Projection expression unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Reduce expression
    # ========================================================================

    def reduce_expression(self, args: list[Any]) -> dict[str, Any]:
        """Transform REDUCE expression for list aggregation with accumulator.

        REDUCE syntax: REDUCE(accumulator = initial, variable IN list | step_expression)

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

    def reduce_accumulator(self, args: list[Any]) -> dict[str, Any]:
        """Transform the accumulator declaration in a REDUCE expression.

        Accumulator syntax: variable_name = initial_expression

        Args:
            args: [variable_name, initialization_expression].

        Returns:
            Dict with variable name and init expression.

        """
        variable = args[0] if args else None
        init = args[1] if len(args) > 1 else None
        return {"variable": variable, "init": init}

    def reduce_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable from a REDUCE expression.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Quantifier expressions
    # ========================================================================

    def quantifier_expression(self, args: list[Any]) -> dict[str, Any]:
        """Transform quantifier expressions (ALL, ANY, SINGLE, NONE) for predicate testing.

        Quantifier syntax: QUANTIFIER(variable IN list WHERE predicate)

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

    def quantifier(self, args: list[Any]) -> str:
        """Extract and normalize the quantifier keyword (ALL, ANY, SINGLE, NONE).

        Args:
            args: Quantifier keyword token.

        Returns:
            Uppercased quantifier string: "ALL", "ANY", "SINGLE", or "NONE".

        """
        if not args:
            return "ALL"
        return str(args[0]).upper()

    def quantifier_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable from a quantifier expression.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Map projection
    # ========================================================================

    def map_projection(self, args: list[Any]) -> dict[str, Any]:
        """Transform map projection for selecting/transforming object properties.

        Map projection syntax: variable { property1, .property2, property3: expression, ...}

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

    def map_elements(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated map projection elements into a list.

        Args:
            args: Individual map_element nodes.

        Returns:
            List of map element specifications (empty list if no elements).

        """
        return list(args) if args else []

    def map_element(
        self,
        args: list[Any],
    ) -> dict[str, Any] | Any | None:
        """Transform a single map projection element.

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
