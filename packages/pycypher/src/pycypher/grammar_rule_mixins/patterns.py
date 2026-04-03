"""Mixin for graph pattern matching grammar rules.

Handles pattern/path construction, node patterns, relationship patterns,
label expressions, property maps, and path length ranges.
"""

from __future__ import annotations

from typing import Any


class PatternRulesMixin:
    """Grammar rule methods for graph pattern matching constructs.

    Handles:
    - Pattern and path pattern construction
    - Pattern elements and shortest path
    - Node patterns (labels, properties, inline WHERE)
    - Relationship patterns (direction, types, variable-length paths)
    - Label expressions (AND, OR, NOT on labels)
    - Property maps and property key-value pairs
    - Path length ranges for variable-length relationships
    """

    def pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a graph pattern (one or more paths).

        Args:
            args: List of path_pattern nodes.

        Returns:
            Dict with type "Pattern" containing list of paths.

        """
        return {"type": "Pattern", "paths": list(args)}

    def path_pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single path pattern with optional variable assignment.

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

    def pattern_element(self, args: list[Any]) -> dict[str, Any]:
        """Transform a pattern element (sequence of nodes and relationships).

        Args:
            args: Alternating node_pattern and relationship_pattern nodes.

        Returns:
            Dict with type "PatternElement" containing ordered list of parts.

        """
        return {"type": "PatternElement", "parts": list(args)}

    def shortest_path(self, args: list[Any]) -> dict[str, Any]:
        """Transform SHORTESTPATH or ALLSHORTESTPATHS function.

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

    def node_pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a node pattern (node in parentheses).

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

    def node_pattern_filler(self, args: list[Any]) -> dict[str, Any]:
        """Extract components from inside node parentheses.

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

    def node_labels(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform node label expressions.

        Args:
            args: List of label_expression nodes.

        Returns:
            Dict with "labels" key containing list of label expressions.

        """
        return {"labels": list(args)}

    def label_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform a single label expression.

        Args:
            args: Label term(s) from the expression.

        Returns:
            Single label term or dict with type "LabelExpression" for complex cases.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelExpression", "parts": list(args)}

    def label_term(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform label OR expressions (e.g., ``Person|Employee``).

        Args:
            args: Label factor nodes separated by ``|``.

        Returns:
            Single factor or dict with type "LabelOr" for multiple factors.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelOr", "terms": list(args)}

    def label_factor(self, args: list[Any]) -> Any | None:
        """Pass through label factors (possibly negated with !).

        Args:
            args: Single label_primary node.

        Returns:
            The label primary unchanged.

        """
        return args[0] if args else None

    def label_primary(self, args: list[Any]) -> Any | None:
        """Pass through primary label expressions.

        Args:
            args: Label name or grouped expression.

        Returns:
            The label value unchanged.

        """
        return args[0] if args else None

    def label_name(self, args: list[Any]) -> str:
        """Extract label name identifier.

        Args:
            args: IDENTIFIER token possibly with leading :.

        Returns:
            Label name string with : and backticks removed.

        """
        return str(args[0]).lstrip(":").strip("`")

    def node_properties(self, args: list[Any]) -> Any | None:
        """Pass through node properties or WHERE clause.

        Args:
            args: Properties dict or WHERE clause.

        Returns:
            The properties/where node unchanged.

        """
        return args[0] if args else None

    def node_where(self, args: list[Any]) -> dict[str, Any]:
        """Transform inline WHERE clause within node pattern.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.

        """
        return {"where": args[0] if args else None}

    # ========================================================================
    # Relationship pattern
    # ========================================================================

    def relationship_pattern(self, args: list[Any]) -> Any | None:
        """Pass through relationship patterns.

        Args:
            args: Single relationship node from directional rules.

        Returns:
            The relationship pattern unchanged.

        """
        return args[0] if args else None

    def full_rel_left(self, args: list[Any]) -> dict[str, Any]:
        """Transform left-pointing relationship (<--).

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "left", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "left", **detail}

    def full_rel_right(self, args: list[Any]) -> dict[str, Any]:
        """Transform right-pointing relationship (-->).

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "right", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "right", **detail}

    def full_rel_both(self, args: list[Any]) -> dict[str, Any]:
        """Transform bidirectional relationship (<-->).

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "both", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "both", **detail}

    def full_rel_any(self, args: list[Any]) -> dict[str, Any]:
        """Transform undirected relationship (---).

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "any", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "any", **detail}

    def rel_detail(self, args: list[Any]) -> dict[str, Any]:
        """Extract details from inside relationship brackets [...].

        Args:
            args: rel_filler dict with relationship components.

        Returns:
            The filler dict unchanged, or empty dict if no details.

        """
        return args[0] if args else {}

    def rel_filler(self, args: list[Any]) -> dict[str, Any]:
        """Extract components from inside relationship brackets.

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

    def rel_types(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform relationship type constraints.

        Args:
            args: List of relationship type names.

        Returns:
            Dict with "types" key containing list of type names.

        """
        return {"types": list(args)}

    def rel_type(self, args: list[Any]) -> str:
        """Extract relationship type name.

        Args:
            args: IDENTIFIER token.

        Returns:
            Type name string with backticks removed.

        """
        return str(args[0]).strip("`")

    def rel_properties(self, args: list[Any]) -> dict[str, Any]:
        """Transform relationship property constraints.

        Args:
            args: Properties map.

        Returns:
            Dict with "properties" key containing the property map.

        """
        return {"properties": args[0] if args else None}

    def rel_where(self, args: list[Any]) -> dict[str, Any]:
        """Transform inline WHERE clause within relationship pattern.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.

        """
        return {"where": args[0] if args else None}

    def path_length(self, args: list[Any]) -> dict[str, Any]:
        r"""Transform variable-length path specification (\*).

        Args:
            args: Optional path_length_range specification.

        Returns:
            Dict with normalized PathLength node for downstream conversion.

        """
        range_spec = args[0] if args else None

        length_node: dict[str, Any] = {
            "type": "PathLength",
            "min": None,
            "max": None,
            "unbounded": False,
        }

        if isinstance(range_spec, dict):
            if "fixed" in range_spec:
                value = range_spec.get("fixed")
                length_node["min"] = value
                length_node["max"] = value
            else:
                if "min" in range_spec:
                    length_node["min"] = range_spec.get("min")
                if "max" in range_spec:
                    length_node["max"] = range_spec.get("max")
                if range_spec.get("unbounded"):
                    length_node["unbounded"] = True
        elif isinstance(range_spec, int):
            length_node["min"] = range_spec
            length_node["max"] = range_spec
        elif range_spec is None:
            length_node["unbounded"] = True
        else:
            # Fallback: attempt to coerce to int for fixed length strings/tokens
            try:
                value = int(str(range_spec))
            except (TypeError, ValueError):
                value = None
            length_node["min"] = value
            length_node["max"] = value

        # When unbounded and no explicit lower bound, default Cypher lower bound is 1
        if length_node["unbounded"] and length_node["min"] is None:
            length_node["min"] = 1

        return {"length": length_node}

    def path_length_range(
        self,
        args: list[Any],
    ) -> int | dict[str, int | None]:
        """Transform path length range specification.

        Args:
            args: One or two integer arguments for range bounds.

        Returns:
            Dict with "fixed", "min"/"max", or "unbounded" keys.

        """
        if len(args) == 1:
            return {"fixed": int(str(args[0]))}
        if len(args) == 2:
            return {
                "min": int(str(args[0])) if args[0] else None,
                "max": int(str(args[1])) if args[1] else None,
            }
        return {"unbounded": True}

    # ========================================================================
    # Properties
    # ========================================================================

    def properties(self, args: list[Any]) -> dict[str, Any]:
        """Extract property map from curly braces {...}.

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

    def property_list(self, args: list[Any]) -> dict[str, dict[str, Any]]:
        """Transform comma-separated property key-value pairs.

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

    def property_key_value(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single property assignment (key: value).

        Args:
            args: [property_name, expression]

        Returns:
            Dict with "key" and "value" for the property assignment.

        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    def property_name(self, args: list[Any]) -> str:
        """Extract property name identifier.

        Args:
            args: IDENTIFIER token.

        Returns:
            Property name string with backticks removed.

        """
        return str(args[0]).strip("`")
