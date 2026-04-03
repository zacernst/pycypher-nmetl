"""Mixin for literal value grammar rules.

Handles number, string, boolean, null, list, map literals,
parameter references, and variable name normalization.
"""

from __future__ import annotations

from typing import Any


class LiteralRulesMixin:
    """Grammar rule methods for literal values, parameters, and variable names.

    Handles:
    - Number literals (signed, unsigned, hex, octal, float, Inf, NaN)
    - String literals (escape sequences)
    - Boolean literals (TRUE / FALSE)
    - NULL literal
    - List literals and list elements
    - Map literals, entries, and individual entries
    - Parameter references ($name, $0)
    - Variable name normalization (backtick stripping)
    """

    # ========================================================================
    # Number literals
    # ========================================================================

    def number_literal(self, args: list[Any]) -> int | float:
        """Transform number literals (integers and floats) into Python values.

        Pass-through — actual conversion happens in signed_number / unsigned_number.

        Args:
            args: Single numeric value from signed_number or unsigned_number.

        Returns:
            Python int or float value, or 0 as fallback.

        """
        return args[0] if args else 0

    def signed_number(self, args: list[Any]) -> int | float | str:
        """Transform signed number literals into Python int or float values.

        Supports integers (-42, +100, -0x2A), floats (-3.14, +2.5e10),
        special values (-INF, +INFINITY, -NAN), and underscore separators.

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

    def unsigned_number(self, args: list[Any]) -> int | float | str:
        """Transform unsigned number literals into Python int or float values.

        Supports decimal, hex (0x), octal (0o), float, special values,
        and underscore separators.

        Args:
            args: Single unsigned number token.

        Returns:
            Python int, float, or special float value (inf/nan).

        """
        s = str(args[0])
        try:
            # Check hex/octal prefixes before float heuristics, since hex
            # digits (a-f) overlap with float suffixes (f, d, e).
            if s.startswith(("0x", "0X")):
                return int(s.replace("_", ""), 16)
            if s.startswith(("0o", "0O")):
                return int(s[2:].replace("_", ""), 8)
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
                return float("inf")
            if "nan" in s.lower():
                return float("nan")
            return s

    # ========================================================================
    # String / Boolean / Null literals
    # ========================================================================

    def string_literal(self, args: list[Any]) -> dict[str, Any]:
        r"""Transform string literals into structured AST nodes.

        Handles quote removal and escape sequences (\\n, \\t, \\r, \\\\, \\', \\").

        Args:
            args: Single string token with quotes.

        Returns:
            Dict with ``type="StringLiteral"`` and the processed string value.

        """
        s = str(args[0])
        if s.startswith(("'", '"')):
            s = s[1:-1]
        s = s.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
        s = s.replace("\\\\", "\\").replace("\\'", "'").replace('\\"', '"')
        return {"type": "StringLiteral", "value": s}

    def true(self, args: list[Any]) -> bool:
        """Transform TRUE keyword into Python True."""
        return True

    def false(self, args: list[Any]) -> bool:
        """Transform FALSE keyword into Python False."""
        return False

    def null_literal(self, args: list[Any]) -> dict[str, Any]:
        """Transform NULL keyword into a NullLiteral AST dict.

        Returns a typed dict rather than Python None to distinguish explicit
        null expressions from absent/optional AST fields.

        """
        return {"type": "NullLiteral"}

    # ========================================================================
    # List literals
    # ========================================================================

    def list_literal(self, args: list[Any]) -> list[Any]:
        """Transform list literal syntax [...] into Python list.

        Args:
            args: list_elements node containing list of element expressions.

        Returns:
            Python list of element values (empty list if no elements).

        """
        return next((a for a in args if isinstance(a, list)), [])

    def list_elements(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated list elements into Python list.

        Args:
            args: Individual element expression nodes.

        Returns:
            Python list of element expressions.

        """
        return list(args) if args else []

    # ========================================================================
    # Map literals
    # ========================================================================

    def map_literal(self, args: list[Any]) -> dict[str, Any]:
        """Transform map literal syntax {...} into MapLiteral AST dict.

        Args:
            args: map_entries node containing dict of key-value pairs.

        Returns:
            Dict with ``type="MapLiteral"`` and ``"value"`` key.

        """
        entries = next(
            (a for a in args if isinstance(a, dict) and "entries" in str(a)),
            {"entries": {}},
        )
        return {"type": "MapLiteral", "value": entries.get("entries", {})}

    def map_entries(self, args: list[Any]) -> dict[str, dict[str, Any]]:
        """Transform comma-separated map entries into an entries wrapper dict.

        Args:
            args: Individual map_entry nodes with key and value.

        Returns:
            Dict with ``"entries"`` key containing the collected key-value map.

        """
        result: dict[str, Any] = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"entries": result}

    def map_entry(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single map entry (key: value) into a key-value dict.

        Args:
            args: [property_name, value_expression].

        Returns:
            Dict with ``"key"`` and ``"value"`` keys.

        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    # ========================================================================
    # Parameters
    # ========================================================================

    def parameter(self, args: list[Any]) -> dict[str, Any]:
        """Transform parameter reference ($param) into a Parameter AST node.

        Args:
            args: Parameter name (string identifier or integer index).

        Returns:
            Dict with ``type="Parameter"`` and the parameter name.

        """
        name = args[0] if args else None
        return {"type": "Parameter", "name": name}

    def parameter_name(self, args: list[Any]) -> int | str:
        """Extract and normalize parameter name or index.

        Numeric indices are parsed as integers; identifiers have backticks stripped.

        Args:
            args: Parameter name token (identifier or number).

        Returns:
            Integer for numeric indices, string for named parameters.

        """
        s = str(args[0])
        try:
            return int(s)
        except ValueError:
            return s.strip("`")

    # ========================================================================
    # Variable name
    # ========================================================================

    def variable_name(self, args: list[Any]) -> str:
        """Extract and normalize variable identifier names.

        Strips backticks from escaped identifiers and converts Token to str.

        Args:
            args: Single identifier token (may have backticks).

        Returns:
            Variable name as a string with backticks removed.

        """
        return str(args[0]).strip("`")
