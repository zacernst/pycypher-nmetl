"""Expression evaluator for the pycypher relational algebra pipeline.

Provides ``ExpressionEvaluator``, a single class that can evaluate AST
expression nodes against both full DataFrames (column-level operations) and
individual rows (``pd.Series``) for per-row computations such as SET clause
processing.

Usage example (row-mode)::

    evaluator = ExpressionEvaluator(context=ctx, relation=my_relation)
    row = df.iloc[0]
    result = evaluator.evaluate_scalar(expr, row)
"""

from __future__ import annotations

import operator as op
from typing import Any

import pandas as pd

from pycypher.ast_models import (
    Arithmetic,
    Expression,
    IntegerLiteral,
    Literal,
    PropertyLookup,
    StringLiteral,
)

# Mapping from Cypher arithmetic operator strings to Python callables.
_ARITHMETIC_OPS: dict[str, Any] = {
    "+": op.add,
    "-": op.sub,
    "*": op.mul,
    "/": op.truediv,
    "%": op.mod,
    "^": op.pow,
}


class ExpressionEvaluator:
    """Evaluates AST expression nodes in the context of a relational pipeline.

    Two evaluation modes are supported:

    * **Column mode** (future): operates over entire DataFrame columns for
      vectorised WITH/RETURN projection.
    * **Row mode** (``evaluate_scalar``): evaluates against a single
      ``pd.Series`` row, used for SET clause processing where each row may
      produce a different value.

    Args:
        context: The ``Context`` carrying entity/relationship table mappings.
        relation: The upstream ``Relation`` providing ``variable_map`` and
            ``variable_type_map`` metadata.

    """

    def __init__(self, context: Any, relation: Any) -> None:
        self.context = context
        self.relation = relation

    def evaluate_scalar(self, expr: Expression, row: pd.Series) -> Any:
        """Evaluate an AST expression against a single DataFrame row.

        Used for SET clause processing where per-row evaluation is needed.

        Supported expression types:
        - ``Literal`` (and all subclasses): returns ``expr.value`` directly.
        - ``PropertyLookup``: looks up the property in the row, checking
          both the bare property name and any entity-type-prefixed variants
          (e.g. ``Person__age`` when the property is ``age``).
        - ``Arithmetic``: recursively evaluates left and right operands, then
          applies the operator.

        Args:
            expr: An AST expression node to evaluate.
            row: A ``pd.Series`` representing a single DataFrame row, keyed
                by column name.

        Returns:
            The scalar result of evaluating the expression.

        Raises:
            KeyError: If a ``PropertyLookup`` refers to a property that
                cannot be found in the row (neither bare nor prefixed).
            NotImplementedError: If the expression type is not yet supported.

        """
        match expr:
            case Literal():
                return expr.value

            case PropertyLookup():
                prop: str | None = expr.property
                if prop is None:
                    raise KeyError("PropertyLookup has no property name set.")

                # 1. Try bare name first (row already has plain column names)
                if prop in row.index:
                    return row[prop]

                # 2. Try all entity-type-prefixed variants present in the row.
                #    Pattern: "<EntityType>__<prop>" where EntityType may contain
                #    underscores but the separator is always "__".
                for col in row.index:
                    if isinstance(col, str) and col.endswith(f"__{prop}"):
                        return row[col]

                raise KeyError(
                    f"Property '{prop}' not found in row. "
                    f"Available columns: {list(row.index)}"
                )

            case Arithmetic():
                if expr.left is None or expr.right is None:
                    raise ValueError(
                        "Arithmetic expression has None operand(s); "
                        "both left and right must be non-None."
                    )
                left_val: Any = self.evaluate_scalar(expr.left, row)
                right_val: Any = self.evaluate_scalar(expr.right, row)
                operator_str: str = expr.operator
                if operator_str not in _ARITHMETIC_OPS:
                    raise NotImplementedError(
                        f"Arithmetic operator '{operator_str}' is not supported."
                    )
                return _ARITHMETIC_OPS[operator_str](left_val, right_val)

            case _:
                raise NotImplementedError(
                    f"evaluate_scalar does not support expression type "
                    f"'{type(expr).__name__}'. "
                    "Supported types: Literal, PropertyLookup, Arithmetic."
                )
