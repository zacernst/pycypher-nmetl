"""
PropertyChange data model and evaluation for SET clause operations.

This module provides data structures and evaluation logic for representing
and processing property modifications in SET clauses.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

import pandas as pd
from pycypher.ast_models import Variable, Literal, PropertyLookup, Arithmetic, Expression


class PropertyChangeType(Enum):
    """Types of property changes supported in SET clauses."""

    SET_PROPERTY = "set_property"          # SET n.prop = value
    SET_ALL_PROPERTIES = "set_all"         # SET n = {map}
    ADD_ALL_PROPERTIES = "add_all"         # SET n += {map}
    SET_LABELS = "set_labels"              # SET n:Label


@dataclass
class PropertyChange:
    """Represents a single property modification operation.

    This class captures all the information needed to apply a property change
    to a DataFrame row during SET clause execution.
    """

    variable_type: str                                    # Entity type (e.g., "Person")
    variable_column: str                                  # ID column name
    change_type: PropertyChangeType
    property_name: Optional[str] = None                   # For SET_PROPERTY
    value_expression: Optional[Expression] = None         # For SET_PROPERTY
    properties_map: Optional[Dict[str, Expression]] = None  # For SET_ALL/ADD_ALL
    labels: Optional[List[str]] = None                    # For SET_LABELS


class PropertyModificationEvaluator:
    """Evaluates expressions in the context of property modifications.

    This class handles the evaluation of expressions within SET clauses,
    including property lookups, arithmetic, and complex expressions.
    """

    def evaluate_property_value(
        self,
        expr: Expression,
        df: pd.DataFrame,
        row_index: int
    ) -> Any:
        """Evaluate an expression for a specific DataFrame row.

        Args:
            expr: The AST expression to evaluate
            df: DataFrame containing the current data
            row_index: Index of the row to evaluate for

        Returns:
            The evaluated value

        Raises:
            KeyError: If referenced property doesn't exist
            IndexError: If row_index is invalid
        """
        # Check row bounds early
        if row_index >= len(df) or row_index < 0:
            raise IndexError(f"Row index {row_index} out of range")

        if isinstance(expr, Literal):
            return expr.value

        elif isinstance(expr, PropertyLookup):
            property_name = expr.property

            # Handle prefixed column names (e.g., "Person__name" instead of "name")
            if property_name not in df.columns:
                # Look for prefixed versions
                prefixed_cols = [col for col in df.columns if col.endswith(f"__{property_name}")]
                if prefixed_cols:
                    property_name = prefixed_cols[0]
                else:
                    raise KeyError(f"Property '{property_name}' not found in DataFrame")

            if row_index >= len(df):
                raise IndexError(f"Row index {row_index} out of range")
            value = df.iloc[row_index][property_name]
            # Convert numpy types to native Python types
            if hasattr(value, 'item'):  # numpy scalar
                return value.item()
            return value

        elif isinstance(expr, Arithmetic):
            left_val = self.evaluate_property_value(expr.left, df, row_index)
            right_val = self.evaluate_property_value(expr.right, df, row_index)

            if expr.operator == "+":
                return left_val + right_val
            elif expr.operator == "-":
                return left_val - right_val
            elif expr.operator == "*":
                return left_val * right_val
            elif expr.operator == "/":
                return left_val / right_val
            else:
                raise ValueError(f"Unsupported arithmetic operator: {expr.operator}")

        else:
            raise ValueError(f"Unsupported expression type: {type(expr)}")

    def evaluate_properties_map(
        self,
        properties_map: Dict[str, Expression],
        df: pd.DataFrame,
        row_index: int
    ) -> Dict[str, Any]:
        """Evaluate a properties map for SET n = {map} operations.

        Args:
            properties_map: Dictionary mapping property names to expressions
            df: DataFrame containing the current data
            row_index: Index of the row to evaluate for

        Returns:
            Dictionary mapping property names to evaluated values
        """
        result = {}
        for prop_name, expr in properties_map.items():
            result[prop_name] = self.evaluate_property_value(expr, df, row_index)
        return result