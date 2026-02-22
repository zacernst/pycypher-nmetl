"""Expression evaluator for converting AST expressions to pandas operations.

This module provides functionality to evaluate Cypher AST expressions against
pandas DataFrames, enabling property access, function calls, and computations
during WITH and RETURN clause processing.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from shared.logger import LOGGER

from pycypher.ast_models import (
    Expression,
    PropertyLookup,
    Variable,
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
    BooleanLiteral,
    NullLiteral,
    ListLiteral,
)
from pycypher.relational_models import (
    Context,
    Relation,
    ID_COLUMN,
    ColumnName,
)


class ExpressionEvaluator:
    """Evaluates AST expressions against pandas DataFrames.
    
    Handles:
    - PropertyLookup (p.name) - requires joining with entity tables
    - Variable references (p)
    - Literal values
    
    Future extensions will support:
    - Arithmetic operations
    - Function calls
    - Comparisons
    """
    
    def __init__(self, context: Context, relation: Relation):
        """Initialize evaluator.
        
        Args:
            context: Context with entity and relationship mappings
            relation: Source relation containing current data
        """
        self.context = context
        self.relation = relation
        
    def evaluate(
        self, 
        expression: Expression, 
        df: pd.DataFrame
    ) -> tuple[pd.Series, str]:
        """Evaluate expression and return result series with source column name.
        
        Args:
            expression: AST expression to evaluate
            df: DataFrame to evaluate expression against
            
        Returns:
            Tuple of (result Series, source column name)
            
        Raises:
            NotImplementedError: For unsupported expression types
        """
        match expression:
            case PropertyLookup(expression=var_expr, property=prop_name):
                # Property access: p.name
                return self._evaluate_property_lookup(var_expr, prop_name, df)
                
            case Variable(name=var_name):
                # Variable reference: p
                return self._evaluate_variable(var_name, df)
                
            case IntegerLiteral(value=val) | FloatLiteral(value=val) | \
                 StringLiteral(value=val) | BooleanLiteral(value=val):
                # Constant literal
                return self._evaluate_literal(val, df)
                
            case NullLiteral():
                # NULL literal
                return pd.Series([None] * len(df), dtype=object), "null"
                
            case ListLiteral(value=val):
                # List literal - return as constant series
                return pd.Series([val] * len(df)), "list"
                
            case _:
                raise NotImplementedError(
                    f"Expression type {type(expression).__name__} not yet supported "
                    f"in Phase 1. Only PropertyLookup, Variable, and Literal are supported."
                )
    
    def _evaluate_property_lookup(
        self, 
        var_expr: Expression, 
        prop_name: str,
        df: pd.DataFrame
    ) -> tuple[pd.Series, str]:
        """Evaluate property access like p.name.
        
        Strategy:
        1. Determine which variable is being accessed
        2. Get the entity type for that variable
        3. Join with entity table to fetch the property
        4. Return the property column
        
        Args:
            var_expr: Variable expression (e.g., Variable("p"))
            prop_name: Property name (e.g., "name")
            df: Current DataFrame
            
        Returns:
            Tuple of (property values as Series, column name)
        """
        # Extract variable from expression
        if isinstance(var_expr, Variable):
            variable = var_expr
        else:
            raise NotImplementedError(
                f"PropertyLookup on {type(var_expr).__name__} not supported yet"
            )
        
        # Find variable in relation's variable_map
        if variable not in self.relation.variable_map:
            raise ValueError(
                f"Variable {variable.name} not found in current relation. "
                f"Available variables: {[v.name for v in self.relation.variable_map.keys()]}"
            )
        
        # Get entity type for this variable
        if variable not in self.relation.variable_type_map:
            raise ValueError(
                f"Variable {variable.name} has no type information. "
                "Cannot access properties."
            )
        
        entity_type = self.relation.variable_type_map[variable]
        
        # Get the ID column for this variable in the current DataFrame
        id_column = self.relation.variable_map[variable]
        
        if id_column not in df.columns:
            raise ValueError(
                f"Column {id_column} not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )
        
        # Get entity table from context
        if entity_type not in self.context.entity_mapping.mapping:
            raise ValueError(
                f"Entity type {entity_type} not found in context. "
                f"Available types: {list(self.context.entity_mapping.mapping.keys())}"
            )
        
        entity_table = self.context.entity_mapping[entity_type]
        
        # Convert entity table to DataFrame
        entity_df = entity_table.to_pandas(context=self.context)
        
        # Check if property exists in entity table
        prefixed_prop_name = f"{entity_type}__{prop_name}"
        if prefixed_prop_name not in entity_df.columns:
            raise ValueError(
                f"Property {prop_name} not found in entity {entity_type}. "
                f"Available properties: {[col.replace(f'{entity_type}__', '') for col in entity_df.columns if col.startswith(f'{entity_type}__')]}"
            )
        
        # Get the entity ID column name
        entity_id_column = f"{entity_type}__{ID_COLUMN}"
        
        # Perform join to fetch property values
        # Use a temporary column name to avoid conflicts
        temp_id_col = f"__temp_id_{entity_type}__"
        df_with_temp = df.copy()
        df_with_temp[temp_id_col] = df[id_column]
        
        # Join on ID
        joined_df = df_with_temp.merge(
            entity_df[[entity_id_column, prefixed_prop_name]],
            left_on=temp_id_col,
            right_on=entity_id_column,
            how="left"
        )
        
        # Extract property column
        property_series = joined_df[prefixed_prop_name]
        
        LOGGER.debug(
            msg=f"Evaluated property access {variable.name}.{prop_name}: "
            f"found {len(property_series)} values"
        )
        
        return property_series, prefixed_prop_name
    
    def _evaluate_variable(
        self, 
        var_name: str, 
        df: pd.DataFrame
    ) -> tuple[pd.Series, str]:
        """Evaluate variable reference.
        
        Returns the ID column for the variable.
        
        Args:
            var_name: Variable name
            df: Current DataFrame
            
        Returns:
            Tuple of (ID column as Series, column name)
        """
        # Find variable in relation
        variable = None
        for var in self.relation.variable_map.keys():
            if var.name == var_name:
                variable = var
                break
        
        if variable is None:
            raise ValueError(
                f"Variable {var_name} not found in current relation. "
                f"Available variables: {[v.name for v in self.relation.variable_map.keys()]}"
            )
        
        column_name = self.relation.variable_map[variable]
        
        if column_name not in df.columns:
            raise ValueError(
                f"Column {column_name} not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )
        
        return df[column_name], column_name
    
    def _evaluate_literal(
        self, 
        value: Any, 
        df: pd.DataFrame
    ) -> tuple[pd.Series, str]:
        """Evaluate literal value.
        
        Returns a constant Series with the literal value.
        
        Args:
            value: Literal value
            df: Current DataFrame (used for determining series length)
            
        Returns:
            Tuple of (constant Series, column name "literal")
        """
        return pd.Series([value] * len(df)), "literal"
    
    def evaluate_aggregation(
        self,
        agg_expression: Expression,
        df: pd.DataFrame
    ) -> Any:
        """Evaluate an aggregation function against a DataFrame.
        
        Supports:
        - collect(expr): Returns list of all values
        - count(expr): Returns count of non-null values
        - count(*): Returns total row count
        - sum(expr): Returns sum of numeric values
        - avg(expr): Returns average of numeric values
        - min(expr): Returns minimum value
        - max(expr): Returns maximum value
        
        Args:
            agg_expression: Aggregation function expression (FunctionInvocation or CountStar)
            df: DataFrame to aggregate over
            
        Returns:
            Aggregated value (scalar or list for collect)
            
        Raises:
            ValueError: For unsupported aggregation functions
            NotImplementedError: For complex aggregation scenarios
        """
        from pycypher.ast_models import FunctionInvocation, CountStar
        
        # Handle COUNT(*)
        if isinstance(agg_expression, CountStar):
            return len(df)
        
        # Handle function invocations
        if not isinstance(agg_expression, FunctionInvocation):
            raise ValueError(
                f"Expected FunctionInvocation or CountStar for aggregation, "
                f"got {type(agg_expression).__name__}"
            )
        
        # Get function name
        func_name = agg_expression.name
        if isinstance(func_name, dict):
            # Namespaced function (e.g., {namespace: "math", name: "sum"})
            func_name = func_name.get("name", "")
        
        func_name_lower = func_name.lower()
        
        # Get arguments
        arguments = agg_expression.arguments or {}
        
        # Extract the expression to aggregate
        # Arguments can be positional (list) or named (dict)
        arg_expr = None
        
        if isinstance(arguments, dict):
            # Named arguments - look for 'expression', 'args', or 'arguments'
            if 'expression' in arguments:
                arg_expr = arguments['expression']
            elif 'args' in arguments and arguments['args']:
                arg_expr = arguments['args'][0]
            elif 'arguments' in arguments and arguments['arguments']:
                # Parser produces {'distinct': False, 'arguments': [...]}
                arg_expr = arguments['arguments'][0]
            elif len(arguments) == 1:
                # Single argument, use its value
                arg_expr = list(arguments.values())[0]
        elif isinstance(arguments, list) and arguments:
            # Positional arguments
            arg_expr = arguments[0]
        
        if arg_expr is None and func_name_lower != 'count':
            raise ValueError(
                f"Aggregation function {func_name} requires an argument expression"
            )
        
        # For count() without args, treat as count(*)
        if func_name_lower == 'count' and arg_expr is None:
            return len(df)
        
        # At this point, arg_expr must be non-None (type guard for type checker)
        assert arg_expr is not None, "arg_expr should not be None at this point"
        
        # Evaluate the argument expression to get values
        values_series, _ = self.evaluate(arg_expr, df)
        
        # Apply the appropriate aggregation
        if func_name_lower == 'collect':
            # Return list of all values (including nulls if present)
            return values_series.tolist()
        
        elif func_name_lower == 'count':
            # Count non-null values
            return int(values_series.notna().sum())
        
        elif func_name_lower == 'sum':
            # Sum numeric values
            result = values_series.sum()
            return float(result) if pd.notna(result) else 0.0
        
        elif func_name_lower == 'avg':
            # Average of numeric values
            result = values_series.mean()
            return float(result) if pd.notna(result) else None
        
        elif func_name_lower == 'min':
            # Minimum value
            result = values_series.min()
            return result if pd.notna(result) else None
        
        elif func_name_lower == 'max':
            # Maximum value
            result = values_series.max()
            return result if pd.notna(result) else None
        
        else:
            raise ValueError(
                f"Unsupported aggregation function: {func_name}. "
                f"Supported functions: collect, count, sum, avg, min, max"
            )
