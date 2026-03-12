"""Scalar function registry and implementations for Cypher expressions.

Scalar functions operate on individual values (vectorized across rows),
as opposed to aggregation functions which reduce multiple rows to one value.

This module provides:
- ScalarFunctionRegistry: Singleton registry for managing scalar functions
- Built-in function implementations (string, conversion, utility functions)
- Plugin architecture for registering custom functions

Example:
    >>> import pandas as pd
    >>> from pycypher.scalar_functions import ScalarFunctionRegistry
    >>> 
    >>> registry = ScalarFunctionRegistry.get_instance()
    >>> input_series = pd.Series(['hello', 'world'])
    >>> result = registry.execute("toUpper", [input_series])
    >>> print(result.tolist())
    ['HELLO', 'WORLD']
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd
from shared.logger import LOGGER


@dataclass
class FunctionMetadata:
    """Metadata about a registered scalar function.

    Attributes:
        name: Function name (display version, preserves case)
        callable: Function implementation (accepts Series args, returns Series)
        min_args: Minimum number of required arguments
        max_args: Maximum number of arguments (None = unlimited)
        description: Human-readable description
        example: Example usage string
    """

    name: str
    callable: Callable[..., pd.Series]
    min_args: int
    max_args: Optional[int]
    description: str
    example: str


class ScalarFunctionRegistry:
    """Singleton registry for scalar functions.

    This registry manages all scalar functions available in Cypher expressions.
    Functions are registered at module import time and can be executed by name.

    Architecture:
    - Singleton pattern ensures single source of truth
    - Function names are case-insensitive for Cypher compatibility
    - All functions operate on pandas Series (vectorized operations)
    - Returns Series with same length as inputs

    Example:
        >>> registry = ScalarFunctionRegistry.get_instance()
        >>> registry.register_function(
        ...     name="double",
        ...     callable=lambda s: s * 2,
        ...     min_args=1,
        ...     max_args=1,
        ...     description="Double the value"
        ... )
        >>> result = registry.execute("double", [pd.Series([1, 2, 3])])
    """

    _instance: Optional[ScalarFunctionRegistry] = None

    def __init__(self) -> None:
        """Initialize registry with empty function map.

        Note:
            Use get_instance() instead of direct instantiation to ensure singleton.
        """
        self._functions: dict[str, FunctionMetadata] = {}

    @classmethod
    def get_instance(cls) -> ScalarFunctionRegistry:
        """Get or create singleton instance.

        Returns:
            The singleton ScalarFunctionRegistry instance
        """
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._register_builtin_functions()
        return cls._instance

    def register_function(
        self,
        name: str,
        callable: Callable[..., pd.Series],
        min_args: int,
        max_args: Optional[int] = None,
        description: str = "",
        example: str = "",
    ) -> None:
        """Register a scalar function.

        Args:
            name: Function name (case-insensitive)
            callable: Function implementation (accepts Series args, returns Series)
            min_args: Minimum number of required arguments
            max_args: Maximum number of arguments (None = unlimited)
            description: Human-readable description
            example: Example usage string

        Example:
            >>> registry.register_function(
            ...     name="toUpper",
            ...     callable=lambda s: s.str.upper(),
            ...     min_args=1,
            ...     max_args=1,
            ...     description="Convert string to uppercase",
            ...     example="toUpper('hello') → 'HELLO'"
            ... )
        """
        name_lower = name.lower()
        if name_lower in self._functions:
            LOGGER.warning(msg=f"Overwriting existing function: {name}")

        self._functions[name_lower] = FunctionMetadata(
            name=name,
            callable=callable,
            min_args=min_args,
            max_args=max_args,
            description=description,
            example=example,
        )

        LOGGER.debug(
            msg=f"Registered scalar function: {name} "
            f"(args: {min_args}-{max_args or '∞'})"
        )

    def has_function(self, name: str) -> bool:
        """Check if a function is registered.

        Args:
            name: Function name (case-insensitive)

        Returns:
            True if function is registered, False otherwise
        """
        return name.lower() in self._functions

    def execute(
        self, name: str, args: list[pd.Series], **kwargs: Any
    ) -> pd.Series:
        """Execute a registered scalar function.

        Args:
            name: Function name
            args: List of argument Series
            **kwargs: Additional keyword arguments

        Returns:
            Result Series with same length as input

        Raises:
            ValueError: If function not found or arg count invalid
            RuntimeError: If function execution fails
        """
        name_lower = name.lower()

        if name_lower not in self._functions:
            available = ", ".join(sorted(self._functions.keys()))
            raise ValueError(
                f"Unknown scalar function: {name}. "
                f"Available functions: {available}"
            )

        func_meta = self._functions[name_lower]

        # Validate argument count
        num_args = len(args)
        if num_args < func_meta.min_args:
            raise ValueError(
                f"Function {name} requires at least {func_meta.min_args} "
                f"argument{'s' if func_meta.min_args != 1 else ''}, got {num_args}"
            )
        if func_meta.max_args is not None and num_args > func_meta.max_args:
            raise ValueError(
                f"Function {name} accepts at most {func_meta.max_args} "
                f"argument{'s' if func_meta.max_args != 1 else ''}, got {num_args}"
            )

        # Execute function
        try:
            result = func_meta.callable(*args, **kwargs)

            # Ensure result is a Series
            if not isinstance(result, pd.Series):
                raise TypeError(
                    f"Function {name} must return pd.Series, "
                    f"got {type(result).__name__}"
                )

            return result

        except Exception as e:
            raise RuntimeError(f"Error executing function {name}: {e}") from e

    def _register_builtin_functions(self) -> None:
        """Register built-in Cypher scalar functions.

        Registers functions in categories:
        - String functions (toUpper, toLower, trim, substring, size)
        - Type conversion (toString, toInteger, toFloat, toBoolean)
        - Utility functions (coalesce)
        """
        self._register_string_functions()
        self._register_conversion_functions()
        self._register_utility_functions()

    def _register_string_functions(self) -> None:
        """Register string manipulation functions."""

        # toUpper(str) -> STR
        self.register_function(
            name="toUpper",
            callable=lambda s: s.str.upper(),
            min_args=1,
            max_args=1,
            description="Convert string to uppercase",
            example="toUpper('hello') → 'HELLO'",
        )

        # toLower(str) -> str
        self.register_function(
            name="toLower",
            callable=lambda s: s.str.lower(),
            min_args=1,
            max_args=1,
            description="Convert string to lowercase",
            example="toLower('HELLO') → 'hello'",
        )

        # trim(str) -> str (remove leading/trailing whitespace)
        self.register_function(
            name="trim",
            callable=lambda s: s.str.strip(),
            min_args=1,
            max_args=1,
            description="Remove leading and trailing whitespace",
            example="trim('  hello  ') → 'hello'",
        )

        # substring(str, start, length?) -> str
        def _substring(
            s: pd.Series, start: pd.Series, length: pd.Series | None = None
        ) -> pd.Series:
            """Extract substring (0-indexed in Cypher).

            Args:
                s: String series
                start: Starting index (0-based)
                length: Optional length to extract

            Returns:
                Substring series
            """
            # Convert start to integer (use first value if constant)
            start_val = int(start.iloc[0]) if len(start) > 0 else 0

            if length is not None:
                # Extract with specified length
                length_val = int(length.iloc[0]) if len(length) > 0 else 0
                return s.str.slice(start_val, start_val + length_val)
            else:
                # Extract from start to end
                return s.str.slice(start_val)

        self.register_function(
            name="substring",
            callable=_substring,
            min_args=2,
            max_args=3,
            description="Extract substring from string",
            example="substring('hello', 1, 3) → 'ell'",
        )

        # size(str) -> int (string length)
        # Note: In Cypher, size() works on both strings and lists
        def _size(s: pd.Series) -> pd.Series:
            """Get length of string or list.

            Args:
                s: Series containing strings or lists

            Returns:
                Series of lengths
            """
            # Try string length first
            if s.dtype == object:
                # Check if contains strings or lists
                first_val = s.iloc[0] if len(s) > 0 else None
                if isinstance(first_val, str):
                    return s.str.len()
                elif isinstance(first_val, list):
                    # Type cast to ensure it's a Series
                    result: pd.Series = s.apply(len)  # type: ignore[assignment]
                    return result
            # Fallback to string length
            return s.str.len()

        self.register_function(
            name="size",
            callable=_size,
            min_args=1,
            max_args=1,
            description="Get string or list length",
            example="size('hello') → 5, size([1,2,3]) → 3",
        )

    def _register_conversion_functions(self) -> None:
        """Register type conversion functions."""

        # toString(value) -> str
        def _to_string(s: pd.Series) -> pd.Series:
            """Convert value to string, preserving nulls.

            Handles pandas float upcasting: when a Series contains integers
            mixed with None, pandas upcasts to float64 (e.g., 42 -> 42.0).
            Detects this at the Series level: if the Series has float dtype,
            contains nulls, and ALL non-null values are integer-valued, it
            was likely an integer column upcasted by pandas.  In that case,
            integer-valued floats are rendered without decimal (42.0 -> "42").

            Args:
                s: Series to convert

            Returns:
                String series with nulls preserved as None
            """
            # Detect pandas null-upcasted integer columns:
            # float64 dtype + has nulls + every non-null value is integer-valued
            _is_upcasted_int = False
            if s.dtype == np.float64 and s.isna().any():
                non_null = s.dropna()
                if len(non_null) > 0:
                    _is_upcasted_int = bool(
                        (non_null == np.floor(non_null)).all()
                        and np.isfinite(non_null).all()
                    )

            def _convert_value(x: object) -> str | None:
                if pd.isna(x):
                    return None
                if _is_upcasted_int and isinstance(x, float):
                    return str(int(x))
                return str(x)

            result = s.apply(_convert_value)
            return pd.Series(result)  # Explicit cast to satisfy type checker

        self.register_function(
            name="toString",
            callable=_to_string,
            min_args=1,
            max_args=1,
            description="Convert value to string",
            example="toString(42) → '42'",
        )

        # toInteger(value) -> int
        def _to_integer(s: pd.Series) -> pd.Series:
            """Convert to integer via truncation, handling nulls and errors.

            Cypher's toInteger() truncates toward zero (like C/Java int cast):
            - toInteger(3.14)  -> 3
            - toInteger(2.99)  -> 2
            - toInteger(-1.7)  -> -1
            - toInteger("abc") -> null

            Args:
                s: Series to convert

            Returns:
                Integer series (Int64 supports nulls), invalid values become null
            """
            # Convert to numeric first (handles strings like "3.14" -> 3.14)
            numeric = pd.to_numeric(s, errors="coerce")
            # Truncate toward zero (not round) per Cypher semantics
            truncated = pd.Series(np.fix(numeric), index=numeric.index)
            return truncated.astype("Int64")

        self.register_function(
            name="toInteger",
            callable=_to_integer,
            min_args=1,
            max_args=1,
            description="Convert value to integer (nulls on error)",
            example="toInteger('42') → 42, toInteger('abc') → null",
        )

        # toFloat(value) -> float
        def _to_float(s: pd.Series) -> pd.Series:
            """Convert to float, handling nulls and errors.

            Args:
                s: Series to convert

            Returns:
                Float series, invalid values become null
            """
            return pd.to_numeric(s, errors="coerce")

        self.register_function(
            name="toFloat",
            callable=_to_float,
            min_args=1,
            max_args=1,
            description="Convert value to float (nulls on error)",
            example="toFloat('3.14') → 3.14, toFloat('abc') → null",
        )

        # toBoolean(value) -> bool
        def _to_boolean(s: pd.Series) -> pd.Series:
            """Convert to boolean following Cypher rules.

            Cypher boolean conversion:
            - Strings: 'true'/'TRUE' → True, 'false'/'FALSE' → False
            - Numbers: 0 → False, non-zero → True
            - Other values: null

            Args:
                s: Series to convert

            Returns:
                Boolean series with nulls for invalid values
            """
            # Convert to string and normalize
            str_series = s.astype(str).str.lower()

            # Map string values to booleans
            bool_map = {"true": True, "false": False, "1": True, "0": False}
            result = str_series.map(bool_map)

            # Preserve original nulls
            result[s.isna()] = None

            return result

        self.register_function(
            name="toBoolean",
            callable=_to_boolean,
            min_args=1,
            max_args=1,
            description="Convert value to boolean",
            example="toBoolean('true') → true, toBoolean('false') → false",
        )

    def _register_utility_functions(self) -> None:
        """Register general utility functions."""

        # coalesce(value1, value2, ...) -> first non-null
        def _coalesce(*series: pd.Series) -> pd.Series:
            """Return first non-null value across arguments.

            Args:
                *series: Variable number of Series to check

            Returns:
                Series with first non-null value from each position

            Example:
                coalesce([null, 'a'], [null, 'b'], ['c', 'c'])
                → ['c', 'a']
            """
            if not series:
                raise ValueError("coalesce requires at least one argument")

            result = series[0].copy()
            for s in series[1:]:
                # Fill nulls with values from next series
                result = result.fillna(s)

            return result

        self.register_function(
            name="coalesce",
            callable=_coalesce,
            min_args=1,
            max_args=None,  # Unlimited arguments
            description="Return first non-null value from arguments",
            example="coalesce(null, 'default') → 'default'",
        )
