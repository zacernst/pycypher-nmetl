"""Scalar function registry and implementations for Cypher expressions.

Scalar functions operate on entire pandas Series at once (vectorized across
rows), as opposed to aggregation functions which reduce multiple rows to one
value.  Functions never call Python per-row loops where a vectorised
alternative exists.

Vectorisation strategy by category
------------------------------------
- **Math** (abs, ceil, floor, sign, sqrt, cbrt, log, log2, log10, exp, pow,
  hypot, fmod): numpy C-level operations via a ``_make_math1_np`` /
  ``_make_trig1_np`` factory.  One numpy array call per Series.
- **Trigonometric** (sin, cos, tan, asin, acos, atan, atan2, sinh, cosh,
  tanh, cot, haversin, degrees, radians): same numpy factory.
- **String predicates** (startsWith, endsWith, contains): pandas ``.str``
  Cython accessor; ``contains`` always uses ``regex=False`` for literal
  matching.
- **String transforms** (toUpper, toLower, trim, ltrim, rtrim, etc.):
  pandas ``.str`` accessor.
- **Type conversion** (toInteger, toFloat): ``pd.to_numeric`` + numpy
  ``np.fix``; no per-row Python.
- All remaining categories use ``pd.Series.apply()`` because their logic is
  inherently element-wise (e.g. list operations, temporal parsing, encoding).
  ``round()`` uses Python's ``decimal`` module (no numpy equivalent).  It
  supports an optional third *mode* argument selecting one of seven Neo4j 5.x
  rounding modes: ``HALF_UP`` (default, ties away from zero), ``HALF_DOWN``
  (ties toward zero), ``HALF_EVEN`` (banker's rounding), ``CEILING``,
  ``FLOOR``, ``UP`` (always away from zero), ``DOWN`` (truncation).

This module provides:
- ScalarFunctionRegistry: Singleton registry for managing scalar functions
- Built-in function implementations:

  * **String**: toUpper, toLower, upper, lower, trim, ltrim, rtrim,
    substring, size, left, right, replace, split, reverse, length, isEmpty
  * **Extended string**: lpad, rpad, repeat, btrim, indexOf, charAt, char, charCodeAt, normalize,
    toStringOrNull, startsWith, endsWith, contains, byteSize, join
  * **Type conversion**: toString, toInteger, toFloat, toBoolean,
    toBooleanOrNull, toIntegerOrNull, toFloatOrNull
  * **Math**: abs, ceil, floor, round, sign, sqrt, cbrt, log, exp, log10, pow,
    sinh, cosh, tanh, log2, hypot, fmod, gcd, lcm
  * **Bitwise**: bitAnd, bitOr, bitXor, bitNot, bitShiftLeft, bitShiftRight
  * **Trigonometric**: sin, cos, tan, asin, acos, atan, atan2, cot, haversin
  * **Angle conversion**: degrees, radians
  * **Constants and random**: pi, e, rand
  * **List**: head, last, tail, range, sort, flatten, toStringList, toIntegerList, toFloatList, toBooleanList, toList, min, max
  * **Map**: keys, values, properties
  * **Temporal (parse)**: date, datetime, localdatetime, duration
  * **Temporal (truncate)**: date.truncate, datetime.truncate, localdatetime.truncate
  * **Temporal (now)**: timestamp, localtime, localdate
  * **Type introspection**: valueType
  * **Type predicates**: isString, isInteger, isFloat, isBoolean, isList, isMap
  * **Hash & encoding**: md5, sha1, sha256, encodeBase64, decodeBase64
  * **Utility**: coalesce, id, elementId, nullIf, isNaN, isInfinite, isFinite, infinity, randomUUID, exists
  * **Function aliases**: now (timestamp), len (length), str (toString),
    int (toInteger), float (toFloat), bool (toBoolean)

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

import base64 as _b64
import hashlib as _hl
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
from shared.helpers import is_null_value, suggest_close_match
from shared.logger import LOGGER

from pycypher.scalar_functions import (
    conversion_functions,
    extended_string_functions,
    list_functions,
    math_functions,
    temporal_functions,
    utility_functions,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_NORMALIZE_FORMS: frozenset[str] = frozenset(
    {"NFC", "NFD", "NFKC", "NFKD", "NFKCCaseFold"},
)


_is_null = is_null_value  # Canonical null-check from shared.helpers


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
    max_args: int | None
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

    **WHERE clause usage**: all registered functions are safe for use in WHERE
    clause predicates (``MATCH ... WHERE f(p.prop) = value``).  They operate
    row-by-row and are evaluated by ``ExpressionEvaluator._evaluate_scalar_function``
    which joins against the entity table on-demand to fetch property values.

    **NOT permitted in WHERE**: aggregation functions (``count``, ``sum``, ``avg``,
    ``min``, ``max``, ``collect``) are intentionally absent from this registry.
    The query translator in ``star.py`` guards against aggregations in WHERE predicates
    and raises a ``ValueError`` with an actionable message before evaluation begins.

    **Null safety contract**: every registered function must return ``NaN``/``None``
    for null inputs rather than raising.  All built-in functions satisfy this contract.
    Custom functions registered via ``register_function`` should follow the same rule.

    Example:
        >>> registry = ScalarFunctionRegistry.get_instance()
        >>> registry.register_function(
        ...     name="double",
        ...     callable=lambda s: s * 2,
        ...     min_args=1,
        ...     max_args=1,
        ...     description="Double the value",
        ...     example="double(3) → 6",
        ... )
        >>> result = registry.execute("double", [pd.Series([1, 2, 3])])

    """

    _instance: ScalarFunctionRegistry | None = None

    def __init__(self) -> None:
        """Initialize registry with empty function map.

        Note:
            Use get_instance() instead of direct instantiation to ensure singleton.

        """
        self._functions: dict[str, FunctionMetadata] = {}
        self._builtin_names: frozenset[str] = frozenset()

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
        max_args: int | None = None,
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
            f"(args: {min_args}-{max_args or '∞'})",
        )

    def _register_function_aliases(self) -> None:
        """Register common function aliases for improved user experience.

        Registers intuitive aliases that users naturally expect:
        - now() → timestamp() (common in many databases)
        - len() → length() (Python-style)
        - str() → toString() (Python-style)
        - int() → toInteger() (Python-style)
        - float() → toFloat() (Python-style)
        - bool() → toBoolean() (Python-style)
        """
        # now() -> timestamp() — common temporal function alias
        self.register_function(
            name="now",
            callable=self._functions["timestamp"].callable,
            min_args=0,
            max_args=1,
            description="Return current epoch milliseconds (alias for timestamp)",
            example="now() → 1700000000000",
        )

        # len() -> length() — Python-style string length function
        self.register_function(
            name="len",
            callable=self._functions["length"].callable,
            min_args=1,
            max_args=1,
            description="Return the length of a string (alias for length)",
            example="len('hello') → 5",
        )

        # str() -> toString() — Python-style type conversion
        self.register_function(
            name="str",
            callable=self._functions["tostring"].callable,
            min_args=1,
            max_args=1,
            description="Convert value to string (alias for toString)",
            example="str(42) → '42'",
        )

        # int() -> toInteger() — Python-style type conversion
        self.register_function(
            name="int",
            callable=self._functions["tointeger"].callable,
            min_args=1,
            max_args=1,
            description="Convert value to integer (alias for toInteger)",
            example="int('42') → 42",
        )

        # float() -> toFloat() — Python-style type conversion
        self.register_function(
            name="float",
            callable=self._functions["tofloat"].callable,
            min_args=1,
            max_args=1,
            description="Convert value to float (alias for toFloat)",
            example="float('3.14') → 3.14",
        )

        # bool() -> toBoolean() — Python-style type conversion
        self.register_function(
            name="bool",
            callable=self._functions["toboolean"].callable,
            min_args=1,
            max_args=1,
            description="Convert value to boolean (alias for toBoolean)",
            example="bool('true') → true",
        )

    def has_function(self, name: str) -> bool:
        """Check if a function is registered.

        Args:
            name: Function name (case-insensitive)

        Returns:
            True if function is registered, False otherwise

        """
        return name.lower() in self._functions

    def list_functions(self) -> list[str]:
        """Return a sorted list of all registered function names.

        Names are lowercased, matching Cypher's case-insensitive semantics.

        Returns:
            Sorted list of registered function names.

        """
        return sorted(self._functions.keys())

    def execute(
        self,
        name: str,
        args: list[pd.Series],
        **kwargs: Any,
    ) -> pd.Series:
        """Execute a registered scalar function.

        Args:
            name: Function name
            args: List of argument Series
            **kwargs: Additional keyword arguments

        Returns:
            Result Series with same length as input

        Raises:
            UnsupportedFunctionError: If function name is not registered.
            FunctionArgumentError: If argument count is outside valid range.
            TypeError: If the function returns a non-Series result, or raises
                TypeError internally during execution.
            RuntimeError: If the function raises an unexpected exception at
                runtime (i.e. not ValueError or TypeError).

        """
        name_lower = name.lower()

        if name_lower not in self._functions:
            from pycypher.exceptions import UnsupportedFunctionError

            available_keys = sorted(self._functions.keys())
            hint = suggest_close_match(name_lower, available_keys)
            exc = UnsupportedFunctionError(
                function_name=name,
                supported_functions=available_keys,
                category="scalar",
            )
            if hint:
                # Append "Did you mean?" hint to the exception message
                exc.args = (f"{exc.args[0]}{hint}",)
            raise exc

        func_meta = self._functions[name_lower]

        # Validate argument count
        num_args = len(args)
        if num_args < func_meta.min_args:
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name=name,
                expected_args=func_meta.min_args,
                actual_args=num_args,
                argument_description=f"at least {func_meta.min_args}",
            )
        if func_meta.max_args is not None and num_args > func_meta.max_args:
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name=name,
                expected_args=func_meta.max_args,
                actual_args=num_args,
                argument_description=f"at most {func_meta.max_args}",
            )

        # Execute function; let TypeError/ValueError from validation propagate directly.
        # Only unexpected runtime exceptions are wrapped in RuntimeError.
        try:
            result = func_meta.callable(*args, **kwargs)
        except (TypeError, ValueError):
            raise
        except Exception as e:
            msg = f"Error executing function {name}: {e}"
            raise RuntimeError(msg) from e

        # Ensure result is a Series — WrongCypherTypeError propagates to caller.
        if not isinstance(result, pd.Series):
            from pycypher.exceptions import WrongCypherTypeError

            msg = (
                f"Function {name} must return pd.Series, "
                f"got {type(result).__name__}"
            )
            raise WrongCypherTypeError(
                msg,
            )

        return result

    def _register_builtin_functions(self) -> None:
        """Register built-in Cypher scalar functions.

        Registers functions in categories:
        - String functions (toUpper, toLower, trim, substring, size)
        - Extended string functions (left, right, ltrim, rtrim, replace, split, join, reverse, length, isEmpty)
        - Type conversion (toString, toInteger, toFloat, toBoolean)
        - Math functions (abs, ceil, floor, round, sign, sqrt, log, exp)
        - List functions (head, last, tail, range)
        - Map functions (keys, values, properties)
        - Hash & encoding functions (md5, sha1, sha256, encodeBase64, decodeBase64)
        - Utility functions (coalesce)
        """
        self._register_string_functions()
        extended_string_functions.register(self)
        conversion_functions.register(self)
        self._register_value_type_function()
        self._register_type_predicate_functions()
        math_functions.register(self)
        list_functions.register(self)
        self._register_map_functions()
        utility_functions.register(self)
        self._register_hash_encoding_functions()
        temporal_functions.register(self)
        self._register_function_aliases()
        self._builtin_names = frozenset(self._functions.keys())

    def _register_string_functions(self) -> None:
        """Register string manipulation functions.

        Cypher usage::

            MATCH (p:Person) WHERE toUpper(p.name) = 'ALICE' RETURN p
            MATCH (p:Person) RETURN substring(p.name, 0, 3) AS initials
            MATCH (p:Person) RETURN size(p.name) AS name_length

        Null handling: all string functions return null for null inputs.
        """
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

        # upper(str) / lower(str) — SQL-style aliases for toUpper / toLower
        self.register_function(
            name="upper",
            callable=lambda s: s.str.upper(),
            min_args=1,
            max_args=1,
            description="Convert string to uppercase (alias for toUpper)",
            example="upper('hello') → 'HELLO'",
        )
        self.register_function(
            name="lower",
            callable=lambda s: s.str.lower(),
            min_args=1,
            max_args=1,
            description="Convert string to lowercase (alias for toLower)",
            example="lower('HELLO') → 'hello'",
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
            s: pd.Series,
            start: pd.Series,
            length: pd.Series | None = None,
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
                # Check if contains strings or lists - find first non-null value
                first_val = None
                for val in s:
                    if val is not None:
                        first_val = val
                        break
                if isinstance(first_val, str):
                    return s.str.len()
                if isinstance(first_val, list):
                    # Vectorized list length: explicit loop over numpy object array
                    # avoids Series.__init__ overhead of per-element function calls
                    arr = s.to_numpy(dtype=object)
                    result_list = []
                    for val in arr:
                        if val is None:
                            result_list.append(None)
                        else:
                            result_list.append(len(val))
                    return pd.Series(result_list, index=s.index)
            # Fallback to string length (convert to string first for object dtype)
            return s.astype(str).str.len()

        self.register_function(
            name="size",
            callable=_size,
            min_args=1,
            max_args=1,
            description="Get string or list length",
            example="size('hello') → 5, size([1,2,3]) → 3",
        )

    def _register_value_type_function(self) -> None:
        """Register the valueType() function (Neo4j 5.x).

        Cypher usage::

            MATCH (n) RETURN n.prop, valueType(n.prop) AS type
            MATCH (n) WHERE valueType(n.value) = 'INTEGER' RETURN n

        Returns the Cypher type of a value as a string, element-wise.

        Type mapping:
          - null           → "NULL"
          - bool / numpy bool_ → "BOOLEAN"
          - int / numpy integer → "INTEGER"
          - float / numpy floating → "FLOAT"
          - str            → "STRING"
          - list / empty   → "LIST<NOTHING>" / "LIST<T NOT NULL>" / "LIST<T>" / "LIST<ANY>"
          - dict           → "MAP"
        """

        def _cypher_type_of(v: object) -> str:
            """Return the Cypher type name for a single Python value."""
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "NULL"
            # bool must be checked before int (bool is a subclass of int)
            if isinstance(v, (bool, np.bool_)):
                return "BOOLEAN"
            if isinstance(v, (int, np.integer)):
                return "INTEGER"
            if isinstance(v, (float, np.floating)):
                return "FLOAT"
            if isinstance(v, str):
                return "STRING"
            if isinstance(v, dict):
                return "MAP"
            if isinstance(v, list):
                if not v:
                    return "LIST<NOTHING>"
                element_types: set[str] = set()
                has_null = False
                for item in v:
                    if item is None or (
                        isinstance(item, float) and math.isnan(item)
                    ):
                        has_null = True
                    else:
                        element_types.add(_cypher_type_of(item))
                if not element_types:
                    # All nulls
                    return "LIST<NULL>"
                if len(element_types) > 1:
                    return "LIST<ANY>"
                elem_type = next(iter(element_types))
                return (
                    f"LIST<{elem_type}>"
                    if has_null
                    else f"LIST<{elem_type} NOT NULL>"
                )
            # Unknown Python type — fall back to string representation
            return "ANY"

        def _value_type(s: pd.Series) -> pd.Series:
            """Return Cypher type name for each element of the series.

            Args:
                s: Input series of any dtype

            Returns:
                String series with the Cypher type name of each value

            """
            return pd.Series(
                [_cypher_type_of(v) for v in s],
                index=s.index,
                dtype=object,
            )

        self.register_function(
            name="valueType",
            callable=_value_type,
            min_args=1,
            max_args=1,
            description=(
                "Return the Cypher type of a value as a string "
                "(Neo4j 5.x): NULL, BOOLEAN, INTEGER, FLOAT, STRING, "
                "LIST<...>, MAP"
            ),
            example="valueType(42) → 'INTEGER', valueType(null) → 'NULL'",
        )

    def _register_type_predicate_functions(self) -> None:
        """Register Neo4j 5.x type predicate functions.

        Cypher usage::

            MATCH (p:Person) WHERE isString(p.name) RETURN p
            MATCH (n) WHERE isInteger(n.count) RETURN n.count
            MATCH (n) RETURN valueType(n.prop) AS type

        All six functions follow the same contract:
        - Return ``True`` when the value matches the named type.
        - Return ``False`` when the value is a non-null value of a different type.
        - Return ``null`` (``None``) when the value is ``null``.
        - NaN is treated as a float value, not null (opposite of ``_is_null``).

        Important subtype ordering:
        - ``bool`` / ``np.bool_`` are subclasses of ``int`` in Python — always
          check boolean first so ``isInteger(True)`` returns ``False``.
        - ``float('nan')`` passes ``_is_null()`` in our helpers but IS a float
          for type-predicate purposes; use ``_is_strictly_null`` (``x is None``)
          as the null guard here.

        Functions registered: ``isString``, ``isInteger``, ``isFloat``,
        ``isBoolean``, ``isList``, ``isMap``.
        """

        def _strictly_null(v: object) -> bool:
            """Return True only for Python None — float('nan') is NOT null here."""
            return v is None

        def _make_type_pred(type_check: Any) -> Any:
            """Factory: return a null-safe Series→Series function applying type_check.

            Uses an explicit Python loop over the underlying numpy object array,
            which avoids the ``pd.Series.__init__`` overhead of ``s.apply()``
            while preserving identical semantics for object-dtype Series.
            For numeric type predicates (isFloat, isInteger, isBoolean) that
            have known pandas dtype equivalents, dedicated implementations with
            dtype fast paths are registered directly — this factory handles the
            remaining predicates (isString, isList, isMap) that have no fast path.

            Args:
                type_check: Callable ``(v: object) -> bool`` applied per element.

            Returns:
                Series function ``(s: pd.Series) -> pd.Series``.

            """

            def _fn(s: pd.Series) -> pd.Series:
                arr = s.to_numpy(dtype=object)
                result: list[object] = []
                for v in arr:
                    result.append(None if _strictly_null(v) else type_check(v))
                return pd.Series(result, index=s.index)

            return _fn

        # isString(x) → true iff x is a string
        self.register_function(
            name="isString",
            callable=_make_type_pred(lambda v: isinstance(v, str)),
            min_args=1,
            max_args=1,
            description="Return true if value is a string, false otherwise, null for null",
            example="isString('hello') → true, isString(42) → false",
        )

        # isInteger(x) → true iff x is an integer (not boolean — bool ⊂ int)
        # Fast paths: int64 dtype → all True; float64/bool dtype → all False.
        def _is_integer_pred(s: pd.Series) -> pd.Series:
            """Return True for integers (not bool), None for null, False otherwise.

            Fast path for int64 dtype: all values are integers — all True.
            Fast path for float64 or bool dtype: no integers — all False.
            Object dtype: explicit element loop with bool exclusion.

            Args:
                s: Input Series.

            Returns:
                Boolean Series — True where the value is an integer.

            """
            if pd.api.types.is_integer_dtype(
                s,
            ) and not pd.api.types.is_bool_dtype(s):
                return pd.Series(np.ones(len(s), dtype=bool), index=s.index)
            if pd.api.types.is_float_dtype(s) or pd.api.types.is_bool_dtype(s):
                return pd.Series(np.zeros(len(s), dtype=bool), index=s.index)
            arr = s.to_numpy(dtype=object)
            result: list[object] = []
            for v in arr:
                result.append(
                    None
                    if _strictly_null(v)
                    else (
                        isinstance(v, (int, np.integer))
                        and not isinstance(v, (bool, np.bool_))
                    ),
                )
            return pd.Series(result, index=s.index)

        self.register_function(
            name="isInteger",
            callable=_is_integer_pred,
            min_args=1,
            max_args=1,
            description="Return true if value is an integer (not boolean), null for null",
            example="isInteger(42) → true, isInteger(true) → false",
        )

        # isFloat(x) → true iff x is a float (including NaN — NaN is a float)
        # Fast paths: float64 dtype → all True; int64/bool dtype → all False.
        def _is_float_pred(s: pd.Series) -> pd.Series:
            """Return True for float values (including NaN), None for null, False otherwise.

            NaN is a valid IEEE 754 float value; isFloat(NaN) returns True.

            Fast path for float64 dtype: all values are floats — all True.
            Fast path for int64 or bool dtype: no floats — all False.
            Object dtype: explicit element loop.

            Args:
                s: Input Series.

            Returns:
                Boolean Series — True where the value is a float.

            """
            if pd.api.types.is_float_dtype(s):
                return pd.Series(np.ones(len(s), dtype=bool), index=s.index)
            if pd.api.types.is_integer_dtype(s) or pd.api.types.is_bool_dtype(
                s,
            ):
                return pd.Series(np.zeros(len(s), dtype=bool), index=s.index)
            arr = s.to_numpy(dtype=object)
            result: list[object] = []
            for v in arr:
                result.append(
                    None
                    if _strictly_null(v)
                    else isinstance(v, (float, np.floating)),
                )
            return pd.Series(result, index=s.index)

        self.register_function(
            name="isFloat",
            callable=_is_float_pred,
            min_args=1,
            max_args=1,
            description="Return true if value is a float (including NaN), null for null",
            example="isFloat(3.14) → true, isFloat(42) → false",
        )

        # isBoolean(x) → true iff x is a boolean
        # Fast paths: bool dtype → all True; int64/float64 dtype → all False.
        def _is_boolean_pred(s: pd.Series) -> pd.Series:
            """Return True for boolean values, None for null, False otherwise.

            Fast path for bool dtype: all values are booleans — all True.
            Fast path for int64 or float64 dtype: no booleans — all False.
            Object dtype: explicit element loop.

            Args:
                s: Input Series.

            Returns:
                Boolean Series — True where the value is a boolean.

            """
            if pd.api.types.is_bool_dtype(s):
                return pd.Series(np.ones(len(s), dtype=bool), index=s.index)
            if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(
                s,
            ):
                return pd.Series(np.zeros(len(s), dtype=bool), index=s.index)
            arr = s.to_numpy(dtype=object)
            result: list[object] = []
            for v in arr:
                result.append(
                    None
                    if _strictly_null(v)
                    else isinstance(v, (bool, np.bool_)),
                )
            return pd.Series(result, index=s.index)

        self.register_function(
            name="isBoolean",
            callable=_is_boolean_pred,
            min_args=1,
            max_args=1,
            description="Return true if value is a boolean, null for null",
            example="isBoolean(true) → true, isBoolean(1) → false",
        )

        # isList(x) → true iff x is a list
        self.register_function(
            name="isList",
            callable=_make_type_pred(lambda v: isinstance(v, list)),
            min_args=1,
            max_args=1,
            description="Return true if value is a list, null for null",
            example="isList([1, 2]) → true, isList('abc') → false",
        )

        # isMap(x) → true iff x is a map/dict
        self.register_function(
            name="isMap",
            callable=_make_type_pred(lambda v: isinstance(v, dict)),
            min_args=1,
            max_args=1,
            description="Return true if value is a map, null for null",
            example="isMap({a: 1}) → true, isMap([1]) → false",
        )

    def _register_hash_encoding_functions(self) -> None:
        """Register cryptographic hash and Base64 encoding functions.

        Registers the following Neo4j 4.4+ compatible functions:

        * ``md5(string)`` — 32-char lowercase hex MD5 digest
        * ``sha1(string)`` — 40-char lowercase hex SHA-1 digest
        * ``sha256(string)`` — 64-char lowercase hex SHA-256 digest
        * ``encodeBase64(string)`` — standard Base64-encoded string (no newlines)
        * ``decodeBase64(string)`` — UTF-8 string decoded from Base64 input

        All five functions propagate null: a null input returns null output.
        Input strings are encoded as UTF-8 before hashing/encoding.
        """

        def _null_guard(val: object) -> bool:
            """Return True when *val* is null (None or NaN float)."""
            return val is None or (isinstance(val, float) and val != val)

        def _md5(s: pd.Series) -> pd.Series:
            """Return 32-char lowercase hex MD5 digest; null → null."""
            return s.map(
                lambda v: (
                    None
                    if _null_guard(v)
                    else _hl.md5(str(v).encode("utf-8"), usedforsecurity=False).hexdigest()
                ),
            )

        def _sha1(s: pd.Series) -> pd.Series:
            """Return 40-char lowercase hex SHA-1 digest; null → null."""
            return s.map(
                lambda v: (
                    None
                    if _null_guard(v)
                    else _hl.sha1(str(v).encode("utf-8"), usedforsecurity=False).hexdigest()
                ),
            )

        def _sha256(s: pd.Series) -> pd.Series:
            """Return 64-char lowercase hex SHA-256 digest; null → null."""
            return s.map(
                lambda v: (
                    None
                    if _null_guard(v)
                    else _hl.sha256(str(v).encode("utf-8")).hexdigest()
                ),
            )

        def _encode_base64(s: pd.Series) -> pd.Series:
            """Encode string as Base64 (UTF-8, no line breaks); null → null."""
            return s.map(
                lambda v: (
                    None
                    if _null_guard(v)
                    else _b64.b64encode(str(v).encode("utf-8")).decode("utf-8")
                ),
            )

        def _decode_base64(s: pd.Series) -> pd.Series:
            """Decode Base64 string to UTF-8 plaintext; null → null."""
            return s.map(
                lambda v: (
                    None
                    if _null_guard(v)
                    else _b64.b64decode(str(v).encode("utf-8")).decode("utf-8")
                ),
            )

        self.register_function(
            name="md5",
            callable=_md5,
            min_args=1,
            max_args=1,
            description="Return the MD5 hash of a string as a 32-char lowercase hex digest",
            example="md5('hello') → '5d41402abc4b2a76b9719d911017c592'",
        )
        self.register_function(
            name="sha1",
            callable=_sha1,
            min_args=1,
            max_args=1,
            description="Return the SHA-1 hash of a string as a 40-char lowercase hex digest",
            example="sha1('hello') → 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'",
        )
        self.register_function(
            name="sha256",
            callable=_sha256,
            min_args=1,
            max_args=1,
            description="Return the SHA-256 hash of a string as a 64-char lowercase hex digest",
            example="sha256('hello') → '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'",
        )
        self.register_function(
            name="encodeBase64",
            callable=_encode_base64,
            min_args=1,
            max_args=1,
            description="Return the Base64 encoding of a string (no line breaks)",
            example="encodeBase64('hello') → 'aGVsbG8='",
        )
        self.register_function(
            name="decodeBase64",
            callable=_decode_base64,
            min_args=1,
            max_args=1,
            description="Decode a Base64-encoded string and return the UTF-8 plaintext",
            example="decodeBase64('aGVsbG8=') → 'hello'",
        )

    def _register_map_functions(self) -> None:
        """Register map introspection functions: keys, values, properties.

        These handle *map literal* arguments (Python ``dict`` values).
        The ``binding_evaluator`` handles the special case where the argument is
        a ``Variable`` bound to a graph node or relationship by doing a
        context-level column lookup.  For all other cases — map literals such as
        ``keys({a: 1, b: 2})`` — the fallthrough path reaches the scalar registry,
        and these entries provide the correct result.
        """

        def _keys(s: pd.Series) -> pd.Series:
            """Return list of keys from each map; null/non-map → null."""

            def _one(x: object) -> object:
                if _is_null(x):
                    return None
                if isinstance(x, dict):
                    return list(x.keys())
                return None

            return cast(pd.Series, s.apply(_one))

        self.register_function(
            name="keys",
            callable=_keys,
            min_args=1,
            max_args=1,
            description="Return the list of keys in a map",
            example="keys({a: 1, b: 2}) → ['a', 'b']",
        )

        def _values(s: pd.Series) -> pd.Series:
            """Return list of values from each map; null/non-map → null."""

            def _one(x: object) -> object:
                if _is_null(x):
                    return None
                if isinstance(x, dict):
                    return list(x.values())
                return None

            return cast(pd.Series, s.apply(_one))

        self.register_function(
            name="values",
            callable=_values,
            min_args=1,
            max_args=1,
            description="Return the list of values in a map",
            example="values({a: 1, b: 2}) → [1, 2]",
        )

        def _properties(s: pd.Series) -> pd.Series:
            """Return map as-is (identity for dicts); null/non-map → null."""

            def _one(x: object) -> object:
                if _is_null(x):
                    return None
                if isinstance(x, dict):
                    return x
                return None

            return cast(pd.Series, s.apply(_one))

        self.register_function(
            name="properties",
            callable=_properties,
            min_args=1,
            max_args=1,
            description="Return all properties of a map or node/relationship as a map",
            example="properties({name: 'Alice'}) → {name: 'Alice'}",
        )
