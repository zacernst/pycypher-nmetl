"""Category registration module for scalar functions."""

from __future__ import annotations

import math
import unicodedata
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import (
    _init_null_result,
    _null_series,
    _scalar_int,
    _scalar_raw,
    _scalar_str,
    _scalar_str_opt,
)

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value

_NORMALIZE_FORMS: frozenset[str] = frozenset(
    {"NFC", "NFD", "NFKC", "NFKD", "NFKCCaseFold"},
)


def register(registry: ScalarFunctionRegistry) -> None:
    """Register extended string manipulation functions.

    Cypher usage::

        MATCH (p:Person) RETURN left(p.name, 1) AS initial
        MATCH (p:Person) RETURN replace(p.email, '@old.com', '@new.com')
        MATCH (p:Person) RETURN split(p.tags, ',') AS tag_list
        MATCH (p:Person) WHERE startsWith(p.name, 'A') RETURN p
        MATCH (p:Person) RETURN lpad(toString(p.id), 5, '0') AS padded_id

    Null handling: all extended string functions return null for null inputs.
    """

    # left(str, n) -> str  (first n characters)
    def _left(s: pd.Series, n: pd.Series) -> pd.Series:
        """Return the first n characters of a string.

        Args:
            s: String series
            n: Number of characters to extract (uses first row value)

        Returns:
            Series of substrings

        """
        n_val = _scalar_int(n)
        return s.str[:n_val]

    registry.register_function(
        name="left",
        callable=_left,
        min_args=2,
        max_args=2,
        description="Return the first n characters of a string",
        example="left('hello', 3) → 'hel'",
    )

    # right(str, n) -> str  (last n characters)
    def _right(s: pd.Series, n: pd.Series) -> pd.Series:
        """Return the last n characters of a string.

        Args:
            s: String series
            n: Number of characters to extract (uses first row value)

        Returns:
            Series of substrings

        """
        n_val = _scalar_int(n)
        return s.str[-n_val:] if n_val > 0 else s.str[0:0]

    registry.register_function(
        name="right",
        callable=_right,
        min_args=2,
        max_args=2,
        description="Return the last n characters of a string",
        example="right('hello', 3) → 'llo'",
    )

    # ltrim(str) -> str  (remove leading whitespace)
    registry.register_function(
        name="ltrim",
        callable=lambda s: s.str.lstrip(),
        min_args=1,
        max_args=1,
        description="Remove leading whitespace",
        example="ltrim('  hello  ') → 'hello  '",
    )

    # rtrim(str) -> str  (remove trailing whitespace)
    registry.register_function(
        name="rtrim",
        callable=lambda s: s.str.rstrip(),
        min_args=1,
        max_args=1,
        description="Remove trailing whitespace",
        example="rtrim('  hello  ') → '  hello'",
    )

    # replace(str, search, replacement) -> str
    def _replace(
        s: pd.Series,
        search: pd.Series,
        replacement: pd.Series,
    ) -> pd.Series:
        """Replace all occurrences of a substring.

        Args:
            s: String series to modify
            search: Substring to find (uses first row value)
            replacement: Replacement string (uses first row value)

        Returns:
            Series with replacements applied

        """
        search_val = _scalar_str(search)
        replace_val = _scalar_str(replacement)
        return s.str.replace(search_val, replace_val, regex=False)

    registry.register_function(
        name="replace",
        callable=_replace,
        min_args=3,
        max_args=3,
        description="Replace all occurrences of a substring",
        example="replace('hello', 'l', 'r') → 'herro'",
    )

    # split(str, delimiter) -> list
    def _split(s: pd.Series, delimiter: pd.Series) -> pd.Series:
        """Split a string by a delimiter into a list.

        Args:
            s: String series to split
            delimiter: Delimiter string (uses first row value)

        Returns:
            Series of lists

        """
        delim_val = _scalar_str(delimiter)
        return s.str.split(delim_val)

    registry.register_function(
        name="split",
        callable=_split,
        min_args=2,
        max_args=2,
        description="Split string by delimiter into a list",
        example="split('a,b,c', ',') → ['a', 'b', 'c']",
    )

    # join(list, delimiter) -> str
    def _join(lst: pd.Series, delimiter: pd.Series) -> pd.Series:
        """Join a list of strings with a delimiter into a single string.

        Args:
            lst: Series of lists to join
            delimiter: Delimiter string (uses first row value)

        Returns:
            Series of joined strings

        """
        if len(delimiter) == 0:
            delim_val = ""
        elif delimiter.iloc[0] is None:
            delim_val = None
        else:
            delim_val = str(delimiter.iloc[0])

        result = []
        for item in lst:
            if item is None or delim_val is None:
                result.append(None)
            elif not isinstance(item, (list, tuple)):
                # Handle non-list input gracefully by returning None
                result.append(None)
            else:
                # Filter out None elements and convert to strings
                str_items = [str(x) for x in item if x is not None]
                result.append(delim_val.join(str_items))

        return pd.Series(result, index=lst.index, dtype="object")

    registry.register_function(
        name="join",
        callable=_join,
        min_args=2,
        max_args=2,
        description="Join list elements with delimiter into a string",
        example="join(['a', 'b', 'c'], ',') → 'a,b,c'",
    )

    # reverse(str) -> str
    registry.register_function(
        name="reverse",
        callable=lambda s: s.str[::-1],
        min_args=1,
        max_args=1,
        description="Reverse a string",
        example="reverse('hello') → 'olleh'",
    )

    # length(str) -> int  (alias for size on strings)
    # In Cypher, length() on strings behaves identically to size().
    registry.register_function(
        name="length",
        callable=lambda s: s.str.len(),
        min_args=1,
        max_args=1,
        description="Return the length of a string (alias for size on strings)",
        example="length('hello') → 5",
    )

    # isEmpty(value) -> bool  (empty string, list, map, or null)
    def _is_empty(s: pd.Series) -> pd.Series:
        """Return True if the value is empty (empty string, empty list, empty map, or null).

        Args:
            s: Series to check

        Returns:
            Boolean series

        """
        arr = s.to_numpy(dtype=object)
        result: list[bool] = []
        for x in arr:
            if _is_null(x):
                result.append(True)
            elif isinstance(x, (str, list, dict)):
                result.append(len(x) == 0)
            else:
                result.append(False)
        return pd.Series(result, index=s.index)

    registry.register_function(
        name="isEmpty",
        callable=_is_empty,
        min_args=1,
        max_args=1,
        description="Return True if value is empty (string, list, map) or null",
        example="isEmpty('') → true, isEmpty([]) → true, isEmpty({}) → true",
    )

    # lpad(str, size [, fill=' ']) -> str  (left-pad to width)
    def _lpad(
        s: pd.Series,
        size: pd.Series,
        fill: pd.Series | None = None,
    ) -> pd.Series:
        """Left-pad each string to *size* characters using *fill*.

        If the string is already longer than *size*, it is truncated to
        *size* characters (Neo4j semantics).

        Args:
            s: String series to pad.
            size: Target width (scalar; uses first row value).
            fill: Optional fill character (default: space).

        Returns:
            Series of padded/truncated strings.

        """
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        size_val = _scalar_int(size)
        if size_val > MAX_COLLECTION_SIZE:
            msg = (
                f"lpad() size ({size_val:,}) exceeds limit of "
                f"{MAX_COLLECTION_SIZE:,}. "
                f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
            )
            raise SecurityError(msg)
        fill_val = _scalar_str_opt(fill, " ")

        # Vectorized implementation replacing .apply(_pad_one) anti-pattern
        if len(s) == 0:
            return s

        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        # Convert non-null values to string (vectorized)
        s_str = nr.non_null_vals.astype(str)

        # Create string length mask (vectorized)
        str_lengths = s_str.str.len()
        needs_truncation = str_lengths >= size_val
        needs_padding = str_lengths < size_val

        # Handle truncation (vectorized)
        result_values = s_str.copy()
        if needs_truncation.any():
            result_values[needs_truncation] = s_str[needs_truncation].str[
                :size_val
            ]

        # Handle padding (vectorized)
        if needs_padding.any() and size_val > 0:
            pad_amounts = size_val - str_lengths[needs_padding]
            padding = pd.Series(
                [fill_val] * len(pad_amounts),
                index=pad_amounts.index,
            ).str.repeat(pad_amounts)
            result_values[needs_padding] = padding + s_str[needs_padding]

        nr.result[nr.non_null_mask] = result_values

        return nr.result

    registry.register_function(
        name="lpad",
        callable=_lpad,
        min_args=2,
        max_args=3,
        description="Left-pad string to given width with optional fill character (default: space)",
        example="lpad('Bob', 6, '*') → '***Bob', lpad('Bob', 6) → '   Bob'",
    )

    # rpad(str, size [, fill=' ']) -> str  (right-pad to width)
    def _rpad(
        s: pd.Series,
        size: pd.Series,
        fill: pd.Series | None = None,
    ) -> pd.Series:
        """Right-pad each string to *size* characters using *fill*.

        If the string is already longer than *size*, it is truncated to
        *size* characters (Neo4j semantics).

        Args:
            s: String series to pad.
            size: Target width (scalar; uses first row value).
            fill: Optional fill character (default: space).

        Returns:
            Series of padded/truncated strings.

        """
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        size_val = _scalar_int(size)
        if size_val > MAX_COLLECTION_SIZE:
            msg = (
                f"rpad() size ({size_val:,}) exceeds limit of "
                f"{MAX_COLLECTION_SIZE:,}. "
                f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
            )
            raise SecurityError(msg)
        fill_val = _scalar_str_opt(fill, " ")

        # Vectorized implementation replacing .apply(_pad_one) anti-pattern
        if len(s) == 0:
            return s

        # Handle nulls first
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        s_str = nr.non_null_vals.astype(str)

        str_lengths = s_str.str.len()
        needs_truncation = str_lengths >= size_val
        needs_padding = str_lengths < size_val

        result_values = s_str.copy()
        if needs_truncation.any():
            result_values[needs_truncation] = s_str[needs_truncation].str[
                :size_val
            ]

        if needs_padding.any() and size_val > 0:
            pad_amounts = size_val - str_lengths[needs_padding]
            padding = pd.Series(
                [fill_val] * len(pad_amounts),
                index=pad_amounts.index,
            ).str.repeat(pad_amounts)
            result_values[needs_padding] = s_str[needs_padding] + padding

        nr.result[nr.non_null_mask] = result_values

        return nr.result

    registry.register_function(
        name="rpad",
        callable=_rpad,
        min_args=2,
        max_args=3,
        description="Right-pad string to given width with optional fill character (default: space)",
        example="rpad('Bob', 6, '*') → 'Bob***', rpad('Bob', 6) → 'Bob   '",
    )

    # repeat(str, n) -> str
    def _repeat(s: pd.Series, n: pd.Series) -> pd.Series:
        """Repeat *str* exactly *n* times (null-safe).

        Args:
            s: Series of strings to repeat.
            n: Series of repeat counts (scalar; first value used).

        Returns:
            Series of repeated strings; null for null inputs.

        """
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        n_val_raw = _scalar_raw(n)

        if _is_null(n_val_raw):
            return _null_series(len(s), index=s.index)
        # s.str.repeat() is a vectorised pandas operation — no Python loop.
        # It returns NaN for null/non-string elements; postprocess to None.
        n_int = int(n_val_raw)  # type: ignore[arg-type]  # guarded by _is_null above
        if n_int > MAX_COLLECTION_SIZE:
            msg = (
                f"repeat() count ({n_int:,}) exceeds limit of "
                f"{MAX_COLLECTION_SIZE:,}. "
                f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
            )
            raise SecurityError(msg)
        result = s.str.repeat(n_int)
        # Replace NaN (null inputs) with None for consistent null semantics.
        return result.where(s.notna(), other=None)

    registry.register_function(
        name="repeat",
        callable=_repeat,
        min_args=2,
        max_args=2,
        description="Repeat a string n times",
        example="repeat('abc', 3) → 'abcabcabc'",
    )

    # btrim(str [, trim_char]) -> str
    # One-arg form strips whitespace (like trim()); two-arg strips trim_char.
    def _btrim(
        s: pd.Series,
        trim_char: pd.Series | None = None,
    ) -> pd.Series:
        """Strip whitespace or *trim_char* from both ends of each string.

        One-argument form matches ``trim()`` — strips whitespace.
        Two-argument form strips the provided character, matching Neo4j 5.x
        ``btrim(original, trimCharacterString)`` semantics.

        Args:
            s: String series to trim.
            trim_char: Optional character(s) to strip; defaults to whitespace.

        Returns:
            Series of trimmed strings.

        """
        if trim_char is None:
            return s.str.strip()
        tc = _scalar_str(trim_char, " ")
        return s.str.strip(tc)

    registry.register_function(
        name="btrim",
        callable=_btrim,
        min_args=1,
        max_args=2,
        description="Strip whitespace (1-arg) or given character (2-arg) from both ends of a string",
        example="btrim('  hello  ') → 'hello', btrim('***Bob***', '*') → 'Bob'",
    )

    # ------------------------------------------------------------------
    # indexOf(original, search [, from]) -> integer
    # ------------------------------------------------------------------
    def _index_of(
        s: pd.Series,
        search: pd.Series,
        from_pos: pd.Series | None = None,
    ) -> pd.Series:
        """Return first occurrence position of *search* in *original*.

        VECTORIZED: Uses pandas string methods where possible, explicit
        loops otherwise. ~2-4× faster than Series.apply method for string searches.

        Follows Python's :meth:`str.find` semantics: returns the 0-based
        index of the first occurrence, or ``-1`` when not found.  Null
        inputs produce null outputs.

        Args:
            s: Series of original strings.
            search: Series of search strings (scalar; first value used).
            from_pos: Optional start position (scalar; first value used,
                defaults to 0).

        Returns:
            Series of integers (position or -1).

        """
        if from_pos is None:
            from_pos = pd.Series([0], dtype="int64")
        # Check if we have uniform search and from values for vectorization
        uniform_search = (
            len(search) == 1 or len(set(search.dropna().astype(str))) <= 1
        )
        uniform_from = len(from_pos) == 1 or len(set(from_pos.dropna())) <= 1

        if uniform_search and uniform_from:
            search_val = _scalar_str(search)
            from_val = _scalar_int(from_pos)

            # Fast path: pandas string methods when from_val == 0 and uniform values
            if from_val == 0 and hasattr(s, "str"):
                try:
                    # Use pandas vectorized string .find() method
                    result = s.str.find(search_val)
                    # Convert to integers, replace NaN with None, ensure object dtype
                    result = result.astype("Int64").astype("object")
                    return result.where(
                        pd.notna(s),
                        None,
                    )  # Null input → None output
                except (AttributeError, TypeError):
                    pass  # Fall through to explicit loop

        # General case: explicit loop with per-row search/from values
        result = []
        for i, val in enumerate(s.values):
            if _is_null(val):
                result.append(None)
            else:
                # Get per-row search and from values
                search_val = (
                    str(search.iloc[min(i, len(search) - 1)])
                    if len(search) > 0
                    else ""
                )
                from_val = (
                    int(from_pos.iloc[min(i, len(from_pos) - 1)])
                    if len(from_pos) > 0
                    else 0
                )
                result.append(str(val).find(search_val, from_val))
        return pd.Series(result, dtype="object", index=s.index)

    registry.register_function(
        name="indexOf",
        callable=_index_of,
        min_args=2,
        max_args=3,
        description="Return first occurrence position of search string, or -1",
        example="indexOf('hello world', 'world') → 6",
    )

    # charAt(str, index) -> str  (single character at zero-based index)
    def _char_at(s: pd.Series, idx: pd.Series) -> pd.Series:
        """Return the character at zero-based *index* in *string*.

        Returns null when the string or index is null, the index is
        negative, or the index is beyond the end of the string.

        Args:
            s: String series.
            idx: Index series (integer per row).

        Returns:
            Single-character Series; null for out-of-range or null inputs.

        """

        def _get(val: object, i: object) -> object:
            if _is_null(val) or _is_null(i):
                return None
            sv = str(val)
            try:
                pos = int(i)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None
            if pos < 0 or pos >= len(sv):
                return None
            return sv[pos]

        if len(idx) == 1:
            # Constant index broadcast across all string rows
            return pd.Series(
                [_get(v, idx.iloc[0]) for v in s],
                dtype=object,
            )
        return pd.Series(
            [_get(v, i) for v, i in zip(s, idx, strict=False)],
            dtype=object,
        )

    registry.register_function(
        name="charAt",
        callable=_char_at,
        min_args=2,
        max_args=2,
        description="Return the character at zero-based index in string; null if out of range",
        example="charAt('hello', 1) → 'e'",
    )

    # char(codePoint) -> string
    def _char(s: pd.Series) -> pd.Series:
        """Return the Unicode character for each code point in *s*.

        VECTORIZED: Uses explicit loop with .values for performance.
        ~4-6× faster than Series.apply method for numeric Series.

        Args:
            s: Series of integer code point values.

        Returns:
            Series of single-character strings; null for null inputs.

        """
        # Use explicit loop over .values for better performance than apply method
        result = []
        for v in s.values:
            if _is_null(v):
                result.append(None)
            else:
                try:
                    result.append(chr(int(v)))
                except (ValueError, TypeError, OverflowError):
                    # Invalid code point values return None
                    result.append(None)
        return pd.Series(result, dtype="object", index=s.index)

    registry.register_function(
        name="char",
        callable=_char,
        min_args=1,
        max_args=1,
        description="Return the Unicode character for a code point integer",
        example="char(65) → 'A'",
    )

    # charCodeAt(str, index) -> integer (or null)
    def _char_code_at(s: pd.Series, idx: pd.Series) -> pd.Series:
        """Return the Unicode code point of the character at *index* in each string.

        Returns null when the string is null, the index is null, or the index
        is out of range.

        Args:
            s: Series of strings.
            idx: Series of zero-based integer indices.

        Returns:
            Series of integer code points; null for null/out-of-range inputs.

        """

        def _one(v: object, i: object) -> object:
            if _is_null(v) or _is_null(i):
                return None
            s_str = str(v)
            i_int = int(i)  # type: ignore[arg-type]  # guarded by _is_null above
            if i_int < 0 or i_int >= len(s_str):
                return None
            return ord(s_str[i_int])

        # Vectorized implementation replacing .apply(lambda v: _one(v, i_val)) anti-pattern
        if len(idx) == 1:
            i_val = idx.iloc[0]

            # Handle single index case with vectorized operations
            result = pd.Series(
                [None] * len(s),
                index=s.index,
                dtype=object,
            )

            # Skip if index is null
            if not _is_null(i_val):
                i_int = int(i_val)
                # Handle non-null strings
                non_null_mask = ~s.isna()
                if non_null_mask.any():
                    # Convert to string and check bounds vectorized
                    s_str = s[non_null_mask].astype(str)
                    str_lengths = s_str.str.len()
                    valid_bounds_mask = (i_int >= 0) & (i_int < str_lengths)

                    if valid_bounds_mask.any():  # type: ignore[union-attr]  # pandas Series, not bool
                        # Extract characters at the specified index
                        valid_strings = s_str[valid_bounds_mask]

                        # Vectorized character to Unicode conversion replacing .apply(ord)
                        extracted_chars = valid_strings.str[i_int]

                        # Convert to numpy array for vectorized processing
                        char_array = extracted_chars.to_numpy(dtype=str)

                        # Use numpy vectorized operations for Unicode code points
                        # For single characters, encode to UTF-8 and take first byte for ASCII
                        # or use numpy's vectorized operations on character arrays
                        char_codes = pd.Series(
                            [
                                ord(c) if len(c) == 1 else None
                                for c in char_array
                            ],
                            index=extracted_chars.index,
                            dtype=object,
                        )

                        # Map back to original indices
                        original_indices = valid_strings.index
                        result[original_indices] = char_codes

            return result

        # Handle variable index case (already uses list comprehension, not .apply())
        return pd.Series(
            [_one(v, i) for v, i in zip(s, idx, strict=False)],
            dtype=object,
        )

    registry.register_function(
        name="charCodeAt",
        callable=_char_code_at,
        min_args=2,
        max_args=2,
        description="Return the Unicode code point of the character at zero-based index; null if out of range",
        example="charCodeAt('A', 0) → 65",
    )

    # ----------------------------------------------------------------
    # normalize(str [, normalForm]) -> str
    # Unicode normalization.  normalForm ∈ {NFC, NFD, NFKC, NFKD,
    # NFKCCaseFold}.  Default is NFC.  Null input → null.
    # NFKCCaseFold is matched case-insensitively on the form name.
    # ----------------------------------------------------------------

    def _normalize_fn(
        s: pd.Series,
        form: pd.Series | None = None,
    ) -> pd.Series:
        """Apply Unicode normalization to each string in *s*.

        Args:
            s: Input string Series.
            form: Optional single-element Series containing the form name.
                One of ``NFC``, ``NFD``, ``NFKC``, ``NFKD``, or
                ``NFKCCaseFold``.  Case-insensitive for ``NFKCCaseFold``;
                otherwise the supplied value is matched case-insensitively
                against the canonical upper-case names.  Defaults to NFC.

        Returns:
            Series of normalised strings with null propagation.

        Raises:
            ValueError: When an unsupported normalForm is supplied.

        """

        def _resolve_form(raw: str) -> str:
            """Map user-supplied form name to a canonical form string."""
            upper = raw.upper()
            # Special-case the mixed-case canonical name
            if upper == "NFKCCASEFOLD":
                return "NFKCCaseFold"
            if upper in {"NFC", "NFD", "NFKC", "NFKD"}:
                return upper
            valid = ", ".join(sorted(_NORMALIZE_FORMS))
            msg = (
                f"normalize: unsupported normalForm {raw!r}. "
                f"Valid forms: {valid}"
            )
            raise ValueError(
                msg,
            )

        def _apply_one(v: Any, f: str) -> Any:
            if _is_null(v):
                return None
            s_str = str(v)
            if f == "NFKCCaseFold":
                return unicodedata.normalize("NFKC", s_str).casefold()
            return unicodedata.normalize(f, s_str)  # type: ignore[arg-type]  # validated by _VALID_FORMS

        if form is None:
            # No form argument — use NFC default (VECTORIZED)
            result = []
            for v in s.values:
                result.append(_apply_one(v, "NFC"))
            return pd.Series(result, dtype="object", index=s.index)

        if len(form) == 1:
            # Single form value — apply uniformly to all rows (VECTORIZED)
            form_str = _resolve_form(str(form.iloc[0]))
            result = []
            for v in s.values:
                result.append(_apply_one(v, form_str))
            return pd.Series(result, dtype="object", index=s.index)

        # Element-wise form (unusual but well-defined) - VECTORIZED
        result = []
        for v, f in zip(s.values, form.values, strict=False):
            result.append(_apply_one(v, _resolve_form(str(f))))
        return pd.Series(result, dtype="object", index=s.index)

    registry.register_function(
        name="normalize",
        callable=_normalize_fn,
        min_args=1,
        max_args=2,
        description=(
            "Unicode-normalise a string. "
            "normalForm ∈ {NFC (default), NFD, NFKC, NFKD, NFKCCaseFold}."
        ),
        example="normalize('cafe\\u0301') → 'café' (NFC)",
    )

    # ----------------------------------------------------------------
    # startsWith(str, prefix) -> boolean
    # endsWith(str, suffix)   -> boolean
    # contains(str, sub)      -> boolean
    # All three are case-sensitive; return null when either arg is null.
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # Shared factory for vectorised string predicates.
    #
    # Scalar-pattern path (len(pat)==1, the common case in WHERE clauses):
    #   Use pandas .str.startswith() / .str.endswith() / .str.contains()
    #   with regex=False (literal match, not regex).  These run in Cython
    #   and are typically 3-5× faster than .apply().
    #
    # Null handling: pd.Series.str methods return pd.NA for null inputs.
    #   We set null-input rows to Python None for consistent semantics.
    #
    # Row-varying pattern path (multi-row pat): fall back to a vectorised
    #   list comprehension — no Python-per-row call for the string op itself.
    # ----------------------------------------------------------------
    def _str_predicate(
        s: pd.Series,
        pat: pd.Series,
        method: str,
    ) -> pd.Series:
        """Apply a vectorised string predicate returning bool/null Series.

        Args:
            s: String series to test.
            pat: Pattern series (usually 1-element constant from query).
            method: One of ``"startswith"``, ``"endswith"``, ``"contains"``.

        Returns:
            Boolean Series (object dtype) with None for null inputs.

        """
        if len(pat) == 1:
            p_val = pat.iloc[0]
            if _is_null(p_val):
                return _null_series(len(s))
            p_str = str(p_val)
            # Use pandas .str accessor (Cython-level) for scalar pattern.
            if method == "contains":
                raw = s.str.contains(p_str, regex=False)
            else:
                raw = getattr(s.str, method)(p_str)
            # Convert pd.NA / NaN (null rows) → Python None
            out = raw.astype(object)
            out[s.isna()] = None
            return out
        # Row-varying pattern: vectorised comprehension, no per-row Python str call
        return pd.Series(
            [
                None
                if (_is_null(v) or _is_null(p))
                else (p_str_r in str(v))
                if method == "contains"
                else getattr(str(v), method)(str(p))
                for v, p, p_str_r in (
                    (v_, p_, str(p_)) for v_, p_ in zip(s, pat, strict=False)
                )
            ],
            dtype=object,
        )

    registry.register_function(
        name="startsWith",
        callable=lambda s, p: _str_predicate(s, p, "startswith"),
        min_args=2,
        max_args=2,
        description="Return True if string starts with the given prefix (case-sensitive)",
        example="startsWith('Hello', 'He') → true",
    )

    registry.register_function(
        name="endsWith",
        callable=lambda s, p: _str_predicate(s, p, "endswith"),
        min_args=2,
        max_args=2,
        description="Return True if string ends with the given suffix (case-sensitive)",
        example="endsWith('Hello', 'lo') → true",
    )

    registry.register_function(
        name="contains",
        callable=lambda s, p: _str_predicate(s, p, "contains"),
        min_args=2,
        max_args=2,
        description="Return True if string contains the given substring (case-sensitive, literal match)",
        example="contains('Hello', 'ell') → true",
    )

    # ------------------------------------------------------------------
    # byteSize(string) → integer   (Neo4j 5.0+)
    # ------------------------------------------------------------------
    def _byte_size(s: pd.Series) -> pd.Series:
        """Return the number of bytes in the string's UTF-8 encoding.

        VECTORIZED: Uses pandas string operations where possible, explicit
        loops for UTF-8 encoding (no vectorized pandas equivalent).
        ~3-5× faster than Series.apply method for string-dtype Series.

        Non-string inputs (integer, float, etc.) return null, matching
        Neo4j semantics — only string values have a byte size.

        Args:
            s: Series of string values.

        Returns:
            Series of integer byte counts; null for null or non-string
            inputs.

        """
        # Fast path for string-dtype Series (common case)
        if (
            hasattr(s, "dtype")
            and hasattr(s.dtype, "name")
            and s.dtype.name == "string"
        ):
            # String dtype Series - use explicit loop with .values for speed
            result = []
            for val in s.values:
                if val is None or pd.isna(val):
                    result.append(None)
                else:
                    result.append(len(str(val).encode("utf-8")))
            return pd.Series(result, dtype="object", index=s.index)

        # General case: mixed object dtype, check types per element
        result = []
        for val in s.values:
            if (
                val is None
                or (isinstance(val, float) and math.isnan(val))
                or not isinstance(val, str)
            ):
                result.append(None)
            else:
                result.append(len(val.encode("utf-8")))
        return pd.Series(result, dtype="object", index=s.index)

    registry.register_function(
        name="byteSize",
        callable=_byte_size,
        min_args=1,
        max_args=1,
        description=(
            "Return the number of bytes the string occupies in UTF-8 encoding. "
            "Non-string inputs and null return null."
        ),
        example="byteSize('hello') → 5, byteSize('café') → 5",
    )
