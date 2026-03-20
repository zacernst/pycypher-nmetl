"""Shared utility functions used across the PyCypher-NMETL project.

This module provides common helper functions for URI handling, encoding/decoding
objects, data type conversions, and user-experience helpers that are used
throughout the project.
"""

from __future__ import annotations

import base64
import difflib
import json
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, urlparse

from shared.logger import LOGGER

if TYPE_CHECKING:
    from collections.abc import Iterable


def is_null_value(x: object) -> bool:
    """Return ``True`` if *x* is ``None`` or a floating-point ``NaN``.

    This is the canonical null-check used throughout the PyCypher evaluator
    pipeline.  Cypher treats both ``None`` and ``NaN`` as null, so every
    point that needs to distinguish "has a value" from "is missing" should
    call this function rather than re-implementing the check inline.

    Args:
        x: The value to test.

    Returns:
        ``True`` when *x* is ``None`` or ``float('nan')``.

    """
    return x is None or (isinstance(x, float) and math.isnan(x))


def is_null_raw_list(value: object) -> bool:
    """Return ``True`` if *value* is null, empty, or not a valid iterable collection.

    Used by collection evaluators to detect when a row's list expression
    produced a missing/non-list value so each caller can handle the
    degenerate case (skip iteration, treat as empty list, or return the
    accumulator unchanged).

    Handles ``None``, empty pandas arrays/Series, empty Python lists/tuples,
    and scalar pandas null values (``NaN``, ``pd.NA``, ``pd.NaT``).

    Args:
        value: A single Python value from a ``pd.Series`` iteration.

    Returns:
        ``True`` if the value should be treated as an empty/missing list.

    """
    import pandas as pd

    if value is None:
        return True

    # pandas arrays/Series: check emptiness before boolean context
    # (avoids "ambiguous truth value of array" ValueError).
    if hasattr(value, "dtype") and hasattr(value, "__len__"):
        return len(value) == 0

    # Standard Python sequences
    if isinstance(value, (list, tuple)):
        return len(value) == 0

    # Scalar pandas null values (NaN, pd.NA, pd.NaT)
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def suggest_close_match(
    target: str,
    candidates: Iterable[str],
    cutoff: float = 0.6,
) -> str:
    """Return a "Did you mean '…'?" hint string when a close match is found.

    Uses :func:`difflib.get_close_matches` to find the single best candidate
    within *cutoff* similarity.  Returns an empty string when the target already
    appears verbatim in *candidates* (no hint needed for correct input) or when
    no candidate is close enough.

    Args:
        target: The misspelled or unknown name entered by the user.
        candidates: Iterable of valid names to compare against.
        cutoff: Minimum similarity ratio in ``[0, 1]``.  Defaults to ``0.6``,
            matching the project-wide convention used at all three call sites.

    Returns:
        A hint string such as ``"  Did you mean 'tolower'?"`` (with a leading
        two-space indent for inline appending to error messages), or ``""`` when
        no close match exists.

    Examples:
        >>> suggest_close_match("persn", ["person", "company"])
        "  Did you mean 'person'?"
        >>> suggest_close_match("xyz", ["person", "company"])
        ''
        >>> suggest_close_match("person", ["person"])  # exact match — no hint
        ''

    """
    candidates_list = list(candidates)
    # Exact match: no hint needed.
    if target in candidates_list:
        return ""
    matches = difflib.get_close_matches(
        target,
        candidates_list,
        n=1,
        cutoff=cutoff,
    )
    if matches:
        return f"  Did you mean '{matches[0]}'?"
    return ""


def ensure_uri(uri_input: str | ParseResult | Path) -> ParseResult:
    """Ensure input is converted to a ParseResult URI object."""
    if isinstance(uri_input, ParseResult):
        pass
    elif isinstance(uri_input, str):
        uri_input = urlparse(uri_input)
    elif isinstance(uri_input, Path):
        uri_input = urlparse(uri_input.as_uri())
    else:
        msg = f"URI must be a string or ParseResult, not {type(uri_input)}"
        raise ValueError(
            msg,
        )
    # Redact credentials before logging to prevent password leakage
    if uri_input.password:
        _host = uri_input.hostname or ""
        _port = f":{uri_input.port}" if uri_input.port else ""
        _safe = uri_input._replace(netloc=f"{_host}{_port}")
    else:
        _safe = uri_input
    LOGGER.debug("URI converted: %s", _safe)
    return uri_input


def decode(encoded_data: str) -> Any:
    """Decode a base64-encoded JSON object.

    Accepts JSON-serializable Python values (dicts, lists, strings, numbers,
    booleans, ``None``).  Raises :class:`ValueError` for any input that is not
    valid base64-encoded JSON — including pickle payloads, which are rejected
    to prevent deserialisation of arbitrary code.

    Args:
        encoded_data: Base64-encoded UTF-8 JSON string, as produced by
            :func:`encode`.

    Returns:
        The decoded Python value.

    Raises:
        ValueError: If the input is not valid base64-encoded JSON.

    """
    try:
        decoded_object = json.loads(base64.b64decode(encoded_data))
    except Exception as e:
        msg = f"Error decoding base64 string: {e}"
        raise ValueError(msg) from e
    return decoded_object


def encode(source_object: Any, to_bytes: bool = False) -> str | bytes:
    """Encode a JSON-serializable Python object as a base64 string.

    Accepts dicts, lists, strings, numbers, booleans, and ``None``.  Raises
    :class:`ValueError` for types that are not JSON-serializable (e.g. sets,
    custom class instances, bytes).  Using JSON rather than ``pickle`` prevents
    deserialisation of arbitrary code in :func:`decode`.

    Args:
        source_object: A JSON-serializable Python value.
        to_bytes: If ``True``, return the base64 string as :class:`bytes`
            instead of :class:`str`.

    Returns:
        Base64-encoded representation as :class:`str` (default) or
        :class:`bytes`.

    Raises:
        ValueError: If *source_object* is not JSON-serializable or if
            encoding fails for any other reason.

    """
    try:
        encoded_result = base64.b64encode(
            json.dumps(source_object).encode("utf-8"),
        ).decode("utf-8")
    except Exception as e:
        LOGGER.error(
            "Error encoding object to base64 string: %s",
            source_object,
        )
        msg = f"Error encoding object to base64 string: {e}"
        raise ValueError(msg) from e
    if to_bytes:
        return encoded_result.encode("utf-8")
    return encoded_result


def ensure_bytes(input_value: Any, **encoding_kwargs: Any) -> bytes | None:
    """Ensure the input value is converted to bytes."""
    if input_value is None:
        return None
    if isinstance(input_value, bytes):
        return input_value
    return input_value.encode(**encoding_kwargs)
