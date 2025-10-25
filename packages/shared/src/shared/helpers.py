"""Shared utility functions used across the PyCypher-NMETL project.

This module provides common helper functions for URI handling, encoding/decoding
objects, and data type conversions that are used throughout the project.
"""

from __future__ import annotations

import base64
import pickle
from pathlib import Path
from typing import Any
from urllib.parse import ParseResult, urlparse

from shared.logger import LOGGER


def ensure_uri(uri_input: str | ParseResult | Path) -> ParseResult:
    """Ensure input is converted to a ParseResult URI object."""
    if isinstance(uri_input, ParseResult):
        pass
    elif isinstance(uri_input, str):
        uri_input = urlparse(uri_input)
    elif isinstance(uri_input, Path):
        uri_input = urlparse(uri_input.as_uri())
    else:
        raise ValueError(
            f"URI must be a string or ParseResult, not {type(uri_input)}"
        )
    LOGGER.debug("URI converted: %s", uri_input)
    return uri_input


def decode(encoded_data: str) -> Any:
    """Decode a base64 encoded pickled object.

    Args:
        encoded_data: Base64 encoded string containing pickled object data.

    Returns:
        The decoded Python object.

    Raises:
        ValueError: If decoding fails due to invalid data.
    """
    try:
        decoded_object = pickle.loads(base64.b64decode(encoded_data))
    except Exception as e:
        raise ValueError(f"Error decoding base64 string: {e}") from e
    return decoded_object


def encode(source_object: Any, to_bytes: bool = False) -> str | bytes:
    """Encode a Python object as a base64 string.

    Args:
        source_object: Python object to encode.
        to_bytes: If True, return bytes instead of string.

    Returns:
        Base64 encoded representation as string or bytes.

    Raises:
        ValueError: If encoding fails due to unpickleable object.
    """
    try:
        encoded_result = base64.b64encode(pickle.dumps(source_object)).decode("utf-8")
    except Exception as e:
        LOGGER.error("Error encoding object to base64 string: %s", source_object)
        raise ValueError(f"Error encoding object to base64 string: {e}") from e
    if to_bytes:
        return encoded_result.encode("utf-8")
    return encoded_result


def ensure_bytes(input_value: Any, **encoding_kwargs) -> bytes:
    """Ensure the input value is converted to bytes."""
    if input_value is None:
        return
    elif isinstance(input_value, bytes):
        return input_value
    return input_value.encode(**encoding_kwargs)
