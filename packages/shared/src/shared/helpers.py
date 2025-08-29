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


def ensure_uri(uri: str | ParseResult | Path) -> ParseResult:
    """
    Ensure that the URI is parsed.

    Args:
        uri: The URI to ensure is parsed

    Returns:
        The URI as a ``ParseResult``
    """
    if isinstance(uri, ParseResult):
        pass
    elif isinstance(uri, str):
        uri = urlparse(uri)
    elif isinstance(uri, Path):
        uri = urlparse(uri.as_uri())
    else:
        raise ValueError(
            f"URI must be a string or ParseResult, not {type(uri)}"
        )
    LOGGER.debug("URI converted: %s", uri)
    return uri


def decode(encoded: str) -> Any:
    """Decode a base64 encoded pickled object.
    
    Args:
        encoded: Base64 encoded string containing pickled object data.
        
    Returns:
        The decoded Python object.
        
    Raises:
        ValueError: If decoding fails due to invalid data.
    """
    try:
        decoded = pickle.loads(base64.b64decode(encoded))
    except Exception as e:
        raise ValueError(f"Error decoding base64 string: {e}") from e
    return decoded


def encode(obj: Any, to_bytes: bool = False) -> str | bytes:
    """Encode a Python object as a base64 string.
    
    Args:
        obj: Python object to encode.
        to_bytes: If True, return bytes instead of string.
        
    Returns:
        Base64 encoded representation as string or bytes.
        
    Raises:
        ValueError: If encoding fails due to unpickleable object.
    """
    try:
        encoded = base64.b64encode(pickle.dumps(obj)).decode("utf-8")
    except Exception as e:
        LOGGER.error("Error encoding object to base64 string: %s", obj)
        raise ValueError(f"Error encoding object to base64 string: {e}") from e
    if to_bytes:
        return encoded.encode("utf-8")
    return encoded


def ensure_bytes(value: Any, **kwargs) -> bytes:
    """Convert a value to bytes if it isn't already.
    
    This is a utility function to ensure consistent byte representation
    of values. Should be removed once proper type checking is in place.
    
    Args:
        value: Value to convert to bytes.
        **kwargs: Additional arguments passed to encode() method.
        
    Returns:
        Byte representation of the value, or None if value is None.
    """
    if value is None:
        return
    elif isinstance(value, bytes):
        return value
    return value.encode(**kwargs)
