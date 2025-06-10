"""Place for functions that might be used across the project."""

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
    """Decode a base64 encoded string."""
    try:
        decoded = pickle.loads(base64.b64decode(encoded))
    except Exception as e:
        raise ValueError(f"Error decoding base64 string: {e}") from e
    return decoded


def encode(obj: Any, to_bytes: bool = False) -> str | bytes:
    """Encode an object as a base64 string."""
    try:
        encoded = base64.b64encode(pickle.dumps(obj)).decode("utf-8")
    except Exception as e:
        LOGGER.error("Error encoding object to base64 string: %s", obj)
        raise ValueError(f"Error encoding object to base64 string: {e}") from e
    if to_bytes:
        return encoded.encode("utf-8")
    return encoded


def ensure_bytes(value: Any, **kwargs) -> bytes:
    """Change the value to a bytestring if it isn't one already. We should be able
    to get rid of this eventually after everything's been typechecked correctly.
    """
    if isinstance(value, bytes):
        return value
    return value.encode(**kwargs)
