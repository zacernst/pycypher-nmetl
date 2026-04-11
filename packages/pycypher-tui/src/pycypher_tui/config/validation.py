"""Cached validation for TUI real-time feedback.

Wraps :mod:`pycypher.ingestion.validation` with caching to avoid
repeated expensive validation on unchanged configurations.  The TUI
calls validation on every keystroke; caching ensures sub-millisecond
responses for unchanged configs.
"""

from __future__ import annotations

from typing import Any

from pycypher.ingestion.config import PipelineConfig
from pycypher.ingestion.validation import (
    ValidationResult,
    validate_config,
)
from pycypher.ingestion.validation import (
    validate_field as _validate_field,
)

__all__ = ["CachedValidator"]


class CachedValidator:
    """Validation wrapper with config-change-aware caching.

    Caches the last :class:`ValidationResult` and returns it if the
    config has not changed (compared by identity of the serialised form).
    """

    def __init__(self) -> None:
        self._last_key: str | None = None
        self._last_result: ValidationResult | None = None

    def _config_key(self, config: PipelineConfig) -> str:
        """Produce a cache key from a config (cheap hash of serialised form)."""
        return config.model_dump_json()

    def validate(self, config: PipelineConfig) -> ValidationResult:
        """Validate the config, returning a cached result if unchanged.

        Args:
            config: The pipeline configuration to validate.

        Returns:
            A :class:`ValidationResult` (may be the cached instance).
        """
        key = self._config_key(config)
        if key == self._last_key and self._last_result is not None:
            return self._last_result

        result = validate_config(config)
        self._last_key = key
        self._last_result = result
        return result

    def validate_field(self, field_path: str, value: Any) -> ValidationResult:
        """Validate a single field incrementally (no caching — already fast).

        Args:
            field_path: Dotted field path.
            value: The field value to check.

        Returns:
            A :class:`ValidationResult` with at most one error.
        """
        return _validate_field(field_path, value)

    def clear_cache(self) -> None:
        """Invalidate the cached result."""
        self._last_key = None
        self._last_result = None
