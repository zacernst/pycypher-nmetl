"""Tests for centralized configuration validation in pycypher.config.

Verifies that:
1. Default values are correct when no env vars are set.
2. Valid env vars are parsed correctly.
3. Invalid env vars produce warnings and fall back to defaults.
4. Negative values are rejected with warnings.

Run with:
    uv run pytest tests/test_config_validation.py -v
"""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest


class TestConfigDefaults:
    """Default values when no environment variables are set."""

    def test_default_query_timeout(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.QUERY_TIMEOUT_S is None

    def test_default_max_cross_join_rows(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_CROSS_JOIN_ROWS == 10_000_000

    def test_default_result_cache_max_mb(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.RESULT_CACHE_MAX_MB == 100

    def test_default_result_cache_ttl(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.RESULT_CACHE_TTL_S == 0.0

    def test_default_max_unbounded_path_hops(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_UNBOUNDED_PATH_HOPS == 20


class TestConfigOverrides:
    """Environment variables override defaults."""

    def test_query_timeout_override(self) -> None:
        with patch.dict("os.environ", {"PYCYPHER_QUERY_TIMEOUT_S": "30.5"}):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.QUERY_TIMEOUT_S == pytest.approx(30.5)

    def test_max_cross_join_override(self) -> None:
        with patch.dict(
            "os.environ", {"PYCYPHER_MAX_CROSS_JOIN_ROWS": "5000"}
        ):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_CROSS_JOIN_ROWS == 5000

    def test_underscore_separator_in_int(self) -> None:
        with patch.dict(
            "os.environ", {"PYCYPHER_MAX_CROSS_JOIN_ROWS": "1_000_000"}
        ):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_CROSS_JOIN_ROWS == 1_000_000


class TestConfigValidation:
    """Invalid values fall back to defaults with warnings."""

    def test_non_numeric_timeout_falls_back(self) -> None:
        with patch.dict("os.environ", {"PYCYPHER_QUERY_TIMEOUT_S": "abc"}):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.QUERY_TIMEOUT_S is None

    def test_non_numeric_int_falls_back(self) -> None:
        with patch.dict("os.environ", {"PYCYPHER_MAX_CROSS_JOIN_ROWS": "xyz"}):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_CROSS_JOIN_ROWS == 10_000_000

    def test_negative_int_falls_back(self) -> None:
        with patch.dict("os.environ", {"PYCYPHER_MAX_CROSS_JOIN_ROWS": "-1"}):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.MAX_CROSS_JOIN_ROWS == 10_000_000

    def test_negative_float_falls_back(self) -> None:
        with patch.dict("os.environ", {"PYCYPHER_QUERY_TIMEOUT_S": "-5.0"}):
            import pycypher.config as cfg

            cfg = importlib.reload(cfg)
            assert cfg.QUERY_TIMEOUT_S is None
