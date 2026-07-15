"""Phase 1 (out-of-core DuckDB) — spill configuration.

Verifies that DuckDBBackend's opt-in spill settings (memory_limit,
temp_directory, max_temp_directory_size, preserve_insertion_order) are applied
via the injection-safe ``connect(config=...)`` dict, honour environment-variable
fallbacks, and leave DuckDB defaults untouched when unset (no behaviour change).

See docs/duckdb_out_of_core_design.md, Phase 1.
"""

from __future__ import annotations

import re

import pandas as pd
import pytest
from pycypher.backends.duckdb_backend import DuckDBBackend, _spill_config


def _setting(backend: DuckDBBackend, name: str) -> object:
    return backend._conn.execute(
        f"SELECT current_setting('{name}')",
    ).fetchone()[0]


def _to_bytes(mem: str) -> float:
    """Parse a DuckDB memory string like '488.2 MiB' / '4.0 GiB' to bytes."""
    match = re.match(r"([\d.]+)\s*([KMGT]?i?B)", mem)
    assert match, f"unparseable memory string: {mem!r}"
    value = float(match.group(1))
    unit = match.group(2)
    factors = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    return value * factors[unit]


class TestNoConfig:
    def test_empty_config_when_nothing_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "PYCYPHER_DUCKDB_MEMORY_LIMIT",
            "PYCYPHER_DUCKDB_TEMP_DIRECTORY",
            "PYCYPHER_DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
            "PYCYPHER_DUCKDB_PRESERVE_INSERTION_ORDER",
        ):
            monkeypatch.delenv(var, raising=False)
        assert _spill_config(
            memory_limit=None,
            temp_directory=None,
            max_temp_directory_size=None,
            preserve_insertion_order=None,
        ) == {}

    def test_default_backend_still_works(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "PYCYPHER_DUCKDB_MEMORY_LIMIT",
            "PYCYPHER_DUCKDB_TEMP_DIRECTORY",
            "PYCYPHER_DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
            "PYCYPHER_DUCKDB_PRESERVE_INSERTION_ORDER",
        ):
            monkeypatch.delenv(var, raising=False)
        backend = DuckDBBackend()
        # preserve_insertion_order defaults to DuckDB's default (True) when unset.
        assert _setting(backend, "preserve_insertion_order") is True
        backend.close()


class TestExplicitConfig:
    def test_memory_limit_applied(self) -> None:
        backend = DuckDBBackend(memory_limit="512MB")
        got = _to_bytes(str(_setting(backend, "memory_limit")))
        # 512 MB == 512e6 bytes; allow rounding to MiB.
        assert abs(got - 512e6) / 512e6 < 0.05
        backend.close()

    def test_temp_directory_applied(self, tmp_path) -> None:
        target = str(tmp_path / "spill")
        backend = DuckDBBackend(temp_directory=target)
        assert _setting(backend, "temp_directory") == target
        backend.close()

    def test_max_temp_directory_size_applied(self) -> None:
        backend = DuckDBBackend(max_temp_directory_size="7GB")
        got = str(_setting(backend, "max_temp_directory_size"))
        assert "90%" not in got  # changed from the default
        backend.close()

    def test_preserve_insertion_order_false(self) -> None:
        backend = DuckDBBackend(preserve_insertion_order=False)
        assert _setting(backend, "preserve_insertion_order") is False
        backend.close()


class TestEnvFallback:
    def test_env_var_used_when_arg_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_MEMORY_LIMIT", "256MB")
        backend = DuckDBBackend()
        got = _to_bytes(str(_setting(backend, "memory_limit")))
        assert abs(got - 256e6) / 256e6 < 0.05
        backend.close()

    def test_explicit_arg_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_MEMORY_LIMIT", "256MB")
        backend = DuckDBBackend(memory_limit="512MB")
        got = _to_bytes(str(_setting(backend, "memory_limit")))
        assert abs(got - 512e6) / 512e6 < 0.05
        backend.close()

    def test_preserve_insertion_order_env_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_PRESERVE_INSERTION_ORDER", "false")
        backend = DuckDBBackend()
        assert _setting(backend, "preserve_insertion_order") is False
        backend.close()

    def test_invalid_bool_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_PRESERVE_INSERTION_ORDER", "maybe")
        with pytest.raises(ValueError, match="Invalid boolean"):
            DuckDBBackend()


class TestFunctionalWithSpillConfig:
    def test_query_correct_with_low_memory_limit(self, tmp_path) -> None:
        # With a low memory_limit and a temp_directory set, normal operations
        # still return correct results (spilling transparently if needed).
        backend = DuckDBBackend(
            memory_limit="512MB",
            temp_directory=str(tmp_path / "spill"),
        )
        left = pd.DataFrame({"__ID__": range(1000), "v": range(1000)})
        right = pd.DataFrame({"__ID__": range(500, 1500), "w": range(1000)})
        out = backend.join(left, right, on="__ID__")
        assert len(out) == 500
        agg = backend.aggregate(out, [], {"total": ("v", "sum")})
        assert agg["total"].iloc[0] == sum(range(500, 1000))
        backend.close()
