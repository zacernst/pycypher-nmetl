"""Phase 6 — Spark backend registration and selection.

These tests do not need a running Spark cluster for the registry/config
assertions; only the ones that actually instantiate a backend require
PySpark (guarded by the ``spark_session`` fixture / ``spark`` marker).
"""

from __future__ import annotations

import pytest
from pycypher.backend_engine import (
    _BACKEND_FACTORIES,
    _FALLBACK_CHAIN,
    SparkBackend,
    select_backend,
)
from pycypher.ingestion.config import PipelineConfig


class TestRegistry:
    def test_spark_in_factories(self) -> None:
        assert _BACKEND_FACTORIES["spark"] is SparkBackend

    def test_spark_excluded_from_fallback_chain(self) -> None:
        # auto-selection must never pick Spark.
        assert "spark" not in _FALLBACK_CHAIN

    def test_unknown_hint_still_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend hint"):
            select_backend(hint="bogus")


class TestConfig:
    def test_config_accepts_spark(self) -> None:
        cfg = PipelineConfig(version="1.0", backend_engine="spark")
        assert cfg.backend_engine == "spark"

    def test_config_rejects_unknown_backend(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PipelineConfig(version="1.0", backend_engine="hadoop")


class TestSelection:
    @pytest.mark.spark
    def test_select_backend_spark(self, spark_session) -> None:
        backend = select_backend(hint="spark")
        assert backend.name == "spark"

    @pytest.mark.spark
    def test_context_builds_spark_backend(self, spark_session) -> None:
        import pandas as pd
        from pycypher.ingestion.context_builder import ContextBuilder

        people = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        ctx = ContextBuilder().add_entity("Person", people, id_col="id").build(
            backend="spark",
        )
        assert ctx.backend_name == "spark"
        assert ctx.backend.name == "spark"
