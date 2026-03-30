"""Multi-query executor — the user-facing API for query composition.

Orchestrates all multi-query components into a single entry point:

1. **Input validation** — validates query pairs before processing
2. **Dependency analysis** — determines execution order from AST analysis
3. **Query combination** — merges queries with ``WITH *`` and RETURN stripping
4. **Execution** — runs the combined query via :meth:`Star.execute_query`

Provides both full-pipeline execution and individual stage access
(validate, analyze, combine) for debugging and testing.

Example::

    from pycypher.multi_query_executor import MultiQueryExecutor
    from pycypher.relational_models import Context
    from pycypher.star import Star

    ctx = Context()
    star = Star(context=ctx)
    executor = MultiQueryExecutor()

    result = executor.execute_multi_query([
        ("q1", "CREATE (n:Person {name: 'Alice'})"),
        ("q2", "MATCH (n:Person) RETURN n.name"),
    ], star)

"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

import pandas as pd
from shared.logger import LOGGER, set_query_id, reset_query_id
from shared.otel import trace_phase, trace_query

from pycypher.input_validator import InputValidationResult, InputValidator
from pycypher.multi_query_analyzer import (
    DependencyGraph,
    QueryDependencyAnalyzer,
)
from pycypher.query_combiner import QueryCombiner

if TYPE_CHECKING:
    from pycypher.star import Star


class MultiQueryExecutor:
    """Orchestrates multi-query composition and execution.

    Integrates input validation, dependency analysis, query combination,
    and execution into a single API.  Each stage is also accessible
    individually for debugging and testing.
    """

    def __init__(self) -> None:
        """Initialise the executor with internal components."""
        self._validator = InputValidator()
        self._analyzer = QueryDependencyAnalyzer()
        self._combiner = QueryCombiner()

    # ------------------------------------------------------------------
    # Individual stage access
    # ------------------------------------------------------------------

    def validate(
        self,
        queries: list[tuple[str, str]],
    ) -> InputValidationResult:
        """Validate inputs without executing.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            :class:`~pycypher.input_validator.InputValidationResult`.

        """
        return self._validator.validate(queries)

    def analyze(
        self,
        queries: list[tuple[str, str]],
    ) -> DependencyGraph:
        """Analyze dependencies without executing.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            :class:`~pycypher.multi_query_analyzer.DependencyGraph`.

        """
        return self._analyzer.analyze(queries)

    def combine(
        self,
        queries: list[tuple[str, str]],
    ) -> str:
        """Combine queries into a single Cypher string without executing.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.

        Returns:
            Combined Cypher query string.

        """
        return self._combiner.combine(queries)

    # ------------------------------------------------------------------
    # Full pipeline execution
    # ------------------------------------------------------------------

    def execute_multi_query(
        self,
        queries: list[tuple[str, str]],
        star: Star,
    ) -> pd.DataFrame:
        """Execute multiple queries as a single combined query.

        Pipeline: validate → analyze → combine → execute.

        Args:
            queries: List of ``(query_id, cypher_string)`` pairs.
            star: :class:`~pycypher.star.Star` instance for execution.

        Returns:
            Result DataFrame from combined query execution.

        Raises:
            ValueError: If input validation fails.

        """
        if not queries:
            LOGGER.debug("multi-query execute called with empty query list")
            return pd.DataFrame()

        query_ids = [qid for qid, _ in queries]
        pipeline_id = uuid.uuid4().hex[:12]
        qid_token = set_query_id(pipeline_id)
        t0 = time.perf_counter()

        LOGGER.info(
            "pipeline: start  pipeline_id=%s  queries=%d  ids=[%s]",
            pipeline_id,
            len(queries),
            ", ".join(query_ids),
        )

        with trace_query(
            f"PIPELINE({len(queries)} queries)",
            query_id=pipeline_id,
        ) as span:
            span.set_attribute("pycypher.pipeline_id", pipeline_id)
            span.set_attribute("pycypher.pipeline_query_count", len(queries))
            span.set_attribute("pycypher.pipeline_query_ids", query_ids)

            try:
                # Step 1: Validate inputs
                with trace_phase("validate", query_id=pipeline_id) as vspan:
                    t_validate = time.perf_counter()
                    validation = self._validator.validate(queries)
                    validate_ms = (time.perf_counter() - t_validate) * 1000.0
                    vspan.set_attribute("pycypher.validate_time_ms", round(validate_ms, 2))

                if not validation.is_valid:
                    elapsed = time.perf_counter() - t0
                    LOGGER.warning(
                        "pipeline: validation failed  pipeline_id=%s  errors=%d  elapsed=%.3fs",
                        pipeline_id,
                        len(validation.errors),
                        elapsed,
                    )
                    span.set_attribute("pycypher.pipeline_status", "validation_error")
                    msg = "Input validation failed:\n" + "\n".join(
                        f"  - {e}" for e in validation.errors
                    )
                    raise ValueError(msg)

                LOGGER.debug(
                    "pipeline: validation passed  queries=%d  elapsed=%.1fms",
                    len(queries),
                    validate_ms,
                )

                # Step 2: Combine (includes dependency analysis + topological sort)
                with trace_phase("combine", query_id=pipeline_id) as cspan:
                    t_combine = time.perf_counter()
                    combined_cypher = self._combiner.combine(queries)
                    combine_ms = (time.perf_counter() - t_combine) * 1000.0
                    cspan.set_attribute("pycypher.combine_time_ms", round(combine_ms, 2))
                    cspan.set_attribute("pycypher.combined_query_length", len(combined_cypher))

                LOGGER.debug(
                    "pipeline: combined  chars=%d  elapsed=%.1fms",
                    len(combined_cypher),
                    combine_ms,
                )

                # Step 3: Execute via existing Star path
                # (Star.execute_query already has its own metrics/audit/OTel)
                with trace_phase("execute", query_id=pipeline_id):
                    t_execute = time.perf_counter()
                    result = star.execute_query(combined_cypher)
                    execute_ms = (time.perf_counter() - t_execute) * 1000.0

                elapsed = time.perf_counter() - t0
                nrows = len(result) if isinstance(result, pd.DataFrame) else 0

                LOGGER.info(
                    "pipeline: done  pipeline_id=%s  queries=%d  rows=%d  "
                    "validate=%.1fms  combine=%.1fms  execute=%.1fms  total=%.3fs",
                    pipeline_id,
                    len(queries),
                    nrows,
                    validate_ms,
                    combine_ms,
                    execute_ms,
                    elapsed,
                )

                span.set_attribute("pycypher.pipeline_status", "ok")
                span.set_attribute("result.rows", nrows)
                span.set_attribute("pycypher.elapsed_ms", round(elapsed * 1000.0, 2))
                span.set_attribute("pycypher.validate_time_ms", round(validate_ms, 2))
                span.set_attribute("pycypher.combine_time_ms", round(combine_ms, 2))
                span.set_attribute("pycypher.execute_time_ms", round(execute_ms, 2))

                return result

            except Exception as exc:
                elapsed = time.perf_counter() - t0
                LOGGER.error(
                    "pipeline: failed  pipeline_id=%s  queries=%d  "
                    "error=%s  elapsed=%.3fs",
                    pipeline_id,
                    len(queries),
                    type(exc).__name__,
                    elapsed,
                )
                span.set_attribute("pycypher.pipeline_status", "error")
                span.set_attribute("pycypher.error_type", type(exc).__name__)
                span.set_attribute("pycypher.elapsed_ms", round(elapsed * 1000.0, 2))
                raise

            finally:
                reset_query_id(qid_token)
