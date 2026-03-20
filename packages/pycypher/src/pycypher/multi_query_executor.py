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

from typing import TYPE_CHECKING

import pandas as pd

from pycypher.input_validator import InputValidationResult, InputValidator
from pycypher.multi_query_analyzer import (
    DependencyGraph,
    QueryDependencyAnalyzer,
)
from pycypher.query_combiner import QueryCombiner
from shared.logger import LOGGER

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
        LOGGER.info(
            "multi-query execute: %d queries [%s]",
            len(queries),
            ", ".join(query_ids),
        )

        # Step 1: Validate inputs
        validation = self._validator.validate(queries)
        if not validation.is_valid:
            LOGGER.warning(
                "multi-query validation failed: %d errors",
                len(validation.errors),
            )
            msg = "Input validation failed:\n" + "\n".join(
                f"  - {e}" for e in validation.errors
            )
            raise ValueError(msg)
        LOGGER.debug("multi-query validation passed for %d queries", len(queries))

        # Step 2: Combine (includes dependency analysis + topological sort)
        combined_cypher = self._combiner.combine(queries)
        LOGGER.debug(
            "multi-query combined into %d-char Cypher string",
            len(combined_cypher),
        )

        # Step 3: Execute via existing Star path
        result = star.execute_query(combined_cypher)
        LOGGER.info(
            "multi-query execute complete: %d rows returned",
            len(result),
        )
        return result
