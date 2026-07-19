"""Protocol interface for subquery execution — breaks circular imports.

The ``SubqueryExecutor`` protocol lets evaluators (e.g. ``ExistsEvaluator``)
execute a full subquery against a ``BindingFrame`` without importing
:class:`~pycypher.star.Star` directly. The concrete implementation is
registered onto :class:`~pycypher.relational_models.Context` by
``Star.__init__`` (the composition root).

Usage in sub-evaluators::

    result_df = self.frame.context.subquery_executor.execute(query, frame)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pandas as pd

    from pycypher.ast_models import Query
    from pycypher.binding_frame import BindingFrame


@runtime_checkable
class SubqueryExecutor(Protocol):
    """Minimal interface for executing a subquery against a frame."""

    def execute(self, query: Query, initial_frame: BindingFrame) -> pd.DataFrame:
        """Execute *query* seeded with *initial_frame* and return the result.

        Args:
            query: The Cypher AST subquery to execute.
            initial_frame: The seed :class:`BindingFrame` to execute against.

        Returns:
            A ``pd.DataFrame`` of the subquery's result rows.

        """
        ...
