"""Protocol interface for expression evaluation — breaks circular imports.

The ``ExpressionEvaluatorProtocol`` defines the minimal contract that
sub-evaluators (collection, exists, etc.) need from the top-level
expression evaluator.  By depending on this protocol instead of the
concrete :class:`~pycypher.binding_evaluator.BindingExpressionEvaluator`,
circular import chains are eliminated.

Usage in sub-evaluators::

    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol

    def some_method(self, evaluator: ExpressionEvaluatorProtocol) -> ...:
        result = evaluator.evaluate(expr)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pycypher.ast_models import Expression
    from pycypher.binding_frame import BindingFrame
    from pycypher.cypher_types import FrameSeries


@runtime_checkable
class ExpressionEvaluatorProtocol(Protocol):
    """Minimal interface for recursive expression evaluation.

    Any evaluator that provides ``evaluate()`` and ``frame`` satisfies
    this protocol, including :class:`BindingExpressionEvaluator`.
    """

    @property
    def frame(self) -> BindingFrame:
        """The :class:`BindingFrame` against which expressions are evaluated."""
        ...

    def evaluate(self, expression: Expression) -> FrameSeries:
        """Evaluate an AST expression and return a per-row Series.

        Args:
            expression: The Cypher AST expression node to evaluate.

        Returns:
            A ``pd.Series`` of per-row results.

        """
        ...


class ExpressionEvaluatorFactory(Protocol):
    """Factory protocol for constructing evaluators from a BindingFrame.

    Sub-evaluators that need to create new evaluators for sub-frames
    (e.g. list comprehension, REDUCE) depend on this factory instead
    of importing the concrete class.
    """

    def __call__(self, frame: BindingFrame) -> ExpressionEvaluatorProtocol:
        """Create an evaluator bound to the given frame.

        Args:
            frame: The :class:`BindingFrame` to bind the evaluator to.

        Returns:
            An :class:`ExpressionEvaluatorProtocol` instance.

        """
        ...
