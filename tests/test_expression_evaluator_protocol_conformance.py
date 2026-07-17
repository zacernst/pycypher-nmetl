"""Conformance tests for ExpressionEvaluatorProtocol implementations.

Parameterized over every concrete class that claims to satisfy
``ExpressionEvaluatorProtocol`` (currently just ``BindingExpressionEvaluator``),
so a future second implementation is checked against the same contract for
free by adding it to ``EVALUATOR_FACTORIES`` below.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import IntegerLiteral, Variable
from pycypher.binding_evaluator import BindingExpressionEvaluator
from pycypher.binding_frame import BindingFrame
from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)

ID_COLUMN = "__ID__"


def _make_frame(n: int = 4) -> BindingFrame:
    """Create a minimal BindingFrame with *n* rows and a Person entity table."""
    df = pd.DataFrame(
        {
            "p": range(1, n + 1),
            ID_COLUMN: range(1, n + 1),
        },
    )
    person_table = EntityTable.from_dataframe(
        "Person",
        pd.DataFrame(
            {
                ID_COLUMN: range(1, n + 1),
                "name": [f"Person{i}" for i in range(1, n + 1)],
                "age": [20 + i for i in range(1, n + 1)],
            },
        ),
    )
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return BindingFrame(
        bindings=df,
        context=context,
        type_registry={"p": "Person"},
    )


# Concrete implementations of ExpressionEvaluatorProtocol, keyed by name for
# readable test IDs. Add new implementations here to run them through the
# same conformance suite.
EVALUATOR_FACTORIES = {
    "BindingExpressionEvaluator": BindingExpressionEvaluator,
}


@pytest.fixture(params=list(EVALUATOR_FACTORIES), ids=list(EVALUATOR_FACTORIES))
def evaluator(request: pytest.FixtureRequest) -> ExpressionEvaluatorProtocol:
    factory = EVALUATOR_FACTORIES[request.param]
    return factory(_make_frame(3))


class TestProtocolConformance:
    """Structural and behavioral checks any ExpressionEvaluatorProtocol
    implementation must satisfy."""

    def test_satisfies_runtime_checkable_protocol(
        self, evaluator: ExpressionEvaluatorProtocol,
    ) -> None:
        assert isinstance(evaluator, ExpressionEvaluatorProtocol)

    def test_frame_property_returns_binding_frame(
        self, evaluator: ExpressionEvaluatorProtocol,
    ) -> None:
        assert isinstance(evaluator.frame, BindingFrame)

    def test_evaluate_literal_returns_series_of_correct_length(
        self, evaluator: ExpressionEvaluatorProtocol,
    ) -> None:
        result = evaluator.evaluate(IntegerLiteral(value=42))
        assert list(result) == [42, 42, 42]

    def test_evaluate_known_variable_returns_bound_values(
        self, evaluator: ExpressionEvaluatorProtocol,
    ) -> None:
        result = evaluator.evaluate(Variable(name="p"))
        assert list(result) == [1, 2, 3]
