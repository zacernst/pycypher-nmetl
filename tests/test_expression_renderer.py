"""TDD tests for ExpressionRenderer — visitor-pattern replacement for Star._expr_display_text.

The ExpressionRenderer handles human-readable text generation from Cypher AST
expression nodes, used for auto-naming RETURN columns when no explicit AS alias
is present.

Run with:
    uv run pytest tests/test_expression_renderer_tdd.py -v
"""

from pycypher.ast_models import (
    And,
    Arithmetic,
    BooleanLiteral,
    CaseExpression,
    Comparison,
    CountStar,
    FloatLiteral,
    FunctionInvocation,
    IndexLookup,
    IntegerLiteral,
    LabelPredicate,
    ListComprehension,
    Not,
    NullCheck,
    NullLiteral,
    Or,
    Parameter,
    PropertyLookup,
    Reduce,
    Slicing,
    StringLiteral,
    Unary,
    Variable,
    Xor,
)
from pycypher.expression_renderer import ExpressionRenderer


class TestExpressionRendererBasicLiterals:
    """Test rendering of literal expression types."""

    def setup_method(self) -> None:
        self.renderer = ExpressionRenderer()

    def test_variable(self) -> None:
        assert self.renderer.render(Variable(name="n")) == "n"

    def test_property_lookup(self) -> None:
        expr = PropertyLookup(expression=Variable(name="n"), property="name")
        assert self.renderer.render(expr) == "name"

    def test_integer_literal(self) -> None:
        assert self.renderer.render(IntegerLiteral(value=42)) == "42"

    def test_float_literal(self) -> None:
        assert self.renderer.render(FloatLiteral(value=3.14)) == "3.14"

    def test_string_literal(self) -> None:
        assert self.renderer.render(StringLiteral(value="hello")) == "hello"

    def test_boolean_literal_true(self) -> None:
        assert self.renderer.render(BooleanLiteral(value=True)) == "true"

    def test_boolean_literal_false(self) -> None:
        assert self.renderer.render(BooleanLiteral(value=False)) == "false"

    def test_null_literal(self) -> None:
        assert self.renderer.render(NullLiteral()) == "null"

    def test_parameter(self) -> None:
        assert self.renderer.render(Parameter(name="age")) == "$age"

    def test_count_star(self) -> None:
        assert self.renderer.render(CountStar()) == "count(*)"


class TestExpressionRendererFunctions:
    """Test rendering of function invocations."""

    def setup_method(self) -> None:
        self.renderer = ExpressionRenderer()

    def test_function_with_dict_args(self) -> None:
        expr = FunctionInvocation(
            name="count",
            arguments={"arguments": [Variable(name="n")], "distinct": False},
        )
        assert self.renderer.render(expr) == "count(n)"

    def test_function_no_args(self) -> None:
        expr = FunctionInvocation(
            name="rand",
            arguments={"arguments": []},
        )
        assert self.renderer.render(expr) == "rand()"

    def test_function_none_args(self) -> None:
        expr = FunctionInvocation(
            name="rand",
            arguments=None,
        )
        assert self.renderer.render(expr) == "rand()"


class TestExpressionRendererOperators:
    """Test rendering of operator expressions."""

    def setup_method(self) -> None:
        self.renderer = ExpressionRenderer()

    def test_arithmetic(self) -> None:
        expr = Arithmetic(
            operator="+",
            left=Variable(name="a"),
            right=IntegerLiteral(value=1),
        )
        assert self.renderer.render(expr) == "a + 1"

    def test_comparison(self) -> None:
        expr = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="n"), property="age"),
            right=IntegerLiteral(value=30),
        )
        assert self.renderer.render(expr) == "age > 30"

    def test_null_check(self) -> None:
        expr = NullCheck(
            operand=Variable(name="x"),
            operator="IS NULL",
        )
        assert self.renderer.render(expr) == "x IS NULL"

    def test_not(self) -> None:
        expr = Not(operand=Variable(name="flag"))
        assert self.renderer.render(expr) == "NOT flag"

    def test_and(self) -> None:
        expr = And(operands=[Variable(name="a"), Variable(name="b")])
        assert self.renderer.render(expr) == "a AND b"

    def test_or(self) -> None:
        expr = Or(operands=[Variable(name="a"), Variable(name="b")])
        assert self.renderer.render(expr) == "a OR b"

    def test_xor(self) -> None:
        expr = Xor(operands=[Variable(name="a"), Variable(name="b")])
        assert self.renderer.render(expr) == "a XOR b"

    def test_unary_negation(self) -> None:
        expr = Unary(operator="-", operand=Variable(name="x"))
        assert self.renderer.render(expr) == "-x"

    def test_unary_none_operand(self) -> None:
        expr = Unary(operator="-", operand=None)
        assert self.renderer.render(expr) is None

    def test_label_predicate(self) -> None:
        expr = LabelPredicate(
            operand=Variable(name="n"),
            labels=["Person", "Employee"],
        )
        assert self.renderer.render(expr) == "n:Person:Employee"


class TestExpressionRendererComplex:
    """Test rendering of complex expression types."""

    def setup_method(self) -> None:
        self.renderer = ExpressionRenderer()

    def test_index_lookup(self) -> None:
        expr = IndexLookup(
            expression=Variable(name="list"),
            index=IntegerLiteral(value=0),
        )
        assert self.renderer.render(expr) == "list[0]"

    def test_slicing(self) -> None:
        expr = Slicing(
            expression=Variable(name="list"),
            start=IntegerLiteral(value=1),
            end=IntegerLiteral(value=3),
        )
        assert self.renderer.render(expr) == "list[1..3]"

    def test_slicing_no_start(self) -> None:
        expr = Slicing(
            expression=Variable(name="list"),
            start=None,
            end=IntegerLiteral(value=3),
        )
        assert self.renderer.render(expr) == "list[..3]"

    def test_list_comprehension_with_map(self) -> None:
        expr = ListComprehension(
            variable=Variable(name="x"),
            list_expr=Variable(name="items"),
            map_expr=Arithmetic(
                operator="*",
                left=Variable(name="x"),
                right=IntegerLiteral(value=2),
            ),
        )
        assert self.renderer.render(expr) == "[x IN items | x * 2]"

    def test_list_comprehension_without_map(self) -> None:
        expr = ListComprehension(
            variable=Variable(name="x"),
            list_expr=Variable(name="items"),
        )
        assert self.renderer.render(expr) == "[x IN items]"

    def test_case_expression(self) -> None:
        expr = CaseExpression()
        assert self.renderer.render(expr) == "case"

    def test_reduce(self) -> None:
        expr = Reduce(
            accumulator=Variable(name="total"),
            initial=IntegerLiteral(value=0),
            variable=Variable(name="x"),
            list_expr=Variable(name="nums"),
            expression=Arithmetic(
                operator="+",
                left=Variable(name="total"),
                right=Variable(name="x"),
            ),
        )
        assert self.renderer.render(expr) == "reduce(total, nums)"

    def test_unknown_type_returns_none(self) -> None:
        """Unrecognized expression types should return None."""
        assert self.renderer.render("some_unknown_thing") is None


class TestExpressionRendererIntegrationWithStar:
    """Verify ExpressionRenderer produces identical results to Star._expr_display_text."""

    def test_renderer_matches_star_for_property_lookup(self) -> None:
        """ExpressionRenderer should return same result as Star._expr_display_text."""
        import pandas as pd
        from pycypher import Star
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )

        person_df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"]})
        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": EntityTable.from_dataframe("Person", person_df),
                },
            ),
        )
        star = Star(context=ctx)

        renderer = ExpressionRenderer()
        expr = PropertyLookup(expression=Variable(name="n"), property="name")

        # Both should produce "name"
        assert star._renderer.render(expr) == renderer.render(expr)

    def test_renderer_matches_star_for_function(self) -> None:
        import pandas as pd
        from pycypher import Star
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )

        person_df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"]})
        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": EntityTable.from_dataframe("Person", person_df),
                },
            ),
        )
        star = Star(context=ctx)

        renderer = ExpressionRenderer()
        expr = FunctionInvocation(
            name="count",
            arguments={"arguments": [Variable(name="n")]},
        )

        assert star._renderer.render(expr) == renderer.render(expr)
