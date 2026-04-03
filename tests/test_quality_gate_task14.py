"""Quality gate tests for Task #14: CypherASTTransformer migration.

These tests validate that the CompositeTransformer correctly delegates
all grammar rule methods to specialized transformers, with no fallback
to CypherASTTransformer needed. They verify:

1. CompositeTransformer delegation completeness (all 169 methods)
2. No fallback to CypherASTTransformer
3. Method caching behavior
4. Thread safety of delegation
5. Grammar rule coverage across all 5 mixins
6. Regression: key Cypher constructs produce correct AST
"""

from __future__ import annotations

import threading

import pytest

from pycypher.grammar_rule_mixins import (
    ClauseRulesMixin,
    ExpressionRulesMixin,
    FunctionRulesMixin,
    LiteralRulesMixin,
    PatternRulesMixin,
)
from pycypher.grammar_transformers import (
    CompositeTransformer,
    ExpressionTransformer,
    FunctionTransformer,
    LiteralTransformer,
    PatternTransformer,
    StatementTransformer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_MIXINS = [
    LiteralRulesMixin,
    ExpressionRulesMixin,
    FunctionRulesMixin,
    PatternRulesMixin,
    ClauseRulesMixin,
]

TRANSFORMER_MAP = {
    LiteralRulesMixin: LiteralTransformer,
    ExpressionRulesMixin: ExpressionTransformer,
    FunctionRulesMixin: FunctionTransformer,
    PatternRulesMixin: PatternTransformer,
    ClauseRulesMixin: StatementTransformer,
}


def _mixin_methods(mixin_cls) -> list[str]:
    """Return public method names defined on a mixin class."""
    return [
        name for name in dir(mixin_cls)
        if not name.startswith("_") and callable(getattr(mixin_cls, name))
    ]


def _all_mixin_methods() -> list[str]:
    """Return all public method names across all mixins."""
    methods = []
    for mixin in ALL_MIXINS:
        methods.extend(_mixin_methods(mixin))
    return methods


# ===========================================================================
# 1. Delegation completeness
# ===========================================================================


class TestDelegationCompleteness:
    """Verify every mixin method is resolvable through CompositeTransformer."""

    def test_all_mixin_methods_resolvable(self):
        """Every public method from all 5 mixins must be resolvable."""
        ct = CompositeTransformer()
        missing = []
        for method_name in _all_mixin_methods():
            try:
                getattr(ct, method_name)
            except AttributeError:
                missing.append(method_name)
        assert missing == [], f"Methods not resolvable via CompositeTransformer: {missing}"

    def test_literal_methods_delegate_to_literal_transformer(self):
        ct = CompositeTransformer()
        for method_name in _mixin_methods(LiteralRulesMixin):
            method = getattr(ct, method_name)
            assert isinstance(method.__self__, LiteralTransformer), (
                f"{method_name} should delegate to LiteralTransformer"
            )

    def test_expression_methods_delegate_to_expression_transformer(self):
        ct = CompositeTransformer()
        for method_name in _mixin_methods(ExpressionRulesMixin):
            method = getattr(ct, method_name)
            assert isinstance(method.__self__, ExpressionTransformer), (
                f"{method_name} should delegate to ExpressionTransformer"
            )

    def test_function_methods_delegate_to_function_transformer(self):
        ct = CompositeTransformer()
        for method_name in _mixin_methods(FunctionRulesMixin):
            method = getattr(ct, method_name)
            assert isinstance(method.__self__, FunctionTransformer), (
                f"{method_name} should delegate to FunctionTransformer"
            )

    def test_pattern_methods_delegate_to_pattern_transformer(self):
        ct = CompositeTransformer()
        for method_name in _mixin_methods(PatternRulesMixin):
            method = getattr(ct, method_name)
            assert isinstance(method.__self__, PatternTransformer), (
                f"{method_name} should delegate to PatternTransformer"
            )

    def test_clause_methods_delegate_to_statement_transformer(self):
        ct = CompositeTransformer()
        for method_name in _mixin_methods(ClauseRulesMixin):
            method = getattr(ct, method_name)
            assert isinstance(method.__self__, StatementTransformer), (
                f"{method_name} should delegate to StatementTransformer"
            )

    def test_total_method_count_at_least_169(self):
        """Regression guard: total delegated methods should be >= 169."""
        total = sum(len(_mixin_methods(m)) for m in ALL_MIXINS)
        assert total >= 169


# ===========================================================================
# 2. No fallback to CypherASTTransformer
# ===========================================================================


class TestNoFallbackRequired:
    """Verify CompositeTransformer never needs CypherASTTransformer fallback."""

    def test_composite_has_no_fallback_attribute(self):
        """CompositeTransformer should not have a set_fallback_transformer method."""
        ct = CompositeTransformer()
        assert not hasattr(ct, "set_fallback_transformer")

    def test_composite_delegates_have_no_fallback(self):
        ct = CompositeTransformer()
        for delegate in ct._delegates:
            assert not hasattr(delegate, "_fallback"), (
                f"{type(delegate).__name__} should not have _fallback"
            )

    def test_unknown_method_raises_attribute_error(self):
        ct = CompositeTransformer()
        with pytest.raises(AttributeError, match="No transformer handles"):
            getattr(ct, "completely_nonexistent_method")

    def test_private_method_raises_attribute_error(self):
        ct = CompositeTransformer()
        with pytest.raises(AttributeError):
            getattr(ct, "_private_method")


# ===========================================================================
# 3. Method caching
# ===========================================================================


class TestMethodCaching:
    """Verify resolved methods are cached on instance __dict__."""

    def test_first_access_caches_on_instance(self):
        ct = CompositeTransformer()
        assert "number_literal" not in ct.__dict__

        _ = ct.number_literal  # trigger delegation
        assert "number_literal" in ct.__dict__

    def test_cached_method_is_same_object(self):
        ct = CompositeTransformer()
        first = ct.number_literal
        second = ct.number_literal
        assert first is second

    def test_cache_survives_multiple_accesses(self):
        ct = CompositeTransformer()
        methods = [ct.number_literal for _ in range(100)]
        assert all(m is methods[0] for m in methods)

    def test_different_methods_cached_independently(self):
        ct = CompositeTransformer()
        nl = ct.number_literal
        ae = ct.add_expression
        np = ct.node_pattern
        mc = ct.match_clause

        assert nl is not ae
        assert ae is not np
        assert np is not mc

        # All cached
        assert "number_literal" in ct.__dict__
        assert "add_expression" in ct.__dict__
        assert "node_pattern" in ct.__dict__
        assert "match_clause" in ct.__dict__


# ===========================================================================
# 4. Thread safety
# ===========================================================================


class TestThreadSafety:
    """Verify CompositeTransformer is safe for concurrent access."""

    def test_concurrent_method_resolution(self):
        """Multiple threads resolving different methods concurrently."""
        ct = CompositeTransformer()
        errors: list[Exception] = []
        methods_to_resolve = _all_mixin_methods()

        def resolve_batch(start: int, step: int):
            try:
                for i in range(start, len(methods_to_resolve), step):
                    getattr(ct, methods_to_resolve[i])
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=resolve_batch, args=(i, 4))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Thread errors: {errors}"

    def test_concurrent_same_method_resolution(self):
        """Multiple threads resolving the same method."""
        ct = CompositeTransformer()
        results: list = []

        def resolve():
            method = ct.number_literal
            results.append(method)

        threads = [threading.Thread(target=resolve) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == 10
        # All should resolve to the same method
        assert all(r is results[0] for r in results)


# ===========================================================================
# 5. Grammar rule coverage per mixin
# ===========================================================================


class TestLiteralTransformerCoverage:
    """Verify LiteralTransformer handles key literal types."""

    def setup_method(self):
        self.t = LiteralTransformer()

    def test_number_literal(self):
        assert self.t.number_literal([42]) == 42

    def test_signed_number_integer(self):
        assert self.t.signed_number(["123"]) == 123

    def test_signed_number_negative(self):
        assert self.t.signed_number(["-5"]) == -5

    def test_signed_number_float(self):
        assert self.t.signed_number(["3.14"]) == pytest.approx(3.14)

    def test_string_literal_returns_dict(self):
        result = self.t.string_literal(["'hello'"])
        assert isinstance(result, dict)
        assert result["type"] == "StringLiteral"
        assert result["value"] == "hello"

    def test_true_literal(self):
        assert self.t.true([]) is True

    def test_false_literal(self):
        assert self.t.false([]) is False

    def test_null_literal_returns_dict(self):
        result = self.t.null_literal([])
        assert isinstance(result, dict)
        assert result["type"] == "NullLiteral"

    def test_list_literal(self):
        result = self.t.list_literal([1, 2, 3])
        assert isinstance(result, list)

    def test_map_literal(self):
        result = self.t.map_literal([("key", "value")])
        assert isinstance(result, dict)


class TestExpressionTransformerCoverage:
    """Verify ExpressionTransformer handles key expression types."""

    def setup_method(self):
        self.t = ExpressionTransformer()

    def test_or_expression_single_passthrough(self):
        """Single arg passthrough — no wrapping."""
        result = self.t.or_expression(["only"])
        assert result == "only"

    def test_and_expression_single_passthrough(self):
        result = self.t.and_expression(["only"])
        assert result == "only"

    def test_not_expression_passthrough(self):
        """Single non-NOT arg passes through unchanged."""
        result = self.t.not_expression(["expr"])
        assert result == "expr"

    def test_comparison_expression_single_passthrough(self):
        result = self.t.comparison_expression(["only"])
        assert result == "only"

    def test_property_lookup(self):
        result = self.t.property_lookup(["name"])
        assert isinstance(result, dict)
        assert result["type"] == "PropertyLookup"

    def test_add_expression_single_passthrough(self):
        result = self.t.add_expression([42])
        assert result == 42


class TestPatternTransformerCoverage:
    """Verify PatternTransformer handles key pattern types."""

    def setup_method(self):
        self.t = PatternTransformer()

    def test_node_pattern_empty(self):
        result = self.t.node_pattern([])
        assert isinstance(result, dict)
        assert result["type"] == "NodePattern"

    def test_relationship_pattern_empty_returns_none(self):
        """Empty args returns None (no pattern element)."""
        result = self.t.relationship_pattern([])
        assert result is None

    def test_node_labels(self):
        result = self.t.node_labels(["Person"])
        assert isinstance(result, (list, dict))


class TestStatementTransformerCoverage:
    """Verify StatementTransformer handles key clause types."""

    def setup_method(self):
        self.t = StatementTransformer()

    def test_return_clause(self):
        result = self.t.return_clause([{"type": "ReturnBody", "items": []}])
        assert result["type"] == "ReturnStatement"

    def test_where_clause(self):
        result = self.t.where_clause([{"type": "ComparisonExpression"}])
        assert result["type"] == "WhereClause"

    def test_match_clause(self):
        pattern = {"type": "Pattern"}
        result = self.t.match_clause([pattern])
        assert result["type"] == "MatchClause"

    def test_order_clause(self):
        result = self.t.order_clause([])
        assert result["type"] == "OrderClause"

    def test_limit_clause(self):
        result = self.t.limit_clause([10])
        assert result["type"] == "LimitClause"
        assert result["value"] == 10

    def test_skip_clause(self):
        result = self.t.skip_clause([5])
        assert result["type"] == "SkipClause"
        assert result["value"] == 5

    def test_create_clause(self):
        result = self.t.create_clause([{"type": "Pattern"}])
        assert result["type"] == "CreateClause"

    def test_delete_clause(self):
        result = self.t.delete_clause([{"type": "Variable"}])
        assert result["type"] == "DeleteClause"

    def test_unwind_clause(self):
        result = self.t.unwind_clause([[1, 2, 3], "x"])
        assert result["type"] == "UnwindClause"


# ===========================================================================
# 6. CompositeTransformer direct methods
# ===========================================================================


class TestCompositeDirectMethods:
    """Verify methods defined directly on CompositeTransformer."""

    def test_has_transform_method(self):
        ct = CompositeTransformer()
        assert hasattr(ct, "transform")
        assert callable(ct.transform)

    def test_has_ambig_method(self):
        ct = CompositeTransformer()
        assert hasattr(ct, "_ambig")

    def test_ambig_single_arg(self):
        ct = CompositeTransformer()
        assert ct._ambig(["only"]) == "only"

    def test_ambig_multiple_args_returns_first(self):
        ct = CompositeTransformer()
        assert ct._ambig(["first", "second"]) == "first"


# ===========================================================================
# 7. Mixin inheritance chain verification
# ===========================================================================


class TestMixinInheritance:
    """Verify transformer classes correctly inherit from mixins."""

    def test_literal_transformer_inherits_literal_mixin(self):
        assert issubclass(LiteralTransformer, LiteralRulesMixin)

    def test_expression_transformer_inherits_expression_mixin(self):
        assert issubclass(ExpressionTransformer, ExpressionRulesMixin)

    def test_function_transformer_inherits_function_mixin(self):
        assert issubclass(FunctionTransformer, FunctionRulesMixin)

    def test_pattern_transformer_inherits_pattern_mixin(self):
        assert issubclass(PatternTransformer, PatternRulesMixin)

    def test_statement_transformer_inherits_clause_mixin(self):
        assert issubclass(StatementTransformer, ClauseRulesMixin)

    def test_no_method_name_collisions_across_mixins(self):
        """No two mixins should define the same method name."""
        all_methods: dict[str, str] = {}
        collisions = []
        for mixin in ALL_MIXINS:
            for name in _mixin_methods(mixin):
                if name in all_methods:
                    collisions.append(f"{name}: {all_methods[name]} vs {mixin.__name__}")
                all_methods[name] = mixin.__name__
        assert collisions == [], f"Method name collisions: {collisions}"
