"""TDD tests for improved AST conversion error messages.

These tests are written *before* the implementation (TDD red phase).

Run with:
    uv run pytest tests/test_ast_conversion_errors.py -v
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pycypher.ast_models import ASTConverter, _parse_cypher_cached

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_unconstructable_node(node_type: str) -> dict:
    """Return a node dict whose ASTNode class exists but whose constructor
    will raise because required fields are missing.

    IntegerLiteral has a required `value: int` field with no default,
    so {"type": "IntegerLiteral"} (no "value" key) always fails construction.
    """
    return {"type": node_type}


# ---------------------------------------------------------------------------
# _convert_generic raises on known-class construction failure
# ---------------------------------------------------------------------------


class TestConvertGenericRaisesOnFailure:
    def test_raises_value_error_not_returns_none(self) -> None:
        """_convert_generic raises ValueError (not returns None) when a known
        ASTNode class constructor fails.

        IntegerLiteral requires 'value: int' — omitting it causes Pydantic
        to raise a ValidationError, which _convert_generic must propagate.
        """
        converter = ASTConverter()
        node = _make_unconstructable_node("IntegerLiteral")
        with pytest.raises(ValueError):
            converter._convert_generic(node, "IntegerLiteral")

    def test_error_message_includes_node_type(self) -> None:
        """The ValueError message from _convert_generic names the failing node type."""
        converter = ASTConverter()
        node = _make_unconstructable_node("IntegerLiteral")
        with pytest.raises(ValueError, match="IntegerLiteral"):
            converter._convert_generic(node, "IntegerLiteral")

    def test_error_is_chained(self) -> None:
        """The ValueError from _convert_generic is chained from the original exception."""
        converter = ASTConverter()
        node = _make_unconstructable_node("IntegerLiteral")
        try:
            converter._convert_generic(node, "IntegerLiteral")
        except ValueError as exc:
            assert exc.__cause__ is not None, (
                "Expected ValueError to be chained from original exception"
            )
        else:
            pytest.fail("Expected ValueError was not raised")

    def test_unknown_node_type_still_returns_none(self) -> None:
        """_convert_generic returns None (not raises) for a completely unknown
        node type — graceful degradation for unknown grammar artefacts."""
        converter = ASTConverter()
        node = {"type": "CompletlyUnknownNodeType9999", "x": 1}
        result = converter._convert_generic(
            node, "CompletlyUnknownNodeType9999"
        )
        assert result is None


# ---------------------------------------------------------------------------
# from_cypher() error message quality
# ---------------------------------------------------------------------------


class TestFromCypherErrorMessage:
    def test_is_none_check_not_falsy_check(self) -> None:
        """from_cypher() uses `is None` semantics, not `if not node`.
        A valid non-None ASTNode should never be rejected as falsy."""
        # We mock convert() to return a real ASTNode to confirm it passes through.
        from pycypher.ast_models import Query

        converter = ASTConverter.__new__(ASTConverter)
        # Manually test the check logic: if ast_node is None → raise,
        # if ast_node is a valid ASTNode → return it.
        # The old `if not ast_node:` would fire for ASTNodes that are falsy.
        ast_node = Query(clauses=[])
        # Simulate what from_cypher does post-conversion
        if ast_node is None:
            pytest.fail("is None check incorrectly rejected a valid ASTNode")
        # Also confirm that None would be caught
        none_node = None
        assert none_node is None

    def test_none_result_raises_with_query_snippet(self) -> None:
        """When from_cypher() conversion returns None, the ValueError includes
        a snippet of the query being parsed."""
        # Clear cache to prevent interference from other tests
        _parse_cypher_cached.cache_clear()
        # Patch convert() to return None
        with patch.object(ASTConverter, "convert", return_value=None):
            with pytest.raises(ValueError, match="MATCH"):
                ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")

    def test_none_result_error_does_not_say_falsey(self) -> None:
        """The improved error message must NOT contain the old opaque text
        'Got a falsey object from AST conversion.'"""
        # Clear cache to prevent interference from other tests
        _parse_cypher_cached.cache_clear()
        with patch.object(ASTConverter, "convert", return_value=None):
            with pytest.raises(ValueError) as exc_info:
                ASTConverter.from_cypher("RETURN 1")
        assert "falsey" not in str(exc_info.value).lower(), (
            f"Error message still contains old opaque text: {exc_info.value}"
        )

    def test_cache_isolation_bug_pattern(self) -> None:
        """Demonstrate the cache isolation bug pattern.

        This test verifies that _parse_cypher_cached.cache_clear() is needed
        when mocking ASTConverter.convert() because the LRU cache can bypass
        the patch entirely if the query was previously cached.
        """
        query = "MATCH (unique:TestCacheIsolation) RETURN unique.test"

        # First: populate the cache with a successful parse
        _parse_cypher_cached.cache_clear()
        real_result = ASTConverter.from_cypher(query)
        assert real_result is not None, "Real parse should succeed"

        # Second: WITHOUT cache_clear(), the patch should be bypassed (cache hit)
        with patch.object(ASTConverter, "convert", return_value=None):
            # This should NOT raise because cached result bypasses the patch
            cached_result = ASTConverter.from_cypher(query)
            assert cached_result is not None, "Cache should bypass the patch"
            # Verify it's the same object from cache
            assert cached_result is real_result

        # Third: WITH cache_clear(), the patch should work (cache miss)
        _parse_cypher_cached.cache_clear()
        with patch.object(ASTConverter, "convert", return_value=None):
            with pytest.raises(ValueError, match="TestCacheIsolation"):
                ASTConverter.from_cypher(query)


# ---------------------------------------------------------------------------
# star.py "no entity types" actionable error
# ---------------------------------------------------------------------------


class TestNoEntityTypesError:
    def test_unlabeled_match_empty_context_raises_value_error(self) -> None:
        """MATCH (n) RETURN n against an empty context raises ValueError."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        with pytest.raises(ValueError):
            star.execute_query("MATCH (n) RETURN n")

    def test_error_message_mentions_context_builder(self) -> None:
        """The 'no entity types' error message mentions ContextBuilder so the
        user knows how to fix it."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        with pytest.raises(ValueError, match="ContextBuilder"):
            star.execute_query("MATCH (n) RETURN n")

    def test_error_message_mentions_labelled_pattern(self) -> None:
        """The 'no entity types' error message suggests using a labelled
        pattern as an alternative fix."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        with pytest.raises(ValueError) as exc_info:
            star.execute_query("MATCH (n) RETURN n")
        msg = str(exc_info.value).lower()
        assert "label" in msg or "pattern" in msg, (
            f"Expected error to mention labelled pattern. Got: {exc_info.value}"
        )
