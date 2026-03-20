"""Tests for Context and Star __repr__ methods.

Verifies that REPL/notebook display shows backend, entity types
with row counts, and relationship types with row counts.
"""

from __future__ import annotations

from pycypher.relational_models import Context
from pycypher.star import Star


class TestContextRepr:
    """Test Context.__repr__ output."""

    def test_empty_context(self) -> None:
        """Empty context shows backend and empty entities."""
        ctx = Context()
        r = repr(ctx)
        assert "backend='pandas'" in r
        assert "entities={}" in r

    def test_with_entities(self, person_context: Context) -> None:
        """Context with entities shows type names and row counts."""
        r = repr(person_context)
        assert "backend='pandas'" in r
        assert "'Person': 4" in r

    def test_with_relationships(self, social_context: Context) -> None:
        """Context with relationships shows rel types and row counts."""
        r = repr(social_context)
        assert "'Person': 4" in r
        assert "'KNOWS': 3" in r

    def test_starts_with_context(self, person_context: Context) -> None:
        """Repr starts with 'Context(' and ends with ')'."""
        r = repr(person_context)
        assert r.startswith("Context(")
        assert r.endswith(")")


class TestStarRepr:
    """Test Star.__repr__ output."""

    def test_empty_star(self) -> None:
        """Empty Star shows backend and empty entities."""
        star = Star()
        r = repr(star)
        assert "backend='pandas'" in r
        assert "entities={}" in r

    def test_with_entities(self, person_star: Star) -> None:
        """Star with entities shows type names and row counts."""
        r = repr(person_star)
        assert "backend='pandas'" in r
        assert "'Person': 4" in r

    def test_with_relationships(self, social_star: Star) -> None:
        """Star with relationships shows rel types and row counts."""
        r = repr(social_star)
        assert "'Person': 4" in r
        assert "'KNOWS': 3" in r

    def test_starts_with_star(self, person_star: Star) -> None:
        """Repr starts with 'Star(' and ends with ')'."""
        r = repr(person_star)
        assert r.startswith("Star(")
        assert r.endswith(")")
