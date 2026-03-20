"""Tests for relational_models.py coverage gaps.

Covers:
- flatten() utility
"""

from __future__ import annotations

from pycypher.relational_models import flatten

# ---------------------------------------------------------------------------
# flatten()
# ---------------------------------------------------------------------------


class TestFlatten:
    """Tests for the flatten() utility."""

    def test_flat_list(self) -> None:
        assert flatten([1, 2, 3]) == [1, 2, 3]

    def test_nested_list(self) -> None:
        assert flatten([1, [2, 3], [4, [5, 6]]]) == [1, 2, 3, 4, 5, 6]

    def test_empty_list(self) -> None:
        assert flatten([]) == []

    def test_deeply_nested(self) -> None:
        assert flatten([[[["a"]]]]) == ["a"]
