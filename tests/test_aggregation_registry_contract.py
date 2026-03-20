"""Contract test: KNOWN_AGGREGATIONS must equal _AGG_OPS ∪ _PERCENTILE_AGGREGATIONS.

Without this test, a developer could add an aggregation to _AGG_OPS without
updating KNOWN_AGGREGATIONS (or vice versa), breaking:
- _contains_aggregation() in star.py (group-by detection)
- _validate_aggregation_in_context() in semantic_validator.py
- Any code that uses KNOWN_AGGREGATIONS as the authoritative aggregation list

This is a TDD-first contract test — written before the KNOWN_AGGREGATIONS
derivation fix is applied.  It should PASS once the derivation is in place.
"""

from __future__ import annotations

from pycypher.aggregation_evaluator import (
    _AGG_OPS,
    _PERCENTILE_AGGREGATIONS,
    KNOWN_AGGREGATIONS,
)


class TestAggregationRegistryContract:
    """KNOWN_AGGREGATIONS must be exactly _AGG_OPS ∪ _PERCENTILE_AGGREGATIONS."""

    def test_known_aggregations_equals_agg_ops_union_percentile(self) -> None:
        """KNOWN_AGGREGATIONS must be derivable from the dispatch tables."""
        expected = frozenset(_AGG_OPS.keys()) | _PERCENTILE_AGGREGATIONS
        assert KNOWN_AGGREGATIONS == expected, (
            f"KNOWN_AGGREGATIONS diverged from dispatch tables.\n"
            f"  In KNOWN_AGGREGATIONS but not in dispatch tables: "
            f"{KNOWN_AGGREGATIONS - expected}\n"
            f"  In dispatch tables but not in KNOWN_AGGREGATIONS: "
            f"{expected - KNOWN_AGGREGATIONS}"
        )

    def test_all_agg_ops_are_known(self) -> None:
        """Every function in _AGG_OPS must be in KNOWN_AGGREGATIONS."""
        for name in _AGG_OPS:
            assert name in KNOWN_AGGREGATIONS, (
                f"Aggregation '{name}' is in _AGG_OPS but missing from KNOWN_AGGREGATIONS"
            )

    def test_all_percentile_aggs_are_known(self) -> None:
        """Every function in _PERCENTILE_AGGREGATIONS must be in KNOWN_AGGREGATIONS."""
        for name in _PERCENTILE_AGGREGATIONS:
            assert name in KNOWN_AGGREGATIONS, (
                f"Aggregation '{name}' is in _PERCENTILE_AGGREGATIONS but missing "
                f"from KNOWN_AGGREGATIONS"
            )

    def test_no_unknown_entries_in_known_aggregations(self) -> None:
        """KNOWN_AGGREGATIONS must not contain functions absent from dispatch tables."""
        all_dispatch_names = (
            frozenset(_AGG_OPS.keys()) | _PERCENTILE_AGGREGATIONS
        )
        extra = KNOWN_AGGREGATIONS - all_dispatch_names
        assert not extra, (
            f"KNOWN_AGGREGATIONS contains entries not in any dispatch table: {extra}"
        )
