"""Query complexity analysis and resource estimation.

:class:`QueryComplexityAnalyzer` inspects Cypher query text to estimate
computational cost and resource requirements. This drives routing
decisions in the :class:`~fastopendata.loadbalancer.balancer.LoadBalancer`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ComplexityTier(Enum):
    """Coarse complexity classification for routing purposes."""

    TRIVIAL = "trivial"
    LIGHT = "light"
    MODERATE = "moderate"
    HEAVY = "heavy"
    EXTREME = "extreme"


@dataclass(frozen=True)
class QueryComplexity:
    """Estimated complexity and resource requirements for a query.

    Attributes
    ----------
    tier : ComplexityTier
        Coarse classification.
    score : float
        Numeric complexity score (higher = more expensive).
    estimated_rows : int
        Estimated output cardinality.
    join_count : int
        Number of join operations detected.
    filter_count : int
        Number of WHERE clauses / filter predicates.
    has_aggregation : bool
        Whether the query uses aggregation functions.
    has_ordering : bool
        Whether the query uses ORDER BY.
    has_limit : bool
        Whether the query constrains output with LIMIT.
    pattern_count : int
        Number of MATCH patterns.
    capabilities_needed : set[str]
        Processing capabilities needed (scan, join, aggregate, etc.).

    """

    tier: ComplexityTier
    score: float
    estimated_rows: int = 1000
    join_count: int = 0
    filter_count: int = 0
    has_aggregation: bool = False
    has_ordering: bool = False
    has_limit: bool = False
    pattern_count: int = 0
    capabilities_needed: frozenset[str] = frozenset()

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "tier": self.tier.value,
            "score": round(self.score, 2),
            "estimated_rows": self.estimated_rows,
            "join_count": self.join_count,
            "filter_count": self.filter_count,
            "has_aggregation": self.has_aggregation,
            "has_ordering": self.has_ordering,
            "has_limit": self.has_limit,
            "pattern_count": self.pattern_count,
            "capabilities_needed": sorted(self.capabilities_needed),
        }


# Regex patterns for query feature detection
_MATCH_PATTERN = re.compile(r"\bMATCH\b", re.IGNORECASE)
_WHERE_PATTERN = re.compile(r"\bWHERE\b", re.IGNORECASE)
_RELATIONSHIP_PATTERN = re.compile(r"\[[\w:]*\]")
_AGG_PATTERN = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|COLLECT)\s*\(",
    re.IGNORECASE,
)
_ORDER_PATTERN = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r"\bLIMIT\b", re.IGNORECASE)
_UNWIND_PATTERN = re.compile(r"\bUNWIND\b", re.IGNORECASE)
_WITH_PATTERN = re.compile(r"\bWITH\b", re.IGNORECASE)
_OPTIONAL_MATCH = re.compile(r"\bOPTIONAL\s+MATCH\b", re.IGNORECASE)


class QueryComplexityAnalyzer:
    """Analyzes Cypher query text to estimate computational complexity.

    Uses heuristic pattern matching on the query string to identify
    expensive operations (joins, aggregations, sorts) and produce a
    complexity score that drives load balancing routing decisions.

    Parameters
    ----------
    base_row_estimate : int
        Default estimated rows when no LIMIT is present.

    """

    def __init__(self, base_row_estimate: int = 1000) -> None:
        self._base_row_estimate = base_row_estimate

    def analyze(self, query: str) -> QueryComplexity:
        """Analyze a Cypher query and return its complexity estimate.

        Parameters
        ----------
        query : str
            The Cypher query text.

        Returns
        -------
        QueryComplexity
            Estimated complexity and resource requirements.

        """
        match_count = len(_MATCH_PATTERN.findall(query))
        optional_count = len(_OPTIONAL_MATCH.findall(query))
        pattern_count = match_count + optional_count

        rel_count = len(_RELATIONSHIP_PATTERN.findall(query))
        filter_count = len(_WHERE_PATTERN.findall(query))
        has_agg = bool(_AGG_PATTERN.search(query))
        has_order = bool(_ORDER_PATTERN.search(query))
        has_limit = bool(_LIMIT_PATTERN.search(query))
        has_unwind = bool(_UNWIND_PATTERN.search(query))
        with_count = len(_WITH_PATTERN.findall(query))

        # Join count heuristic: relationships create joins
        join_count = rel_count

        # Estimate rows
        estimated_rows = self._base_row_estimate
        if has_limit:
            limit_match = re.search(r"\bLIMIT\s+(\d+)", query, re.IGNORECASE)
            if limit_match:
                estimated_rows = min(estimated_rows, int(limit_match.group(1)))

        # Complexity score: weighted sum of features
        score = 1.0
        score += pattern_count * 5.0
        score += join_count * 15.0  # Joins are expensive
        score += optional_count * 10.0  # OPTIONAL MATCH = outer join
        score += (1.0 if has_agg else 0.0) * 8.0
        score += (1.0 if has_order else 0.0) * 5.0
        score += (1.0 if has_unwind else 0.0) * 3.0
        score += with_count * 2.0
        # Filters reduce work
        score -= filter_count * 2.0
        # LIMIT reduces work
        if has_limit and estimated_rows < self._base_row_estimate:
            score -= 5.0
        score = max(1.0, score)

        # Determine tier from score
        tier = self._score_to_tier(score)

        # Determine needed capabilities
        capabilities: set[str] = {"scan"}
        if join_count > 0:
            capabilities.add("join")
        if has_agg:
            capabilities.add("aggregate")
        if filter_count > 0:
            capabilities.add("filter")
        if has_order:
            capabilities.add("sort")

        return QueryComplexity(
            tier=tier,
            score=score,
            estimated_rows=estimated_rows,
            join_count=join_count,
            filter_count=filter_count,
            has_aggregation=has_agg,
            has_ordering=has_order,
            has_limit=has_limit,
            pattern_count=pattern_count,
            capabilities_needed=frozenset(capabilities),
        )

    @staticmethod
    def _score_to_tier(score: float) -> ComplexityTier:
        """Map a numeric score to a complexity tier."""
        if score <= 5.0:
            return ComplexityTier.TRIVIAL
        if score <= 15.0:
            return ComplexityTier.LIGHT
        if score <= 35.0:
            return ComplexityTier.MODERATE
        if score <= 70.0:
            return ComplexityTier.HEAVY
        return ComplexityTier.EXTREME
