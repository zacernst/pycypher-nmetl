"""CLI Intelligence System for query suggestions and performance hints.

Provides intelligent query assistance by leveraging the QueryFingerprinter
for structural similarity detection and pattern-based analysis for
performance optimization recommendations.

Components:
    QuerySuggestionEngine — suggests similar queries from a pattern library
    PerformanceHintEngine — analyzes queries for anti-patterns and hints
    QuerySuggestion — data model for a query suggestion
    PerformanceHint — data model for a performance hint
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from shared.logger import LOGGER

__all__ = [
    "PerformanceHint",
    "PerformanceHintEngine",
    "QuerySuggestion",
    "QuerySuggestionEngine",
    "RecommendationEngine",
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuerySuggestion:
    """A suggested query pattern.

    Attributes:
        query: The original query text.
        description: Human-readable description of the query.
        similarity: Similarity score (0.0-1.0) to the input query.
    """

    query: str
    description: str
    similarity: float = 1.0


@dataclass(frozen=True)
class PerformanceHint:
    """A performance optimization hint.

    Attributes:
        message: Description of the issue or pattern detected.
        severity: One of 'info' or 'warning'.
        suggestion: Recommended action to improve performance.
    """

    message: str
    severity: str  # "info" or "warning"
    suggestion: str = ""


# ---------------------------------------------------------------------------
# Common Cypher patterns for suggestion preloading
# ---------------------------------------------------------------------------

_COMMON_PATTERNS: list[tuple[str, str]] = [
    (
        "MATCH (n) RETURN n LIMIT 25",
        "Browse first 25 nodes of any type",
    ),
    (
        "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
        "Find a specific person by name",
    ),
    (
        "MATCH (a)-[r]->(b) RETURN type(r), count(*)",
        "Count relationships by type",
    ),
    (
        "MATCH (n) RETURN DISTINCT labels(n), count(*)",
        "Count nodes by label",
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        "Find who knows whom",
    ),
    (
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.name ORDER BY p.age LIMIT 10",
        "Top 10 people older than 30 by age",
    ),
    (
        "MATCH (p:Person) RETURN avg(p.age), min(p.age), max(p.age)",
        "Aggregate age statistics",
    ),
    (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
        "Find friends within 3 hops of Alice",
    ),
]


# ---------------------------------------------------------------------------
# Query Suggestion Engine
# ---------------------------------------------------------------------------


class QuerySuggestionEngine:
    """Suggests similar queries from a registered pattern library.

    Uses :class:`~pycypher.query_learning.QueryFingerprinter` for
    structural similarity detection so that queries differing only
    in literal values are recognized as structurally identical.
    """

    def __init__(self) -> None:
        self._patterns: list[_RegisteredPattern] = []
        self._fingerprinter = _get_fingerprinter()

    @classmethod
    def with_common_patterns(cls) -> QuerySuggestionEngine:
        """Create an engine preloaded with common Cypher patterns."""
        engine = cls()
        for query_text, description in _COMMON_PATTERNS:
            engine.register_query(query_text, description=description)
        return engine

    def register_query(
        self,
        query_text: str,
        *,
        description: str = "",
    ) -> None:
        """Register a query pattern for future suggestion matching."""
        fp = self._compute_fingerprint(query_text)
        self._patterns.append(
            _RegisteredPattern(
                query=query_text,
                description=description,
                fingerprint_digest=fp.digest if fp else None,
                clause_signature=fp.clause_signature if fp else "",
                entity_types=fp.entity_types if fp else (),
                relationship_types=fp.relationship_types if fp else (),
            ),
        )

    def suggest(
        self,
        query_text: str,
        *,
        max_results: int = 5,
    ) -> list[QuerySuggestion]:
        """Suggest similar queries from the pattern library.

        Args:
            query_text: The query to find suggestions for.
            max_results: Maximum number of suggestions to return.

        Returns:
            List of QuerySuggestion ordered by similarity (highest first).
        """
        if not query_text or not query_text.strip():
            return []

        fp = self._compute_fingerprint(query_text)
        if fp is None:
            return []

        scored: list[QuerySuggestion] = []
        seen_digests: set[str] = set()

        for pattern in self._patterns:
            if pattern.fingerprint_digest is None:
                continue

            # Exact structural match
            if pattern.fingerprint_digest == fp.digest:
                if pattern.fingerprint_digest in seen_digests:
                    continue
                seen_digests.add(pattern.fingerprint_digest)
                scored.append(
                    QuerySuggestion(
                        query=pattern.query,
                        description=pattern.description,
                        similarity=1.0,
                    ),
                )
                continue

            # Partial similarity: shared entity/relationship types
            sim = self._compute_similarity(fp, pattern)
            if sim > 0.5:
                key = pattern.fingerprint_digest
                if key in seen_digests:
                    continue
                seen_digests.add(key)
                scored.append(
                    QuerySuggestion(
                        query=pattern.query,
                        description=pattern.description,
                        similarity=sim,
                    ),
                )

        scored.sort(key=lambda s: s.similarity, reverse=True)
        return scored[:max_results]

    @property
    def pattern_count(self) -> int:
        """Number of registered patterns."""
        return len(self._patterns)

    def _compute_fingerprint(self, query_text: str) -> Any:
        """Compute fingerprint, returning None on parse failure."""
        try:
            from pycypher.ast_converter import ASTConverter

            ast = ASTConverter.from_cypher(query_text)
            return self._fingerprinter.fingerprint(ast)
        except Exception:  # noqa: BLE001 — best-effort query fingerprinting
            LOGGER.debug(
                "Failed to fingerprint query: %s",
                query_text[:80],
            )
            return None

    def _compute_similarity(
        self,
        fp: Any,
        pattern: _RegisteredPattern,
    ) -> float:
        """Compute similarity between a fingerprint and a registered pattern."""
        score = 0.0
        total = 0.0

        # Clause signature similarity (most important)
        total += 2.0
        if fp.clause_signature == pattern.clause_signature:
            score += 2.0

        # Entity type overlap
        if fp.entity_types or pattern.entity_types:
            total += 1.0
            fp_set = set(fp.entity_types)
            pat_set = set(pattern.entity_types)
            if fp_set and pat_set:
                overlap = len(fp_set & pat_set)
                union = len(fp_set | pat_set)
                score += overlap / union if union > 0 else 0.0

        # Relationship type overlap
        if fp.relationship_types or pattern.relationship_types:
            total += 1.0
            fp_set = set(fp.relationship_types)
            pat_set = set(pattern.relationship_types)
            if fp_set and pat_set:
                overlap = len(fp_set & pat_set)
                union = len(fp_set | pat_set)
                score += overlap / union if union > 0 else 0.0

        return score / total if total > 0 else 0.0


@dataclass
class _RegisteredPattern:
    """Internal storage for a registered query pattern."""

    query: str
    description: str
    fingerprint_digest: str | None
    clause_signature: str
    entity_types: tuple[str, ...]
    relationship_types: tuple[str, ...]


def _get_fingerprinter() -> Any:
    """Get or create a QueryFingerprinter instance."""
    from pycypher.query_learning import QueryFingerprinter

    return QueryFingerprinter()


# ---------------------------------------------------------------------------
# Performance Hint Engine
# ---------------------------------------------------------------------------

# Regex patterns for query anti-pattern detection
_RE_RETURN_STAR = re.compile(r"\bRETURN\s+\*", re.IGNORECASE)
_RE_ORDER_BY = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
_RE_LIMIT = re.compile(r"\bLIMIT\b", re.IGNORECASE)
_RE_WHERE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_RE_MATCH = re.compile(r"\bMATCH\b", re.IGNORECASE)
_RE_VAR_LENGTH_PATH = re.compile(r"\[\s*\*\s*\]", re.IGNORECASE)
_RE_BOUNDED_VAR_LENGTH = re.compile(
    r"\[\s*\*\s*\d+\s*\.\.\s*\d+\s*\]",
    re.IGNORECASE,
)


class PerformanceHintEngine:
    """Analyzes Cypher queries for anti-patterns and performance issues.

    Performs static analysis on query text to detect common performance
    pitfalls and suggest improvements.
    """

    def analyze(self, query_text: str) -> list[PerformanceHint]:
        """Analyze a query and return performance hints.

        Args:
            query_text: The Cypher query string to analyze.

        Returns:
            List of PerformanceHint objects, ordered by severity.
        """
        if not query_text or not query_text.strip():
            return []

        hints: list[PerformanceHint] = []
        upper = query_text.upper()

        # Check RETURN *
        if _RE_RETURN_STAR.search(query_text):
            hints.append(
                PerformanceHint(
                    message=(
                        "RETURN * projects all columns which may "
                        "include unnecessary data."
                    ),
                    severity="warning",
                    suggestion=(
                        "Specify only the columns you need, "
                        "e.g. RETURN p.name, p.age"
                    ),
                ),
            )

        # Check ORDER BY without LIMIT
        has_order = _RE_ORDER_BY.search(query_text)
        has_limit = _RE_LIMIT.search(query_text)
        if has_order and not has_limit:
            hints.append(
                PerformanceHint(
                    message=(
                        "ORDER BY without LIMIT sorts the entire "
                        "result set."
                    ),
                    severity="warning",
                    suggestion=(
                        "Add LIMIT to avoid sorting rows that "
                        "won't be used."
                    ),
                ),
            )

        # Check multiple MATCH clauses (cartesian product risk)
        match_count = len(_RE_MATCH.findall(query_text))
        if match_count >= 3:
            hints.append(
                PerformanceHint(
                    message=(
                        f"Query has {match_count} MATCH clauses which "
                        "may cause expensive cartesian products."
                    ),
                    severity="warning",
                    suggestion=(
                        "Combine MATCH patterns where possible or "
                        "use WITH to pipeline intermediate results."
                    ),
                ),
            )

        # Check MATCH without WHERE (unbounded scan)
        has_where = _RE_WHERE.search(query_text)
        has_match = _RE_MATCH.search(query_text)
        if has_match and not has_where and not has_limit:
            hints.append(
                PerformanceHint(
                    message=(
                        "MATCH without WHERE or LIMIT performs an "
                        "unbounded scan."
                    ),
                    severity="info",
                    suggestion=(
                        "Add a WHERE clause to filter results or "
                        "LIMIT to cap the result set."
                    ),
                ),
            )

        # Check unbounded variable-length paths
        has_var_path = _RE_VAR_LENGTH_PATH.search(query_text)
        has_bounded = _RE_BOUNDED_VAR_LENGTH.search(query_text)
        if has_var_path and not has_bounded:
            hints.append(
                PerformanceHint(
                    message=(
                        "Unbounded variable-length path [*] may "
                        "explore the entire graph."
                    ),
                    severity="warning",
                    suggestion=(
                        "Add bounds to the path pattern, "
                        "e.g. [*1..5] to limit traversal depth."
                    ),
                ),
            )

        # Sort by severity: warnings first, then info
        severity_order = {"warning": 0, "info": 1}
        hints.sort(key=lambda h: severity_order.get(h.severity, 2))

        return hints


# ---------------------------------------------------------------------------
# CLI output formatting
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Recommendation Engine — ML-based optimization suggestions
# ---------------------------------------------------------------------------


class RecommendationEngine:
    """Generates ML-based optimization recommendations using learned patterns.

    Combines static analysis from :class:`PerformanceHintEngine` with
    learned data from the :class:`~pycypher.query_learning.QueryLearningStore`
    to provide context-aware optimization recommendations.
    """

    def __init__(self) -> None:
        self._hint_engine = PerformanceHintEngine()
        self._suggestion_engine = QuerySuggestionEngine.with_common_patterns()

    def recommend(
        self,
        query_text: str,
    ) -> list[PerformanceHint]:
        """Generate comprehensive recommendations for a query.

        Combines:
        1. Static anti-pattern detection
        2. Learned selectivity insights (if available)
        3. Learned join strategy recommendations (if available)
        4. Plan cache hit/miss information

        Args:
            query_text: The Cypher query to analyze.

        Returns:
            List of PerformanceHint with recommendations.
        """
        if not query_text or not query_text.strip():
            return []

        # Start with static analysis hints
        hints = list(self._hint_engine.analyze(query_text))

        # Add ML-learned insights from the learning store
        try:
            from pycypher.query_learning import get_learning_store

            store = get_learning_store()
            diagnostics = store.diagnostics()

            # Report plan cache status
            cache_stats = diagnostics.get("plan_cache", {})
            hit_rate = cache_stats.get("hit_rate", 0.0)
            entries = cache_stats.get("entries", 0)

            if entries > 0 and hit_rate > 0.5:
                hints.append(
                    PerformanceHint(
                        message=(
                            f"Plan cache hit rate: {hit_rate:.0%} "
                            f"({entries} cached plans). "
                            "Structurally similar queries reuse "
                            "cached plans."
                        ),
                        severity="info",
                        suggestion=(
                            "Parameterize literal values to maximize "
                            "plan cache reuse."
                        ),
                    ),
                )

            # Report learned selectivity patterns
            sel_count = diagnostics.get("selectivity_patterns", 0)
            if sel_count > 0:
                hints.append(
                    PerformanceHint(
                        message=(
                            f"Learning store has {sel_count} learned "
                            "selectivity pattern(s) for adaptive "
                            "query optimization."
                        ),
                        severity="info",
                        suggestion=(
                            "Run queries repeatedly to build learned "
                            "selectivity data for better optimization."
                        ),
                    ),
                )

            # Report learned join strategies
            join_count = diagnostics.get("join_buckets_tracked", 0)
            if join_count > 0:
                hints.append(
                    PerformanceHint(
                        message=(
                            f"Learning store has {join_count} learned "
                            "join strategy bucket(s) for adaptive "
                            "join selection."
                        ),
                        severity="info",
                        suggestion=(
                            "The query planner will automatically use "
                            "historically best-performing join "
                            "strategies."
                        ),
                    ),
                )

            # Try to fingerprint and check cache
            try:
                from pycypher.ast_converter import ASTConverter

                ast = ASTConverter.from_cypher(query_text)
                fp = store.fingerprint(ast)
                cached = store.get_cached_plan(fp)
                if cached is not None:
                    hints.append(
                        PerformanceHint(
                            message=(
                                "Cached plan found for this query "
                                "structure (fingerprint match)."
                            ),
                            severity="info",
                            suggestion=(
                                "This query structure has been "
                                "optimized before. Execution should "
                                "be faster."
                            ),
                        ),
                    )
            except Exception:  # noqa: BLE001 — parse failures expected for partial queries
                pass

        except Exception:  # noqa: BLE001 — best-effort recommendation lookup
            LOGGER.debug(
                "Could not access learning store for recommendations",
                exc_info=True,
            )

        return hints


# ---------------------------------------------------------------------------
# CLI output formatting
# ---------------------------------------------------------------------------


def format_suggestions(suggestions: list[QuerySuggestion]) -> str:
    """Format query suggestions for CLI output."""
    if not suggestions:
        return ""

    lines = ["", "Similar query patterns:"]
    for i, s in enumerate(suggestions, 1):
        lines.append(
            f"  {i}. {s.description} "
            f"(similarity: {s.similarity:.0%})"
        )
        lines.append(f"     {s.query}")
    return "\n".join(lines)


def format_hints(hints: list[PerformanceHint]) -> str:
    """Format performance hints for CLI output."""
    if not hints:
        return ""

    lines = ["", "Performance hints:"]
    for h in hints:
        icon = "!" if h.severity == "warning" else "i"
        lines.append(f"  [{icon}] {h.message}")
        if h.suggestion:
            lines.append(f"      -> {h.suggestion}")
    return "\n".join(lines)
