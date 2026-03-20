"""Tests for the query timeout mechanism.

Validates that ``Star.execute_query(timeout_seconds=...)`` raises
``QueryTimeoutError`` when the deadline is exceeded, and that normal
queries complete successfully when no timeout or a generous timeout is set.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from pycypher import Context, ContextBuilder, QueryTimeoutError, Star


@pytest.fixture
def star() -> Star:
    """Return a Star instance with a simple Person entity."""
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    context = ContextBuilder().add_entity("Person", df).build()
    return Star(context=context)


class TestQueryTimeoutBasic:
    """Basic timeout enforcement tests."""

    def test_no_timeout_succeeds(self, star: Star) -> None:
        """Query without timeout runs normally."""
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 3

    def test_generous_timeout_succeeds(self, star: Star) -> None:
        """Query with generous timeout completes fine."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name",
            timeout_seconds=60.0,
        )
        assert len(result) == 3

    def test_zero_timeout_raises(self, star: Star) -> None:
        """A zero-second timeout should raise immediately at the first clause."""
        with pytest.raises(QueryTimeoutError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=0.0,
            )
        assert exc_info.value.timeout_seconds == 0.0
        assert exc_info.value.elapsed_seconds >= 0.0

    def test_tiny_timeout_raises(self, star: Star) -> None:
        """An extremely small timeout raises QueryTimeoutError."""
        with pytest.raises(QueryTimeoutError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=1e-9,
            )


class TestQueryTimeoutErrorAttributes:
    """Verify QueryTimeoutError carries useful diagnostic info."""

    def test_timeout_seconds_attribute(self) -> None:
        err = QueryTimeoutError(timeout_seconds=5.0, elapsed_seconds=6.1)
        assert err.timeout_seconds == 5.0
        assert err.elapsed_seconds == 6.1

    def test_query_fragment_attribute(self) -> None:
        err = QueryTimeoutError(
            timeout_seconds=1.0,
            query_fragment="MATCH (n) RETURN n",
        )
        assert "MATCH (n) RETURN n" in str(err)

    def test_long_query_fragment_truncated(self) -> None:
        long_query = "MATCH (n) " * 20
        err = QueryTimeoutError(
            timeout_seconds=1.0,
            query_fragment=long_query,
        )
        assert "..." in str(err)

    def test_is_timeout_error(self) -> None:
        """QueryTimeoutError inherits from TimeoutError for broad catches."""
        err = QueryTimeoutError(timeout_seconds=1.0)
        assert isinstance(err, TimeoutError)


class TestQueryTimeoutCleanup:
    """Ensure timeout doesn't leave the context in a dirty state."""

    def test_deadline_cleared_after_timeout(self, star: Star) -> None:
        """After a timeout, the deadline is cleared so subsequent queries work."""
        with pytest.raises(QueryTimeoutError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=0.0,
            )
        # Deadline should be cleared
        assert star.context._query_deadline is None
        assert star.context._query_timeout_seconds is None

    def test_subsequent_query_works_after_timeout(self, star: Star) -> None:
        """After a timeout, the next query (without timeout) succeeds."""
        with pytest.raises(QueryTimeoutError):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=0.0,
            )
        # Next query should work fine
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 3

    def test_parameters_cleared_after_timeout(self, star: Star) -> None:
        """Parameters don't leak across queries after a timeout."""
        with pytest.raises(QueryTimeoutError):
            star.execute_query(
                "MATCH (p:Person) WHERE p.name = $name RETURN p.name",
                parameters={"name": "Alice"},
                timeout_seconds=0.0,
            )
        assert star.context._parameters == {}

    def test_shadow_state_cleared_after_timeout(self, star: Star) -> None:
        """Shadow write layers are cleaned up after timeout."""
        with pytest.raises(QueryTimeoutError):
            star.execute_query(
                "MATCH (p:Person) SET p.age = 99 RETURN p.name",
                timeout_seconds=0.0,
            )
        assert star.context._shadow == {}
        assert star.context._shadow_rels == {}


class TestContextDeadlineMethods:
    """Unit tests for Context.set_deadline / check_timeout / clear_deadline."""

    def test_set_deadline_with_timeout(self) -> None:
        ctx = Context()
        ctx.set_deadline(5.0)
        assert ctx._query_timeout_seconds == 5.0
        assert ctx._query_deadline is not None

    def test_set_deadline_none(self) -> None:
        ctx = Context()
        ctx.set_deadline(None)
        assert ctx._query_deadline is None
        assert ctx._query_timeout_seconds is None

    def test_check_timeout_no_deadline(self) -> None:
        """check_timeout is a no-op when no deadline is set."""
        ctx = Context()
        ctx.check_timeout()  # Should not raise

    def test_check_timeout_expired(self) -> None:
        ctx = Context()
        ctx.set_deadline(0.0)  # Immediate expiry
        with pytest.raises(QueryTimeoutError):
            ctx.check_timeout()

    def test_check_timeout_not_expired(self) -> None:
        ctx = Context()
        ctx.set_deadline(60.0)  # Generous
        ctx.check_timeout()  # Should not raise

    def test_clear_deadline(self) -> None:
        ctx = Context()
        ctx.set_deadline(5.0)
        ctx.clear_deadline()
        assert ctx._query_deadline is None
        assert ctx._query_timeout_seconds is None
        ctx.check_timeout()  # Should not raise after clear


class TestTimeoutEnvironmentVariable:
    """Test PYCYPHER_QUERY_TIMEOUT_S env var fallback."""

    def test_env_var_default_applies(self, star: Star) -> None:
        """When timeout_seconds is not passed, env var default is used."""
        with patch("pycypher.star._DEFAULT_TIMEOUT_S", 0.0):
            with pytest.raises(QueryTimeoutError):
                star.execute_query("MATCH (p:Person) RETURN p.name")

    def test_explicit_overrides_env_var(self, star: Star) -> None:
        """Explicit timeout_seconds takes precedence over env var."""
        with patch("pycypher.star._DEFAULT_TIMEOUT_S", 0.0):
            # Explicit generous timeout should override the 0.0 default
            result = star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=60.0,
            )
            assert len(result) == 3

    def test_negative_timeout_raises_value_error(self, star: Star) -> None:
        """Negative timeout raises ValueError, not QueryTimeoutError."""
        with pytest.raises(ValueError, match="non-negative"):
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=-1.0,
            )


class TestTimeoutWithMockClock:
    """Use mock to simulate time passing and verify per-clause checking."""

    def test_timeout_checked_per_clause(self, star: Star) -> None:
        """Verify check_timeout is called during clause iteration."""
        with patch.object(
            type(star.context),
            "check_timeout",
            wraps=star.context.check_timeout,
        ) as mock_check:
            star.execute_query(
                "MATCH (p:Person) RETURN p.name",
                timeout_seconds=60.0,
            )
            # At minimum, check should have been called for the MATCH clause
            assert mock_check.call_count >= 1
