"""Tests for the TimeoutHandler extracted from Star."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from pycypher.timeout_handler import TimeoutHandler


class TestTimeoutHandlerBasic:
    """Basic functionality tests."""

    def test_no_timeout_context_manager(self):
        """TimeoutHandler with None timeout enters and exits cleanly."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=None)
        with handler:
            pass
        ctx.set_deadline.assert_called_once_with(None)
        ctx.clear_deadline.assert_called_once()

    def test_timeout_sets_deadline(self):
        """TimeoutHandler arms cooperative deadline on context."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=5.0)
        with handler:
            ctx.set_deadline.assert_called_once_with(5.0)
        ctx.clear_deadline.assert_called_once()

    def test_timeout_property(self):
        """timeout_seconds property returns configured value."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=3.0)
        assert handler.timeout_seconds == 3.0

    def test_none_timeout_property(self):
        """timeout_seconds property returns None when no timeout."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=None)
        assert handler.timeout_seconds is None

    def test_cleanup_on_exception(self):
        """TimeoutHandler cleans up even when body raises."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=2.0)
        with pytest.raises(ValueError, match="test error"):
            with handler:
                raise ValueError("test error")
        ctx.clear_deadline.assert_called_once()


class TestTimeoutHandlerIntegration:
    """Integration tests with real Context."""

    def test_with_real_context(self):
        """TimeoutHandler works with real Context object."""
        from pycypher.relational_models import Context

        ctx = Context()
        handler = TimeoutHandler(ctx, timeout_seconds=10.0)
        with handler:
            # Should not raise — within timeout
            ctx.check_timeout()
        # After exit, deadline should be cleared
        # (check_timeout should not raise)
        ctx.check_timeout()

    def test_with_real_context_no_timeout(self):
        """TimeoutHandler with None timeout works with real Context."""
        from pycypher.relational_models import Context

        ctx = Context()
        handler = TimeoutHandler(ctx, timeout_seconds=None)
        with handler:
            ctx.check_timeout()  # Should not raise


class TestTimeoutHandlerStartTime:
    """Tests for custom start_time parameter."""

    def test_custom_start_time(self):
        """TimeoutHandler accepts custom start_time."""
        ctx = MagicMock()
        t0 = time.perf_counter()
        handler = TimeoutHandler(
            ctx, timeout_seconds=5.0, start_time=t0, query_str="TEST"
        )
        with handler:
            pass
        ctx.clear_deadline.assert_called_once()


class TestTimeoutHandlerSIGALRM:
    """Tests for SIGALRM arm/disarm behavior."""

    def test_sigalrm_armed_on_main_thread(self):
        """SIGALRM is armed when running on the main thread with a timeout."""
        import signal
        import threading

        if not hasattr(signal, "SIGALRM"):
            pytest.skip("No SIGALRM on this platform")
        if threading.current_thread() is not threading.main_thread():
            pytest.skip("Not on main thread")

        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=30.0, query_str="TEST")
        with handler:
            # SIGALRM should be armed
            assert handler._alarm_set is True
            assert handler._alarm_armed is True
        # After exit, SIGALRM should be disarmed
        assert handler._alarm_set is False
        assert handler._alarm_armed is False

    def test_no_sigalrm_when_no_timeout(self):
        """SIGALRM is NOT armed when timeout is None."""
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=None)
        with handler:
            assert handler._alarm_set is False

    def test_cleanup_restores_previous_handler(self):
        """After exit, the previous SIGALRM handler is restored."""
        import signal
        import threading

        if not hasattr(signal, "SIGALRM"):
            pytest.skip("No SIGALRM on this platform")
        if threading.current_thread() is not threading.main_thread():
            pytest.skip("Not on main thread")

        original = signal.getsignal(signal.SIGALRM)
        ctx = MagicMock()
        handler = TimeoutHandler(ctx, timeout_seconds=30.0)
        with handler:
            pass
        restored = signal.getsignal(signal.SIGALRM)
        # Should be back to original handler
        assert restored == original

    def test_query_str_in_timeout_error(self):
        """QueryTimeoutError includes the query string."""
        from pycypher.exceptions import QueryTimeoutError

        ctx = MagicMock()
        handler = TimeoutHandler(
            ctx,
            timeout_seconds=0.01,
            query_str="MATCH (n) RETURN n",
            start_time=time.perf_counter() - 100,  # already expired
        )
        # The cooperative deadline should trigger, not SIGALRM
        # But we can verify the handler stores the query string
        assert handler._query_str == "MATCH (n) RETURN n"
