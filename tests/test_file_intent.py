"""Tests for shared.file_intent — File Intent Registration System."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from shared.file_intent import Intent, IntentRegistry, RegistrationResult, RiskLevel


# ---------------------------------------------------------------------------
# Risk assessment
# ---------------------------------------------------------------------------


class TestRiskAssessment:
    """Risk level classification for different file types."""

    def test_conftest_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("tests/conftest.py") == RiskLevel.HIGH

    def test_init_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("packages/shared/src/shared/__init__.py") == RiskLevel.HIGH

    def test_base_py_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("src/base.py") == RiskLevel.HIGH

    def test_app_py_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("packages/tui/app.py") == RiskLevel.HIGH

    def test_pyproject_toml_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("pyproject.toml") == RiskLevel.HIGH

    def test_lock_file_is_high_risk(self) -> None:
        registry = IntentRegistry()
        assert registry.assess_risk("uv.lock") == RiskLevel.HIGH

    def test_existing_file_is_medium_risk(self) -> None:
        registry = IntentRegistry(repo_root=Path(__file__).parent.parent)
        # This test file itself exists, so it should be medium risk.
        assert registry.assess_risk("tests/test_file_intent.py") == RiskLevel.MEDIUM

    def test_nonexistent_file_is_low_risk(self) -> None:
        registry = IntentRegistry(repo_root=Path(__file__).parent.parent)
        assert registry.assess_risk("does_not_exist_xyz.py") == RiskLevel.LOW

    def test_custom_high_risk_patterns(self) -> None:
        registry = IntentRegistry(high_risk_patterns=("**/secret.py",))
        assert registry.assess_risk("src/secret.py") == RiskLevel.HIGH
        # Default patterns no longer apply.
        assert registry.assess_risk("conftest.py") != RiskLevel.HIGH


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    """Core registration and conflict detection."""

    def test_register_returns_result(self) -> None:
        registry = IntentRegistry()
        result = registry.register("agent-1", "foo.py", "add feature")
        assert isinstance(result, RegistrationResult)
        assert result.intent.agent == "agent-1"
        assert result.intent.file_path == "foo.py"
        assert result.intent.description == "add feature"

    def test_no_conflict_same_agent(self) -> None:
        registry = IntentRegistry()
        registry.register("agent-1", "foo.py", "first edit")
        result = registry.register("agent-1", "foo.py", "second edit")
        assert not result.has_conflicts

    def test_conflict_different_agents(self) -> None:
        registry = IntentRegistry()
        registry.register("agent-1", "foo.py", "edit A")
        result = registry.register("agent-2", "foo.py", "edit B")
        assert result.has_conflicts
        assert len(result.conflicts) == 1
        assert result.conflicts[0].agent == "agent-1"

    def test_no_conflict_different_files(self) -> None:
        registry = IntentRegistry()
        registry.register("agent-1", "foo.py")
        result = registry.register("agent-2", "bar.py")
        assert not result.has_conflicts

    def test_multiple_conflicts(self) -> None:
        registry = IntentRegistry()
        registry.register("agent-1", "shared.py")
        registry.register("agent-2", "shared.py")
        result = registry.register("agent-3", "shared.py")
        assert len(result.conflicts) == 2


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


class TestQueries:
    """Intent querying methods."""

    def test_get_intents_empty(self) -> None:
        registry = IntentRegistry()
        assert registry.get_intents("foo.py") == []

    def test_get_intents_returns_all(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        registry.register("b", "foo.py")
        assert len(registry.get_intents("foo.py")) == 2

    def test_get_agent_intents(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        registry.register("a", "bar.py")
        registry.register("b", "baz.py")
        assert len(registry.get_agent_intents("a")) == 2
        assert len(registry.get_agent_intents("b")) == 1

    def test_get_conflicts_empty(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        assert registry.get_conflicts() == {}

    def test_get_conflicts_populated(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "shared.py")
        registry.register("b", "shared.py")
        registry.register("a", "solo.py")
        conflicts = registry.get_conflicts()
        assert "shared.py" in conflicts
        assert "solo.py" not in conflicts


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------


class TestRelease:
    """Intent release and cleanup."""

    def test_release_specific_file(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        registry.register("a", "bar.py")
        removed = registry.release("a", "foo.py")
        assert removed == 1
        assert registry.get_intents("foo.py") == []
        assert len(registry.get_intents("bar.py")) == 1

    def test_release_nonexistent(self) -> None:
        registry = IntentRegistry()
        assert registry.release("a", "foo.py") == 0

    def test_release_all(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        registry.register("a", "bar.py")
        registry.register("b", "foo.py")
        removed = registry.release_all("a")
        assert removed == 2
        assert registry.get_agent_intents("a") == []
        assert len(registry.get_intents("foo.py")) == 1  # b's intent remains

    def test_clear(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "foo.py")
        registry.register("b", "bar.py")
        registry.clear()
        assert registry.get_conflicts() == {}
        assert registry.get_intents("foo.py") == []


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


class TestSummary:
    """Summary serialization."""

    def test_summary_format(self) -> None:
        registry = IntentRegistry()
        registry.register("agent-1", "foo.py", "add feature")
        summary = registry.summary()
        assert "foo.py" in summary
        assert summary["foo.py"] == [{"agent": "agent-1", "description": "add feature"}]


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Concurrent access does not corrupt state."""

    def test_concurrent_registrations(self) -> None:
        registry = IntentRegistry()
        errors: list[Exception] = []

        def register_many(agent: str) -> None:
            try:
                for i in range(50):
                    registry.register(agent, f"file_{i}.py", f"edit {i}")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=register_many, args=(f"agent-{n}",)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # Each of 4 agents registered 50 files.
        for n in range(4):
            assert len(registry.get_agent_intents(f"agent-{n}")) == 50


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------


class TestNormalization:
    """Path normalization for consistent key matching."""

    def test_leading_dot_slash_stripped(self) -> None:
        registry = IntentRegistry()
        registry.register("a", "./foo.py")
        assert len(registry.get_intents("foo.py")) == 1

    def test_absolute_path_relative_to_root(self) -> None:
        registry = IntentRegistry(repo_root="/repo")
        registry.register("a", "/repo/src/foo.py")
        assert len(registry.get_intents("src/foo.py")) == 1
