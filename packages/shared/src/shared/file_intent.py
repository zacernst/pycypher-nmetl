"""File Intent Registration System for multi-agent coordination.

Provides structural enforcement to prevent file conflicts when multiple agents
work concurrently. Agents register intent before editing files; the system
classifies risk levels and warns on conflicts.

Risk Levels
-----------
- **HIGH**: Shared infrastructure files (``conftest.py``, ``__init__.py`` at
  package roots, ``base.py``, ``app.py``, ``pyproject.toml``).  Edits require
  explicit coordination.
- **MEDIUM**: Existing implementation files.  Concurrent edits generate a
  warning.
- **LOW**: New files that don't yet exist on disk.  No coordination needed.

Usage
-----
>>> from shared.file_intent import IntentRegistry
>>> registry = IntentRegistry()
>>> result = registry.register("agent-1", "packages/shared/src/shared/helpers.py", "add utility")
>>> result.risk
<RiskLevel.MEDIUM: 'medium'>
>>> result.conflicts
[]
"""

from __future__ import annotations

import fnmatch
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional


class RiskLevel(Enum):
    """File risk classification for concurrent editing."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# Glob patterns for high-risk files.  Matched against the path relative to
# the repository root (or the absolute path if no root is configured).
_DEFAULT_HIGH_RISK_PATTERNS: tuple[str, ...] = (
    "**/conftest.py",
    "**/base.py",
    "**/app.py",
    "**/__init__.py",
    "**/pyproject.toml",
    "**/*.lock",
)


@dataclass(frozen=True)
class Intent:
    """A single registered editing intent."""

    agent: str
    file_path: str
    description: str
    timestamp: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class RegistrationResult:
    """Outcome of registering a file intent."""

    intent: Intent
    risk: RiskLevel
    conflicts: list[Intent]

    @property
    def has_conflicts(self) -> bool:
        return len(self.conflicts) > 0


class IntentRegistry:
    """Thread-safe registry of file-editing intents.

    Parameters
    ----------
    high_risk_patterns
        Glob patterns identifying high-risk files.  Defaults to common shared
        infrastructure files.
    repo_root
        Optional repository root used to resolve relative paths and check
        file existence for risk classification.
    """

    def __init__(
        self,
        high_risk_patterns: tuple[str, ...] | None = None,
        repo_root: str | Path | None = None,
    ) -> None:
        self._high_risk_patterns = high_risk_patterns or _DEFAULT_HIGH_RISK_PATTERNS
        self._repo_root = Path(repo_root) if repo_root else None
        self._lock = threading.Lock()
        # file_path -> list of intents
        self._intents: dict[str, list[Intent]] = {}

    # ------------------------------------------------------------------
    # Risk assessment
    # ------------------------------------------------------------------

    def assess_risk(self, file_path: str) -> RiskLevel:
        """Classify the risk level for *file_path*.

        Parameters
        ----------
        file_path
            Path to the file (relative or absolute).

        Returns
        -------
        RiskLevel
            HIGH for shared infrastructure, LOW for non-existent files,
            MEDIUM otherwise.
        """
        normalized = self._normalize(file_path)

        # Check high-risk patterns first.  Match against both the full
        # normalized path and just the filename so that top-level files
        # like "pyproject.toml" match patterns like "**/pyproject.toml".
        name = Path(normalized).name
        for pattern in self._high_risk_patterns:
            pat_name = pattern.split("/")[-1] if "/" in pattern else pattern
            if fnmatch.fnmatch(normalized, pattern) or fnmatch.fnmatch(name, pat_name):
                return RiskLevel.HIGH

        # If we can resolve the path, check existence.
        resolved = self._resolve(file_path)
        if resolved is not None and not resolved.exists():
            return RiskLevel.LOW

        return RiskLevel.MEDIUM

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        agent: str,
        file_path: str,
        description: str = "",
    ) -> RegistrationResult:
        """Register an agent's intent to edit *file_path*.

        Returns a :class:`RegistrationResult` containing the risk assessment
        and any conflicting intents from other agents.
        """
        normalized = self._normalize(file_path)
        risk = self.assess_risk(normalized)
        intent = Intent(agent=agent, file_path=normalized, description=description)

        with self._lock:
            existing = self._intents.get(normalized, [])
            conflicts = [i for i in existing if i.agent != agent]
            self._intents.setdefault(normalized, []).append(intent)

        return RegistrationResult(intent=intent, risk=risk, conflicts=conflicts)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_intents(self, file_path: str) -> list[Intent]:
        """Return all registered intents for *file_path*."""
        normalized = self._normalize(file_path)
        with self._lock:
            return list(self._intents.get(normalized, []))

    def get_agent_intents(self, agent: str) -> list[Intent]:
        """Return all intents registered by *agent*."""
        with self._lock:
            return [
                intent
                for intents in self._intents.values()
                for intent in intents
                if intent.agent == agent
            ]

    def get_conflicts(self) -> dict[str, list[Intent]]:
        """Return all files with intents from multiple agents.

        Returns a dict mapping file paths to their intent lists, filtered
        to only files with >1 distinct agent.
        """
        with self._lock:
            return {
                path: list(intents)
                for path, intents in self._intents.items()
                if len({i.agent for i in intents}) > 1
            }

    # ------------------------------------------------------------------
    # Management
    # ------------------------------------------------------------------

    def release(self, agent: str, file_path: str) -> int:
        """Remove all intents by *agent* for *file_path*.

        Returns the number of intents removed.
        """
        normalized = self._normalize(file_path)
        with self._lock:
            before = self._intents.get(normalized, [])
            after = [i for i in before if i.agent != agent]
            removed = len(before) - len(after)
            if after:
                self._intents[normalized] = after
            elif normalized in self._intents:
                del self._intents[normalized]
            return removed

    def release_all(self, agent: str) -> int:
        """Remove all intents by *agent* across all files.

        Returns the number of intents removed.
        """
        total = 0
        with self._lock:
            to_delete = []
            for path, intents in self._intents.items():
                before = len(intents)
                self._intents[path] = [i for i in intents if i.agent != agent]
                total += before - len(self._intents[path])
                if not self._intents[path]:
                    to_delete.append(path)
            for path in to_delete:
                del self._intents[path]
        return total

    def clear(self) -> None:
        """Remove all registered intents."""
        with self._lock:
            self._intents.clear()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> dict[str, list[dict[str, str]]]:
        """Return a serialisable summary of all registered intents."""
        with self._lock:
            return {
                path: [
                    {"agent": i.agent, "description": i.description}
                    for i in intents
                ]
                for path, intents in self._intents.items()
            }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _normalize(self, file_path: str) -> str:
        """Normalize *file_path* for consistent dictionary keys."""
        # Strip leading ./ and normalize separators.
        p = Path(file_path)
        if self._repo_root and p.is_absolute():
            try:
                p = p.relative_to(self._repo_root)
            except ValueError:
                pass
        return str(p)

    def _resolve(self, file_path: str) -> Optional[Path]:
        """Try to resolve *file_path* to an absolute path."""
        p = Path(file_path)
        if p.is_absolute():
            return p
        if self._repo_root:
            return self._repo_root / p
        return None
