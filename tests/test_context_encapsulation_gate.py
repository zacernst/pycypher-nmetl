"""Quality gate: only ``relational_models.py`` may reassign a ``Context`` field.

Per Phase 4 of ``IMPLEMENTATION_PLAN.md``, per-query state
(``_shadow``/``_shadow_rels``/``_parameters``/``_query_deadline``/
``_query_timeout_seconds``/``_memory_budget_bytes``) lives behind read-only
``@property`` accessors backed by ``execution_scope.ExecutionScope``.
Reassigning the whole attribute (``context._shadow = ...``) from outside
``relational_models.py`` raises ``AttributeError``; only in-place mutation of
the dict the property returns (``context._shadow[...] = ...``) is valid.
This test guards against reintroducing whole-attribute reassignment.
"""

from __future__ import annotations

import re
from pathlib import Path

_PYCYPHER_SRC = (
    Path(__file__).resolve().parent.parent
    / "packages"
    / "pycypher"
    / "src"
    / "pycypher"
)

#: Matches ``context._foo =`` / ``self.context._foo =`` etc., but not
#: ``==``, ``!=``, ``<=``, ``>=`` comparisons (those never reach a bare
#: ``\s*=`` because the extra operator character breaks adjacency, except
#: for ``==`` itself, excluded via the trailing negative lookahead).
_REASSIGNMENT_PATTERN = re.compile(r"context\._\w+\s*=(?!=)")


def test_no_whole_attribute_context_reassignment_outside_relational_models() -> None:
    offenders: list[str] = []
    for path in _PYCYPHER_SRC.rglob("*.py"):
        if path.name == "relational_models.py":
            continue
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _REASSIGNMENT_PATTERN.search(line):
                offenders.append(f"{path}:{lineno}: {line.strip()}")

    assert not offenders, (
        "Found whole-attribute Context reassignment outside "
        "relational_models.py (use in-place mutation or a Context setter "
        "method instead):\n" + "\n".join(offenders)
    )
