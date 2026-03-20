"""Docstring completeness test for scalar_functions.py.

Every function registered with ``ScalarFunctionRegistry`` must be mentioned
in the module-level docstring of ``scalar_functions``.  This acts as a
living contract: add a function → update the docstring.

TDD: the test is written before the docstring update.
"""

from __future__ import annotations

import pycypher.scalar_functions as _sf_mod
from pycypher.scalar_functions import ScalarFunctionRegistry

# Functions that are intentionally internal / not user-facing; exclude
# from the docstring completeness check.
_INTERNAL_FUNCTIONS: frozenset[str] = frozenset(
    {
        "exists",  # handled at the AST/evaluator level, not a "scalar function"
        "id",  # identity accessor, not a standard Neo4j scalar
    }
)


def test_all_registered_functions_appear_in_module_docstring() -> None:
    """Every public built-in function name appears in the module docstring."""
    registry = ScalarFunctionRegistry.get_instance()
    docstring = (_sf_mod.__doc__ or "").lower()

    missing = []
    for name_lower in registry._builtin_names:
        if name_lower in _INTERNAL_FUNCTIONS:
            continue
        if name_lower not in docstring:
            missing.append(name_lower)

    assert not missing, (
        f"The following registered function(s) are not mentioned in the "
        f"scalar_functions.py module docstring:\n"
        + "\n".join(f"  - {n}" for n in sorted(missing))
    )
