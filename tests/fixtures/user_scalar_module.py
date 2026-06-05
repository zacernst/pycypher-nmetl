"""Fixture module for testing user-defined Cypher function registration.

Used by tests/test_user_scalar_functions_cli.py.  Defines plain scalar Python
functions that get auto-wrapped row-wise by ``register_user_function``.
"""

from __future__ import annotations


def shout(s):
    """Uppercase a string."""
    return str(s).upper()


def plus_n(x, n=1):
    """Add an integer offset (default 1)."""
    return int(x) + int(n)


def _private_helper(x):
    """Underscore-prefixed; should be skipped by ``names: '*'``."""
    return x
