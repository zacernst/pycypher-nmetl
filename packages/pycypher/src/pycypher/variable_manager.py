"""Variable namespace management for Cypher query composition.

Provides conflict detection, systematic renaming, and binding preservation
when composing multiple Cypher queries that may share variable namespaces.

Architecture
------------

::

    VariableManager
    ├── collect_variables()      — gather all defined variable names from AST
    ├── detect_conflicts()       — find overlapping names between two ASTs
    ├── generate_unique_name()   — produce collision-free variable names
    └── rename_variables()       — deep-copy AST with variable names replaced

The rename operation is *immutable* — it returns a new AST tree and never
mutates the original.  This is critical for composability: the caller can
rename one branch of a UNION without affecting the other.
"""

from __future__ import annotations

import time
from typing import Any

from shared.logger import LOGGER

from pycypher.ast_models import ASTNode, Variable

# Upper bound on name-generation retries to prevent infinite loops when
# the ``existing`` set is pathologically large.
_MAX_NAME_GENERATION_ATTEMPTS: int = 10_000


class VariableManager:
    """Manage variable namespaces across Cypher query ASTs.

    Provides utilities for detecting variable conflicts between queries,
    generating unique variable names, and rewriting AST trees with
    renamed variables while preserving structural binding relationships.
    """

    def collect_variables(self, node: ASTNode) -> set[str]:
        """Collect all variable names defined in the AST subtree.

        Traverses the AST and extracts variable names from all
        :class:`Variable` nodes found anywhere in the tree.

        Args:
            node: Root of the AST subtree to scan.

        Returns:
            Set of variable name strings found in the subtree.

        """
        _t0 = time.perf_counter()
        variables = {v.name for v in node.find_all(Variable)}  # type: ignore[union-attr]  # find_all(Variable) returns Variable instances
        LOGGER.debug(
            "collect_variables: found=%d  elapsed=%.3fms",
            len(variables),
            (time.perf_counter() - _t0) * 1000,
        )
        return variables

    def detect_conflicts(
        self,
        query_a: ASTNode,
        query_b: ASTNode,
    ) -> set[str]:
        """Return variable names that appear in both AST subtrees.

        Args:
            query_a: First AST subtree.
            query_b: Second AST subtree.

        Returns:
            Set of variable names present in both subtrees.

        """
        conflicts = self.collect_variables(query_a) & self.collect_variables(
            query_b,
        )
        if conflicts:
            LOGGER.debug(
                "detect_conflicts: conflicts=%s",
                conflicts,
            )
        return conflicts

    def generate_unique_name(
        self,
        base_name: str,
        existing: set[str],
        prefix: str = "__v",
    ) -> str:
        """Generate a unique variable name that avoids collisions.

        Strategy: ``prefix + base_name``, with a numeric suffix appended
        if the candidate already exists in *existing*.

        Args:
            base_name: Original variable name to derive from.
            existing: Set of names that must not be reused.
            prefix: Prefix prepended to the base name.

        Returns:
            A collision-free variable name string.

        Raises:
            SecurityError: If no unique name is found within
                :data:`_MAX_NAME_GENERATION_ATTEMPTS` iterations.

        """
        candidate = f"{prefix}{base_name}"
        if candidate not in existing:
            return candidate
        for counter in range(1, _MAX_NAME_GENERATION_ATTEMPTS + 1):
            suffixed = f"{candidate}_{counter}"
            if suffixed not in existing:
                return suffixed

        from pycypher.exceptions import SecurityError

        msg = (
            f"Could not generate a unique variable name for '{base_name}' "
            f"after {_MAX_NAME_GENERATION_ATTEMPTS} attempts. "
            f"The existing namespace contains too many collisions."
        )
        raise SecurityError(msg)

    def rename_variables(
        self,
        node: ASTNode,
        rename_map: dict[str, str],
    ) -> ASTNode:
        """Return a new AST with variables renamed per the mapping.

        Deep-copies the AST tree, replacing every ``Variable.name`` that
        appears as a key in *rename_map* with the corresponding value.
        Variables not in the map are left unchanged.

        The original AST is **never mutated**.

        Args:
            node: Root AST node to rewrite.
            rename_map: Mapping from old variable name → new variable name.

        Returns:
            A new AST tree with renamed variables.

        Raises:
            SecurityError: If the AST exceeds the maximum nesting depth.

        """
        _t0 = time.perf_counter()
        LOGGER.debug(
            "rename_variables: renames=%d  mapping=%s",
            len(rename_map),
            rename_map,
        )
        if not rename_map:
            # No renames: return the original AST (structural sharing).
            # The caller contract is immutability, so sharing is safe.
            return node
        result = _deep_copy_ast(node, rename_map, 0)
        LOGGER.debug(
            "rename_variables: elapsed=%.3fms",
            (time.perf_counter() - _t0) * 1000,
        )
        return result


def _deep_copy_ast(
    node: ASTNode,
    rename_map: dict[str, str],
    depth: int,
) -> ASTNode:
    """Recursively deep-copy an AST node, applying variable renames.

    Uses structural sharing: subtrees that contain no variables needing
    renaming are returned as-is (no copy), saving memory and time for
    large ASTs where only a few variables are renamed.

    For each field on the Pydantic model:
    - If it's a Variable, apply the rename map to its name
    - If it's another ASTNode, recurse
    - If it's a list/tuple containing ASTNodes, recurse into elements
    - Otherwise, copy the value as-is

    Args:
        node: The AST node to copy.
        rename_map: Variable name substitutions to apply.
        depth: Current recursion depth for security limit enforcement.

    Returns:
        A new ASTNode with renames applied.

    Raises:
        SecurityError: If *depth* exceeds ``MAX_QUERY_NESTING_DEPTH``.

    """
    from pycypher.config import MAX_QUERY_NESTING_DEPTH

    if depth > MAX_QUERY_NESTING_DEPTH:
        from pycypher.exceptions import SecurityError

        msg = (
            f"AST deep-copy exceeded maximum nesting depth "
            f"({MAX_QUERY_NESTING_DEPTH}). The query AST is too deeply "
            f"nested. Adjust PYCYPHER_MAX_QUERY_NESTING_DEPTH to increase."
        )
        raise SecurityError(msg)

    if isinstance(node, Variable):
        new_name = rename_map.get(node.name, node.name)
        return Variable(name=new_name)

    # Build new field values for model_copy
    new_fields: dict[str, Any] = {}
    for field_name in type(node).model_fields:
        value = getattr(node, field_name)
        new_value = _copy_field_value(value, rename_map, depth + 1)
        if new_value is not value:
            new_fields[field_name] = new_value

    if new_fields:
        return node.model_copy(update=new_fields)
    return node.model_copy()


def _copy_field_value(
    value: Any,
    rename_map: dict[str, str],
    depth: int,
) -> Any:
    """Copy a single field value, recursing into AST nodes and collections.

    Args:
        value: The field value to process.
        rename_map: Variable name substitutions.
        depth: Current recursion depth for security limit enforcement.

    Returns:
        The copied value (may be the same object if no changes needed).

    """
    if isinstance(value, Variable):
        new_name = rename_map.get(value.name, value.name)
        return Variable(name=new_name)

    if isinstance(value, ASTNode):
        return _deep_copy_ast(value, rename_map, depth)

    if isinstance(value, list):
        return [_copy_field_value(item, rename_map, depth) for item in value]

    if isinstance(value, tuple):
        return tuple(
            _copy_field_value(item, rename_map, depth) for item in value
        )

    if isinstance(value, dict):
        return {
            k: _copy_field_value(v, rename_map, depth)
            for k, v in value.items()
        }

    return value
