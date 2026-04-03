"""LeapfrogTriejoin — worst-case optimal join algorithm for multi-way joins.

Implements the LeapfrogTriejoin algorithm from Veldhuizen 2014 for efficient
multi-way intersection joins. This is particularly effective for cyclic query
patterns (e.g., triangle queries) where pairwise binary joins produce large
intermediate results.

Architecture
~~~~~~~~~~~~

The algorithm operates on **sorted iterators** over relation columns. For a
multi-way join on a shared variable, instead of building pairwise hash tables,
all relations are intersected simultaneously via a "leapfrog" scan:

1. Sort each relation's join column.
2. Maintain a cursor per relation, always pointing at the current value.
3. Advance cursors in round-robin order, "leaping" past values that cannot
   appear in the intersection.

This yields worst-case optimal time complexity O(N^{w/2}) for w-way joins
on relations of size N, versus O(N^{w-1}) for iterated binary joins.

Integration
~~~~~~~~~~~

- Called from :class:`~pycypher.frame_joiner.FrameJoiner` when 3+ frames
  share a common join variable.
- Falls back to pairwise binary joins when the leapfrog strategy is not
  applicable (no shared variable across all frames, or only 2 frames).

References
~~~~~~~~~~

- Veldhuizen, T. L. (2014). "Leapfrog Triejoin: A Simple, Worst-Case
  Optimal Join Algorithm." ICDT 2014.

"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pycypher.binding_frame import BindingFrame

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Leapfrog iterator over a sorted array
# ---------------------------------------------------------------------------


class LeapfrogIterator:
    """Sorted-array iterator supporting seek (leapfrog) operations.

    Wraps a sorted numpy array and provides O(log N) seek via binary search.

    Args:
        values: A sorted 1-D numpy array of join keys.
        row_indices: Corresponding row indices into the source DataFrame.

    """

    __slots__ = ("_values", "_row_indices", "_pos", "_len")

    def __init__(self, values: np.ndarray, row_indices: np.ndarray) -> None:
        self._values = values
        self._row_indices = row_indices
        self._pos: int = 0
        self._len: int = len(values)

    @property
    def at_end(self) -> bool:
        """True when the iterator is exhausted."""
        return self._pos >= self._len

    @property
    def key(self) -> Any:
        """Current key value. Only valid when ``not at_end``."""
        return self._values[self._pos]

    def seek(self, target: Any) -> None:
        """Advance to the first position >= *target* via binary search.

        Args:
            target: The value to seek to.

        """
        if self._pos >= self._len:
            return
        # Fast path: already at or past target
        if self._values[self._pos] >= target:
            return
        # Binary search in remaining portion
        idx = np.searchsorted(self._values, target, side="left", sorter=None)
        self._pos = int(idx)

    def next(self) -> None:
        """Advance to the next position."""
        self._pos += 1

    def get_rows_for_key(self, key: Any) -> np.ndarray:
        """Return all row indices matching *key* at the current position.

        Scans forward from ``_pos`` while values equal *key*, returning
        the corresponding row indices.

        Args:
            key: The key to collect rows for.

        Returns:
            A numpy array of row indices.

        """
        start = self._pos
        end = start
        while end < self._len and self._values[end] == key:
            end += 1
        return self._row_indices[start:end]


# ---------------------------------------------------------------------------
# Core leapfrog intersection
# ---------------------------------------------------------------------------


def _leapfrog_intersect(iterators: list[LeapfrogIterator]) -> list[Any]:
    """Find all keys present in every iterator via the leapfrog algorithm.

    Args:
        iterators: List of LeapfrogIterator instances, each over a sorted
            array of join keys from one relation.

    Returns:
        List of key values present in the intersection of all iterators.

    Raises:
        RuntimeError: If the iteration bound is exceeded, indicating a bug
            in the seek/advance logic.

    """
    n = len(iterators)
    if n == 0:
        return []

    # Check if any iterator is empty
    if any(it.at_end for it in iterators):
        return []

    # Defensive iteration bound: the total number of distinct keys across all
    # iterators is an upper bound on loop iterations.  Each iteration either
    # finds an intersection key (advancing all iterators past it) or leapfrogs
    # the minimum iterator forward.  Both cases strictly advance at least one
    # cursor, so the loop cannot execute more times than the sum of all
    # iterator lengths.
    max_iterations = sum(it._len for it in iterators)

    # Sort iterators by their current key to establish round-robin order
    iterators.sort(key=lambda it: it.key)

    result_keys: list[Any] = []

    for _step in range(max_iterations):
        # The smallest current key is iterators[0].key
        # The largest current key is iterators[-1].key
        min_key = iterators[0].key
        max_key = iterators[-1].key

        if min_key == max_key:
            # All iterators agree — this key is in the intersection
            result_keys.append(min_key)
            # Advance all iterators past this key
            for it in iterators:
                it.seek(min_key)
                # Move past all duplicates of this key
                while not it.at_end and it.key == min_key:
                    it.next()
                if it.at_end:
                    return result_keys
            # Re-sort for next round
            iterators.sort(key=lambda it: it.key)
        else:
            # Seek the minimum iterator to the maximum key (leapfrog)
            iterators[0].seek(max_key)
            if iterators[0].at_end:
                return result_keys
            # Re-sort to maintain order
            iterators.sort(key=lambda it: it.key)

    # If we exhaust the iteration bound, something is wrong with the
    # seek/advance logic — report it rather than looping forever.
    LOGGER.error(
        "LeapfrogTriejoin: iteration bound (%d) exceeded with %d result keys",
        max_iterations,
        len(result_keys),
    )
    raise RuntimeError(
        f"LeapfrogTriejoin iteration bound ({max_iterations}) exceeded. "
        f"This indicates a bug in the seek/advance logic. "
        f"Found {len(result_keys)} intersection keys before failure."
    )


# ---------------------------------------------------------------------------
# Multi-way join result assembly
# ---------------------------------------------------------------------------


@dataclass
class _RelationInfo:
    """Metadata for one relation participating in the multi-way join."""

    frame_index: int
    join_col: str
    other_cols: list[str]
    sorted_keys: np.ndarray
    row_indices: np.ndarray
    source_df: pd.DataFrame


def _build_relation_infos(
    frames: list[BindingFrame],
    join_var: str,
) -> list[_RelationInfo]:
    """Prepare sorted key arrays and metadata for each frame.

    Args:
        frames: The BindingFrames to join.
        join_var: The shared variable name to join on.

    Returns:
        List of _RelationInfo, one per frame.

    """
    infos: list[_RelationInfo] = []
    for i, frame in enumerate(frames):
        df = frame.bindings
        col_values = df[join_var].values
        # Sort by join key, keeping track of original row indices
        sort_order = np.argsort(col_values, kind="mergesort")
        sorted_keys = col_values[sort_order]
        row_indices = sort_order  # positions in original df

        other_cols = [c for c in df.columns if c != join_var]
        infos.append(
            _RelationInfo(
                frame_index=i,
                join_col=join_var,
                other_cols=other_cols,
                sorted_keys=sorted_keys,
                row_indices=row_indices,
                source_df=df,
            ),
        )
    return infos


def _collect_matching_rows(
    infos: list[_RelationInfo],
    intersection_keys: list[Any],
) -> pd.DataFrame:
    """Build the result DataFrame from intersection keys.

    For each key in the intersection, collects matching rows from all
    relations and produces their Cartesian product (per-key).

    Args:
        infos: Relation metadata from :func:`_build_relation_infos`.
        intersection_keys: Keys present in all relations.

    Returns:
        A DataFrame with the join variable and all other columns from
        all relations.

    """
    if not intersection_keys:
        # Build empty frame with correct columns
        all_cols = [infos[0].join_col]
        for info in infos:
            all_cols.extend(info.other_cols)
        return pd.DataFrame(columns=all_cols)

    result_chunks: list[pd.DataFrame] = []
    join_col = infos[0].join_col

    for key in intersection_keys:
        # For each relation, find all rows matching this key
        row_sets: list[pd.DataFrame] = []
        for info in infos:
            # Binary search for key range in sorted array
            left = int(np.searchsorted(info.sorted_keys, key, side="left"))
            right = int(np.searchsorted(info.sorted_keys, key, side="right"))
            original_indices = info.row_indices[left:right]
            rows = info.source_df.iloc[original_indices]
            # Keep only the non-join columns
            if info.other_cols:
                row_sets.append(rows[info.other_cols].reset_index(drop=True))
            else:
                # No extra columns — just need the count for cross-product
                row_sets.append(pd.DataFrame({"_count": range(len(rows))}))

        # Cross-product all row sets for this key
        combined = row_sets[0]
        for j, rs in enumerate(row_sets[1:], 1):
            # Use unique synthetic column names to avoid collisions
            if "_count" in combined.columns:
                combined = combined.rename(
                    columns={"_count": f"_count_{j - 1}"}
                )
            if "_count" in rs.columns:
                rs = rs.rename(columns={"_count": f"_count_{j}"})
            combined = combined.merge(rs, how="cross")

        # Drop any synthetic _count columns
        count_cols = [c for c in combined.columns if c.startswith("_count")]
        if count_cols:
            combined = combined.drop(columns=count_cols)

        # Add the join key column
        combined[join_col] = key

        result_chunks.append(combined)

    if not result_chunks:
        all_cols = [join_col]
        for info in infos:
            all_cols.extend(info.other_cols)
        return pd.DataFrame(columns=all_cols)

    return pd.concat(result_chunks, ignore_index=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def leapfrog_triejoin(
    frames: list[BindingFrame],
    join_var: str,
) -> BindingFrame:
    """Perform a multi-way LeapfrogTriejoin on BindingFrames sharing *join_var*.

    This is the main entry point. It:

    1. Sorts each frame's join column.
    2. Runs the leapfrog intersection to find common keys.
    3. Assembles the result by cross-producing matching rows per key.

    Falls back gracefully for edge cases (empty frames, single frame).

    Args:
        frames: Two or more BindingFrames that all contain column *join_var*.
        join_var: The shared variable name to join on.

    Returns:
        A new BindingFrame with the intersection result.

    Raises:
        ValueError: If fewer than 2 frames are provided or *join_var* is
            missing from any frame.

    """
    from pycypher.binding_frame import BindingFrame

    if len(frames) < 2:
        msg = f"LeapfrogTriejoin requires at least 2 frames, got {len(frames)}"
        raise ValueError(msg)

    # Validate that all frames have the join variable
    for i, frame in enumerate(frames):
        if join_var not in frame.bindings.columns:
            msg = (
                f"Frame {i} does not contain join variable '{join_var}'. "
                f"Available columns: {list(frame.bindings.columns)}"
            )
            raise ValueError(msg)

    LOGGER.debug(
        "LeapfrogTriejoin: %d-way join on '%s' with frame sizes %s",
        len(frames),
        join_var,
        [len(f.bindings) for f in frames],
    )

    # Build sorted relation info
    infos = _build_relation_infos(frames, join_var)

    # Create leapfrog iterators and find intersection
    iterators = [
        LeapfrogIterator(info.sorted_keys, info.row_indices) for info in infos
    ]
    intersection_keys = _leapfrog_intersect(iterators)

    LOGGER.debug(
        "LeapfrogTriejoin: found %d intersection keys",
        len(intersection_keys),
    )

    # Assemble result
    result_df = _collect_matching_rows(infos, intersection_keys)

    # Merge type registries from all frames
    merged_registry: dict[str, str] = {}
    for frame in frames:
        merged_registry.update(frame.type_registry)

    return BindingFrame(
        bindings=result_df,
        type_registry=merged_registry,
        context=frames[0].context,
    )


def can_use_leapfrog(
    frames: list[BindingFrame],
) -> tuple[bool, str | None]:
    """Check whether LeapfrogTriejoin is applicable to the given frames.

    Returns ``(True, join_var)`` if all frames share at least one common
    variable, making them eligible for multi-way leapfrog join.

    Args:
        frames: The candidate BindingFrames.

    Returns:
        A tuple of (applicable, join_variable_name).

    """
    if len(frames) < 3:
        return False, None

    # Find variables common to ALL frames
    common_vars: set[str] | None = None
    for frame in frames:
        frame_vars = set(frame.var_names)
        if common_vars is None:
            common_vars = frame_vars
        else:
            common_vars &= frame_vars

    if not common_vars:
        return False, None

    # Pick the variable with lowest estimated cardinality (smallest domain)
    best_var = None
    best_card = float("inf")
    for var in common_vars:
        # Use the minimum unique count across frames as a heuristic
        min_unique = min(frame.bindings[var].nunique() for frame in frames)
        if min_unique < best_card:
            best_card = min_unique
            best_var = var

    return True, best_var
