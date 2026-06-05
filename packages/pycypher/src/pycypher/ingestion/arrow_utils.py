"""Utilities for normalising Arrow tables before they enter the pycypher pipeline.

All functions return a ``pa.Table`` with canonical column names:
- ``__ID__`` — unique row identifier
- ``__SOURCE__`` — source node ID for relationships
- ``__TARGET__`` — target node ID for relationships
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from shared.logger import LOGGER

_RESERVED_COLS = {"__ID__", "__SOURCE__", "__TARGET__"}


def _dedup_on_id(table: pa.Table, *, kind: str) -> pa.Table:
    """Return *table* keeping only the first row per unique ``__ID__`` value.

    Logs a warning when duplicates are dropped: a row's ``__ID__`` must be
    unique, and silent dedup would hide the cause of downstream blowup.
    *kind* is ``"entity"`` or ``"relationship"`` — used only in the warning.
    """
    n_before = table.num_rows
    if n_before <= 1:
        return table
    ids = table.column("__ID__").to_numpy(zero_copy_only=False)
    _, first_indices = np.unique(ids, return_index=True)
    if first_indices.size == n_before:
        return table
    first_indices.sort()  # preserve original row order
    table = table.take(pa.array(first_indices))
    n_after = table.num_rows
    noun = "entity" if kind == "entity" else "relationship"
    LOGGER.warning(
        "normalize_%s_table: dropped %d duplicate __ID__ rows (%d → %d). "
        "A %s's __ID__ must be unique; the first occurrence is kept. "
        "If you loaded a fact table at the wrong grain, project to the %s "
        "grain via the source's `query` field (e.g. SELECT DISTINCT ...).",
        kind,
        n_before - n_after,
        n_before,
        n_after,
        noun,
        noun,
    )
    return table


def _dedup_endpoints(table: pa.Table) -> pa.Table:
    """Return *table* keeping only the first row per unique ``(__SOURCE__, __TARGET__)`` pair.

    This collapses parallel edges. Callers that need to preserve multi-edge
    semantics should pass ``allow_multi_edges=True`` to
    :func:`normalize_relationship_table` and skip this step.
    """
    n_before = table.num_rows
    if n_before <= 1:
        return table
    # pandas handles mixed dtypes for composite-key dedup more cleanly than
    # numpy's 2D np.unique, which is fussy with object dtype.
    pairs_df = table.select(["__SOURCE__", "__TARGET__"]).to_pandas()
    is_dup = pairs_df.duplicated(
        subset=["__SOURCE__", "__TARGET__"],
        keep="first",
    ).to_numpy()
    if not is_dup.any():
        return table
    keep_indices = np.nonzero(~is_dup)[0]
    table = table.take(pa.array(keep_indices))
    n_after = table.num_rows
    LOGGER.warning(
        "normalize_relationship_table: collapsed %d duplicate "
        "(__SOURCE__, __TARGET__) edges (%d → %d). "
        "If parallel edges are intentional, set `allow_multi_edges: true` on "
        "the relationship source.",
        n_before - n_after,
        n_before,
        n_after,
    )
    return table


def normalize_entity_table(
    table: pa.Table,
    id_col: str | None = None,
) -> pa.Table:
    """Return *table* with an ``__ID__`` column as the first column.

    Args:
        table: Source Arrow table.
        id_col: Column to rename to ``__ID__``.  If ``None`` or not present,
            a sequential integer column is prepended.

    Returns:
        Arrow table whose first column is ``__ID__``.

    Raises:
        ValueError: If *id_col* is specified but does not exist in *table*.

    """
    if id_col is not None:
        if id_col not in table.schema.names:
            msg = f"id_col {id_col!r} not found in table columns: {table.schema.names}"
            raise ValueError(
                msg,
            )
        # Rename the specified column to __ID__
        idx = table.schema.names.index(id_col)
        new_names = list(table.schema.names)
        new_names[idx] = "__ID__"
        table = table.rename_columns(new_names)
        # Move __ID__ to front if it isn't already
        if table.schema.names[0] != "__ID__":
            cols = table.schema.names
            reordered = ["__ID__"] + [c for c in cols if c != "__ID__"]
            table = table.select(reordered)
        # Dedup on __ID__: an entity row must be uniquely identified by its
        # __ID__. Without this, sources loaded at the wrong grain (e.g. a
        # crosswalk loaded as the State entity) cause join row-count blowup.
        return _dedup_on_id(table, kind="entity")

    # Auto-generate sequential IDs — already unique by construction.
    if "__ID__" not in table.schema.names:
        ids = pa.array(range(len(table)), type=pa.int64())
        table = table.add_column(0, pa.field("__ID__", pa.int64()), ids)
    return table


def normalize_relationship_table(
    table: pa.Table,
    source_col: str,
    target_col: str,
    id_col: str | None = None,
    *,
    allow_multi_edges: bool = False,
) -> pa.Table:
    """Return *table* with ``__SOURCE__``, ``__TARGET__``, and ``__ID__`` columns.

    By default, parallel edges (rows with the same ``(__SOURCE__,
    __TARGET__)`` pair) are collapsed to a single edge.  Pass
    ``allow_multi_edges=True`` to preserve them — this is appropriate when
    each row represents a distinct logical relationship instance (e.g.
    individual transactions between two accounts).

    Args:
        table: Source Arrow table.
        source_col: Column to rename to ``__SOURCE__``.
        target_col: Column to rename to ``__TARGET__``.
        id_col: Column to rename to ``__ID__``.  If ``None``, a sequential
            integer column is prepended (after any dedup).
        allow_multi_edges: When ``False`` (default), rows with duplicate
            ``(__SOURCE__, __TARGET__)`` pairs are collapsed.  When ``True``,
            parallel edges are preserved.

    Returns:
        Arrow table with ``__ID__``, ``__SOURCE__``, and ``__TARGET__`` columns.

    Raises:
        ValueError: If *source_col* or *target_col* are missing from *table*.

    """
    names = table.schema.names
    if source_col not in names:
        msg = f"source_col {source_col!r} not found in table columns: {names}"
        raise ValueError(
            msg,
        )
    if target_col not in names:
        msg = f"target_col {target_col!r} not found in table columns: {names}"
        raise ValueError(
            msg,
        )

    new_names = list(names)
    new_names[new_names.index(source_col)] = "__SOURCE__"
    new_names[new_names.index(target_col)] = "__TARGET__"
    table = table.rename_columns(new_names)

    if id_col is not None:
        if id_col not in table.schema.names:
            msg = f"id_col {id_col!r} not found in table columns: {table.schema.names}"
            raise ValueError(
                msg,
            )
        idx = table.schema.names.index(id_col)
        final_names = list(table.schema.names)
        final_names[idx] = "__ID__"
        table = table.rename_columns(final_names)
        # __ID__ provided by the caller must be unique — same contract as entities.
        table = _dedup_on_id(table, kind="relationship")

    # Default behaviour: collapse duplicate (source, target) pairs.  Done
    # before assigning sequential __ID__s so the IDs stay contiguous.
    if not allow_multi_edges:
        table = _dedup_endpoints(table)

    if "__ID__" not in table.schema.names:
        ids = pa.array(range(len(table)), type=pa.int64())
        table = table.add_column(0, pa.field("__ID__", pa.int64()), ids)

    return table


def infer_attribute_map(table: pa.Table) -> dict[str, str]:
    """Return ``{col: col}`` for every non-reserved column in *table*.

    Reserved columns (``__ID__``, ``__SOURCE__``, ``__TARGET__``) are excluded.

    Args:
        table: Arrow table with normalised column names.

    Returns:
        Attribute map suitable for passing to ``EntityTable`` or
        ``RelationshipTable``.

    """
    return {
        col: col for col in table.schema.names if col not in _RESERVED_COLS
    }
