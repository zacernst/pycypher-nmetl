"""Utilities for normalising Arrow tables before they enter the pycypher pipeline.

All functions return a ``pa.Table`` with canonical column names:
- ``__ID__`` — unique row identifier
- ``__SOURCE__`` — source node ID for relationships
- ``__TARGET__`` — target node ID for relationships
"""

from __future__ import annotations

import pyarrow as pa

_RESERVED_COLS = {"__ID__", "__SOURCE__", "__TARGET__"}


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
        return table

    # Auto-generate sequential IDs
    if "__ID__" not in table.schema.names:
        ids = pa.array(range(len(table)), type=pa.int64())
        table = table.add_column(0, pa.field("__ID__", pa.int64()), ids)
    return table


def normalize_relationship_table(
    table: pa.Table,
    source_col: str,
    target_col: str,
    id_col: str | None = None,
) -> pa.Table:
    """Return *table* with ``__SOURCE__``, ``__TARGET__``, and ``__ID__`` columns.

    Args:
        table: Source Arrow table.
        source_col: Column to rename to ``__SOURCE__``.
        target_col: Column to rename to ``__TARGET__``.
        id_col: Column to rename to ``__ID__``.  If ``None``, a sequential
            integer column is prepended.

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

    # Handle __ID__
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
    elif "__ID__" not in table.schema.names:
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
