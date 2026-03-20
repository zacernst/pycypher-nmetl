"""Neo4j sink: write pycypher query results to a Neo4j graph database.

All writes use ``MERGE`` semantics, making every call idempotent — safe to
re-run as part of a recurring ETL pipeline.  Relationship endpoint nodes are
located with ``MATCH``, so nodes must be written before relationships.

Quick start::

    import pandas as pd
    from pycypher import ContextBuilder, Star
    from pycypher.sinks.neo4j import Neo4jSink, NodeMapping, RelationshipMapping

    context = ContextBuilder().add_entity("Person", persons_df).build()
    result = Star(context=context).execute_query(
        "MATCH (p:Person) RETURN p.__ID__ AS pid, p.name AS name"
    )

    with Neo4jSink("bolt://localhost:7687", "neo4j", "pycypher") as sink:
        sink.write_nodes(
            result,
            NodeMapping(
                label="Person",
                id_column="pid",
                property_columns={"name": "name"},
            ),
        )

Requires the ``neo4j`` extra::

    pip install 'pycypher[neo4j]'
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Self

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, field_validator
from shared.logger import LOGGER

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

try:
    from neo4j import Driver as _Neo4jDriver
    from neo4j import GraphDatabase
except ImportError as _import_err:
    _msg = "Install the neo4j extra to use this sink: pip install 'pycypher[neo4j]'"
    raise ImportError(_msg) from _import_err


# ---------------------------------------------------------------------------
# Mapping models
# ---------------------------------------------------------------------------


class NodeMapping(BaseModel):
    """Describes how DataFrame columns map onto a Neo4j node label.

    The ``id_column`` is used as the MERGE key — its value becomes the
    ``id_property`` on the node.  ``property_columns`` maps each Neo4j
    property name to the DataFrame column that supplies its value.

    Example::

        NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "full_name", "age": "age"},
        )
    """

    label: str = Field(description="Neo4j node label, e.g. 'Person'.")
    id_column: str = Field(
        description="DataFrame column used as the MERGE key.",
    )
    id_property: str = Field(
        default="id",
        description="Neo4j property name written as the merge key.",
    )
    property_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of {neo4j_property_name: dataframe_column_name}.",
    )

    @field_validator("label")
    @classmethod
    def _validate_label(cls, v: str) -> str:
        _validate_cypher_identifier(v, "label")
        return v

    @field_validator("id_property")
    @classmethod
    def _validate_id_property(cls, v: str) -> str:
        _validate_cypher_identifier(v, "id_property")
        return v


class RelationshipMapping(BaseModel):
    """Describes how DataFrame columns map onto a Neo4j relationship type.

    Both endpoint nodes are located with ``MATCH`` using their respective
    id columns, so the nodes must already exist in the database.

    Example::

        RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src_pid",
            target_id_column="tgt_pid",
            property_columns={"since": "since_year"},
        )
    """

    rel_type: str = Field(description="Neo4j relationship type, e.g. 'KNOWS'.")
    source_label: str = Field(description="Label of the source node.")
    target_label: str = Field(description="Label of the target node.")
    source_id_column: str = Field(
        description="DataFrame column containing the source node merge-key value.",
    )
    target_id_column: str = Field(
        description="DataFrame column containing the target node merge-key value.",
    )
    source_id_property: str = Field(
        default="id",
        description="Neo4j property name of the source node merge key.",
    )
    target_id_property: str = Field(
        default="id",
        description="Neo4j property name of the target node merge key.",
    )
    property_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of {neo4j_property_name: dataframe_column_name}.",
    )

    @field_validator("rel_type")
    @classmethod
    def _validate_rel_type(cls, v: str) -> str:
        _validate_cypher_identifier(v, "rel_type")
        return v

    @field_validator("source_label")
    @classmethod
    def _validate_source_label(cls, v: str) -> str:
        _validate_cypher_identifier(v, "source_label")
        return v

    @field_validator("target_label")
    @classmethod
    def _validate_target_label(cls, v: str) -> str:
        _validate_cypher_identifier(v, "target_label")
        return v

    @field_validator("source_id_property")
    @classmethod
    def _validate_source_id_property(cls, v: str) -> str:
        _validate_cypher_identifier(v, "source_id_property")
        return v

    @field_validator("target_id_property")
    @classmethod
    def _validate_target_id_property(cls, v: str) -> str:
        _validate_cypher_identifier(v, "target_id_property")
        return v


# ---------------------------------------------------------------------------
# Type coercion helpers
# ---------------------------------------------------------------------------


def _coerce_value(value: Any) -> Any:
    """Convert a single value to a Python type the Neo4j driver accepts.

    The driver rejects NumPy scalars and ``pandas.Timestamp`` objects.
    ``NaN`` / ``None`` are normalised to ``None`` so callers can filter
    them out cleanly.

    Args:
        value: Any scalar value, possibly a NumPy or pandas type.

    Returns:
        A plain Python int / float / bool / datetime / str, or ``None``.

    """
    # Null check first: np.float64(nan) is both np.floating AND isna, but we
    # want to return None rather than float('nan') so callers can filter it.
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *row* with all values coerced to driver-safe types.

    Args:
        row: Flat dict of column names to raw values.

    Returns:
        New dict with the same keys and coerced values.

    """
    return {k: _coerce_value(v) for k, v in row.items()}


def _drop_nulls(mapping: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *mapping* with all ``None``-valued entries removed.

    Args:
        mapping: Dict potentially containing ``None`` values.

    Returns:
        New dict with only non-``None`` entries.

    """
    return {k: v for k, v in mapping.items() if v is not None}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_cypher_identifier(name: str, field_name: str) -> None:
    """Raise ``ValueError`` if *name* is unsafe to interpolate as a Cypher identifier.

    Backtick-quoting provides no protection when the identifier itself contains
    a backtick — the second backtick closes the quoted name, and the remainder
    executes as free Cypher.

    Beyond the backtick, we also reject:

    * **NUL byte** (``\\x00``) — cannot be safely transmitted.
    * **Curly braces** (``{}``) — syntactically significant in Cypher property
      maps and could alter query semantics.
    * **Square brackets** (``[]``) — used in some Cypher dialects for quoting
      and could be exploited to break out of backtick-quoting.
    * **Backslashes** (``\\``) — escape characters that could alter quoting
      semantics.
    * **Unicode confusables** — characters that normalise (NFKC) to one of the
      above dangerous characters.  For example, U+FF40 (fullwidth grave accent)
      normalises to a regular backtick.

    Args:
        name: Candidate identifier value.
        field_name: Human-readable field name used in error messages.

    Raises:
        ValueError: If *name* is empty or contains any unsafe character.

    """
    import unicodedata

    if not name:
        msg = f"{field_name!r} must not be empty."
        raise ValueError(msg)

    # Apply NFKC normalisation to catch Unicode lookalikes (e.g. fullwidth
    # grave accent U+FF40 → backtick, fullwidth left curly bracket U+FF5B → {).
    normalised = unicodedata.normalize("NFKC", name)

    # Characters that are dangerous inside backtick-quoted Cypher identifiers.
    _DANGEROUS_CHARS: dict[str, str] = {
        "`": "backtick",
        "\x00": "NUL byte",
        "{": "curly brace",
        "}": "curly brace",
        "[": "square bracket",
        "]": "square bracket",
        "\\": "backslash",
    }

    for char, char_name in _DANGEROUS_CHARS.items():
        if char in normalised:
            msg = (
                f"{field_name!r} contains a {char_name} character (or a Unicode "
                f"equivalent that normalises to one), which cannot be safely "
                f"backtick-quoted and would allow Cypher injection. Got: {name!r}"
            )
            raise ValueError(msg)


def _validate_columns(
    df: pd.DataFrame,
    required: list[str],
    context: str,
) -> None:
    """Raise ``ValueError`` if any *required* column is absent from *df*.

    Args:
        df: DataFrame to inspect.
        required: Column names that must be present.
        context: Short description included in the error message (e.g. the
            calling method and label name) to aid debugging.

    Raises:
        ValueError: If one or more required columns are missing.

    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        available = list(df.columns)
        msg = (
            f"{context}: column(s) {missing!r} not found in DataFrame. "
            f"Available columns: {available!r}"
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Cypher template builders
# ---------------------------------------------------------------------------


def _node_merge_cypher(label: str, id_property: str) -> str:
    """Return the Cypher template for a batched node MERGE.

    Args:
        label: Neo4j node label (backtick-quoted in the output).
        id_property: Property name used as the MERGE key.

    Returns:
        Cypher string with ``$rows`` parameter expected as a list of
        ``{id: …, properties: {…}}`` dicts.

    """
    return (
        f"UNWIND $rows AS row\n"
        f"MERGE (n:`{label}` {{`{id_property}`: row.id}})\n"
        f"SET n += row.properties"
    )


def _rel_merge_cypher(
    src_label: str,
    tgt_label: str,
    rel_type: str,
    src_id_prop: str,
    tgt_id_prop: str,
) -> str:
    """Return the Cypher template for a batched relationship MERGE.

    Endpoint nodes are located with ``MATCH`` (not ``MERGE``), so nodes
    must exist before this query runs.

    Args:
        src_label: Label of the source node.
        tgt_label: Label of the target node.
        rel_type: Relationship type (backtick-quoted in the output).
        src_id_prop: Neo4j property name of the source merge key.
        tgt_id_prop: Neo4j property name of the target merge key.

    Returns:
        Cypher string with ``$rows`` parameter expected as a list of
        ``{src_id: …, tgt_id: …, properties: {…}}`` dicts.

    """
    return (
        f"UNWIND $rows AS row\n"
        f"MATCH (src:`{src_label}` {{`{src_id_prop}`: row.src_id}})\n"
        f"MATCH (tgt:`{tgt_label}` {{`{tgt_id_prop}`: row.tgt_id}})\n"
        f"MERGE (src)-[r:`{rel_type}`]->(tgt)\n"
        f"SET r += row.properties"
    )


# ---------------------------------------------------------------------------
# Row serialisers
# ---------------------------------------------------------------------------


def _build_node_rows(
    df: pd.DataFrame,
    mapping: NodeMapping,
) -> list[dict[str, Any]]:
    """Serialise *df* into the row format expected by :func:`_node_merge_cypher`.

    Rows where ``mapping.id_column`` is ``None`` / ``NaN`` are skipped with
    a warning.  Property values that are ``None`` after coercion are excluded
    from the ``properties`` sub-dict (Neo4j property keys must have values).

    Uses ``DataFrame.to_dict('records')`` (Cython-level) rather than
    ``iterrows()`` to eliminate per-row ``pd.Series`` construction overhead.
    Typical speedup: 4–5× on 10k-row batches.

    Args:
        df: Slice of the result DataFrame.
        mapping: Node mapping configuration.

    Returns:
        List of ``{id: …, properties: {…}}`` dicts ready to pass as
        ``$rows`` to the Cypher template.

    """
    id_col = mapping.id_column
    # --- 1. Vectorized null filter ---
    null_mask: pd.Series = df[id_col].isna()
    null_count = int(null_mask.sum())
    if null_count:
        LOGGER.warning(
            f"Skipping {null_count} row(s): null value in id_column={id_col!r}",
        )
    df_clean = df[~null_mask]
    if df_clean.empty:
        return []

    # --- 2. Vectorized record extraction (Cython-level, no Series per row) ---
    prop_col_names = list(mapping.property_columns.values())  # df column names
    prop_neo4j_names = list(
        mapping.property_columns.keys(),
    )  # neo4j prop names
    cols_needed = [id_col, *prop_col_names]
    records: list[dict[str, Any]] = df_clean[cols_needed].to_dict("records")

    # --- 3. Coerce values and build result (cheap dict access, not Series) ---
    rows: list[dict[str, Any]] = []
    for rec in records:
        id_val = _coerce_value(rec[id_col])
        if id_val is None:
            continue  # safety guard: should not happen after null filter
        props = _drop_nulls(
            {
                neo4j_prop: _coerce_value(rec[df_col])
                for neo4j_prop, df_col in zip(
                    prop_neo4j_names,
                    prop_col_names,
                    strict=False,
                )
            },
        )
        rows.append({"id": id_val, "properties": props})
    return rows


def _build_rel_rows(
    df: pd.DataFrame,
    mapping: RelationshipMapping,
) -> list[dict[str, Any]]:
    """Serialise *df* into the row format expected by :func:`_rel_merge_cypher`.

    Rows where either endpoint ID is ``None`` / ``NaN`` are skipped with a
    warning.

    Uses ``DataFrame.to_dict('records')`` (Cython-level) rather than
    ``iterrows()`` to eliminate per-row ``pd.Series`` construction overhead.
    Typical speedup: 4–5× on 10k-row batches.

    Args:
        df: Slice of the result DataFrame.
        mapping: Relationship mapping configuration.

    Returns:
        List of ``{src_id: …, tgt_id: …, properties: {…}}`` dicts ready to
        pass as ``$rows`` to the Cypher template.

    """
    src_col = mapping.source_id_column
    tgt_col = mapping.target_id_column

    # --- 1. Vectorized null filter ---
    null_mask: pd.Series = df[src_col].isna() | df[tgt_col].isna()
    null_count = int(null_mask.sum())
    if null_count:
        LOGGER.warning(
            f"Skipping {null_count} row(s): null endpoint id(s) in src_col={src_col!r} or tgt_col={tgt_col!r}",
        )
    df_clean = df[~null_mask]
    if df_clean.empty:
        return []

    # --- 2. Vectorized record extraction ---
    prop_col_names = list(mapping.property_columns.values())
    prop_neo4j_names = list(mapping.property_columns.keys())
    cols_needed = [src_col, tgt_col, *prop_col_names]
    records: list[dict[str, Any]] = df_clean[cols_needed].to_dict("records")

    # --- 3. Coerce and build result ---
    rows: list[dict[str, Any]] = []
    for rec in records:
        src_id = _coerce_value(rec[src_col])
        tgt_id = _coerce_value(rec[tgt_col])
        if src_id is None or tgt_id is None:
            continue  # safety guard: should not happen after null filter
        props = _drop_nulls(
            {
                neo4j_prop: _coerce_value(rec[df_col])
                for neo4j_prop, df_col in zip(
                    prop_neo4j_names,
                    prop_col_names,
                    strict=False,
                )
            },
        )
        rows.append({"src_id": src_id, "tgt_id": tgt_id, "properties": props})
    return rows


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------


class Neo4jSink:
    """Write-only sink that persists pycypher query results to Neo4j.

    All writes use ``MERGE`` semantics, making every call idempotent.
    Rows are sent to the database in batches via ``UNWIND`` for efficiency.

    The sink is a context manager; the underlying driver is closed on exit::

        with Neo4jSink(uri, user, password) as sink:
            sink.write_nodes(df, node_mapping)
            sink.write_relationships(df, rel_mapping)

    Args:
        uri: Neo4j Bolt URI, e.g. ``"bolt://localhost:7687"``.
        user: Database username.
        password: Database password.
        database: Target database name.  ``None`` uses the server default.
        batch_size: Number of rows sent per ``UNWIND`` transaction.
            Tune this down if individual rows are very wide.
        encrypted: Explicitly enable (``True``) or disable (``False``) TLS
            encryption for the driver connection.  ``None`` (default) defers
            to the Neo4j driver's scheme-based defaults (``bolt://`` is
            unencrypted, ``bolt+s://`` or ``neo4j+s://`` are encrypted).
            Set to ``True`` for production connections to enforce TLS.

    Raises:
        ImportError: If the ``neo4j`` driver package is not installed.

    """

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        *,
        database: str | None = None,
        batch_size: int = 500,
        encrypted: bool | None = None,
    ) -> None:
        self._uri = uri
        self._database = database
        self._batch_size = batch_size
        driver_kwargs: dict[str, Any] = {"auth": (user, password)}
        if encrypted is not None:
            driver_kwargs["encrypted"] = encrypted
        self._driver: _Neo4jDriver = GraphDatabase.driver(uri, **driver_kwargs)

    # ------------------------------------------------------------------
    # Public write methods
    # ------------------------------------------------------------------

    def write_nodes(
        self,
        df: pd.DataFrame,
        mapping: NodeMapping,
    ) -> int:
        """Merge rows of *df* as nodes into Neo4j using *mapping*.

        Args:
            df: DataFrame whose rows represent nodes.  Must contain
                ``mapping.id_column`` and all columns referenced by
                ``mapping.property_columns.values()``.
            mapping: Declares how columns map onto the target label and
                properties.

        Returns:
            Number of rows successfully written.  Rows with a null
            ``id_column`` value are skipped and not counted.

        Raises:
            ValueError: If a required column is absent from *df*.

        """
        required = [mapping.id_column, *mapping.property_columns.values()]
        _validate_columns(
            df,
            required,
            f"write_nodes(label={mapping.label!r})",
        )

        cypher = _node_merge_cypher(mapping.label, mapping.id_property)
        total = self._write_batches(
            df,
            cypher,
            lambda batch: _build_node_rows(batch, mapping),
        )
        LOGGER.info(
            f"write_nodes: wrote {total} row(s) to label {mapping.label!r}",
        )
        return total

    def write_relationships(
        self,
        df: pd.DataFrame,
        mapping: RelationshipMapping,
    ) -> int:
        """Merge rows of *df* as relationships into Neo4j using *mapping*.

        Both endpoint nodes are located with ``MATCH``, so they must already
        exist in the database.  Write nodes first with :meth:`write_nodes`.

        Args:
            df: DataFrame whose rows represent relationships.  Must contain
                ``mapping.source_id_column``, ``mapping.target_id_column``,
                and all columns referenced by
                ``mapping.property_columns.values()``.
            mapping: Declares how columns map onto the target relationship
                type and properties.

        Returns:
            Number of rows successfully written.  Rows with null endpoint
            IDs are skipped.

        Raises:
            ValueError: If a required column is absent from *df*.

        """
        required = [
            mapping.source_id_column,
            mapping.target_id_column,
            *mapping.property_columns.values(),
        ]
        _validate_columns(
            df,
            required,
            f"write_relationships(type={mapping.rel_type!r})",
        )

        cypher = _rel_merge_cypher(
            mapping.source_label,
            mapping.target_label,
            mapping.rel_type,
            mapping.source_id_property,
            mapping.target_id_property,
        )
        total = self._write_batches(
            df,
            cypher,
            lambda batch: _build_rel_rows(batch, mapping),
        )
        LOGGER.info(
            f"write_relationships: wrote {total} row(s) of type {mapping.rel_type!r}",
        )
        return total

    # ------------------------------------------------------------------
    # Internal batch execution
    # ------------------------------------------------------------------

    def _write_batches(
        self,
        df: pd.DataFrame,
        cypher: str,
        build_rows: Callable[[pd.DataFrame], list[dict[str, Any]]],
    ) -> int:
        """Execute *cypher* over all batches sliced from *df*.

        Args:
            df: Full result DataFrame.
            cypher: Cypher query string expecting a ``$rows`` parameter.
            build_rows: Callable that converts a DataFrame slice to the
                ``$rows`` list accepted by *cypher*.

        Returns:
            Total number of rows written across all batches.

        Raises:
            Exception: Re-raises any driver exception after logging context.

        """
        if df.empty:
            return 0

        n_batches = math.ceil(len(df) / self._batch_size)
        LOGGER.debug(
            f"Writing {len(df)} row(s) in {n_batches} batch(es) (batch_size={self._batch_size})",
        )

        total = 0
        with self._driver.session(database=self._database) as session:
            for batch_idx, start in enumerate(
                range(0, len(df), self._batch_size),
                start=1,
            ):
                batch_df = df.iloc[start : start + self._batch_size]
                rows = build_rows(batch_df)
                if not rows:
                    continue
                try:
                    session.run(cypher, rows=rows)  # type: ignore[arg-type]  # neo4j stub expects LiteralString
                    total += len(rows)
                except Exception:
                    # SECURITY: Log only the keys of the first row, not the
                    # values, to avoid leaking PII or sensitive data.
                    first_row_keys = list(rows[0].keys()) if rows else []
                    LOGGER.exception(
                        "Batch %d/%d failed (%d rows, first row keys: %s)",
                        batch_idx,
                        n_batches,
                        len(rows),
                        first_row_keys,
                    )
                    raise

        return total

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying Neo4j driver and release all connections."""
        self._driver.close()

    def __enter__(self) -> Self:
        """Return *self* for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the driver on context-manager exit, even if an error occurred."""
        self.close()


__all__ = [
    "Neo4jSink",
    "NodeMapping",
    "RelationshipMapping",
]
