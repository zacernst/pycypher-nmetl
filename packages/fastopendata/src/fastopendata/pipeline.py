"""Pipeline utilities bridging fastopendata processing to pycypher graphs.

Provides :class:`GraphPipeline` — a builder that collects processed data
from streaming operators, batch DataFrames, or incremental views and
materializes them into a :class:`~pycypher.relational_models.Context`
ready for Cypher queries via :class:`~pycypher.star.Star`.

Example::

    from fastopendata.pipeline import GraphPipeline

    ctx = (
        GraphPipeline()
        .add_entity_dataframe("Person", people_df, id_col="person_id")
        .add_entity_dataframe("City", cities_df, id_col="city_id")
        .add_relationship_dataframe(
            "LIVES_IN", lives_in_df,
            source_col="person_id", target_col="city_id",
        )
        .build_context()
    )

    star = Star(ctx)
    result = star.execute_query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name")

For streaming pipelines, records can be ingested incrementally::

    pipeline = GraphPipeline()
    pipeline.add_entity_records("Sensor", [
        {"__ID__": 1, "type": "temperature", "location": "building_a"},
        {"__ID__": 2, "type": "humidity", "location": "building_b"},
    ])
    pipeline.add_entity_records("Reading", [
        {"__ID__": 100, "sensor_id": 1, "value": 22.5, "timestamp": 1000.0},
    ])
    ctx = pipeline.build_context()

.. versionadded:: 0.0.2
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.relational_models import Context
from pycypher.star import Star

from fastopendata.schema_evolution.lineage import LineageEdge, LineageGraph, LineageNode, NodeType
from fastopendata.schema_evolution.registry import SchemaRegistry
from fastopendata.schema_evolution.schema import FieldSchema, FieldType, TableSchema
from fastopendata.streaming.core import StreamRecord
from fastopendata.streaming.views import IncrementalView

_logger = logging.getLogger(__name__)

# Mapping from pandas dtypes to schema FieldTypes
_DTYPE_MAP: dict[str, FieldType] = {
    "int64": FieldType.INTEGER,
    "int32": FieldType.INTEGER,
    "float64": FieldType.FLOAT,
    "float32": FieldType.FLOAT,
    "bool": FieldType.BOOLEAN,
    "object": FieldType.STRING,
    "string": FieldType.STRING,
    "datetime64[ns]": FieldType.TIMESTAMP,
}


def _schema_from_dataframe(name: str, df: pd.DataFrame) -> TableSchema:
    """Infer a :class:`TableSchema` from a DataFrame's dtypes."""
    fields: list[FieldSchema] = []
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        field_type = _DTYPE_MAP.get(dtype_str, FieldType.STRING)
        has_nulls = bool(df[col].isna().any())
        fields.append(FieldSchema(name=col, field_type=field_type, nullable=has_nulls))
    return TableSchema(name=name, fields=tuple(fields))


class GraphPipeline:
    """Collects data from multiple sources and builds a pycypher Context.

    Supports three ingestion patterns:

    1. **Batch** — add entire DataFrames via :meth:`add_entity_dataframe`
       and :meth:`add_relationship_dataframe`.
    2. **Record-level** — add lists of dicts via :meth:`add_entity_records`
       and :meth:`add_relationship_records` (convenient for streaming
       pipelines that produce records one at a time).
    3. **Streaming views** — add an :class:`IncrementalView` via
       :meth:`add_entity_from_view`, which snapshots the current
       accumulated state of a streaming view.

    All methods return ``self`` for chaining.
    """

    def __init__(
        self,
        schema_registry: SchemaRegistry | None = None,
    ) -> None:
        self._entity_frames: dict[str, pd.DataFrame] = {}
        self._relationship_frames: dict[
            str, tuple[pd.DataFrame, str, str]
        ] = {}
        self._entity_id_cols: dict[str, str | None] = {}
        self._schema_registry = schema_registry
        self._lineage = LineageGraph()

    # ── Batch ingestion ──────────────────────────────────────────────

    def add_entity_dataframe(
        self,
        entity_type: str,
        df: pd.DataFrame,
        *,
        id_col: str | None = None,
    ) -> GraphPipeline:
        """Register an entity type from a pandas DataFrame.

        Parameters
        ----------
        entity_type:
            The entity label (e.g. ``"Person"``).
        df:
            DataFrame containing entity data.
        id_col:
            Column to use as ``__ID__``.  Defaults to auto-generated IDs.

        """
        if self._schema_registry is not None and not df.empty:
            schema = _schema_from_dataframe(entity_type, df)
            existing = self._schema_registry.get_latest(entity_type)
            if existing is not None:
                compat = self._schema_registry.check_compatibility(
                    entity_type, schema
                )
                if not compat.compatible:
                    raise ValueError(
                        f"Schema for '{entity_type}' is incompatible with "
                        f"registered version {existing.version}: "
                        f"{[str(v) for v in compat.violations]}"
                    )
            self._schema_registry.register(schema)
            _logger.info(
                "Schema registered for %s (version %d, %d fields)",
                entity_type,
                schema.version,
                len(schema.fields),
            )

        self._entity_frames[entity_type] = df
        self._entity_id_cols[entity_type] = id_col
        self._lineage.add_node(LineageNode(
            node_id=f"entity:{entity_type}",
            node_type=NodeType.SOURCE,
            name=entity_type,
            metadata={"rows": str(len(df)), "columns": str(len(df.columns))},
        ))
        return self

    def add_relationship_dataframe(
        self,
        relationship_type: str,
        df: pd.DataFrame,
        *,
        source_col: str,
        target_col: str,
    ) -> GraphPipeline:
        """Register a relationship type from a pandas DataFrame.

        Parameters
        ----------
        relationship_type:
            The relationship label (e.g. ``"KNOWS"``).
        df:
            DataFrame containing relationship data.
        source_col:
            Column holding source node IDs.
        target_col:
            Column holding target node IDs.

        """
        self._relationship_frames[relationship_type] = (
            df,
            source_col,
            target_col,
        )
        # Track lineage: relationship node + edges to source/target entities
        rel_node_id = f"relationship:{relationship_type}"
        self._lineage.add_node(LineageNode(
            node_id=rel_node_id,
            node_type=NodeType.TRANSFORM,
            name=relationship_type,
            metadata={
                "rows": str(len(df)),
                "source_col": source_col,
                "target_col": target_col,
            },
        ))
        # Link source entity → relationship if the entity is already registered
        for entity_type in self._entity_frames:
            entity_node_id = f"entity:{entity_type}"
            if self._lineage.get_node(entity_node_id):
                self._lineage.add_edge(LineageEdge(
                    source_id=entity_node_id,
                    target_id=rel_node_id,
                    transformation=f"{entity_type} → {relationship_type}",
                ))
        return self

    # ── Record-level ingestion ───────────────────────────────────────

    def add_entity_records(
        self,
        entity_type: str,
        records: list[dict[str, Any]],
        *,
        id_col: str | None = None,
    ) -> GraphPipeline:
        """Register an entity type from a list of record dicts.

        Parameters
        ----------
        entity_type:
            The entity label.
        records:
            List of dicts, each representing one entity.
        id_col:
            Column to use as ``__ID__``.

        """
        df = pd.DataFrame(records)
        return self.add_entity_dataframe(entity_type, df, id_col=id_col)

    def add_relationship_records(
        self,
        relationship_type: str,
        records: list[dict[str, Any]],
        *,
        source_col: str,
        target_col: str,
    ) -> GraphPipeline:
        """Register a relationship type from a list of record dicts.

        Parameters
        ----------
        relationship_type:
            The relationship label.
        records:
            List of dicts, each representing one relationship.
        source_col:
            Key in each dict holding the source node ID.
        target_col:
            Key in each dict holding the target node ID.

        """
        df = pd.DataFrame(records)
        return self.add_relationship_dataframe(
            relationship_type,
            df,
            source_col=source_col,
            target_col=target_col,
        )

    # ── Streaming view ingestion ─────────────────────────────────────

    def add_entity_from_view(
        self,
        entity_type: str,
        view: IncrementalView,
        *,
        id_col: str | None = None,
    ) -> GraphPipeline:
        """Snapshot an :class:`IncrementalView` as an entity table.

        Takes the current accumulated records from a streaming view
        and registers them as an entity type.  The view's record values
        become the DataFrame columns.

        Parameters
        ----------
        entity_type:
            The entity label.
        view:
            An IncrementalView whose accumulated records will be
            snapshotted.
        id_col:
            Column to use as ``__ID__``.

        """
        records = list(view.snapshot.values())
        if not records:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(records)
        return self.add_entity_dataframe(entity_type, df, id_col=id_col)

    def add_entity_from_stream_records(
        self,
        entity_type: str,
        records: list[StreamRecord],
        *,
        id_col: str | None = None,
    ) -> GraphPipeline:
        """Register an entity type from a list of :class:`StreamRecord` objects.

        Extracts the ``value`` dict from each StreamRecord.

        Parameters
        ----------
        entity_type:
            The entity label.
        records:
            StreamRecord objects from a streaming pipeline.
        id_col:
            Column to use as ``__ID__``.

        """
        dicts = [r.value for r in records]
        if not dicts:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(dicts)
        return self.add_entity_dataframe(entity_type, df, id_col=id_col)

    # ── Context building ─────────────────────────────────────────────

    def build_context(self) -> Context:
        """Assemble all registered data into a pycypher Context.

        Returns
        -------
        Context
            A fully populated context ready for use with
            :class:`~pycypher.star.Star`.

        """
        builder = ContextBuilder()

        for entity_type, df in self._entity_frames.items():
            id_col = self._entity_id_cols.get(entity_type)
            builder.add_entity(entity_type, df, id_col=id_col)

        for rel_type, (
            df,
            source_col,
            target_col,
        ) in self._relationship_frames.items():
            builder.add_relationship(
                rel_type,
                df,
                source_col=source_col,
                target_col=target_col,
            )

        # Finalize lineage: add Context sink with edges from all sources
        context_node_id = "context:pycypher"
        if not self._lineage.get_node(context_node_id):
            self._lineage.add_node(LineageNode(
                node_id=context_node_id,
                node_type=NodeType.SINK,
                name="PycypherContext",
                metadata={
                    "entity_types": str(len(self._entity_frames)),
                    "relationship_types": str(len(self._relationship_frames)),
                },
            ))
            for entity_type in self._entity_frames:
                node_id = f"entity:{entity_type}"
                if self._lineage.get_node(node_id):
                    self._lineage.add_edge(LineageEdge(
                        source_id=node_id,
                        target_id=context_node_id,
                        transformation="build_context",
                    ))
            for rel_type in self._relationship_frames:
                node_id = f"relationship:{rel_type}"
                if self._lineage.get_node(node_id):
                    self._lineage.add_edge(LineageEdge(
                        source_id=node_id,
                        target_id=context_node_id,
                        transformation="build_context",
                    ))

        return builder.build()

    def build_star(self) -> Star:
        """Build a :class:`~pycypher.star.Star` query engine from the pipeline data.

        Convenience method that combines :meth:`build_context` with
        Star instantiation.

        Returns
        -------
        Star
            A query engine ready to execute Cypher queries.

        """
        return Star(self.build_context())

    # ── Inspection ───────────────────────────────────────────────────

    @property
    def lineage(self) -> LineageGraph:
        """Return the data lineage graph tracking pipeline data flow."""
        return self._lineage

    @property
    def entity_types(self) -> list[str]:
        """Return registered entity type names."""
        return list(self._entity_frames.keys())

    @property
    def relationship_types(self) -> list[str]:
        """Return registered relationship type names."""
        return list(self._relationship_frames.keys())

    def entity_count(self, entity_type: str) -> int:
        """Return the number of entities of the given type."""
        df = self._entity_frames.get(entity_type)
        return len(df) if df is not None else 0

    def relationship_count(self, relationship_type: str) -> int:
        """Return the number of relationships of the given type."""
        entry = self._relationship_frames.get(relationship_type)
        return len(entry[0]) if entry is not None else 0


def load_available_datasets(
    data_dir: Path | None = None,
    *,
    max_rows: int | None = None,
) -> GraphPipeline:
    """Discover and load available CSV datasets from the data directory.

    Scans the configured data directory for CSV output files listed in
    ``config.datasets`` and loads each one as an entity type in a
    :class:`GraphPipeline`.  Datasets whose output files do not exist
    (i.e. not yet downloaded via Snakemake) are silently skipped.

    Parameters
    ----------
    data_dir:
        Override the data directory from config. If *None*, uses
        ``config.data_dir``.
    max_rows:
        Optional row limit per dataset (useful for development/preview).

    Returns
    -------
    GraphPipeline
        A pipeline with all discovered datasets loaded as entity types.

    """
    from fastopendata.config import config

    if data_dir is None:
        data_dir = config.data_path
    else:
        data_dir = Path(data_dir)

    pipeline = GraphPipeline()
    loaded = 0

    for name, dataset in config.datasets.items():
        if not dataset.output_file:
            continue

        filepath = data_dir / dataset.output_file
        if not filepath.exists():
            continue

        if dataset.format.upper() not in ("CSV", "PBF/CSV"):
            continue

        try:
            read_kwargs: dict[str, Any] = {}
            if max_rows is not None:
                read_kwargs["nrows"] = max_rows

            df = pd.read_csv(filepath, **read_kwargs)

            # Use dataset name as entity type, converting to PascalCase
            entity_type = name.replace("_", " ").title().replace(" ", "")

            # Auto-detect or generate an ID column
            id_col: str | None = None
            if "__ID__" in df.columns:
                id_col = "__ID__"

            pipeline.add_entity_dataframe(entity_type, df, id_col=id_col)
            loaded += 1
            _logger.info(
                "Loaded dataset %s: %d rows as entity type %s",
                name,
                len(df),
                entity_type,
            )
        except Exception:
            _logger.exception("Failed to load dataset %s from %s", name, filepath)

    _logger.info("Loaded %d/%d available datasets", loaded, len(config.datasets))
    return pipeline
