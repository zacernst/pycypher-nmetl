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

from typing import Any

import pandas as pd
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.relational_models import Context
from pycypher.star import Star

from fastopendata.streaming.core import StreamRecord
from fastopendata.streaming.views import IncrementalView


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

    def __init__(self) -> None:
        self._entity_frames: dict[str, pd.DataFrame] = {}
        self._relationship_frames: dict[
            str, tuple[pd.DataFrame, str, str]
        ] = {}
        self._entity_id_cols: dict[str, str | None] = {}

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
        self._entity_frames[entity_type] = df
        self._entity_id_cols[entity_type] = id_col
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
