"""Fluent builder that assembles a ``Context`` from Arrow-loaded data.

Usage example::

    from pycypher.ingestion import ContextBuilder

    context = (
        ContextBuilder()
        .add_entity("Person", "people.csv", id_col="person_id")
        .add_relationship(
            "KNOWS",
            "knows.csv",
            source_col="from_id",
            target_col="to_id",
        )
        .build()
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from pycypher.ingestion.arrow_utils import (
    normalize_entity_table,
    normalize_relationship_table,
)
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.relational_models import (
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

if TYPE_CHECKING:
    import pyarrow as pa


class ContextBuilder:
    """Fluent builder that assembles a ``Context`` from heterogeneous sources.

    Methods can be chained::

        ctx = ContextBuilder().add_entity(...).add_relationship(...).build()
    """

    def __init__(self) -> None:
        self._entity_tables: list[EntityTable] = []
        self._relationship_tables: list[RelationshipTable] = []

    def add_entity(
        self,
        entity_type: str,
        source: str | pd.DataFrame | pa.Table,
        *,
        id_col: str | None = None,
        query: str | None = None,
    ) -> ContextBuilder:
        """Register an entity type loaded from *source*.

        Args:
            entity_type: The entity label (e.g. ``"Person"``).
            source: Data source — a file path, pandas DataFrame, or Arrow table.
            id_col: Column to use as ``__ID__``.  Defaults to auto-generated
                sequential integers.
            query: Optional SQL query applied after loading (file paths only).

        Returns:
            ``self`` for chaining.

        """
        raw = data_source_from_uri(source, query=query).read()
        table = normalize_entity_table(raw, id_col=id_col)
        entity_table = EntityTable.from_arrow(entity_type, table)
        self._entity_tables.append(entity_table)
        return self

    def add_relationship(
        self,
        relationship_type: str,
        source: str | pd.DataFrame | pa.Table,
        *,
        source_col: str,
        target_col: str,
        id_col: str | None = None,
        query: str | None = None,
    ) -> ContextBuilder:
        """Register a relationship type loaded from *source*.

        Args:
            relationship_type: The relationship label (e.g. ``"KNOWS"``).
            source: Data source — a file path, pandas DataFrame, or Arrow table.
            source_col: Column that holds the source node ID.
            target_col: Column that holds the target node ID.
            id_col: Column to use as ``__ID__``.  Defaults to auto-generated
                sequential integers.
            query: Optional SQL query applied after loading (file paths only).

        Returns:
            ``self`` for chaining.

        """
        raw = data_source_from_uri(source, query=query).read()
        table = normalize_relationship_table(
            raw,
            source_col=source_col,
            target_col=target_col,
            id_col=id_col,
        )
        rel_table = RelationshipTable.from_arrow(relationship_type, table)
        self._relationship_tables.append(rel_table)
        return self

    @classmethod
    def from_dict(
        cls,
        entity_frames: dict[str, pd.DataFrame],
        *,
        id_column: str | None = None,
    ) -> Context:
        """Build a :class:`~pycypher.relational_models.Context` from a dict of DataFrames.

        Each key is a node or relationship label; the corresponding value is a
        pandas DataFrame.  DataFrames are automatically classified:

        * If the DataFrame contains both ``__SOURCE__`` and ``__TARGET__``
          columns it is registered as a **relationship table**.
        * Otherwise it is registered as an **entity table**.

        This allows a single ``from_dict()`` call to supply both nodes and
        edges without needing the verbose ``add_entity`` / ``add_relationship``
        builder chain::

            ctx = ContextBuilder.from_dict({
                "Person": persons_df,        # entity — no __SOURCE__/__TARGET__
                "KNOWS":  knows_df,          # relationship — has both columns
            })

        Args:
            entity_frames: Mapping of label to DataFrame (entity or relationship).
            id_column: Column to use as the ``__ID__`` identity key.  Defaults
                to the standard ``ID_COLUMN`` (``"__ID__"``).  Pass the name of
                an existing column to use it as the identifier.

        Returns:
            A fully populated :class:`~pycypher.relational_models.Context`.

        Raises:
            TypeError: If any value in *entity_frames* is not a
                :class:`pandas.DataFrame`.

        """
        builder = cls()
        for label, df in entity_frames.items():
            if not isinstance(df, pd.DataFrame):
                msg = (
                    f"Expected a pandas DataFrame for label '{label}', "
                    f"got {type(df).__name__}"
                )
                from pycypher.exceptions import WrongCypherTypeError

                raise WrongCypherTypeError(
                    msg,
                )
            cols = set(df.columns)
            if (
                RELATIONSHIP_SOURCE_COLUMN in cols
                and RELATIONSHIP_TARGET_COLUMN in cols
            ):
                # DataFrame has both __SOURCE__ and __TARGET__ — treat as a
                # relationship table using the standard column names.
                builder.add_relationship(
                    label,
                    df,
                    source_col=RELATIONSHIP_SOURCE_COLUMN,
                    target_col=RELATIONSHIP_TARGET_COLUMN,
                    id_col=id_column,
                )
            else:
                builder.add_entity(label, df, id_col=id_column)
        return builder.build()

    def build(self) -> Context:
        """Assemble and return the :class:`~pycypher.relational_models.Context`.

        Returns:
            A fully populated :class:`~pycypher.relational_models.Context`.

        """
        entity_mapping = EntityMapping(
            mapping={t.entity_type: t for t in self._entity_tables},
        )
        relationship_mapping = RelationshipMapping(
            mapping={
                t.relationship_type: t for t in self._relationship_tables
            },
        )
        return Context(
            entity_mapping=entity_mapping,
            relationship_mapping=relationship_mapping,
        )
