"""Data containers and context for PyCypher query execution.

This module defines the data containers (``EntityTable``, ``RelationshipTable``)
and the ``Context`` object that together form the runtime environment for Cypher
query execution via the BindingFrame execution path in ``star.py``.

Data containers
---------------

``EntityTable``
    Holds all entity IDs and attributes for a single entity type.
    Source data is a ``pd.DataFrame`` or ``pyarrow.Table`` with an
    ``__ID__`` column plus attribute columns.  ``to_pandas(context)``
    returns the DataFrame with all columns prefixed by the entity type
    (e.g. ``Person__name``, ``Person____ID__``).

``RelationshipTable``
    Holds all relationship IDs and endpoint references for a single
    relationship type.  Source data must have ``__ID__``, ``__SOURCE__``,
    and ``__TARGET__`` columns.  ``to_pandas(context)`` applies the same
    type-prefix convention (e.g. ``KNOWS____SOURCE__``).

Context and atomicity
---------------------

``Context`` holds the entity and relationship mappings that a ``Star``
instance queries against.  It also implements query-scoped shadow writes:

1. ``begin_query()`` — opens a transaction by initialising an empty shadow
   layer.
2. SET-clause mutations write to ``context._shadow[entity_type]`` (a copy of
   the canonical DataFrame) rather than directly to ``source_obj``.
3. ``commit_query()`` — promotes shadow DataFrames to canonical ``source_obj``
   on each entity table.
4. ``rollback_query()`` — discards the shadow, leaving ``source_obj``
   unchanged.

This guarantees that a failed query never leaves the context in a
partially-mutated state.

Key constants
-------------

``ID_COLUMN``
    Name of the identity column inside source DataFrames (``"__ID__"``).
``RELATIONSHIP_SOURCE_COLUMN``
    Source-node ID column in relationship DataFrames (``"__SOURCE__"``).
``RELATIONSHIP_TARGET_COLUMN``
    Target-node ID column in relationship DataFrames (``"__TARGET__"``).
"""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from shared.logger import LOGGER

from pycypher.ast_models import Algebraizable, Variable, random_hash
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)

__all__ = [
    # Public data containers (re-exported by pycypher.__init__)
    "Context",
    "EntityMapping",
    "EntityTable",
    "RegisteredFunction",
    "RelationshipMapping",
    "RelationshipTable",
    # Constants
    "ID_COLUMN",
    "RELATIONSHIP_SOURCE_COLUMN",
    "RELATIONSHIP_TARGET_COLUMN",
]


def flatten(lst: list[Any]) -> list[Any]:
    """Flatten a nested list."""
    flat_list: list[Any] = []
    for item in lst:
        if isinstance(item, list):
            flat_list.extend(flatten(item))
        else:
            flat_list.append(item)
    return flat_list


EntityType = Annotated[str, ...]
Attribute = Annotated[str, ...]
RelationshipType = Annotated[str, ...]
ColumnName = Annotated[str, ...]
VariableMap = Annotated[dict[Variable, ColumnName], ...]
VariableTypeMap = Annotated[dict[Variable, EntityType | RelationshipType], ...]
AttributeMap = Annotated[dict[Attribute, ColumnName], ...]


class EntityMapping(BaseModel):
    """Mapping from entity types to the corresponding Table."""

    mapping: dict[EntityType, Any] = {}

    def __getitem__(self, key: EntityType) -> Any:
        """Return the table for *key*, raising ``KeyError`` if absent."""
        return self.mapping[key]


class RelationshipMapping(BaseModel):
    """Mapping from relationship types to the corresponding Table."""

    mapping: dict[RelationshipType, Any] = {}

    def __getitem__(self, key: RelationshipType) -> Any:
        """Return the table for *key*, raising ``KeyError`` if absent."""
        return self.mapping[key]


class RegisteredFunction(BaseModel):
    """Represents a registered Cypher function in the context."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    implementation: Callable
    arity: int = 0

    def __call__(self, *args: Any) -> Any:
        """Invoke the wrapped function, validating arity if set."""
        if self.arity and len(args) != self.arity:
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name=self.name,
                expected_args=self.arity,
                actual_args=len(args),
            )
        return self.implementation(*args)


class ProcedureRegistry:
    """Registry of callable Cypher procedures for the CALL clause.

    A *procedure* is a named callable that receives the current
    :class:`Context` plus a list of evaluated argument values and returns a
    list of row dictionaries.  Each dict key corresponds to a column that can
    be referenced by a subsequent ``YIELD`` projection.

    Three built-in ``db.*`` procedures are registered at class creation time:

    * ``db.labels()`` — one row per entity type in the context.
    * ``db.relationshipTypes()`` — one row per relationship type.
    * ``db.propertyKeys()`` — one row per unique user-visible property key
      across all entity and relationship tables.

    Custom procedures can be registered with :meth:`register`.

    Example::

        registry = ProcedureRegistry()

        @registry.register("my.proc")
        def my_proc(context, args):
            return [{"value": 42}]
    """

    def __init__(self) -> None:
        """Initialise the registry and register built-in ``db.*`` procedures."""
        self._procedures: dict[str, Callable] = {}
        self._register_builtins()

    def _register_builtins(self) -> None:
        """Register all built-in db.* procedures."""

        def _db_labels(context: Context, args: list[Any]) -> list[dict]:
            """Return one row per entity type label in the context."""
            return [
                {"label": label}
                for label in sorted(context.entity_mapping.mapping.keys())
            ]

        def _db_relationship_types(
            context: Context,
            args: list[Any],
        ) -> list[dict]:
            """Return one row per relationship type in the context."""
            return [
                {"relationshipType": rel_type}
                for rel_type in sorted(
                    context.relationship_mapping.mapping.keys(),
                )
            ]

        def _db_property_keys(
            context: Context,
            args: list[Any],
        ) -> list[dict]:
            """Return one row per unique user-visible property key across all tables."""
            _internal = {
                ID_COLUMN,
                RELATIONSHIP_SOURCE_COLUMN,
                RELATIONSHIP_TARGET_COLUMN,
            }
            keys: set[str] = set()
            for table in context.entity_mapping.mapping.values():
                df = table.source_obj
                if hasattr(df, "columns"):
                    keys.update(c for c in df.columns if c not in _internal)
            for table in context.relationship_mapping.mapping.values():
                df = table.source_obj
                if hasattr(df, "columns"):
                    keys.update(c for c in df.columns if c not in _internal)
            return [{"propertyKey": k} for k in sorted(keys)]

        self._procedures["db.labels"] = _db_labels
        self._procedures["db.relationshiptypes"] = _db_relationship_types
        self._procedures["db.propertykeys"] = _db_property_keys

    def register(self, name: str) -> Callable:
        """Decorator to register a procedure under *name*.

        Args:
            name: The fully-qualified procedure name (e.g. ``"my.namespace.proc"``).

        Returns:
            A decorator that registers and returns the decorated callable.

        """

        def decorator(fn: Callable) -> Callable:
            """Register *fn* under the procedure name and return it unchanged."""
            self._procedures[name.lower()] = fn
            return fn

        return decorator

    def execute(
        self,
        name: str,
        context: Context,
        args: list[Any],
    ) -> list[dict]:
        """Execute procedure *name* with *args* against *context*.

        Args:
            name: The procedure name (case-insensitive).
            context: The current execution context.
            args: Evaluated argument values.

        Returns:
            A list of row dictionaries, one dict per output row.

        Raises:
            ValueError: If *name* is not a registered procedure.

        """
        fn = self._procedures.get(name.lower())
        if fn is None:
            msg = (
                f"Unknown procedure: '{name}'. "
                "Register it with ProcedureRegistry.register() or check the spelling."
            )
            raise ValueError(
                msg,
            )
        return fn(context, args)


#: Module-level singleton procedure registry — shared across all Star instances.
PROCEDURE_REGISTRY: ProcedureRegistry = ProcedureRegistry()


class Context(BaseModel):
    """Context for translation operations.

    The optional *backend* parameter selects the DataFrame computation engine.
    Pass ``"pandas"`` (default), ``"duckdb"``, or ``"auto"`` to choose a
    backend.  ``"auto"`` inspects the total entity count and selects the most
    efficient engine automatically.  You can also pass a pre-constructed
    :class:`~pycypher.backend_engine.BackendEngine` instance directly.

    Examples::

        # Default — identical to current behaviour
        Context(entity_mapping=..., relationship_mapping=...)

        # Explicit DuckDB backend for analytical workloads
        Context(..., backend="duckdb")

        # Auto-select based on data size
        Context(..., backend="auto")
    """

    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()
    cypher_functions: dict[str, RegisteredFunction] = Field(
        default_factory=dict,
    )

    _shadow: dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    #: Shadow layer for relationship mutations (CREATE / future DELETE).
    _shadow_rels: dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    #: Query parameters injected via ``Star.execute_query(parameters=...)``.
    _parameters: dict[str, Any] = PrivateAttr(default_factory=dict)
    #: Property-lookup index cache: maps entity_type → ``source_df.set_index(ID_COLUMN)``.
    #: Populated lazily by ``BindingFrame.get_property()`` and cleared by
    #: ``commit_query()`` so mutations see fresh data.  The shadow path in
    #: ``get_property`` bypasses this cache entirely.
    _property_lookup_cache: dict[str, pd.DataFrame] = PrivateAttr(
        default_factory=dict,
    )
    #: The DataFrame computation backend.  Resolved lazily from the *backend*
    #: init parameter (string hint or BackendEngine instance).
    _backend: Any = PrivateAttr(default=None)
    #: Raw backend hint supplied at construction time.
    _backend_hint: str | None = PrivateAttr(default=None)
    #: Query deadline (absolute ``time.perf_counter()`` value).  Set by
    #: ``Star.execute_query(timeout_seconds=...)`` before ``begin_query()``.
    #: ``None`` means no timeout.
    _query_deadline: float | None = PrivateAttr(default=None)
    #: The configured timeout in seconds (stored for error messages).
    _query_timeout_seconds: float | None = PrivateAttr(default=None)
    #: Optional memory budget in bytes.  Set by
    #: ``Star.execute_query(memory_budget_bytes=...)``.
    _memory_budget_bytes: int | None = PrivateAttr(default=None)
    #: Data epoch counter — incremented on every mutation commit.
    #: Used by query result caching to detect stale entries.
    _data_epoch: int = PrivateAttr(default=0)

    def model_post_init(self, __context: Any) -> None:
        """Resolve the backend engine after Pydantic initialisation."""
        super().model_post_init(__context)
        # _backend and _backend_hint are set via __init__ override below

    def __init__(self, *, backend: Any = None, **data: Any) -> None:
        """Initialise the query context with entity/relationship data and a backend engine.

        Args:
            backend: Backend engine or hint string (``"pandas"``, ``"duckdb"``,
                ``"polars"``, ``"auto"``).  Defaults to :class:`PandasBackend`
                when ``None`` or ``"pandas"``.
            **data: Pydantic field values — typically ``entity_mapping``,
                ``relationship_mapping``, and ``functions``.
        """
        super().__init__(**data)
        if backend is None or (
            isinstance(backend, str) and backend == "pandas"
        ):
            from pycypher.backend_engine import PandasBackend

            self._backend = PandasBackend()
            self._backend_hint = "pandas"
        elif isinstance(backend, str):
            from pycypher.backend_engine import select_backend

            # Estimate total rows for auto-selection
            total_rows = sum(
                len(t.source_obj) if hasattr(t.source_obj, "__len__") else 0
                for t in self.entity_mapping.mapping.values()
            )
            self._backend = select_backend(
                hint=backend,
                estimated_rows=total_rows,
            )
            self._backend_hint = backend
        else:
            # Pre-constructed BackendEngine instance
            self._backend = backend
            self._backend_hint = getattr(backend, "name", "custom")

    @property
    def backend(self) -> Any:
        """Return the active :class:`~pycypher.backend_engine.BackendEngine`."""
        return self._backend

    @property
    def backend_name(self) -> str:
        """Return the name of the active backend engine (e.g. ``'pandas'``, ``'duckdb'``, ``'polars'``)."""
        return getattr(self._backend, "name", "unknown")

    def __repr__(self) -> str:
        """Return an informative summary for REPL/notebook display.

        Shows backend, entity types with row counts, relationship types
        with row counts, and custom function count.  Example::

            Context(backend='pandas', entities={'Person': 4, 'Company': 2},
                    relationships={'WORKS_AT': 3}, custom_functions=1)
        """
        entity_counts: dict[str, int] = {}
        for name in sorted(self.entity_mapping.mapping):
            table = self.entity_mapping.mapping[name]
            src = getattr(table, "source_obj", None)
            entity_counts[name] = len(src) if src is not None else 0

        rel_counts: dict[str, int] = {}
        for name in sorted(self.relationship_mapping.mapping):
            table = self.relationship_mapping.mapping[name]
            src = getattr(table, "source_obj", None)
            rel_counts[name] = len(src) if src is not None else 0

        n_funcs = len(self.cypher_functions)
        parts = [f"Context(backend={self.backend_name!r}"]
        parts.append(
            f"entities={entity_counts}" if entity_counts else "entities={}",
        )
        if rel_counts:
            parts.append(f"relationships={rel_counts}")
        if n_funcs:
            parts.append(f"custom_functions={n_funcs}")
        return ", ".join(parts) + ")"

    def set_deadline(self, timeout_seconds: float | None) -> None:
        """Arm the per-query timeout clock.

        Call this **before** :meth:`begin_query` so that the deadline is in
        effect for the entire query transaction.

        Args:
            timeout_seconds: Wall-clock budget in seconds, or ``None`` to
                disable timeout enforcement.

        """
        import time

        if timeout_seconds is not None:
            self._query_timeout_seconds = timeout_seconds
            self._query_deadline = time.perf_counter() + timeout_seconds
        else:
            self._query_timeout_seconds = None
            self._query_deadline = None

    def check_timeout(self, query_fragment: str = "") -> None:
        """Raise :class:`~pycypher.exceptions.QueryTimeoutError` if the deadline has passed.

        This is intentionally cheap — a single ``perf_counter()`` comparison —
        so it can be called at the top of every clause iteration without
        measurable overhead.

        Args:
            query_fragment: Optional truncated query text for the error message.

        Raises:
            QueryTimeoutError: If the wall-clock deadline has been exceeded.

        """
        import time

        if self._query_deadline is None:
            return
        elapsed = time.perf_counter() - (
            self._query_deadline - (self._query_timeout_seconds or 0)
        )
        if time.perf_counter() > self._query_deadline:
            from pycypher.exceptions import QueryTimeoutError

            raise QueryTimeoutError(
                timeout_seconds=self._query_timeout_seconds or 0.0,
                elapsed_seconds=elapsed,
                query_fragment=query_fragment,
            )

    def clear_deadline(self) -> None:
        """Disarm the per-query timeout (called in ``finally`` after execution)."""
        self._query_deadline = None
        self._query_timeout_seconds = None

    def begin_query(self) -> None:
        """Initialise the shadow layers for a new query transaction."""
        self._shadow = {}
        self._shadow_rels = {}

    def commit_query(self) -> None:
        """Promote shadow DataFrames to the canonical entity and relationship tables.

        Handles both updates to existing tables *and* newly created tables
        (e.g. from a CREATE clause referencing a label not yet in the mapping).

        The method ensures shadow state is *always* cleared, even if an
        exception occurs mid-commit (e.g. constructing an ``EntityTable``
        for a new label fails).  This prevents stale shadow data from
        leaking into subsequent queries.
        """
        # Capture mutation flag BEFORE clearing shadows.
        had_mutations: bool = bool(self._shadow or self._shadow_rels)

        try:
            for entity_type, shadow_df in self._shadow.items():
                if entity_type in self.entity_mapping.mapping:
                    self.entity_mapping.mapping[
                        entity_type
                    ].source_obj = shadow_df
                else:
                    # New entity type created via CREATE — register an EntityTable.
                    attr_cols = [
                        c for c in shadow_df.columns if c != ID_COLUMN
                    ]
                    self.entity_mapping.mapping[entity_type] = EntityTable(
                        entity_type=entity_type,
                        identifier=entity_type,
                        column_names=list(shadow_df.columns),
                        source_obj_attribute_map={c: c for c in attr_cols},
                        attribute_map={c: c for c in attr_cols},
                        source_obj=shadow_df,
                    )

            for rel_type, shadow_df in self._shadow_rels.items():
                if rel_type in self.relationship_mapping.mapping:
                    self.relationship_mapping.mapping[
                        rel_type
                    ].source_obj = shadow_df
                else:
                    # New relationship type created via CREATE.
                    self.relationship_mapping.mapping[rel_type] = (
                        RelationshipTable(
                            relationship_type=rel_type,
                            identifier=rel_type,
                            column_names=list(shadow_df.columns),
                            source_obj_attribute_map={},
                            attribute_map={},
                            source_obj=shadow_df,
                        )
                    )
        finally:
            # Always clear shadow state — even on failure — to prevent stale
            # shadow data from leaking into subsequent queries.
            self._shadow = {}
            self._shadow_rels = {}

        # Invalidate the property-lookup index cache only when there were actual
        # mutations — purely read-only queries commit with empty shadows, so the
        # cache remains valid and cross-query reuse is preserved.
        if had_mutations:
            self._property_lookup_cache = {}
            self._data_epoch += 1

    def rollback_query(self) -> None:
        """Discard all shadow writes — leaves the context unmodified."""
        self._shadow = {}
        self._shadow_rels = {}
        # Clear property-lookup cache to prevent stale entries that may
        # have been built from shadow-adjacent reads during the failed query.
        self._property_lookup_cache = {}

    def savepoint(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Snapshot the current shadow state for mid-query recovery.

        Returns a lightweight snapshot that can be passed to
        :meth:`restore_savepoint` to roll back shadow mutations to this
        point.  Useful for MERGE ON CREATE/ON MATCH fallback and other
        speculative mutation paths.

        Returns:
            A dict with ``"entities"`` and ``"relationships"`` keys, each
            mapping type names to *copies* of the shadow DataFrames that
            existed at the time of the call.

        """
        return {
            "entities": {k: v.copy() for k, v in self._shadow.items()},
            "relationships": {
                k: v.copy() for k, v in self._shadow_rels.items()
            },
        }

    def restore_savepoint(
        self, savepoint: dict[str, dict[str, pd.DataFrame]]
    ) -> None:
        """Restore shadow state to a previous savepoint.

        Replaces the current shadow layers with the snapshots captured by
        :meth:`savepoint`.  Any mutations made after the savepoint was
        created are discarded.

        Args:
            savepoint: The snapshot returned by :meth:`savepoint`.

        """
        self._shadow = savepoint["entities"]
        self._shadow_rels = savepoint["relationships"]
        self._property_lookup_cache = {}

    def cypher_function(self, func: types.FunctionType) -> types.FunctionType:
        """Decorator to register a function as a Cypher function in the context."""
        LOGGER.info(f"Registering Cypher function: {func.__name__}")
        self.cypher_functions[func.__name__] = RegisteredFunction(
            name=func.__name__,
            implementation=func,
            arity=func.__code__.co_argcount,
        )
        return func


def _prefix_columns(type_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Return *df* with every column renamed to ``{type_name}__{col}``.

    Used by :class:`EntityTable` and :class:`RelationshipTable` to
    disambiguate column names when multiple tables are joined.

    Args:
        type_name: The entity or relationship type string (e.g. ``"Person"``).
        df: Source DataFrame whose columns are to be prefixed.

    Returns:
        A new DataFrame with all columns renamed; the original is unchanged.

    """
    return df.rename(
        columns={col: f"{type_name}__{col}" for col in df.columns},
    )


class Relation(BaseModel):
    """Base class for all relational algebra operators.

    A ``Relation`` represents a lazy tabular computation.  Calling
    ``to_pandas(context)`` materialises the result as a pandas DataFrame.

    Attributes:
        source_algebraizable: The originating AST node, if any.
        variable_map: Maps ``Variable`` AST nodes to the column name that
            holds their ID in the DataFrame produced by this relation.
            Before a WITH clause the values are 32-hex HASH_ID strings;
            after a WITH clause they are plain ALIAS strings.
        variable_type_map: Maps ``Variable`` AST nodes to their entity or
            relationship type string (e.g. ``"Person"``).
        column_names: Ordered list of column names this relation will produce.
        identifier: A random 32-hex identifier unique to this relation instance.

    """

    source_algebraizable: Algebraizable | None = None
    variable_map: VariableMap = {}
    variable_type_map: VariableTypeMap = Field(
        default_factory=dict,
    )  # e.g. "node" or "relationship"
    column_names: list[ColumnName] = []
    identifier: str = Field(default_factory=random_hash)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise the relation, coercing column and variable names to strings.

        Logs a warning if *column_names* is empty, which typically indicates
        a misconfigured operator.
        """
        super().__init__(*args, **kwargs)
        # Note: Pydantic models with default fields might have them set after init if passed as args
        # But we check self.column_names which should be set by super().__init__
        if not self.column_names:
            LOGGER.warning(
                msg=f"Relation {type(self).__name__} created without column_names specified.",
            )

        self.column_names: list[ColumnName] = [
            str(column_name) for column_name in self.column_names
        ]

        self.variable_map: VariableMap = {
            var_name: str(column_name)
            for var_name, column_name in self.variable_map.items()
        }

        self.variable_type_map: VariableTypeMap = dict(
            self.variable_type_map.items(),
        )

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Relation to a pandas DataFrame."""
        msg = "to_pandas not implemented for base Relation class."
        raise NotImplementedError(
            msg,
        )

    def _to_spark_with_prefix(
        self,
        context: Context,
        type_name: str,
        source_obj: Any = None,
    ) -> Any:
        """Shared PySpark conversion logic used by EntityTable and RelationshipTable.

        Handles two cases:
        * *source_obj* already has a ``toPandas`` method — it is a native PySpark
          DataFrame.  Each column is renamed with a ``{type_name}__`` prefix using
          ``withColumnRenamed``, then the DataFrame is returned directly.
        * Otherwise — convert via :meth:`to_pandas` and call
          ``spark.createDataFrame``.

        Args:
            context: PyCypher Context used for the pandas fallback path.
            type_name: The entity or relationship type label used as the column
                prefix (e.g. ``"Person"`` or ``"KNOWS"``).
            source_obj: The raw source object.  Pass ``self.source_obj`` from the
                subclass.  When ``None`` the pandas fallback path is always taken.

        Returns:
            PySpark DataFrame with columns prefixed by *type_name*.

        Raises:
            ImportError: If PySpark is not installed.

        """
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            msg = "PySpark not available. Install PySpark to use to_spark() method."
            raise ImportError(
                msg,
            ) from None

        spark = SparkSession.builder.appName(
            f"PyCypher-{type_name}",
        ).getOrCreate()

        if source_obj is not None and hasattr(source_obj, "toPandas"):
            # Already a PySpark DataFrame — apply column prefix in-place.
            spark_df = source_obj
            for col in spark_df.columns:
                spark_df = spark_df.withColumnRenamed(
                    col,
                    f"{type_name}__{col}",
                )
            return spark_df

        # Fallback: convert through pandas (handles Arrow tables and pandas DFs).
        pandas_df = self.to_pandas(context)
        return spark.createDataFrame(pandas_df)

    def to_spark(self, context: Context) -> Any:
        """Convert the Relation to a PySpark DataFrame.

        Default implementation converts to Pandas first, then to PySpark.
        Subclasses that carry a typed ``source_obj`` should call
        :meth:`_to_spark_with_prefix` directly to also handle the case where
        ``source_obj`` is already a native PySpark DataFrame.

        Args:
            context: PyCypher Context

        Returns:
            PySpark DataFrame

        Raises:
            ImportError: If PySpark is not available

        """
        return self._to_spark_with_prefix(context, type(self).__name__, None)

    def to_dataframe(self, context: Context, backend: str = "pandas") -> Any:
        """Convert the Relation to a DataFrame of the specified backend type.

        Args:
            context: PyCypher Context
            backend: Backend type ('pandas' or 'spark')

        Returns:
            DataFrame (Pandas or PySpark) depending on backend

        Raises:
            ValueError: If backend is not supported

        """
        if backend == "pandas":
            return self.to_pandas(context)
        if backend == "spark":
            return self.to_spark(context)
        msg = f"Unsupported backend: {backend}. Supported: 'pandas', 'spark'"
        raise ValueError(msg)


class RelationIntersection(Relation):
    """Intersection of multiple Relations, implicit Join on shared variables."""

    relation_list: list[Relation]

    def variables_in_common(self) -> set[Variable]:
        """Find variables in common across all relations in the intersection."""
        if not self.relation_list:
            return set()
        variables_in_common: set[Variable] = set(
            self.relation_list[0].variable_map.keys(),
        )
        for relation in self.relation_list[1:]:
            variables_in_common.intersection_update(
                relation.variable_map.keys(),
            )
        return variables_in_common


class EntityTable(Relation):
    """Source of truth for all IDs and attributes for a specific entity type.

    ``to_pandas`` prefixes every column with the entity type, e.g. a source
    DataFrame with columns ``["__ID__", "name", "age"]`` for entity type
    ``"Person"`` is returned as ``["Person____ID__", "Person__name",
    "Person__age"]``.  This prevents column-name collisions when multiple
    entity types are joined together.

    Attributes:
        entity_type: The entity type label (e.g. ``"Person"``).
        source_obj: The underlying pandas (or PySpark) DataFrame.
        attribute_map: Maps attribute names to column names in the source.
        source_obj_attribute_map: Maps attribute names to the raw column names
            in the underlying ``source_obj`` (always strings).

    """

    entity_type: EntityType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, ColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(
        default_factory=dict,
    )  # Assume all table objects (e.g. DataFrames) have string column names.

    @classmethod
    def from_dataframe(
        cls,
        entity_type: str,
        df: pd.DataFrame,
        id_col: str | None = None,
    ) -> EntityTable:
        """Construct an ``EntityTable`` from a pandas DataFrame.

        This is the recommended factory for the common case where you already
        have a pandas DataFrame.  Column names and attribute maps are inferred
        automatically, eliminating the need to specify them manually.

        Args:
            entity_type: The entity label (e.g. ``"Person"``).
            df: Source pandas DataFrame.  Must contain a column named
                ``__ID__`` unless *id_col* is provided.
            id_col: If the identity column in *df* is not named ``__ID__``,
                pass its current name here — it will be renamed automatically.
                Raises :exc:`ValueError` if the column is not found.

        Returns:
            A new :class:`EntityTable` ready for use in a
            :class:`~pycypher.relational_models.Context`.

        Raises:
            ValueError: If *id_col* is specified but not found, or if
                ``__ID__`` is absent and *id_col* is not provided.

        Examples:
            Minimal usage when ``__ID__`` is already present::

                import pandas as pd
                from pycypher import EntityTable, ID_COLUMN

                df = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
                table = EntityTable.from_dataframe("Person", df)

            Custom identity column::

                df = pd.DataFrame({"person_id": [1, 2], "name": ["Alice", "Bob"]})
                table = EntityTable.from_dataframe("Person", df, id_col="person_id")

        """
        if id_col is not None:
            if id_col not in df.columns:
                msg = (
                    f"id_col={id_col!r} not found in DataFrame columns: "
                    f"{list(df.columns)}"
                )
                raise ValueError(
                    msg,
                )
            df = df.rename(columns={id_col: ID_COLUMN})
        elif ID_COLUMN not in df.columns:
            msg = (
                f"DataFrame must contain a '{ID_COLUMN}' column or you must "
                "specify id_col= to identify which column holds entity IDs."
            )
            raise ValueError(
                msg,
            )

        attribute_map: dict[str, str] = {
            col: col for col in df.columns if col != ID_COLUMN
        }
        return cls(
            entity_type=entity_type,
            source_obj=df,
            column_names=list(df.columns),
            attribute_map=attribute_map,
            source_obj_attribute_map=attribute_map,
        )

    @classmethod
    def from_arrow(
        cls,
        entity_type: str,
        table: pa.Table,
        id_col: str | None = None,
    ) -> EntityTable:
        """Construct an ``EntityTable`` from a PyArrow table.

        Args:
            entity_type: The entity label (e.g. ``"Person"``).
            table: Arrow table that already has an ``__ID__`` column (i.e.
                has been passed through
                :func:`~pycypher.ingestion.arrow_utils.normalize_entity_table`).
            id_col: Ignored — the table is expected to be pre-normalised.
                Kept for API symmetry.

        Returns:
            A new :class:`EntityTable` with ``source_obj`` set to *table*.

        """
        from pycypher.ingestion.arrow_utils import infer_attribute_map

        attribute_map = infer_attribute_map(table)
        return cls(
            entity_type=entity_type,
            source_obj=table,
            column_names=table.column_names,
            attribute_map=attribute_map,
            source_obj_attribute_map=attribute_map,
        )

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Materialise this entity table as a pandas DataFrame with prefixed columns.

        Converts the underlying source (Arrow table or DataFrame) to pandas and
        prefixes every column with :attr:`entity_type` to prevent naming collisions
        in downstream joins (e.g. ``Person____ID__``, ``Person__name``).

        Args:
            context: Current execution context (unused but required by interface).

        Returns:
            A ``pd.DataFrame`` with type-prefixed column names.
        """
        import pyarrow as _pa

        raw = self.source_obj
        df: pd.DataFrame = (
            raw.to_pandas() if isinstance(raw, _pa.Table) else raw
        )
        # Disambiguate columns by prefixing with entity type
        return _prefix_columns(self.entity_type, df)

    def to_spark(self, context: Context) -> Any:
        """Convert EntityTable to a PySpark DataFrame.

        Delegates to :meth:`~pycypher.relational_models.Relation._to_spark_with_prefix`
        using :attr:`entity_type` as the column prefix.  Handles both native PySpark
        DataFrames (column rename only) and non-Spark sources (pandas conversion).

        Args:
            context: PyCypher Context

        Returns:
            PySpark DataFrame with columns prefixed by ``{entity_type}__``.

        Raises:
            ImportError: If PySpark is not installed.

        """
        return self._to_spark_with_prefix(
            context,
            self.entity_type,
            self.source_obj,
        )


class RelationshipTable(Relation):
    """Source of truth for all IDs and attributes for a specific relationship type.

    ``to_pandas`` prefixes every column with the relationship type, e.g. a
    source DataFrame with columns ``["__ID__", "__SOURCE__", "__TARGET__"]``
    for relationship type ``"KNOWS"`` is returned as
    ``["KNOWS____ID__", "KNOWS____SOURCE__", "KNOWS____TARGET__"]``.

    Attributes:
        relationship_type: The relationship type label (e.g. ``"KNOWS"``).
        source_obj: The underlying pandas (or PySpark) DataFrame.
        attribute_map: Maps attribute names to column names in the source.
        source_obj_attribute_map: Maps attribute names to the raw column names
            in the underlying ``source_obj`` (always strings).

    """

    relationship_type: RelationshipType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, ColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(
        default_factory=dict,
    )  # Assume all table objects (e.g. DataFrames) have string column names.

    @classmethod
    def from_dataframe(
        cls,
        relationship_type: str,
        df: pd.DataFrame,
        *,
        id_col: str | None = None,
        source_col: str | None = None,
        target_col: str | None = None,
    ) -> RelationshipTable:
        """Construct a ``RelationshipTable`` from a pandas DataFrame.

        This is the recommended factory for the common case where you already
        have a pandas DataFrame describing relationships.  Column names and
        attribute maps are inferred automatically, eliminating the need to
        specify them manually.

        Args:
            relationship_type: The relationship label (e.g. ``"KNOWS"``).
            df: Source pandas DataFrame.  Must contain columns named
                ``__ID__``, ``__SOURCE__``, and ``__TARGET__`` unless the
                corresponding ``*_col`` parameters are provided.
            id_col: If the identity column in *df* is not named ``__ID__``,
                pass its current name here — it will be renamed automatically.
            source_col: If the source-node column is not named
                ``__SOURCE__``, pass its current name here.
            target_col: If the target-node column is not named
                ``__TARGET__``, pass its current name here.

        Returns:
            A new :class:`RelationshipTable` ready for use in a
            :class:`~pycypher.relational_models.Context`.

        Raises:
            ValueError: If a required column (``__ID__``, ``__SOURCE__``,
                ``__TARGET__``) is missing and the corresponding ``*_col``
                parameter was not provided, or if a specified ``*_col`` is
                not found in the DataFrame.

        Examples:
            Minimal usage when canonical columns are already present::

                import pandas as pd
                from pycypher import RelationshipTable

                df = pd.DataFrame({
                    "__ID__": [1, 2],
                    "__SOURCE__": [10, 20],
                    "__TARGET__": [30, 40],
                    "since": [2020, 2021],
                })
                table = RelationshipTable.from_dataframe("KNOWS", df)

            Custom column names::

                df = pd.DataFrame({
                    "rel_id": [1, 2],
                    "from_node": [10, 20],
                    "to_node": [30, 40],
                })
                table = RelationshipTable.from_dataframe(
                    "KNOWS", df,
                    id_col="rel_id",
                    source_col="from_node",
                    target_col="to_node",
                )

        """
        renames: dict[str, str] = {}

        # --- ID column ---
        if id_col is not None:
            if id_col not in df.columns:
                msg = (
                    f"id_col={id_col!r} not found in DataFrame columns: "
                    f"{list(df.columns)}"
                )
                raise ValueError(msg)
            renames[id_col] = ID_COLUMN
        elif ID_COLUMN not in df.columns:
            msg = (
                f"DataFrame must contain a '{ID_COLUMN}' column or you must "
                "specify id_col= to identify which column holds relationship IDs."
            )
            raise ValueError(msg)

        # --- SOURCE column ---
        if source_col is not None:
            if source_col not in df.columns:
                msg = (
                    f"source_col={source_col!r} not found in DataFrame columns: "
                    f"{list(df.columns)}"
                )
                raise ValueError(msg)
            renames[source_col] = RELATIONSHIP_SOURCE_COLUMN
        elif RELATIONSHIP_SOURCE_COLUMN not in df.columns:
            msg = (
                f"DataFrame must contain a '{RELATIONSHIP_SOURCE_COLUMN}' column "
                "or you must specify source_col= to identify which column holds "
                "source-node IDs."
            )
            raise ValueError(msg)

        # --- TARGET column ---
        if target_col is not None:
            if target_col not in df.columns:
                msg = (
                    f"target_col={target_col!r} not found in DataFrame columns: "
                    f"{list(df.columns)}"
                )
                raise ValueError(msg)
            renames[target_col] = RELATIONSHIP_TARGET_COLUMN
        elif RELATIONSHIP_TARGET_COLUMN not in df.columns:
            msg = (
                f"DataFrame must contain a '{RELATIONSHIP_TARGET_COLUMN}' column "
                "or you must specify target_col= to identify which column holds "
                "target-node IDs."
            )
            raise ValueError(msg)

        if renames:
            df = df.rename(columns=renames)

        structural = {
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        }
        attribute_map: dict[str, str] = {
            col: col for col in df.columns if col not in structural
        }
        return cls(
            relationship_type=relationship_type,
            source_obj=df,
            column_names=list(df.columns),
            attribute_map=attribute_map,
            source_obj_attribute_map=attribute_map,
        )

    @classmethod
    def from_arrow(
        cls,
        relationship_type: str,
        table: pa.Table,
        source_col: str | None = None,
        target_col: str | None = None,
    ) -> RelationshipTable:
        """Construct a ``RelationshipTable`` from a PyArrow table.

        Args:
            relationship_type: The relationship label (e.g. ``"KNOWS"``).
            table: Arrow table that already has ``__ID__``, ``__SOURCE__``,
                and ``__TARGET__`` columns (pre-normalised).
            source_col: Ignored — table is expected to be pre-normalised.
            target_col: Ignored — table is expected to be pre-normalised.

        Returns:
            A new :class:`RelationshipTable` with ``source_obj`` set to *table*.

        """
        from pycypher.ingestion.arrow_utils import infer_attribute_map

        attribute_map = infer_attribute_map(table)
        return cls(
            relationship_type=relationship_type,
            source_obj=table,
            column_names=table.column_names,
            attribute_map=attribute_map,
            source_obj_attribute_map=attribute_map,
        )

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the RelationshipTable to a pandas DataFrame."""
        import pyarrow as _pa

        raw = self.source_obj
        df: pd.DataFrame = (
            raw.to_pandas() if isinstance(raw, _pa.Table) else raw
        )
        # Disambiguate columns by prefixing with relationship type
        return _prefix_columns(self.relationship_type, df)

    def to_spark(self, context: Context) -> Any:
        """Convert RelationshipTable to a PySpark DataFrame.

        Delegates to :meth:`~pycypher.relational_models.Relation._to_spark_with_prefix`
        using :attr:`relationship_type` as the column prefix.  Handles both native
        PySpark DataFrames (column rename only) and non-Spark sources (pandas
        conversion).

        Args:
            context: PyCypher Context

        Returns:
            PySpark DataFrame with columns prefixed by ``{relationship_type}__``.

        Raises:
            ImportError: If PySpark is not installed.

        """
        return self._to_spark_with_prefix(
            context,
            self.relationship_type,
            self.source_obj,
        )
