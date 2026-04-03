"""BFS-based path expansion for variable-length and shortest-path patterns.

Extracted from :class:`~pycypher.star.Star` to reduce the god object.
The ``PathExpander`` handles all BFS-related operations:

1. **Variable-length paths** — ``[*m..n]`` hop-bounded BFS expansion.
2. **Shortest path** — ``shortestPath`` and ``allShortestPaths`` BFS.

The expander operates on :class:`~pycypher.binding_frame.BindingFrame`
instances and returns new frames with expanded path results.

Architecture
------------

::

    PathExpander
    ├── expand_variable_length_path()  — BFS with hop-bounded frontier
    └── shortest_path_to_binding_frame()  — BFS + min-hop filter
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX, BindingFrame

if TYPE_CHECKING:
    from pycypher.ast_models import PatternPath, RelationshipDirection
    from pycypher.relational_models import Context

#: Temporary column used during BFS to track the frontier tip.
_VL_TIP_COL: str = "_vl_tip"

#: Hard cap on BFS hops for unbounded variable-length paths.
_MAX_UNBOUNDED_PATH_HOPS: int = 20

#: Hard cap on BFS frontier rows to prevent memory exhaustion in
#: highly-connected graphs.  When the frontier exceeds this after
#: deduplication, a :class:`SecurityError` is raised.
_MAX_FRONTIER_ROWS: int = 1_000_000

#: Hard cap on total accumulated BFS result rows across all hops.
#: Prevents memory exhaustion from collecting many hops of a
#: moderately-connected graph.
_MAX_BFS_TOTAL_ROWS: int = 5_000_000


class PathExpander:
    """BFS-based path expansion for variable-length and shortest-path patterns.

    Algorithm overview
    ~~~~~~~~~~~~~~~~~~

    For a pattern like ``(a)-[:KNOWS*1..3]->(b)``:

    1. **Seed** — the starting BindingFrame provides initial node IDs in
       column ``a``.
    2. **Frontier expansion** — at each hop, the frontier is joined with
       the relationship table to discover next-hop nodes.  The frontier is
       deduplicated per (start, current-tip) pair to avoid exponential
       blowup in cyclic graphs.
    3. **Hop collection** — rows at each hop in ``[min_hops, max_hops]``
       are collected into the result.
    4. **Safety limits** — unbounded paths (``[*]``) are capped at
       :data:`_MAX_UNBOUNDED_PATH_HOPS` (20) hops.  The frontier size
       is capped at :data:`_MAX_FRONTIER_ROWS` (1M rows) and total
       accumulated results at :data:`_MAX_BFS_TOTAL_ROWS` (5M rows);
       exceeding either raises :class:`SecurityError`.

    Complexity: O(V + E * max_hops) per seed row, where V = nodes and
    E = edges of the traversed type.

    Args:
        context: The execution context with entity/relationship tables.

    """

    def __init__(self, context: Context) -> None:
        """Initialize path expander.

        Args:
            context: Execution context with entity/relationship tables
                used for BFS frontier expansion.

        """
        self.context = context

    def expand_variable_length_path(
        self,
        start_frame: BindingFrame,
        start_var: str,
        rel_type: str,
        direction: RelationshipDirection,
        end_var: str,
        end_type: str | None,
        min_hops: int,
        max_hops: int | None,
        anon_counter: list[int],
        path_length_col: str | None = None,
        row_limit: int | None = None,
    ) -> BindingFrame:
        """BFS hop-by-hop expansion of a variable-length relationship pattern.

        Builds a :class:`BindingFrame` containing one row per reachable
        (start, end) pair at each hop count in ``[min_hops, max_hops]``.
        The frontier is deduplicated at each hop to avoid exponential blowup.

        When *row_limit* is provided, expansion stops early once enough rows
        have been collected.

        Args:
            start_frame: BindingFrame whose ``start_var`` column seeds the BFS.
            start_var: Column in *start_frame* that holds the starting node IDs.
            rel_type: Relationship type to traverse.
            direction: Traversal direction.
            end_var: Output column name for the reached endpoint IDs.
            end_type: Entity type string for *end_var* in the type registry.
            min_hops: Minimum hop count to include in results.
            max_hops: Maximum hop count; ``None`` means unbounded.
            anon_counter: Mutable counter for synthetic variable names.
            path_length_col: If given, adds an integer column recording hop count.
            row_limit: If given, stop BFS once this many result rows accumulated.

        Returns:
            A :class:`BindingFrame` with columns from *start_frame* plus
            *end_var* (and optionally *path_length_col*).

        """
        from pycypher.ast_models import RelationshipDirection as _RD
        from pycypher.relational_models import (
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        )

        max_val = (
            max_hops if max_hops is not None else _MAX_UNBOUNDED_PATH_HOPS
        )

        rel_table = self.context.relationship_mapping[rel_type]

        src_col = RELATIONSHIP_SOURCE_COLUMN
        tgt_col = RELATIONSHIP_TARGET_COLUMN
        is_left = direction == _RD.LEFT

        # Cache the edge projection (src, tgt only) to avoid repeated
        # Arrow→pandas conversion and column slicing on every BFS call.
        _edge_cache: dict = getattr(self.context, "_property_lookup_cache", {})
        _edge_key = f"__edge_proj__{rel_type}"
        if _edge_key in _edge_cache:
            edge_df = _edge_cache[_edge_key]
        else:
            try:
                import pyarrow as pa

                raw = rel_table.source_obj
                rel_df: pd.DataFrame = (
                    raw.to_pandas() if isinstance(raw, pa.Table) else raw
                )
            except ImportError:
                rel_df = rel_table.source_obj
            edge_df = rel_df[[src_col, tgt_col]]
            _edge_cache[_edge_key] = edge_df

        # frontier: all columns of start_frame + _VL_TIP_COL (current endpoint)
        frontier = start_frame.bindings.assign(
            **{_VL_TIP_COL: start_frame.bindings[start_var]},
        )

        result_parts: list[pd.DataFrame] = []
        accumulated_rows: int = 0

        for hop in range(1, max_val + 1):
            # Cooperative timeout check — abort BFS if the query deadline passed.
            self.context.check_timeout()

            if len(frontier) == 0:
                break

            if is_left:
                merged = frontier.merge(
                    edge_df,
                    left_on=_VL_TIP_COL,
                    right_on=tgt_col,
                    how="inner",
                ).drop(columns=[tgt_col, _VL_TIP_COL])
                frontier = merged.rename(columns={src_col: _VL_TIP_COL})
            else:
                merged = frontier.merge(
                    edge_df,
                    left_on=_VL_TIP_COL,
                    right_on=src_col,
                    how="inner",
                ).drop(columns=[src_col, _VL_TIP_COL])
                frontier = merged.rename(columns={tgt_col: _VL_TIP_COL})

            if frontier.empty:
                break

            # Deduplicate on (start_var, _vl_tip) to prevent exponential blowup.
            # Use boolean mask instead of drop_duplicates() + reset_index() to
            # avoid two full DataFrame copies per hop — merge doesn't need a
            # contiguous index so we skip the reset entirely.
            _dedup_mask = ~frontier.duplicated(subset=[start_var, _VL_TIP_COL])
            if not _dedup_mask.all():
                frontier = frontier.loc[_dedup_mask]

            if len(frontier) > _MAX_FRONTIER_ROWS:
                from pycypher.exceptions import SecurityError

                msg = (
                    f"BFS frontier at hop {hop} has {len(frontier):,} rows "
                    f"for [:{rel_type}*] — exceeds safety limit of "
                    f"{_MAX_FRONTIER_ROWS:,}. Bind the path length "
                    f"(e.g. [*1..3]) or reduce the graph connectivity."
                )
                raise SecurityError(msg)
            if len(frontier) > 500_000:
                LOGGER.warning(
                    "BFS frontier at hop %d has %s rows for [:%s*] — "
                    "consider bounding the path length",
                    hop,
                    f"{len(frontier):,}",
                    rel_type,
                )

            if hop >= min_hops:
                part = frontier.drop(columns=[_VL_TIP_COL]).assign(
                    **{end_var: frontier[_VL_TIP_COL].values},
                )
                if path_length_col is not None:
                    part = part.assign(**{path_length_col: hop})

                # LIMIT pushdown: trim this hop's results if we have enough
                if row_limit is not None:
                    remaining = row_limit - accumulated_rows
                    if remaining <= 0:
                        break
                    if len(part) > remaining:
                        part = part.iloc[:remaining]

                result_parts.append(part)
                accumulated_rows += len(result_parts[-1])

                if row_limit is not None and accumulated_rows >= row_limit:
                    LOGGER.debug(
                        "BFS early termination: row_limit=%d reached at hop %d",
                        row_limit,
                        hop,
                    )
                    break

                # Hard safety cap on total accumulated BFS rows.
                if accumulated_rows > _MAX_BFS_TOTAL_ROWS:
                    from pycypher.exceptions import SecurityError

                    msg = (
                        f"BFS accumulated {accumulated_rows:,} result rows "
                        f"across {hop} hops for [:{rel_type}*] — exceeds "
                        f"safety limit of {_MAX_BFS_TOTAL_ROWS:,}. "
                        f"Bind the path length (e.g. [*1..3]) or add "
                        f"a LIMIT clause."
                    )
                    raise SecurityError(msg)

        cols = [*list(start_frame.bindings.columns), end_var]
        if path_length_col:
            cols.append(path_length_col)
        type_reg: dict[str, str] = dict(start_frame.type_registry.items())
        if end_type:
            type_reg[end_var] = end_type

        if not result_parts:
            empty = pd.DataFrame({c: pd.Series(dtype=object) for c in cols})
            return BindingFrame(
                bindings=empty,
                type_registry=type_reg,
                context=start_frame.context,
            )

        _bfs_t0 = time.perf_counter()
        combined = pd.concat(result_parts, ignore_index=True)
        LOGGER.debug(
            "BFS concat  parts=%d  rows=%d  elapsed=%.3fs",
            len(result_parts),
            len(combined),
            time.perf_counter() - _bfs_t0,
        )
        return BindingFrame(
            bindings=combined,
            type_registry=type_reg,
            context=start_frame.context,
        )

    def shortest_path_to_binding_frame(
        self,
        path: PatternPath,
        anon_counter: list[int],
        context_frame: BindingFrame | None = None,
        node_scanner: Any = None,
    ) -> BindingFrame:
        """Execute a shortestPath / allShortestPaths pattern via BFS.

        Runs BFS from the start node, then filters to the minimum-hop rows per
        (start, end) pair.

        Args:
            path: PatternPath with ``shortest_path_mode`` set.
            anon_counter: Mutable counter for synthetic variable names.
            context_frame: Optional preceding BindingFrame for variable binding.
            node_scanner: Callable that scans a NodePattern into a BindingFrame.
                Signature: ``(node, anon_counter, context_frame) -> BindingFrame``.

        Returns:
            A BindingFrame with the shortest-path result rows.

        """
        _ANON_NODE_PREFIX = "_anon_node_"

        elements = path.elements
        if len(elements) != 3:
            from pycypher.exceptions import PatternComprehensionError

            msg = (
                f"shortestPath / allShortestPaths requires exactly "
                f"3 pattern elements (node-rel-node), got {len(elements)}. "
                "Example: shortestPath((a:Person)-[:KNOWS*]->(b:Person))"
            )
            raise PatternComprehensionError(msg)

        from pycypher.ast_models import NodePattern, RelationshipPattern

        node_ast_start = elements[0]
        rel_ast = elements[1]
        node_ast_end = elements[2]
        assert isinstance(rel_ast, RelationshipPattern)
        assert isinstance(node_ast_start, NodePattern)
        assert isinstance(node_ast_end, NodePattern)

        # Resolve start variable name
        if node_ast_start.variable is not None:
            start_var = node_ast_start.variable.name
        else:
            start_var = f"{_ANON_NODE_PREFIX}{anon_counter[0]}"
            anon_counter[0] += 1

        # Resolve end variable name
        if node_ast_end.variable is not None:
            end_var = node_ast_end.variable.name
        else:
            end_var = f"{_ANON_NODE_PREFIX}{anon_counter[0]}"
            anon_counter[0] += 1

        # Determine relationship type
        if rel_ast.labels:
            if len(rel_ast.labels) != 1:
                from pycypher.exceptions import PatternComprehensionError

                msg = "shortestPath requires exactly one relationship type label."
                raise PatternComprehensionError(msg)
            rel_type = rel_ast.labels[0]
        else:
            from pycypher.exceptions import PatternComprehensionError

            msg = (
                "shortestPath requires exactly one relationship type. "
                "Use e.g. shortestPath((a)-[:KNOWS*]->(b)) "
                "instead of shortestPath((a)-[*]->(b))"
            )
            raise PatternComprehensionError(msg)

        end_type = node_ast_end.labels[0] if node_ast_end.labels else None

        path_var_name: str | None = (
            path.variable.name if path.variable is not None else None
        )
        _TMP_HOP_COL = "__sp_hops__"

        ctx_bindings = (
            context_frame.bindings if context_frame is not None else None
        )
        start_pre_bound = (
            ctx_bindings is not None and start_var in ctx_bindings.columns
        )
        end_pre_bound = (
            ctx_bindings is not None and end_var in ctx_bindings.columns
        )

        if start_pre_bound:
            assert (
                context_frame is not None
            )  # guaranteed by start_pre_bound check
            bfs_end_var = f"__sp_end_{end_var}__" if end_pre_bound else end_var
            bfs_frame = self.expand_variable_length_path(
                start_frame=context_frame,
                start_var=start_var,
                rel_type=rel_type,
                direction=rel_ast.direction,
                end_var=bfs_end_var,
                end_type=end_type,
                min_hops=1,
                max_hops=None,
                anon_counter=anon_counter,
                path_length_col=_TMP_HOP_COL,
            )
        else:
            if node_scanner is None:
                from pycypher.exceptions import PatternComprehensionError

                msg = "node_scanner is required when start variable is not pre-bound"
                raise PatternComprehensionError(msg)
            start_frame = node_scanner(
                node_ast_start,
                anon_counter,
                context_frame=context_frame,
            )
            bfs_end_var = end_var
            bfs_frame = self.expand_variable_length_path(
                start_frame=start_frame,
                start_var=start_var,
                rel_type=rel_type,
                direction=rel_ast.direction,
                end_var=bfs_end_var,
                end_type=end_type,
                min_hops=1,
                max_hops=None,
                anon_counter=anon_counter,
                path_length_col=_TMP_HOP_COL,
            )

        if bfs_frame.bindings.empty:
            base_cols = (
                list(context_frame.bindings.columns)
                if start_pre_bound and context_frame is not None
                else [start_var]
            )
            out_cols = base_cols + ([] if end_pre_bound else [end_var])
            if path_var_name:
                out_cols.append(f"{PATH_HOP_COLUMN_PREFIX}{path_var_name}")
            empty_df = pd.DataFrame(
                {c: pd.Series(dtype=object) for c in out_cols},
            )
            type_reg = dict(bfs_frame.type_registry)
            return BindingFrame(
                bindings=empty_df,
                type_registry=type_reg,
                context=self.context,
            )

        df = bfs_frame.bindings

        # Keep only minimum-hop rows per (start_var, bfs_end_var) pair.
        min_hops_per_pair = df.groupby([start_var, bfs_end_var], sort=False)[
            _TMP_HOP_COL
        ].transform("min")
        df = df[df[_TMP_HOP_COL] == min_hops_per_pair].reset_index(drop=True)

        if path.shortest_path_mode == "one":
            df = df.drop_duplicates(
                subset=[start_var, bfs_end_var],
                keep="first",
            ).reset_index(drop=True)

        if end_pre_bound:
            df = df[df[bfs_end_var] == df[end_var]].reset_index(drop=True)
            df = df.drop(columns=[bfs_end_var])

        # Rename hop column to the declared path variable (or drop it).
        if path_var_name is not None:
            hop_col_final = f"{PATH_HOP_COLUMN_PREFIX}{path_var_name}"
            df = df.rename(columns={_TMP_HOP_COL: hop_col_final})
        else:
            df = df.drop(columns=[_TMP_HOP_COL])

        type_reg = dict(bfs_frame.type_registry)
        if end_pre_bound and bfs_end_var in type_reg:
            if end_var not in type_reg:
                type_reg[end_var] = type_reg.pop(bfs_end_var)
            else:
                type_reg.pop(bfs_end_var, None)

        return BindingFrame(
            bindings=df.reset_index(drop=True),
            type_registry=type_reg,
            context=self.context,
        )
