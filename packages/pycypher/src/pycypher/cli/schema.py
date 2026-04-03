"""Schema and metadata commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path
from typing import NoReturn

import click


def _cli_error(message: str, *, exit_code: int = 1) -> NoReturn:
    """Print an error message to stderr and exit."""
    from pycypher.cli.common import cli_error

    cli_error(message, exit_code=exit_code)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _context_to_schema_dot(
    entity_types: dict[str, list[str]],
    rel_types: dict[str, tuple[str, str, list[str]]],
) -> str:
    """Render a graph schema as a Graphviz DOT digraph.

    Args:
        entity_types: Maps entity label → list of property names.
        rel_types: Maps relationship label → (source_entity, target_entity, properties).

    Returns:
        DOT source string.

    """
    lines = [
        "digraph Schema {",
        "  rankdir=LR;",
        '  node [shape=record, style=filled, fillcolor="#d4edda"];',
        '  edge [fontsize=10, color="#666666"];',
    ]
    for label, props in sorted(entity_types.items()):
        prop_str = "\\l".join(props) + "\\l" if props else ""
        lines.append(f'  "{label}" [label="{{{label}|{prop_str}}}"];')
    for rel_label, (src, tgt, props) in sorted(rel_types.items()):
        label_parts = [rel_label]
        if props:
            label_parts.append("\\n" + ", ".join(props))
        lines.append(
            f'  "{src}" -> "{tgt}" [label="{" ".join(label_parts)}"];',
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Implementation functions
# ---------------------------------------------------------------------------


def functions_impl(
    *,
    show_agg: bool,
    show_scalar: bool,
    verbose: bool,
    as_json: bool,
) -> None:
    """List all available Cypher functions."""
    import json

    from pycypher.aggregation_evaluator import KNOWN_AGGREGATIONS
    from pycypher.scalar_functions import ScalarFunctionRegistry

    # Default: show both when neither flag is set
    if not show_agg and not show_scalar:
        show_agg = True
        show_scalar = True

    registry = ScalarFunctionRegistry.get_instance()

    if as_json:
        result: dict[str, list[dict[str, str]]] = {}
        if show_scalar:
            scalar_list: list[dict[str, str]] = []
            for name in registry.list_functions():
                entry: dict[str, str] = {
                    "name": name,
                    "category": "scalar",
                }
                meta = registry._functions.get(name)
                if meta and meta.description:
                    entry["description"] = meta.description
                if meta and meta.example:
                    entry["example"] = meta.example
                scalar_list.append(entry)
            result["scalar"] = scalar_list
        if show_agg:
            result["aggregation"] = [
                {"name": name, "category": "aggregation"}
                for name in sorted(KNOWN_AGGREGATIONS)
            ]
        click.echo(json.dumps(result, indent=2))
        return

    if show_scalar:
        func_names = registry.list_functions()
        click.echo(f"Scalar functions ({len(func_names)}):")
        for name in func_names:
            meta = registry._functions.get(name)
            if verbose and meta and meta.description:
                click.echo(f"  {name:30s} {meta.description}")
                if meta.example:
                    click.echo(f"  {'':30s} Example: {meta.example}")
            else:
                click.echo(f"  {name}")

    if show_agg:
        if show_scalar:
            click.echo()
        agg_names = sorted(KNOWN_AGGREGATIONS)
        click.echo(f"Aggregation functions ({len(agg_names)}):")
        for name in agg_names:
            click.echo(f"  {name}")


def schema_impl(
    entity_specs: tuple[str, ...],
    rel_specs: tuple[str, ...],
    default_id_col: str | None,
    fmt: str,
    output_path: Path | None,
) -> None:
    """Inspect the graph schema from data sources."""
    from pycypher.ingestion.context_builder import ContextBuilder
    from pycypher.nmetl_cli import _load_data_source, _parse_entity_arg, _parse_rel_arg
    from pycypher.relational_models import ID_COLUMN

    if not entity_specs and not rel_specs:
        _cli_error("at least one --entity source is required.")

    # Parse specs (reuse existing parsers)
    parsed_entities: list[tuple[str, str, str | None]] = []
    for spec in entity_specs:
        try:
            parsed_entities.append(_parse_entity_arg(spec))
        except ValueError as exc:
            _cli_error(f"invalid --entity spec: {exc}")

    parsed_rels: list[tuple[str, str, str, str]] = []
    for spec in rel_specs:
        try:
            parsed_rels.append(_parse_rel_arg(spec))
        except ValueError as exc:
            _cli_error(f"invalid --rel spec: {exc}")

    # Build context
    builder = ContextBuilder()
    for label, path, id_col in parsed_entities:
        effective_id = id_col or default_id_col
        _load_data_source(
            lambda _l=label, _p=path, _i=effective_id: builder.add_entity(
                _l,
                _p,
                id_col=_i,
            ),
            "entity source",
            path,
        )

    for rel_type, path, src_col, tgt_col in parsed_rels:
        _load_data_source(
            lambda _r=rel_type, _p=path, _s=src_col, _t=tgt_col: (
                builder.add_relationship(
                    _r,
                    _p,
                    source_col=_s,
                    target_col=_t,
                )
            ),
            "relationship source",
            path,
        )

    ctx = builder.build()

    # Extract schema info
    entity_types: dict[str, list[str]] = {}
    for label, table in sorted(ctx.entity_mapping.mapping.items()):
        props = [
            c for c in table.attribute_map if c not in {ID_COLUMN, "__ID__"}
        ]
        entity_types[label] = sorted(props)

    rel_types: dict[str, tuple[str, str, list[str]]] = {}
    for rel_label, table in sorted(ctx.relationship_mapping.mapping.items()):
        # Determine source/target entity types from data
        src_entity = "?"
        tgt_entity = "?"
        props = [
            c
            for c in table.attribute_map
            if c not in {ID_COLUMN, "__ID__", "__SOURCE__", "__TARGET__"}
        ]
        rel_types[rel_label] = (src_entity, tgt_entity, sorted(props))

    # Output
    if fmt == "dot":
        text = _context_to_schema_dot(entity_types, rel_types)
        if output_path is not None:
            output_path.write_text(text, encoding="utf-8")
            click.echo(f"Wrote schema DOT → {output_path}")
        else:
            click.echo(text, nl=False)
    elif fmt == "json":
        import json

        schema_dict = {
            "entities": {
                label: {"properties": props}
                for label, props in entity_types.items()
            },
            "relationships": {
                label: {"source": src, "target": tgt, "properties": props}
                for label, (src, tgt, props) in rel_types.items()
            },
        }
        text = json.dumps(schema_dict, indent=2) + "\n"
        if output_path is not None:
            output_path.write_text(text, encoding="utf-8")
            click.echo(f"Wrote schema JSON → {output_path}")
        else:
            click.echo(text, nl=False)
    else:  # table
        click.echo("Entity Types:")
        click.echo("-" * 50)
        for label, props in sorted(entity_types.items()):
            click.echo(f"  :{label}")
            for p in props:
                click.echo(f"    .{p}")
        if rel_types:
            click.echo("\nRelationship Types:")
            click.echo("-" * 50)
            for rel_label, (src, tgt, props) in sorted(rel_types.items()):
                arrow = f"(:{src})-[:{rel_label}]->(:{tgt})"
                click.echo(f"  {arrow}")
                for p in props:
                    click.echo(f"    .{p}")
        click.echo(
            f"\n{len(entity_types)} entity type(s), "
            f"{len(rel_types)} relationship type(s)",
        )


# ---------------------------------------------------------------------------
# Click command wrappers
# ---------------------------------------------------------------------------


@click.command("functions")
@click.option(
    "--agg",
    "show_agg",
    is_flag=True,
    default=False,
    help="Show aggregation functions only.",
)
@click.option(
    "--scalar",
    "show_scalar",
    is_flag=True,
    default=False,
    help="Show scalar functions only.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show descriptions and examples for each function.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Output function list as JSON (for scripting and IDE integration).",
)
def functions(
    *,
    show_agg: bool,
    show_scalar: bool,
    verbose: bool,
    as_json: bool,
) -> None:
    """List all available Cypher functions."""
    functions_impl(
        show_agg=show_agg,
        show_scalar=show_scalar,
        verbose=verbose,
        as_json=as_json,
    )


@click.command("schema")
@click.option(
    "--entity",
    "entity_specs",
    multiple=True,
    metavar="LABEL=PATH[:ID_COL]",
    help="Entity data source.  Same format as ``nmetl query --entity``.",
)
@click.option(
    "--rel",
    "rel_specs",
    multiple=True,
    metavar="TYPE=PATH:SRC_COL:TGT_COL",
    help="Relationship data source.  Same format as ``nmetl query --rel``.",
)
@click.option(
    "--id-col",
    "default_id_col",
    default=None,
    metavar="COL",
    help="Default ID column name for entity sources that do not specify one.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "dot", "json"], case_sensitive=False),
    default="table",
    help="Output format: 'table' (default), 'dot' (Graphviz), or 'json'.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Write output to a file instead of stdout.",
)
def schema(
    entity_specs: tuple[str, ...],
    rel_specs: tuple[str, ...],
    default_id_col: str | None,
    fmt: str,
    output_path: Path | None,
) -> None:
    """Inspect the graph schema from data sources."""
    schema_impl(entity_specs, rel_specs, default_id_col, fmt, output_path)
