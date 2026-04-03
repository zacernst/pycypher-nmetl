"""Utility commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


# ---------------------------------------------------------------------------
# Implementation
# ---------------------------------------------------------------------------


def compat_check_impl(
    *,
    snapshot: Path | None,
    diff_path: Path | None,
    neo4j_feature: str | None,
    neo4j_all: bool,
) -> None:
    """Check API compatibility and migration status."""
    from shared.compat import (
        NEO4J_COMPAT_NOTES,
        check_neo4j_compat,
        diff_surfaces,
        load_snapshot,
        save_snapshot,
        snapshot_api_surface,
    )

    if snapshot is not None:
        surface = snapshot_api_surface("pycypher")
        save_snapshot(surface, snapshot)
        click.echo(
            f"Saved API snapshot: {len(surface.symbols)} symbols "
            f"(v{surface.version}) → {snapshot}",
        )
        return

    if diff_path is not None:
        old = load_snapshot(diff_path)
        current = snapshot_api_surface("pycypher")
        report = diff_surfaces(old, current)
        click.echo(report.summary())
        if report.has_breaking_changes:
            raise SystemExit(1)
        return

    if neo4j_feature is not None:
        result = check_neo4j_compat(neo4j_feature)
        if result is None:
            click.echo(f"No compatibility notes found for '{neo4j_feature}'.")
            raise SystemExit(1)
        status = "SUPPORTED" if result["supported"] else "NOT SUPPORTED"
        click.echo(f"{result['feature']}: {status}")
        click.echo(f"  {result['notes']}")
        if "workaround" in result:
            click.echo(f"  Workaround: {result['workaround']}")
        return

    if neo4j_all:
        for feature, info in NEO4J_COMPAT_NOTES.items():
            status = "+" if info["supported"] else "-"
            click.echo(f"  [{status}] {feature}")
            click.echo(f"      {info['notes']}")
            if "workaround" in info:
                click.echo(f"      Workaround: {info['workaround']}")
        return

    # Default: show current API surface summary
    surface = snapshot_api_surface("pycypher")
    click.echo(
        f"PyCypher v{surface.version} — {len(surface.symbols)} public symbols",
    )
    by_kind: dict[str, list[str]] = {}
    for sym in surface.symbols.values():
        by_kind.setdefault(sym.kind, []).append(sym.name)
    for kind in sorted(by_kind):
        names = sorted(by_kind[kind])
        click.echo(f"\n  {kind}s ({len(names)}):")
        for name in names:
            click.echo(f"    {name}")


# ---------------------------------------------------------------------------
# Click command wrapper
# ---------------------------------------------------------------------------


@click.command("compat-check")
@click.option(
    "--snapshot",
    type=click.Path(path_type=Path),
    default=None,
    help="Save API surface snapshot to a JSON file.",
)
@click.option(
    "--diff",
    "diff_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Compare current API against a saved snapshot and report changes.",
)
@click.option(
    "--neo4j",
    "neo4j_feature",
    type=str,
    default=None,
    help="Check PyCypher compatibility for a Neo4j Cypher feature.",
)
@click.option(
    "--neo4j-all",
    is_flag=True,
    default=False,
    help="List all Neo4j Cypher compatibility notes.",
)
def compat_check(
    *,
    snapshot: Path | None,
    diff_path: Path | None,
    neo4j_feature: str | None,
    neo4j_all: bool,
) -> None:
    r"""Check API compatibility and migration status.

    Capture the current PyCypher public API surface as a snapshot,
    compare against a previous snapshot to detect breaking changes,
    or check Neo4j Cypher feature compatibility.

    \b
    Examples:
      nmetl compat-check --snapshot api_v0.0.19.json
      nmetl compat-check --diff api_v0.0.18.json
      nmetl compat-check --neo4j "LOAD CSV"
      nmetl compat-check --neo4j-all
    """
    compat_check_impl(
        snapshot=snapshot,
        diff_path=diff_path,
        neo4j_feature=neo4j_feature,
        neo4j_all=neo4j_all,
    )
