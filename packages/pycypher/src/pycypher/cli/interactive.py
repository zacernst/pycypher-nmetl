"""Interactive commands for nmetl CLI."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Load a pipeline config for the REPL session.",
)
@click.option(
    "--history-file",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to command history file (default: ~/.nmetl_history).",
)
@click.option(
    "--startup-commands",
    type=str,
    default=None,
    help="Cypher commands to run on startup (semicolon-separated).",
)
def repl(
    config: Path | None,
    history_file: Path | None,
    startup_commands: str | None,
) -> None:
    """Start an interactive Cypher REPL session."""
    # Import the original implementation
    from pycypher.nmetl_cli import repl as _original_repl

    # Delegate to original implementation
    _original_repl(config, history_file, startup_commands)