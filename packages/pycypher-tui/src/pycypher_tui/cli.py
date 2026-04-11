"""CLI entry points for PyCypher TUI.

Provides the ``pycypher-tui`` command and the ``nmetl tui`` subcommand
for launching the VIM-style pipeline configuration TUI.

Usage::

    # Open an existing pipeline config
    pycypher-tui pipeline.yaml

    # Create a new pipeline from scratch
    pycypher-tui --new my_pipeline.yaml

    # Create from a template
    pycypher-tui --template csv_analytics --new my_pipeline.yaml

    # List available templates
    pycypher-tui --list-templates
"""

from __future__ import annotations

import sys
from pathlib import Path

from pycypher_tui.config.templates import (
    PipelineTemplate,
    get_template,
    list_templates,
)


def _print_templates() -> None:
    """Print available templates to stdout."""
    templates = list_templates()
    if not templates:
        print("No templates available.")
        return

    print("Available pipeline templates:\n")
    max_name = max(len(t.name) for t in templates)
    for t in templates:
        print(f"  {t.name:<{max_name}}  {t.description}  [{t.category}]")
    print(f"\nUse --template <name> --new <file> to create from a template.")


def _create_new_config(
    filepath: Path,
    template_name: str | None = None,
    project_name: str | None = None,
    data_dir: str = "data",
) -> Path:
    """Create a new pipeline config file.

    Args:
        filepath: Path for the new config file.
        template_name: Optional template to base the config on.
        project_name: Project name (defaults to filename stem).
        data_dir: Data directory path for template instantiation.

    Returns:
        The path to the created file.

    Raises:
        SystemExit: If the file already exists or template not found.
    """
    import yaml
    from pycypher.ingestion.config import PipelineConfig

    if filepath.exists():
        print(f"Error: {filepath} already exists. Use without --new to edit.")
        sys.exit(1)

    if project_name is None:
        project_name = filepath.stem

    if template_name:
        tmpl = get_template(template_name)
        if tmpl is None:
            available = [t.name for t in list_templates()]
            print(f"Error: Unknown template '{template_name}'.")
            print(f"Available: {', '.join(available)}")
            sys.exit(1)
        config = tmpl.instantiate(
            project_name=project_name,
            data_dir=data_dir,
        )
    else:
        config = PipelineConfig(version="1.0")

    filepath.parent.mkdir(parents=True, exist_ok=True)
    data = config.model_dump(exclude_none=True, exclude_defaults=True)
    # Always include version
    data["version"] = config.version
    filepath.write_text(
        yaml.dump(data, default_flow_style=False, sort_keys=False)
    )
    print(f"Created: {filepath}")
    return filepath


def run_tui(
    config_path: str | Path | None = None,
    new: bool = False,
    template: str | None = None,
    list_templates_flag: bool = False,
) -> None:
    """Launch the TUI application.

    Args:
        config_path: Optional path to a pipeline config YAML.
        new: If True, create a new config file first.
        template: Template name for --new creation.
        list_templates_flag: If True, print templates and exit.
    """
    if list_templates_flag:
        _print_templates()
        return

    if new and config_path:
        path = Path(config_path)
        _create_new_config(path, template_name=template)
        config_path = path

    from pycypher_tui.app import PyCypherTUI

    app = PyCypherTUI(config_path=config_path)
    app.run()


def cli_main() -> None:
    """Main CLI entry point for ``pycypher-tui`` command.

    Parses command-line arguments and launches the TUI.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="pycypher-tui",
        description="VIM-style TUI for PyCypher ETL pipeline configuration",
    )
    parser.add_argument(
        "config",
        nargs="?",
        default=None,
        help="Path to pipeline configuration YAML file",
    )
    parser.add_argument(
        "--new",
        action="store_true",
        help="Create a new pipeline configuration file",
    )
    parser.add_argument(
        "--template",
        type=str,
        default=None,
        help="Template to use when creating a new config (requires --new)",
    )
    parser.add_argument(
        "--list-templates",
        action="store_true",
        help="List available pipeline templates and exit",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_get_version()}",
    )

    args = parser.parse_args()

    if args.template and not args.new:
        parser.error("--template requires --new")

    if args.new and not args.config:
        parser.error("--new requires a config file path")

    run_tui(
        config_path=args.config,
        new=args.new,
        template=args.template,
        list_templates_flag=args.list_templates,
    )


def _get_version() -> str:
    """Get the package version."""
    try:
        from pycypher_tui import __version__

        return __version__
    except ImportError:
        return "unknown"
