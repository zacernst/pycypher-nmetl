"""Command-line interface for the FastOpenData client."""

from __future__ import annotations

import logging
import sys
from typing import NoReturn

import click
from shared.logger import LOGGER


def _cli_error(message: str, *, exit_code: int = 1) -> NoReturn:
    """Print an error message to stderr and exit."""
    click.echo(f"Error: {message}", err=True)
    sys.exit(exit_code)


# ---------------------------------------------------------------------------
# Top-level CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable INFO-level logging.",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable DEBUG-level logging (implies --verbose).",
)
def cli(*, verbose: bool, debug: bool) -> None:
    r"""FastOpenData CLI for getting started quickly"""

    if debug:
        LOGGER.setLevel(logging.DEBUG)
    elif verbose:
        LOGGER.setLevel(logging.INFO)


@cli.command()
def signup():
    """Sign up for the free tier of FastOpenData"""
    click.echo("Signup will happen here")


@cli.command()
def usage():
    """Check your usage"""
    click.echo("Usage will happen here")


@click.option(
    "--free-form-address",
    is_flag=False,
    help="An entire address as a single string",
)
@click.option(
    "--street-number",
    is_flag=False,
    help="Street and number (e.g. 123 Main Street)",
)
@click.option(
    "--city",
    is_flag=False,
    help="City name",
)
@click.option(
    "--state",
    is_flag=False,
    help="State name or abbreviation",
)
@click.option(
    "--zip-code",
    is_flag=False,
    help="ZIP code, optionally with +4 digits",
)
@cli.command()
def get_address_data():
    """Get all available data for a single US address"""
    click.echo("We will fetch an address...")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the client"""
    cli()


if __name__ == "__main__":
    main()
