"""Command line interface for nmetl"""

import sys

import click
from nmetl.configuration import load_session_config


# fmt: off
@click.group()
def main():
    """
\b
 _   _ __  __ _____ _____     _     \n| \\ | |  \\/  | ____|_   _|_ _| |    \n|  \\| | |\\/| |  _|   | |/ _` | |    \n| |\\  | |  | | |___  | | (_| | |___ \n|_| \\_|_|  |_|_____| |_|\\__,_|_____|\n
\b
ETL with less insanity
    """
# fmt: on


@main.command()
@click.argument("pathname", type=click.Path(exists=True))
def run(pathname: str):
    """
    Read a config file and run an ETL job
    """
    session = load_session_config(pathname)
    session()


@main.command()
@click.argument("pathname", type=click.Path(exists=True))
def validate(pathname: str):
    """
    Read a config file and validate it
    """
    try:
        load_session_config(pathname)
    except Exception as e:  # pylint: disable=broad-exception-caught
        click.echo(f"Invalid config file: {e}")
        sys.exit(1)
    sys.exit(0)
