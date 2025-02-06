"""Command line interface for nmetl"""

import sys

import click
import pyfiglet

from pycypher.util.configuration import load_goldberg_config


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
    goldberg = load_goldberg_config(pathname)
    goldberg()


@main.command()
@click.argument("pathname", type=click.Path(exists=True))
def validate(pathname: str):
    """
    Read a config file and validate it
    """
    try:
        load_goldberg_config(pathname)
    except Exception as e:  # pylint: disable=broad-exception-caught
        click.echo(f"Invalid config file: {e}")
        sys.exit(1)
    sys.exit(0)


main.__doc__ = pyfiglet.figlet_format("nmetl", font="slant")
