"""Command line interface for pycypher"""

import sys

import click

from pycypher.cypher_parser import CypherParser


@click.group()
def main():
    """Entrypoint for the PyCypher CLI"""


@main.command()
@click.argument("query")
def parse(query: str):
    """
    Parse a Cypher query
    """
    CypherParser(query).parsed.print_tree()


@main.command()
@click.argument("query")
def validate(query: str):
    """
    Validate a Cypher query
    """
    try:
        CypherParser(query)
        sys.exit(0)
    except Exception as _:
        click.echo("Invalid Cypher query")
        sys.exit(1)
