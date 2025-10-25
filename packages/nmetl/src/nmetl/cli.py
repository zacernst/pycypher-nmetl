"""Command line interface for pycypher"""

import sys

import click
from pycypher.cypher_parser import CypherParser


@click.group()
def main():
    """Entrypoint for the PyCypher CLI"""


@main.command()
@click.argument("cypher_query")
def parse(cypher_query: str):
    """
    Parse a Cypher query and display its parse tree.
    
    Args:
        cypher_query: The Cypher query string to parse
    """
    CypherParser(cypher_query).parse_tree.print_tree()


@main.command()
@click.argument("query")
def validate(query: str):
    """
    Validate a Cypher query
    """
    try:
        CypherParser(query)
        sys.exit(0)
    except Exception as _:  # pylint: disable=broad-exception-caught
        click.echo("Invalid Cypher query")
        sys.exit(1)
