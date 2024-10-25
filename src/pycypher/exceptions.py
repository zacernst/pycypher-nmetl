"""
Custom exceptions for PyCypher
"""


class CypherParsingError(Exception):
    """To be thrown when the `CypherParser` cannot parse the expression at all."""

    pass


class UnexpectedCypherStructureError(Exception):
    """To be thrown when the `CypherParser` can parse the expression, but it has an unexpected structure."""

    pass
