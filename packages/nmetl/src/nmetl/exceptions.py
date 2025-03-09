"""
Custom exceptions for PyCypher
"""


class CypherParsingError(Exception):
    """To be thrown when the `CypherParser` cannot parse the expression at all."""


class UnexpectedCypherStructureError(Exception):
    """To be thrown when the `CypherParser` can parse the expression, but it has
    an unexpected structure."""


class WrongCypherTypeError(Exception):
    """To be thrown when the `CypherParser` can parse the expression, but it has
    an unexpected type."""


class InvalidCastError(Exception):
    """The Session tries to do an impossible cast on a row value."""


class UnknownDataSourceError(Exception):
    """The Session tries to access a data source that is not available."""
