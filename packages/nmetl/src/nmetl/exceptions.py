"""Custom exceptions for the PyCypher-NMETL package.

This module defines exception classes used throughout the NMETL (Network
Metadata ETL) package for handling specific error conditions during data
processing, queue operations, and trigger execution.
"""


class EmptyQueueError(Exception):
    """Exception raised when attempting to access an empty queue.
    
    This exception is thrown when queue operations are attempted on
    empty queues where data is expected to be present.
    """


class CypherParsingError(Exception):
    """Exception raised when Cypher query parsing fails completely.
    
    This exception is thrown when the CypherParser encounters a query
    that cannot be parsed due to syntax errors or unsupported constructs.
    """


class BadTriggerReturnAnnotationError(Exception):
    """Exception raised when a trigger function has an invalid return annotation.
    
    This exception is thrown when trigger functions are defined with
    return type annotations that don't match the expected trigger patterns.
    """


class UnexpectedCypherStructureError(Exception):
    """Exception raised when parsed Cypher has unexpected structure.
    
    This exception is thrown when the CypherParser successfully parses
    a query but the resulting AST structure doesn't match expectations
    for the current processing context.
    """


class WrongCypherTypeError(Exception):
    """Exception raised when parsed Cypher has unexpected type.
    
    This exception is thrown when the CypherParser successfully parses
    a query but the resulting type doesn't match what was expected
    for the current operation.
    """


class InvalidCastError(Exception):
    """Exception raised when data type casting fails.
    
    This exception is thrown when the Session attempts to cast a row
    value to an incompatible type during data processing.
    """


class UnknownDataSourceError(Exception):
    """Exception raised when accessing a non-existent data source.
    
    This exception is thrown when the Session attempts to access
    a data source that has not been registered or is not available.
    """
