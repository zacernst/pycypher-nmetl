"""Custom exceptions for the PyCypher package.

This module defines custom exception classes used throughout the PyCypher
package for handling specific error conditions during query parsing and
execution.
"""


class WrongCypherTypeError(Exception):
    """Exception raised when a Cypher expression has an unexpected type.
    
    This exception is thrown when the CypherParser can parse an expression
    but the resulting type is not what was expected for the given context.
    
    Attributes:
        message: Human-readable error message describing the type mismatch.
    """

    def __init__(self, message):
        """Initialize the exception with an error message.
        
        Args:
            message: Description of the type error that occurred.
        """
        self.message = message
        super().__init__(self.message)


class InvalidCastError(Exception):
    """Exception raised when a type cast operation fails.
    
    This exception is thrown when attempting to cast a value to an
    incompatible type during query processing or data conversion.
    
    Attributes:
        message: Human-readable error message describing the cast failure.
    """

    def __init__(self, message):
        """Initialize the exception with an error message.
        
        Args:
            message: Description of the cast error that occurred.
        """
        self.message = message
        super().__init__(self.message)
