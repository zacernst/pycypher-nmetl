"""
Data Types Module
===============

This module defines data types used for type casting in the ETL pipeline.
It provides a set of classes for converting values to specific types.
"""

from typing import Any


class DataType:
    """
    Base class for data type casting.

    This class defines the interface for data type casting classes.
    Subclasses should implement the `cast` method.
    """
    def __call__(self, value: Any):
        """
        Call the cast method on the value.

        Args:
            value (Any): The value to cast.

        Returns:
            The cast value.
        """
        return self.cast(value)


class _Anything(DataType):
    """
    Data type that returns the value unchanged.
    """
    def cast(self, value: Any):
        """
        Return the value unchanged.

        Args:
            value (Any): The value to cast.

        Returns:
            Any: The original value unchanged.
        """
        return value


class _Integer(DataType):
    """
    Data type that casts values to integers.
    """
    def cast(self, value: Any):
        """
        Cast the value to an integer.

        Args:
            value (Any): The value to cast.

        Returns:
            int: The value cast to an integer.

        Raises:
            ValueError: If the value cannot be cast to an integer.
        """
        return int(value)


class _PositiveInteger(DataType):
    """
    Data type that casts values to positive integers.
    """
    def cast(self, value: Any):
        """
        Cast the value to a positive integer.

        Args:
            value (Any): The value to cast.

        Returns:
            int: The absolute value cast to an integer.

        Raises:
            ValueError: If the value cannot be cast to a float or integer.
        """
        return abs(int(float(value)))


class _String(DataType):
    """
    Data type that casts values to strings.
    """
    def cast(self, value: Any):
        """
        Cast the value to a string.

        Args:
            value (Any): The value to cast.

        Returns:
            str: The value cast to a string.
        """
        return str(value)


class _Float(DataType):
    """
    Data type that casts values to floats.
    """
    def cast(self, value: Any):
        """
        Cast the value to a float.

        Args:
            value (Any): The value to cast.

        Returns:
            float: The value cast to a float.

        Raises:
            ValueError: If the value cannot be cast to a float.
        """
        return float(value)


class _Boolean(DataType):
    """
    Data type that casts values to booleans.
    """
    def cast(self, value: Any):
        """
        Cast the value to a boolean.

        Args:
            value (Any): The value to cast.

        Returns:
            bool: The value cast to a boolean.
        """
        return bool(value)


Anything = _Anything()
Integer = _Integer()
PositiveInteger = _PositiveInteger()
String = _String()
Float = _Float()
Boolean = _Boolean()
