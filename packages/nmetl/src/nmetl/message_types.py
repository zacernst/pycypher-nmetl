"""
Special messages to be put onto queues to mark events.
"""

from abc import ABC
from typing import Any, Optional


class Message(ABC):  # pylint: disable=too-few-public-methods
    """ABC for messages."""

    def __init__(self):
        """
        Initialize a Message instance.
        """
        pass

    def __repr__(self):
        """
        Return a string representation of the Message instance.

        Returns:
            str: The name of the class.
        """
        return self.__class__.__name__


class EndOfData(Message):  # pylint: disable=too-few-public-methods
    """The data source is exhausted."""

    def __init__(self, data_source: Optional["DataSource"] = None):
        """
        Initialize an EndOfData message.

        Args:
            data_source (Optional[DataSource]): The data source that has been exhausted. Defaults to None.
        """
        self.data_source = data_source


class DataSourcesExhausted(Message):  # pylint: disable=too-few-public-methods
    """All data sources are exhausted."""

    def __init__(self):
        """
        Initialize a DataSourcesExhausted message.
        """
        pass


class RawDatum(Message):  # pylint: disable=too-few-public-methods
    """This is a bit of data directly from a data source."""

    def __init__(
        self,
        mappings: Optional[list["DataSourceMapping"]] = None,
        row: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize a RawDatum message.

        Args:
            data_source (Optional[DataSource]): The data source that produced this datum. Defaults to None.
            row (Optional[dict[str, Any]]): The row of data. Defaults to None.
        """
        self.row = row
        self.mappings = mappings

    def __repr__(self):
        """
        Return a string representation of the RawDatum instance.

        Returns:
            str: A string representation in the format "RawDatum(row)".
        """
        return f"{self.__class__.__name__}({self.row})"


class ComputationRequest(Message):  # pylint: disable=too-few-public-methods
    """We might not need this after all."""
