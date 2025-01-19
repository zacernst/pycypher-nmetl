"""
Special messages to be put onto queues to mark events.
"""

from abc import ABC
from typing import Any, Optional


class Message(ABC):  # pylint: disable=too-few-public-methods
    """ABC for messages."""

    def __init__(self):
        pass

    def __repr__(self):
        return self.__class__.__name__


class EndOfData(Message):  # pylint: disable=too-few-public-methods
    """The data source is exhausted."""

    def __init__(self, data_source: Optional["DataSource"] = None):
        self.data_source = data_source


class DataSourcesExhausted(Message):  # pylint: disable=too-few-public-methods
    """All data sources are exhausted."""

    def __init__(self):
        pass


class RawDatum(Message):  # pylint: disable=too-few-public-methods
    """This is a bit of data directly from a data source."""

    def __init__(
        self,
        data_source: Optional["DataSource"] = None,
        row: Optional[dict[str, Any]] = None,
    ):
        self.row = row
        self.data_source = data_source

    def __repr__(self):
        return f"{self.__class__.__name__}({self.row})"


class ComputationRequest(Message):  # pylint: disable=too-few-public-methods
    """We might not need this after all."""
