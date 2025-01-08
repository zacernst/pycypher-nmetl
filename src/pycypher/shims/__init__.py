"""Abstract base class"""

from abc import ABC, abstractmethod


class Shim(ABC):
    """Abstract base class for interfaces between data structures and
    the Cypher query language.
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def make_fact_collection(self):
        """Overridden"""
