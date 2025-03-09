"""Abstract base class"""

from abc import ABC, abstractmethod


class Shim(ABC):
    """
    Shim is an abstract base class that defines the interface for shims.

    Methods
    -------
    __init__(*args, **kwargs)
        Abstract method for initializing the shim.

    __repr__()
        Abstract method for returning the string representation of the shim.

    __str__()
        Abstract method for returning the string representation of the shim.

    make_fact_collection()
        Abstract method for creating a fact collection. This method should be overridden by subclasses.
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
