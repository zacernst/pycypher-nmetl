from abc import ABC, abstractmethod


class Shim(ABC):
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
