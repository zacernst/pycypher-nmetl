"""
This is a mixin class that provides methods for walking and printing
the AST.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generator, Tuple, Type

from rich import print as rprint
from rich.tree import Tree
from shared.logger import LOGGER


class TreeMixin(ABC):
    """
    TreeMixin is an abstract base class that provides a mixin for tree structures,
    typically used for Abstract Syntax Trees (ASTs). It includes methods for
    printing, walking, and navigating the tree.

    Attributes:
        parent: The parent node of the current node.

    """

    parent = None

    def print_tree(self):
        """Uses ``rich`` to print the tree representation of the AST."""
        rprint(self.tree())  # pragma: no cover

    @property
    def children(self) -> Generator[TreeMixin | str | None]:
        """Each node should have a children property that returns a generator of its children."""
        yield None

    @abstractmethod
    def tree(self) -> Tree:  # pragma: no cover
        """Generates a tree representation of the AST which can be pretty-printed
        with the ``rich`` library.
        """

    def get_node_variables(
        self,
    ) -> Generator[Tuple[str, TreeMixin], None, None]:
        for vertex in self.walk():
            if name_label := (getattr(vertex, "name_label", None) or getattr(vertex, "name_label", None)):
                yield name_label.name, vertex

    def get_vertex_variables(
        self,
    ) -> Generator[Tuple[str, TreeMixin], None, None]:
        for vertex in self.walk():
            if 'Node' == vertex.__class__.__name__:
                yield vertex.name_label.name, vertex
                LOGGER.warning('Got node')  

    def walk(self) -> Generator[TreeMixin]:
        """Generator that yields every node of the AST.

        Note that this will **not** work if there is a list directly inside another list.
        But that shouldn't happen in an AST anyway.
        """
        for child in self.children:
            if child is None:
                continue
            yield child
            if isinstance(child, list):
                for c in child:
                    if not hasattr(c, "walk"):
                        continue
                    c.parent = self
                    yield from c.walk()
                continue
            if not hasattr(child, "walk"):
                continue
            child.parent = self
            yield from child.walk()

    @property
    def root(self):
        """
        Returns the root node of the tree.

        This method traverses up the tree by following the parent references
        until it finds the root node (a node with no parent).

        Returns:
            The root node of the tree.
        """
        if self.parent is None:
            return self
        return self.parent.root

    @property
    def parse_obj(self):
        """
        Returns the root object of the tree.

        If the current object has no parent, it is considered the root and is returned.
        Otherwise, the method recursively traverses up the tree to find and return the root object.

        Returns:
            The root object of the tree.
        """
        if self.parent is None:
            return self
        return self.parent.root

    def enclosing_class(self, cls: Type[TreeMixin]) -> TreeMixin:
        """
        Finds the nearest enclosing instance of the specified class type.

        This method traverses up the parent hierarchy to find the nearest
        instance of the specified class type. If the current instance is
        of the specified class type, it returns itself. If no such instance
        is found in the hierarchy, a ValueError is raised.

        Args:
            cls (Type[TreeMixin]): The class type to search for in the parent hierarchy.

        Returns:
            TreeMixin: The nearest enclosing instance of the specified class type.

        Raises:
            ValueError: If no enclosing instance of the specified class type is found.
        """
        if isinstance(self, cls):
            return self
        if self.parent is None:
            raise ValueError(f"Could not find enclosing class {cls}")
        return self.parent.enclosing_class(cls)
