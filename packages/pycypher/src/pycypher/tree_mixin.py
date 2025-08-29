"""TreeMixin module for Abstract Syntax Tree operations.

This module provides the TreeMixin abstract base class that enables
tree traversal, printing, and navigation operations for AST nodes.
It's designed to be mixed into classes that represent tree structures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generator, Tuple, Type

from rich import print as rprint
from rich.tree import Tree
from shared.logger import LOGGER


class TreeMixin(ABC):
    """Abstract base class providing tree structure operations for AST nodes.

    This mixin class provides common functionality for tree-like data structures,
    particularly Abstract Syntax Trees. It includes methods for tree traversal,
    pretty printing, and parent-child navigation.

    Attributes:
        parent: Reference to the parent node in the tree structure.
    """

    parent = None

    def print_tree(self):
        """Print a visual representation of the tree using Rich formatting.
        
        Uses the Rich library to display a formatted tree structure
        in the console with colors and indentation.
        """
        rprint(self.tree())  # pragma: no cover

    @property
    def children(self) -> Generator[TreeMixin | str | None]:
        """Generate child nodes of this tree node.
        
        This property should be overridden by subclasses to yield
        their actual child nodes.
        
        Yields:
            Child nodes, strings, or None values.
        """
        yield None

    @abstractmethod
    def tree(self) -> Tree:  # pragma: no cover
        """Generate a Rich Tree representation of this node.
        
        This method must be implemented by subclasses to provide
        a visual tree representation suitable for Rich formatting.
        
        Returns:
            Rich Tree object representing this node and its children.
        """

    def get_node_variables(
        self,
    ) -> Generator[Tuple[str, TreeMixin], None, None]:
        """Extract variable names and their corresponding nodes from the tree.
        
        Walks through the tree to find nodes with name_label attributes
        and yields tuples of (variable_name, node).
        
        Yields:
            Tuples of (variable_name, tree_node) for nodes with labels.
        """
        for vertex in self.walk():
            if name_label := (getattr(vertex, "name_label", None) or getattr(vertex, "name_label", None)):
                yield name_label.name, vertex

    def get_vertex_variables(
        self,
    ) -> Generator[Tuple[str, TreeMixin], None, None]:
        """Extract vertex variables specifically from Node objects in the tree.
        
        Walks through the tree to find Node class instances and yields
        tuples of their variable names and node objects.
        
        Yields:
            Tuples of (variable_name, node) for Node class instances.
        """
        for vertex in self.walk():
            if 'Node' == vertex.__class__.__name__:
                yield vertex.name_label.name, vertex
                LOGGER.warning('Got node')  

    def walk(self) -> Generator[TreeMixin]:
        """Perform depth-first traversal of the tree.

        Recursively walks through all child nodes in the tree, yielding each
        node encountered. Handles both individual nodes and lists of nodes.
        Sets parent references during traversal.
        
        Note: This will not work correctly if there are nested lists of nodes,
        but this should not occur in a well-formed AST.
        
        Yields:
            TreeMixin: Each node in the tree in depth-first order.
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
