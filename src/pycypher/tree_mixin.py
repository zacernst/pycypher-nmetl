"""
This is a mixin class that provides methods for walking and printing
the AST.
"""

from __future__ import annotations

from typing import Generator, Type

from rich import print as rprint
from rich.tree import Tree


class TreeMixin:
    """Mixin class that provides methods for walking and printing the AST."""

    parent = None

    def print_tree(self):
        """Uses ``rich`` to print the tree representation of the AST."""
        rprint(self.tree())

    @property
    def children(self) -> Generator[TreeMixin | str | None]:
        """Each node should have a children property that returns a generator of its children."""
        yield None

    def tree(self) -> Tree:
        """Generates a tree representation of the AST which can be pretty-printed
        with the ``rich`` library.
        """
        t = Tree(self.__class__.__name__)
        for child in self.children:
            if child is None:
                continue
            t.add(child.tree())
        return t

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
        """Returns the root node of the AST."""
        if self.parent is None:
            return self
        return self.parent.root

    @property
    def parse_obj(self):
        """Returns the parse object thst contains the AST and other stuff."""
        if self.parent is None:
            return self
        return self.parent.root

    def enclosing_class(self, cls: Type[TreeMixin]) -> TreeMixin:
        """Returns the first enclosing node of the given class."""
        if isinstance(self, cls):
            return self
        if self.parent is None:
            raise ValueError(f"Could not find enclosing class {cls}")
        return self.parent.enclosing_class(cls)
