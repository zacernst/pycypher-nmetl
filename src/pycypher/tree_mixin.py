"""
This is a mixin class that provides methods for walking and printing
the AST.
"""

from __future__ import annotations

from typing import Generator

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
        """Generator that yields every node of the AST."""
        for child in self.children:
            if child is None:
                continue
            yield child
            if not hasattr(child, "walk"):
                continue
            child.parent = self
            for i in child.walk():
                yield i
