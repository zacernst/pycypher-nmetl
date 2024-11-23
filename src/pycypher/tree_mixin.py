from __future__ import annotations

from typing import Generator

from rich import print as rprint
from rich.tree import Tree


class TreeMixin:
    parent = None

    def print_tree(self):
        rprint(self.tree())

    @property
    def children(self) -> Generator[TreeMixin | str | None]:
        yield None

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for child in self.children:
            if child is None:
                continue
            t.add(child.tree())
        return t

    def walk(self) -> Generator[TreeMixin]:
        for child in self.children:
            if child is None:
                continue
            yield child
            if not hasattr(child, "walk"):
                continue
            child.parent = self
            for i in child.walk():
                yield i
