from __future__ import annotations

from abc import ABC
from typing import Any, Dict, Generator, List, Optional

from rich.tree import Tree


class ProjectionTree(ABC):
    """Abstract base class for projection tree structures.

    Provides common functionality for managing hierarchical projection relationships.

    Attributes:
        children: List of child ProjectionTree nodes.
        parent: Optional parent ProjectionTree node.
    """

    def __init__(self, parent: Optional[Projection | ProjectionList] = None):
        """Initialize a ProjectionTree node.

        Args:
            parent: Optional parent node in the projection tree.
        """
        self.children: List[ProjectionTree] = []
        self.parent: Optional[Projection | ProjectionList] = parent
        if parent:
            self.parent.children.append(self)

    @property
    def root(self) -> Projection | ProjectionList:
        """Get the root node of the projection tree.

        Returns:
            The root Projection or ProjectionList node.
        """
        if hasattr(self, "parent") and self.parent is not None:
            return self.parent.root
        if self.parent is None:
            return self
        return self.parent.root


class Projection(ProjectionTree):
    """A dictionary-like mapping of variable names to values.

    Represents a single row of query results, mapping variable names
    to their corresponding values in the current context.

    Attributes:
        projection: Dictionary mapping variable names to values.
    """

    def __init__(
        self,
        projection: Dict[str, Any],
        parent: Optional[Projection | ProjectionList] = None,
    ):
        """Initialize a Projection with variable mappings.

        Args:
            projection: Dictionary mapping variable names to values.
            parent: Optional parent node in the projection tree.
        """
        self.projection: Dict[str, Any] = projection
        super().__init__(parent=parent)

    def __bool__(self) -> bool:
        return not self.is_empty()

    def pythonify(self) -> dict[str, Any]:
        """Convert projection to a plain Python dictionary.

        Returns:
            Dictionary with pythonified values where applicable.
        """
        return {
            key: value.pythonify() if hasattr(value, "pythonify") else value
            for key, value in self.projection.items()
        }

    def tree(self) -> Tree:
        """Create a visual tree representation of the projection.

        Returns:
            Rich Tree object for display purposes.
        """
        tree: Tree = Tree(f"Projection[{len(self)}]")
        for key, value in self.projection:
            tree.add(f"{key} = {value}")
        return tree

    def keys(self) -> Generator[str, None, None]:
        """Generate all variable names in this projection.

        Yields:
            Variable names as strings.
        """
        yield from self.projection.keys()

    def __hash__(self) -> int:
        """Return hash value based on projection contents.

        Returns:
            Hash value for this projection.
        """
        return hash(tuple(sorted(self.projection.items())))

    def items(self) -> Generator[tuple[str, Any], None, None]:
        """Generate all key-value pairs in this projection.

        Yields:
            Tuples of (variable_name, value).
        """
        yield from self.projection.items()

    def values(self) -> Generator[Any, None, None]:
        """Generate all values in this projection.

        Yields:
            Values from the projection dictionary.
        """
        yield from self.projection.values()

    def subset(self, aliases: list[str]) -> Projection:
        return Projection(
            projection={alias: self.projection[alias] for alias in aliases}
        )

    def __repr__(self) -> str:
        return f"Projection[{self.projection}]"

    def __getitem__(self, key: str) -> Any:
        return self.projection[key]

    def __contains__(self, key: str) -> bool:
        return key in self.projection

    def __setitem__(self, key: str, value: Any) -> None:
        self.projection[key] = value

    def __len__(self) -> int:
        return len(self.projection)

    def update(self, other: Projection) -> None:
        self.projection.update(other.projection)

    def is_empty(self) -> bool:
        return len(self) == 0

    def conflicts_with(self, projection: Projection) -> bool:
        """Returns True if this projection conflicts with the given projection."""
        for key in self.projection:
            if key in projection and self.projection[key] != projection[key]:
                return True
        return False

    def __eq__(self, other: Projection) -> bool:
        return (
            isinstance(other, Projection)
            and self.projection == other.projection
        )

    def __add__(self, other: Projection) -> Projection:
        """Returns a new projection that is the union of this projection and the given projection."""
        if not isinstance(other, Projection):
            raise TypeError(f"Cannot add Projection to {type(other)}")
        elif self.conflicts_with(other):
            raise ValueError(f"Projections conflict: {self} and {other}")
        new_projection: dict[str, Any] = self.projection.copy()
        new_projection.update(other.projection)
        return Projection(projection=new_projection)

    def __lt__(self, other: Projection) -> bool:
        """Returns True if this projection is a subset of the given projection."""
        if not isinstance(other, Projection):
            raise TypeError(f"Cannot compare Projection to {type(other)}")
        for key in self.projection:
            if (
                key not in other.projection
                or self.projection[key] != other.projection[key]
            ):
                return False
        else:
            return True

    def find_variable(self, variable: str) -> Any:
        if variable in self.projection:
            yield self.projection[variable]
        elif self.parent is None:
            yield None
        elif isinstance(self.parent, (Projection, ProjectionList)):
            for i in self.parent.find_variable(variable):
                yield i
        else:
            raise ValueError("This should never happen")

    def __lt__(self, other: Projection) -> bool:
        if not isinstance(other, Projection):
            raise ValueError(
                f"Cannot compare Projection to non-Projection {other}"
            )
        # self is the subset
        # self < other
        for key in self.projection:
            if (
                key not in other.projection
                or self.projection[key] != other.projection[key]
            ):
                return False
        return True


class ProjectionList(ProjectionTree):
    """A list of projections."""

    def __init__(
        self,
        projection_list: List[Projection],
        parent: Optional[Projection | ProjectionList] = None,
    ):
        self.projection_list: List[Projection] = projection_list
        super().__init__(parent=parent)

    def find_variable(self, variable: str) -> Any:
        """Returns the value of the variable in the projection list."""
        for projection in self.projection_list:
            for i in projection.find_variable(variable):
                yield i
        if self.parent is None:
            yield None
        else:
            for i in self.parent.find_variable(variable):
                yield i

    def append(self, projection: Projection) -> None:
        self.projection_list.append(projection)

    def pythonify(self) -> list[dict[str, Any]]:
        """Returns a python list representation of the projection list."""
        return [projection.pythonify() for projection in self.projection_list]

    def __iadd__(self, other: ProjectionList | Projection):
        if isinstance(other, ProjectionList):
            self.projection_list.extend(other.projection_list)
        else:
            self.projection_list.append(other)
        return self

    def __contains__(self, other: Projection) -> bool:
        if not isinstance(other, Projection):
            raise TypeError(f"Cannot compare ProjectionList to {type(other)}")
        return other in self.projection_list

    def tree(self) -> Tree:
        """Returns a rich tree representation of the projection list."""
        tree = Tree(f"ProjectionList[{len(self)}]")
        for projection in self.projection_list:
            tree.add(projection.tree())
        return tree

    def __repr__(self) -> str:
        return f"ProjectionList[{self.projection_list}]"

    def __len__(self) -> int:
        return len(self.projection_list)

    def __getitem__(self, index: int) -> Projection:
        return self.projection_list[index]

    def __eq__(self, other: ProjectionList) -> bool:
        if not isinstance(other, ProjectionList):
            raise ValueError(f"Cannot compare ProjectionList to {type(other)}")
        left_outer: List[Projection] = [
            projection
            for projection in self.projection_list
            if projection not in other.projection_list
        ]
        right_outer: List[Projection] = [
            projection
            for projection in other.projection_list
            if projection not in self.projection_list
        ]
        return not left_outer and not right_outer

    def __bool__(self) -> bool:
        return not self.is_empty()

    def is_empty(self) -> bool:
        return len(self) == 0

    def unique(self) -> None:
        tmp_list: List[Projection] = []
        for projection in self.projection_list:
            if projection not in tmp_list:
                tmp_list.append(projection)
        self.projection_list = tmp_list

    def zip(self, other_list: ProjectionList) -> ProjectionList:
        """Returns a new projection list that is the union of this projection list and the given projection list."""
        if not isinstance(other_list, ProjectionList):
            raise TypeError(f"Cannot add ProjectionList to {type(other_list)}")
        new_projection_list: ProjectionList = ProjectionList([])
        for index, projection in enumerate(self.projection_list):
            new_projection_list += (
                projection + other_list.projection_list[index]
            )
        return new_projection_list
