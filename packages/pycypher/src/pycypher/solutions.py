from __future__ import annotations


from abc import ABC
from typing import Optional, Dict, Any, List, Generator
from rich.tree import Tree

class ProjectionTree(ABC):
    
    @property
    def root(self) -> Projection | ProjectionList:
        '''Returns the root of the projection tree.'''
        if hasattr(self, 'parent') and self.parent is not None:
            return self.parent.root
        if self.parent is None:
            return self
        return self.parent.root
    
    def find_variable(self, variable: str) -> Optional[Projection]:
        '''Returns the projection that contains the given variable.'''
        if variable in self:
            return self[variable]
        if hasattr(self, 'parent') and self.parent is not None:
            return self.parent.find_variable(variable)
        return None


class Projection(ProjectionTree):
    '''Basically a dictionary mapping strings to other strings or literals, etc.'''
    def __init__(self, projection: Dict[str, Any], parent: Optional[Projection | ProjectionList] = None):
        self.projection: Dict[str, Any] = projection
        self.parent: Optional[Projection | ProjectionList] = parent
    
    def pythonify(self) -> dict[str, Any]:
        '''Returns a python dictionary representation of the projection.'''
        return {key: value.pythonify() if hasattr(value, 'pythonify') else value for key, value in self.projection.items()}

    def tree(self) -> Tree:
        '''Returns a rich tree representation of the projection.'''
        tree: Tree = Tree(f'Projection[{len(self)}]')
        for key, value in self.projection:
            tree.add(f'{key} = {value}')
        return tree
    
    def keys(self) -> Generator[str, None, None]:
        yield from self.projection.keys()
    
    def __hash__(self) -> int:
        return hash(tuple(sorted(self.projection.items())))
    
    def items(self) -> Generator[tuple[str, Any], None, None]:
        yield from self.projection.items()
    
    def values(self) -> Generator[Any, None, None]:
        yield from self.projection.values()
    
    def __repr__(self) -> str:
        return f'Projection[{self.projection}]'
    
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
        '''Returns True if this projection conflicts with the given projection.'''
        for key in self.projection:
            if key in projection and self.projection[key] != projection[key]:
                return True
        return False
    
    def __eq__(self, other: Projection) -> bool:
        return isinstance(other, Projection) and self.projection == other.projection
    
    def __add__(self, other: Projection) -> Projection:
        '''Returns a new projection that is the union of this projection and the given projection.'''
        if not isinstance(other, Projection):
            raise TypeError(f'Cannot add Projection to {type(other)}')
        elif self.conflicts_with(other):
            raise ValueError(f'Projections conflict: {self} and {other}')
        new_projection: dict[str, Any] = self.projection.copy()
        new_projection.update(other.projection)
        return Projection(projection=new_projection)
    
    def __lt__(self, other: Projection) -> bool:
        '''Returns True if this projection is a subset of the given projection.'''
        if not isinstance(other, Projection):
            raise TypeError(f'Cannot compare Projection to {type(other)}')
        for key in self.projection:
            if key not in other.projection or self.projection[key] != other.projection[key]:
                return False
        return True


class ProjectionList(ProjectionTree):
    '''A list of projections.'''
    def __init__(self, projection_list: List[Projection], parent: Optional[Projection | ProjectionList] = None):
        self.projection_list: List[Projection] = projection_list
        self.parent: Optional[Projection | ProjectionList] = parent

    def pythonify(self) -> list[dict[str, Any]]:
        '''Returns a python list representation of the projection list.'''
        return [projection.pythonify() for projection in self.projection_list] 

    def __iadd__(self, other: ProjectionList | Projection):
        if isinstance(other, ProjectionList):
            self.projection_list.extend(other.projection_list)
        else:
            self.projection_list.append(other)
        return self
    
    def __contains__(self, other: Projection) -> bool:
        if not isinstance(other, Projection):
            raise TypeError(f'Cannot compare ProjectionList to {type(other)}')
        return other in self.projection_list
    
    def tree(self) -> Tree:
        '''Returns a rich tree representation of the projection list.'''
        tree = Tree(f'ProjectionList[{len(self)}]')
        for projection in self.projection_list:
            tree.add(projection.tree())
        return tree
    
    def __repr__(self) -> str:
        return f'ProjectionList[{self.projection_list}]'
    
    def __len__(self) -> int:
        return len(self.projection_list)
    
    def __getitem__(self, index: int) -> Projection:
        return self.projection_list[index]
    
    def __eq__(self, other: ProjectionList) -> bool:
        if not isinstance(other, ProjectionList):
            raise ValueError(f'Cannot compare ProjectionList to {type(other)}')
        left_outer: List[Projection] = [
            projection for projection in self.projection_list if projection not in other.projection_list
        ]
        right_outer: List[Projection] = [
            projection for projection in other.projection_list if projection not in self.projection_list
        ]
        return not left_outer and not right_outer

    
    def is_empty(self) -> bool:
        return len(self) == 0
    
    def unique(self) -> None:
        tmp_list: List[Projection] = []
        for projection in self.projection_list:
            if projection not in tmp_list:
                tmp_list.append(projection)
        self.projection_list = tmp_list
    
    def zip(self, other_list: ProjectionList) -> ProjectionList:
        '''Returns a new projection list that is the union of this projection list and the given projection list.'''
        if not isinstance(other_list, ProjectionList):
            raise TypeError(f'Cannot add ProjectionList to {type(other_list)}')
        new_projection_list: ProjectionList = ProjectionList([])
        for index, projection in enumerate(self.projection_list):
            new_projection_list += projection + other_list.projection_list[index]
        return new_projection_list
