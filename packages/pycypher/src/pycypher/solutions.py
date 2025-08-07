from __future__ import annotations


from typing import Dict, Any, List, Generator
from rich.tree import Tree


class Projection:
    '''Basically a dictionary mapping strings to other strings or literals, etc.'''
    def __init__(self, projection: Dict[str, Any]):
        self.projection: Dict[str, Any] = projection
    
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


class ProjectionList:
    '''A list of projections.'''
    def __init__(self, projection_list: List[Projection]):
        self.projection_list: List[Projection] = projection_list
    
    def __iadd__(self, other: ProjectionList | Projection):
        if isinstance(other, ProjectionList):
            self.projection_list.extend(other.projection_list)
        else:
            self.projection_list.append(other)
        return self
    
    def tree(self) -> Tree:
        '''Returns a rich tree representation of the projection list.'''
        tree = Tree(f'ProjectionList[{len(self)}]')
        import pdb; pdb.set_trace()
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
        return isinstance(other, ProjectionList) and self.projection_list == other.projection_list
    
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

