from docutils.transforms.components import Filter
from pydantic import BaseModel
import rich
from typing_extensions import Annotated
from pycypher.ast_models import Variable, NodePattern, Algebraizable
import copy
from typing import Literal, Any, Optional
from shared.logger import LOGGER

from enum import Enum

LOGGER.setLevel('DEBUG')
ID_COLUMN: str = "__ID__"

EntityType: Annotated[..., ...] = Annotated[str, ...]
RelationshipType: Annotated[..., ...] = Annotated[str, ...]
HashedColumn: Annotated[..., ...] = Annotated[str, ...]
ColumnName: Annotated[..., ...] = Annotated[str, ...]
ColumnHashMap: Annotated[..., ...] = Annotated[dict[str, str], ...]

class Algebraic(BaseModel):
    '''Base class for algebraic structures in the relational model.'''
    pass


class EntityMapping(BaseModel):
    '''Mapping from entity types to the corresponding Table.'''
    mapping: dict[EntityType, Any] = {}

    def __getitem__(self, key: EntityType) -> Any:
        return self.mapping[key]


class RelationshipMapping(BaseModel):
    '''Mapping from relationship types to the corresponding Table.'''
    mapping: dict[RelationshipType, Any] = {}

    def __getitem__(self, key: RelationshipType) -> Any:
        return self.mapping[key]


class Relation(Algebraic):
    '''A `Relation` represents a tabular data structure with some metadata.'''
    pass


class EntityTable(Relation):
    '''Source of truth for all IDs and a

    ttributes for a specific entity type.'''
    entity_type: EntityType


class RelationshipTable(Relation):
    '''Source of truth for all IDs and attributes for a specific relationship type.'''
    relationship_type: RelationshipType


class Projection(Relation):
    '''Selection of specific columns from a Relation.
    
    To be used in `RETURN` and `WITH` clauses.'''
    relation: Relation
    columns: list[ColumnName]


#


class JoinType(Enum):
    '''Enumeration of join types.'''
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL" 
    OUTER = "OUTER"


class Join(Relation):
    '''Join represents a join operation between two Relations.'''
    join_type: JoinType = JoinType.INNER
    left: Relation
    right: Relation
    on_left: list[ColumnName]
    on_right: list[ColumnName]


class SelectColumns(Relation):
    '''Filter represents a filtering operation on a Relation.'''
    relation: Relation
    columns: list[ColumnName]


class FilterRows(Relation):
    '''Filter represents a filtering operation on a Relation.'''
    relation: Relation
    condition: BooleanCondition  # Placeholder for condition expression


class Context(BaseModel):
    '''Context for translation operations.'''
    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()
    column_hash_map: ColumnHashMap = {}


class BooleanCondition(BaseModel):
    pass


class Equals(BooleanCondition):
    '''Equality condition between two expressions.'''
    left: Any
    right: Any


class AttributeEqualsValue(Equals):
    '''Condition that an attribute equals a specific value.'''
    pass


class Star:
    '''Translation operator'''

    def __init__(self, obj: Algebraizable | Star, context: Context = Context()) -> None:
        self.obj: Algebraizable | Star = obj
        self.context: Context = context
    
    def to_relation(self) -> Relation:
        '''Convert the object to a Relation.'''
        LOGGER.debug(msg=f"Starting to_relation conversion for {self.obj}.")
        match self.obj:
            case NodePattern(variable=_, labels=_, properties=properties) if len(properties) == 0:  # ty:ignore[unresolved-reference]
                LOGGER.debug(msg="Translating NodePattern with no properties.")
                return self._from_node_pattern(node=self.obj)
            case NodePattern(variable=_, labels=_, properties=properties) if len(properties) == 1:  # ty:ignore[unresolved-reference]
                LOGGER.debug(msg="Translating NodePattern with one property.")
                return self._from_node_pattern_one_attr(node=self.obj)
            case NodePattern(variable=_, labels=_, properties=properties) if len(properties) > 1:  # ty:ignore[unresolved-reference]
                LOGGER.debug(msg=f"Translating NodePattern with {len(properties)} properties.")
                return self._from_node_pattern_multiple_attrs(node=self.obj)
            case _:
                raise NotImplementedError(f"Translation for {type(self.obj)} is not implemented.")
    
    def _from_node_pattern(self, node: NodePattern) -> Projection:
        '''Convert a NodePattern to an EntityTable.'''
        # Placeholder implementation
        out: Projection = Projection(
            relation=self.context.entity_mapping[node.labels[0]],
            columns=[ID_COLUMN]
        )
        return out
    
    def _from_node_pattern_one_attr(self, node: NodePattern) -> Projection:
        '''Convert a NodePattern with one property to an EntityTable with that property.'''
        # Placeholder implementation
        base_node: NodePattern = NodePattern(variable=node.variable, labels=node.labels) 
        attr1: str = list(node.properties.keys())[0]
        val1: Any = list(node.properties.values())[0]
        out: Projection = Projection(
            relation=Join(
                left=Star(obj=base_node, context=self.context).to_relation(),
                right=FilterRows(
                    relation=self.context.entity_mapping[node.labels[0]],
                    condition=AttributeEqualsValue(left=attr1, right=val1),
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            columns=[ID_COLUMN],
        )
        return out
    
    def _from_node_pattern_multiple_attrs(self, node: NodePattern) -> Projection:
        '''Convert a NodePattern with multiple properties to an EntityTable with those properties.'''
        last_attr: str = list(node.properties.keys())[-1]
        last_val: Any = node.properties[last_attr]
        base_node: NodePattern = NodePattern(
            variable=node.variable, 
            labels=node.labels,
            properties={
                k: v for k, v in node.properties.items() if k != last_attr
            }
        )
        out: Projection = Projection(
            relation=Join(
                left=Star(obj=base_node, context=self.context).to_relation(),
                right=FilterRows(
                    relation=self.context.entity_mapping[node.labels[0]],
                    condition=AttributeEqualsValue(left=last_attr, right=last_val),
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            columns=[ID_COLUMN],
        )
        return out


if __name__ == "__main__":
    LOGGER.info(msg="Module loaded successfully.")

    node = NodePattern(
        variable=Variable(name="p"),
        labels=['Person'],
        properties={'name': 'Alice', 'age': 30}
    )

    context: Context = Context(
        entity_mapping=EntityMapping(
            mapping={
                'Person': EntityTable(entity_type='Person')
            }
        ),
        relationship_mapping=RelationshipMapping(
            mapping={} 
        ),
        column_hash_map={},


    )
    star: Star = Star(obj=node, context=context)
    translation: Relation = star.to_relation()
    rich.print(translation)
