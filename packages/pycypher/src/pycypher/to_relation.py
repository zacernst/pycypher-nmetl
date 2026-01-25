from __future__ import annotations
from pydantic import Field, BaseModel
import rich
from typing_extensions import Annotated
from pycypher.ast_models import (
    random_hash,
    RelationshipDirection,
    PatternPath,
    Variable,
    RelationshipPattern,
    NodePattern,
    Algebraizable,
)
from typing import Optional, Never, Any, cast
from shared.logger import LOGGER

from enum import Enum

LOGGER.setLevel(level="DEBUG")
ID_COLUMN: str = "__ID__"
RELATIONSHIP_SOURCE_COLUMN: str = "__SOURCE__"
RELATIONSHIP_TARGET_COLUMN: str = "__TARGET__"

EntityType: Annotated[..., ...] = Annotated[str, ...]
RelationshipType: Annotated[..., ...] = Annotated[str, ...]
DisambiguatedColumnName: Annotated[..., ...] = Annotated[str, ...]
ColumnName: Annotated[..., ...] = Annotated[str, ...]
VariableMap: Annotated[..., ...] = Annotated[dict[str, DisambiguatedColumnName], ...]

class DisambiguatedColumnName(BaseModel):
    """A column name that is disambiguated with its source relation."""
    
    relation_identifier: str
    column_name: str

# class Algebraic(BaseModel):
#     """Base class for algebraic structures in the relational model."""
# 
#     source_algebraizable: Optional[Algebraizable] = None


class EntityMapping(BaseModel):
    """Mapping from entity types to the corresponding Table."""

    mapping: dict[EntityType, Any] = {}

    def __getitem__(self, key: EntityType) -> Any:
        return self.mapping[key]


class RelationshipMapping(BaseModel):
    """Mapping from relationship types to the corresponding Table."""

    mapping: dict[RelationshipType, Any] = {}

    def __getitem__(self, key: RelationshipType) -> Any:
        return self.mapping[key]


class Relation(BaseModel):
    """A `Relation` represents a tabular data structure with some metadata."""
    source_algebraizable: Optional[Algebraizable] = None
    variable_map: VariableMap = {}
    column_names: list[str | DisambiguatedColumnName] =  []
    identifier: str = Field(default_factory=lambda: random_hash())

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not hasattr(self, 'column_names') or not self.column_names:
            LOGGER.warning(msg="Relation created without column_names specified.")
        self.column_names: list[DisambiguatedColumnName] = [
            column_name 
            if isinstance(column_name, DisambiguatedColumnName) 
            else DisambiguatedColumnName(
                relation_identifier=self.identifier,
                column_name=column_name
            )
            for column_name in self.column_names]
        
        self.variable_map: VariableMap= {
            var_name: (
                column_name 
                if isinstance(column_name, DisambiguatedColumnName) 
                else DisambiguatedColumnName(
                    relation_identifier=self.identifier,
                    column_name=column_name
                )
            )
            for var_name, column_name in self.variable_map.items()
        }

class EntityTable(Relation):
    """Source of truth for all IDs and attributes for a specific entity type."""
    
    entity_type: EntityType


class RelationshipTable(Relation):
    """Source of truth for all IDs and attributes for a specific relationship type."""

    relationship_type: RelationshipType
    

class Projection(Relation):
    """Selection of specific columns from a Relation.

    To be used in `RETURN` and `WITH` clauses."""

    relation: Relation


class JoinType(Enum):
    """Enumeration of join types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    OUTER = "OUTER"


class Join(Relation):
    """Join represents a join operation between two Relations."""

    join_type: JoinType = JoinType.INNER
    left: Relation
    right: Relation
    on_left: list[ColumnName]
    on_right: list[ColumnName]


class SelectColumns(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    columns: list[ColumnName]


class FilterRows(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    condition: BooleanCondition  # Placeholder for condition expression


class Context(BaseModel):
    """Context for translation operations."""

    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()


class BooleanCondition(BaseModel):
    pass


class Equals(BooleanCondition):
    """Equality condition between two expressions."""

    left: Any
    right: Any


class AttributeEqualsValue(Equals):
    """Condition that an attribute equals a specific value."""

    pass


class Star:
    """Translation operator."""

    def __init__(
        self, context: Context = Context()
    ) -> None:
        self.context: Context = context

    def to_relation(self, obj: Algebraizable) -> Relation:
        """Convert the object to a Relation. Recursively handles different AST node types."""
        LOGGER.debug(msg=f"Starting to_relation conversion for {obj}.")
        match obj:
            case NodePattern(variable=_, labels=_, properties=properties) if (
                len(properties) == 0             # (n:Thing)
            ):
                LOGGER.debug(msg="Translating NodePattern with no properties.")
                out: Projection = self._from_node_pattern(node=obj)
            case NodePattern(variable=_, labels=_, properties=properties) if (
                len(properties) == 1             # (n:Thing {prop1: val1})
            ):
                LOGGER.debug(msg="Translating NodePattern with one property.")
                out: Projection = self._from_node_pattern_one_attr(node=obj)
            case NodePattern(variable=_, labels=_, properties=properties) if (
                len(properties) > 1              # (n:Thing {prop1: val1, prop2: val2}) 
            ):
                LOGGER.debug(
                    msg=f"Translating NodePattern with {len(properties)} properties."
                )
                out: Projection = self._from_node_pattern_multiple_attrs(node=obj)
            case RelationshipPattern(            # -[r:KNOWS]->
                variable=_, types=_, properties=properties
            ) if len(properties) == 0:
                LOGGER.debug(
                    msg="Translating RelationshipPattern with no properties."
                )
                out: Projection = self._from_relationship_pattern(relationship=obj)
            case RelationshipPattern(
                variable=_, types=_, properties=properties
            ) if len(properties) == 1:
                LOGGER.debug(
                    msg="Translating RelationshipPattern with one property."
                )
                raise NotImplementedError('Properties in relationships not yet implemented.')
            case RelationshipPattern(
                variable=_, types=_, properties=properties
            ) if len(properties) > 1:
                LOGGER.debug(
                    msg="Translating RelationshipPattern with multiple properties."
                )
                raise NotImplementedError('Properties in relationships not yet implemented.')
            case PatternPath() as pattern_path:  # (p1)-[r:KNOWS]->(p2)
                LOGGER.debug(msg="Translating PatternPath.")
                out: Relation = self._from_pattern_path(pattern_path=pattern_path)
            
            case _:
                raise NotImplementedError(
                    f"Translation for {type(obj)} is not implemented."
                )
        # Return the constructed Relation
        # But first, record the original `obj` in the Relation for traceability
        out.source_algebraizable: Algebraizable = obj  
        return out
    
    def _from_pattern_path(
        self, pattern_path: PatternPath
    ) -> Relation:
        """Convert a PatternPath to a Relation."""
        # Placeholder implementation
        elements: list[Relation] = [
            self.to_relation(obj=element) for element in pattern_path.elements
        ]
        LOGGER.debug(msg=f"Decomposed PatternPath into {len(elements)} elements.")
        LOGGER.debug(msg=f'Elements: {"\n".join(str(e) for e in elements)}')
        if not elements:
            raise ValueError("PatternPath must have at least two elements.")
        elif len(elements) == 1:
            return elements[0]
        accumulated_value: Relation = self._binary_join(left=elements[0], right=elements[1])
        for element in elements[2:]:
            accumulated_value: Relation = self._binary_join(left=accumulated_value, right=element)
        return accumulated_value
    
    def _binary_join(self, left: Relation, right: Relation) -> Relation:
        """Perform a smart binary join between two Relations, depending on the
        specific types of the Relations."""
        # Relation objects have some way of indicating their join keys,
        # This is in the source_algebraizable attribute
        match (left.source_algebraizable, right.source_algebraizable):
            case (NodePattern(), RelationshipPattern()) if cast(typ=RelationshipPattern, val=right.source_algebraizable).direction == RelationshipDirection.RIGHT:
                LOGGER.debug(msg="Joining Node with Relationship.")
                left_join_key: str = ID_COLUMN
                right_join_key: str = RELATIONSHIP_SOURCE_COLUMN
                join_type: JoinType = JoinType.INNER
            case (NodePattern(), RelationshipPattern()) if cast(typ=RelationshipPattern, val=right.source_algebraizable).direction == RelationshipDirection.LEFT:
                LOGGER.debug(msg="Joining Node with Relationship.")
                left_join_key: str = ID_COLUMN
                right_join_key: str = RELATIONSHIP_TARGET_COLUMN
                join_type: JoinType = JoinType.INNER
            case (RelationshipPattern(), NodePattern()) if cast(typ=RelationshipPattern, val=left.source_algebraizable).direction == RelationshipDirection.RIGHT:
                LOGGER.debug(msg="Joining Relationship with Node.")
                left_join_key: str = RELATIONSHIP_TARGET_COLUMN
                right_join_key: str = ID_COLUMN
                join_type: JoinType = JoinType.INNER
            case (RelationshipPattern(), NodePattern()) if cast(typ=RelationshipPattern, val=left.source_algebraizable).direction == RelationshipDirection.LEFT:
                LOGGER.debug(msg="Joining Relationship with Node.")
                left_join_key: str = RELATIONSHIP_SOURCE_COLUMN
                right_join_key: str = ID_COLUMN
                join_type: JoinType = JoinType.INNER
            case (NodePattern(), NodePattern()):
                # This should never happen
                raise ValueError("Cannot join two NodePatterns directly.")
            case (RelationshipPattern(), RelationshipPattern()):
                # This should never happen
                raise ValueError("Cannot join two RelationshipPatterns directly.")

            case _:
                # This should never happen
                raise NotImplementedError(
                    f"Join for the given Relation types is not implemented: {type(left)} and {type(right)}.")
        # Each Relation object has to track a mapping from variables to columns
        return Join(
            left=left,
            right=right,
            on_left=[left_join_key],
            on_right=[right_join_key],
            join_type=join_type,
        )

    def _from_relationship_pattern(
        self, relationship: RelationshipPattern
    ) -> Projection:
        """Convert a RelationshipPattern to a RelationshipTable."""
        # Placeholder implementation
        out: Projection = Projection(
            relation=self.context.relationship_mapping[
                relationship.types[0]
            ],  # Only first type for now
            column_names=[ID_COLUMN],
        )
        # Attach variable mapping if variable is present
        # Note: We do this only for basis cases without properties
        if relationship.variable:
            out.variable_map[relationship.variable.name] = ID_COLUMN  # disambiguate?
        return out

    def _from_relationship_pattern_one_attr(
        self, relationship: RelationshipPattern
    ) -> Never:
        """Convert a RelationshipPattern with one property to a RelationshipTable with that property."""
        raise NotImplementedError("Not yet implemented.")

    def _from_relationship_pattern_multiple_attrs(
        self, relationship: RelationshipPattern
    ) -> Never:
        """Convert a RelationshipPattern with multiple properties to a RelationshipTable with those properties."""
        raise NotImplementedError("Not yet implemented.")

    def _from_node_pattern(self, node: NodePattern) -> Projection:
        """Convert a NodePattern to an EntityTable."""
        # Placeholder implementation
        out: Projection = Projection(
            relation=self.context.entity_mapping[node.labels[0]],
            column_names=[
                DisambiguatedColumnName(
                    relation_identifier=self.context.entity_mapping[node.labels[0]].identifier,
                    column_name=ID_COLUMN
                    )
                ]
        )
        # Attach variable mapping if variable is present
        # Note: We do this only for basis cases without properties
        if node.variable:
            out.variable_map[node.variable.name] = DisambiguatedColumnName(
                relation_identifier=self.context.entity_mapping[node.labels[0]].identifier,
                column_name=ID_COLUMN
            )
        return out

    def _from_node_pattern_one_attr(self, node: NodePattern) -> Projection:
        """Convert a NodePattern with one property to an EntityTable with that property."""
        # Placeholder implementation
        base_node: NodePattern = NodePattern(
            variable=node.variable, labels=node.labels
        )
        attr1: str = list(node.properties.keys())[0]
        val1: Any = list(node.properties.values())[0]
        out: Projection = Projection(
            relation=Join(
                left=self.to_relation(obj=base_node),
                right=FilterRows(
                    relation=self.context.entity_mapping[node.labels[0]],
                    condition=AttributeEqualsValue(left=attr1, right=val1),
                    column_names=[ID_COLUMN],
                    variable_map={node.variable.name: ID_COLUMN}
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            column_names=[ID_COLUMN],
        )
        return out

    def _from_node_pattern_multiple_attrs(
        self, node: NodePattern
    ) -> Projection:
        """Convert a NodePattern with multiple properties to an EntityTable with those properties."""
        last_attr: str = list(node.properties.keys())[-1]
        last_val: Any = node.properties[last_attr]
        base_node: NodePattern = NodePattern(
            variable=node.variable,
            labels=node.labels,
            properties={
                k: v for k, v in node.properties.items() if k != last_attr
            },
        )
        out: Projection = Projection(
            relation=Join(
                left=self.to_relation(obj=base_node),
                right=FilterRows(
                    relation=self.context.entity_mapping[node.labels[0]],
                    condition=AttributeEqualsValue(left=last_attr, right=last_val),
                    column_names=[ID_COLUMN],
                    variable_map={node.variable.name: ID_COLUMN}
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            column_names=[ID_COLUMN],
        )
        return out


if __name__ == "__main__":
    LOGGER.info(msg="Module loaded successfully.")
            
    entity_table_person = EntityTable(entity_type="Person", column_names=[ID_COLUMN, 'name', 'age'])
    relationship_table_knows = RelationshipTable(relationship_type="KNOWS", column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN])

    node_0 = NodePattern(
        variable=Variable(name="n"),
        labels=["Person"],
        properties={},
    )

    node_1 = NodePattern(
        variable=Variable(name="p1"),
        labels=["Person"],
        properties={"name": "Alice", "age": 30},
    )
    
    node_2 = NodePattern(
        variable=Variable(name="p2"),
        labels=["Person"],
        properties={"name": "Bob", "age": 40},
    )

    relationship: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r"),
        types=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    alice_knows_bob: PatternPath = PatternPath(
        elements=[node_1, relationship, node_2]
    )

    context: Context = Context(
        entity_mapping=EntityMapping(
            mapping={"Person": entity_table_person}
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": relationship_table_knows
            }
        ),
    )
    star: Star = Star(context=context)
    print("Translating NodePattern:")
    print("(p:Person {name: 'Alice', age: 30})")

    translation: Relation = star.to_relation(obj=node_0)
    rich.print(translation)
    import pdb; pdb.set_trace()

    translation: Relation = star.to_relation(obj=node_1)
    rich.print(translation)

    import pdb; pdb.set_trace()

    print("\n---\n")
    print("Translating RelationshipPattern:")
    print("-[r:KNOWS]->")
    translation: Relation = star.to_relation(obj=relationship)
    rich.print(translation)
    import pdb; pdb.set_trace()

    print("\n---\n")
    print("Translating PatternPath:")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person {name: 'Bob', age: 40})")
    translation: Relation = star.to_relation(obj=alice_knows_bob)
    rich.print(translation) 

