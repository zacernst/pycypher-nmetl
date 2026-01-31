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
import pandas as pd
from typing import Optional, Never, Any, cast
from shared.logger import LOGGER

from enum import Enum

LOGGER.setLevel(level="DEBUG")
ID_COLUMN: str = "__ID__"
RELATIONSHIP_SOURCE_COLUMN: str = "__SOURCE__"
RELATIONSHIP_TARGET_COLUMN: str = "__TARGET__"



class DisambiguatedColumnName(BaseModel):
    """A column name that is disambiguated with its source relation."""
    
    relation_identifier: str
    column_name: str

    def __str__(self) -> str:
        return f"{self.relation_identifier}::{self.column_name}"

EntityType = Annotated[str, ...]
RelationshipType = Annotated[str, ...]
ColumnName: Annotated[..., ...] = Annotated[str, ...]
VariableMap = Annotated[dict[Variable, DisambiguatedColumnName], ...]

# class Algebraic(BaseModel):
#     """Base class for algebraic structures in the relational model."""
# 
#     source_algebraizable: Optional[Algebraizable] = None


class BooleanCondition(BaseModel):
    pass


class Equals(BooleanCondition):
    """Equality condition between two expressions."""

    left: Any
    right: Any


class AttributeEqualsValue(Equals):
    """Condition that an attribute equals a specific value."""

    pass


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
        
        self.variable_map: VariableMap = {
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
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Relation to a pandas DataFrame."""
        raise NotImplementedError("to_pandas() not implemented for base Relation class.")

class EntityTable(Relation):
    """Source of truth for all IDs and attributes for a specific entity type."""
    
    entity_type: EntityType
    source_obj: Any = Field(default=None, repr=False)

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the EntityTable to a pandas DataFrame."""
        disambiguate_columns_mapping: dict[str, str] = {
            cast(typ=DisambiguatedColumnName, val=disambiguated_col).column_name: str(object=disambiguated_col) for disambiguated_col in self.column_names
        }
        new_df: pd.DataFrame = self.source_obj.rename(columns=disambiguate_columns_mapping)
        return new_df


class RelationshipTable(Relation):
    """Source of truth for all IDs and attributes for a specific relationship type."""

    relationship_type: RelationshipType
    source_obj: Any = Field(default=None, repr=False)
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the EntityTable to a pandas DataFrame."""
        disambiguate_columns_mapping: dict[str, str] = {
            cast(typ=DisambiguatedColumnName, val=disambiguated_col).column_name: str(object=disambiguated_col) for disambiguated_col in self.column_names
        }
        new_df: pd.DataFrame = self.source_obj.rename(columns=disambiguate_columns_mapping)
        return new_df
    

class Projection(Relation):
    """Selection of specific columns from a Relation.

    To be used in `RETURN` and `WITH` clauses."""

    relation: Relation

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Projection to a pandas DataFrame."""
        base_df: pd.DataFrame = self.relation.to_pandas(context=context)
        # column_renaming_map: dict[str, str] = {
        #     str(object=base_column_name): str(
        #         object=DisambiguatedColumnName(
        #             relation_identifier=self.identifier,
        #             column_name=cast(typ=DisambiguatedColumnName, val=base_column_name).column_name
        #         ) 
        #     ) for base_column_name in self.relation.column_names}
        column_renaming_map: dict[str, str] = {
            str(object=base_column_name): str(
                object=DisambiguatedColumnName(
                    relation_identifier=self.identifier, 
                    column_name=cast(typ=DisambiguatedColumnName, val=base_column_name).column_name
                )
            ) for base_column_name in self.relation.column_names}
        projected_df: pd.DataFrame = base_df.rename(columns=column_renaming_map)[
            [str(object=column_name) for column_name in self.column_names]
        ]
        return projected_df


class JoinType(Enum):
    """Enumeration of join types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"    
    FULL = "FULL"


class Join(Relation):
    """Join represents a join operation between two Relations."""

    join_type: JoinType = JoinType.INNER
    left: Relation
    right: Relation
    on_left: list[ColumnName]
    on_right: list[ColumnName]

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Join to a pandas DataFrame."""
        left_df: pd.DataFrame = self.left.to_pandas(context=context)
        right_df: pd.DataFrame = self.right.to_pandas(context=context)
        join_type_str: str = self.join_type.value.lower()
        joined_df: pd.DataFrame = pd.merge(
            left=left_df,
            right=right_df,
            how=join_type_str,
            left_on=[str(object=col) for col in self.on_left],
            right_on=[str(object=col) for col in self.on_right],
            suffixes=(
                f"_{self.left.identifier}",
                f"_{self.right.identifier}",
            ),
        )
        return joined_df


class SelectColumns(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    columns: list[ColumnName]


class FilterRows(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    condition: BooleanCondition  # Placeholder for condition expression

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.column_names: list[str | DisambiguatedColumnName] = self.column_names or self.relation.column_names
        # self.variable_map = self.relation.variable_map

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the FilterRows to a pandas DataFrame."""
        # Placeholder implementation
        df: pd.DataFrame = self.relation.to_pandas(context=context)
        # Apply filtering based on condition (not implemented)
        match self.condition:
            case AttributeEqualsValue():
                filtered_df: pd.DataFrame = df[
                    df[
                        str(object=DisambiguatedColumnName(
                            relation_identifier=self.relation.identifier,
                            column_name=self.condition.left)
                        )
                    ] == self.condition.right
                ]
                column_renaming_map: dict[str, str] = {
                    str(object=base_column_name): str(
                        object=DisambiguatedColumnName(
                            relation_identifier=self.identifier, 
                            column_name=cast(typ=DisambiguatedColumnName, 
                            val=base_column_name).column_name
                        )
                    ) for base_column_name in self.relation.column_names}
                projected_df: pd.DataFrame = filtered_df.rename(columns=column_renaming_map)
            case _:
                raise NotImplementedError("Condition type not implemented in to_pandas.")
        return filtered_df


class Context(BaseModel):
    """Context for translation operations."""

    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()


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
            raise ValueError("PatternPath must have at least one element.")
        elif len(elements) == 1:
            return elements[0]
        accumulated_value: Relation = self._binary_join(left=elements[0], right=elements[1])
        import pdb; pdb.set_trace()
        for element in elements[2:]:
            accumulated_value: Relation = self._binary_join(left=accumulated_value, right=element)
        return accumulated_value
    
    def _binary_join(self, left: Relation, right: Relation) -> Relation:
        """Perform a smart binary join between two Relations, depending on the
        specific types of the Relations."""

        # TODO: Add variable mapping assignments and columns with correct
        #       disambiguation logic and renaming if necessary here.
        match (left.source_algebraizable, right.source_algebraizable):
            case (NodePattern(), RelationshipPattern()) if cast(typ=RelationshipPattern, val=right.source_algebraizable).direction == RelationshipDirection.RIGHT:
                # Case (1)
                # Get the RelationshipTail: for example: (n)-[r:KNOWS]
                # Identify the variable attached to the node
                # Identify the column corresponding to that variable in the node table
                # Identify the relation type and relationship source column
                # Inner join on relationship source column to node ID column
                # Keep the column from the node; drop the column from the relationship source
                # Drop the target variable column from the relationship table
                # Update variable mapping to include variable columns from node
                #     and relationship ID column from relationship tablelea
                LOGGER.debug(msg="Joining Node with Relationship for (n)-[r:RELATIONSHIP] (tail)`.")
                node_variable: Variable = cast(typ=NodePattern, val=left.source_algebraizable).variable
                node_variable_column: DisambiguatedColumnName = left.variable_map[node_variable]
                relationship_variable: Variable = cast(typ=RelationshipPattern, val=right.source_algebraizable).variable
                relationship_variable_column: DisambiguatedColumnName = right.variable_map[relationship_variable]
                left_join_key: DisambiguatedColumnName = node_variable_column
                right_join_key: DisambiguatedColumnName = relationship_variable_column
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
                import pdb; pdb.set_trace()
            case (NodePattern(), RelationshipPattern()) if cast(typ=RelationshipPattern, val=right.source_algebraizable).direction == RelationshipDirection.LEFT:
                LOGGER.debug(msg="Joining Node with Relationship for (n)-[r:RELATIONSHIP] (tail)`.")
                left_join_key: str = ID_COLUMN
                right_join_key: str = RELATIONSHIP_TARGET_COLUMN
                join_type: JoinType = JoinType.INNER
            case (RelationshipPattern(), NodePattern()) if cast(typ=RelationshipPattern, val=left.source_algebraizable).direction == RelationshipDirection.RIGHT:
                
                LOGGER.debug(msg="Joining Relationship with Node")


                left_join_key: str = RELATIONSHIP_TARGET_COLUMN
                right_join_key: str = ID_COLUMN
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
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
                LOGGER.debug(msg="Joining two complex Relations (Projection/Join).")
                left_join_key: str = ID_COLUMN
                right_join_key: str = ID_COLUMN
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
                # This should never happen
                # import pdb; pdb.set_trace()
                # raise NotImplementedError(
                #     f"Join for the given Relation types is not implemented: {type(left)} and {type(right)}.")
        # Each Relation object has to track a mapping from variables to columns
        out: Join = Join(
            left=left,
            right=right,
            on_left=[left_join_key],
            on_right=[right_join_key],
            join_type=join_type,
            variable_map=variable_map,
        )
        return out

    def _from_relationship_pattern(
        self, relationship: RelationshipPattern
    ) -> RelationshipTable:
        """Convert a RelationshipPattern to a RelationshipTable."""
        # Break into two cases: RelationshipTail and RelationshipHead

        # Case (1)
        # Get the RelationshipTail: for example: (n)-[r:KNOWS]
        # Identify the variable attached to the node
        # Identify the column corresponding to that variable in the node table
        # Identify the relation type and relationship source column
        # Inner join on relationship source column to node ID column
        # Keep the column from the node; drop the column from the relationship source
        # Drop the target variable column from the relationship table
        # Update variable mapping to include variable columns from node
        #     and relationship ID column from relationship table

        relationship_table: RelationshipTable = self.context.relationship_mapping[relationship.types[0]]
        relationship_table.variable_map: dict[Variable, DisambiguatedColumnName] = {
            relationship.variable: DisambiguatedColumnName(relation_identifier=relationship_table.identifier, column_name=ID_COLUMN)
        } if relationship.variable else {}
        return relationship_table

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
                ],
            variable_map={node.variable: DisambiguatedColumnName(relation_identifier=self.context.entity_mapping[node.labels[0]].identifier, column_name=ID_COLUMN)} if node.variable else {}
        )
        # Attach variable mapping if variable is present
        # Note: We do this only for basis cases without properties
        return out

    def _from_node_pattern_one_attr(self, node: NodePattern) -> Projection:
        """Convert a NodePattern with one property to an EntityTable with that property."""
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
                    variable_map={node.variable: DisambiguatedColumnName(relation_identifier=self.context.entity_mapping[node.labels[0]].identifier, column_name=ID_COLUMN)} if node.variable else {}
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            column_names=[ID_COLUMN],
            variable_map={node.variable: DisambiguatedColumnName(relation_identifier=self.context.entity_mapping[node.labels[0]].identifier, column_name=ID_COLUMN)} if node.variable else {}
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
                    variable_map={node.variable: DisambiguatedColumnName(relation_identifier=self.context.entity_mapping[node.labels[0]].identifier, column_name=ID_COLUMN)} if node.variable else {}
                ),
                on_left=[ID_COLUMN],
                on_right=[ID_COLUMN],
            ),
            column_names=[ID_COLUMN],
            variable_map={node.variable: DisambiguatedColumnName(relation_identifier=self.context.entity_mapping[node.labels[0]].identifier, column_name=ID_COLUMN)} if node.variable else {}
        )
        return out
    

if __name__ == "__main__":
    LOGGER.info(msg="Module loaded successfully.")

    entity_df_person: pd.DataFrame = pd.DataFrame(
        data={
            ID_COLUMN: [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [30, 40, 50],
        }
    )

    entity_df_city: pd.DataFrame = pd.DataFrame(
        data={
            ID_COLUMN: [4, 5],
            'name': ['New York', 'Los Angeles'],
            'population': [8000000, 4000000],
        }
    )

    relationship_df_lives_in: pd.DataFrame = pd.DataFrame(
        data={  
            ID_COLUMN: [20, 21, 22],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [4, 5, 4],
        }
    )

    relationship_df_knows: pd.DataFrame = pd.DataFrame(
        data={  
            ID_COLUMN: [10, 11],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2],
            RELATIONSHIP_TARGET_COLUMN: [2, 3],
        }
    )

            
    entity_table_person = EntityTable(entity_type="Person", column_names=[ID_COLUMN, 'name', 'age'], source_obj=entity_df_person)
    entity_table_city = EntityTable(entity_type="City", column_names=[ID_COLUMN, 'name', 'population'], source_obj=entity_df_city)
    relationship_table_lives_in = RelationshipTable(relationship_type="LIVES_IN", column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN], source_obj=relationship_df_lives_in)
    relationship_table_knows = RelationshipTable(relationship_type="KNOWS", column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN], source_obj=relationship_df_knows)
    node_0 = NodePattern(
        variable=Variable(name="n"),
        labels=["Person"],
        properties={},
    )

    node_3 = NodePattern(
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
    
    node_3 = NodePattern(
        variable=Variable(name="p3"),
        labels=["Person"],
        properties={"name": "Carol", "age": 50},
    )

    node_4 = NodePattern(
        variable=Variable(name="c1"),
        labels=["City"],
        properties={"name": "New York"},
    )

    relationship: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r"),
        types=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )
    
    relationship_1: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="s"),
        types=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    relationship_2: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="t"),
        types=["LIVES_IN"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )


    alice_knows_bob: PatternPath = PatternPath(
        elements=[node_1, relationship, node_2]
    )

    alice_knows_bob_knows_carol: PatternPath = PatternPath(
        elements=[node_1, relationship, node_2, relationship_1, node_3]
    )

    alice_knows_someone_lives_in_new_york: PatternPath = PatternPath(
        elements=[node_1, relationship, node_2, relationship_2, node_4]
    )

    path_empty_attrs: PatternPath = PatternPath(
        elements=[node_0, relationship, node_3]
    )   

    context: Context = Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": entity_table_person,
                "City": entity_table_city
            },
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": relationship_table_knows,
                "LIVES_IN": relationship_table_lives_in,
            }
        ),
    )
    star: Star = Star(context=context)
    
    translation: Relation = star.to_relation(obj=path_empty_attrs)
    rich.print(translation)

    print("Translating NodePattern:")
    print("(p:Person {name: 'Alice', age: 30})")

    translation: Relation = star.to_relation(obj=node_0)
    rich.print(translation)

    translation: Relation = star.to_relation(obj=node_1)
    rich.print(translation)

    print("Translating PatternPath:")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person {name: 'Bob', age: 40})")
    translation: Relation = star.to_relation(obj=alice_knows_bob)
    rich.print(translation) 
    
    print("\n---\n")
    print("Translating PatternPath:")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person {name: 'Bob', age: 40})-[s:KNOWS]->(p3:Person {name: 'Carol', age: 50})")
    translation: Relation = star.to_relation(obj=alice_knows_bob_knows_carol)
    rich.print(translation) 

    filter_rows_df: pd.DataFrame = FilterRows(relation=entity_table_person, condition=AttributeEqualsValue(left='name', right='Carol')).to_pandas(context=context)
    rich.print(filter_rows_df)

    projection_df: pd.DataFrame = Projection(relation=entity_table_person, column_names=['name']).to_pandas(context=context)
    rich.print(projection_df)
    
    projection_df: pd.DataFrame = Projection(
        relation=FilterRows(
            relation=entity_table_person, 
            condition=AttributeEqualsValue(
                left='name',
                right='Carol'
            ),
        ), column_names=['name']).to_pandas(context=context)
    rich.print(projection_df)

    print("\n---\n")
    print("Translating PatternPath:")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person)-[t:LIVES_IN]->(c1:City {name: 'New York'})")
    translation: Relation = star.to_relation(obj=alice_knows_someone_lives_in_new_york)
    rich.print(translation)