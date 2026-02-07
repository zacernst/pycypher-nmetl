from __future__ import annotations
import copy
from pydantic import Field, BaseModel
import rich
import sys
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
from typing import Optional, Any, cast
from shared.logger import LOGGER

from enum import Enum

LOGGER.setLevel(level="DEBUG")
ID_COLUMN: str = "__ID__"
RELATIONSHIP_SOURCE_COLUMN: str = "__SOURCE__"
RELATIONSHIP_TARGET_COLUMN: str = "__TARGET__"

def flatten(lst: list[Any]) -> list[Any]:
    """Flatten a nested list."""
    flat_list: list[Any] = []
    for item in lst:
        if isinstance(item, list):
            flat_list.extend(flatten(item))
        else:
            flat_list.append(item)
    return flat_list

class DisambiguatedColumnName(BaseModel):
    """A column name that is disambiguated with its source relation."""
    
    relation_identifier: str
    column_name: str

    def __str__(self) -> str:
        return f"{self.relation_identifier}::{self.column_name}"
    
    def __hash__(self) -> int:
        return hash((self.relation_identifier, self.column_name))
    
    def to_pandas_column(self) -> str:
        """Convert to a pandas-compatible column name."""
        return str(f'{self.relation_identifier}__{self.column_name}')

EntityType = Annotated[str, ...]
Attribute = Annotated[str, ...]
RelationshipType = Annotated[str, ...]
ColumnName: Annotated[..., ...] = Annotated[str, ...]
VariableMap = Annotated[dict[Variable, DisambiguatedColumnName], ...]
AttributeMap = Annotated[dict[Attribute, DisambiguatedColumnName], ...]


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
    source_algebraizable: Optional[Algebraizable | list[Algebraizable]] = None
    variable_map: VariableMap = {}
    column_names: list[DisambiguatedColumnName] =  []
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
        raise NotImplementedError("to_pandas not implemented for base Relation class.")


class EntityTable(Relation):
    """Source of truth for all IDs and attributes for a specific entity type."""
    
    entity_type: EntityType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, DisambiguatedColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(default_factory=dict)  # Assume all table objects (e.g. DataFrames) have string column names.

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the EntityTable to a pandas DataFrame."""
        df: pd.DataFrame = self.source_obj
        column_renaming_dict: dict[str, str] = {
            ID_COLUMN: DisambiguatedColumnName(
                relation_identifier=self.entity_type,
                column_name=ID_COLUMN
            ).to_pandas_column()
        }
        for attribute, disambiguated_column_name in self.attribute_map.items():
            source_column_name: str = self.source_obj_attribute_map[attribute]
            column_renaming_dict[source_column_name] = disambiguated_column_name.to_pandas_column()
        df: pd.DataFrame = df.rename(columns=column_renaming_dict)
        return df


class RelationshipTable(Relation):
    """Source of truth for all IDs and attributes for a specific relationship type."""

    relationship_type: RelationshipType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, DisambiguatedColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(default_factory=dict)  # Assume all table objects (e.g. DataFrames) have string column names.
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the RelationshipTable to a pandas DataFrame."""
        df: pd.DataFrame = self.source_obj
        column_renaming_dict: dict[str, str] = {
            ID_COLUMN: DisambiguatedColumnName(
                relation_identifier=self.relationship_type,
                column_name=ID_COLUMN
            ).to_pandas_column(),
            RELATIONSHIP_SOURCE_COLUMN: DisambiguatedColumnName(
                relation_identifier=self.relationship_type,
                column_name=RELATIONSHIP_SOURCE_COLUMN
            ).to_pandas_column(),
            RELATIONSHIP_TARGET_COLUMN: DisambiguatedColumnName(
                relation_identifier=self.relationship_type,
                column_name=RELATIONSHIP_TARGET_COLUMN
            ).to_pandas_column(),
        }
        for attribute, disambiguated_column_name in self.attribute_map.items():
            source_column_name: str = self.source_obj_attribute_map[attribute]
            column_renaming_dict[source_column_name] = disambiguated_column_name.to_pandas_column()
        df: pd.DataFrame = df.rename(columns=column_renaming_dict)
        return df
    

class Projection(Relation):
    """Selection of specific columns from a Relation.

    To be used in `RETURN` and `WITH` clauses."""

    relation: Relation
    projected_column_names: dict[DisambiguatedColumnName, DisambiguatedColumnName]

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
    on_left: list[DisambiguatedColumnName]
    on_right: list[DisambiguatedColumnName]

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
    column_names: list[DisambiguatedColumnName]


class FilterRows(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    condition: BooleanCondition  # Placeholder for condition expression
    column_map: dict[DisambiguatedColumnName, DisambiguatedColumnName] = Field(default_factory=dict)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # self.column_names: list[DisambiguatedColumnName] = []  # self.column_names or self.relation.column_names
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
                len(properties) >= 1             # (n:Thing {prop1: val1})
            ):
                LOGGER.debug(msg="Translating NodePattern with one or more properties.")
                out: FilterRows = self._from_node_pattern_with_attrs(node=obj)
            case RelationshipPattern(            # -[r:KNOWS]->
                variable=_, labels=_, properties=properties
            ) if len(properties) == 0:
                LOGGER.debug(
                    msg="Translating RelationshipPattern with no properties."
                )
                out: RelationshipTable = self._from_relationship_pattern(relationship=obj)
            case RelationshipPattern(
                variable=_, labels=_, properties=properties
            ) if len(properties) >= 1:
                LOGGER.debug(
                    msg=f"Translating RelationshipPattern with {len(properties)} properties."
                )
                out: FilterRows = self._from_relationship_pattern_with_attrs(relationship=obj)
            case PatternPath() as pattern_path:  # (p1)-[r:KNOWS]->(p2)
                LOGGER.debug(msg="Translating PatternPath.")
                out: Relation = self._from_pattern_path(pattern_path=pattern_path)
            
            case _:
                raise NotImplementedError(
                    f"Translation for {type(obj)} is not implemented."
                )
        # Record the original `obj` in the Relation for traceability
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
                right_join_key: DisambiguatedColumnName = DisambiguatedColumnName(
                    relation_identifier=right.identifier,
                    column_name=RELATIONSHIP_SOURCE_COLUMN
                )
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
                join: Join = Join(
                    left=left,
                    right=right,
                    source_algebraizable=flatten([left.source_algebraizable, right.source_algebraizable]),
                    on_left=[left_join_key],
                    on_right=[right_join_key],
                    join_type=join_type,
                    variable_map=variable_map,
                    column_names=left.column_names + right.column_names,  # NOTE: Should not be any collisions here
                )
                # TODO: The SelectColumns logic below needs refinement
                # For now, return the join directly
                return join
                # variable_relation: Relation = SelectColumns(
                #     relation=join,
                #     source_algebraizable=flatten([join.source_algebraizable]),
                #     identifier=random_hash(),
                #     variable_map=join.variable_map,
                #     column_names=[column_name for column_name in join.column_names if column_name in join.variable_map.values()]
                # )
            case (NodePattern(), RelationshipPattern()) if cast(typ=RelationshipPattern, val=right.source_algebraizable).direction == RelationshipDirection.LEFT:
                LOGGER.debug(msg="Joining Node with Relationship for (n)<-[r:RELATIONSHIP] (tail)`.")
                node_variable: Variable = cast(typ=NodePattern, val=left.source_algebraizable).variable
                node_variable_column: DisambiguatedColumnName = left.variable_map[node_variable]
                left_join_key: DisambiguatedColumnName = node_variable_column
                right_join_key: DisambiguatedColumnName = DisambiguatedColumnName(
                    relation_identifier=right.identifier,
                    column_name=RELATIONSHIP_TARGET_COLUMN
                )
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
            case (RelationshipPattern(), NodePattern()) if cast(typ=RelationshipPattern, val=left.source_algebraizable).direction == RelationshipDirection.RIGHT:
                LOGGER.debug(msg="Joining Relationship with Node for [r:RELATIONSHIP]->(n)")
                node_variable: Variable = cast(typ=NodePattern, val=right.source_algebraizable).variable
                node_variable_column: DisambiguatedColumnName = right.variable_map[node_variable]
                left_join_key: DisambiguatedColumnName = DisambiguatedColumnName(
                    relation_identifier=left.identifier,
                    column_name=RELATIONSHIP_TARGET_COLUMN
                )
                right_join_key: DisambiguatedColumnName = node_variable_column
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
            case (RelationshipPattern(), NodePattern()) if cast(typ=RelationshipPattern, val=left.source_algebraizable).direction == RelationshipDirection.LEFT:
                LOGGER.debug(msg="Joining Relationship with Node for [r:RELATIONSHIP]<-(n)")
                node_variable: Variable = cast(typ=NodePattern, val=right.source_algebraizable).variable
                node_variable_column: DisambiguatedColumnName = right.variable_map[node_variable]
                left_join_key: DisambiguatedColumnName = DisambiguatedColumnName(
                    relation_identifier=left.identifier,
                    column_name=RELATIONSHIP_SOURCE_COLUMN
                )
                right_join_key: DisambiguatedColumnName = node_variable_column
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
            case (NodePattern(), NodePattern()):
                # This should never happen
                raise ValueError("Cannot join two NodePatterns directly.")
            case (RelationshipPattern(), RelationshipPattern()):
                # This should never happen
                raise ValueError("Cannot join two RelationshipPatterns directly.")

            case _:
                LOGGER.debug(msg="Joining two complex Relations (Projection/Join).")
                # For complex relations, find ID columns in column_names
                left_id_col = next((col for col in left.column_names if col.column_name == ID_COLUMN), None)
                right_id_col = next((col for col in right.column_names if col.column_name == ID_COLUMN), None)
                if not left_id_col or not right_id_col:
                    raise ValueError(f"Cannot find ID columns for join: left={left.column_names}, right={right.column_names}")
                left_join_key: DisambiguatedColumnName = left_id_col
                right_join_key: DisambiguatedColumnName = right_id_col
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map
                }
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
        relation: RelationshipTable = copy.deepcopy(self.context.relationship_mapping[relationship.labels[0]])
        relation.variable_map: dict[Variable, DisambiguatedColumnName] = {
            relationship.variable: DisambiguatedColumnName(
                relation_identifier=relation.identifier,
                column_name=ID_COLUMN
            )
        }
        return relation

    def _from_relationship_pattern_with_attrs(self, relationship: RelationshipPattern) -> FilterRows:
        """Convert a RelationshipPattern with attributes to a filtered RelationshipTable.
        
        Uses induction: pops one attribute, recursively processes remaining attributes,
        then applies a filter for the popped attribute.
        
        Args:
            relationship: RelationshipPattern with one or more properties
            
        Returns:
            FilterRows relation that filters the base relationship by all property constraints
        """
        # Pop one attribute off (inductive step)
        attr1: str = list(relationship.properties.keys())[0]
        val1: Any = list(relationship.properties.values())[0]
        
        # Create base relationship with remaining attributes
        base_relationship: RelationshipPattern = RelationshipPattern(
            variable=relationship.variable,
            labels=relationship.labels,
            direction=relationship.direction,  # CRITICAL: Must preserve direction!
            properties={
                attr: val 
                for attr, val in relationship.properties.items() 
                if attr != attr1
            }
        )
        
        # Recursive call (eventually hits base case with no properties)
        base_relationship_relation: Relation = self.to_relation(obj=base_relationship)
        
        # Create filtered relation
        filtered_relation_identifier: str = random_hash()
        filtered_relation: FilterRows = FilterRows(
            relation=base_relationship_relation,
            condition=AttributeEqualsValue(left=attr1, right=val1),
            column_map={
                DisambiguatedColumnName(
                    relation_identifier=base_relationship_relation.identifier,
                    column_name=disambiguated_column_name.column_name
                ): DisambiguatedColumnName(
                    relation_identifier=filtered_relation_identifier,
                    column_name=disambiguated_column_name.column_name
                ) 
                for disambiguated_column_name in base_relationship_relation.column_names
            },
            column_names=[
                DisambiguatedColumnName(
                    relation_identifier=filtered_relation_identifier, 
                    column_name=disambiguated_column_name.column_name
                ) 
                for disambiguated_column_name in base_relationship_relation.column_names
            ],
            identifier=filtered_relation_identifier,
            variable_map={
                variable: DisambiguatedColumnName(
                    relation_identifier=filtered_relation_identifier,
                    column_name=disambiguated_column_name.column_name
                ) 
                for variable, disambiguated_column_name in base_relationship_relation.variable_map.items()
            }
        )
        return filtered_relation

    def _from_node_pattern(self, node: NodePattern) -> Projection:
        """Convert a NodePattern to an EntityTable."""
        # Placeholder implementation
        relation = copy.deepcopy(self.context.entity_mapping[node.labels[0]])
        relation.variable_map = {
            node.variable: DisambiguatedColumnName(
                relation_identifier=relation.identifier,
                column_name=ID_COLUMN
            )
        } if node.variable else {}
        return relation
        # projection_identifier: str = random_hash()
        # out: Projection = Projection(
        #     identifier=projection_identifier,
        #     relation=self.context.entity_mapping[node.labels[0]],
        #     projected_column_names={
        #         DisambiguatedColumnName(
        #             relation_identifier=self.context.entity_mapping[node.labels[0]].identifier,
        #             column_name=column_name.column_name
        #         ): DisambiguatedColumnName(
        #             relation_identifier=projection_identifier,
        #             column_name=column_name.column_name
        #         )
        #         for column_name in self.context.entity_mapping[node.labels[0]].column_names
        #     },
        #     variable_map={node.variable: DisambiguatedColumnName(relation_identifier=projection_identifier, column_name=ID_COLUMN)} if node.variable else {}
        # )
        # # Attach variable mapping if variable is present
        # # Note: We do this only for basis cases without properties
        # return out

    def _from_node_pattern_with_attrs(self, node: NodePattern) -> FilterRows:
        """Convert a NodePattern with one property: value to a Relation with that property and value."""
        
        attr1: str = list(node.properties.keys())[0]
        val1: Any = list(node.properties.values())[0]
        base_node: NodePattern = NodePattern(
            variable=node.variable, labels=node.labels,
            properties={
                attr: val for attr, val in node.properties.items() if attr != attr1
            },
        )  # TODO: Define the base_node as the current node with one property popped off
        base_node_relation: Relation = self.to_relation(obj=base_node)  # left side for join
        filtered_relation_identifier: str = random_hash()
        filtered_relation: FilterRows = FilterRows(
            relation=base_node_relation,
            condition=AttributeEqualsValue(left=attr1, right=val1),
            column_map={  # The column_map here is from base relation to filtered relation
                          # It eliminates the need for a Projection after the FilterRows
                DisambiguatedColumnName(
                    relation_identifier=base_node_relation.identifier,
                    column_name=disambiguated_column_name.column_name
                ): DisambiguatedColumnName(
                    relation_identifier=filtered_relation_identifier,
                    column_name=disambiguated_column_name.column_name
                ) for disambiguated_column_name in base_node_relation.column_names
            },
            column_names=[DisambiguatedColumnName(relation_identifier=filtered_relation_identifier, column_name=disambiguated_column_name.column_name) for disambiguated_column_name in base_node_relation.column_names],
            identifier=filtered_relation_identifier,
            variable_map={
                variable: DisambiguatedColumnName(
                    relation_identifier=filtered_relation_identifier,
                    column_name=disambiguated_column_name.column_name
                ) for variable, disambiguated_column_name in base_node_relation.variable_map.items()
            }
        )
        return filtered_relation


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
            'since': [2015, 2018, 2020],
            'rent': [2000, 1500, 2200],
        }
    )

    relationship_df_knows: pd.DataFrame = pd.DataFrame(
        data={  
            ID_COLUMN: [10, 11],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2],
            RELATIONSHIP_TARGET_COLUMN: [2, 3],
            'since': [2020, 2019],
            'strength': [0.9, 0.7],
            'verified': [True, False],
        }
    )

            
    entity_table_person = EntityTable(
        entity_type="Person", 
        column_names=[
            DisambiguatedColumnName(relation_identifier="Person", column_name=ID_COLUMN),
            DisambiguatedColumnName(relation_identifier="Person", column_name='name'),
            DisambiguatedColumnName(relation_identifier="Person", column_name='age'),
        ],
        source_obj_attribute_map={
            'name': 'name',
            'age': 'age',
        },
        attribute_map = {
            'name': DisambiguatedColumnName(relation_identifier='Person', column_name='name'),
            'age': DisambiguatedColumnName(relation_identifier='Person', column_name='age'),
        },
        source_obj=entity_df_person
    )
    print(entity_table_person.to_pandas(context=Context())) 
    # entity_table_city = EntityTable(entity_type="City", column_names=[ID_COLUMN, 'name', 'population'], source_obj=entity_df_city)
    entity_table_city = EntityTable(
        entity_type="City", 
        column_names=[
            DisambiguatedColumnName(relation_identifier="City", column_name=ID_COLUMN),
            DisambiguatedColumnName(relation_identifier="City", column_name='name'),
            DisambiguatedColumnName(relation_identifier="City", column_name='population'),
        ],
        source_obj_attribute_map={
            'name': 'name',
            'population': 'population',
        },
        attribute_map = {
            'name': DisambiguatedColumnName(relation_identifier='City', column_name='name'),
            'population': DisambiguatedColumnName(relation_identifier='City', column_name='population'),
        },
        source_obj=entity_df_city
    )
    print(entity_table_city.to_pandas(context=Context())) 
    #Jsys.exit(0)
    relationship_table_lives_in_identifier: str = random_hash()
    relationship_table_lives_in = RelationshipTable(
        relationship_type="LIVES_IN", 
        identifier=relationship_table_lives_in_identifier,
        column_names=[
            DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name=ID_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name=RELATIONSHIP_SOURCE_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name=RELATIONSHIP_TARGET_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name='since'),
            DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name='rent'),
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            'since': 'since',
            'rent': 'rent',
        },
        attribute_map = {
            RELATIONSHIP_SOURCE_COLUMN: DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name=RELATIONSHIP_SOURCE_COLUMN),
            RELATIONSHIP_TARGET_COLUMN: DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name=RELATIONSHIP_TARGET_COLUMN),
            'since': DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name='since'),
            'rent': DisambiguatedColumnName(relation_identifier=relationship_table_lives_in_identifier, column_name='rent'),
        },
        source_obj=relationship_df_lives_in
    )
    relationship_table_knows_identifier: str = random_hash()
    relationship_table_knows = RelationshipTable(
        relationship_type="KNOWS", 
        column_names=[
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name=ID_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name=RELATIONSHIP_SOURCE_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name=RELATIONSHIP_TARGET_COLUMN),
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='since'),
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='strength'),
            DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='verified'),
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            'since': 'since',
            'strength': 'strength',
            'verified': 'verified',
        },
        attribute_map = {
            RELATIONSHIP_SOURCE_COLUMN: DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name=RELATIONSHIP_SOURCE_COLUMN),
            RELATIONSHIP_TARGET_COLUMN: DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name=RELATIONSHIP_TARGET_COLUMN),
            'since': DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='since'),
            'strength': DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='strength'),
            'verified': DisambiguatedColumnName(relation_identifier=relationship_table_knows_identifier, column_name='verified'),
        },
        source_obj=relationship_df_knows
    )
    print(relationship_table_knows.to_pandas(context=Context())) 
    print(relationship_table_lives_in.to_pandas(context=Context())) 
    
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
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )
    
    relationship_1: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="s"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    relationship_2: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="t"),
        labels=["LIVES_IN"],
        direction=RelationshipDirection.RIGHT,
        properties={},
    )

    # Test relationships with attributes
    relationship_with_one_attr: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r1"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={"since": 2020},
    )

    relationship_with_two_attrs: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r2"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={"since": 2020, "strength": 0.9},
    )

    relationship_with_three_attrs: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r3"),
        labels=["KNOWS"],
        direction=RelationshipDirection.RIGHT,
        properties={"since": 2020, "strength": 0.9, "verified": True},
    )

    relationship_lives_in_with_attrs: RelationshipPattern = RelationshipPattern(
        variable=Variable(name="r4"),
        labels=["LIVES_IN"],
        direction=RelationshipDirection.RIGHT,
        properties={"since": 2015, "rent": 2000},
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
    
    print("\n" + "="*80)
    print("TESTING RELATIONSHIP ATTRIBUTES - INDUCTIVE APPROACH")
    print("="*80 + "\n")
    
    # Test 1: Base case - relationship with no attributes
    print("Test 1: Relationship with NO attributes")
    print("-[r:KNOWS]->")
    translation: Relation = star.to_relation(obj=relationship_1)
    rich.print(translation)
    assert isinstance(translation, RelationshipTable), "Base case should return RelationshipTable"
    print("✓ Base case works\n")

    # Test 2: Relationship with ONE attribute
    print("Test 2: Relationship with ONE attribute")
    print("-[r1:KNOWS {since: 2020}]->")
    translation_one_attr: Relation = star.to_relation(obj=relationship_with_one_attr)
    rich.print(translation_one_attr)
    assert isinstance(translation_one_attr, FilterRows), "Should return FilterRows for relationship with attributes"
    assert translation_one_attr.condition.left == "since"
    assert translation_one_attr.condition.right == 2020
    print("✓ Single attribute works\n")

    # Test 3: Relationship with TWO attributes
    print("Test 3: Relationship with TWO attributes")
    print("-[r2:KNOWS {since: 2020, strength: 0.9}]->")
    translation_two_attrs: Relation = star.to_relation(obj=relationship_with_two_attrs)
    rich.print(translation_two_attrs)
    assert isinstance(translation_two_attrs, FilterRows), "Should return FilterRows"
    # Should be nested FilterRows
    assert isinstance(translation_two_attrs.relation, FilterRows), "Should have nested FilterRows for multiple attributes"
    print("✓ Two attributes work (nested FilterRows)\n")

    # Test 4: Relationship with THREE attributes
    print("Test 4: Relationship with THREE attributes")
    print("-[r3:KNOWS {since: 2020, strength: 0.9, verified: True}]->")
    translation_three_attrs: Relation = star.to_relation(obj=relationship_with_three_attrs)
    rich.print(translation_three_attrs)
    assert isinstance(translation_three_attrs, FilterRows), "Should return FilterRows"
    assert isinstance(translation_three_attrs.relation, FilterRows), "Should have nested FilterRows"
    assert isinstance(translation_three_attrs.relation.relation, FilterRows), "Should have double-nested FilterRows"
    print("✓ Three attributes work (triple-nested FilterRows)\n")

    # Test 5: Different relationship type with attributes
    print("Test 5: Different relationship type (LIVES_IN) with attributes")
    print("-[r4:LIVES_IN {since: 2015, rent: 2000}]->")
    translation_lives_in: Relation = star.to_relation(obj=relationship_lives_in_with_attrs)
    rich.print(translation_lives_in)
    assert isinstance(translation_lives_in, FilterRows), "Should return FilterRows"
    print("✓ Different relationship type works\n")

    print("\n" + "="*80)
    print("REGRESSION TESTS - EXISTING FUNCTIONALITY")
    print("="*80 + "\n")
    
    # Regression test 1: NodePattern with no attributes
    print("Regression Test 1: NodePattern with no attributes")
    print("(n:Person)")
    translation: Relation = star.to_relation(obj=node_0)
    rich.print(translation)
    print("✓ Node with no attributes still works\n")

    # Regression test 2: NodePattern with attributes
    print("Regression Test 2: NodePattern with attributes")
    print("(p:Person {name: 'Alice', age: 30})")
    translation: Relation = star.to_relation(obj=node_1)
    rich.print(translation)
    assert isinstance(translation, FilterRows), "Should return FilterRows for node with attributes"
    print("✓ Node with attributes still works\n")

    # Regression test 3: PatternPath without relationship attributes
    print("Regression Test 3: PatternPath without relationship attributes")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person {name: 'Bob', age: 40})")
    translation: Relation = star.to_relation(obj=alice_knows_bob)
    rich.print(translation)
    print("✓ PatternPath without relationship attributes still works\n")
    
    print("\n" + "="*80)
    print("ALL TESTS PASSED!")
    print("="*80 + "\n")
    
    sys.exit(0)
    
    # Old test code below (kept for reference but not executed)
    
    print("Translating NodePattern:")
    print("(p:Person {name: 'Alice', age: 30})")


    translation: Relation = star.to_relation(obj=node_0)
    rich.print(translation)
    

    translation: Relation = star.to_relation(obj=node_1)
    rich.print(translation)

    translation: Relation = star.to_relation(obj=relationship_1)
    rich.print(translation)
    

    print("Translating PatternPath:")
    print("(p1:Person {name: 'Alice', age: 30})-[r:KNOWS]->(p2:Person {name: 'Bob', age: 40})")
    translation: Relation = star.to_relation(obj=alice_knows_bob)
    rich.print(translation) 
    
    sys.exit(0)
    
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