from __future__ import annotations
from enum import Enum
from typing import Optional, Any, cast
from typing_extensions import Annotated
from pydantic import Field, BaseModel
import pandas as pd
from shared.logger import LOGGER
from pycypher.ast_models import (
    random_hash,
    Variable,
    Algebraizable,
)

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


EntityType = Annotated[str, ...]
Attribute = Annotated[str, ...]
RelationshipType = Annotated[str, ...]
ColumnName: Annotated[..., ...] = Annotated[str, ...]
VariableMap = Annotated[dict[Variable, ColumnName], ...]
VariableTypeMap = Annotated[dict[Variable, EntityType | RelationshipType], ...]
AttributeMap = Annotated[dict[Attribute, ColumnName], ...]


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


class Context(BaseModel):
    """Context for translation operations."""

    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()


class Relation(BaseModel):
    """A `Relation` represents a tabular data structure with some metadata."""

    source_algebraizable: Algebraizable | None = None
    variable_map: VariableMap = {}
    variable_type_map: VariableTypeMap = Field(
        default_factory=dict
    )  # e.g. "node" or "relationship"
    column_names: list[ColumnName] = []
    identifier: str = Field(default_factory=lambda: random_hash())

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Note: Pydantic models with default fields might have them set after init if passed as args
        # But we check self.column_names which should be set by super().__init__
        if not self.column_names:
            LOGGER.warning(
                msg=f"Relation {type(self).__name__} created without column_names specified."
            )

        self.column_names: list[ColumnName] = [
            str(column_name) for column_name in self.column_names
        ]

        self.variable_map: VariableMap = {
            var_name: str(column_name)
            for var_name, column_name in self.variable_map.items()
        }

        self.variable_type_map: VariableTypeMap = {
            var_name: var_type
            for var_name, var_type in self.variable_type_map.items()
        }

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Relation to a pandas DataFrame."""
        raise NotImplementedError(
            "to_pandas not implemented for base Relation class."
        )


class RelationIntersection(Relation):
    """Intersection of multiple Relations, implicit Join on shared variables."""

    relation_list: list[Relation]

    def variables_in_common(self):
        """Find variables in common across all relations in the intersection."""
        if not self.relation_list:
            return set()
        variables_in_common: set[Variable] = set(
            self.relation_list[0].variable_map.keys()
        )
        for relation in self.relation_list[1:]:
            variables_in_common.intersection_update(
                relation.variable_map.keys()
            )
        return variables_in_common


class EntityTable(Relation):
    """Source of truth for all IDs and attributes for a specific entity type."""

    entity_type: EntityType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, ColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(
        default_factory=dict
    )  # Assume all table objects (e.g. DataFrames) have string column names.

    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = self.source_obj
        # Disambiguate columns by prefixing with entity type
        rename_map = {col: f"{self.entity_type}__{col}" for col in df.columns}
        return df.rename(columns=rename_map)


class RelationshipTable(Relation):
    """Source of truth for all IDs and attributes for a specific relationship type."""

    relationship_type: RelationshipType
    source_obj: Any = Field(default=None, repr=False)
    attribute_map: dict[Attribute, ColumnName] = Field(default_factory=dict)
    source_obj_attribute_map: dict[Attribute, str] = Field(
        default_factory=dict
    )  # Assume all table objects (e.g. DataFrames) have string column names.

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the RelationshipTable to a pandas DataFrame."""
        df: pd.DataFrame = self.source_obj
        # Disambiguate columns by prefixing with relationship type
        rename_map = {
            col: f"{self.relationship_type}__{col}" for col in df.columns
        }
        return df.rename(columns=rename_map)


class Projection(Relation):
    """Selection of specific columns from a Relation.

    To be used in `RETURN` and `WITH` clauses."""

    relation: Relation
    projected_column_names: dict[ColumnName, ColumnName]

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Projection to a pandas DataFrame."""
        base_df: pd.DataFrame = self.relation.to_pandas(context=context)
        projected_df: pd.DataFrame = base_df.rename(
            columns=self.projected_column_names
        )[list(self.projected_column_names.values())]
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
        """Convert the Join to a pandas DataFrame.
        
        Only returns ID columns corresponding to variables, not attribute columns.
        """
        left_df: pd.DataFrame = self.left.to_pandas(context=context)
        right_df: pd.DataFrame = self.right.to_pandas(context=context)
        join_type_str: str = self.join_type.value.lower()
        joined_df: pd.DataFrame = pd.merge(
            left=left_df,
            right=right_df,
            how=join_type_str,
            left_on=self.on_left,
            right_on=self.on_right,
        )
        
        # Only keep columns that correspond to variables (ID columns)
        # Use column_names if specified, otherwise use variable_map values
        if self.column_names:
            columns_to_keep = [col for col in self.column_names if col in joined_df.columns]
        else:
            columns_to_keep = [col for col in self.variable_map.values() if col in joined_df.columns]
        
        return joined_df[columns_to_keep]


class SelectColumns(Relation):
    """Relation that projects a subset of columns from another relation."""

    relation: Relation
    column_names: list[ColumnName]

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the SelectColumns into a pandas DataFrame."""
        df: pd.DataFrame = self.relation.to_pandas(context=context)
        # Select only the columns specified
        return df[[str(object=col) for col in self.column_names]]


class FilterRows(Relation):
    """Filter represents a filtering operation on a Relation."""

    relation: Relation
    condition: BooleanCondition  # Placeholder for condition expression
    column_map: dict[ColumnName, ColumnName] = Field(default_factory=dict)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the FilterRows relation to a pandas DataFrame.
        
        Only returns ID columns corresponding to variables, not attribute columns.
        Attributes are fetched on-demand via joins when needed.
        """
        base_df: pd.DataFrame = self.relation.to_pandas(context=context)

        match self.condition:
            case AttributeEqualsValue():
                # Filtering by attribute with specific value
                condition_attribute = self.condition.left
                condition_value: Any = (
                    self.condition.right.evaluate()
                    if hasattr(self.condition.right, "evaluate")
                    else self.condition.right
                )
                entity_type: EntityType = self.relation.variable_type_map[
                    next(iter(self.relation.variable_map.keys()))
                ]

                # Fetch entity table to access attribute for filtering
                entity_df: pd.DataFrame = context.entity_mapping[
                    entity_type
                ].to_pandas(context=context)
                
                # Get the attribute column name with entity type prefix
                attribute_column_name: ColumnName = context.entity_mapping[
                    entity_type
                ].attribute_map[condition_attribute]
                prefixed_attribute_column_name = f"{entity_type}__{attribute_column_name}"
                
                # Filter entity table by attribute
                filtered_entity_df: pd.DataFrame = entity_df[
                    entity_df[prefixed_attribute_column_name] == condition_value
                ]

                # Get the ID column name from base relation
                base_entity_id_column: Optional[ColumnName] = None
                for var, col in self.relation.variable_map.items():
                    if self.relation.variable_type_map[var] == entity_type:
                        base_entity_id_column = col
                        break
                else:
                    raise ValueError(
                        f"No variable of type {entity_type} found in relation's variable_type_map."
                    )
                
                prefixed_id_column = f"{entity_type}__{ID_COLUMN}"
                
                # Semi-join: keep only rows in base_df whose IDs match filtered entities
                filtered_df = base_df[
                    base_df[base_entity_id_column].isin(
                        filtered_entity_df[prefixed_id_column]
                    )
                ]
                
                # Return only ID columns corresponding to variables, not attribute columns
                # Use column_names if specified, otherwise use variable_map values
                if self.column_names:
                    columns_to_keep = [col for col in self.column_names if col in filtered_df.columns]
                    filtered_df = filtered_df[columns_to_keep]
                elif self.variable_map:
                    columns_to_keep = [col for col in self.variable_map.values() if col in filtered_df.columns]
                    filtered_df = filtered_df[columns_to_keep]
                
                return filtered_df

            case _:
                raise NotImplementedError(
                    f"Condition type {type(self.condition)} not implemented yet."
                )
