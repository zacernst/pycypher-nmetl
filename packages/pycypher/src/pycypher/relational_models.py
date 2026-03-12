from __future__ import annotations
from enum import Enum
from functools import wraps
from typing import Callable, Optional, Any, cast
from typing_extensions import Annotated
from pydantic import Field, BaseModel
import pandas as pd
from shared.logger import LOGGER
from pycypher.ast_models import (
    random_hash,
    Variable,
    Algebraizable,
)
from pycypher.property_change import PropertyChange

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


class RegisteredFunction(BaseModel):
    """Represents a registered Cypher function in the context."""

    model_config = {"arbitrary_types_allowed": True}

    name: str
    implementation: Callable
    arity: int = 0

    def __call__(self, *args) -> Any:
        if self.arity and len(args) != self.arity:
            raise ValueError(
                f"Function {self.name} expects {self.arity} arguments, got {len(args)}"
            )
        return self.implementation(*args)


class Context(BaseModel):
    """Context for translation operations."""

    entity_mapping: EntityMapping = EntityMapping()
    relationship_mapping: RelationshipMapping = RelationshipMapping()
    cypher_functions: dict[str, RegisteredFunction] = Field(default_factory=dict)

    def cypher_function(self, func):
        '''Decorator to register a function as a Cypher function in the context.'''

        LOGGER.info(f'Registering Cypher function: {func.__name__}')
        self.cypher_functions[func.__name__] = RegisteredFunction(
            name=func.__name__,
            implementation=func,
            arity=func.__code__.co_argcount,
        )
        return func


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
    CROSS = "CROSS"


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
        
        # Handle cross join specially (no join keys)
        if self.join_type == JoinType.CROSS or (not self.on_left and not self.on_right):
            joined_df: pd.DataFrame = pd.merge(
                left=left_df,
                right=right_df,
                how='cross',
            )
        else:
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


class PropertyModification(Relation):
    """Applies property modifications to entities.

    Represents SET clause operations that modify or add properties
    to entities in a relation. Each PropertyChange describes a specific
    modification to be applied.

    Attributes:
        relation: Source relation to modify
        property_changes: List of property changes to apply
        context: Context with entity/relationship mappings for evaluation
    """

    base_relation: Relation
    modifications: list[PropertyChange] = Field(default_factory=list)
    context: "Context" = Field(default=None)

    def to_pandas(self, context: "Context" = None) -> pd.DataFrame:
        """Convert this relation to a pandas DataFrame by applying property modifications."""
        # Use provided context or the stored context
        evaluation_context = context or self.context
        source_df = self.base_relation.to_pandas(evaluation_context).copy()

        # For SET operations that reference properties, we need full entity data, not just ID columns
        # Enrich the DataFrame with full entity data if needed
        source_df = self._ensure_full_entity_data(source_df, evaluation_context)

        # Apply property changes if any
        if self.modifications:
            # Import here to avoid circular dependency
            from pycypher.property_change import PropertyModificationEvaluator

            evaluator = PropertyModificationEvaluator()

            # Apply each property change
            for change in self.modifications:
                source_df = self._apply_property_change(source_df, change, evaluator)

            # Update original entity tables in context with new properties
            self._update_source_entity_tables(source_df, evaluation_context)

        # Preserve original column structure for pipeline continuity
        # We need to map back to the original base relation's column names
        original_columns = self.base_relation.column_names
        LOGGER.debug(f"Original base relation columns: {original_columns}")
        LOGGER.debug(f"Current source_df columns: {source_df.columns.tolist()}")

        # Create a mapping to restore original column names where possible
        final_df = pd.DataFrame()

        # First, handle the original columns that should be preserved
        for orig_col in original_columns:
            if orig_col in source_df.columns:
                # Direct match - just copy
                final_df[orig_col] = source_df[orig_col]
            else:
                # Look for a column that could map to this original column
                # This handles the case where we joined with entity data and got prefixed columns
                found = False
                for current_col in source_df.columns:
                    # Handle ID column mappings - both standard __ID__ and hash ID columns
                    if current_col.endswith(f"__{ID_COLUMN}"):
                        # This could be the ID column for the original hash column
                        # Check if the original column looks like a hash ID (32+ hex chars)
                        if (len(orig_col) >= 32 and
                            all(c in '0123456789abcdef' for c in orig_col.lower())):
                            # Map the entity ID column to the original hash column
                            final_df[orig_col] = source_df[current_col]
                            found = True
                            break
                        elif orig_col.endswith(("__ID__", ID_COLUMN)):
                            # Standard ID column mapping
                            final_df[orig_col] = source_df[current_col]
                            found = True
                            break

                if not found:
                    LOGGER.warning(f"Could not find mapping for original column {orig_col}")

        # Next, add any new property columns (from SET operations) with prefixes removed
        for col in source_df.columns:
            if col not in final_df.columns and "__" in col and not col.startswith("__"):
                # This is a new property column - remove prefix for output
                new_name = col.split("__", 1)[1]
                final_df[new_name] = source_df[col]

        LOGGER.debug(f"Final PropertyModification columns: {final_df.columns.tolist()}")
        return final_df

    def _apply_property_change(
        self,
        df: pd.DataFrame,
        change: PropertyChange,
        evaluator
    ) -> pd.DataFrame:
        """Apply a single property change to the DataFrame."""
        from pycypher.property_change import PropertyChangeType
        from shared.logger import LOGGER

        LOGGER.debug(f"_apply_property_change called for {change.variable_type}.{change.property_name}")
        LOGGER.debug(f"Input DataFrame columns: {df.columns.tolist()}")

        # Determine column name prefix by looking at existing columns
        column_prefix = None
        for col in df.columns:
            if "__" in col:
                column_prefix = col.split("__")[0]
                break

        # If no prefix found but we know the entity type, use that
        if not column_prefix and change.variable_type:
            column_prefix = change.variable_type

        LOGGER.debug(f"Using column prefix: {column_prefix}")

        if change.change_type == PropertyChangeType.SET_PROPERTY:
            # SET n.prop = value
            new_values = []
            for i in range(len(df)):
                value = evaluator.evaluate_property_value(
                    change.value_expression, df, i
                )
                # Convert numpy types to native Python types if needed
                if hasattr(value, 'item'):  # numpy scalar
                    value = value.item()
                new_values.append(value)

            # Use prefixed column name if there's a prefix pattern
            column_name = change.property_name
            if column_prefix:
                column_name = f"{column_prefix}__{change.property_name}"

            # Force native Python types to avoid numpy types
            df[column_name] = pd.Series(new_values, dtype=object)

        elif change.change_type == PropertyChangeType.SET_LABELS:
            # SET n:Label - store labels (implementation depends on label storage strategy)
            # For now, we'll add a special __labels__ column
            if "__labels__" not in df.columns:
                df["__labels__"] = [[] for _ in range(len(df))]

            for i in range(len(df)):
                current_labels = df.at[i, "__labels__"] if df.at[i, "__labels__"] else []
                new_labels = list(set(current_labels + change.labels))
                df.at[i, "__labels__"] = new_labels

        elif change.change_type == PropertyChangeType.SET_ALL_PROPERTIES:
            # SET n = {map} - replace all properties with map contents
            for i in range(len(df)):
                props_map = evaluator.evaluate_properties_map(
                    change.properties_map, df, i
                )
                for prop_name, value in props_map.items():
                    df.at[i, prop_name] = value

        elif change.change_type == PropertyChangeType.ADD_ALL_PROPERTIES:
            # SET n += {map} - add properties from map (preserve existing)
            for i in range(len(df)):
                props_map = evaluator.evaluate_properties_map(
                    change.properties_map, df, i
                )
                for prop_name, value in props_map.items():
                    df.at[i, prop_name] = value

        return df

    def _update_source_entity_tables(self, modified_df: pd.DataFrame, context: "Context") -> None:
        """Update original entity tables in context with new properties from SET operations.

        This ensures that subsequent queries can find the new properties in the entity tables,
        maintaining the architecture where expression evaluator checks against original tables.

        Args:
            modified_df: DataFrame with applied property changes (with prefixed columns)
            context: Context containing entity mappings to update
        """
        from pycypher.property_change import PropertyChangeType
        from shared.logger import LOGGER

        LOGGER.debug(f"_update_source_entity_tables called with {len(self.modifications)} modifications")
        LOGGER.debug(f"Modified DataFrame columns: {modified_df.columns.tolist()}")

        # Group modifications by entity type
        entity_modifications = {}
        for change in self.modifications:
            if change.variable_type not in entity_modifications:
                entity_modifications[change.variable_type] = []
            entity_modifications[change.variable_type].append(change)

        LOGGER.debug(f"Entity modifications grouped by type: {list(entity_modifications.keys())}")

        # Update each affected entity table
        for entity_type, changes in entity_modifications.items():
            if entity_type not in context.entity_mapping.mapping:
                LOGGER.warning(f"Entity type {entity_type} not found in context mapping")
                continue

            entity_table = context.entity_mapping.mapping[entity_type]
            LOGGER.debug(f"Updating entity table for {entity_type}")
            LOGGER.debug(f"Original source_obj columns: {entity_table.source_obj.columns.tolist()}")

            # Get the current source DataFrame (without prefixes)
            source_df = entity_table.source_obj.copy()

            # Find new columns added by SET operations (with or without entity type prefix)
            # Only consider columns that are likely to be new properties, not ID columns or hash columns
            new_columns = {}
            for col in modified_df.columns:
                prop_name = None

                if col.startswith(f"{entity_type}__"):
                    # Extract property name without prefix
                    candidate_prop = col.split("__", 1)[1]
                    # Skip ID columns and hash-like columns
                    if not self._is_id_or_hash_column(candidate_prop):
                        prop_name = candidate_prop
                elif not col.startswith("__") and "__" not in col:
                    # Column without prefix - might be a new property
                    # Check if it's not an ID column, hash column, or existing property
                    if (col not in source_df.columns and
                        not col.startswith(("__ID__", ID_COLUMN)) and
                        not self._is_id_or_hash_column(col)):
                        prop_name = col

                # Handle both new and existing properties
                if prop_name:
                    # Get values from modified DataFrame and store without prefix
                    new_columns[prop_name] = modified_df[col].tolist()
                    if prop_name not in source_df.columns:
                        LOGGER.debug(f"Found new property {prop_name} from column {col}")
                    else:
                        LOGGER.debug(f"Found existing property {prop_name} to update from column {col}")

            LOGGER.debug(f"Properties to add/update for {entity_type}: {list(new_columns.keys())}")

            # Add/update columns in source DataFrame
            for prop_name, values in new_columns.items():
                # Ensure we have the right number of values
                if len(values) != len(source_df):
                    # If lengths don't match, this might be a partial update
                    # For now, we'll extend with None values or truncate as needed
                    if len(values) < len(source_df):
                        values.extend([None] * (len(source_df) - len(values)))
                    else:
                        values = values[:len(source_df)]

                source_df[prop_name] = values

                if prop_name not in entity_table.attribute_map:
                    LOGGER.debug(f"Added new column {prop_name} to entity table {entity_type}")
                    # Update attribute maps to register new property
                    entity_table.attribute_map[prop_name] = prop_name
                    entity_table.source_obj_attribute_map[prop_name] = prop_name
                else:
                    LOGGER.debug(f"Updated existing column {prop_name} in entity table {entity_type}")

            # Update the source object with new columns
            entity_table.source_obj = source_df
            LOGGER.debug(f"Updated entity table {entity_type} source_obj columns: {entity_table.source_obj.columns.tolist()}")

    def _is_id_or_hash_column(self, column_name: str) -> bool:
        """Check if a column name looks like an ID or hash column rather than a user property.

        Args:
            column_name: Column name to check

        Returns:
            True if the column appears to be an ID/hash column
        """
        # Skip columns that look like hash IDs (32+ hex characters)
        if len(column_name) >= 32 and all(c in '0123456789abcdef' for c in column_name.lower()):
            return True

        # Skip columns that are clearly ID-related
        id_indicators = ['id', '__id__', 'uuid', 'guid']
        if column_name.lower() in id_indicators or column_name.lower().endswith('_id'):
            return True

        return False

    def _ensure_full_entity_data(self, df: pd.DataFrame, context: "Context") -> pd.DataFrame:
        """Ensure DataFrame has full entity data for SET expression evaluation.

        If the DataFrame only contains ID columns, join with full entity data from context.

        Args:
            df: Current DataFrame (might only have ID columns)
            context: Context with full entity mappings

        Returns:
            DataFrame with full entity data
        """
        from shared.logger import LOGGER

        # Identify which entity types are being modified
        entity_types = set()
        for change in self.modifications:
            if change.variable_type:
                entity_types.add(change.variable_type)

        if not entity_types:
            return df

        LOGGER.debug(f"Ensuring full entity data for types: {entity_types}")
        LOGGER.debug(f"Input DataFrame columns: {df.columns.tolist()}")

        # For each entity type, check if we need to join with full data
        enriched_df = df.copy()

        for entity_type in entity_types:
            if entity_type not in context.entity_mapping.mapping:
                continue

            entity_table = context.entity_mapping.mapping[entity_type]
            full_entity_df = entity_table.to_pandas(context)

            # Check if we already have the entity data or just ID columns
            entity_id_col = f"{entity_type}__{ID_COLUMN}"
            has_full_data = any(col.startswith(f"{entity_type}__") and
                              not col.endswith(f"__{ID_COLUMN}")
                              for col in enriched_df.columns)

            if not has_full_data and entity_id_col in full_entity_df.columns:
                # Find the ID column in the current DataFrame
                id_col_in_current = None
                for col in enriched_df.columns:
                    # Check if this is an entity ID column (either prefixed or a hash that represents an ID)
                    if (col == entity_id_col or
                        (len(col) >= 32 and all(c in '0123456789abcdef' for c in col.lower()))):
                        id_col_in_current = col
                        break

                if id_col_in_current and entity_id_col in full_entity_df.columns:
                    LOGGER.debug(f"Joining with full entity data for {entity_type}")
                    LOGGER.debug(f"Joining on {id_col_in_current} = {entity_id_col}")

                    # Prepare for join - make sure ID columns have same name
                    if id_col_in_current != entity_id_col:
                        # Create a mapping for join
                        join_df = full_entity_df.copy()
                        current_for_join = enriched_df.copy()

                        # Rename the ID column in current DataFrame to match
                        current_for_join = current_for_join.rename(columns={id_col_in_current: entity_id_col})

                        # Join with full entity data
                        enriched_df = current_for_join.merge(
                            join_df,
                            on=entity_id_col,
                            how='left',
                            suffixes=('', '_duplicate')
                        )

                        # Remove duplicate columns
                        enriched_df = enriched_df.loc[:, ~enriched_df.columns.str.endswith('_duplicate')]
                    else:
                        # Direct join
                        enriched_df = enriched_df.merge(
                            full_entity_df,
                            on=entity_id_col,
                            how='left',
                            suffixes=('', '_duplicate')
                        )
                        enriched_df = enriched_df.loc[:, ~enriched_df.columns.str.endswith('_duplicate')]

        LOGGER.debug(f"Enriched DataFrame columns: {enriched_df.columns.tolist()}")
        return enriched_df


class ExpressionProjection(Relation):
    """Projects computed expressions as new columns.
    
    Unlike simple Projection (column renaming), this evaluates
    expressions like p.name, p.age, etc. and creates new columns.
    
    Used primarily in WITH clauses to evaluate expressions and create
    aliases for the results.
    
    Attributes:
        relation: Source relation
        expressions: Dict mapping output column name (alias) to AST expression
        context: Context with entity/relationship mappings for evaluation
    
    Example:
        WITH p.name AS person_name, p.age AS age
        -> ExpressionProjection(
            expressions={
                "person_name": PropertyLookup(expression=Variable("p"), property="name"),
                "age": PropertyLookup(expression=Variable("p"), property="age")
            }
        )
    """
    
    relation: Relation
    expressions: dict[ColumnName, Any]  # Maps alias -> Expression (Any to avoid circular import)
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the ExpressionProjection to a pandas DataFrame.
        
        Evaluates each expression against the base DataFrame and creates
        new columns with the results.
        
        Args:
            context: Context with entity/relationship mappings
            
        Returns:
            DataFrame with computed expression columns
        """
        from pycypher.expression_evaluator import ExpressionEvaluator
        
        # Get base DataFrame
        base_df = self.relation.to_pandas(context=context)
        
        # Create evaluator
        evaluator = ExpressionEvaluator(context=context, relation=self.relation)
        
        # Build result DataFrame with expression columns
        result_columns = {}
        
        for alias, expression in self.expressions.items():
            # Evaluate expression
            series, _ = evaluator.evaluate(expression, base_df)
            result_columns[alias] = series
        
        # Create result DataFrame
        result_df = pd.DataFrame(result_columns)
        
        LOGGER.debug(
            msg=f"ExpressionProjection created {len(result_df)} rows with columns: {list(result_df.columns)}"
        )
        
        return result_df


class Aggregation(Relation):
    """Represents aggregation operations (full-table aggregations).
    
    Applies aggregation functions like COLLECT(), COUNT(), SUM(), AVG(), MIN(), MAX()
    to entire columns without grouping.
    
    This is for Phase 2 - simple aggregations without GROUP BY.
    Phase 3 will add grouped aggregations.
    
    Attributes:
        relation: Source relation
        aggregations: Dict mapping output column name (alias) to aggregation expression
        context: Context for evaluation
    
    Example:
        WITH count(p) AS person_count, collect(p.name) AS names
        -> Aggregation(
            aggregations={
                "person_count": FunctionInvocation(name="count", arguments=...),
                "names": FunctionInvocation(name="collect", arguments=...)
            }
        )
    
    Supported aggregation functions:
        - collect(expr): Collect all values into a list
        - count(expr): Count non-null values
        - count(*): Count all rows (uses CountStar AST node)
        - sum(expr): Sum all numeric values
        - avg(expr): Average of numeric values
        - min(expr): Minimum value
        - max(expr): Maximum value
    """
    
    relation: Relation
    aggregations: dict[ColumnName, Any]  # Maps alias -> aggregation Expression
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the Aggregation to a pandas DataFrame.
        
        Evaluates each aggregation function against the base DataFrame and creates
        a single-row result with aggregated values.
        
        Args:
            context: Context with entity/relationship mappings
            
        Returns:
            Single-row DataFrame with aggregated values
        """
        from pycypher.expression_evaluator import ExpressionEvaluator
        
        # Get base DataFrame
        base_df = self.relation.to_pandas(context=context)
        
        # Create evaluator
        evaluator = ExpressionEvaluator(context=context, relation=self.relation)
        
        # Build result dictionary with aggregated values
        result_dict = {}
        
        for alias, agg_expr in self.aggregations.items():
            # Evaluate aggregation
            agg_value = evaluator.evaluate_aggregation(agg_expr, base_df)
            result_dict[alias] = [agg_value]  # Wrap in list for DataFrame
        
        # Create single-row result DataFrame
        result_df = pd.DataFrame(result_dict)
        
        LOGGER.debug(
            msg=f"Aggregation created 1 row with columns: {list(result_df.columns)}"
        )
        
        return result_df


class GroupedAggregation(Relation):
    """Represents grouped aggregation operations (GROUP BY aggregations).
    
    Applies aggregation functions grouped by one or more columns.
    This is Phase 3 - aggregations with GROUP BY.
    
    Attributes:
        relation: Source relation
        grouping_expressions: Dict mapping column name to expression (non-aggregation)
        aggregations: Dict mapping column name to aggregation expression
    
    Example:
        WITH p.city AS city, count(*) AS count
        -> GroupedAggregation(
            grouping_expressions={
                "city": PropertyLookup(expression=Variable("p"), property="city")
            },
            aggregations={
                "count": CountStar()
            }
        )
    
    The result will have one row per unique combination of grouping column values,
    with aggregations computed for each group.
    """
    
    relation: Relation
    grouping_expressions: dict[ColumnName, Any]  # Maps alias -> Expression
    aggregations: dict[ColumnName, Any]  # Maps alias -> aggregation Expression
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert the GroupedAggregation to a pandas DataFrame.
        
        Evaluates grouping expressions, groups by them, then computes aggregations
        for each group.
        
        Args:
            context: Context with entity/relationship mappings
            
        Returns:
            DataFrame with one row per group, containing grouping columns and aggregated values
        """
        from pycypher.expression_evaluator import ExpressionEvaluator
        
        # Get base DataFrame
        base_df = self.relation.to_pandas(context=context)
        
        # Create evaluator
        evaluator = ExpressionEvaluator(context=context, relation=self.relation)
        
        # Step 1: Evaluate grouping expressions
        grouping_columns = {}
        for alias, expr in self.grouping_expressions.items():
            series, _ = evaluator.evaluate(expr, base_df)
            grouping_columns[alias] = series
        
        # Create a temporary DataFrame with grouping columns
        temp_df = base_df.copy()
        for alias, series in grouping_columns.items():
            temp_df[alias] = series
        
        # Step 2: Group by the grouping columns
        if not grouping_columns:
            # No grouping columns - this shouldn't happen in Phase 3
            # Fall back to full-table aggregation behavior
            raise ValueError("GroupedAggregation requires at least one grouping expression")
        
        grouping_column_names = list(grouping_columns.keys())
        
        # For pandas groupby: pass string for single column, list for multiple
        # This ensures group_key is scalar for single column, tuple for multiple
        if len(grouping_column_names) == 1:
            grouped = temp_df.groupby(grouping_column_names[0], dropna=False)
        else:
            grouped = temp_df.groupby(grouping_column_names, dropna=False)
        
        # Step 3: Apply aggregations to each group
        result_rows = []
        
        for group_key, group_df in grouped:
            row_dict = {}
            
            # Add grouping column values
            if len(grouping_column_names) == 1:
                # Single grouping column - group_key is a scalar
                row_dict[grouping_column_names[0]] = group_key
            else:
                # Multiple grouping columns - group_key is a tuple
                for i, col_name in enumerate(grouping_column_names):
                    row_dict[col_name] = group_key[i]
            
            # Compute aggregations for this group
            for alias, agg_expr in self.aggregations.items():
                agg_value = evaluator.evaluate_aggregation(agg_expr, group_df)
                row_dict[alias] = agg_value
            
            result_rows.append(row_dict)
        
        # Create result DataFrame
        if result_rows:
            result_df = pd.DataFrame(result_rows)
        else:
            # No groups - return empty DataFrame with correct columns
            all_columns = list(grouping_columns.keys()) + list(self.aggregations.keys())
            result_df = pd.DataFrame(columns=all_columns)
        
        LOGGER.debug(
            msg=f"GroupedAggregation created {len(result_df)} rows with columns: {list(result_df.columns)}"
        )
        
        return result_df


if __name__ == '__main__':
    # Basic test of Context and Relation classes
    context = Context()

    @context.cypher_function
    def my_custom_function(x):
        return x * 2
    
    print("Registered Cypher functions in context:")
    for func_name in context.cypher_functions:
        print(f" - {func_name}")
        print(context.cypher_functions[func_name])