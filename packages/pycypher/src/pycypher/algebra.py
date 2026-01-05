"""Relational algebra system for graph-based query processing.

This module provides the algebraic foundation for translating graph patterns
(nodes and relationships) into relational operations that can be executed on
pandas DataFrames. It implements the core operators needed for graph query
processing: joins, filters, projections, and column operations.

The module uses column hashing to avoid naming conflicts during complex multi-way
joins and maintains variable-to-column mappings to support Cypher-style variable
binding across query patterns.

Example:
    >>> person_node = Node(variable="p", label="Person", attributes={"name": "Alice"})
    >>> city_node = Node(variable="c", label="City", attributes={})
    >>> lives_in = Relationship(
    ...     variable="r",
    ...     label="LIVES_IN",
    ...     source_node=person_node,
    ...     target_node=city_node
    ... )
    >>> algebra = lives_in.to_algebra(context)
    >>> result_df = algebra.to_pandas(context)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pydantic import field_validator, BaseModel
from typing import Any, List, Optional
from enum import Enum
import random
import hashlib
import rich
import pandas as pd
import ibis
from ibis.expr.types import Table as IbisTable


def random_hash() -> str:
    """Generate a random hash string for column naming.
    
    Creates a unique identifier by hashing a random float. Used to generate
    collision-resistant column names during algebraic operations.
    
    Returns:
        str: A 32-character hexadecimal hash string.
    """
    return hashlib.md5(
        bytes(str(random.random()), encoding="utf-8")
    ).hexdigest()


class JoinType(str, Enum):
    """Enumeration of supported SQL join types.
    
    Attributes:
        INNER: Inner join - returns only matching rows from both tables.
        LEFT: Left outer join - returns all rows from left table.
        RIGHT: Right outer join - returns all rows from right table.
        FULL: Full outer join - returns all rows from both tables.
    """
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class GraphObjectType(BaseModel):
    """Base class for graph-level objects (nodes and relationships).
    
    Serves as a marker class to distinguish graph objects from algebraic operators.
    Graph objects can be converted to algebraic operations via `to_algebra()` methods.
    """
    pass


class Algebraic(BaseModel, ABC):
    """Abstract base class for all relational algebra operators.
    
    All algebraic operators maintain mappings between variable names, column names,
    and hashed column identifiers to support complex multi-way joins without naming
    conflicts. These operators can be composed to build complex query plans.
    
    Attributes:
        variables_to_columns: Maps Cypher variable names to hashed column names.
        column_name_to_hash: Maps original column names to their hashed versions.
        hash_to_column_name: Reverse mapping from hashed names to original names.
    """
    variables_to_columns: dict[str, str] = {}
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}

    @abstractmethod
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert this algebraic expression to a pandas DataFrame.
        
        Args:
            context: The execution context containing entity and relationship data.
            
        Returns:
            pd.DataFrame: The result of executing this algebraic operation.
        """
        ...

    @abstractmethod
    def to_ibis(self, context: Context) -> IbisTable:
        """Convert this algebraic expression to an Ibis table.
        
        Args:
            context: The execution context containing entity and relationship data.
            
        Returns:
            IbisTable: The result of executing this algebraic operation as an Ibis table.
        """
        ...
    

class Table(Algebraic):
    """Base class for table representations.
    
    Provides a unique identifier for each table instance, automatically generating
    a random hash if no identifier is provided.
    
    Attributes:
        identifier: Unique identifier for this table instance.
    """
    identifier: str = ""

    @field_validator("identifier", mode="after")
    @classmethod
    def set_identifier(cls, v: str) -> str:
        """Generate a random identifier if none was provided.
        
        Args:
            v: The identifier value (may be empty string).
            
        Returns:
            str: The provided identifier or a newly generated hash.
        """
        if v == "":
            return random_hash()
        return v


class EntityTable(Algebraic):
    """Represents a table of graph entities (nodes).
    
    EntityTable stores data about a particular type of node in the graph, along
    with its attributes. It maintains column name mappings to support collision-free
    joins with other tables.
    
    Attributes:
        entity_type: The type/label of entities in this table (e.g., "Person").
        attributes: List of attribute names for this entity type.
        entity_identifier_attribute: The attribute that uniquely identifies entities.
    """
    entity_type: str
    attributes: List[str]
    entity_identifier_attribute: str

    def __init__(self, **data: Any):
        """Initialize the entity table and create column hash mappings.
        
        Args:
            **data: Keyword arguments for entity_type, attributes, and
                entity_identifier_attribute.
        """
        super().__init__(**data)
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert this entity table to a pandas DataFrame with hashed column names.
        
        Args:
            context: The execution context containing the actual entity data.
            
        Returns:
            pd.DataFrame: The entity data with columns renamed to their hashed versions.
        """
        df: pd.DataFrame = context.obj_map[self.entity_type].rename(
            mapper=self.column_name_to_hash,
            axis=1,
        )
        return df

    def to_ibis(self, context: Context) -> IbisTable:
        """Convert this entity table to an Ibis table with hashed column names.
        
        Args:
            context: The execution context containing the actual entity data.
            
        Returns:
            IbisTable: The entity data with columns renamed to their hashed versions.
        """
        # Convert pandas DataFrame to Ibis table
        table: IbisTable = ibis.memtable(context.obj_map[self.entity_type])
        # Rename columns according to the hash mapping
        for old_name, new_name in self.column_name_to_hash.items():
            table = table.rename({new_name: old_name})
        return table


class RelationshipTable(Table):
    """Represents a table of graph relationships (edges).
    
    RelationshipTable stores data about connections between entities, including
    the source and target entity types. Each relationship can have its own attributes.
    
    Attributes:
        relationship_type: The type/label of this relationship (e.g., "LIVES_IN").
        source_entity_type: The entity type at the source of the relationship.
        target_entity_type: The entity type at the target of the relationship.
        attributes: List of attribute names for this relationship type.
        relationship_identifier_attribute: Optional unique identifier for relationships.
    """
    relationship_type: str
    source_entity_type: str
    target_entity_type: str
    attributes: List[str]
    relationship_identifier_attribute: Optional[str] = (
        None  # Maybe use later for rel attributes?
    )

    def __init__(self, **data: Any) -> None:
        """Initialize the relationship table and create column hash mappings.
        
        Args:
            **data: Keyword arguments for relationship_type, source_entity_type,
                target_entity_type, and attributes.
        """
        super().__init__(**data)
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert this relationship table to a pandas DataFrame.
        
        Args:
            context: The execution context containing the actual relationship data.
            
        Returns:
            pd.DataFrame: The relationship data.
        """
        df: pd.DataFrame = context.obj_map[self.relationship_type]
        return df

    def to_ibis(self, context: Context) -> IbisTable:
        """Convert this relationship table to an Ibis table.
        
        Args:
            context: The execution context containing the actual relationship data.
            
        Returns:
            IbisTable: The relationship data as an Ibis table.
        """
        table: IbisTable = ibis.memtable(context.obj_map[self.relationship_type])
        return table


class Context(BaseModel):
    """Execution context for algebraic operations.
    
    Context maintains the schema information (entity and relationship tables) and
    the actual data (as pandas DataFrames) needed to execute algebraic expressions.
    
    Attributes:
        entity_tables: List of entity table schemas.
        relationship_tables: List of relationship table schemas.
        obj_map: Dictionary mapping entity/relationship types to their DataFrame data.
    """
    entity_tables: List[EntityTable]
    relationship_tables: List[RelationshipTable]
    obj_map: dict[str, Any] = {}

    def get_entity_table(self, entity_type: str) -> EntityTable:
        """Retrieve an entity table by type.
        
        Args:
            entity_type: The type/label of the entity table to retrieve.
            
        Returns:
            EntityTable: The matching entity table schema.
            
        Raises:
            ValueError: If no entity table with the given type is found.
        """
        for entity_table in self.entity_tables:
            if entity_table.entity_type == entity_type:
                return entity_table
        else:
            raise ValueError(f"Entity table for type {entity_type} not found")

    def get_relationship_table(
        self, relationship_type: str
    ) -> RelationshipTable:
        """Retrieve a relationship table by type.
        
        Args:
            relationship_type: The type/label of the relationship table to retrieve.
            
        Returns:
            RelationshipTable: The matching relationship table schema.
            
        Raises:
            ValueError: If no relationship table with the given type is found.
        """
        for relationship_table in self.relationship_tables:
            if relationship_table.relationship_type == relationship_type:
                return relationship_table
        else:
            raise ValueError(
                f"Relationship table for type {relationship_type} not found"
            )


class Evaluable(Algebraic):
    """Base class for evaluable expressions.
    
    Marker class for expressions that can be evaluated to produce a value.
    """
    pass


class Boolean(Algebraic):
    """Base class for boolean conditions used in filters.
    
    Marker class for representing boolean predicates that can be evaluated
    against rows in a DataFrame.
    """
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Boolean conditions do not directly convert to DataFrames.
        
        Raises:
            NotImplementedError: Always, since boolean conditions are not tables.
        """
        raise NotImplementedError("Boolean conditions cannot be converted to DataFrames")

    def to_ibis(self, context: Context) -> IbisTable:
        """Boolean conditions do not directly convert to Ibis tables.
        
        Raises:
            NotImplementedError: Always, since boolean conditions are not tables.
        """
        raise NotImplementedError("Boolean conditions cannot be converted to Ibis tables")


class HasAttributeValue(Boolean):
    """Condition that checks if an attribute has a specific value.
    
    Used in Filter operations to select rows where a particular attribute
    matches the specified value.
    
    Attributes:
        attribute: The name of the attribute to check.
        value: The value to match against (can be string, number, boolean, or None).
    """
    attribute: str
    value: str | int | float | bool | None


class VariableAttributeValue(Evaluable):
    """Expression that retrieves the value of an attribute for a given variable.
    
    Used to reference attribute values in expressions, such as in equality checks.
    
    Attributes:
        variable: The variable name whose attribute is being accessed.
        attribute: The name of the attribute to retrieve.
    """
    variable: str
    attribute: str


THE_TRUE: Boolean = Boolean()
THE_FALSE: Boolean = Boolean()


class PrimitiveValue(Evaluable):
    """Expression that represents a primitive constant value.
    
    Used in expressions to represent literal values such as strings, numbers,
    booleans, or None.
    
    Attributes:
        value: The primitive value (string, number, boolean, or None).
    """
    value: str | int | float | bool | None


class Float(PrimitiveValue):
    """Expression that represents a floating-point constant value.
    
    Attributes:
        value: The float value.
    """
    value: float


class Integer(PrimitiveValue):
    """Expression that represents an integer constant value.
    
    Attributes:
        value: The integer value.
    """
    value: int


class String(PrimitiveValue):
    """Expression that represents a string constant value.
    
    Attributes:
        value: The string value.
    """
    value: str


class Conjunction(Boolean):
    """Condition that represents a logical AND of multiple boolean conditions.
    
    Used in Filter operations to combine multiple predicates that must all be
    satisfied for a row to be included.
    
    Attributes:
        conditions: List of boolean conditions to combine.
    """
    left: Boolean
    right: Boolean

    def evaluate(self, context: Context) -> bool:
        """Evaluate the conjunction of the left and right conditions.
        
        Returns:
            bool: True if both conditions are True, False otherwise.
        """
        return self.left.evaluate() and self.right.evaluate()


class Disjunction(Boolean):
    """Condition that represents a logical OR of multiple boolean conditions.
    
    Used in Filter operations to combine multiple predicates where at least one
    must be satisfied for a row to be included.
    
    Attributes:
        conditions: List of boolean conditions to combine.
    """
    left: Boolean
    right: Boolean


class Negation(Boolean):
    """Condition that represents the logical NOT of a boolean condition.
    
    Used in Filter operations to invert a predicate, selecting rows that do not
    satisfy the given condition.
    
    Attributes:
        condition: The boolean condition to negate.
    """
    condition: Boolean


class IsEqual(Boolean):
    """Condition that checks if two attributes are equal.
    
    Used in Filter operations to select rows where the values of two attributes
    are equal.
    
    Attributes:
        attribute_1: The name of the first attribute.
        attribute_2: The name of the second attribute.
    """
    left: Evaluable
    right: Evaluable


class IsGreaterThan(Boolean):
    """Condition that checks if one attribute is greater than another.
    
    Used in Filter operations to select rows where the value of one attribute
    exceeds that of another.
    
    Attributes:
        attribute_1: The name of the first attribute.
        attribute_2: The name of the second attribute.
    """
    left: Evaluable
    right: Evaluable


class IsLessThan(Boolean):
    """Condition that checks if one attribute is less than another.
    
    Used in Filter operations to select rows where the value of one attribute
    is less than that of another.
    
    Attributes:
        attribute_1: The name of the first attribute.
        attribute_2: The name of the second attribute.
    """
    left: Evaluable
    right: Evaluable


class Node(GraphObjectType):
    """Represents a node (vertex) in a graph pattern.
    
    Nodes have a label (entity type), a variable name for binding in queries,
    and optionally a set of attribute constraints. When converted to algebra,
    a node becomes an EntityTable optionally filtered by its attributes.
    
    Attributes:
        variable: The variable name to bind this node to (e.g., "p" for person).
        label: The entity type/label (e.g., "Person").
        attributes: Dictionary of attribute name-value pairs for filtering.
    """
    variable: str
    label: str
    attributes: dict = {}

    def to_algebra(self, context: Context) -> Filter | EntityTable:
        """Convert this node to an algebraic expression.
        
        Creates an EntityTable for the node's label, then applies Filter operations
        for each attribute constraint. The variable is mapped to the entity's
        identifier column.
        
        Args:
            context: The execution context containing entity table schemas.
            
        Returns:
            Filter | EntityTable: An EntityTable if no attributes, otherwise a Filter
                chain wrapping the EntityTable.
        """
        entity_table: EntityTable = context.get_entity_table(self.label)
        entity_table.variables_to_columns[self.variable] = (
            entity_table.column_name_to_hash[
                entity_table.entity_identifier_attribute
            ]
        )
        out: None | Filter = None
        for attr_name, attr_value in self.attributes.items():
            out = Filter(
                table=entity_table if not out else out,
                column_name_to_hash=entity_table.column_name_to_hash,
                hash_to_column_name=entity_table.hash_to_column_name,
                variables_to_columns=entity_table.variables_to_columns,
                condition=HasAttributeValue(
                    attribute=attr_name, value=attr_value
                ),
            )
        return out or entity_table

    def __str__(self) -> str:
        """Return a Cypher-like string representation of this node.
        
        Returns:
            str: Node representation in the format "(label:variable)".
        """
        return f"({self.label}:{self.variable})"


class DropColumn(Algebraic):
    """Algebraic operation to remove a column from a table.
    
    Implements the relational projection operation that excludes a specific column.
    The execute flag can be set to False to defer execution, which is useful when
    building up complex query plans.
    
    Attributes:
        table: The input table to drop a column from.
        column_name: The name of the column to drop.
        execute: Whether to actually execute the drop (default True).
    """
    table: Algebraic | EntityTable | Join | DropColumn | SelectColumns
    column_name: str
    execute: bool = True

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the column drop operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The input DataFrame with the specified column removed,
                or unchanged if execute=False or column doesn't exist.
        """
        df: pd.DataFrame = self.table.to_pandas(context)
        if not self.execute:
            return df
        if self.column_name in df.columns:
            df_dropped: pd.DataFrame = df.drop(columns=[self.column_name])
            return df_dropped
        else:
            return df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the column drop operation on an Ibis table.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: The input table with the specified column removed,
                or unchanged if execute=False or column doesn't exist.
        """
        table: IbisTable = self.table.to_ibis(context)
        if not self.execute:
            return table
        if self.column_name in table.columns:
            table_dropped: IbisTable = table.drop(self.column_name)
            return table_dropped
        else:
            return table


class SelectColumns(Algebraic):
    """Algebraic operation to select specific columns from a table.
    
    Implements the relational projection operation that keeps only the specified
    columns, discarding all others.
    
    Attributes:
        table: The input table to select columns from.
        column_names: List of (hashed) column names to keep.
    """
    table: EntityTable | Join | DropColumn | SelectColumns
    column_names: list[str]  # list of hashed column names

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the column selection.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: A DataFrame containing only the specified columns.
        """
        df: pd.DataFrame = self.table.to_pandas(context)
        selected_df: pd.DataFrame = df[self.column_names]  # pyrefly:ignore[bad-assignment]
        return selected_df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the column selection on an Ibis table.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: An Ibis table containing only the specified columns.
        """
        table: IbisTable = self.table.to_ibis(context)
        selected_table: IbisTable = table.select(self.column_names)
        return selected_table


class RelationshipConjunction(GraphObjectType):
    """Represents a conjunction of multiple relationships in a graph pattern.
    
    This class implements graph pattern matching by joining multiple relationship
    traversals together. Relationships are joined on their common variables,
    effectively representing connected paths through the graph.
    
    Attributes:
        relationships: List of relationships to conjoin (must have >= 2 elements).
    """
    relationships: List[Relationship]

    def _join_two_relationships(self, relationship_1_alg: Algebraic, relationship_2_alg: Algebraic) -> DropColumn:
        """Join two relationship algebraic expressions on their common variables.
        
        Identifies variables that appear in both relationships and performs an
        inner join on the corresponding columns. Drops duplicate columns from
        the right table after the join.
        
        Args:
            relationship_1_alg: The first relationship's algebraic expression.
            relationship_2_alg: The second relationship's algebraic expression.
            
        Returns:
            DropColumn: An algebraic expression representing the joined relationships.
            
        Raises:
            ValueError: If the relationships have no common variables (would result
                in a Cartesian product).
        """
        # Identify the variables for relationship_1 and relationship_2
        relationship_1_variables: set[str] = set(relationship_1_alg.variables_to_columns.keys())
        relationship_2_variables: set[str] = set(relationship_2_alg.variables_to_columns.keys())
        # Will join on the common_variables
        common_variables: set[str] = relationship_1_variables & relationship_2_variables
        if not common_variables:
            raise ValueError("Nobody likes a Cartesian product!")
        # Find the columns corresponding to the common variables in both relationships
        left_join_combos: List[str] = []
        right_join_combos: List[str] = []
        for var in common_variables:
            left_on: str = relationship_1_alg.variables_to_columns[var]
            right_on: str = relationship_2_alg.variables_to_columns[var]
            left_join_combos.append(left_on)
            right_join_combos.append(right_on)
        assert left_join_combos
        assert right_join_combos
        join: MultiJoin = MultiJoin(
            left=relationship_1_alg,
            right=relationship_2_alg,
            left_on=left_join_combos,
            right_on=right_join_combos,
            join_type=JoinType.INNER,
            variables_to_columns={**relationship_1_alg.variables_to_columns, **relationship_2_alg.variables_to_columns},
            column_name_to_hash={**relationship_1_alg.column_name_to_hash, **relationship_2_alg.column_name_to_hash},
            hash_to_column_name={**relationship_1_alg.hash_to_column_name, **relationship_2_alg.hash_to_column_name},
        )
        for duplicate_var in right_join_combos:
            dropped: DropColumn = DropColumn(
                table=join,
                column_name=duplicate_var,
                column_name_to_hash=join.column_name_to_hash,
                hash_to_column_name=join.hash_to_column_name,
                variables_to_columns=join.variables_to_columns,
                execute=False,  # Skip the Drop!
            )
        return dropped  # pyrefly:ignore[unbound-name]

    def to_algebra(self, context: Context) -> Algebraic:
        """Convert this relationship conjunction to an algebraic expression.
        
        Iteratively joins all relationships in the conjunction, starting with the
        first two and progressively adding each subsequent relationship.
        
        Args:
            context: The execution context containing table schemas.
            
        Returns:
            Algebraic: An algebraic expression representing all joined relationships.
            
        Raises:
            AssertionError: If fewer than 2 relationships are provided.
        """
        assert len(self.relationships) >= 2, "Need at least two relationships to form a conjunction"
        left_rel: Relationship = self.relationships[0]
        left_obj: RenameColumn | DropColumn= left_rel.to_algebra(context)
        for rel in self.relationships[1:]:
            rel_alg: RenameColumn | DropColumn = rel.to_algebra(context)
            conjoined: DropColumn = self._join_two_relationships(left_obj, rel_alg)
            left_obj = conjoined
        return conjoined  # pyrefly:ignore[unbound-name]


class MultiJoin(Algebraic):
    """Multi-column join operation between two tables.
    
    Performs a join on multiple column pairs simultaneously. This is used when
    joining relationships that share multiple variables, requiring all corresponding
    columns to match.
    
    Attributes:
        left: The left table in the join.
        right: The right table in the join.
        join_type: The type of join to perform (currently only INNER is implemented).
        left_on: List of column names from the left table to join on.
        right_on: List of column names from the right table to join on.
        variable_list: Optional list of variables involved in this join.
    """
    left: EntityTable | RelationshipTable | Filter | Join | Algebraic
    right: EntityTable | RelationshipTable | Filter | Join | Algebraic
    join_type: JoinType = JoinType.INNER
    left_on: List[str]
    right_on: List[str]
    variable_list: List[str] = []

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the multi-column join.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The result of joining the two tables on multiple columns.
            
        Raises:
            ValueError: If an unsupported join type is specified.
        """
        left_df: pd.DataFrame = self.left.to_pandas(context)
        right_df: pd.DataFrame = self.right.to_pandas(context)
        if self.join_type == JoinType.INNER:
            merged_df: pd.DataFrame = pd.merge(
                left_df,
                right_df,
                how="inner",
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=("_left", "_right"),
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the multi-column join on Ibis tables.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: The result of joining the two tables on multiple columns.
            
        Raises:
            ValueError: If an unsupported join type is specified.
        """
        left_table: IbisTable = self.left.to_ibis(context)
        right_table: IbisTable = self.right.to_ibis(context)
        if self.join_type == JoinType.INNER:
            # Build join predicates for multiple columns
            predicates = [
                left_table[left_col] == right_table[right_col]
                for left_col, right_col in zip(self.left_on, self.right_on)
            ]
            # Combine predicates with AND
            combined_predicate = predicates[0]
            for pred in predicates[1:]:
                combined_predicate = combined_predicate & pred
            merged_table: IbisTable = left_table.join(
                right_table,
                combined_predicate,
                how="inner"
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_table


class Join(Algebraic):
    """Single-column join operation between two tables.
    
    Performs a standard relational join on a single column pair. This is the
    fundamental operation for combining entity and relationship tables.
    
    Attributes:
        left: The left table in the join.
        right: The right table in the join.
        join_type: The type of join to perform (currently only INNER is implemented).
        left_on: The column name from the left table to join on.
        right_on: The column name from the right table to join on.
        variable_list: Optional list of variables involved in this join.
    """
    left: EntityTable | RelationshipTable | Filter | Join | Algebraic
    right: EntityTable | RelationshipTable | Filter | Join | Algebraic
    join_type: JoinType = JoinType.INNER
    left_on: str
    right_on: str
    variable_list: List[str] = []

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the join operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The result of joining the two tables.
            
        Raises:
            ValueError: If an unsupported join type is specified.
        """
        left_df: pd.DataFrame = self.left.to_pandas(context)
        right_df: pd.DataFrame = self.right.to_pandas(context)
        if self.join_type == JoinType.INNER:
            merged_df: pd.DataFrame = pd.merge(
                left_df,
                right_df,
                how="inner",
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=("_left", "_right"),
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the join operation on Ibis tables.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: The result of joining the two tables.
            
        Raises:
            ValueError: If an unsupported join type is specified.
        """
        left_table: IbisTable = self.left.to_ibis(context)
        right_table: IbisTable = self.right.to_ibis(context)
        if self.join_type == JoinType.INNER:
            merged_table: IbisTable = left_table.join(
                right_table,
                left_table[self.left_on] == right_table[self.right_on],
                how="inner"
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_table


class Filter(Algebraic):
    """Relational selection operation that filters rows based on a condition.
    
    Implements the relational selection operator (σ in relational algebra), which
    selects only those rows that satisfy a given boolean condition.
    
    Attributes:
        table: The table to filter.
        condition: The boolean condition to evaluate for each row.
    """
    table: Join | Filter | DropColumn | EntityTable | RelationshipTable
    condition: HasAttributeValue

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the filter operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: A DataFrame containing only rows that satisfy the condition.
            
        Raises:
            ValueError: If an unsupported condition type is provided.
        """
        df: pd.DataFrame = self.table.to_pandas(context)
        match self.condition:
            case HasAttributeValue():
                column_name: str = self.table.column_name_to_hash[
                    self.condition.attribute
                ]
                filtered_df: pd.DataFrame = df[  # pyrefly:ignore[bad-assignment]
                    df[column_name] == self.condition.value
                ]
            case _:
                raise ValueError(
                    f"Unsupported condition type: {type(self.condition)}"
                )
        return filtered_df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the filter operation on an Ibis table.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: An Ibis table containing only rows that satisfy the condition.
            
        Raises:
            ValueError: If an unsupported condition type is provided.
        """
        table: IbisTable = self.table.to_ibis(context)
        match self.condition:
            case HasAttributeValue():
                column_name: str = self.table.column_name_to_hash[
                    self.condition.attribute
                ]
                filtered_table: IbisTable = table.filter(
                    table[column_name] == self.condition.value
                )
            case _:
                raise ValueError(
                    f"Unsupported condition type: {type(self.condition)}"
                )
        return filtered_table


class Relationship(GraphObjectType):
    """Represents a directed relationship (edge) in a graph pattern.
    
    A relationship connects two nodes (source and target) and has a label indicating
    the relationship type. When converted to algebra, it becomes a series of joins
    between the source entity table, the relationship table, and the target entity table.
    
    Attributes:
        variable: The variable name to bind this relationship to.
        label: The relationship type/label (e.g., "LIVES_IN").
        attributes: Optional dictionary of attribute constraints (not currently used).
        source_node: The node at the source end of the relationship.
        target_node: The node at the target end of the relationship.
    """
    variable: str
    label: str
    attributes: Optional[dict] = None
    source_node: Node
    target_node: Node

    def to_algebra(self, context: Context) -> RenameColumn:
        """Convert this relationship to an algebraic expression.
        
        Creates a complex join pattern:
        1. Converts source and target nodes to algebra (EntityTable or Filter)
        2. Joins source node with relationship table on source_name
        3. Joins result with target node on target_name
        4. Drops the source_name and target_name columns
        5. Selects only relevant columns
        6. Renames relationship_id to a hashed column name
        
        Args:
            context: The execution context containing table schemas.
            
        Returns:
            RenameColumn: An algebraic expression representing the complete
                relationship traversal.
        """
        relationship_table: RelationshipTable = context.get_relationship_table(
            self.label
        )
        relationship_table.variables_to_columns[self.variable] = (
            "relationship_id"
        )
        source_table: EntityTable | Filter = self.source_node.to_algebra(
            context
        )
        target_table: EntityTable | Filter = self.target_node.to_algebra(
            context
        )
        left_join = Join(
            left=source_table,
            right=relationship_table,
            left_on=source_table.variables_to_columns[
                self.source_node.variable
            ],
            right_on="source_name",
            join_type=JoinType.INNER,
            variables_to_columns=source_table.variables_to_columns,
            column_name_to_hash=source_table.column_name_to_hash,
            hash_to_column_name=source_table.hash_to_column_name,
        )
        left_join.variables_to_columns[self.variable] = (
            relationship_table.variables_to_columns[self.variable]
        )  # TODO

        right_join = Join(
            left=left_join,
            right=target_table,
            left_on="target_name",
            right_on=target_table.variables_to_columns[
                self.target_node.variable
            ],
            join_type=JoinType.INNER,
            variables_to_columns={
                **left_join.variables_to_columns,
                **target_table.variables_to_columns,
            },
            column_name_to_hash={
                **left_join.column_name_to_hash,
                **target_table.column_name_to_hash,
            },
            hash_to_column_name={
                **left_join.hash_to_column_name,
                **target_table.hash_to_column_name,
            },
        )

        dropped: DropColumn = DropColumn(
            table=DropColumn(
                table=right_join,
                column_name_to_hash=right_join.column_name_to_hash,
                hash_to_column_name=right_join.hash_to_column_name,
                column_name="source_name",
                variables_to_columns=right_join.variables_to_columns,
            ),
            column_name="target_name",
            column_name_to_hash=right_join.column_name_to_hash,
            hash_to_column_name=right_join.hash_to_column_name,
            variables_to_columns=right_join.variables_to_columns,
        )

        selected: SelectColumns = SelectColumns(
            table=dropped,
            column_names=list(right_join.variables_to_columns.values())
            + ["relationship_id"],
            column_name_to_hash=dropped.column_name_to_hash,
            hash_to_column_name=dropped.hash_to_column_name,
            variables_to_columns=right_join.variables_to_columns,
        )

        new_column_name: str = random_hash()
        renamed: RenameColumn = RenameColumn(
            table=selected,
            old_column_name="relationship_id",
            new_column_name=new_column_name,
            column_name_to_hash=selected.column_name_to_hash,
            hash_to_column_name=selected.hash_to_column_name,
            variables_to_columns=selected.variables_to_columns,
        )

        renamed.column_name_to_hash[self.variable] = new_column_name
        renamed.hash_to_column_name[new_column_name] = self.variable
        renamed.variables_to_columns[self.variable] = new_column_name
        return renamed

    def __str__(self) -> str:
        """Return a Cypher-like string representation of this relationship.
        
        Returns:
            str: Relationship representation in the format
                "(source_var)-[:LABEL]->(target_var)".
        """
        return f"({self.source_node.variable})-[:{self.label}]->({self.target_node.variable})"


class RenameColumn(Algebraic):
    """Algebraic operation to rename a column in a table.
    
    Changes the name of a column while preserving all data. This is used to
    maintain consistent naming conventions and avoid conflicts in complex queries.
    
    Attributes:
        table: The table containing the column to rename.
        old_column_name: The current name of the column.
        new_column_name: The new name for the column.
        variables_to_columns: Mapping of variables to column names.
    """
    table: EntityTable | Join | DropColumn | SelectColumns
    old_column_name: str
    new_column_name: str
    variables_to_columns: dict[str, str] = {}

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the column rename operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The DataFrame with the renamed column.
        """
        df: pd.DataFrame = self.table.to_pandas(context)
        renamed_df: pd.DataFrame = df.rename(
            columns={self.old_column_name: self.new_column_name}
        )
        return renamed_df

    def to_ibis(self, context: Context) -> IbisTable:
        """Execute the column rename operation on an Ibis table.
        
        Args:
            context: The execution context.
            
        Returns:
            IbisTable: The Ibis table with the renamed column.
        """
        table: IbisTable = self.table.to_ibis(context)
        renamed_table: IbisTable = table.rename(
            {self.new_column_name: self.old_column_name}
        )
        return renamed_table



if __name__ == "__main__":
    # Define tables
    person_table: EntityTable = EntityTable(
        name="Person",
        entity_type="Person",
        attributes=["name", "age"],
        entity_identifier_attribute="name",
    )

    city_table = EntityTable(
        name="City",
        entity_type="City",
        attributes=["name", "population"],
        entity_identifier_attribute="name",
    )

    state_table = EntityTable(
        name="State",
        entity_type="State",
        attributes=["name", "mittenlike", "humid"],
        entity_identifier_attribute="name",
    )

    lives_in_table = RelationshipTable(
        name="LIVES_IN",
        relationship_type="LIVES_IN",
        source_entity_type="Person",
        target_entity_type="City",
        attributes=[],
    )
    
    city_in_state_table = RelationshipTable(
        name="CITY_IN_STATE",
        relationship_type="CITY_IN_STATE",
        source_entity_type="City",
        target_entity_type="State",
        attributes=[],
    )

    # Define nodes and relationship
    person_node: Node = Node(
        variable="p", label="Person", attributes={"name": "Alice"}
    )
    person_df = pd.DataFrame(
        data=[
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "carol", "age": 22},
        ]
    )

    city_node: Node = Node(variable="c", label="City", attributes={})
    city_df = pd.DataFrame(
        data=[
            {"name": "Cairo", "population": 100},
            {"name": "Alexandria", "population": 50},
        ],
    )
    
    state_node: Node = Node(variable="s", label="State", attributes={})
    state_df = pd.DataFrame(
        data=[
            {"name": "Georgia", "humid": True, "mittenlike": False},
            {"name": "Virginia", "humid": False, "mittenlike": False},
            {"name": "Michigan", "humid": False, "mittenlike": True},
        ],
    )

    lives_in: Relationship = Relationship(
        variable="livesin",
        label="LIVES_IN",
        source_node=person_node,
        target_node=city_node,
    )
    

    lives_in_df: pd.DataFrame = pd.DataFrame(
        data=[
            {
                "source_name": "Alice",
                "target_name": "Cairo",
                "relationship_id": "r1",
            },
            {
                "source_name": "Bob",
                "target_name": "Alexandria",
                "relationship_id": "r2",
            },
            {
                "source_name": "carol",
                "target_name": "Cairo",
                "relationship_id": "r3",
            },
        ],
    )
    
    city_in_state: Relationship = Relationship(
        variable="citystate",
        label="CITY_IN_STATE",
        source_node=city_node,
        target_node=state_node,
    )
    
    city_in_state_df: pd.DataFrame = pd.DataFrame(
        data=[
            {
                "source_name": "Cairo",
                "target_name": "Georgia",
                "relationship_id": "in1",
            },
            {
                "source_name": "Alexandria",
                "target_name": "Virginia",
                "relationship_id": "in2",
            },
            {
                "source_name": "Kalamazoo",
                "target_name": "Michigan",
                "relationship_id": "in3",
            },
        ],
    )

    context: Context = Context(
        entity_tables=[city_table, state_table, person_table],
        relationship_tables=[lives_in_table, city_in_state_table],
        obj_map={
            "Person": person_df,
            "City": city_df,
            "LIVES_IN": lives_in_df,
            "State": state_df,
            "CITY_IN_STATE": city_in_state_df,
        },
    )

    # lives_in_alg: Algebraic = lives_in.to_algebra(context)
    # rich.print(lives_in_alg)

    city_in_state_alg: Algebraic = city_in_state.to_algebra(context)
    rich.print(city_in_state_alg)

    relationship_conjunction: RelationshipConjunction = RelationshipConjunction(
        relationships=[lives_in, city_in_state]
    )
    conjunction_alg: Algebraic = relationship_conjunction.to_algebra(context)
    rich.print(conjunction_alg)
    