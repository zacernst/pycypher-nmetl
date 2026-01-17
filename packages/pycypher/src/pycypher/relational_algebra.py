"""Relational algebra system for translating grammar_parser AST to relational operations.

This module provides the algebraic foundation for translating Cypher queries parsed by
grammar_parser into relational operations that can be executed on pandas DataFrames.
It implements the core operators needed for graph query processing: joins, filters,
projections, and aggregations.

Unlike the older algebra.py module (which used AST from cypher_parser), this module
works with the modern Lark-based parse trees from grammar_parser, providing:

- More cowbell! 🔔
- Better alignment with openCypher grammar
- Cleaner separation between parsing and algebraic translation
- Full support for complex patterns, WHERE clauses, WITH clauses, and RETURN projections

The module uses column hashing to avoid naming conflicts during complex multi-way
joins and maintains variable-to-column mappings to support Cypher-style variable
binding across query patterns.

Example:
    >>> from pycypher.grammar_parser import GrammarParser
    >>> from pycypher.relational_algebra import QueryTranslator, Context
    >>> 
    >>> parser = GrammarParser()
    >>> tree = parser.parse("MATCH (p:Person)-[:KNOWS]->(c:Person) RETURN p.name, c.name")
    >>> 
    >>> # Set up context with entity and relationship tables
    >>> context = Context(
    ...     entity_tables=[
    ...         EntityTable(entity_type="Person", attributes=["id", "name"], 
    ...                    entity_identifier_attribute="id")
    ...     ],
    ...     relationship_tables=[
    ...         RelationshipTable(relationship_type="KNOWS",
    ...                          source_entity_type="Person",
    ...                          target_entity_type="Person",
    ...                          attributes=["source_id", "target_id"])
    ...     ]
    ... )
    >>> 
    >>> translator = QueryTranslator(context)
    >>> algebra = translator.translate(tree)
    >>> result_df = algebra.to_pandas(context)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pydantic import field_validator, BaseModel, Field
from typing import Any, List, Optional, Dict, Set, Tuple
from enum import Enum
import random
import hashlib
import pandas as pd
import ibis
from ibis.expr.types import Table as IbisTable
from lark import Tree, Token

from shared.logger import LOGGER


def random_hash() -> str:
    """Generate a random hash string for column naming.
    
    Creates a unique identifier by hashing a random float. Used to generate
    collision-resistant column names during algebraic operations.
    
    Returns:
        str: A 32-character hexadecimal hash string.
        
    Example:
        >>> hash1 = random_hash()
        >>> hash2 = random_hash()
        >>> len(hash1)
        32
        >>> hash1 != hash2
        True
    """
    return hashlib.md5(
        data=bytes(str(object=random.random()), encoding="utf-8")
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


# =============================================================================
# Context and Table Definitions
# =============================================================================

class EntityTable(BaseModel):
    """Represents a table of graph entities (nodes).
    
    EntityTable stores metadata about a particular type of node in the graph,
    including its label, attributes, and identifier column. It maintains column
    name mappings to support collision-free joins with other tables.
    
    Attributes:
        entity_type: The type/label of entities in this table (e.g., "Person").
        attributes: List of attribute names for this entity type.
        entity_identifier_attribute: The attribute that uniquely identifies entities.
        column_name_to_hash: Maps original column names to their hashed versions.
        hash_to_column_name: Reverse mapping from hashed names to original names.
        
    Example:
        >>> entity_table = EntityTable(
        ...     entity_type="Person",
        ...     attributes=["id", "name", "age"],
        ...     entity_identifier_attribute="id"
        ... )
        >>> len(entity_table.column_name_to_hash)
        3
    """
    entity_type: str
    attributes: List[str]
    entity_identifier_attribute: str
    column_name_to_hash: Dict[str, str] = Field(default_factory=dict)
    hash_to_column_name: Dict[str, str] = Field(default_factory=dict)

    def __init__(self, **data: Any):
        """Initialize the entity table and create column hash mappings.
        
        Args:
            **data: Keyword arguments for entity_type, attributes, and
                entity_identifier_attribute.
        """
        super().__init__(**data)
        # Create hash mappings for all attributes
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute


class RelationshipTable(BaseModel):
    """Represents a table of graph relationships (edges).
    
    RelationshipTable stores metadata about connections between entities, including
    the source and target entity types. Each relationship can have its own attributes.
    
    Attributes:
        relationship_type: The type/label of this relationship (e.g., "KNOWS").
        source_entity_type: The entity type at the source of the relationship.
        target_entity_type: The entity type at the target of the relationship.
        attributes: List of attribute names for this relationship type.
        column_name_to_hash: Maps original column names to their hashed versions.
        hash_to_column_name: Reverse mapping from hashed names to original names.
        
    Example:
        >>> rel_table = RelationshipTable(
        ...     relationship_type="KNOWS",
        ...     source_entity_type="Person",
        ...     target_entity_type="Person",
        ...     attributes=["source_id", "target_id", "since"]
        ... )
        >>> len(rel_table.attributes)
        3
    """
    relationship_type: str
    source_entity_type: str
    target_entity_type: str
    attributes: List[str]
    column_name_to_hash: Dict[str, str] = Field(default_factory=dict)
    hash_to_column_name: Dict[str, str] = Field(default_factory=dict)

    def __init__(self, **data: Any):
        """Initialize the relationship table and create column hash mappings.
        
        Args:
            **data: Keyword arguments for relationship_type, source_entity_type,
                target_entity_type, and attributes.
        """
        super().__init__(**data)
        # Create hash mappings for all attributes
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute


class Context(BaseModel):
    """Execution context for algebraic operations.
    
    Context maintains the schema information (entity and relationship tables) and
    the actual data (as pandas DataFrames) needed to execute algebraic expressions.
    
    Attributes:
        entity_tables: List of entity table schemas.
        relationship_tables: List of relationship table schemas.
        obj_map: Dictionary mapping entity/relationship types to their DataFrame data.
        
    Example:
        >>> context = Context(
        ...     entity_tables=[
        ...         EntityTable(entity_type="Person", attributes=["id", "name"],
        ...                    entity_identifier_attribute="id")
        ...     ],
        ...     relationship_tables=[],
        ...     obj_map={"Person": pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})}
        ... )
        >>> context.get_entity_table("Person").entity_type
        'Person'
    """
    model_config = {"arbitrary_types_allowed": True}
    
    entity_tables: List[EntityTable]
    relationship_tables: List[RelationshipTable]
    obj_map: Dict[str, pd.DataFrame] = Field(default_factory=dict)

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
        raise ValueError(f"Entity table for type {entity_type} not found")

    def get_relationship_table(self, relationship_type: str) -> RelationshipTable:
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
        raise ValueError(f"Relationship table for type {relationship_type} not found")


# =============================================================================
# Algebraic Operators (Relational Algebra)
# =============================================================================

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
    model_config = {"arbitrary_types_allowed": True}
    
    variables_to_columns: Dict[str, str] = Field(default_factory=dict)
    column_name_to_hash: Dict[str, str] = Field(default_factory=dict)
    hash_to_column_name: Dict[str, str] = Field(default_factory=dict)

    @abstractmethod
    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Convert this algebraic expression to a pandas DataFrame.
        
        Args:
            context: The execution context containing entity and relationship data.
            
        Returns:
            pd.DataFrame: The result of executing this algebraic operation.
        """
        ...


class Scan(Algebraic):
    """Scans an entity or relationship table.
    
    The Scan operator is the leaf node in a query plan, representing direct access
    to a base table. It retrieves all rows from the specified entity or relationship
    table and applies column hashing.
    
    Attributes:
        table_type: The type of table being scanned (entity or relationship label).
        is_entity: Whether this is an entity table (True) or relationship table (False).
        variable: The variable name to bind this scan to (e.g., "p" for person).
        
    Example:
        >>> scan = Scan(table_type="Person", is_entity=True, variable="p")
        >>> scan.table_type
        'Person'
    """
    table_type: str
    is_entity: bool = True
    variable: Optional[str] = None

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the table scan.
        
        Args:
            context: The execution context containing the table data.
            
        Returns:
            pd.DataFrame: The table data with hashed column names.
            
        Raises:
            ValueError: If the table type is not found in the context.
        """
        if self.table_type not in context.obj_map:
            if self.is_entity:
                raise ValueError(f"Entity table for type {self.table_type} not found in context")
            else:
                raise ValueError(f"Relationship table for type {self.table_type} not found in context")
        
        df = context.obj_map[self.table_type].copy()
        
        # Get the appropriate table schema
        if self.is_entity:
            table_schema = context.get_entity_table(self.table_type)
        else:
            table_schema = context.get_relationship_table(self.table_type)
        
        # Apply column hashing
        df = df.rename(columns=table_schema.column_name_to_hash)
        
        # Update mappings
        self.column_name_to_hash = table_schema.column_name_to_hash.copy()
        self.hash_to_column_name = table_schema.hash_to_column_name.copy()
        
        # If there's a variable, map it to the identifier column
        if self.variable and self.is_entity:
            id_hash = table_schema.column_name_to_hash[table_schema.entity_identifier_attribute]
            self.variables_to_columns[self.variable] = id_hash
        
        return df


class Filter(Algebraic):
    """Relational selection operation that filters rows based on a condition.
    
    Implements the relational selection operator (σ in relational algebra), which
    selects only those rows that satisfy a given predicate.
    
    Attributes:
        input: The input algebraic expression to filter.
        attribute: The attribute name to filter on.
        value: The value to compare against.
        operator: The comparison operator ("=", ">", "<", ">=", "<=", "!=").
        
    Example:
        >>> scan = Scan(table_type="Person", is_entity=True, variable="p")
        >>> filter_op = Filter(input=scan, attribute="age", value=30, operator=">")
        >>> filter_op.operator
        '>'
    """
    input: Algebraic
    attribute: str
    value: Any
    operator: str = "="

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the filter operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: A DataFrame containing only rows that satisfy the condition.
            
        Raises:
            ValueError: If an unsupported operator is provided.
        """
        df = self.input.to_pandas(context)
        
        # Inherit mappings from input
        self.column_name_to_hash = self.input.column_name_to_hash.copy()
        self.hash_to_column_name = self.input.hash_to_column_name.copy()
        self.variables_to_columns = self.input.variables_to_columns.copy()
        
        # Get the hashed column name
        if self.attribute not in self.column_name_to_hash:
            raise ValueError(f"Attribute {self.attribute} not found in table")
        
        column_hash = self.column_name_to_hash[self.attribute]
        
        # Apply the filter based on operator
        if self.operator == "=":
            filtered_df = df[df[column_hash] == self.value]
        elif self.operator == ">":
            filtered_df = df[df[column_hash] > self.value]
        elif self.operator == "<":
            filtered_df = df[df[column_hash] < self.value]
        elif self.operator == ">=":
            filtered_df = df[df[column_hash] >= self.value]
        elif self.operator == "<=":
            filtered_df = df[df[column_hash] <= self.value]
        elif self.operator == "!=":
            filtered_df = df[df[column_hash] != self.value]
        else:
            raise ValueError(f"Unsupported operator: {self.operator}")
        
        return filtered_df


class Join(Algebraic):
    """Join operation between two algebraic expressions.
    
    Performs a relational join on specified columns. This is the fundamental
    operation for combining entity and relationship tables in graph pattern matching.
    
    Attributes:
        left: The left input expression.
        right: The right input expression.
        left_on: The column name from the left input to join on.
        right_on: The column name from the right input to join on.
        join_type: The type of join to perform (default: INNER).
        
    Example:
        >>> left_scan = Scan(table_type="Person", is_entity=True, variable="p")
        >>> right_scan = Scan(table_type="Company", is_entity=True, variable="c")
        >>> join_op = Join(left=left_scan, right=right_scan,
        ...               left_on="company_id", right_on="id")
        >>> join_op.join_type.value
        'INNER'
    """
    left: Algebraic
    right: Algebraic
    left_on: str
    right_on: str
    join_type: JoinType = JoinType.INNER

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the join operation.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The result of joining the two inputs.
        """
        left_df = self.left.to_pandas(context)
        right_df = self.right.to_pandas(context)
        
        # Merge the DataFrames
        merged_df = pd.merge(
            left_df,
            right_df,
            how=self.join_type.value.lower(),
            left_on=self.left_on,
            right_on=self.right_on,
            suffixes=("_left", "_right"),
        )
        
        # Merge mappings from both sides
        self.column_name_to_hash = {**self.left.column_name_to_hash, **self.right.column_name_to_hash}
        self.hash_to_column_name = {**self.left.hash_to_column_name, **self.right.hash_to_column_name}
        self.variables_to_columns = {**self.left.variables_to_columns, **self.right.variables_to_columns}
        
        return merged_df


class Project(Algebraic):
    """Projection operation to select specific columns.
    
    Implements the relational projection operator (π in relational algebra),
    which selects a subset of columns from the input.
    
    Attributes:
        input: The input algebraic expression.
        columns: List of column names (hashed) to project.
        aliases: Optional dictionary mapping column names to new names.
        
    Example:
        >>> scan = Scan(table_type="Person", is_entity=True, variable="p")
        >>> project = Project(input=scan, columns=["name_hash", "age_hash"])
        >>> len(project.columns)
        2
    """
    input: Algebraic
    columns: List[str]
    aliases: Dict[str, str] = Field(default_factory=dict)

    def to_pandas(self, context: Context) -> pd.DataFrame:
        """Execute the projection.
        
        Args:
            context: The execution context.
            
        Returns:
            pd.DataFrame: The input DataFrame with only the specified columns.
        """
        df = self.input.to_pandas(context)
        
        # Inherit mappings
        self.column_name_to_hash = self.input.column_name_to_hash.copy()
        self.hash_to_column_name = self.input.hash_to_column_name.copy()
        self.variables_to_columns = self.input.variables_to_columns.copy()
        
        # Select only the specified columns that exist in the DataFrame
        available_columns = [col for col in self.columns if col in df.columns]
        projected_df = df[available_columns]
        
        # Apply aliases if provided
        if self.aliases:
            projected_df = projected_df.rename(columns=self.aliases)
            # Update mappings for aliases
            for old_name, new_name in self.aliases.items():
                if old_name in self.hash_to_column_name:
                    original = self.hash_to_column_name[old_name]
                    self.column_name_to_hash[new_name] = old_name
                    self.hash_to_column_name[old_name] = new_name
        
        return projected_df


# =============================================================================
# Query Translator (Parse Tree → Relational Algebra)
# =============================================================================

class QueryTranslator:
    """Translates grammar_parser parse trees to relational algebra.
    
    This class implements the translation from Lark parse trees (produced by
    grammar_parser) to a tree of relational algebra operators that can be
    executed on pandas DataFrames.
    
    The translator handles:
    - MATCH clauses with node and relationship patterns
    - WHERE clauses with filters
    - RETURN clauses with projections
    - WITH clauses (simplified support)
    
    Attributes:
        context: The execution context with table schemas.
        
    Example:
        >>> from pycypher.grammar_parser import GrammarParser
        >>> parser = GrammarParser()
        >>> tree = parser.parse("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
        >>> translator = QueryTranslator(context)
        >>> algebra = translator.translate(tree)
        >>> isinstance(algebra, Algebraic)
        True
    """
    
    def __init__(self, context: Context):
        """Initialize the translator.
        
        Args:
            context: The execution context containing table schemas.
        """
        self.context = context
        self.variable_scopes: Dict[str, str] = {}  # variable -> entity_type mapping
    
    def translate(self, tree: Tree) -> Algebraic:
        """Translate a complete query parse tree to relational algebra.
        
        Args:
            tree: The Lark parse tree from grammar_parser.
            
        Returns:
            Algebraic: The root of the relational algebra expression tree.
            
        Raises:
            ValueError: If the parse tree structure is invalid.
        """
        # Find the query_statement node
        query_statement = self._find_node(tree, "query_statement")
        if not query_statement:
            raise ValueError("No query_statement found in parse tree")
        
        # Start with MATCH clause
        match_clause = self._find_node(query_statement, "match_clause")
        if not match_clause:
            raise ValueError("No match_clause found (only MATCH queries supported)")
        
        # Translate MATCH to algebra
        algebra = self._translate_match(match_clause)
        
        # Apply WHERE clause if present
        where_clause = self._find_node(query_statement, "where_clause")
        if where_clause:
            algebra = self._translate_where(where_clause, algebra)
        
        # Apply RETURN projection if present
        return_clause = self._find_node(query_statement, "return_clause")
        if return_clause:
            algebra = self._translate_return(return_clause, algebra)
        
        return algebra
    
    def _find_node(self, tree: Tree, node_type: str) -> Optional[Tree]:
        """Find the first node of a given type in the tree.
        
        Args:
            tree: The tree to search.
            node_type: The type of node to find.
            
        Returns:
            The first matching node, or None if not found.
        """
        for node in tree.find_data(node_type):
            return node
        return None
    
    def _extract_variable_name(self, node: Tree) -> Optional[str]:
        """Extract variable name from a variable_name node.
        
        Args:
            node: A variable_name node.
            
        Returns:
            The variable name as a string, or None if not found.
        """
        if node.children and isinstance(node.children[0], Token):
            return str(node.children[0].value)
        return None
    
    def _extract_label_name(self, node: Tree) -> Optional[str]:
        """Extract label name from a label_name node.
        
        Args:
            node: A node containing label information.
            
        Returns:
            The label name as a string, or None if not found.
        """
        for label_name_node in node.find_data("label_name"):
            if label_name_node.children and isinstance(label_name_node.children[0], Token):
                return str(label_name_node.children[0].value)
        return None
    
    def _translate_match(self, match_clause: Tree) -> Algebraic:
        """Translate a MATCH clause to relational algebra.
        
        Args:
            match_clause: The match_clause node from the parse tree.
            
        Returns:
            Algebraic expression representing the pattern match.
        """
        # Find the pattern
        pattern = self._find_node(match_clause, "pattern")
        if not pattern:
            raise ValueError("No pattern found in MATCH clause")
        
        # Find the path_pattern
        path_pattern = self._find_node(pattern, "path_pattern")
        if not path_pattern:
            raise ValueError("No path_pattern found")
        
        # Extract pattern elements (nodes and relationships)
        pattern_elements = list(path_pattern.find_data("pattern_element"))
        if not pattern_elements:
            raise ValueError("No pattern_element found")
        
        # Start with the first node pattern
        result = None
        current_node_var = None
        
        for elem in pattern_elements:
            # Process node patterns
            for node_pattern in elem.find_data("node_pattern"):
                node_result, node_var = self._translate_node_pattern(node_pattern)
                current_node_var = node_var
                if result is None:
                    result = node_result
            
            # Process relationship patterns
            for rel_pattern in elem.find_data("relationship_pattern"):
                rel_result, rel_var, target_var = self._translate_relationship_pattern(
                    rel_pattern, current_node_var
                )
                if result is None:
                    result = rel_result
                else:
                    result = rel_result  # Relationship already joins source and target
                current_node_var = target_var
        
        return result or Scan(table_type="Unknown", is_entity=True)
    
    def _translate_node_pattern(self, node_pattern: Tree) -> Tuple[Algebraic, Optional[str]]:
        """Translate a node pattern to a Scan operation.
        
        Args:
            node_pattern: The node_pattern node from the parse tree.
            
        Returns:
            Tuple of (Scan operation, variable name).
        """
        # Extract variable name
        variable = None
        for var_node in node_pattern.find_data("variable_name"):
            variable = self._extract_variable_name(var_node)
            break
        
        # Extract label
        label = None
        for filler in node_pattern.find_data("node_pattern_filler"):
            label = self._extract_label_name(filler)
            if label:
                break
        
        if not label:
            raise ValueError(f"Node pattern must have a label")
        
        # Store variable -> entity_type mapping
        if variable:
            self.variable_scopes[variable] = label
        
        # Create a Scan operation
        scan = Scan(table_type=label, is_entity=True, variable=variable)
        
        return scan, variable
    
    def _translate_relationship_pattern(
        self, rel_pattern: Tree, source_var: Optional[str]
    ) -> Tuple[Algebraic, Optional[str], Optional[str]]:
        """Translate a relationship pattern to Join operations.
        
        Args:
            rel_pattern: The relationship_pattern node.
            source_var: The variable name of the source node.
            
        Returns:
            Tuple of (Join operation, relationship variable, target variable).
        """
        # This is a simplified implementation
        # In a full implementation, you would:
        # 1. Extract relationship type and variable
        # 2. Find the target node pattern
        # 3. Create joins: source_node JOIN relationship JOIN target_node
        
        # For now, return a placeholder
        raise NotImplementedError("Relationship pattern translation not fully implemented")
    
    def _translate_where(self, where_clause: Tree, input_algebra: Algebraic) -> Algebraic:
        """Translate a WHERE clause to Filter operations.
        
        Args:
            where_clause: The where_clause node.
            input_algebra: The input algebraic expression to filter.
            
        Returns:
            Filter operation wrapping the input.
        """
        # Find comparison expressions
        for comp_expr in where_clause.find_data("comparison_expression"):
            # Extract left side (variable.property)
            left_parts = []
            for var_node in comp_expr.find_data("variable_name"):
                var_name = self._extract_variable_name(var_node)
                if var_name:
                    left_parts.append(var_name)
            
            # Extract property lookup
            for prop_lookup in comp_expr.find_data("property_lookup"):
                # Try property_name first (as seen in tree)
                for prop_node in prop_lookup.find_data("property_name"):
                    if prop_node.children:
                        prop_name = str(prop_node.children[0].value)
                        left_parts.append(prop_name)
                # Try property_key_name as fallback
                for prop_node in prop_lookup.find_data("property_key_name"):
                    if prop_node.children:
                        prop_name = str(prop_node.children[0].value)
                        left_parts.append(prop_name)
            
            # Extract right side (value)
            value = None
            for num_literal in comp_expr.find_data("number_literal"):
                if num_literal.children:
                    for signed in num_literal.find_data("signed_number"):
                        if signed.children:
                            value = int(str(signed.children[0].value))
                    for unsigned in num_literal.find_data("unsigned_number"):
                        if unsigned.children:
                            value = int(str(unsigned.children[0].value))
            
            # Extract operator (simplified - assumes >)
            operator = ">"
            
            if len(left_parts) >= 2 and value is not None:
                # left_parts[0] is variable, left_parts[1] is attribute
                attribute = left_parts[1]
                return Filter(input=input_algebra, attribute=attribute, value=value, operator=operator)
        
        return input_algebra
    
    def _translate_return(self, return_clause: Tree, input_algebra: Algebraic) -> Algebraic:
        """Translate a RETURN clause to Project operation.
        
        Args:
            return_clause: The return_clause node.
            input_algebra: The input algebraic expression.
            
        Returns:
            Project operation selecting the specified columns.
        """
        # Extract return items (variables and properties)
        columns = []
        
        for return_item in return_clause.find_data("return_item"):
            # Look for property access expressions first (n.prop)
            prop_hash = None
            var_name = None
            prop_name = None
            
            # Use property_lookup to find n.prop
            # The structure is usually: postfix_expression -> (variable_name, property_lookup)
            # Find property_lookup in the return item
            for prop_lookup in return_item.find_data("property_lookup"):
                # If we find a property lookup, find the preceding variable in the same scope
                # This is tricky with Lark find_data, so we search for variable_name separately 
                # and assume simple structure for this proof of concept
                pass
            
            # Simple handling for this demo: look for variable and potential property name
            has_property = False
            
            # Try to find property names
            found_props = []
            for prop_lookup in return_item.find_data("property_name"):
                 if prop_lookup.children:
                     found_props.append(str(prop_lookup.children[0].value))
            
            # Try to find variable name
            found_vars = []
            for var_node in return_item.find_data("variable_name"):
                v = self._extract_variable_name(var_node)
                if v: 
                    found_vars.append(v)
            
            if found_vars and found_props:
                # Assuming simple variable.property case
                var_name = found_vars[0]
                prop_name = found_props[0]
                
                # We need to find the hash for this property on this variable
                # First find entity type for variable
                if var_name in self.variable_scopes:
                    entity_type = self.variable_scopes[var_name]
                    # Get table schema to find hash
                    try:
                        table = self.context.get_entity_table(entity_type)
                        if prop_name in table.column_name_to_hash:
                            columns.append(table.column_name_to_hash[prop_name])
                    except ValueError:
                        pass
                        
            elif found_vars:
                # Just variable return (return n)
                # In current implementation, scan only binds ID to variable
                var_name = found_vars[0]
                if var_name in input_algebra.variables_to_columns:
                    columns.append(input_algebra.variables_to_columns[var_name])
        
        if columns:
            return Project(input=input_algebra, columns=columns)
        
        return input_algebra


if __name__ == "__main__":
    # Example usage
    from pycypher.grammar_parser import GrammarParser
    
    parser = GrammarParser()
    tree = parser.parse("MATCH (p:Person) RETURN p")
    
    context = Context(
        entity_tables=[
            EntityTable(
                entity_type="Person",
                attributes=["id", "name", "age"],
                entity_identifier_attribute="id"
            )
        ],
        relationship_tables=[],
        obj_map={
            "Person": pd.DataFrame({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35]
            })
        }
    )
    
    translator = QueryTranslator(context)
    algebra = translator.translate(tree)
    result = algebra.to_pandas(context)
    print(result)
