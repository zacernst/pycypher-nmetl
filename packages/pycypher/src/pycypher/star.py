from __future__ import annotations
import copy
from typing import Optional, Any, cast
import rich
from shared.logger import LOGGER
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import (
    random_hash,
    ASTConverter,
    RelationshipDirection,
    Pattern,
    PatternPath,
    PatternIntersection,
    Variable,
    RelationshipPattern,
    NodePattern,
    Algebraizable,
    Set,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    flatten,
    VariableMap,
    VariableTypeMap,
    Context,
    Relation,
    EntityTable,
    RelationIntersection,
    RelationshipTable,
    Projection,
    JoinType,
    Join,
    SelectColumns,
    FilterRows,
    AttributeEqualsValue,
    ExpressionProjection,
    Aggregation,
    GroupedAggregation,
    ColumnName,
)
import pandas as pd


class Star:
    """Translation operator."""

    def __init__(self, context: Context = Context()) -> None:
        self.context: Context = context
        # Track variable-to-type mappings during pattern processing
        self.variable_type_registry: dict[Variable, str] = {}

    def _extract_variable_types(self, pattern: Pattern) -> None:
        """Extract variable-to-type mappings from all NodePatterns in a Pattern."""
        for path in pattern.paths:
            for element in path.elements:
                if isinstance(element, NodePattern) and element.labels and element.variable:
                    # Record the first label as the variable's type
                    if element.variable not in self.variable_type_registry:
                        self.variable_type_registry[element.variable] = element.labels[0]
                        LOGGER.debug(msg=f"Registered variable {element.variable} as type {element.labels[0]}")

    def to_relation(self, obj: Algebraizable | str) -> Relation:
        """Convert the object to a Relation. Recursively handles different AST node types."""
        LOGGER.debug(msg=f"Starting to_relation conversion for {obj}.")
        match obj:
            case str():
                pattern_obj: Pattern = self.to_relation(
                    ASTConverter().from_cypher(obj)
                )
            case NodePattern(variable=_, labels=_, properties=properties) if (
                len(properties) == 0  # (n:Thing)
            ):
                LOGGER.debug(msg="Translating NodePattern with no properties.")
                out: Projection = self._from_node_pattern(node=obj)
            case NodePattern(variable=_, labels=_, properties=properties) if (
                len(properties) >= 1  # (n:Thing {prop1: val1})
            ):
                LOGGER.debug(
                    msg="Translating NodePattern with one or more properties."
                )
                out: FilterRows = self._from_node_pattern_with_attrs(node=obj)
            case RelationshipPattern(  # -[r:KNOWS]->
                variable=_, labels=_, properties=properties
            ) if len(properties) == 0:
                LOGGER.debug(
                    msg="Translating RelationshipPattern with no properties."
                )
                out: RelationshipTable = self._from_relationship_pattern(
                    relationship=obj
                )
            case RelationshipPattern(
                variable=_, labels=_, properties=properties
            ) if len(properties) >= 1:
                LOGGER.debug(
                    msg=f"Translating RelationshipPattern with {len(properties)} properties."
                )
                out: FilterRows = self._from_relationship_pattern_with_attrs(
                    relationship=obj
                )
            case Pattern():
                LOGGER.debug(msg="Translating Pattern.")
                # Extract variable types before processing
                self._extract_variable_types(pattern=obj)
                out: Relation = self._from_pattern(pattern=obj)
            case PatternPath() as pattern_path:  # (p1)-[r:KNOWS]->(p2)
                LOGGER.debug(msg="Translating PatternPath.")
                out: Relation = self._from_pattern_path(
                    pattern_path=pattern_path
                )

            case _:
                raise NotImplementedError(
                    f"Translation for {type(obj)} is not implemented."
                )
        # Record the original `obj` in the Relation for traceability
        out.source_algebraizable: Algebraizable = obj
        return out

    def _from_pattern(self, pattern: Pattern) -> Relation:
        """Break down the `Pattern` into NodePattern objects and pairs of NodePattern and RelationshipPattern.
        Reverse the directions of RelationshipPatterns to reduce the number of cases. Translate each element
        of the Pattern, then join them pairwise.
        """
        elements: list[Relation] = [
            self.to_relation(obj=element) for element in pattern.paths
        ]
        if not elements:
            raise ValueError("PatternPath must have at least one element.")
        elif len(elements) == 1:
            LOGGER.debug(msg="Pattern has only one element, no joins needed.")
            return elements[0]
        else:  # Two or more elements
            LOGGER.debug(
                msg=f"Pattern has {len(elements)} elements, performing pairwise joins."
            )
            pairwise_joined_relations: list[Relation] = []
            for index, first_element in enumerate(elements[:-1]):
                LOGGER.debug(msg=f"Element {index}: {first_element}")
                second_element: Relation = elements[index + 1]
                LOGGER.debug(msg=f"Element {index + 1}: {second_element}")
                pairwise_joined_relations.append(
                    self._binary_join(left=first_element, right=second_element)
                )

        out: Relation = self._from_intersection_list(
            relations=pairwise_joined_relations
        )
        return out

    def _from_pattern_path(self, pattern_path: PatternPath) -> Relation:
        """Convert a PatternPath to a Relation."""
        # Convert PatternPath to a list of Relationship/Node
        elements: list[Relation] = [
            self.to_relation(obj=element) for element in pattern_path.elements
        ]
        LOGGER.debug(
            msg=f"Decomposed PatternPath into {len(elements)} elements."
        )
        LOGGER.debug(msg=f"Elements: {'\n'.join(str(e) for e in elements)}")
        if not elements:
            raise ValueError("PatternPath must have at least one element.")
        elif len(elements) == 1:
            return elements[0]
        else:  # Two or more elements
            pairwise_joined_relations: list[Relation] = []
            for index, first_element in enumerate(elements[:-1]):
                second_element: Relation = elements[index + 1]
                LOGGER.debug(msg=f"Joining element {index} and {index + 1}.")

                match (
                    first_element.source_algebraizable,
                    second_element.source_algebraizable,
                ):
                    case (
                        NodePattern() as node_pattern,
                        RelationshipPattern() as relationship_pattern,
                    ):
                        normalized_pair = (node_pattern, relationship_pattern)
                        joiner = (
                            self._from_node_relationship_tail
                            if relationship_pattern.direction
                            == RelationshipDirection.RIGHT
                            else self._from_node_relationship_head
                        )
                    case (
                        RelationshipPattern() as relationship_pattern,
                        NodePattern() as node_pattern,
                    ):
                        normalized_pair = (node_pattern, relationship_pattern)
                        joiner = (
                            self._from_node_relationship_tail
                            if relationship_pattern.direction
                            == RelationshipDirection.LEFT
                            else self._from_node_relationship_head
                        )
                    case _:
                        raise ValueError(
                            "Unexpected pair of elements in PatternPath: "
                            f"{type(first_element.source_algebraizable)} and "
                            f"{type(second_element.source_algebraizable)}. "
                            "Expected NodePattern and RelationshipPattern."
                        )

                node_pattern, relationship_pattern = normalized_pair

                # joiner = (
                #     self._from_node_relationship_head
                #     if relationship_pattern.direction == RelationshipDirection.RIGHT
                #     else self._from_node_relationship_tail
                # )

                pairwise_joined_relations.append(
                    joiner(
                        node=node_pattern, relationship=relationship_pattern
                    )
                )
            relation_intersection: RelationIntersection = RelationIntersection(
                relation_list=pairwise_joined_relations
            )
            # join_variables = relation_intersection.variables_in_common()
            # Each node/relationship pair has been Joined. Now we need to perform a final Join across all pairs.
            # Start with the first element, then iteratively join the next to it.
            joined_relation = relation_intersection.relation_list[0]
            for relation in relation_intersection.relation_list[1:]:
                join_variables = RelationIntersection(
                    relation_list=[joined_relation, relation]
                ).variables_in_common()
                left_join_on_map = {
                    join_variable: joined_relation.variable_map[join_variable]
                    for join_variable in join_variables
                }
                right_join_on_map = {
                    join_variable: relation.variable_map[join_variable]
                    for join_variable in join_variables
                }
                joined_relation: Join = Join(
                    left=joined_relation,
                    right=relation,
                    join_type=JoinType.INNER,
                    on_left=list(left_join_on_map.values()),
                    on_right=list(right_join_on_map.values()),
                    source_algebraizable=pattern_path,  # Wrong, strictly speaking
                    variable_map={
                        **joined_relation.variable_map,
                        **relation.variable_map,
                    },
                    variable_type_map={
                        **joined_relation.variable_type_map,
                        **relation.variable_type_map,
                    },
                    column_names=joined_relation.column_names
                    + relation.column_names,
                )
            # Remove all columns except variables
            projected_joined_relation = Projection(
                relation=joined_relation,
                projected_column_names={
                    column_name: column_name
                    for column_name in joined_relation.variable_map.values()
                },
                source_algebraizable=joined_relation.source_algebraizable,
                variable_map=copy.deepcopy(joined_relation.variable_map),
                variable_type_map=copy.deepcopy(
                    joined_relation.variable_type_map
                ),
                column_names=[
                    column_name
                    for column_name in joined_relation.variable_map.values()
                ],
            )
            return projected_joined_relation

    def _from_node_relationship_tail(
        self, node: NodePattern, relationship: RelationshipPattern
    ) -> Relation:
        """Convert a NodePattern and adjacent RelationshipPattern with direction (n)-[r]-> to a Relation."""
        # First convert the Node and Relationship separately
        node_relation: Relation = self._from_node_pattern(node=node)
        relationship_relation: Relation = self._from_relationship_pattern(
            relationship=relationship
        )

        # Then perform a binary join on the Node and Relationship using the appropriate keys based on the relationship direction
        node_variable = node.variable
        node_variable_column = node_relation.variable_map[node_variable]
        relationship_variable = relationship.variable
        relationship_variable_column = relationship_relation.variable_map[
            relationship_variable
        ]
        
        # Look up the actual source column from the relationship Projection's
        # projected_column_names (which maps input→output column names).
        # This works with the unique-hash naming scheme.
        rel_label = relationship.labels[0]
        rel_source_input = f"{rel_label}__{RELATIONSHIP_SOURCE_COLUMN}"
        rel_source_column = relationship_relation.projected_column_names[rel_source_input]

        joined_relation: Relation = Join(
            left=node_relation,
            right=relationship_relation,
            source_algebraizable=PatternIntersection(
                pattern_list=[node, relationship]
            ),
            on_left=[node_variable_column],
            on_right=[rel_source_column],
            join_type=JoinType.INNER,
            variable_map={
                **node_relation.variable_map,
                **relationship_relation.variable_map,
            },
            variable_type_map={
                **node_relation.variable_type_map,
                **relationship_relation.variable_type_map,
            },
            column_names=list(node_relation.variable_map.values())
            + list(relationship_relation.variable_map.values()),
        )

        return joined_relation

    def _from_node_relationship_head(
        self, node: NodePattern, relationship: RelationshipPattern
    ) -> Relation:
        """Convert a NodePattern and adjacent RelationshipPattern with direction -[r]->(n) to a Relation."""
        # First convert the Node and Relationship separately
        node_relation: Relation = self._from_node_pattern(node=node)
        relationship_relation: Relation = self._from_relationship_pattern(
            relationship=relationship
        )

        # Then perform a binary join on the Node and Relationship using the appropriate keys based on the relationship direction
        node_variable = node.variable
        node_variable_column = node_relation.variable_map[node_variable]
        relationship_variable = relationship.variable
        relationship_variable_column = relationship_relation.variable_map[
            relationship_variable
        ]
        
        # Look up the actual target column from the relationship Projection's
        # projected_column_names (which maps input→output column names).
        # This works with the unique-hash naming scheme.
        rel_label = relationship.labels[0]
        rel_target_input = f"{rel_label}__{RELATIONSHIP_TARGET_COLUMN}"
        rel_target_column = relationship_relation.projected_column_names[rel_target_input]

        joined_relation: Relation = Join(
            left=node_relation,
            right=relationship_relation,
            source_algebraizable=PatternIntersection(
                pattern_list=[node, relationship]
            ),
            on_left=[node_variable_column],
            on_right=[rel_target_column],
            join_type=JoinType.INNER,
            variable_map={
                **node_relation.variable_map,
                **relationship_relation.variable_map,
            },
            variable_type_map={
                **node_relation.variable_type_map,
                **relationship_relation.variable_type_map,
            },
            column_names=list(node_relation.variable_map.values())
            + list(relationship_relation.variable_map.values()),
        )

        return joined_relation

    def _from_intersection_list(self, relations: list[Relation]) -> Relation:
        accumulated_value: Relation = relations[0]
        for next_relation in relations[1:]:
            # Get trhe variables in common
            common_variables: set[Variable] = set(
                accumulated_value.variable_map.keys()
            ).intersection(set(next_relation.variable_map.keys()))
            # Assume for now that there are common variables. We will need to perform a cross product in the future
            if not common_variables:
                raise NotImplementedError(
                    "No common variables found for join. Cross product not implemented yet."
                )
            # Perform the binary join using the common variables
            variable_map = {
                **accumulated_value.variable_map,
                **next_relation.variable_map,
            }
            variable_type_map = {
                **accumulated_value.variable_type_map,
                **next_relation.variable_type_map,
            }
            column_names = (
                accumulated_value.column_names + next_relation.column_names
            )

            accumulated_value = Join(
                left=accumulated_value,
                right=next_relation,
                join_type=JoinType.INNER,
                on_left=[
                    accumulated_value.variable_map[var]
                    for var in common_variables
                ],
                on_right=[
                    next_relation.variable_map[var] for var in common_variables
                ],
                variable_map=variable_map,
                variable_type_map=variable_type_map,
                column_names=column_names,
                source_algebraizable=PatternIntersection(
                    pattern_list=[
                        accumulated_value.source_algebraizable,
                        next_relation.source_algebraizable,
                    ]
                ),
            )

        return accumulated_value

    def _binary_join(self, left: Relation, right: Relation) -> Relation:
        """Perform a smart binary join between two Relations, depending on the
        specific types of the Relations."""

        match (left.source_algebraizable, right.source_algebraizable):
            case (NodePattern(), RelationshipPattern()) if (
                cast(
                    typ=RelationshipPattern, val=right.source_algebraizable
                ).direction
                == RelationshipDirection.RIGHT
            ):
                LOGGER.debug(
                    msg="Joining Node with Relationship for -[r:RELATIONSHIP]->(n) (head)`."
                )
                node_variable: Variable = cast(
                    typ=NodePattern, val=left.source_algebraizable
                ).variable
                node_variable_column: ColumnName = left.variable_map[
                    node_variable
                ]
                left_join_key: ColumnName = node_variable_column
                right_join_key: ColumnName = right.variable_map[
                    cast(
                        typ=RelationshipPattern, val=right.source_algebraizable
                    ).variable
                ]
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map,
                }
                variable_type_map: VariableTypeMap = {
                    **left.variable_type_map,
                    **right.variable_type_map,
                }
                join: Join = Join(
                    left=left,
                    right=right,
                    source_algebraizable=PatternIntersection(
                        pattern_list=flatten(
                            [left.source_algebraizable, right.source_algebraizable]
                        )
                    ),
                    on_left=[left_join_key],
                    on_right=[right_join_key],
                    join_type=join_type,
                    variable_map=variable_map,
                    variable_type_map=variable_type_map,
                    column_names=left.column_names
                    + right.column_names,  # NOTE: Should not be any collisions here
                )
                variable_relation: Relation = SelectColumns(
                    relation=join,
                    source_algebraizable=PatternIntersection(
                        pattern_list=flatten([join.source_algebraizable])
                    ),
                    identifier=random_hash(),
                    variable_map=join.variable_map,
                    variable_type_map=join.variable_type_map,
                    column_names=[
                        column_name
                        for column_name in join.variable_map.values()
                    ],
                )
                return variable_relation
            case (NodePattern(), RelationshipPattern()) if (
                cast(
                    typ=RelationshipPattern, val=right.source_algebraizable
                ).direction
                == RelationshipDirection.LEFT
            ):
                LOGGER.debug(
                    msg="Joining Node with Relationship for (n)<-[r:RELATIONSHIP] (tail)`."
                )
                # (n)<-[r] is equivalent to [r]->(n)
                # Swap to (Rel, Node) and set Rel direction to RIGHT (to match existing logic)
                new_right = copy.copy(right)
                new_right.source_algebraizable = cast(
                    RelationshipPattern, right.source_algebraizable
                ).model_copy(update={"direction": RelationshipDirection.RIGHT})
                return self._binary_join(left=new_right, right=left)
            case (RelationshipPattern(), NodePattern()) if (
                cast(
                    typ=RelationshipPattern, val=left.source_algebraizable
                ).direction
                == RelationshipDirection.RIGHT
            ):
                LOGGER.debug(
                    msg="Joining Node with Relationship for -[r:RELATIONSHIP]->(n) (head)`."
                )
                node_variable: Variable = cast(
                    typ=NodePattern, val=right.source_algebraizable
                ).variable
                node_variable_column: ColumnName = right.variable_map[
                    node_variable
                ]

                left_join_key: ColumnName = RELATIONSHIP_TARGET_COLUMN
                right_join_key: ColumnName = node_variable_column
                join_type: JoinType = JoinType.INNER
                variable_map: VariableMap = {
                    **left.variable_map,
                    **right.variable_map,
                }
                variable_type_map: VariableTypeMap = {
                    **left.variable_type_map,
                    **right.variable_type_map,
                }
                join: Join = Join(
                    left=left,
                    right=right,
                    source_algebraizable=PatternIntersection(
                        pattern_list=flatten(
                            [left.source_algebraizable, right.source_algebraizable]
                        )
                    ),
                    on_left=[left_join_key],
                    on_right=[right_join_key],
                    join_type=join_type,
                    variable_map=variable_map,
                    variable_type_map=variable_type_map,
                    column_names=left.column_names
                    + right.column_names,  # NOTE: Should not be any collisions here
                )
                variable_relation: Relation = SelectColumns(
                    relation=join,
                    source_algebraizable=PatternIntersection(
                        pattern_list=flatten([join.source_algebraizable])
                    ),
                    identifier=random_hash(),
                    variable_map=join.variable_map,
                    variable_type_map=join.variable_type_map,
                    column_names=[
                        column_name
                        for column_name in join.variable_map.values()
                    ],
                )
                return variable_relation
            case (RelationshipPattern(), NodePattern()) if (
                cast(
                    typ=RelationshipPattern, val=left.source_algebraizable
                ).direction
                == RelationshipDirection.LEFT
            ):
                # <-[r]-(n). Equivalent to (n)-[r]->.
                # Swap to (Node, Rel) and set Rel direction to RIGHT.
                new_left = copy.copy(left)
                new_left.source_algebraizable = cast(
                    RelationshipPattern, left.source_algebraizable
                ).model_copy(update={"direction": RelationshipDirection.RIGHT})
                return self._binary_join(left=right, right=new_left)

            case (NodePattern(), NodePattern()):
                # Two standalone NodePatterns - should never happen in well-formed patterns
                raise ValueError("Cannot join two NodePatterns directly.")
            case (RelationshipPattern(), RelationshipPattern()):
                # Two standalone RelationshipPatterns - should never happen
                raise ValueError(
                    "Cannot join two RelationshipPatterns directly."
                )
            case (PatternPath(), PatternPath()) | (PatternPath(), _) | (_, PatternPath()):
                # Join two PatternPaths or a PatternPath with another relation type
                # This handles comma-separated patterns like (p:Person), (p)-[k:KNOWS]->(q)
                LOGGER.debug(
                    msg=f"Joining PatternPath-based relations on common variables."
                )
                # Find variables in common
                left_vars = set(left.variable_map.keys())
                right_vars = set(right.variable_map.keys())
                common_vars = left_vars & right_vars
                
                if not common_vars:
                    # No common variables - cross product
                    LOGGER.debug(msg="No common variables, performing cross product.")
                    variable_map = {**left.variable_map, **right.variable_map}
                    variable_type_map = {**left.variable_type_map, **right.variable_type_map}
                    # Column names should not have duplicates since no common variables
                    column_names = left.column_names + right.column_names
                    
                    join: Join = Join(
                        left=left,
                        right=right,
                        source_algebraizable=PatternIntersection(
                            pattern_list=[left.source_algebraizable, right.source_algebraizable]
                        ),
                        on_left=[],
                        on_right=[],
                        join_type=JoinType.CROSS,
                        variable_map=variable_map,
                        variable_type_map=variable_type_map,
                        column_names=column_names,
                    )
                    return join
                else:
                    # Join on common variables
                    LOGGER.debug(msg=f"Joining on common variables: {common_vars}")
                    left_join_cols = [left.variable_map[var] for var in common_vars]
                    right_join_cols = [right.variable_map[var] for var in common_vars]
                    
                    variable_map = {**left.variable_map, **right.variable_map}
                    variable_type_map = {**left.variable_type_map, **right.variable_type_map}
                    
                    # Only include unique columns (deduplicate common variable columns)
                    # For common variables, use the left side's column
                    unique_columns = []
                    seen_vars = set()
                    
                    # Add all left columns
                    for var, col in left.variable_map.items():
                        if var not in seen_vars:
                            unique_columns.append(col)
                            seen_vars.add(var)
                    
                    # Add right columns only if variable not already seen
                    for var, col in right.variable_map.items():
                        if var not in seen_vars:
                            unique_columns.append(col)
                            seen_vars.add(var)
                    
                    join: Join = Join(
                        left=left,
                        right=right,
                        source_algebraizable=PatternIntersection(
                            pattern_list=[left.source_algebraizable, right.source_algebraizable]
                        ),
                        on_left=left_join_cols,
                        on_right=right_join_cols,
                        join_type=JoinType.INNER,
                        variable_map=variable_map,
                        variable_type_map=variable_type_map,
                        column_names=unique_columns,
                    )
                    return join

            case _:
                raise NotImplementedError(
                    f"Join logic for {type(left.source_algebraizable)} and {type(right.source_algebraizable)} is not implemented."
                )

    def _from_relationship_pattern(
        self, relationship: RelationshipPattern
    ) -> Projection:
        """Convert a RelationshipPattern to a RelationshipTable.

        Uses unique random hashes for all output columns to ensure that
        multiple instances of the same relationship type (e.g. two separate
        KNOWS edges in a multi-hop path) have distinct column names and
        never collide during pd.merge operations.
        """
        relationship_relation: RelationshipTable = (
            self.context.relationship_mapping[relationship.labels[0]]
        )
        
        lbl = relationship.labels[0]
        prefixed_id_col = f"{lbl}__{ID_COLUMN}"
        prefixed_source_col = f"{lbl}__{RELATIONSHIP_SOURCE_COLUMN}"
        prefixed_target_col = f"{lbl}__{RELATIONSHIP_TARGET_COLUMN}"

        # Generate unique column names for this specific relationship instance
        new_id_column: ColumnName = random_hash()
        new_source_column: ColumnName = random_hash()
        new_target_column: ColumnName = random_hash()
        
        relation: Projection = Projection(
            relation=relationship_relation,
            projected_column_names={
                prefixed_id_col: new_id_column,
                prefixed_source_col: new_source_column,
                prefixed_target_col: new_target_column,
            },
            variable_map={relationship.variable: new_id_column},
            variable_type_map={relationship.variable: relationship.labels[0]},
            column_names=[
                new_id_column,
                new_source_column,
                new_target_column,
            ],
            identifier=random_hash(),
            source_algebraizable=relationship,
        )
        return relation

    def _from_relationship_pattern_with_attrs(
        self, relationship: RelationshipPattern
    ) -> FilterRows:
        """Convert a RelationshipPattern with properties to a filtered RelationshipTable.
        
        Uses inductive approach: take first property, create base relationship with
        remaining properties, then wrap in FilterRows for the first property.
        """
        if not relationship.properties:
            raise ValueError("RelationshipPattern must have properties for this method.")
        
        # Extract first property
        attr1: str = list(relationship.properties.keys())[0]
        val1: Any = list(relationship.properties.values())[0]

        # Create base relationship with remaining properties
        base_relationship: RelationshipPattern = RelationshipPattern(
            variable=relationship.variable,
            labels=relationship.labels,
            direction=relationship.direction,
            properties={
                attr: val
                for attr, val in relationship.properties.items()
                if attr != attr1
            },
        )

        # Recursively convert base relationship
        base_relationship_relation: Relation = self.to_relation(obj=base_relationship)

        # Wrap in FilterRows for the first property
        filtered_relation_identifier: str = random_hash()
        filtered_relation: FilterRows = FilterRows(
            relation=base_relationship_relation,
            condition=AttributeEqualsValue(left=attr1, right=val1),
            identifier=filtered_relation_identifier,
            variable_map=base_relationship_relation.variable_map,
            variable_type_map=base_relationship_relation.variable_type_map,
            source_algebraizable=relationship,
            column_names=list(base_relationship_relation.variable_map.values()),
        )

        return filtered_relation

    def _from_node_pattern_no_attrs(self, node: NodePattern) -> Projection:
        """Convert a NodePattern to an EntityTable."""
        # Handle NodePattern with empty labels (variable reference)
        if not node.labels:
            # Look up the label from the variable registry
            if node.variable in self.variable_type_registry:
                label = self.variable_type_registry[node.variable]
                LOGGER.debug(msg=f"Using registered type {label} for variable {node.variable}")
            else:
                raise ValueError(
                    f"Variable {node.variable} used without labels and not found in registry. "
                    "Variable must be declared with a label before being referenced."
                )
        else:
            label = node.labels[0]
            # Register this variable's type for future references
            if node.variable and node.variable not in self.variable_type_registry:
                self.variable_type_registry[node.variable] = label
        
        entity_relation: EntityTable = self.context.entity_mapping[label]
        
        new_column_name: ColumnName = random_hash()
        relation: Projection = Projection(
            relation=entity_relation,
            projected_column_names={
                f"{label}__{ID_COLUMN}": new_column_name
            },
            variable_map={node.variable: new_column_name},
            variable_type_map={node.variable: label},
            column_names=[new_column_name],
            identifier=random_hash(),
            source_algebraizable=node,
        )
        return relation

    def _from_node_pattern(self, node: NodePattern) -> Projection | FilterRows:
        """Convert a NodePattern to an EntityTable. If there are properties, convert to a Projection and then FilterRows."""
        if not node.properties:
            return self._from_node_pattern_no_attrs(node=node)
        else:
            return self._from_node_pattern_with_attrs(node=node)

    def _from_node_pattern_with_attrs(self, node: NodePattern) -> FilterRows:
        """Convert a NodePattern with one property: value to a Relation with that property and value."""

        attr1: str = list(node.properties.keys())[0]
        val1: Any = list(node.properties.values())[0]
        base_node: NodePattern = NodePattern(
            variable=node.variable,
            labels=node.labels,
            properties={
                attr: val
                for attr, val in node.properties.items()
                if attr != attr1
            },
        )
        base_node_relation: Relation = self.to_relation(obj=base_node)

        filtered_relation_identifier: str = random_hash()
        filtered_relation: FilterRows = FilterRows(
            relation=base_node_relation,
            condition=AttributeEqualsValue(left=attr1, right=val1),
            identifier=filtered_relation_identifier,
            variable_map=base_node_relation.variable_map,
            variable_type_map=base_node_relation.variable_type_map,
            source_algebraizable=node,
            column_names=list(base_node_relation.variable_map.values()),
        )

        return filtered_relation

    def to_pandas(self, relation: Relation) -> pd.DataFrame:
        """Convert the EntityTable to a pandas DataFrame."""
        # Delegates to the relation's own to_pandas method which is now properly implemented in classes
        return relation.to_pandas(context=self.context)

    def _from_with_clause(self, with_clause: Any, input_relation: Relation) -> Relation:
        """Translate WITH clause to relational algebra.
        
        Phase 3 Implementation:
        - Supports simple expression projection (property access, variables, literals)
        - Supports full-table aggregations (COLLECT, COUNT, SUM, AVG, MIN, MAX)
        - Supports grouped aggregations (GROUP BY)
        - Assumes all items have aliases
        - No WHERE/ORDER BY/DISTINCT/SKIP/LIMIT support yet (Phase 4)
        
        Args:
            with_clause: AST With node
            input_relation: Relation from previous MATCH clause
            
        Returns:
            Relation with projected/aggregated columns and new variable scope
            
        Raises:
            NotImplementedError: For unsupported features (filters, etc.)
        """
        from pycypher.ast_models import With, ReturnItem
        
        if not isinstance(with_clause, With):
            raise TypeError(f"Expected With clause, got {type(with_clause).__name__}")
        
        LOGGER.debug(
            msg=f"Processing WITH clause with {len(with_clause.items)} items"
        )
        
        # Check for unsupported features
        if with_clause.where is not None:
            raise NotImplementedError(
                "WHERE clause in WITH not supported yet (Phase 4)"
            )
        if with_clause.order_by is not None:
            raise NotImplementedError(
                "ORDER BY in WITH not supported yet (Phase 4)"
            )
        if with_clause.distinct:
            raise NotImplementedError(
                "DISTINCT in WITH not supported yet (Phase 4)"
            )
        if with_clause.skip is not None or with_clause.limit is not None:
            raise NotImplementedError(
                "SKIP/LIMIT in WITH not supported yet (Phase 4)"
            )
        
        # All items should have expressions (checked by parser, but verify)
        for item in with_clause.items:
            if item.expression is None:
                raise ValueError("WITH item must have an expression")
            if item.alias is None:
                raise ValueError(
                    "All WITH items must have aliases. "
                    f"Missing alias for expression: {item.expression}"
                )
        
        # Classify items as aggregations or grouping expressions
        agg_items = [
            item for item in with_clause.items 
            if self._contains_aggregation(item.expression)
        ]
        non_agg_items = [
            item for item in with_clause.items 
            if not self._contains_aggregation(item.expression)
        ]
        
        # Route to appropriate implementation
        if not agg_items:
            # No aggregations - simple expression projection (Phase 1)
            relation = self._apply_expression_projection(input_relation, with_clause.items)
        elif not non_agg_items:
            # All aggregations - full-table aggregation (Phase 2)
            relation = self._apply_aggregation(input_relation, with_clause.items)
        else:
            # Mixed - grouped aggregation (Phase 3)
            relation = self._apply_grouped_aggregation(
                input_relation, 
                non_agg_items, 
                agg_items
            )
        
        return relation
    
    def _contains_aggregation(self, expression: Any) -> bool:
        """Check if expression contains aggregate functions.
        
        Distinguishes between scalar functions (toUpper, toString, etc.) 
        and aggregation functions (collect, count, sum, avg, min, max).
        
        Scalar functions operate on individual values (row-by-row),
        while aggregations reduce multiple rows to a single value.
        
        Args:
            expression: AST expression to check
            
        Returns:
            True if expression contains aggregation functions, False for scalar functions
        """
        from pycypher.ast_models import FunctionInvocation, CountStar
        from pycypher.scalar_functions import ScalarFunctionRegistry
        
        # Check if expression itself is an aggregate function
        if isinstance(expression, CountStar):
            return True
        
        if isinstance(expression, FunctionInvocation):
            # Extract function name
            func_name = expression.name
            if isinstance(func_name, dict):
                # Namespaced function: {namespace: "db", name: "labels"}
                func_name = func_name.get("name", "")
            
            if isinstance(func_name, str):
                func_name_lower = func_name.lower()
                
                # Check if it's a registered scalar function
                # Scalar functions are NOT aggregations
                registry = ScalarFunctionRegistry.get_instance()
                if registry.has_function(func_name_lower):
                    return False
                
                # Check if it's a known aggregation function
                known_aggregations = {'collect', 'count', 'sum', 'avg', 'min', 'max'}
                if func_name_lower in known_aggregations:
                    return True
                
                # Unknown function - default to scalar (safer for WITH clause)
                # This allows new scalar functions to work without updating this method
                LOGGER.warning(
                    msg=f"Unknown function '{func_name}', treating as scalar function"
                )
                return False
        
        # Recursively check sub-expressions
        # For now, we only handle simple cases
        # Future: traverse expression tree fully
        
        return False
    
    def _apply_aggregation(
        self,
        input_relation: Relation,
        items: list[Any]
    ) -> Relation:
        """Create Aggregation relation from ReturnItems with aggregation functions.
        
        Phase 2: Full-table aggregations only (no GROUP BY)
        
        Args:
            input_relation: Source relation
            items: List of ReturnItem with aggregation expressions and aliases
            
        Returns:
            Aggregation relation with aggregated values
        """
        from pycypher.relational_models import Aggregation
        from pycypher.ast_models import Variable
        
        # Build aggregations dict: alias -> aggregation expression
        aggregations = {}
        for item in items:
            alias = item.alias
            expression = item.expression
            aggregations[alias] = expression
            
            LOGGER.debug(
                msg=f"WITH aggregation: {alias} = {type(expression).__name__}"
            )
        
        # Create new variable map for the aggregation
        # Only aliased variables are visible after WITH
        new_variable_map = {}
        new_variable_type_map = {}
        
        for item in items:
            alias = item.alias
            var = Variable(name=alias)
            new_variable_map[var] = alias
            # Aggregated values don't have entity types
            # (they're scalars or lists, not entity references)
        
        # Create Aggregation relation
        aggregation = Aggregation(
            relation=input_relation,
            aggregations=aggregations,
            variable_map=new_variable_map,
            variable_type_map=new_variable_type_map,
            column_names=list(aggregations.keys()),
            identifier=random_hash(),
            source_algebraizable=None,
        )
        
        LOGGER.debug(
            msg=f"Created Aggregation with {len(aggregations)} columns: {list(aggregations.keys())}"
        )
        
        return aggregation
    
    def _apply_expression_projection(
        self,
        input_relation: Relation,
        items: list[Any]
    ) -> Relation:
        """Create ExpressionProjection relation from ReturnItems.
        
        Args:
            input_relation: Source relation
            items: List of ReturnItem with expression and alias
            
        Returns:
            ExpressionProjection relation with evaluated expressions
        """
        from pycypher.relational_models import ExpressionProjection
        from pycypher.ast_models import Variable, ReturnItem
        
        # Build expressions dict: alias -> expression
        expressions = {}
        for item in items:
            alias = item.alias
            expression = item.expression
            expressions[alias] = expression
            
            LOGGER.debug(
                msg=f"WITH projection: {alias} = {type(expression).__name__}"
            )
        
        # Create new variable map for the projection
        # Only aliased variables are visible after WITH
        new_variable_map = {}
        new_variable_type_map = {}
        
        for item in items:
            alias = item.alias
            var = Variable(name=alias)
            new_variable_map[var] = alias
            
            # Try to infer type from expression
            # For property lookups, preserve the entity type
            from pycypher.ast_models import PropertyLookup
            if isinstance(item.expression, PropertyLookup):
                # Get variable from property lookup
                if isinstance(item.expression.expression, Variable):
                    source_var = item.expression.expression
                    if source_var in input_relation.variable_type_map:
                        # Preserve entity type (property values have same conceptual type)
                        new_variable_type_map[var] = input_relation.variable_type_map[source_var]
            elif isinstance(item.expression, Variable):
                # Direct variable passthrough
                if item.expression in input_relation.variable_type_map:
                    new_variable_type_map[var] = input_relation.variable_type_map[item.expression]
        
        # Create ExpressionProjection relation
        projection = ExpressionProjection(
            relation=input_relation,
            expressions=expressions,
            variable_map=new_variable_map,
            variable_type_map=new_variable_type_map,
            column_names=list(expressions.keys()),
            identifier=random_hash(),
            source_algebraizable=None,  # WITH clause doesn't have a direct algebraizable source
        )
        
        LOGGER.debug(
            msg=f"Created ExpressionProjection with {len(expressions)} columns: {list(expressions.keys())}"
        )
        
        return projection
    
    def _apply_grouped_aggregation(
        self,
        input_relation: Relation,
        grouping_items: list[Any],
        aggregation_items: list[Any]
    ) -> Relation:
        """Create GroupedAggregation relation from ReturnItems.
        
        Phase 3: Grouped aggregations (GROUP BY)
        
        Args:
            input_relation: Source relation
            grouping_items: List of ReturnItem with non-aggregation expressions (grouping keys)
            aggregation_items: List of ReturnItem with aggregation expressions
            
        Returns:
            GroupedAggregation relation with grouped and aggregated values
        """
        from pycypher.relational_models import GroupedAggregation
        from pycypher.ast_models import Variable
        
        # Build grouping expressions dict: alias -> expression
        grouping_expressions = {}
        for item in grouping_items:
            alias = item.alias
            expression = item.expression
            grouping_expressions[alias] = expression
            
            LOGGER.debug(
                msg=f"WITH grouping: {alias} = {type(expression).__name__}"
            )
        
        # Build aggregations dict: alias -> aggregation expression
        aggregations = {}
        for item in aggregation_items:
            alias = item.alias
            expression = item.expression
            aggregations[alias] = expression
            
            LOGGER.debug(
                msg=f"WITH aggregation: {alias} = {type(expression).__name__}"
            )
        
        # Create new variable map for the result
        # All columns (grouping + aggregations) are visible after WITH
        new_variable_map = {}
        new_variable_type_map = {}
        
        # Add grouping column variables
        for item in grouping_items:
            alias = item.alias
            var = Variable(name=alias)
            new_variable_map[var] = alias
            # Grouping columns may preserve types from source expressions
            # (implementation could be enhanced to track this)
        
        # Add aggregation result variables
        for item in aggregation_items:
            alias = item.alias
            var = Variable(name=alias)
            new_variable_map[var] = alias
            # Aggregations don't have entity types (they're scalars or lists)
        
        # Create GroupedAggregation relation
        grouped_agg = GroupedAggregation(
            relation=input_relation,
            grouping_expressions=grouping_expressions,
            aggregations=aggregations,
            variable_map=new_variable_map,
            variable_type_map=new_variable_type_map,
            column_names=list(grouping_expressions.keys()) + list(aggregations.keys()),
            identifier=random_hash(),
            source_algebraizable=None,
        )
        
        LOGGER.debug(
            msg=f"Created GroupedAggregation with {len(grouping_expressions)} grouping columns "
            f"and {len(aggregations)} aggregations: {list(grouped_agg.column_names)}"
        )
        
        return grouped_agg

    def _from_return_clause(self, return_clause: Any, input_relation: Relation) -> Relation:
        """Translate RETURN clause to relational algebra.
        
        Phase 1 Implementation:
        - Supports simple expression projection (property access, variables, literals)
        - Supports full-table aggregations (COLLECT, COUNT, SUM, AVG, MIN, MAX)
        - Supports grouped aggregations (GROUP BY)
        - Assumes all items have aliases (enforces alias requirement)
        - No WHERE support (RETURN doesn't have WHERE)
        - No DISTINCT/ORDER BY/SKIP/LIMIT support yet (Phase 4)
        
        Args:
            return_clause: AST Return node
            input_relation: Relation from previous clause(s)
            
        Returns:
            Relation with projected/aggregated columns (terminal projection)
            
        Raises:
            NotImplementedError: For unsupported features (DISTINCT, ORDER BY, etc.)
            ValueError: If items lack required aliases
        """
        from pycypher.ast_models import Return, ReturnItem
        
        if not isinstance(return_clause, Return):
            raise TypeError(f"Expected Return clause, got {type(return_clause).__name__}")
        
        LOGGER.debug(
            msg=f"Processing RETURN clause with {len(return_clause.items)} items"
        )
        
        # Check for unsupported features (Phase 4)
        if return_clause.order_by is not None:
            raise NotImplementedError(
                "ORDER BY in RETURN not supported yet (Phase 4)"
            )
        if return_clause.distinct:
            raise NotImplementedError(
                "DISTINCT in RETURN not supported yet (Phase 4)"
            )
        if return_clause.skip is not None or return_clause.limit is not None:
            raise NotImplementedError(
                "SKIP/LIMIT in RETURN not supported yet (Phase 4)"
            )
        
        # All items should have expressions (checked by parser, but verify)
        for item in return_clause.items:
            if item.expression is None:
                raise ValueError("RETURN item must have an expression")
            # For RETURN, alias is required for proper DataFrame column naming
            if item.alias is None:
                raise ValueError(
                    "All RETURN items must have aliases. "
                    f"Missing alias for expression: {item.expression}"
                )
        
        # Classify items as aggregations or grouping expressions
        agg_items = [
            item for item in return_clause.items 
            if self._contains_aggregation(item.expression)
        ]
        non_agg_items = [
            item for item in return_clause.items 
            if not self._contains_aggregation(item.expression)
        ]
        
        # Route to appropriate implementation (same logic as WITH)
        if not agg_items:
            # No aggregations - simple expression projection (Phase 1)
            relation = self._apply_expression_projection(input_relation, return_clause.items)
        elif not non_agg_items:
            # All aggregations - full-table aggregation (Phase 2)
            relation = self._apply_aggregation(input_relation, return_clause.items)
        else:
            # Mixed - grouped aggregation (Phase 3)
            relation = self._apply_grouped_aggregation(
                input_relation, 
                non_agg_items, 
                agg_items
            )
        
        return relation

    def _from_set_clause(self, set_clause: Any, input_relation: Relation) -> Relation:
        """Translate SET clause to relational algebra.

        Converts SET clause items to PropertyChange objects and creates a
        PropertyModification relation to apply the changes.

        Args:
            set_clause: SET clause AST node
            input_relation: Input relation to modify

        Returns:
            PropertyModification relation with changes applied
        """
        from pycypher.ast_models import (
            SetPropertyItem, SetLabelsItem, SetAllPropertiesItem, AddAllPropertiesItem, SetItem
        )
        from pycypher.property_change import PropertyChange, PropertyChangeType
        from pycypher.relational_models import PropertyModification

        if not isinstance(set_clause, Set):
            raise TypeError(f"Expected Set clause, got {type(set_clause).__name__}")

        LOGGER.debug(f"Processing SET clause with {len(set_clause.items)} items")

        # Convert SET items to PropertyChange objects
        property_changes = []

        for item in set_clause.items:
            if isinstance(item, SetPropertyItem):
                # SET n.prop = value
                change = PropertyChange(
                    variable_type=input_relation.variable_type_map.get(item.variable),
                    variable_column=input_relation.variable_map.get(item.variable),
                    change_type=PropertyChangeType.SET_PROPERTY,
                    property_name=item.property,
                    value_expression=item.value
                )
                property_changes.append(change)
                LOGGER.debug(f"SET property: {item.variable.name}.{item.property}")

            elif isinstance(item, SetLabelsItem):
                # SET n:Label
                change = PropertyChange(
                    variable_type=input_relation.variable_type_map.get(item.variable),
                    variable_column=input_relation.variable_map.get(item.variable),
                    change_type=PropertyChangeType.SET_LABELS,
                    labels=item.labels
                )
                property_changes.append(change)
                LOGGER.debug(f"SET labels: {item.variable.name}:{':'.join(item.labels)}")

            elif isinstance(item, SetAllPropertiesItem):
                # SET n = {map}
                # Convert the expression to a properties map
                if hasattr(item.properties, 'value') and isinstance(item.properties.value, dict):
                    from pycypher.ast_models import Literal
                    properties_map = {
                        prop: Literal(value=val) if not hasattr(val, '__class__') else val
                        for prop, val in item.properties.value.items()
                    }
                else:
                    # Handle other expression types (variables, etc.)
                    properties_map = {"_properties": item.properties}

                change = PropertyChange(
                    variable_type=input_relation.variable_type_map.get(item.variable),
                    variable_column=input_relation.variable_map.get(item.variable),
                    change_type=PropertyChangeType.SET_ALL_PROPERTIES,
                    properties_map=properties_map
                )
                property_changes.append(change)
                LOGGER.debug(f"SET all properties: {item.variable.name} = {{map}}")

            elif isinstance(item, AddAllPropertiesItem):
                # SET n += {map}
                if hasattr(item.properties, 'value') and isinstance(item.properties.value, dict):
                    from pycypher.ast_models import Literal
                    properties_map = {
                        prop: Literal(value=val) if not hasattr(val, '__class__') else val
                        for prop, val in item.properties.value.items()
                    }
                else:
                    properties_map = {"_properties": item.properties}

                change = PropertyChange(
                    variable_type=input_relation.variable_type_map.get(item.variable),
                    variable_column=input_relation.variable_map.get(item.variable),
                    change_type=PropertyChangeType.ADD_ALL_PROPERTIES,
                    properties_map=properties_map
                )
                property_changes.append(change)
                LOGGER.debug(f"ADD all properties: {item.variable.name} += {{map}}")

            elif isinstance(item, SetItem):
                # Handle generic SetItem for backward compatibility with parser
                if item.property and item.expression:
                    # SET n.prop = value
                    change = PropertyChange(
                        variable_type=input_relation.variable_type_map.get(item.variable),
                        variable_column=input_relation.variable_map.get(item.variable),
                        change_type=PropertyChangeType.SET_PROPERTY,
                        property_name=item.property,
                        value_expression=item.expression
                    )
                    property_changes.append(change)
                    LOGGER.debug(f"SET property (generic): {item.variable.name}.{item.property}")
                elif item.labels:
                    # SET n:Label
                    change = PropertyChange(
                        variable_type=input_relation.variable_type_map.get(item.variable),
                        variable_column=input_relation.variable_map.get(item.variable),
                        change_type=PropertyChangeType.SET_LABELS,
                        labels=item.labels
                    )
                    property_changes.append(change)
                    LOGGER.debug(f"SET labels (generic): {item.variable.name}:{':'.join(item.labels)}")
                else:
                    raise ValueError(f"Invalid SetItem: missing property/expression or labels")

            else:
                raise NotImplementedError(f"SET item type {type(item).__name__} not supported")

        # Create PropertyModification relation
        return PropertyModification(
            base_relation=input_relation,
            modifications=property_changes,
            context=self.context,
            variable_map=input_relation.variable_map,
            variable_type_map=input_relation.variable_type_map,
            column_names=input_relation.column_names  # Will be updated by PropertyModification
        )

    def execute_query(self, query: Any) -> pd.DataFrame:
        """Execute a complete Cypher query and return results as DataFrame.
        
        Processes clauses sequentially:
        1. MATCH clause(s) - build base relation
        2. WITH clause(s) - transform relation (if present)
        3. RETURN clause - final projection
        
        Args:
            query: Cypher query string or Query AST node
            
        Returns:
            DataFrame with columns matching RETURN clause aliases
            
        Raises:
            ValueError: If query structure is invalid
            NotImplementedError: For unsupported clause types
        """
        from pycypher.ast_models import Query, Match, With, Return, ASTConverter
        
        # Parse string to AST if needed
        if isinstance(query, str):
            converter = ASTConverter()
            query = converter.from_cypher(query)
        
        if not isinstance(query, Query):
            raise TypeError(f"Expected Query, got {type(query).__name__}")
        
        if not query.clauses:
            raise ValueError("Query must have at least one clause")
        
        LOGGER.debug(msg=f"Executing query with {len(query.clauses)} clauses")
        
        # Process clauses sequentially
        current_relation = None
        
        for i, clause in enumerate(query.clauses):
            LOGGER.debug(msg=f"Processing clause {i}: {type(clause).__name__}")
            
            if isinstance(clause, Match):
                # MATCH clause - build base relation from pattern
                if current_relation is not None:
                    # Multiple MATCH clauses - join with previous relation on common variables
                    match_relation = self.to_relation(clause.pattern)
                    left_vars = set(current_relation.variable_map.keys())
                    right_vars = set(match_relation.variable_map.keys())
                    common_vars = left_vars & right_vars

                    if common_vars:
                        left_join_cols = [current_relation.variable_map[var] for var in common_vars]
                        right_join_cols = [match_relation.variable_map[var] for var in common_vars]

                        variable_map = {**current_relation.variable_map, **match_relation.variable_map}
                        variable_type_map = {**current_relation.variable_type_map, **match_relation.variable_type_map}

                        # Deduplicate columns for common variables (keep left side)
                        unique_columns = []
                        seen_vars: set[Variable] = set()
                        for var, col in current_relation.variable_map.items():
                            if var not in seen_vars:
                                unique_columns.append(col)
                                seen_vars.add(var)
                        for var, col in match_relation.variable_map.items():
                            if var not in seen_vars:
                                unique_columns.append(col)
                                seen_vars.add(var)

                        current_relation = Join(
                            left=current_relation,
                            right=match_relation,
                            join_type=JoinType.INNER,
                            on_left=left_join_cols,
                            on_right=right_join_cols,
                            variable_map=variable_map,
                            variable_type_map=variable_type_map,
                            column_names=unique_columns,
                        )
                    else:
                        # No common variables — cross product
                        variable_map = {**current_relation.variable_map, **match_relation.variable_map}
                        variable_type_map = {**current_relation.variable_type_map, **match_relation.variable_type_map}
                        current_relation = Join(
                            left=current_relation,
                            right=match_relation,
                            join_type=JoinType.CROSS,
                            on_left=[],
                            on_right=[],
                            variable_map=variable_map,
                            variable_type_map=variable_type_map,
                            column_names=current_relation.column_names + match_relation.column_names,
                        )
                else:
                    # First MATCH clause
                    current_relation = self.to_relation(clause.pattern)
                    
            elif isinstance(clause, With):
                # WITH clause - transform current relation
                if current_relation is None:
                    raise ValueError("WITH clause requires preceding MATCH clause")
                current_relation = self._from_with_clause(clause, current_relation)
                
            elif isinstance(clause, Return):
                # RETURN clause - final projection
                if current_relation is None:
                    raise ValueError("RETURN clause requires preceding MATCH clause")
                current_relation = self._from_return_clause(clause, current_relation)

            elif isinstance(clause, Set):
                # SET clause - property modifications
                if current_relation is None:
                    raise ValueError("SET clause requires preceding MATCH clause")
                current_relation = self._from_set_clause(clause, current_relation)

            else:
                raise NotImplementedError(
                    f"Clause type {type(clause).__name__} not supported yet"
                )
        
        # Execute final relation to get DataFrame
        if current_relation is None:
            raise ValueError("Query produced no relation")
        
        result_df = current_relation.to_pandas(context=self.context)
        
        LOGGER.debug(
            msg=f"Query execution complete. Result shape: {result_df.shape}"
        )
        
        return result_df
