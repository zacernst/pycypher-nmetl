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
    ColumnName,
)
import pandas as pd


class Star:
    """Translation operator."""

    def __init__(self, context: Context = Context()) -> None:
        self.context: Context = context

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
                    how=JoinType.INNER,
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
        
        # Get the prefixed source column name from the relationship
        rel_label = relationship.labels[0]
        rel_source_column = f"{rel_label}__{RELATIONSHIP_SOURCE_COLUMN}"

        joined_relation: Relation = Join(
            left=node_relation,
            right=relationship_relation,
            source_algebraizable=PatternIntersection(
                pattern_list=[node, relationship]
            ),
            on_left=[node_variable_column],
            on_right=[rel_source_column],
            how=JoinType.INNER,
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
        
        # Get the prefixed target column name from the relationship
        rel_label = relationship.labels[0]
        rel_target_column = f"{rel_label}__{RELATIONSHIP_TARGET_COLUMN}"

        joined_relation: Relation = Join(
            left=node_relation,
            right=relationship_relation,
            source_algebraizable=PatternIntersection(
                pattern_list=[node, relationship]
            ),
            on_left=[node_variable_column],
            on_right=[rel_target_column],
            how=JoinType.INNER,
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

            import pdb

            pdb.set_trace()

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
                    source_algebraizable=flatten(
                        [left.source_algebraizable, right.source_algebraizable]
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
                    source_algebraizable=flatten([join.source_algebraizable]),
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
                    source_algebraizable=flatten(
                        [left.source_algebraizable, right.source_algebraizable]
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
                    source_algebraizable=flatten([join.source_algebraizable]),
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
                # This should never happen
                raise ValueError("Cannot join two NodePatterns directly.")
            case (RelationshipPattern(), RelationshipPattern()):
                # This should never happen
                raise ValueError(
                    "Cannot join two RelationshipPatterns directly."
                )

            case _:
                raise NotImplementedError(
                    f"Join logic for {type(left.source_algebraizable)} and {type(right.source_algebraizable)} is not implemented."
                )

    def _from_relationship_pattern(
        self, relationship: RelationshipPattern
    ) -> Projection:
        """Convert a RelationshipPattern to a RelationshipTable."""
        relationship_relation: RelationshipTable = (
            self.context.relationship_mapping[relationship.labels[0]]
        )
        
        lbl = relationship.labels[0]
        prefixed_id_col = f"{lbl}__{ID_COLUMN}"
        prefixed_source_col = f"{lbl}__{RELATIONSHIP_SOURCE_COLUMN}"
        prefixed_target_col = f"{lbl}__{RELATIONSHIP_TARGET_COLUMN}"
        
        relation: Projection = Projection(
            relation=relationship_relation,
            projected_column_names={
                prefixed_id_col: prefixed_id_col,
                prefixed_source_col: prefixed_source_col,
                prefixed_target_col: prefixed_target_col,
            },
            variable_map={relationship.variable: prefixed_id_col},
            variable_type_map={relationship.variable: relationship.labels[0]},
            column_names=[
                prefixed_id_col,
                prefixed_source_col,
                prefixed_target_col,
            ],
            identifier=random_hash(),
            source_algebraizable=relationship,
        )
        return relation

    def _from_relationship_pattern_with_attrs(
        self, relationship: RelationshipPattern
    ) -> FilterRows:
        raise NotImplementedError(
            "Filtering on relationship properties is not implemented yet."
        )
        # if not relationship.properties:
        #     raise ValueError("RelationshipPattern must have properties for this method.")
        #
        # """Convert a RelationshipPattern with attributes to a filtered RelationshipTable."""
        # attr1: str = list(relationship.properties.keys())[0]
        # val1: Any = list(relationship.properties.values())[0]

        # # Create base relationship with remaining attributes
        # base_relationship: RelationshipPattern = RelationshipPattern(
        #     variable=relationship.variable,
        #     labels=relationship.labels,
        #     direction=relationship.direction,
        #     properties={
        #         attr: val
        #         for attr, val in relationship.properties.items()
        #         if attr != attr1
        #     },
        # )

        # base_relationship_relation: Relation = self.to_relation(obj=base_relationship)

        # filtered_relation_identifier: str = random_hash()
        # filtered_relation: FilterRows = FilterRows(
        #     relation=base_relationship_relation,
        #     condition=AttributeEqualsValue(left=attr1, right=val1),
        #     source_algebraizable=relationship,
        # )
        # return filtered_relation

    def _from_node_pattern_no_attrs(self, node: NodePattern) -> Projection:
        """Convert a NodePattern to an EntityTable."""
        entity_relation: EntityTable = self.context.entity_mapping[
            node.labels[0]
        ]
        
        lbl = node.labels[0]
        new_column_name: ColumnName = random_hash()
        relation: Projection = Projection(
            relation=entity_relation,
            projected_column_names={
                f"{lbl}__{ID_COLUMN}": new_column_name
            },
            variable_map={node.variable: new_column_name},
            variable_type_map={node.variable: node.labels[0]},
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
