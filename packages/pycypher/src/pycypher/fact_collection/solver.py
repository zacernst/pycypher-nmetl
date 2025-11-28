from __future__ import annotations

from pycypher.fact import AtomicFact, FactNodeHasLabel, FactNodeHasAttributeWithValue, FactRelationshipHasSourceNode, FactRelationshipHasTargetNode, FactRelationshipHasLabel
from pycypher.node_classes import Node, RelationshipChain
from pycypher.cypher_parser import CypherParser
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection

from typing import Any, Generator
import itertools
import copy


class ConstraintBag:
    def __init__(self):
        self.bag: set[Any] = set()
        self.next_constraint_id: int = 1
    
    def add_constraint(self, constraint: Any) -> int:
        if isinstance(constraint, AtomicConstraint):
            constraint.constraint_id = self.next_constraint_id
            self.next_constraint_id += 1
        self.bag.add(constraint)
    
    def __iadd__(self, other) -> 'ConstraintBag':
        self.add_constraint(other)
        return self
    
    def __repr__(self) -> str:
        return f"ConstraintBag({len(self.bag)})"
    
    def __iter__(self) -> Any:
        return iter(self.bag)
    
    def assignments_of_variable(self, variable: str) -> Generator[Any, None, None]:
        for constraint in copy.deepcopy(self).walk():
            match constraint:
                case VariableAssignedToNode() | VariableAssignedToRelationship():
                    if constraint.variable == variable:
                        yield constraint
                case _:
                    pass
    
    def walk(self) -> Generator[Any, None, None]:
        for constraint in self.bag:
            if hasattr(constraint, 'walk'):
                yield from constraint.walk()
            yield constraint
    
    def build_atomic_constraint_mapping(self) -> dict[int, AtomicConstraint]:
        mapping: dict[int, AtomicConstraint] = {}
        constraint_id: int = 1
        for constraint in self.walk():
            if isinstance(constraint, AtomicConstraint):
                mapping[constraint] = constraint_id
                constraint_id += 1
        return mapping


class AtomicConstraint:
    pass


class VariableAssignedToNode(AtomicConstraint):
    def __init__(self, variable: str, node_id: str):
        self.variable = variable
        self.node_id = node_id
        self.constraint_id: int | None = None
    
    def __repr__(self) -> str:
        return f"VariableAssignedToNode(variable={self.variable}, node_id={self.node_id})"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, VariableAssignedToNode):
            return False
        return self.variable == other.variable and self.node_id == other.node_id
    
    def __hash__(self) -> int:
        return hash((self.variable, self.node_id))
    
    def walk(self) -> Generator[Any, None, None]:
        yield self


class VariableAssignedToRelationship(AtomicConstraint):
    def __init__(self, variable: str, relationship_id: str):
        self.variable = variable
        self.relationship_id = relationship_id
        self.constraint_id: int | None = None
    
    def walk(self) -> Generator[Any, None, None]:
        yield self


class IfThen:
    def __init__(self, if_constraint: Any, then_constraint: Any):
        self.if_constraint = if_constraint
        self.then_constraint = then_constraint
    
    def walk(self) -> Generator[Any, None, None]:
        yield self
        yield from self.if_constraint.walk()
        yield from self.then_constraint.walk()

class Negation:
    def __init__(self, constraint: Any):
        self.constraint = constraint

    def walk(self) -> Generator[Any, None, None]:
        yield self
        yield from self.constraint.walk()


class AtMostOne:
    def __init__(self, disjunction: Disjunction):
        self.disjunction = disjunction
    
    def walk(self) -> Generator[Any, None, None]:
        yield self
        yield from self.disjunction.walk()
    

class Disjunction:
    def __init__(self, constraints: list[Any]):
        self.constraints = constraints
    
    def add_constraint(self, constraint: Any):
        self.constraints.append(constraint)
    
    def __iadd__(self, other) -> 'Disjunction':
        self.add_constraint(other)
        return self
    
    def exactly_one(self) -> Conjunction:
        conjunction = Conjunction()
        for disjunction_1, disjunction_2 in itertools.combinations(self.constraints, 2):
            disjunction = Disjunction([Negation(disjunction_1), Negation(disjunction_2)])
            conjunction.add_constraint(disjunction)
        return conjunction
    
    def walk(self) -> Generator[Any, None, None]:
        yield self
        for constraint in self.constraints:
            yield from constraint.walk()


class ExactlyOne:
    def __init__(self, disjunction: Disjunction):
        self.disjunction = disjunction
    
    def walk(self) -> Generator[Any, None, None]:
        yield self
        yield from self.disjunction.walk()


class Conjunction:
    def __init__(self, constraints: list[Any]):
        self.constraints = constraints
    
    def add_constraint(self, constraint: Any):
        self.constraints.append(constraint)
    
    def __iadd__(self, other) -> 'Conjunction':
        self.add_constraint(other)
        return self
    
    def walk(self) -> Generator[Any, None, None]:
        yield self
        for constraint in self.constraints:
            yield from constraint.walk()


def main():
    query: str = "MATCH (t:Tract)-[r:in]->(c:County) RETURN t, c"
    parser = CypherParser(query)
    all_relationship_chains: list[RelationshipChain] = []
    all_nodes: list[Node] = []
    for child in parser.parse_tree.walk():
        match child:
            case Node():
                all_nodes.append(child)
            case RelationshipChain():
                all_relationship_chains.append(child)
            case _:
                pass

    constraint_bag = ConstraintBag()

    fact_collection = FoundationDBFactCollection(foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster')
    for node in all_nodes:
        node_variable = node.name_label.name
        node_assignment_disjunction = Disjunction([])
        for item in fact_collection.node_has_specific_label_facts(node.name_label.label):
            print(f"Variable {node_variable} can map to Node ID {item.node_id} with Label {item.label}")
            variable_assigned_to_node = VariableAssignedToNode(node_variable, item.node_id)
            node_assignment_disjunction += variable_assigned_to_node
        constraint_bag += ExactlyOne(node_assignment_disjunction)

    for relationship_variable in all_relationship_chains:
        relationship_variable = relationship_variable.relationship.relationship.name_label.name
        relationship_assignment_disjunction = Disjunction([])
        for item in fact_collection.relationship_has_label_facts():
            print(f"Variable {relationship_variable} can map to Relationship ID {item.relationship_id} with Label {item.relationship_label}")
            variable_assigned_to_relationship = VariableAssignedToRelationship(relationship_variable, item.relationship_id)
            relationship_assignment_disjunction += variable_assigned_to_relationship
        constraint_bag += ExactlyOne(relationship_assignment_disjunction)


    relationship_has_source_node_constraints: dict = {}
    relationship_has_target_node_constraints: dict = {}
    for relationship_chain in all_relationship_chains:
        # for each relationship assignment, find source and target node assignments
        relationship_variable = relationship_chain.relationship.relationship.name_label.name
        relationship_source_node_variable = relationship_chain.source_node.name_label.name
        relationship_target_node_variable = relationship_chain.target_node.name_label.name
        # if r is X then s is Y

        # For each possible assignment of the relationship variable, find the source node assignment
        for relationship_assignment in constraint_bag.assignments_of_variable(relationship_variable):
            source_node_conjunction = Conjunction([])
            target_node_conjunction = Conjunction([])
            for fact in fact_collection.relationship_has_source_node_facts():
                if fact.relationship_id == relationship_assignment.relationship_id:
                    source_node_assignment = VariableAssignedToNode(relationship_source_node_variable, fact.source_node_id)
                    if_then_constraint = IfThen(relationship_assignment, source_node_assignment)
                    source_node_conjunction += if_then_constraint
            for fact in fact_collection.relationship_has_target_node_facts():
                if fact.relationship_id == relationship_assignment.relationship_id:
                    target_node_assignment = VariableAssignedToNode(relationship_target_node_variable, fact.target_node_id)
                    if_then_constraint = IfThen(relationship_assignment, target_node_assignment)
                    target_node_conjunction += if_then_constraint
            constraint_bag += source_node_conjunction
            constraint_bag += target_node_conjunction
    
    import pdb; pdb.set_trace()


    # node_variables = [n.name_label.name for n in parser.walk() if isinstance(n, Node)]
    # node_assignments_conjunction = Conjunction([])
    # for node_variable in node_variables:
    #     node_assignments_disjunction = Disjunction([])
    #     for node_id in all_node_ids:
    #         # Create a new constraint that this variable maps to this node_name
    #         new_constraint = Constraint(
    #             constraint_id,
    #             VariableAssignedToNode(node_variable, node_id)
    #         )
    #         constraint_id += 1
    #         node_assignments_disjunction.add_constraint(new_constraint)
    #     node_assignments_conjunction.add_constraint(node_assignments_disjunction)
    #     node_assignments_conjunction.add_constraint(
    #         node_assignments_disjunction.exactly_one()
    #     )

    # relationship_chains = [n for n in self.walk() if isinstance(n, RelationshipChain)]
    # relationship_assignments_conjunction = Conjunction([])
    # for relationship_chain in relationship_chains:
    #     relationship_variable = relationship_chain.relationship.relationship.name_label.name
    #     relationship_assignments_disjunction = Disjunction([])
    #     for relationship_id in all_relationship_ids:
    #         # Create a new constraint that this variable maps to this relationship_id
    #         new_constraint = Constraint(
    #             constraint_id,
    #             VariableAssignedToRelationship(relationship_variable, relationship_id)
    #         )
    #         constraint_id += 1
    #         relationship_assignments_disjunction.add_constraint(new_constraint)
    #     relationship_assignments_conjunction.add_constraint(relationship_assignments_disjunction)
    #     relationship_assignments_conjunction.add_constraint(
    #         relationship_assignments_disjunction.exactly_one()
    #     )


if __name__ == '__main__':
    main()



# for n in self.walk():
#     if isinstance(n, Node):
#         # Get the variable (assume there is one)
#         variable = n.name_label.name
#         node_assignment_disjunction = {}
#         for node_id in all_node_ids:
#             # Create a new constraint that this variable maps to this node_name
#             new_constraint = Constraint(contraint, VariableAssignedToNode(variable, node_id))
#             node_assignment_disjunction[constraint] = new_constraint
#             constraint += 1
#         # Node cannot have two assignments at once
#         for var1, var2 in itertools.combinations(
#             node_assignment_disjunction.keys(), 2
#         ):
#             exclusion_constraints.append([-var1, -var2])
#         # Each assignment entails all the facts about that node
#         for assertion_number, (variable, node_id) in node_assignment_disjunction.items():
#             for fact_assertion_number, fact in constraints['FactNodeHasLabel'].item():    
#                 if fact.node_id == node_id:
#                     fact_assertion = [-assertion_number, fact_assertion_number]
#                     exclustion_constraints.append(fact_assertion)
#             for fact_assertion_number, fact in constraints['FactNodeHasAttributeWithValue'].items():
# 
#         # Get all the nodes from the constraint list
# 