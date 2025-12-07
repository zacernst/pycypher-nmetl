"""
Comprehensive unit tests for the solver.py module.

This test suite focuses on testing the constraint classes and logic,
excluding Cypher-specific functionality.
"""

import pytest
from typing import Generator

# Import the classes we need to test
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "pycypher" / "src"))

from pycypher.fact_collection.solver import (
    ConstraintBag,
    AtomicConstraint,
    VariableAssignedToNode,
    VariableAssignedToRelationship,
    IfThen,
    Negation,
    AtMostOne,
    Disjunction,
    ExactlyOne,
    Conjunction,
    Projection,
)


class TestConstraintBag:
    """Comprehensive tests for the ConstraintBag class."""
    
    def test_initialization(self):
        """Test that ConstraintBag initializes with empty bag and no mapping."""
        bag = ConstraintBag()
        
        assert isinstance(bag.bag, set)
        assert len(bag.bag) == 0
        assert bag._atomic_constraint_mapping is None
    
    def test_add_constraint_single(self):
        """Test adding a single constraint to the bag."""
        bag = ConstraintBag()
        constraint = VariableAssignedToNode("x", "node1")
        
        bag.add_constraint(constraint)
        
        assert len(bag.bag) == 1
        assert constraint in bag.bag
    
    def test_add_constraint_multiple(self):
        """Test adding multiple different constraints."""
        bag = ConstraintBag()
        constraint1 = VariableAssignedToNode("x", "node1")
        constraint2 = VariableAssignedToNode("y", "node2")
        constraint3 = VariableAssignedToRelationship("r", "rel1")
        
        bag.add_constraint(constraint1)
        bag.add_constraint(constraint2)
        bag.add_constraint(constraint3)
        
        assert len(bag.bag) == 3
        assert constraint1 in bag.bag
        assert constraint2 in bag.bag
        assert constraint3 in bag.bag
    
    def test_add_constraint_duplicate(self):
        """Test that adding duplicate constraints doesn't increase size (set behavior)."""
        bag = ConstraintBag()
        constraint = VariableAssignedToNode("x", "node1")
        
        bag.add_constraint(constraint)
        bag.add_constraint(constraint)  # Add same constraint again
        
        # Set should only contain one instance
        assert len(bag.bag) == 1
    
    def test_iadd_operator(self):
        """Test the += operator for adding constraints."""
        bag = ConstraintBag()
        constraint = VariableAssignedToNode("x", "node1")

        bag += constraint
        result = bag

        assert result is bag  # Should return self for chaining
        assert len(bag.bag) == 1
        assert constraint in bag.bag
    
    def test_iadd_operator_chaining(self):
        """Test chaining multiple += operations."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        c3 = VariableAssignedToNode("z", "node3")
        
        bag += c1
        bag += c2
        bag += c3
        
        assert len(bag.bag) == 3
        assert c1 in bag.bag
        assert c2 in bag.bag
        assert c3 in bag.bag
    
    def test_repr(self):
        """Test string representation shows constraint count."""
        bag = ConstraintBag()
        
        assert repr(bag) == "ConstraintBag(0)"
        
        bag += VariableAssignedToNode("x", "node1")
        assert repr(bag) == "ConstraintBag(1)"
        
        bag += VariableAssignedToNode("y", "node2")
        assert repr(bag) == "ConstraintBag(2)"
    
    def test_iter(self):
        """Test that ConstraintBag is iterable."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        
        bag += c1
        bag += c2
        
        # Should be able to iterate
        constraints_list = list(bag)
        assert len(constraints_list) == 2
        assert c1 in constraints_list
        assert c2 in constraints_list
    
    def test_walk_simple(self):
        """Test walk() with simple atomic constraints."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        
        bag += c1
        bag += c2
        
        walked = list(bag.walk())
        
        # For atomic constraints, walk yields the constraint itself
        assert len(walked) == 2
        assert c1 in walked
        assert c2 in walked
    
    def test_walk_nested_constraints(self):
        """Test walk() with nested constraint structures."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        disj = Disjunction([c1, c2])
        
        bag += disj
        
        walked = list(bag.walk())
        
        # Should walk into the disjunction and yield all constraints
        assert len(walked) > 2  # disjunction + its constraints
        assert disj in walked
        assert c1 in walked
        assert c2 in walked
    
    def test_build_atomic_constraint_mapping_empty(self):
        """Test building mapping with no constraints."""
        bag = ConstraintBag()
        
        mapping = bag.build_atomic_constraint_mapping()
        
        assert isinstance(mapping, dict)
        assert len(mapping) == 0
    
    def test_build_atomic_constraint_mapping_single(self):
        """Test building mapping with a single atomic constraint."""
        bag = ConstraintBag()
        constraint = VariableAssignedToNode("x", "node1")
        bag += constraint
        
        mapping = bag.build_atomic_constraint_mapping()
        
        assert len(mapping) == 1
        assert constraint in mapping
        assert mapping[constraint] == 1  # First constraint gets ID 1
    
    def test_build_atomic_constraint_mapping_multiple(self):
        """Test building mapping with multiple atomic constraints."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        c3 = VariableAssignedToRelationship("r", "rel1")
        
        bag += c1
        bag += c2
        bag += c3
        
        mapping = bag.build_atomic_constraint_mapping()
        
        assert len(mapping) == 3
        assert all(c in mapping for c in [c1, c2, c3])
        assert all(isinstance(mapping[c], int) for c in [c1, c2, c3])
        assert all(mapping[c] >= 1 for c in [c1, c2, c3])
        
        # IDs should be unique
        ids = [mapping[c] for c in [c1, c2, c3]]
        assert len(set(ids)) == 3
    
    def test_build_atomic_constraint_mapping_sequential(self):
        """Test that mapping assigns sequential IDs starting from 1."""
        bag = ConstraintBag()
        constraints = [
            VariableAssignedToNode("x", "node1"),
            VariableAssignedToNode("y", "node2"),
            VariableAssignedToNode("z", "node3"),
        ]
        
        for c in constraints:
            bag += c
        
        mapping = bag.build_atomic_constraint_mapping()
        ids = sorted(mapping.values())
        
        assert ids == [1, 2, 3]
    
    def test_build_atomic_constraint_mapping_nested(self):
        """Test mapping with nested constraints - only atomic constraints get IDs."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        disj = Disjunction([c1, c2])
        
        bag += disj
        
        mapping = bag.build_atomic_constraint_mapping()
        
        # Only atomic constraints should be in mapping
        assert len(mapping) == 2
        assert c1 in mapping
        assert c2 in mapping
        assert disj not in mapping  # Disjunction is not atomic
    
    def test_atomic_constraint_mapping_property_lazy(self):
        """Test that atomic_constraint_mapping is lazily computed."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        # Initially should be None
        assert bag._atomic_constraint_mapping is None
        
        # Access property triggers computation
        mapping = bag.atomic_constraint_mapping
        
        assert bag._atomic_constraint_mapping is not None
        assert mapping is bag._atomic_constraint_mapping
    
    def test_atomic_constraint_mapping_property_cached(self):
        """Test that atomic_constraint_mapping is cached."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        mapping1 = bag.atomic_constraint_mapping
        mapping2 = bag.atomic_constraint_mapping
        
        # Should return the same object
        assert mapping1 is mapping2
    
    def test_assignments_of_variable_none(self):
        """Test finding assignments when variable has no assignments."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        assignments = list(bag.assignments_of_variable("y"))  # Different variable
        
        assert len(assignments) == 0
    
    def test_assignments_of_variable_single(self):
        """Test finding a single assignment for a variable."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        assignments = list(bag.assignments_of_variable("x"))
        
        assert len(assignments) == 1
        assert c1 in assignments
    
    def test_assignments_of_variable_multiple_nodes(self):
        """Test finding multiple node assignments for the same variable."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("x", "node2")
        c3 = VariableAssignedToNode("x", "node3")
        
        bag += c1
        bag += c2
        bag += c3
        
        assignments = list(bag.assignments_of_variable("x"))
        
        assert len(assignments) == 3
        assert all(a in assignments for a in [c1, c2, c3])
    
    def test_assignments_of_variable_relationships(self):
        """Test finding relationship assignments."""
        bag = ConstraintBag()
        r1 = VariableAssignedToRelationship("r", "rel1")
        r2 = VariableAssignedToRelationship("r", "rel2")
        
        bag += r1
        bag += r2
        
        assignments = list(bag.assignments_of_variable("r"))
        
        assert len(assignments) == 2
        assert r1 in assignments
        assert r2 in assignments
    
    def test_assignments_of_variable_mixed(self):
        """Test finding assignments with mixed constraint types."""
        bag = ConstraintBag()
        n1 = VariableAssignedToNode("x", "node1")
        r1 = VariableAssignedToRelationship("r", "rel1")
        n2 = VariableAssignedToNode("y", "node2")
        
        bag += n1
        bag += r1
        bag += n2
        
        # Should only get node assignment for x
        assignments_x = list(bag.assignments_of_variable("x"))
        assert len(assignments_x) == 1
        assert n1 in assignments_x
        
        # Should only get relationship assignment for r
        assignments_r = list(bag.assignments_of_variable("r"))
        assert len(assignments_r) == 1
        assert r1 in assignments_r
    
    def test_assignments_of_variable_nested(self):
        """Test finding assignments within nested structures."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("x", "node2")
        disj = Disjunction([c1, c2])
        
        bag += disj
        
        assignments = list(bag.assignments_of_variable("x"))
        
        # Should find assignments within the disjunction
        assert len(assignments) == 2
        assert c1 in assignments
        assert c2 in assignments
    
    def test_cnf_empty_bag(self):
        """Test CNF conversion with empty bag."""
        bag = ConstraintBag()
        
        cnf = bag.cnf()
        
        assert isinstance(cnf, Conjunction)
        assert len(cnf.constraints) == 0
    
    def test_cnf_single_constraint(self):
        """Test CNF conversion with a single constraint."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        cnf = bag.cnf()
        
        assert isinstance(cnf, Conjunction)
        # Should converge to a stable form
        assert cnf == cnf.cnf()
    
    def test_cnf_converges(self):
        """Test that CNF conversion reaches a fixpoint."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        disj = Disjunction([c1, c2])
        bag += disj
        
        cnf = bag.cnf()
        cnf2 = cnf.cnf()
        
        # Should be stable after conversion
        assert cnf == cnf2
    
    def test_cnf_with_conjunction(self):
        """Test CNF with conjunctions."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        conj = Conjunction([c1, c2])
        bag += conj
        
        cnf = bag.cnf()
        
        assert isinstance(cnf, Conjunction)
    
    def test_sat_returns_list(self):
        """Test that sat() returns a list structure."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        # Note: This will print output, but we're just testing it returns
        result = bag.sat()
        
        assert isinstance(result, list)
    
    def test_constraint_bag_with_complex_structure(self):
        """Test ConstraintBag with a complex nested structure."""
        bag = ConstraintBag()
        
        # Create a complex structure: ExactlyOne(Disjunction([...]))
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("x", "node2")
        c3 = VariableAssignedToNode("x", "node3")
        disj = Disjunction([c1, c2, c3])
        exactly_one = ExactlyOne(disj)
        
        bag += exactly_one
        
        # Should be able to walk and find all atomic constraints
        walked = list(bag.walk())
        assert c1 in walked
        assert c2 in walked
        assert c3 in walked
        
        # Should be able to build mapping
        mapping = bag.build_atomic_constraint_mapping()
        assert len(mapping) == 3
        
        # Should be able to convert to CNF
        cnf = bag.cnf()
        assert isinstance(cnf, Conjunction)
    
    def test_constraint_bag_type_safety(self):
        """Test that ConstraintBag properly handles different constraint types."""
        bag = ConstraintBag()
        
        # Add various types
        node = VariableAssignedToNode("n", "node1")
        rel = VariableAssignedToRelationship("r", "rel1")
        neg = Negation(node)
        disj = Disjunction([node, rel])
        conj = Conjunction([node, rel])
        
        bag += node
        bag += rel
        bag += neg
        bag += disj
        bag += conj
        
        assert len(bag.bag) == 5
    
    def test_constraint_bag_generator_behavior(self):
        """Test that walk() and assignments_of_variable() return generators."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        bag += c1
        
        walk_result = bag.walk()
        assert isinstance(walk_result, Generator)
        
        assignments_result = bag.assignments_of_variable("x")
        assert isinstance(assignments_result, Generator)
    
    def test_constraint_bag_immutability_of_iteration(self):
        """Test that iterating doesn't modify the bag."""
        bag = ConstraintBag()
        c1 = VariableAssignedToNode("x", "node1")
        c2 = VariableAssignedToNode("y", "node2")
        bag += c1
        bag += c2
        
        original_size = len(bag.bag)
        
        # Iterate multiple times
        list(bag)
        list(bag.walk())
        list(bag.assignments_of_variable("x"))
        
        # Size should not change
        assert len(bag.bag) == original_size
    
    def test_constraint_bag_empty_operations(self):
        """Test that operations work correctly on empty bag."""
        bag = ConstraintBag()
        
        # All operations should work without errors
        assert list(bag) == []
        assert list(bag.walk()) == []
        assert list(bag.assignments_of_variable("x")) == []
        assert bag.build_atomic_constraint_mapping() == {}
        assert bag.atomic_constraint_mapping == {}
        
        cnf = bag.cnf()
        assert isinstance(cnf, Conjunction)


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
