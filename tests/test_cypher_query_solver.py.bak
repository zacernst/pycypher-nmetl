"""
Comprehensive unit tests for the CypherQuerySolver class.

This module tests the SAT-based Cypher query solver that converts graph pattern
matching queries into boolean constraint satisfaction problems.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import Generator

from pycypher.fact_collection.solver import (
    CypherQuerySolver,
    ConstraintBag,
    Conjunction,
    Disjunction,
    ExactlyOne,
    IfThen,
    Negation,
    AtomicConstraint,
    VariableAssignedToNode,
    VariableAssignedToRelationship,
)
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from pycypher.node_classes import (
    Node,
    NodeNameLabel,
    Relationship,
    RelationshipChain,
    RelationshipLeftRight,
    TreeMixin,
)
from pycypher.solutions import Projection, ProjectionList
from pycypher.fact import (
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)


class TestCypherQuerySolverInitialization:
    """Tests for CypherQuerySolver initialization."""

    def test_initialization_with_fact_collection(self):
        """Test that CypherQuerySolver initializes with a fact collection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        assert solver.fact_collection is fact_collection

    def test_initialization_stores_fact_collection_reference(self):
        """Test that the solver stores a reference to the fact collection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        assert hasattr(solver, 'fact_collection')
        assert solver.fact_collection == fact_collection


class TestExtractQueryElements:
    """Tests for extract_query_elements method."""

    def test_extract_single_node(self):
        """Test extracting a single node from query AST."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        # Create a simple AST with one node
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        nodes, chains = solver.extract_query_elements(ast)
        
        assert len(nodes) == 1
        assert nodes[0] == node
        assert len(chains) == 0

    def test_extract_multiple_nodes(self):
        """Test extracting multiple nodes from query AST."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        node1 = Node(name_label=NodeNameLabel(name="n", label="Person"))
        node2 = Node(name_label=NodeNameLabel(name="m", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node1, node2]
        
        nodes, chains = solver.extract_query_elements(ast)
        
        assert len(nodes) == 2
        assert node1 in nodes
        assert node2 in nodes

    def test_extract_relationship_chain(self):
        """Test extracting relationship chains from query AST."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        chain = Mock(spec=RelationshipChain)
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, chain]
        
        nodes, chains = solver.extract_query_elements(ast)
        
        assert len(nodes) == 0
        assert len(chains) == 1
        assert chains[0] == chain

    def test_extract_mixed_elements(self):
        """Test extracting both nodes and relationship chains."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        node1 = Node(name_label=NodeNameLabel(name="n", label="Person"))
        node2 = Node(name_label=NodeNameLabel(name="m", label="Person"))
        chain = Mock(spec=RelationshipChain)
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node1, chain, node2]
        
        nodes, chains = solver.extract_query_elements(ast)
        
        assert len(nodes) == 2
        assert len(chains) == 1

    def test_extract_no_elements(self):
        """Test extraction when AST has no nodes or chains."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast]
        
        nodes, chains = solver.extract_query_elements(ast)
        
        assert len(nodes) == 0
        assert len(chains) == 0


class TestCreateNodeConstraints:
    """Tests for create_node_constraints method."""

    def test_create_constraints_for_single_node_single_match(self):
        """Test creating constraints when one node matches one database node."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        
        with patch('builtins.print'):  # Suppress print output
            solver.create_node_constraints([node], constraint_bag)
        
        assert len(constraint_bag.bag) == 1
        assert isinstance(constraint_bag.bag[0], ExactlyOne)

    def test_create_constraints_for_single_node_multiple_matches(self):
        """Test creating constraints when one node matches multiple database nodes."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person"),
            FactNodeHasLabel(node_id="node2", label="Person"),
            FactNodeHasLabel(node_id="node3", label="Person"),
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        
        with patch('builtins.print'):
            solver.create_node_constraints([node], constraint_bag)
        
        assert len(constraint_bag.bag) == 1
        exactly_one = constraint_bag.bag[0]
        assert isinstance(exactly_one, ExactlyOne)
        assert len(exactly_one.disjunction.constraints) == 3

    def test_create_constraints_for_multiple_nodes(self):
        """Test creating constraints for multiple query nodes."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.side_effect = [
            [FactNodeHasLabel(node_id="node1", label="Person")],
            [FactNodeHasLabel(node_id="node2", label="Company")],
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node1 = Node(name_label=NodeNameLabel(name="n", label="Person"))
        node2 = Node(name_label=NodeNameLabel(name="m", label="Company"))
        
        with patch('builtins.print'):
            solver.create_node_constraints([node1, node2], constraint_bag)
        
        assert len(constraint_bag.bag) == 2

    def test_create_constraints_verifies_variable_names(self):
        """Test that constraints use correct variable names."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node = Node(name_label=NodeNameLabel(name="custom_var", label="Person"))
        
        with patch('builtins.print'):
            solver.create_node_constraints([node], constraint_bag)
        
        exactly_one = constraint_bag.bag[0]
        assignment = exactly_one.disjunction.constraints[0]
        assert isinstance(assignment, VariableAssignedToNode)
        assert assignment.variable == "custom_var"

    def test_create_constraints_verifies_node_ids(self):
        """Test that constraints use correct node IDs from database."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="specific_id_123", label="Person")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        
        with patch('builtins.print'):
            solver.create_node_constraints([node], constraint_bag)
        
        exactly_one = constraint_bag.bag[0]
        assignment = exactly_one.disjunction.constraints[0]
        assert assignment.node_id == "specific_id_123"

    def test_create_constraints_for_empty_node_list(self):
        """Test creating constraints when no nodes are provided."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        solver.create_node_constraints([], constraint_bag)
        
        assert len(constraint_bag.bag) == 0


class TestCreateRelationshipConstraints:
    """Tests for create_relationship_constraints method."""

    def test_create_constraints_for_single_relationship(self):
        """Test creating constraints for a single relationship."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_label_facts.return_value = [
            FactRelationshipHasLabel(relationship_id="rel1", relationship_label="KNOWS")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        # Create relationship chain with relationship
        rel_name_label = NodeNameLabel(name="r", label="KNOWS")
        relationship = Mock()
        relationship.relationship.name_label = rel_name_label
        chain = Mock(spec=RelationshipChain)
        chain.relationship = relationship
        
        with patch('builtins.print'):
            solver.create_relationship_constraints([chain], constraint_bag)
        
        assert len(constraint_bag.bag) == 1
        assert isinstance(constraint_bag.bag[0], ExactlyOne)

    def test_create_constraints_for_multiple_relationship_matches(self):
        """Test creating constraints when relationship matches multiple in DB."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_label_facts.return_value = [
            FactRelationshipHasLabel(relationship_id="rel1", relationship_label="KNOWS"),
            FactRelationshipHasLabel(relationship_id="rel2", relationship_label="KNOWS"),
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        rel_name_label = NodeNameLabel(name="r", label="KNOWS")
        relationship = Mock()
        relationship.relationship.name_label = rel_name_label
        chain = Mock(spec=RelationshipChain)
        chain.relationship = relationship
        
        with patch('builtins.print'):
            solver.create_relationship_constraints([chain], constraint_bag)
        
        exactly_one = constraint_bag.bag[0]
        assert len(exactly_one.disjunction.constraints) == 2

    def test_skip_chain_without_relationship(self):
        """Test that chains without relationships are skipped."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        chain = Mock(spec=RelationshipChain)
        chain.relationship = None
        
        solver.create_relationship_constraints([chain], constraint_bag)
        
        assert len(constraint_bag.bag) == 0

    def test_create_constraints_verifies_relationship_variable_names(self):
        """Test that constraints use correct relationship variable names."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_label_facts.return_value = [
            FactRelationshipHasLabel(relationship_id="rel1", relationship_label="KNOWS")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        rel_name_label = NodeNameLabel(name="custom_rel", label="KNOWS")
        relationship = Mock()
        relationship.relationship.name_label = rel_name_label
        chain = Mock(spec=RelationshipChain)
        chain.relationship = relationship
        
        with patch('builtins.print'):
            solver.create_relationship_constraints([chain], constraint_bag)
        
        exactly_one = constraint_bag.bag[0]
        assignment = exactly_one.disjunction.constraints[0]
        assert isinstance(assignment, VariableAssignedToRelationship)
        assert assignment.variable == "custom_rel"


class TestCreateRelationshipEndpointConstraints:
    """Tests for create_relationship_endpoint_constraints method."""

    def test_create_endpoint_constraints_basic(self):
        """Test creating source and target node constraints for relationships."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_source_node_facts.return_value = [
            FactRelationshipHasSourceNode(relationship_id="rel1", source_node_id="node1")
        ]
        fact_collection.relationship_has_target_node_facts.return_value = [
            FactRelationshipHasTargetNode(relationship_id="rel1", target_node_id="node2")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        # Add relationship assignment first
        rel_assignment = VariableAssignedToRelationship("r", "rel1")
        constraint_bag.add_constraint(rel_assignment)
        
        # Create relationship chain
        rel_name_label = NodeNameLabel(name="r", label="KNOWS")
        relationship = Mock()
        relationship.relationship.name_label = rel_name_label
        
        source_node = Mock()
        source_node.name_label.name = "n"
        target_node = Mock()
        target_node.name_label.name = "m"
        
        chain = Mock(spec=RelationshipChain)
        chain.relationship = relationship
        chain.source_node = source_node
        chain.target_node = target_node
        
        solver.create_relationship_endpoint_constraints([chain], constraint_bag)
        
        # Should have added source and target constraints
        assert len(constraint_bag.bag) > 1

    def test_skip_chain_without_relationship_in_endpoint_constraints(self):
        """Test that chains without relationships are skipped."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        chain = Mock(spec=RelationshipChain)
        chain.relationship = None
        
        solver.create_relationship_endpoint_constraints([chain], constraint_bag)
        
        # No constraints should be added
        assert len(constraint_bag.bag) == 0

    def test_endpoint_constraints_create_if_then_implications(self):
        """Test that endpoint constraints create IfThen implications."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_source_node_facts.return_value = [
            FactRelationshipHasSourceNode(relationship_id="rel1", source_node_id="node1")
        ]
        fact_collection.relationship_has_target_node_facts.return_value = [
            FactRelationshipHasTargetNode(relationship_id="rel1", target_node_id="node2")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        # Add relationship assignment
        rel_assignment = VariableAssignedToRelationship("r", "rel1")
        constraint_bag.add_constraint(rel_assignment)
        
        # Create relationship chain
        rel_name_label = NodeNameLabel(name="r", label="KNOWS")
        relationship = Mock()
        relationship.relationship.name_label = rel_name_label
        
        source_node = Mock()
        source_node.name_label.name = "n"
        target_node = Mock()
        target_node.name_label.name = "m"
        
        chain = Mock(spec=RelationshipChain)
        chain.relationship = relationship
        chain.source_node = source_node
        chain.target_node = target_node
        
        solver.create_relationship_endpoint_constraints([chain], constraint_bag)
        
        # Check that IfThen constraints were added
        conjunctions = [c for c in constraint_bag.bag if isinstance(c, Conjunction)]
        assert len(conjunctions) > 0


class TestSolveQuery:
    """Tests for solve_query method."""

    def test_solve_query_returns_conjunction(self):
        """Test that solve_query returns a Conjunction in CNF."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = []
        fact_collection.relationship_has_label_facts.return_value = []
        
        solver = CypherQuerySolver(fact_collection)
        
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast]
        
        with patch('builtins.print'):
            result = solver.solve_query(ast)
        
        assert isinstance(result, Conjunction)

    def test_solve_query_extracts_elements(self):
        """Test that solve_query extracts nodes and relationships."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        with patch('builtins.print'):
            with patch.object(solver, 'extract_query_elements', wraps=solver.extract_query_elements) as mock_extract:
                result = solver.solve_query(ast)
                mock_extract.assert_called_once_with(ast)

    def test_solve_query_creates_all_constraint_types(self):
        """Test that solve_query creates node, relationship, and endpoint constraints."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        
        solver = CypherQuerySolver(fact_collection)
        
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast]
        
        with patch('builtins.print'):
            with patch.object(solver, 'create_node_constraints') as mock_node:
                with patch.object(solver, 'create_relationship_constraints') as mock_rel:
                    with patch.object(solver, 'create_relationship_endpoint_constraints') as mock_endpoint:
                        result = solver.solve_query(ast)
                        
                        mock_node.assert_called_once()
                        mock_rel.assert_called_once()
                        mock_endpoint.assert_called_once()

    def test_solve_query_converts_to_cnf(self):
        """Test that solve_query converts constraints to CNF."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person")
        ]
        
        solver = CypherQuerySolver(fact_collection)
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        with patch('builtins.print'):
            result = solver.solve_query(ast)
        
        # Result should be in CNF form (Conjunction)
        assert isinstance(result, Conjunction)


class TestGetClauses:
    """Tests for get_clauses method."""

    def test_get_clauses_returns_tuple(self):
        """Test that get_clauses returns a 3-tuple."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        cnf = Conjunction([])
        
        result = solver.get_clauses(cnf)
        
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_get_clauses_returns_clauses_list(self):
        """Test that first element is a list of clauses."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        # Create simple CNF
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        assert isinstance(clauses, list)

    def test_get_clauses_returns_mappings(self):
        """Test that second and third elements are dictionaries."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        assert isinstance(reverse_map, dict)
        assert isinstance(forward_map, dict)

    def test_get_clauses_creates_bidirectional_mapping(self):
        """Test that reverse and forward maps are inverses."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        # Check bidirectional mapping
        for constraint, var_id in forward_map.items():
            assert reverse_map[var_id] == constraint

    def test_get_clauses_handles_disjunction(self):
        """Test that disjunctions are converted to clauses."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint1 = VariableAssignedToNode("n", "node1")
        constraint2 = VariableAssignedToNode("n", "node2")
        disj = Disjunction([constraint1, constraint2])
        cnf = Conjunction([disj])
        
        clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        assert len(clauses) > 0
        assert all(isinstance(clause, list) for clause in clauses)

    def test_get_clauses_handles_atomic_constraint(self):
        """Test that atomic constraints become unit clauses."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        # Should have one unit clause
        assert len(clauses) == 1
        assert len(clauses[0]) == 1


class TestToDimacs:
    """Tests for to_dimacs method."""

    def test_to_dimacs_returns_string(self):
        """Test that to_dimacs returns a string."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        cnf = Conjunction([])
        
        result = solver.to_dimacs(cnf)
        
        assert isinstance(result, str)

    def test_to_dimacs_includes_comment_line(self):
        """Test that DIMACS output includes comment line."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        result = solver.to_dimacs(cnf)
        
        lines = result.split('\n')
        assert any(line.startswith('c ') for line in lines)

    def test_to_dimacs_includes_problem_line(self):
        """Test that DIMACS output includes problem definition line."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        result = solver.to_dimacs(cnf)
        
        lines = result.split('\n')
        assert any(line.startswith('p cnf ') for line in lines)

    def test_to_dimacs_clauses_end_with_zero(self):
        """Test that DIMACS clauses end with 0."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        constraint = VariableAssignedToNode("n", "node1")
        cnf = Conjunction([constraint])
        
        result = solver.to_dimacs(cnf)
        
        lines = result.split('\n')
        clause_lines = [line for line in lines if not line.startswith('c ') and not line.startswith('p ')]
        for line in clause_lines:
            if line.strip():
                assert line.strip().endswith(' 0')


class TestSolutionToProjection:
    """Tests for solution_to_projection method."""

    def test_solution_to_projection_single_node(self):
        """Test converting solution with single node to projection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        solution = [VariableAssignedToNode("n", "node1")]
        
        projection = solver.solution_to_projection(solution)
        
        assert isinstance(projection, Projection)
        assert projection["n"] == "node1"

    def test_solution_to_projection_multiple_nodes(self):
        """Test converting solution with multiple nodes to projection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        solution = [
            VariableAssignedToNode("n", "node1"),
            VariableAssignedToNode("m", "node2"),
        ]
        
        projection = solver.solution_to_projection(solution)
        
        assert projection["n"] == "node1"
        assert projection["m"] == "node2"

    def test_solution_to_projection_with_relationship(self):
        """Test converting solution with relationship to projection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        solution = [
            VariableAssignedToNode("n", "node1"),
            VariableAssignedToRelationship("r", "rel1"),
            VariableAssignedToNode("m", "node2"),
        ]
        
        projection = solver.solution_to_projection(solution)
        
        assert projection["n"] == "node1"
        assert projection["r"] == "rel1"
        assert projection["m"] == "node2"

    def test_solution_to_projection_empty_solution(self):
        """Test converting empty solution to projection."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        solution = []
        
        projection = solver.solution_to_projection(solution)
        
        assert isinstance(projection, Projection)
        assert len(projection.projection) == 0


class TestExtractReturnVariables:
    """Tests for _extract_return_variables method."""

    def test_extract_single_return_variable(self):
        """Test extracting single variable from RETURN clause."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        query = "MATCH (n:Person) RETURN n"
        
        variables = solver._extract_return_variables(query)
        
        assert "n" in variables

    def test_extract_multiple_return_variables(self):
        """Test extracting multiple variables from RETURN clause."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, r, m"
        
        variables = solver._extract_return_variables(query)
        
        assert "n" in variables
        assert "r" in variables
        assert "m" in variables

    def test_extract_return_variables_no_return_clause(self):
        """Test extracting variables when no RETURN clause exists."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        query = "MATCH (n:Person)"
        
        variables = solver._extract_return_variables(query)
        
        assert len(variables) == 0

    def test_extract_return_variables_with_limit(self):
        """Test extracting variables when LIMIT clause is present."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        query = "MATCH (n:Person) RETURN n LIMIT 10"
        
        variables = solver._extract_return_variables(query)
        
        assert "n" in variables
        assert "LIMIT" not in variables

    def test_extract_return_variables_with_order_by(self):
        """Test extracting variables when ORDER BY clause is present."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        query = "MATCH (n:Person) RETURN n ORDER BY n.name"
        
        variables = solver._extract_return_variables(query)
        
        assert "n" in variables
        assert "ORDER" not in variables


class TestSolverIntegration:
    """Integration tests for the complete solver workflow."""

    def test_end_to_end_simple_query(self):
        """Test complete workflow from query to CNF."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person"),
            FactNodeHasLabel(node_id="node2", label="Person"),
        ]
        
        solver = CypherQuerySolver(fact_collection)
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        with patch('builtins.print'):
            cnf = solver.solve_query(ast)
        
        # Should have CNF with constraints
        assert isinstance(cnf, Conjunction)
        assert len(cnf.constraints) > 0

    def test_end_to_end_get_clauses_from_cnf(self):
        """Test getting SAT clauses from CNF."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person"),
        ]
        
        solver = CypherQuerySolver(fact_collection)
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        with patch('builtins.print'):
            cnf = solver.solve_query(ast)
            clauses, reverse_map, forward_map = solver.get_clauses(cnf)
        
        assert len(clauses) > 0
        assert len(reverse_map) > 0
        assert len(forward_map) > 0

    def test_end_to_end_generate_dimacs(self):
        """Test generating DIMACS output from query."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = [
            FactNodeHasLabel(node_id="node1", label="Person"),
        ]
        
        solver = CypherQuerySolver(fact_collection)
        
        node = Node(name_label=NodeNameLabel(name="n", label="Person"))
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast, node]
        
        with patch('builtins.print'):
            cnf = solver.solve_query(ast)
            dimacs = solver.to_dimacs(cnf)
        
        assert isinstance(dimacs, str)
        assert 'p cnf' in dimacs

    def test_solution_conversion_workflow(self):
        """Test converting SAT solutions to Projections."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        # Create mock solution
        solution = [
            VariableAssignedToNode("n", "node1"),
            VariableAssignedToRelationship("r", "rel1"),
            VariableAssignedToNode("m", "node2"),
        ]
        
        projection = solver.solution_to_projection(solution)
        
        assert isinstance(projection, Projection)
        assert len(projection.projection) == 3
        assert projection["n"] == "node1"
        assert projection["r"] == "rel1"
        assert projection["m"] == "node2"


class TestIfThen:
    """Tests for the IfThen implication constraint class."""

    def test_initialization_stores_constraints(self):
        """Test that IfThen stores both if and then constraints."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        
        if_then = IfThen(if_constraint, then_constraint)
        
        assert if_then.if_constraint == if_constraint
        assert if_then.then_constraint == then_constraint

    def test_initialization_with_different_constraint_types(self):
        """Test IfThen with different types of constraints."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToRelationship("r", "rel1")
        
        if_then = IfThen(if_constraint, then_constraint)
        
        assert isinstance(if_then.if_constraint, VariableAssignedToNode)
        assert isinstance(if_then.then_constraint, VariableAssignedToRelationship)

    def test_walk_yields_self_first(self):
        """Test that walk() yields self before sub-constraints."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        walked = list(if_then.walk())
        
        assert walked[0] == if_then

    def test_walk_yields_if_constraint(self):
        """Test that walk() yields the if_constraint."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        walked = list(if_then.walk())
        
        assert if_constraint in walked

    def test_walk_yields_then_constraint(self):
        """Test that walk() yields the then_constraint."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        walked = list(if_then.walk())
        
        assert then_constraint in walked

    def test_walk_yields_all_elements(self):
        """Test that walk() yields all elements in correct order."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        walked = list(if_then.walk())
        
        # Should yield: if_then, if_constraint, then_constraint
        assert len(walked) == 3
        assert walked[0] == if_then
        assert walked[1] == if_constraint
        assert walked[2] == then_constraint

    def test_cnf_returns_disjunction(self):
        """Test that cnf() returns a Disjunction."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        cnf_result = if_then.cnf()
        
        assert isinstance(cnf_result, Disjunction)

    def test_cnf_creates_negation_of_if_constraint(self):
        """Test that cnf() creates ¬if_constraint ∨ then_constraint."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        cnf_result = if_then.cnf()
        
        # Should contain a Negation
        walked = list(cnf_result.walk())
        negations = [c for c in walked if isinstance(c, Negation)]
        assert len(negations) > 0

    def test_cnf_includes_then_constraint(self):
        """Test that cnf() includes the then_constraint."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        cnf_result = if_then.cnf()
        
        # then_constraint should be in the disjunction
        walked = list(cnf_result.walk())
        assert then_constraint in walked

    def test_cnf_implication_equivalence(self):
        """Test that P → Q is converted to ¬P ∨ Q."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        cnf_result = if_then.cnf()
        
        # Should be a disjunction with 2 elements
        assert isinstance(cnf_result, Disjunction)
        assert len(cnf_result.constraints) == 2

    def test_equality_same_constraints(self):
        """Test equality when both if and then constraints match."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint1, then_constraint1)
        
        if_constraint2 = VariableAssignedToNode("x", "node1")
        then_constraint2 = VariableAssignedToNode("y", "node2")
        if_then2 = IfThen(if_constraint2, then_constraint2)
        
        assert if_then1 == if_then2

    def test_equality_different_if_constraint(self):
        """Test inequality when if_constraint differs."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint1, then_constraint)
        
        if_constraint2 = VariableAssignedToNode("x", "node99")
        if_then2 = IfThen(if_constraint2, then_constraint)
        
        assert if_then1 != if_then2

    def test_equality_different_then_constraint(self):
        """Test inequality when then_constraint differs."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint, then_constraint1)
        
        then_constraint2 = VariableAssignedToNode("y", "node99")
        if_then2 = IfThen(if_constraint, then_constraint2)
        
        assert if_then1 != if_then2

    def test_equality_different_type(self):
        """Test inequality when comparing with different type."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        assert if_then != "not an IfThen"
        assert if_then != 123
        assert if_then != None

    def test_hash_consistent_for_equal_objects(self):
        """Test that equal IfThen objects have the same hash."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint1, then_constraint1)
        
        if_constraint2 = VariableAssignedToNode("x", "node1")
        then_constraint2 = VariableAssignedToNode("y", "node2")
        if_then2 = IfThen(if_constraint2, then_constraint2)
        
        assert hash(if_then1) == hash(if_then2)

    def test_hash_different_for_unequal_objects(self):
        """Test that unequal IfThen objects have different hashes (usually)."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint1, then_constraint1)
        
        if_constraint2 = VariableAssignedToNode("x", "node99")
        then_constraint2 = VariableAssignedToNode("y", "node99")
        if_then2 = IfThen(if_constraint2, then_constraint2)
        
        # Usually different, but hash collisions are possible
        assert hash(if_then1) != hash(if_then2)

    def test_usable_in_set(self):
        """Test that IfThen objects can be used in sets."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        if_then1 = IfThen(if_constraint1, then_constraint1)
        
        if_constraint2 = VariableAssignedToNode("x", "node1")
        then_constraint2 = VariableAssignedToNode("y", "node2")
        if_then2 = IfThen(if_constraint2, then_constraint2)
        
        constraint_set = {if_then1, if_then2}
        
        # Should have only one element since they're equal
        assert len(constraint_set) == 1

    def test_usable_in_dict(self):
        """Test that IfThen objects can be used as dictionary keys."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        constraint_dict = {if_then: "test_value"}
        
        assert constraint_dict[if_then] == "test_value"

    def test_nested_if_then_walk(self):
        """Test walk() with nested IfThen constraints."""
        if_constraint1 = VariableAssignedToNode("x", "node1")
        then_constraint1 = VariableAssignedToNode("y", "node2")
        inner_if_then = IfThen(if_constraint1, then_constraint1)
        
        if_constraint2 = VariableAssignedToNode("z", "node3")
        outer_if_then = IfThen(if_constraint2, inner_if_then)
        
        walked = list(outer_if_then.walk())
        
        # Should include outer, if_constraint2, inner_if_then, and its contents
        assert outer_if_then in walked
        assert if_constraint2 in walked
        assert inner_if_then in walked

    def test_cnf_with_complex_constraints(self):
        """Test cnf() conversion with complex nested constraints."""
        if_constraint = VariableAssignedToNode("x", "node1")
        then_constraint = VariableAssignedToNode("y", "node2")
        if_then = IfThen(if_constraint, then_constraint)
        
        # Convert to CNF multiple times - should be idempotent
        cnf1 = if_then.cnf()
        cnf2 = cnf1.cnf()
        
        # Both should be Disjunctions
        assert isinstance(cnf1, Disjunction)
        assert isinstance(cnf2, Disjunction)

    def test_if_then_with_disjunction_as_if_constraint(self):
        """Test IfThen where if_constraint is a Disjunction."""
        constraint1 = VariableAssignedToNode("x", "node1")
        constraint2 = VariableAssignedToNode("x", "node2")
        if_constraint = Disjunction([constraint1, constraint2])
        then_constraint = VariableAssignedToNode("y", "node3")
        
        if_then = IfThen(if_constraint, then_constraint)
        
        assert isinstance(if_then.if_constraint, Disjunction)
        assert if_then.then_constraint == then_constraint

    def test_if_then_with_conjunction_as_then_constraint(self):
        """Test IfThen where then_constraint is a Conjunction."""
        if_constraint = VariableAssignedToNode("x", "node1")
        constraint1 = VariableAssignedToNode("y", "node2")
        constraint2 = VariableAssignedToNode("z", "node3")
        then_constraint = Conjunction([constraint1, constraint2])
        
        if_then = IfThen(if_constraint, then_constraint)
        
        assert if_then.if_constraint == if_constraint
        assert isinstance(if_then.then_constraint, Conjunction)

    def test_multiple_if_then_in_constraint_bag(self):
        """Test adding multiple IfThen constraints to a ConstraintBag."""
        if_then1 = IfThen(
            VariableAssignedToNode("x", "node1"),
            VariableAssignedToNode("y", "node2")
        )
        if_then2 = IfThen(
            VariableAssignedToNode("z", "node3"),
            VariableAssignedToNode("w", "node4")
        )
        
        bag = ConstraintBag()
        bag.add_constraint(if_then1)
        bag.add_constraint(if_then2)
        
        assert len(bag.bag) == 2
        assert if_then1 in bag.bag
        assert if_then2 in bag.bag

    def test_if_then_cnf_conversion_in_constraint_bag(self):
        """Test that IfThen converts to CNF correctly within ConstraintBag."""
        if_then = IfThen(
            VariableAssignedToNode("x", "node1"),
            VariableAssignedToNode("y", "node2")
        )
        
        bag = ConstraintBag()
        bag.add_constraint(if_then)
        
        cnf = bag.cnf()
        
        # Should produce a Conjunction containing the converted IfThen
        assert isinstance(cnf, Conjunction)


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_handle_empty_ast(self):
        """Test handling AST with no elements."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        solver = CypherQuerySolver(fact_collection)
        
        ast = Mock(spec=TreeMixin)
        ast.walk.return_value = [ast]
        
        with patch('builtins.print'):
            cnf = solver.solve_query(ast)
        
        assert isinstance(cnf, Conjunction)

    def test_handle_node_with_no_database_matches(self):
        """Test handling query node that matches no database nodes."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.node_has_specific_label_facts.return_value = []
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        node = Node(name_label=NodeNameLabel(name="n", label="NonexistentLabel"))
        
        with patch('builtins.print'):
            solver.create_node_constraints([node], constraint_bag)
        
        # Should still create ExactlyOne with empty disjunction
        assert len(constraint_bag.bag) == 1

    def test_multiple_relationship_chains(self):
        """Test handling multiple relationship chains in one query."""
        fact_collection = Mock(spec=FoundationDBFactCollection)
        fact_collection.relationship_has_label_facts.return_value = []
        
        solver = CypherQuerySolver(fact_collection)
        constraint_bag = ConstraintBag()
        
        chain1 = Mock(spec=RelationshipChain)
        chain1.relationship = None
        chain2 = Mock(spec=RelationshipChain)
        chain2.relationship = None
        
        solver.create_relationship_constraints([chain1, chain2], constraint_bag)
        
        # Both chains have no relationship, so no constraints added
        assert len(constraint_bag.bag) == 0
