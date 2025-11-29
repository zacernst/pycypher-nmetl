from __future__ import annotations

from pycypher.node_classes import Node, RelationshipChain
from pycypher.cypher_parser import CypherParser
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection

from typing import Any, Generator
import itertools
import copy


class ConstraintBag:
    """
    A collection of constraints for SAT-based query solving.
    
    ConstraintBag manages a set of logical constraints that can be converted to
    Conjunctive Normal Form (CNF) and solved using SAT solvers. It provides utilities
    for constraint management, CNF conversion, and SAT encoding.
    
    Attributes:
        bag: Set of constraints in the collection
        _atomic_constraint_mapping: Cached mapping of atomic constraints to integer IDs
    
    Example:
        >>> bag = ConstraintBag()
        >>> bag += VariableAssignedToNode('x', 'node1')
        >>> cnf = bag.cnf()
        >>> solution = bag.sat()
    """
    
    def __init__(self):
        """Initialize an empty ConstraintBag."""
        self.bag: set[Any] = set()
        self._atomic_constraint_mapping = None
    
    def add_constraint(self, constraint: Any) -> int:
        """
        Add a constraint to the bag.
        
        Args:
            constraint: Any constraint object to add to the collection
            
        Returns:
            Integer (currently unused, for backward compatibility)
        """
        self.bag.add(constraint)
    
    def __iadd__(self, other) -> 'ConstraintBag':
        """
        Add a constraint using the += operator.
        
        Args:
            other: Constraint to add
            
        Returns:
            Self for method chaining
        """
        self.add_constraint(other)
        return self
    
    def __repr__(self) -> str:
        """Return string representation showing number of constraints."""
        return f"ConstraintBag({len(self.bag)})"
    
    def __iter__(self) -> Any:
        """Iterate over constraints in the bag."""
        return iter(self.bag)
    
    @property
    def atomic_constraint_mapping(self) -> dict[AtomicConstraint, int]:
        """
        Get mapping of atomic constraints to integer variable IDs for SAT encoding.
        
        This property is lazily computed and cached. Each unique atomic constraint
        is assigned a sequential integer ID starting from 1, which is used for
        SAT solver encoding.
        
        Returns:
            Dictionary mapping AtomicConstraint instances to their integer IDs
        """
        if self._atomic_constraint_mapping is None:
            self._atomic_constraint_mapping = self.build_atomic_constraint_mapping()
        return self._atomic_constraint_mapping
    
    def assignments_of_variable(self, variable: str) -> Generator[Any, None, None]:
        """
        Find all assignments for a given variable name.
        
        Walks through all constraints and yields those that assign the specified
        variable to a node or relationship.
        
        Args:
            variable: Name of the variable to find assignments for
            
        Yields:
            VariableAssignedToNode or VariableAssignedToRelationship instances
        """
        for constraint in copy.deepcopy(self).walk():
            match constraint:
                case VariableAssignedToNode() | VariableAssignedToRelationship():
                    if constraint.variable == variable:
                        yield constraint
                case _:
                    pass
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through all constraints and sub-constraints.
        
        Traverses the constraint tree depth-first, yielding each constraint
        that has a walk() method, then yielding the constraint itself.
        
        Yields:
            All constraints and sub-constraints in the bag
        """
        for constraint in self.bag:
            if hasattr(constraint, 'walk'):
                yield from constraint.walk()
            yield constraint
    
    def build_atomic_constraint_mapping(self) -> dict[AtomicConstraint, int]:
        """
        Build mapping from atomic constraints to sequential integer IDs.
        
        Walks through all constraints and assigns each unique atomic constraint
        a sequential integer starting from 1. This mapping is used for SAT encoding.
        
        Returns:
            Dictionary mapping AtomicConstraint instances to integer IDs
        """
        mapping: dict[int, AtomicConstraint] = {}
        constraint_id: int = 1
        for constraint in self.walk():
            if isinstance(constraint, AtomicConstraint) and constraint not in mapping:
                mapping[constraint] = constraint_id
                constraint_id += 1
        return mapping
    
    def cnf(self):
        """
        Convert all constraints to Conjunctive Normal Form (CNF).
        
        Iteratively applies CNF conversion rules until a fixpoint is reached,
        ensuring the result is in proper CNF form suitable for SAT solvers.
        
        Returns:
            Conjunction representing the CNF form of all constraints
        """
        conjunction: Conjunction = Conjunction([])
        for constraint in self.bag:
            conjunction += constraint
        iteration = conjunction.cnf()
        next_iteration = iteration.cnf()
        while iteration != next_iteration:
            iteration = next_iteration
            next_iteration = iteration.cnf()
        return next_iteration
    
    def sat(self) -> list[list[int]]:
        """
        Convert constraints to SAT encoding.
        
        Converts the constraint bag to CNF and then to SAT format using
        the atomic constraint mapping. Currently prints the CNF form for debugging.
        
        Returns:
            List of lists of integers representing SAT clauses
            
        Note:
            This is a placeholder for SAT solver integration. In production,
            this should interface with an actual SAT solver library.
        """
        # Placeholder for SAT solver integrationr
        cnf = self.cnf()
        print("CNF Form:", cnf)
        # Here you would integrate with a SAT solver library
        return cnf.sat(atomic_constraint_mapping=self.atomic_constraint_mapping)


class AtomicConstraint:
    """
    Base class for atomic (indivisible) constraints.
    
    Atomic constraints are the basic building blocks that cannot be further
    decomposed. They are assigned integer IDs for SAT encoding and are already
    in CNF form (they represent themselves).
    
    Subclasses should represent specific types of atomic constraints such as
    variable assignments to nodes or relationships.
    """
    
    def cnf(self) -> Any:
        """
        Return CNF form of this constraint (itself, as it's already atomic).
        
        Returns:
            Self, as atomic constraints are already in CNF
        """
        return self
    
    def sat(self, atomic_constraint_mapping: dict[AtomicConstraint, int]) -> int:
        """
        Get SAT encoding integer for this atomic constraint.
        
        Args:
            atomic_constraint_mapping: Mapping from atomic constraints to integer IDs
            
        Returns:
            Integer ID representing this constraint in SAT encoding
        """
        return atomic_constraint_mapping[self]


class VariableAssignedToNode(AtomicConstraint):
    """
    Atomic constraint representing assignment of a variable to a specific node.
    
    This constraint asserts that a query variable (e.g., 'x' in MATCH (x:Label))
    is bound to a specific node identified by node_id.
    
    Attributes:
        variable: Name of the query variable
        node_id: Unique identifier of the node
        constraint_id: Optional integer ID for this constraint
    
    Example:
        >>> constraint = VariableAssignedToNode('person', 'node_123')
        >>> constraint.variable
        'person'
        >>> constraint.node_id
        'node_123'
    """
    
    def __init__(self, variable: str, node_id: str):
        """
        Initialize a variable-to-node assignment constraint.
        
        Args:
            variable: Name of the query variable being assigned
            node_id: Unique identifier of the node being assigned to
        """
        self.variable = variable
        self.node_id = node_id
        self.constraint_id: int | None = None
    
    def __repr__(self) -> str:
        """Return string representation of the assignment."""
        return f"VariableAssignedToNode(variable={self.variable}, node_id={self.node_id})"
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on variable and node_id.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both variable and node_id match, False otherwise
        """
        if not isinstance(other, VariableAssignedToNode):
            return False
        return self.variable == other.variable and self.node_id == other.node_id
    
    def __hash__(self) -> int:
        """Return hash based on variable and node_id tuple."""
        return hash((self.variable, self.node_id))
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Yield self for constraint tree traversal.
        
        Yields:
            This constraint instance
        """
        yield self


class VariableAssignedToRelationship(AtomicConstraint):
    """
    Atomic constraint representing assignment of a variable to a specific relationship.
    
    This constraint asserts that a query variable (e.g., 'r' in MATCH ()-[r:TYPE]->())
    is bound to a specific relationship identified by relationship_id.
    
    Attributes:
        variable: Name of the query variable
        relationship_id: Unique identifier of the relationship
        constraint_id: Optional integer ID for this constraint
    
    Example:
        >>> constraint = VariableAssignedToRelationship('knows', 'rel_456')
        >>> constraint.variable
        'knows'
        >>> constraint.relationship_id
        'rel_456'
    """
    
    def __init__(self, variable: str, relationship_id: str):
        """
        Initialize a variable-to-relationship assignment constraint.
        
        Args:
            variable: Name of the query variable being assigned
            relationship_id: Unique identifier of the relationship being assigned to
        """
        self.variable = variable
        self.relationship_id = relationship_id
        self.constraint_id: int | None = None
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Yield self for constraint tree traversal.
        
        Yields:
            This constraint instance
        """
        yield self
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on variable and relationship_id.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both variable and relationship_id match, False otherwise
        """
        if not isinstance(other, VariableAssignedToRelationship):
            return False
        return self.variable == other.variable and self.relationship_id == other.relationship_id

    def __hash__(self) -> int:
        """Return hash based on variable and relationship_id tuple."""
        return hash((self.variable, self.relationship_id))


class IfThen:
    """
    Logical implication constraint: if P then Q.
    
    Represents a conditional constraint where if the if_constraint is satisfied,
    then the then_constraint must also be satisfied. In logic: P → Q, which is
    equivalent to ¬P ∨ Q in CNF.
    
    Attributes:
        if_constraint: The antecedent (condition) constraint
        then_constraint: The consequent (result) constraint
    
    Example:
        >>> # If variable x is node1, then variable y must be node2
        >>> if_then = IfThen(
        ...     VariableAssignedToNode('x', 'node1'),
        ...     VariableAssignedToNode('y', 'node2')
        ... )
    """
    
    def __init__(self, if_constraint: Any, then_constraint: Any):
        """
        Initialize an implication constraint.
        
        Args:
            if_constraint: Constraint that serves as the condition
            then_constraint: Constraint that must hold if condition is true
        """
        self.if_constraint = if_constraint
        self.then_constraint = then_constraint
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this constraint and its sub-constraints.
        
        Yields:
            Self, followed by all sub-constraints from if_constraint and then_constraint
        """
        yield self
        yield from self.if_constraint.walk()
        yield from self.then_constraint.walk()
    
    def cnf(self) -> Disjunction:
        """
        Convert implication to CNF using the equivalence: P → Q ≡ ¬P ∨ Q.
        
        Returns:
            Disjunction representing the CNF form (¬if_constraint ∨ then_constraint)
        """
        disjunction = Disjunction([])
        disjunction += Negation(self.if_constraint)
        disjunction += self.then_constraint
        return disjunction.cnf()
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on both if_constraint and then_constraint.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both constraints match, False otherwise
        """
        if not isinstance(other, IfThen):
            return False
        return self.if_constraint == other.if_constraint and self.then_constraint == other.then_constraint
    
    def __hash__(self) -> int:
        """Return hash based on both constraints."""
        return hash((self.if_constraint, self.then_constraint))


class Negation:
    """
    Logical negation of a constraint.
    
    Represents the negation (NOT) of another constraint. When converted to CNF,
    applies De Morgan's laws to push negation down to atomic constraints.
    
    De Morgan's laws:
        - ¬(P ∨ Q) ≡ ¬P ∧ ¬Q
        - ¬(P ∧ Q) ≡ ¬P ∨ ¬Q
        - ¬¬P ≡ P
    
    Attributes:
        constraint: The constraint being negated
    
    Example:
        >>> neg = Negation(VariableAssignedToNode('x', 'node1'))
        >>> # Represents: x is NOT assigned to node1
    """
    
    def __init__(self, constraint: Any):
        """
        Initialize a negation constraint.
        
        Args:
            constraint: The constraint to negate
        """
        self.constraint = constraint

    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this negation and its sub-constraints.
        
        Yields:
            Self, followed by all sub-constraints from the negated constraint
        """
        yield self
        yield from self.constraint.walk()
    
    def cnf(self) -> Any:
        """
        Convert negation to CNF by applying De Morgan's laws.
        
        Pushes negation down through disjunctions and conjunctions until
        it reaches atomic constraints.
        
        Returns:
            CNF form with negation pushed to atomic level
        """
        match self.constraint:
            case Disjunction():
                conjunction = Conjunction([])
                for disjunct in self.constraint.constraints:
                    conjunction += Negation(disjunct)
                return conjunction.cnf()
            case Conjunction():
                disjunction = Disjunction([])
                for conjunct in self.constraint.constraints:
                    disjunction += Negation(conjunct)
                return disjunction.cnf()
            case Negation(constraint=inner_constraint):
                return inner_constraint.cnf()
            case _:
                return Negation(constraint=self.constraint.cnf())
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on the negated constraint.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both negate the same constraint, False otherwise
        """
        if not isinstance(other, Negation):
            return False
        return self.constraint == other.constraint

    def __hash__(self) -> int:
        """Return hash based on the negated constraint."""
        return hash(self.constraint)
    
    def sat(self, atomic_constraint_mapping: dict[AtomicConstraint, int]) -> int:
        """
        Get SAT encoding for negated constraint.
        
        In SAT encoding, negation is represented by multiplying the variable ID by -1.
        
        Args:
            atomic_constraint_mapping: Mapping from atomic constraints to integer IDs
            
        Returns:
            Negative integer representing the negated constraint
        """
        return -1 * self.constraint.sat(atomic_constraint_mapping=atomic_constraint_mapping)


class AtMostOne:
    """
    Cardinality constraint: at most one of the given options can be true.
    
    Ensures that among a set of constraints in a disjunction, at most one
    can be satisfied. This is implemented by creating pairwise negations:
    for each pair of constraints (A, B), add (¬A ∨ ¬B).
    
    Attributes:
        disjunction: Disjunction containing the constraints
    
    Example:
        >>> # At most one of {x=node1, x=node2, x=node3} can be true
        >>> options = Disjunction([
        ...     VariableAssignedToNode('x', 'node1'),
        ...     VariableAssignedToNode('x', 'node2'),
        ...     VariableAssignedToNode('x', 'node3')
        ... ])
        >>> at_most_one = AtMostOne(options)
    """
    
    def __init__(self, disjunction: Disjunction):
        """
        Initialize an at-most-one cardinality constraint.
        
        Args:
            disjunction: Disjunction of constraints where at most one can be true
        """
        self.disjunction = disjunction
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this constraint and the disjunction.
        
        Yields:
            Self, followed by all constraints in the disjunction
        """
        yield self
        yield from self.disjunction.walk()
    
    def cnf(self) -> Conjunction:
        """
        Convert at-most-one constraint to CNF.
        
        For n constraints, this creates n(n-1)/2 pairwise exclusion clauses.
        For each pair (A, B), adds the clause (¬A ∨ ¬B) meaning "not both A and B".
        
        Returns:
            Conjunction of pairwise negation disjunctions
        """
        conjunction = Conjunction([])
        for disjunction_1, disjunction_2 in itertools.combinations(self.disjunction.constraints, 2):
            disjunction = Disjunction([Negation(disjunction_1), Negation(disjunction_2)])
            conjunction.add_constraint(disjunction)
        return conjunction.cnf()
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on the disjunction.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if disjunctions match, False otherwise
        """
        if not isinstance(other, AtMostOne):
            return False
        return self.disjunction == other.disjunction

    def __hash__(self) -> int:
        """Return hash based on the disjunction."""
        return hash(self.disjunction)


class Disjunction:
    """
    Logical disjunction (OR) of multiple constraints.
    
    Represents P₁ ∨ P₂ ∨ ... ∨ Pₙ, where at least one constraint must be satisfied.
    When converted to CNF, nested disjunctions are flattened.
    
    Attributes:
        constraints: List of constraints to be OR'ed together
    
    Example:
        >>> # x can be node1 OR node2 OR node3
        >>> disj = Disjunction([
        ...     VariableAssignedToNode('x', 'node1'),
        ...     VariableAssignedToNode('x', 'node2'),
        ...     VariableAssignedToNode('x', 'node3')
        ... ])
    """
    
    def __init__(self, constraints: list[Any]):
        """
        Initialize a disjunction.
        
        Args:
            constraints: List of constraints to combine with OR
        """
        self.constraints = constraints
    
    def add_constraint(self, constraint: Any):
        """
        Add a constraint to the disjunction.
        
        Args:
            constraint: Constraint to add
        """
        self.constraints.append(constraint)
    
    def __iadd__(self, other) -> 'Disjunction':
        """
        Add a constraint using the += operator.
        
        Args:
            other: Constraint to add
            
        Returns:
            Self for method chaining
        """
        self.add_constraint(other)
        return self
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this disjunction and all sub-constraints.
        
        Yields:
            Self, followed by all constraints and their sub-constraints
        """
        yield self
        for constraint in self.constraints:
            yield from constraint.walk()
    
    def cnf(self):
        """
        Convert disjunction to CNF by flattening nested disjunctions.
        
        Recursively converts each constraint to CNF, then flattens any
        nested disjunctions into a single-level disjunction.
        
        Returns:
            Flattened Disjunction in CNF form
        """
        out = Disjunction([c.cnf() for c in self.constraints])
        inner_disjunctions = [c for c in out.constraints if isinstance(c, Disjunction)]
        inner_non_disjunctions = [c for c in out.constraints if not isinstance(c, Disjunction)]
        final_disjunction = Disjunction([])
        for inner_disjunction in inner_disjunctions:
            for inner_constraint in inner_disjunction.constraints:
                final_disjunction += inner_constraint
        for inner_non_disjunction in inner_non_disjunctions:
            final_disjunction += inner_non_disjunction
        return final_disjunction

    
    def __eq__(self, other) -> bool:
        """
        Check equality based on constraint sets (order-independent).
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both contain the same constraints, False otherwise
        """
        if not isinstance(other, Disjunction):
            return False
        return set(self.constraints) == set(other.constraints)

    def __hash__(self) -> int:
        """Return hash based on frozenset of constraints."""
        return hash(frozenset(self.constraints))
    
    def sat(self, atomic_constraint_mapping: dict[AtomicConstraint, int]) -> list[int]:
        """
        Get SAT encoding for this disjunction.
        
        Collects SAT representations of all constraints in the disjunction,
        forming a single SAT clause.
        
        Args:
            atomic_constraint_mapping: Mapping from atomic constraints to integer IDs
            
        Returns:
            List of integers representing the SAT clause
        """
        results = []
        for constraint in self.constraints:
            result = constraint.sat(atomic_constraint_mapping=atomic_constraint_mapping)
            if result is not None:
                results.append(result)
        return results


class ExactlyOne:
    """
    Cardinality constraint: exactly one of the given options must be true.
    
    Combines "at least one" (the disjunction itself) with "at most one"
    to ensure exactly one constraint is satisfied. This is commonly used
    for variable assignments where each variable must map to exactly one value.
    
    Attributes:
        disjunction: Disjunction containing the constraints
    
    Example:
        >>> # x must be assigned to exactly one node
        >>> options = Disjunction([
        ...     VariableAssignedToNode('x', 'node1'),
        ...     VariableAssignedToNode('x', 'node2'),
        ...     VariableAssignedToNode('x', 'node3')
        ... ])
        >>> exactly_one = ExactlyOne(options)
    """
    
    def __init__(self, disjunction: Disjunction):
        """
        Initialize an exactly-one cardinality constraint.
        
        Args:
            disjunction: Disjunction of constraints where exactly one must be true
        """
        self.disjunction = disjunction
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this constraint and the disjunction.
        
        Yields:
            Self, followed by all constraints in the disjunction
        """
        yield self
        yield from self.disjunction.walk()
    
    def cnf(self) -> Any:
        """
        Convert exactly-one constraint to CNF.
        
        Combines:
        - At least one: The disjunction itself
        - At most one: Pairwise negations from AtMostOne
        
        Returns:
            Conjunction combining both constraints in CNF form
        """
        at_least_one = self.disjunction.cnf()
        at_most_one = AtMostOne(self.disjunction).cnf()
        conjunction = Conjunction([])
        conjunction += at_least_one
        conjunction += at_most_one
        return conjunction.cnf()
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on the disjunction.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if disjunctions match, False otherwise
        """
        if not isinstance(other, ExactlyOne):
            return False
        return self.disjunction == other.disjunction
    
    def __hash__(self) -> int:
        """Return hash based on the disjunction."""
        return hash(self.disjunction)


class Conjunction:
    """
    Logical conjunction (AND) of multiple constraints.
    
    Represents P₁ ∧ P₂ ∧ ... ∧ Pₙ, where all constraints must be satisfied.
    In CNF, this is the top-level structure containing all clauses.
    When converted to CNF, nested conjunctions are flattened.
    
    Attributes:
        constraints: List of constraints to be AND'ed together
    
    Example:
        >>> # x must be node1 AND y must be node2
        >>> conj = Conjunction([
        ...     VariableAssignedToNode('x', 'node1'),
        ...     VariableAssignedToNode('y', 'node2')
        ... ])
    """
    
    def __init__(self, constraints: list[Any]):
        """
        Initialize a conjunction.
        
        Args:
            constraints: List of constraints to combine with AND
        """
        self.constraints = constraints
    
    def add_constraint(self, constraint: Any):
        """
        Add a constraint to the conjunction.
        
        Args:
            constraint: Constraint to add
        """
        self.constraints.append(constraint)
    
    def __iadd__(self, other) -> 'Conjunction':
        """
        Add a constraint using the += operator.
        
        Args:
            other: Constraint to add
            
        Returns:
            Self for method chaining
        """
        self.add_constraint(other)
        return self
    
    def walk(self) -> Generator[Any, None, None]:
        """
        Recursively walk through this conjunction and all sub-constraints.
        
        Yields:
            Self, followed by all constraints and their sub-constraints
        """
        yield self
        for constraint in self.constraints:
            yield from constraint.walk()
    
    def cnf(self):
        """
        Convert conjunction to CNF by flattening nested conjunctions.
        
        Recursively converts each constraint to CNF, then flattens any
        nested conjunctions into a single-level conjunction. This ensures
        the final form is a conjunction of disjunctions.
        
        Returns:
            Flattened Conjunction in CNF form
        """
        out = Conjunction([c.cnf() for c in self.constraints])
        inner_conjunctions = [c for c in out.constraints if isinstance(c, Conjunction)]
        inner_non_conjunctions = [c for c in out.constraints if not isinstance(c, Conjunction)]
        final_conjunction = Conjunction([])
        for inner_conjunction in inner_conjunctions:
            for inner_constraint in inner_conjunction.constraints:
                final_conjunction += inner_constraint
        for inner_non_conjunction in inner_non_conjunctions:
            final_conjunction += inner_non_conjunction
        return final_conjunction
    
    def __eq__(self, other) -> bool:
        """
        Check equality based on constraint sets (order-independent).
        
        Args:
            other: Object to compare with
            
        Returns:
            True if both contain the same constraints, False otherwise
        """
        if not isinstance(other, Conjunction):
            return False
        return set(self.constraints) == set(other.constraints)
    
    def __hash__(self) -> int:
        """Return hash based on frozenset of constraints."""
        return hash(frozenset(self.constraints))

    def sat(self, atomic_constraint_mapping: dict[AtomicConstraint, int]) -> list[int]:
        """
        Get SAT encoding for this conjunction.
        
        Collects SAT representations of all constraints in the conjunction.
        In proper CNF, this should return a list of clauses (lists of integers).
        
        Args:
            atomic_constraint_mapping: Mapping from atomic constraints to integer IDs
            
        Returns:
            List of integers or lists representing SAT clauses
        """
        results = []
        for constraint in self.constraints:
            result = constraint.sat(atomic_constraint_mapping=atomic_constraint_mapping)
            if result is not None:
                results.append(result)
        return results
    
    def to_dimacs(self, atomic_constraint_mapping: dict[AtomicConstraint, int]) -> str:
        """
        Convert CNF to DIMACS format for SAT solvers.
        
        DIMACS CNF format:
        - Comment lines start with 'c'
        - Problem line: 'p cnf <num_vars> <num_clauses>'
        - Each clause is a space-separated list of integers ending with 0
        - Positive integer n represents variable n
        - Negative integer -n represents negation of variable n
        
        Args:
            atomic_constraint_mapping: Dictionary mapping atomic constraints to integer variable IDs
            
        Returns:
            String in DIMACS CNF format ready for SAT solver input
            
        Example:
            >>> cnf = bag.cnf()
            >>> dimacs = cnf.to_dimacs(bag.atomic_constraint_mapping)
            >>> with open('problem.cnf', 'w') as f:
            ...     f.write(dimacs)
        """
        clauses = []
        
        # Process each constraint in the conjunction
        for constraint in self.constraints:
            if isinstance(constraint, Disjunction):
                # Disjunction becomes a clause
                clause = constraint.sat(atomic_constraint_mapping)
                if isinstance(clause, list):
                    clauses.append(clause)
                else:
                    clauses.append([clause])
            elif isinstance(constraint, (AtomicConstraint, Negation)):
                # Single literal becomes a unit clause
                literal = constraint.sat(atomic_constraint_mapping)
                clauses.append([literal])
            else:
                # For other types, try to get SAT representation
                result = constraint.sat(atomic_constraint_mapping)
                if isinstance(result, list):
                    clauses.append(result)
                else:
                    clauses.append([result])
        
        num_vars = len(atomic_constraint_mapping)
        num_clauses = len(clauses)
        
        # Build DIMACS string
        lines = [
            "c CNF generated from constraint solver",
            f"p cnf {num_vars} {num_clauses}"
        ]
        
        for clause in clauses:
            # Each clause is a list of integers, terminated by 0
            clause_str = " ".join(map(str, clause)) + " 0"
            lines.append(clause_str)
        
        return "\n".join(lines)


if __name__ == '__main__':
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
    
