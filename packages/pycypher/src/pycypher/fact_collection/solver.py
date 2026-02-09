from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generator, Optional

from pycypher.cypher_parser import CypherParser
from pycypher.node_classes import Node, RelationshipChain, TreeMixin

if TYPE_CHECKING:
    from pycypher.fact_collection.foundationdb import (
        FoundationDBFactCollection,
    )

import copy
import itertools

import pycosat
from pycypher.solutions import Projection, ProjectionList
from pysat.solvers import Glucose3


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

    def __iadd__(self, other) -> "ConstraintBag":
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
            self._atomic_constraint_mapping = (
                self.build_atomic_constraint_mapping()
            )
        return self._atomic_constraint_mapping

    def assignments_of_variable(
        self, variable: str
    ) -> Generator[Any, None, None]:
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
                case (
                    VariableAssignedToNode() | VariableAssignedToRelationship()
                ):
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
            if hasattr(constraint, "walk"):
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
            if (
                isinstance(constraint, AtomicConstraint)
                and constraint not in mapping
            ):
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
        cnf = self.cnf()
        return cnf.sat(
            atomic_constraint_mapping=self.atomic_constraint_mapping
        )


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

    def sat(
        self, atomic_constraint_mapping: dict[AtomicConstraint, int]
    ) -> int:
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
        return (
            self.variable == other.variable and self.node_id == other.node_id
        )

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
        return (
            self.variable == other.variable
            and self.relationship_id == other.relationship_id
        )

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
        return (
            self.if_constraint == other.if_constraint
            and self.then_constraint == other.then_constraint
        )

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

    def sat(
        self, atomic_constraint_mapping: dict[AtomicConstraint, int]
    ) -> int:
        """
        Get SAT encoding for negated constraint.

        In SAT encoding, negation is represented by multiplying the variable ID by -1.

        Args:
            atomic_constraint_mapping: Mapping from atomic constraints to integer IDs

        Returns:
            Negative integer representing the negated constraint
        """
        return -1 * self.constraint.sat(
            atomic_constraint_mapping=atomic_constraint_mapping
        )


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
        for disjunction_1, disjunction_2 in itertools.combinations(
            self.disjunction.constraints, 2
        ):
            disjunction = Disjunction(
                [Negation(disjunction_1), Negation(disjunction_2)]
            )
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

    def __iadd__(self, other) -> "Disjunction":
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
        inner_disjunctions = [
            c for c in out.constraints if isinstance(c, Disjunction)
        ]
        inner_non_disjunctions = [
            c for c in out.constraints if not isinstance(c, Disjunction)
        ]
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

    def sat(
        self, atomic_constraint_mapping: dict[AtomicConstraint, int]
    ) -> list[int]:
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
            result = constraint.sat(
                atomic_constraint_mapping=atomic_constraint_mapping
            )
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

    def __iadd__(self, other) -> "Conjunction":
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
        inner_conjunctions = [
            c for c in out.constraints if isinstance(c, Conjunction)
        ]
        inner_non_conjunctions = [
            c for c in out.constraints if not isinstance(c, Conjunction)
        ]
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

    def sat(
        self, atomic_constraint_mapping: dict[AtomicConstraint, int]
    ) -> list[int]:
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
            result = constraint.sat(
                atomic_constraint_mapping=atomic_constraint_mapping
            )
            if result is not None:
                results.append(result)
        return results

    def to_dimacs(
        self, atomic_constraint_mapping: dict[AtomicConstraint, int]
    ) -> str:
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
            f"p cnf {num_vars} {num_clauses}",
        ]

        for clause in clauses:
            # Each clause is a list of integers, terminated by 0
            clause_str = " ".join(map(str, clause)) + " 0"
            lines.append(clause_str)

        return "\n".join(lines)


class CypherQuerySolver:
    """
    SAT-based solver for Cypher graph queries.

    This class converts Cypher graph pattern matching queries into boolean
    constraint satisfaction problems that can be solved using SAT solvers.
    It extracts nodes and relationships from the query, maps them to possible
    database entities, and generates constraints in CNF.

    The solving process:
    1. Parse Cypher query to extract graph patterns
    2. For each query variable, find possible database entity matches
    3. Create "exactly one" constraints for variable assignments
    4. Create implication constraints for relationship endpoints
    5. Convert all constraints to CNF
    6. Output in DIMACS format for SAT solver

    Attributes:
        fact_collection: Database fact collection for querying graph data

    Example:
        >>> solver = CypherQuerySolver(fact_collection)
        >>> cnf = solver.solve_query("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m")
        >>> dimacs = solver.to_dimacs(cnf)
    """

    def __init__(self, fact_collection: FoundationDBFactCollection):
        """
        Initialize the Cypher query solver.

        Args:
            fact_collection: FoundationDB fact collection for graph data access
        """
        self.fact_collection = fact_collection

    def extract_query_elements(
        self, ast: TreeMixin
    ) -> tuple[list[Node], list[RelationshipChain]]:
        """
        Parse Cypher query and extract nodes and relationship chains.

        Walks through the parsed query AST and collects all Node and
        RelationshipChain objects that represent the graph pattern to match.

        Args:
            query: Cypher query string to parse

        Returns:
            Tuple of (list of nodes, list of relationship chains)

        Example:
            >>> nodes, chains = solver.extract_query_elements(
            ...     "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
            ... )
            >>> len(nodes)
            2
            >>> len(chains)
            1
        """
        all_nodes: list[Node] = []
        all_relationship_chains: list[RelationshipChain] = []

        for child in ast.walk():
            match child:
                case Node():
                    all_nodes.append(child)
                case RelationshipChain():
                    all_relationship_chains.append(child)
                case _:
                    pass

        return all_nodes, all_relationship_chains

    def create_node_constraints(
        self, nodes: list[Node], constraint_bag: ConstraintBag
    ) -> None:
        """
        Create constraints for node variable assignments.

        For each node in the query pattern, finds all matching nodes in the
        database (by label) and creates an ExactlyOne constraint ensuring
        the query variable is assigned to exactly one database node.

        Args:
            nodes: List of Node objects from the parsed query
            constraint_bag: ConstraintBag to add constraints to (modified in-place)

        Side Effects:
            - Adds ExactlyOne constraints to constraint_bag
            - Prints possible node assignments (for debugging)

        Example:
            For query "MATCH (n:Person)", if database has persons with IDs
            [p1, p2, p3], creates constraint: ExactlyOne(n=p1 ∨ n=p2 ∨ n=p3)
        """
        for node in nodes:
            node_variable = node.name_label.name
            node_assignment_disjunction = Disjunction([])

            # Find all database nodes matching this label
            for item in self.fact_collection.node_has_specific_label_facts(
                node.name_label.label
            ):
                print(
                    f"Variable {node_variable} can map to Node ID {item.node_id} with Label {item.label}"
                )
                variable_assigned_to_node = VariableAssignedToNode(
                    node_variable, item.node_id
                )
                node_assignment_disjunction += variable_assigned_to_node

            # Each variable must be assigned to exactly one node
            constraint_bag += ExactlyOne(node_assignment_disjunction)

    def create_relationship_constraints(
        self,
        relationship_chains: list[RelationshipChain],
        constraint_bag: ConstraintBag,
    ) -> None:
        """
        Create constraints for relationship variable assignments.

        For each relationship in the query pattern, finds all matching
        relationships in the database (by label) and creates an ExactlyOne
        constraint ensuring the relationship variable is assigned to exactly
        one database relationship.

        Args:
            relationship_chains: List of RelationshipChain objects from parsed query
            constraint_bag: ConstraintBag to add constraints to (modified in-place)

        Side Effects:
            - Adds ExactlyOne constraints to constraint_bag
            - Prints possible relationship assignments (for debugging)

        Example:
            For query "MATCH ()-[r:KNOWS]->()", if database has relationships
            [r1, r2], creates constraint: ExactlyOne(r=r1 ∨ r=r2)
        """
        for relationship_chain in relationship_chains:
            if relationship_chain.relationship is None:
                continue  # Skip if no relationship variable
            relationship_variable = (
                relationship_chain.relationship.relationship.name_label.name
            )
            relationship_assignment_disjunction = Disjunction([])

            # Find all database relationships matching this label
            for item in self.fact_collection.relationship_has_label_facts():
                print(
                    f"Variable {relationship_variable} can map to Relationship ID {item.relationship_id} with Label {item.relationship_label}"
                )
                variable_assigned_to_relationship = (
                    VariableAssignedToRelationship(
                        relationship_variable, item.relationship_id
                    )
                )
                relationship_assignment_disjunction += (
                    variable_assigned_to_relationship
                )

            # Each relationship variable must be assigned to exactly one relationship
            constraint_bag += ExactlyOne(relationship_assignment_disjunction)

    def create_relationship_endpoint_constraints(
        self,
        relationship_chains: list[RelationshipChain],
        constraint_bag: ConstraintBag,
    ) -> None:
        """
        Create implication constraints for relationship source and target nodes.

        For each relationship chain (pattern like (a)-[r]->(b)), creates
        implication constraints ensuring that:
        - If r is assigned to relationship X, then a must be X's source node
        - If r is assigned to relationship X, then b must be X's target node

        This ensures structural consistency: relationships can only connect
        their actual endpoint nodes from the database.

        Args:
            relationship_chains: List of RelationshipChain objects from parsed query
            constraint_bag: ConstraintBag to add constraints to (modified in-place)

        Side Effects:
            - Adds IfThen implication constraints to constraint_bag

        Example:
            For pattern (n)-[r]->(m), if r could be relationship R1 with
            source S and target T, adds constraints:
            - If r=R1 then n=S
            - If r=R1 then m=T
        """
        for relationship_chain in relationship_chains:
            if relationship_chain.relationship is None:
                continue  # Skip if no relationship variable
            relationship_variable = (
                relationship_chain.relationship.relationship.name_label.name
            )
            relationship_source_node_variable = (
                relationship_chain.source_node.name_label.name
            )
            relationship_target_node_variable = (
                relationship_chain.target_node.name_label.name
            )

            # For each possible relationship assignment, constrain source and target nodes
            for (
                relationship_assignment
            ) in constraint_bag.assignments_of_variable(relationship_variable):
                source_node_conjunction = Conjunction([])
                target_node_conjunction = Conjunction([])

                # Find source node for this relationship assignment
                for (
                    fact
                ) in self.fact_collection.relationship_has_source_node_facts():
                    if (
                        fact.relationship_id
                        == relationship_assignment.relationship_id
                    ):
                        source_node_assignment = VariableAssignedToNode(
                            relationship_source_node_variable,
                            fact.source_node_id,
                        )
                        # If relationship is X, then source must be Y
                        if_then_constraint = IfThen(
                            relationship_assignment, source_node_assignment
                        )
                        source_node_conjunction += if_then_constraint

                # Find target node for this relationship assignment
                for (
                    fact
                ) in self.fact_collection.relationship_has_target_node_facts():
                    if (
                        fact.relationship_id
                        == relationship_assignment.relationship_id
                    ):
                        target_node_assignment = VariableAssignedToNode(
                            relationship_target_node_variable,
                            fact.target_node_id,
                        )
                        # If relationship is X, then target must be Z
                        if_then_constraint = IfThen(
                            relationship_assignment, target_node_assignment
                        )
                        target_node_conjunction += if_then_constraint

                constraint_bag += source_node_conjunction
                constraint_bag += target_node_conjunction

    def solve_query(self, ast: TreeMixin) -> Conjunction:
        """
        Convert Cypher query to CNF constraints for SAT solving.

        This is the main entry point that orchestrates the full solving process:
        1. Parse query to extract graph patterns
        2. Create node assignment constraints
        3. Create relationship assignment constraints
        4. Create relationship endpoint constraints
        5. Convert all constraints to CNF

        Args:
            query: Cypher query string to solve

        Returns:
            Conjunction in CNF form representing all query constraints

        Example:
            >>> solver = CypherQuerySolver(fact_collection)
            >>> cnf = solver.solve_query("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m")
            >>> # cnf now contains boolean constraints that can be fed to SAT solver
        """
        # Extract query elements
        nodes, relationship_chains = self.extract_query_elements(ast)

        # Initialize constraint collection
        constraint_bag = ConstraintBag()

        # Build constraints for nodes
        self.create_node_constraints(nodes, constraint_bag)

        # Build constraints for relationships
        self.create_relationship_constraints(
            relationship_chains, constraint_bag
        )

        # Build constraints for relationship endpoints
        self.create_relationship_endpoint_constraints(
            relationship_chains, constraint_bag
        )

        # Convert to CNF
        cnf = constraint_bag.cnf()

        return cnf

    def get_clauses(
        self, cnf: Conjunction
    ) -> tuple[
        list[list[int]],
        dict[int, AtomicConstraint],
        dict[AtomicConstraint, int],
    ]:
        """
        Extract clauses and mappings for SAT solver input.

        Converts the CNF conjunction into a list of clauses (lists of integers)
        suitable for direct use with SAT solver libraries like python-sat or pycosat.
        Also provides bidirectional mappings between variable IDs and constraints
        for interpreting solutions.

        Args:
            cnf: Conjunction in CNF form from solve_query()

        Returns:
            Tuple of (clauses, reverse_mapping, forward_mapping):
            - clauses: List of lists of integers, each inner list is a clause
            - reverse_mapping: Dict mapping variable IDs (int) back to AtomicConstraints
            - forward_mapping: Dict mapping AtomicConstraints to variable IDs (int)

        Example:
            >>> from pysat.solvers import Glucose3
            >>>
            >>> solver = CypherQuerySolver(fact_collection)
            >>> cnf = solver.solve_query(query)
            >>> clauses, reverse_map, forward_map = solver.get_clauses(cnf)
            >>>
            >>> # Use with python-sat
            >>> with Glucose3() as sat:
            ...     for clause in clauses:
            ...         sat.add_clause(clause)
            ...     if sat.solve():
            ...         model = sat.get_model()
            ...         for var_id in model:
            ...             if var_id > 0:
            ...                 print(f"True: {reverse_map[var_id]}")
        """
        # Build atomic constraint mapping
        constraint_bag = ConstraintBag()
        for constraint in cnf.walk():
            if isinstance(constraint, AtomicConstraint):
                constraint_bag.add_constraint(constraint)

        forward_mapping = constraint_bag.atomic_constraint_mapping
        reverse_mapping = {v: k for k, v in forward_mapping.items()}

        # Extract clauses from the CNF
        clauses = []
        for constraint in cnf.constraints:
            if isinstance(constraint, Disjunction):
                clause = constraint.sat(forward_mapping)
                if isinstance(clause, list):
                    clauses.append(clause)
                else:
                    clauses.append([clause])
            elif isinstance(constraint, (AtomicConstraint, Negation)):
                literal = constraint.sat(forward_mapping)
                clauses.append([literal])
            else:
                # Handle other constraint types
                result = constraint.sat(forward_mapping)
                if isinstance(result, list):
                    clauses.append(result)
                else:
                    clauses.append([result])

        return clauses, reverse_mapping, forward_mapping

    def to_dimacs(self, cnf: Conjunction) -> str:
        """
        Convert CNF constraints to DIMACS format for SAT solvers.

        Args:
            cnf: Conjunction in CNF form from solve_query()

        Returns:
            String in DIMACS CNF format ready for SAT solver

        Example:
            >>> cnf = solver.solve_query(query)
            >>> dimacs = solver.to_dimacs(cnf)
            >>> with open('problem.cnf', 'w') as f:
            ...     f.write(dimacs)
        """
        # Build atomic constraint mapping
        constraint_bag = ConstraintBag()
        for constraint in cnf.walk():
            if isinstance(constraint, AtomicConstraint):
                constraint_bag.add_constraint(constraint)

        return cnf.to_dimacs(constraint_bag.atomic_constraint_mapping)

    def solutions(
        self, query: str
    ) -> Generator[list[AtomicConstraint], None, None]:
        """
        Generate all satisfying assignments for the given Cypher query.

        Uses the python-sat library to find all solutions to the CNF
        constraints generated from the query. Yields each solution as
        a list of AtomicConstraints that are assigned True.

        Args:
            query: Cypher query string to solve

        Yields:
            Lists of AtomicConstraints representing each solution
        """

        cnf = self.solve_query(query)
        clauses, reverse_map, forward_map = self.get_clauses(cnf)

        with Glucose3() as sat_solver:
            for clause in clauses:
                sat_solver.add_clause(clause)

            for solution in sat_solver.enum_models():
                true_assignments = [
                    reverse_map[var_id] for var_id in solution if var_id > 0
                ]

                yield true_assignments

    def solution_to_projection(
        self, solution: list[AtomicConstraint]
    ) -> Projection:
        """
        Convert a single solution to a Projection object.

        Takes a list of true AtomicConstraints from a SAT solution and
        converts it to a Projection (dictionary mapping variable names to values).

        Args:
            solution: List of AtomicConstraints that are True in this solution

        Returns:
            Projection object mapping variable names to node/relationship IDs

        Example:
            >>> for solution in solver.solutions(query):
            ...     projection = solver.solution_to_projection(solution)
            ...     print(projection.pythonify())
            {'n': 'node_123', 'r': 'rel_456', 'm': 'node_789'}
        """
        projection_dict = {}

        for constraint in solution:
            if isinstance(constraint, VariableAssignedToNode):
                projection_dict[constraint.variable] = constraint.node_id
            elif isinstance(constraint, VariableAssignedToRelationship):
                projection_dict[constraint.variable] = (
                    constraint.relationship_id
                )

        return Projection(projection_dict)

    def solutions_to_projection_list(self, ast: TreeMixin) -> ProjectionList:
        """
        Generate a ProjectionList containing all solutions to the query.

        Solves the Cypher query using SAT solver, converts each solution to
        a Projection, and returns them all in a ProjectionList.

        Args:
            query: Cypher query string to solve

        Returns:
            ProjectionList containing all solutions as Projection objects

        Example:
            >>> solver = CypherQuerySolver(fact_collection)
            >>> projection_list = solver.solutions_to_projection_list(
            ...     "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, r, m"
            ... )
            >>> print(f"Found {len(projection_list)} solutions")
            >>> for projection in projection_list:
            ...     print(projection.pythonify())
        """
        projections = []

        for solution in self.solutions(ast):
            projection = self.solution_to_projection(solution)
            projections.append(projection)

        return ProjectionList(projections)

    def solutions_to_projection_list_filtered(
        self, query: str, return_variables: list[str] | None = None
    ) -> ProjectionList:
        """
        Generate a ProjectionList with only specified variables from RETURN clause.

        Like solutions_to_projection_list, but filters each Projection to include
        only the variables specified in return_variables (mimicking SQL SELECT).

        Args:
            query: Cypher query string to solve
            return_variables: List of variable names to include in projections.
                            If None, extracts from query's RETURN clause.

        Returns:
            ProjectionList with filtered Projection objects

        Example:
            >>> # Only return 'n' and 'm', exclude 'r'
            >>> projection_list = solver.solutions_to_projection_list_filtered(
            ...     "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m",
            ...     return_variables=['n', 'm']
            ... )
            >>> for projection in projection_list:
            ...     print(projection.pythonify())  # Only has 'n' and 'm'
            {'n': 'node_123', 'm': 'node_789'}
        """
        # Extract RETURN variables from query if not provided
        if return_variables is None:
            return_variables = self._extract_return_variables(query)

        projections = []

        for solution in self.solutions(query):
            full_projection = self.solution_to_projection(solution)

            # Filter to only include return variables
            filtered_dict = {
                var: full_projection[var]
                for var in return_variables
                if var in full_projection
            }

            filtered_projection = Projection(filtered_dict)
            projections.append(filtered_projection)

        return ProjectionList(projections)

    def _extract_return_variables(self, query: str) -> list[str]:
        """
        Extract variable names from the RETURN clause of a Cypher query.

        Args:
            query: Cypher query string

        Returns:
            List of variable names mentioned in RETURN clause

        Example:
            >>> vars = solver._extract_return_variables("MATCH (n)-[r]->(m) RETURN n, m")
            >>> print(vars)
            ['n', 'm']
        """
        # Simple extraction - looks for RETURN keyword and splits on commas
        # This is a basic implementation; a full parser would be more robust
        query_upper = query.upper()
        if "RETURN" not in query_upper:
            return []

        return_part = query.split("RETURN", 1)[1].strip()

        # Remove ORDER BY, LIMIT, etc.
        for keyword in ["ORDER BY", "LIMIT", "SKIP", "WHERE"]:
            if keyword in return_part.upper():
                return_part = return_part.upper().split(keyword)[0]

        # Split on comma and clean up
        variables = [var.strip() for var in return_part.split(",")]

        # Remove any aggregation functions, property access, etc.
        # For now, just take variable names (letters, numbers, underscore)
        import re

        clean_variables = []
        for var in variables:
            # Extract just the variable name (before any dots or parens)
            match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)", var)
            if match:
                clean_variables.append(match.group(1))

        return clean_variables


# # Example query
# query: str = "MATCH (t:Tract)-[r:in]->(c:County), (s:Tract)-[r:in]->(c:County) RETURN t, c"
#
# # Initialize fact collection
# fact_collection = FoundationDBFactCollection(
#     foundationdb_cluster_file='/pycypher-nmetl/fdb.cluster'
# )
#
# # Create solver and solve query
# solver = CypherQuerySolver(fact_collection)
# cnf = solver.solve_query(query)
# clauses, reverse_map, forward_map = solver.get_clauses(cnf)
#
# print(f"Number of clauses: {len(clauses)}")
# print(f"Number of variables: {len(forward_map)}")
#
# print("\nSolving with pycosat...")
# for solution in pycosat.itersolve(clauses):
#
#     if solution != "UNSAT":
#         print("✓ SAT - Solution found!")
#         print("\nTrue assignments:")
#         for var_id in solution:
#             if var_id > 0:
#                 constraint = reverse_map[var_id]
#                 print(f"  {var_id}: {constraint}")
#
#         print(f"\nTotal true variables: {sum(1 for v in solution if v > 0)}")
#     else:
#         print("✗ UNSAT - No solution exists")


def cypher_query_to_cnf(
    query: str, fact_collection: FoundationDBFactCollection
) -> Conjunction:
    """
    High-level function to convert a Cypher query to CNF constraints.

    This is a convenience function that creates a CypherQuerySolver and
    uses it to convert a Cypher query into boolean constraints in
    Conjunctive Normal Form suitable for SAT solvers.

    Args:
        query: Cypher query string (e.g., "MATCH (n:Person) RETURN n")
        fact_collection: FoundationDB fact collection for graph data

    Returns:
        Conjunction representing the query constraints in CNF

    Example:
        >>> fact_collection = FoundationDBFactCollection(cluster_file='fdb.cluster')
        >>> cnf = cypher_query_to_cnf(
        ...     "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m",
        ...     fact_collection
        ... )
        >>> # Use cnf.to_dimacs() to get SAT solver input
    """
    solver = CypherQuerySolver(fact_collection)
    return solver.solve_query(query)


if __name__ == "__main__":
    """
    Example usage demonstrating the CypherQuerySolver.
    
    This example shows multiple ways to use the solver output:
    1. Basic CNF output
    2. DIMACS format for file-based SAT solvers
    3. Direct integration with python-sat library
    4. Direct integration with pycosat library
    """

    # Example query
    query: str = "MATCH (t:Tract)-[r:in]->(c:County), (s:Tract)-[r:in]->(c:County) RETURN t, c"

    from pycypher.fact_collection.foundationdb import (
        FoundationDBFactCollection,
    )
    from pycypher.parsing.cypher_parser import parse_cypher_query

    # Initialize fact collection
    fact_collection = FoundationDBFactCollection(
        foundationdb_cluster_file="/pycypher-nmetl/fdb.cluster"
    )

    ast = parse_cypher_query(query)

    # Create solver and solve query
    solver = CypherQuerySolver(fact_collection)
    cnf = solver.solve_query(ast)

    # ===================================================================
    # Method 1: Basic CNF output
    # ===================================================================
    print("\n" + "=" * 70)
    print("METHOD 1: CNF Form (for inspection)")
    print("=" * 70)
    print(f"CNF: {cnf}")
    print(f"Number of constraints: {len(cnf.constraints)}")

    # ===================================================================
    # Method 2: DIMACS format (for external SAT solvers)
    # ===================================================================
    print("\n" + "=" * 70)
    print("METHOD 2: DIMACS Format (for MiniSat, CryptoMiniSat, etc.)")
    print("=" * 70)
    dimacs = solver.to_dimacs(cnf)
    print(dimacs)
    print("\nTo use with external solver:")
    print(
        "  1. Save to file: with open('problem.cnf', 'w') as f: f.write(dimacs)"
    )
    print("  2. Run solver: minisat problem.cnf solution.txt")
    print("  3. Check exit code: 10=SAT, 20=UNSAT")

    # ===================================================================
    # Method 3: python-sat library (recommended)
    # ===================================================================
    print("\n" + "=" * 70)
    print("METHOD 3: python-sat Library (Recommended)")
    print("=" * 70)

    try:
        from pysat.solvers import Glucose3

        clauses, reverse_map, forward_map = solver.get_clauses(cnf)

        print(f"Number of clauses: {len(clauses)}")
        print(f"Number of variables: {len(forward_map)}")
        print(f"\nFirst few clauses: {clauses[:5]}")

        # Create and solve with Glucose3
        with Glucose3() as sat_solver:
            for clause in clauses:
                sat_solver.add_clause(clause)

            print("\nSolving with Glucose3...")
            if sat_solver.solve():
                print("✓ SAT - Solution found!")
                model = sat_solver.get_model()

                print("\nTrue assignments:")
                for var_id in model:
                    if var_id > 0:  # Positive literals are true
                        constraint = reverse_map[var_id]
                        print(f"  {var_id}: {constraint}")

                print(
                    f"\nTotal true variables: {sum(1 for v in model if v > 0)}"
                )
            else:
                print("✗ UNSAT - No solution exists")

    except ImportError:
        print("python-sat not installed. Install with: pip install python-sat")
        print("\nExample usage code:")
        print("""
    from pysat.solvers import Glucose3
    
    solver = CypherQuerySolver(fact_collection)
    cnf = solver.solve_query(query)
    clauses, reverse_map, forward_map = solver.get_clauses(cnf)
    
    with Glucose3() as sat:
        for clause in clauses:
            sat.add_clause(clause)
        
        if sat.solve():
            model = sat.get_model()
            for var_id in model:
                if var_id > 0:
                    print(f"True: {reverse_map[var_id]}")
        """)

    # ===================================================================
    # Method 4: pycosat library (fast C-based solver)
    # ===================================================================
    print("\n" + "=" * 70)
    print("METHOD 4: pycosat Library (Fast C-based)")
    print("=" * 70)

    try:
        import pycosat

        clauses, reverse_map, forward_map = solver.get_clauses(cnf)

        print(f"Number of clauses: {len(clauses)}")
        print(f"Number of variables: {len(forward_map)}")

        print("\nSolving with pycosat...")
        solution_count = 0
        for solution in pycosat.itersolve(clauses):
            if solution != "UNSAT":
                solution_count += 1
                if solution_count <= 3:  # Only print first 3 solutions
                    print(f"\n✓ SAT - Solution {solution_count} found!")
                    print("True assignments:")
                    for var_id in solution:
                        if var_id > 0:
                            constraint = reverse_map[var_id]
                            print(f"  {var_id}: {constraint}")

        print(f"\nTotal solutions found: {solution_count}")

    except ImportError:
        print("pycosat not installed. Install with: pip install pycosat")
        print("\nExample usage code:")
        print("""
    import pycosat
    
    solver = CypherQuerySolver(fact_collection)
    cnf = solver.solve_query(query)
    clauses, reverse_map, _ = solver.get_clauses(cnf)
    
    solution = pycosat.solve(clauses)
    if solution != "UNSAT":
        for var_id in solution:
            if var_id > 0:
                print(f"True: {reverse_map[var_id]}")
        """)

    # ===================================================================
    # Method 5: ProjectionList conversion (recommended for integration)
    # ===================================================================
    print("\n" + "=" * 70)
    print("METHOD 5: Convert to ProjectionList (Recommended for Integration)")
    print("=" * 70)

    print("\nConverting solutions to ProjectionList...")
    print(
        "This provides a clean interface matching the rest of the pycypher system.\n"
    )

    # Get all solutions as ProjectionList
    projection_list = solver.solutions_to_projection_list(query)

    print(f"Number of solutions: {len(projection_list)}")

    if len(projection_list) > 0:
        print("\nFirst few solutions:")
        for i, projection in enumerate(projection_list[:3]):
            print(f"\n  Solution {i + 1}:")
            for key, value in projection.items():
                print(f"    {key} = {value}")

        # Show pythonified version
        print("\nAs Python dictionaries (pythonify):")
        for i, projection in enumerate(projection_list[:3]):
            print(f"  Solution {i + 1}: {projection.pythonify()}")

        # Filter to only RETURN variables
        print("\n--- Filtered to RETURN variables only ---")
        filtered_list = solver.solutions_to_projection_list_filtered(
            query,
            return_variables=["t", "c"],  # Only return t and c
        )
        print(f"Filtered solutions: {len(filtered_list)}")
        for i, projection in enumerate(filtered_list[:3]):
            print(f"  Solution {i + 1}: {projection.pythonify()}")

    # ===================================================================
    # Summary
    # ===================================================================
    print("\n" + "=" * 70)
    print("SUMMARY: Recommended Workflow")
    print("=" * 70)
    print("""
1. For pycypher integration (BEST for this system):
   - Use solutions_to_projection_list() or solutions_to_projection_list_filtered()
   - Returns ProjectionList compatible with rest of pycypher
   - Easy to work with as Python dictionaries via .pythonify()

2. For Python SAT integration:
   - Use python-sat library with get_clauses() method
   - Fast, flexible, multiple solver backends
   - Easy to interpret results with reverse_map

3. For maximum performance:
   - Use pycosat library (C-based PicoSAT)
   - Fastest for large problems
   - Similar API to python-sat

4. For external tools:
   - Use to_dimacs() to generate DIMACS CNF file
   - Compatible with MiniSat, CryptoMiniSat, Z3, etc.
   - Good for integration with other systems
    """)
