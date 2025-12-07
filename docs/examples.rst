Code Examples
=============

This section provides practical examples of using the NMETL framework and related packages.

Basic Examples
--------------

Session Setup
~~~~~~~~~~~~~

.. code-block:: python

   from nmetl.session import Session
   from pycypher.fact_collection.foundationdb import FoundationDBFactCollection

   # Create a session
   session = Session(session_config_file="config.toml")
   
   # Attach a fact collection
   fact_collection = FoundationDBFactCollection()
   session.attach_fact_collection(fact_collection)

Defining Triggers
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from nmetl.trigger import VariableAttribute
   
   @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
   def has_sand(beachy) -> VariableAttribute["c", "sandy"]:
       """Compute whether a city is sandy based on beach status."""
       return beachy

Data Source Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from nmetl.data_source import DataSource
   from nmetl.helpers import ensure_uri
   
   # Load CSV data
   csv_uri = ensure_uri("data/cities.csv")
   data_source = DataSource.from_uri(csv_uri, name="cities")

PyCypher Examples
-----------------

Parsing Cypher Queries
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.cypher_parser import CypherParser
   
   query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
   parser = CypherParser(query)
   ast = parser.parse_tree

SAT Solver Integration
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.fact_collection.solver import CypherQuerySolver
   from pysat.solvers import Glucose3
   
   # Initialize solver
   solver = CypherQuerySolver(fact_collection)
   
   # Solve query
   query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, m"
   cnf = solver.solve_query(query)
   
   # Get clauses
   clauses, reverse_map, forward_map = solver.get_clauses(cnf)
   
   # Use SAT solver
   with Glucose3() as sat:
       for clause in clauses:
           sat.add_clause(clause)
       
       if sat.solve():
           model = sat.get_model()
           for var_id in model:
               if var_id > 0:
                   constraint = reverse_map[var_id]
                   print(f"True: {constraint}")

Working with Projections
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.solutions import Projection, ProjectionList
   
   # Create a projection
   projection = Projection(projection={"n": "node1", "m": "node2"})
   
   # Access values
   node_n = projection["n"]
   
   # Create projection list
   projection_list = ProjectionList(projection_list=[
       Projection({"n": "node1", "m": "node2"}),
       Projection({"n": "node3", "m": "node4"}),
   ])
   
   # Iterate
   for proj in projection_list:
       print(proj.pythonify())

Fact Collections
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.fact import (
       FactNodeHasLabel,
       FactNodeHasAttributeWithValue,
       FactRelationshipHasSourceNode,
   )
   from pycypher.fact_collection.simple import SimpleFactCollection
   
   # Create fact collection
   fc = SimpleFactCollection()
   
   # Add facts
   fc.add_fact(FactNodeHasLabel(node_id="person1", label="Person"))
   fc.add_fact(FactNodeHasAttributeWithValue(
       node_id="person1",
       attribute="name",
       value="Alice"
   ))
   
   # Query facts
   for fact in fc.node_has_label_facts():
       print(f"Node {fact.node_id} has label {fact.label}")

Advanced Examples
-----------------

Custom Constraint Creation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.fact_collection.solver import (
       ConstraintBag,
       VariableAssignedToNode,
       ExactlyOne,
       Disjunction,
       IfThen,
   )
   
   # Create constraint bag
   bag = ConstraintBag()
   
   # Add exactly-one constraint
   options = Disjunction([
       VariableAssignedToNode("x", "node1"),
       VariableAssignedToNode("x", "node2"),
       VariableAssignedToNode("x", "node3"),
   ])
   bag.add_constraint(ExactlyOne(options))
   
   # Add implication
   if_constraint = VariableAssignedToNode("x", "node1")
   then_constraint = VariableAssignedToNode("y", "node2")
   bag.add_constraint(IfThen(if_constraint, then_constraint))
   
   # Convert to CNF
   cnf = bag.cnf()

DIMACS Export
~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.fact_collection.solver import CypherQuerySolver
   
   solver = CypherQuerySolver(fact_collection)
   cnf = solver.solve_query(query)
   
   # Export to DIMACS format
   dimacs_str = solver.to_dimacs(cnf)
   
   # Write to file
   with open("problem.cnf", "w") as f:
       f.write(dimacs_str)

Queue Processing
~~~~~~~~~~~~~~~~

.. code-block:: python

   from nmetl.queue_processor import CheckFactAgainstTriggersQueueProcessor
   from pycypher.fact import FactNodeHasAttributeWithValue
   from pycypher.node_classes import Literal
   
   # Create processor
   processor = CheckFactAgainstTriggersQueueProcessor(session)
   
   # Process a fact
   fact = FactNodeHasAttributeWithValue(
       node_id="city1",
       attribute="has_beach",
       value=Literal(True)
   )
   
   results = processor.process_item_from_queue(fact)

See Also
--------

- :doc:`../api/index` - Complete API reference
- :doc:`../nmetl_tutorial` - Getting started tutorial
- :doc:`../SAT_SOLVER_INTEGRATION` - SAT solver integration guide
