Quick Reference
===============

This page provides a quick reference for common tasks in NMETL.

Installation
------------

.. code-block:: bash

   pip install -e packages/nmetl
   pip install -e packages/pycypher
   pip install -e packages/shared
   pip install -e packages/fastopendata

Common Imports
--------------

NMETL
~~~~~

.. code-block:: python

   from nmetl.session import Session
   from nmetl.configuration import load_session_config
   from nmetl.data_source import DataSource
   from nmetl.trigger import VariableAttribute, NodeRelationship

PyCypher
~~~~~~~~

.. code-block:: python

   from pycypher.cypher_parser import CypherParser
   from pycypher.solutions import Projection, ProjectionList
   from pycypher.fact import (
       FactNodeHasLabel,
       FactNodeHasAttributeWithValue,
       FactRelationshipHasLabel,
   )
   from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
   from pycypher.fact_collection.solver import CypherQuerySolver

Common Patterns
---------------

Initialize Session
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   session = Session(session_config_file="config.toml")
   fact_collection = FoundationDBFactCollection()
   session.attach_fact_collection(fact_collection)

Define a Trigger
~~~~~~~~~~~~~~~~

.. code-block:: python

   @session.trigger("MATCH (c:City) WITH c.population AS pop RETURN pop")
   def population_category(pop) -> VariableAttribute["c", "category"]:
       if pop > 1000000:
           return "large"
       elif pop > 100000:
           return "medium"
       else:
           return "small"

Parse Cypher Query
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   parser = CypherParser("MATCH (n:Person) RETURN n")
   ast = parser.parse_tree

Solve Query with SAT
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   solver = CypherQuerySolver(fact_collection)
   cnf = solver.solve_query(ast)
   clauses, reverse_map, forward_map = solver.get_clauses(cnf)

Work with Facts
~~~~~~~~~~~~~~~

.. code-block:: python

   # Add a fact
   fact = FactNodeHasLabel(node_id="n1", label="Person")
   fact_collection.add_fact(fact)
   
   # Query facts
   for fact in fact_collection.node_has_label_facts():
       print(fact)

Create Projections
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   projection = Projection({"n": "node1", "m": "node2"})
   value = projection["n"]  # "node1"

Class Hierarchy
---------------

Fact Types
~~~~~~~~~~

- ``FactNodeHasLabel``
- ``FactNodeHasAttributeWithValue``
- ``FactRelationshipHasLabel``
- ``FactRelationshipHasSourceNode``
- ``FactRelationshipHasTargetNode``
- ``FactNodeRelatedToNode``

Fact Collections
~~~~~~~~~~~~~~~~

- ``SimpleFactCollection`` - In-memory implementation
- ``FoundationDBFactCollection`` - FoundationDB backend
- ``RocksDBFactCollection`` - RocksDB backend
- ``Etcd3FactCollection`` - Etcd3 backend

Constraint Types
~~~~~~~~~~~~~~~~

- ``AtomicConstraint`` - Base class for atomic constraints
- ``VariableAssignedToNode`` - Variable to node assignment
- ``VariableAssignedToRelationship`` - Variable to relationship assignment
- ``Conjunction`` - Logical AND
- ``Disjunction`` - Logical OR
- ``Negation`` - Logical NOT
- ``IfThen`` - Implication (P → Q)
- ``ExactlyOne`` - Cardinality constraint
- ``AtMostOne`` - Cardinality constraint

Configuration Files
-------------------

Session Config (TOML)
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: toml

   [session]
   name = "my_session"
   
   [foundationdb]
   cluster_file = "/etc/foundationdb/fdb.cluster"
   
   [logging]
   level = "INFO"

Data Source Mapping (YAML)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   data_sources:
     - name: cities
       uri: file://data/cities.csv
       mappings:
         - attribute_key: name
           identifier_key: city_id
           attribute: CityName
           label: City

CLI Commands
------------

NMETL CLI
~~~~~~~~~

.. code-block:: bash

   # Run NMETL session
   nmetl run --config session_config.toml
   
   # Validate configuration
   nmetl validate --config session_config.toml

PyCypher CLI
~~~~~~~~~~~~

.. code-block:: bash

   # Parse Cypher query
   pycypher parse "MATCH (n:Person) RETURN n"
   
   # List facts
   pycypher facts list

Debugging Tips
--------------

Enable Debug Logging
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import logging
   from shared.logger import LOGGER
   
   LOGGER.setLevel(logging.DEBUG)

Inspect CNF
~~~~~~~~~~~

.. code-block:: python

   cnf = solver.solve_query(query)
   for constraint in cnf.constraints:
       print(constraint)

Check Constraint Mappings
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   bag = ConstraintBag()
   # ... add constraints ...
   mapping = bag.atomic_constraint_mapping
   for constraint, var_id in mapping.items():
       print(f"{var_id}: {constraint}")

See Also
--------

- :doc:`api/index` - Full API documentation
- :doc:`examples` - Detailed examples
- :doc:`nmetl_tutorial` - Tutorial
