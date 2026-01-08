Variables in PyCypher
=====================

Understanding Variable Representation
--------------------------------------

As of the latest version, PyCypher uses a consistent ``Variable`` class throughout the AST to represent all variable references. This ensures type safety and makes variable tracking more robust.

The Variable Class
------------------

.. code-block:: python

   from pycypher.ast_models import Variable
   
   # Create a variable
   var = Variable(name="person")
   
   # Access the name
   print(var.name)  # "person"

Where Variables Are Used
-------------------------

Variables appear in many AST node types:

Node Patterns
~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import NodePattern, Variable
   
   # Node pattern with variable
   node = NodePattern(
       variable=Variable(name="n"),
       labels=["Person"],
       properties=None
   )

Relationship Patterns
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import RelationshipPattern, Variable
   
   # Relationship pattern with variable
   rel = RelationshipPattern(
       variable=Variable(name="r"),
       types=["KNOWS"],
       properties=None,
       direction="outgoing"
   )

Property Lookups
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import PropertyLookup, Variable
   
   # Property lookup on a variable
   prop = PropertyLookup(
       expression=Variable(name="person"),
       property_name="age"
   )

List Comprehensions
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import ListComprehension, Variable
   
   comp = ListComprehension(
       variable=Variable(name="x"),
       list_expression=some_list,
       filter_expression=some_filter,
       map_expression=some_map
   )

Migration from String Variables
--------------------------------

If you have code that previously used string variables, you need to update it:

**Old code:**

.. code-block:: python

   node = NodePattern(
       variable="n",  # String
       labels=["Person"]
   )

**New code:**

.. code-block:: python

   from pycypher.ast_models import Variable
   
   node = NodePattern(
       variable=Variable(name="n"),  # Variable instance
       labels=["Person"]
   )

Benefits
--------

Using ``Variable`` instances provides several advantages:

1. **Type Safety**: Pydantic validates that variables are proper Variable instances
2. **Consistency**: Same representation everywhere in the AST
3. **Extensibility**: Can add metadata to variables (scope, type, etc.)
4. **Better Validation**: Can track variable definitions and references more easily

See Also
--------

* :doc:`../api/pycypher` - Complete API reference for Variable class
* :doc:`../tutorials/basic_query_parsing` - Tutorial on working with variables
