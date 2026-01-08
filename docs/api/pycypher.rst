PyCypher API
============

The PyCypher package provides comprehensive openCypher query parsing and AST processing.

AST Models
----------

.. automodule:: pycypher.ast_models
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Key Classes
~~~~~~~~~~~

Variable
^^^^^^^^

.. autoclass:: pycypher.ast_models.Variable
   :members:
   :special-members: __init__

Query and Statement Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: pycypher.ast_models.Query
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.RegularQuery
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.SingleQuery
   :members:
   :show-inheritance:

Pattern Models
^^^^^^^^^^^^^^

.. autoclass:: pycypher.ast_models.NodePattern
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.RelationshipPattern
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.PatternPath
   :members:
   :show-inheritance:

Expression Models
^^^^^^^^^^^^^^^^^

.. autoclass:: pycypher.ast_models.PropertyLookup
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.ListComprehension
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.PatternComprehension
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.MapProjection
   :members:
   :show-inheritance:

.. autoclass:: pycypher.ast_models.Reduce
   :members:
   :show-inheritance:

Grammar Parser
--------------

.. automodule:: pycypher.grammar_parser
   :members:
   :undoc-members:
   :show-inheritance:

Key Classes
~~~~~~~~~~~

GrammarParser
^^^^^^^^^^^^^

.. autoclass:: pycypher.grammar_parser.GrammarParser
   :members:
   :special-members: __init__

CypherASTTransformer
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: pycypher.grammar_parser.CypherASTTransformer
   :members:
   :show-inheritance:

ASTConverter
^^^^^^^^^^^^

.. autoclass:: pycypher.ast_models.ASTConverter
   :members:
   :special-members: __init__

Validation
----------

.. automodule:: pycypher.validation
   :members:
   :undoc-members:
   :show-inheritance:

Solver
------

.. automodule:: pycypher.solver
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions
----------

.. automodule:: pycypher.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
