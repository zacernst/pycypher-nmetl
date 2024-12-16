Shims
=====

In order to use `pycypher` as a search engine (as opposed to just using it to parse Cypher into an AST), you need to have a subclass of ``Shim`` appropriate for your data source.

The job of a ``Shim`` is to generate a ``FactCollection`` object from your data source. When you call ``CypherParser.solutions`` on your shim, it will automatically generate the required ``FactCollection`` and return the solution(s) to your query.

``Shim`` is a very simple abstract base class that requires you to implement a single method, ``make_fact_collection``. This method should return a ``FactCollection`` object. For reference, we include a shim for NetworkX's ``DiGraph`` (directed graph) class, which is documented in detail. But generally speaking, there is a standard strategy for implementing your ``make_fact_collection`` method, which goes like this:

#. Make a new ``FactCollection`` object.
#. Traverse your graph, and:
    - for each node:
        - if it has a label, create a ``FactNodeHasLabel`` object, initialized with the node's ID and label;
        - if it has any attributes, create a ``FactNodeHasProperty`` object for each attribute, initialized with the node's ID, attribute name, and attribute value.
    - for each edge:
        - if it has a label, create a ``FactRelationshipHasLabel`` object, initialized with the edge's ID and label;
        - if it has any attributes, create a ``FactRelationshipHasAttribute`` object for each attribute, initialized with the edge's ID, attribute name, and attribute value;
        - create a ``FactEdgeHasSourceNode`` object, initialized with the edge's ID and source node ID;
        - create a ``FactEdgeHasTargetNode`` object, initialized with the edge's ID and target node ID.
    - Add those facts to your ``FactCollection`` object.
#. Return the ``FactCollection`` object.

The NetworkX shim is an example of this procedure. It inherits from ``Shim``, takes a ``networkx.DiGraph`` object in its ``__init__`` method, and implements the ``make_fact_collection`` method. That method traverses the graph, defining a list of ``Fact`` objects. Then it adds those ``Fact`` objects to a ``FactCollection``.

If you follow this procedure, you should be able to call ``CypherParser.solutions`` on your shim and get the results you expect.

.. automodule:: pycypher.shims
   :members:


   .. rubric:: Functions

   .. autosummary::
      :recursive:

      Shim
   