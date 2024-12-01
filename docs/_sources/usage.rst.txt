.. _installation:

===============================
Cypher AST Generator for Python
===============================

This is a *work in progress*, by which I mean, “ugly, but fixable.” It
is also woefully incomplete. It generates an abstract syntax tree for
Cypher statements that use only a subset of the language. That subset is
growing, but it’s still small.

Additionally, this package contains the beginning of a query engine that
is designed to accept Cypher queries and return results from arbitrary
graph structures in Python. This functionality is in a *very* early
state, and works only for trivial queries.

The hope is that this will be useful for building modules that can take
advantage of the Cypher query language, by eliminating the need to do
all the boring work of writing a parser and generating an AST.

How to use it
-------------

Don’t. But if you really want to, then:

.. code:: python

   >>> from pycypher.parser import CypherParser
   >>> cypher = CypherParser("MATCH (n:Thing) RETURN n.foo")
   >>> cypher.parsed.print_tree()

   Cypher
   └── Query
       ├── Match
       │   └── Node
       │       └── NodeNameLabel
       │           ├── n
       │           └── Thing
       └── Return
           └── Projection
               └── ObjectAttributeLookup
                   ├── n
                   └── foo
   >>> print(cypher.parsed)
   Cypher(Query(Match(Node(NodeNameLabel(n, Thing), None)), Return(Projection([ObjectAttributeLookup(n, foo)]))))

If you want to understand what’s happening, what Python classes are
being built, etc., then you’ll have to use the source, Luke. Check out
the ``__main__`` function at the end of the ``cypher.py`` script. There
are no docs yet. Like I said, this is a *work in progress*.

Requirements
------------

Mac and Linux
~~~~~~~~~~~~~

You’ll need to be able to run ``uv`` in order to use the ``Makefile``.
To install ``uv`` on Linux or Mac:

.. code:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

If you don’t have ``make`` on your Mac, then you should:

.. code:: bash

   brew install make

And if you don’t have ``brew``, then install it with:

.. code:: bash

   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

If you’re running Linux without ``make``, then follow the directions for
your distribution. For example, on Ubuntu, you can:

.. code:: bash

   sudo apt install make

Windows
~~~~~~~

On Windows, erase your hard drive, install Linux, and then follow the
directions above.

Setting everything up
---------------------

To set up the virtual environment, install all the dependencies, install
the right version of Python, build the package, install it as an
editable project, run a bunch of unit tests, and build HTML
documentation, do:

.. code:: bash

   make all

To clean everything up, deleting the virtual environment, documentation,
and so on, do:

.. code:: bash

   make clean

You don’t *need* to use the ``Makefile``, and therefore you don’t *need*
to have ``uv`` installed on your system. But that’s what all the cool
kids are using these days.