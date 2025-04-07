======================================
PyCypher-NMETL: Declarative ETL Using Cypher
======================================

**PyCypher-NMETL** is a Python monorepo containing three main packages:

1. **PyCypher**: Parses Cypher queries into an AST consisting of Python objects
2. **NMETL**: Implements a declarative ETL system which utilizes the Cypher parser
3. **FastOpenData**: Utilities for working with open data sources

This documentation covers all three packages and their components.

Installation
------------

Mac and Linux
~~~~~~~~~~~~~

You'll need to be able to run ``uv`` in order to use the ``Makefile``.
To install ``uv`` on Linux or Mac:

.. code:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

If you don't have ``make`` on your Mac, then you should:

.. code:: bash

   brew install make

And if you don't have ``brew``, then install it with:

.. code:: bash

   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

If you're running Linux without ``make``, then follow the directions for
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
documentation, clone the repository and do:

.. code:: bash

   make all

To clean everything up, deleting the virtual environment, documentation,
and so on, do:

.. code:: bash

   make clean

You don't *need* to use the ``Makefile``, and therefore you don't *need*
to have ``uv`` installed on your system. But that's what all the cool
kids are using these days.

.. note::

   This project is in alpha stage but being actively developed.

Contents
--------

.. toctree::
   :maxdepth: 2
   
   Home <self>
   pycypher/index
   nmetl/index
   fastopendata/index
   api
