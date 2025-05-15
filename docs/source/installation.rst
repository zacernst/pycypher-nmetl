Installation
============

Prerequisites
------------

* Python 3.10 or higher
* uv package manager (recommended)
* RocksDB (optional, but highly recommended)

If you don't have ``uv``, you can install it with:

.. code-block:: bash

    curl -LsSf https://astral.sh/uv/install.sh | sh

You can still install everything without ``uv``, but you can't use the ``Makefile``.

RocksDB is a key-value store that is used as a database backend. It is not strictly required, but it is highly recommended for any serious testing. It is available through various package managers. For example, if you're running on a Mac and you have Homebrew, you can:

.. code-block:: bash

    brew install rocksdb

If you're running on Linux, you can use your distribution's package manager. For example, on Ubuntu, you can:

.. code-block:: bash

    sudo apt install librocksdb-dev

Installing from Source
----------------------

Because this is a work in progress, there is no PyPI package yet. But installing from source is easy.

Clone the repository:

.. code-block:: bash

    git clone https://github.com/zacernst/pycypher-nmetl.git
    cd pycypher-nmetl

Assuming you have ``uv`` (see above), the ``Makefile`` will do everything for you:

.. code-block:: bash

    make

If you see a lot of ``pytest`` unit tests pass, then everything worked. Specifically, the ``make`` command will:

* Install all the dependencies
* Build the package
* Install the package as an editable project
* Run a suite of unit tests
* Build HTML documentation

Note on the Repository Structure
--------------------------------

The repository is a monorepo with three packages:

* ``pycypher``: Parses Cypher queries into Python objects
* ``nmetl``: Declarative ETL framework using PyCypher
* ``fastopendata``: Utilities for working with open data sources

The ETL tool is ``nmetl``, and it relies on ``pycypher``. ``fastopendata`` is a separate package that I'm currently 
using as a testbed; it is probably not useful to anyone else.

The ``pycypher`` package is a prerequisite for ``nmetl``, which is a prerequisite for ``fastopendata``. The ``Makefile``
will build and install all three packages in the correct order.