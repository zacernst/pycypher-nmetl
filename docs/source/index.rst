.. pycypher-nmetl documentation master file, created by
   sphinx-quickstart on Mon Apr  7 08:55:57 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Documentation
=============

Introduction
------------

**nmetl** is a Python library that provides a declarative approach to ETL (Extract, Transform, Load). It is fundamentally different from other ETL systems such as Airflow, Luigi, AWS Glue, and so on. If this is your first time looking at this project, you should probably start with the :ref:`Overview`.

More precisely, ``nmetl`` is the ETL tool, and it uses ``pycypher`` to parse conditions and queries. This is all explained in much more detail in the :ref:`Overview`. 

.. warning::
     This is a work in progress. The API is not yet stable, and the documentation is incomplete. Using it for production would be a bad idea.

Current Status
--------------

The ``nmetl`` package is currently in a very early stage of development. It is not yet ready for production use. The API is not yet stable, and the documentation is incomplete. It "works on my computer" (tm). Think of it for now as a proof-of-concept, meant to demonstrate a new way of thinking about ETL. It is, however, under active development and I've been able to use it end-to-end on personal projects.

Like everything else in this repository, the name of ``nmetl`` is a work in progress. For now, it stands for "New Methods for ETL"; I've been pronouncing it "new metal" and writing "New METaL".

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   overview
   installation
   tutorial
   contributing

.. toctree::
   :maxdepth: 1
   :caption: API Reference:

   autoapi/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
