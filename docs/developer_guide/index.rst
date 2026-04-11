Developer Guide
===============

Information for developers contributing to PyCypher.

.. toctree::
   :maxdepth: 2
   :caption: Developer Topics:

   architecture
   adr/index
   contributing
   testing
   release
   security
   threat_model

New Contributors
-----------------

Start with the `CONTRIBUTING.md <https://github.com/pycypher/pycypher-nmetl/blob/main/CONTRIBUTING.md>`_
file at the repository root — it covers quick start setup, what to work on,
debugging tips, and the submission process.  The pages below go deeper into
each topic.

Overview
--------

This guide is for developers who want to contribute to PyCypher:

* :doc:`architecture` — Monorepo layout, package dependencies, the
  Lark-to-Pydantic parsing pipeline, and the BindingFrame execution model.
* :doc:`contributing` — Development environment setup (``uv``, Python 3.14t),
  code style (ruff, ty, Google docstrings), branch naming, and PR process.
* :doc:`testing` — Running tests with pytest, writing new tests, parallel
  execution, and coverage reporting.
* :doc:`release` — Semantic versioning, pre-release checklist, building and
  publishing packages, and CI/CD integration.
* :doc:`security` — Threat model, input validation, resource limits,
  audit logging, and deployment checklist.
