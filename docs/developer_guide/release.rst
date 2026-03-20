Release Process
===============

How PyCypher versions are numbered, built, and published.

.. contents:: In this guide
   :local:
   :depth: 2

Version Numbering
-----------------

PyCypher follows `Semantic Versioning <https://semver.org/>`_ (``MAJOR.MINOR.PATCH``):

- **MAJOR** — incompatible API changes
- **MINOR** — new functionality, backwards-compatible
- **PATCH** — backwards-compatible bug fixes

The current version is declared in:

- ``pyproject.toml`` (root workspace)
- ``packages/pycypher/pyproject.toml``
- ``packages/shared/pyproject.toml``

All three must be updated in sync for a release.

Pre-Release Checklist
---------------------

Before cutting a release, verify:

1. **All tests pass**

   .. code-block:: bash

      uv run pytest

2. **Type checking passes**

   .. code-block:: bash

      uv run ty check

3. **Formatting is clean**

   .. code-block:: bash

      make format

4. **Documentation builds without errors**

   .. code-block:: bash

      make docs

5. **CHANGELOG or commit history** documents all notable changes since the
   last release

Bump Version
~~~~~~~~~~~~

The Makefile provides a ``BUMP`` variable (default: ``micro``):

.. code-block:: bash

   # Patch bump (0.0.1 → 0.0.2)
   make bump BUMP=micro

   # Minor bump (0.0.1 → 0.1.0)
   make bump BUMP=minor

   # Major bump (0.0.1 → 1.0.0)
   make bump BUMP=major

If the ``bump`` target is not available, update the version strings in the
three ``pyproject.toml`` files manually.

Building Packages
-----------------

.. code-block:: bash

   # Build wheel and sdist for each package
   uv build packages/shared
   uv build packages/pycypher

Built artefacts are placed in ``dist/``.

Publishing
----------

.. code-block:: bash

   # Publish to PyPI (requires credentials)
   uv publish dist/*

   # Or publish to a test index first
   uv publish --index testpypi dist/*

Ensure your PyPI credentials are configured (e.g. via ``~/.pypirc`` or
environment variables ``UV_PUBLISH_TOKEN``).

Post-Release
------------

After a successful publish:

1. Tag the release in git: ``git tag v0.1.0 && git push origin v0.1.0``
2. Create a GitHub Release from the tag with release notes
3. Bump the version in ``pyproject.toml`` files to the next development
   version (e.g. ``0.1.1.dev0``) if desired

CI/CD
-----

The GitHub Actions workflow (``.github/workflows/ci.yml``) runs on every push
and PR:

- Tests on Python 3.14 (free-threaded build)
- Linting and formatting checks
- Type checking with ``ty``

Releases can be automated by configuring a publish workflow triggered on
tagged commits.
