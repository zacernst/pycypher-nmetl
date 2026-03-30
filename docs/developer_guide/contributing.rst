Contributing
============

How to set up a development environment and contribute to PyCypher.

.. contents:: In this guide
   :local:
   :depth: 2

Development Environment
-----------------------

Requirements
~~~~~~~~~~~~

- Python 3.14 or higher (free-threaded build recommended: ``3.14t``)
- `uv <https://docs.astral.sh/uv/>`_ for dependency management
- Docker (via Rancher Desktop or Docker Desktop) for containerised testing
- Git

Initial Setup
~~~~~~~~~~~~~

.. code-block:: bash

   # Clone the repository
   git clone <repository-url>
   cd pycypher-nmetl

   # Install dependencies (uv manages the workspace virtualenv)
   uv sync

   # Verify the setup
   uv run pytest --co -q   # list collected tests without running

All Python operations go through ``uv`` — it manages the workspace virtual
environment automatically.

Project Structure
~~~~~~~~~~~~~~~~~

This is a **monorepo workspace** with two interdependent packages:

::

   packages/
   ├── pycypher/    # Cypher parser, AST, relational algebra, ETL
   └── shared/      # Common utilities, logging, telemetry

   tests/           # All tests live at the repo root
   docs/            # Sphinx documentation
   examples/        # Runnable example scripts

Dependency order: ``shared`` → ``pycypher``.  The Makefile handles build
ordering automatically.

Code Style
----------

Formatting
~~~~~~~~~~

.. code-block:: bash

   make format   # Runs isort + ruff format

Ruff is configured in the root ``pyproject.toml`` with ``select = ["ALL"]``
(highly strict linting).  Fix any issues before committing.

Type Annotations
~~~~~~~~~~~~~~~~

**All functions and methods must have type annotations.**  Use ``ty`` (not
mypy) for type checking:

.. code-block:: bash

   uv run ty check

Docstrings
~~~~~~~~~~

Use `Google-style docstrings <https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings>`_.
Sphinx autodoc extracts documentation from source, so always update docstrings
when modifying public APIs.

.. code-block:: python

   def example_function(name: str, count: int = 1) -> list[str]:
       """Short summary of what this function does.

       Args:
           name: Description of name parameter.
           count: Description of count parameter.

       Returns:
           Description of return value.

       Raises:
           ValueError: When name is empty.

       """

Running Tests
-------------

.. code-block:: bash

   # Quick iteration during development (parallel, minimal output, skips slow tests)
   make test-quick

   # Run all tests (parallel, 8 threads)
   make test

   # Run a specific test file
   make test-file FILE=tests/test_ast_models.py

   # Stop on first failure
   make test-fast

   # Re-run only previously failed tests
   make test-failed

   # Run with coverage report
   make coverage

See :doc:`testing` for the full testing guide.

Making Changes
--------------

Branch Naming
~~~~~~~~~~~~~

Use descriptive branch names that indicate the type of change:

- ``feature/add-temporal-functions``
- ``fix/undefined-variable-error``
- ``docs/update-contributing-guide``

Commit Messages
~~~~~~~~~~~~~~~

Write clear, concise commit messages:

- Start with an imperative verb (Add, Fix, Update, Remove, Refactor)
- Keep the first line under 72 characters
- Reference issues or context in the body when relevant

Pull Request Process
--------------------

1. **Create a branch** from ``main``
2. **Make your changes** — keep PRs focused on a single concern
3. **Run the pre-PR check** — a single command that formats, lints, type-checks,
   and runs the test suite:

   .. code-block:: bash

      make check   # runs: lock-check → format → lint → typecheck → test-fast

4. **Push and open a PR** against ``main``

PR descriptions should include:

- A summary of what changed and why
- How to test the changes
- Any breaking changes or migration notes

Code Review
~~~~~~~~~~~

- All PRs require review before merging
- Reviewers check for correctness, test coverage, and adherence to project
  conventions
- Address review feedback promptly; use the conversation to discuss trade-offs

Adding Dependencies
-------------------

.. code-block:: bash

   # Edit the relevant package's pyproject.toml, then:
   uv sync

Never use ``pip install`` directly — the workspace relies on ``uv`` for
consistent dependency resolution.

Documentation
-------------

.. code-block:: bash

   # Build docs
   make docs

   # Or directly:
   uv run sphinx-build -b html docs docs/_build/html

Update documentation alongside code changes.  See :doc:`../tutorials/index`
for tutorial conventions.

Editor Integration (LSP)
------------------------

PyCypher includes a built-in Language Server Protocol (LSP) server that
provides real-time Cypher query assistance in any LSP-capable editor.  No
external extensions are required — the server uses only the standard library.

Features
~~~~~~~~

- **Diagnostics** — parse errors and semantic validation as you type
- **Completion** — keywords, functions, and entity labels
- **Hover** — function documentation and signatures
- **Signature Help** — parameter hints inside function calls
- **Go to Definition** — jump to variable definitions
- **Formatting** — auto-format Cypher queries

Starting the Server
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   python -m pycypher.cypher_lsp

The server communicates via JSON-RPC over stdin/stdout per the LSP
specification.

VS Code
~~~~~~~

Add to your ``.vscode/settings.json``:

.. code-block:: json

   {
       "pycypher.lspServer.path": "python -m pycypher.cypher_lsp"
   }

For workspace-specific configuration, create ``.vscode/settings.json`` at
the repository root.

Neovim (nvim-lspconfig)
~~~~~~~~~~~~~~~~~~~~~~~

Add to your Neovim configuration (``init.lua`` or equivalent):

.. code-block:: lua

   local lspconfig = require('lspconfig')
   local configs = require('lspconfig.configs')

   if not configs.pycypher then
       configs.pycypher = {
           default_config = {
               cmd = { 'python', '-m', 'pycypher.cypher_lsp' },
               filetypes = { 'cypher' },
               root_dir = lspconfig.util.find_git_ancestor,
               settings = {},
           },
       }
   end

   lspconfig.pycypher.setup({})

Emacs (lsp-mode)
~~~~~~~~~~~~~~~~

Add to your Emacs configuration:

.. code-block:: elisp

   (with-eval-after-load 'lsp-mode
     (add-to-list 'lsp-language-id-configuration '(cypher-mode . "cypher"))
     (lsp-register-client
      (make-lsp-client
       :new-connection (lsp-stdio-connection '("python" "-m" "pycypher.cypher_lsp"))
       :activation-fn (lsp-activate-on "cypher")
       :server-id 'pycypher-lsp)))

File Type Detection
~~~~~~~~~~~~~~~~~~~

Most editors need to associate ``.cypher`` files with the Cypher file type.
For Neovim, add:

.. code-block:: lua

   vim.filetype.add({ extension = { cypher = 'cypher' } })

For VS Code, add to ``settings.json``:

.. code-block:: json

   {
       "files.associations": {
           "*.cypher": "cypher"
       }
   }
