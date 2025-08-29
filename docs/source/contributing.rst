Contributing
============

We welcome contributions to the pycypher-nmetl project!

Development Setup
----------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

       git clone https://github.com/your-username/pycypher-nmetl.git
       cd pycypher-nmetl

3. Install development dependencies:

   .. code-block:: bash

       uv pip install -e ".[dev]"

4. Create a branch for your feature:

   .. code-block:: bash

       git checkout -b feature-name

5. Make your changes and commit them:

   .. code-block:: bash

       git commit -m "Description of your changes"

6. Push your changes to your fork:

   .. code-block:: bash

       git push origin feature-name

7. Open a pull request on GitHub

Code Style
----------

We use ruff for code formatting and linting. Before submitting a pull request, please run:

.. code-block:: bash

    uv run ruff check .
    uv run ruff format .

Testing
-------

We use pytest for testing. To run the tests:

.. code-block:: bash

    uv run pytest

Documentation
-------------

We use Sphinx for documentation. To build the documentation:

.. code-block:: bash

    cd docs
    make html
