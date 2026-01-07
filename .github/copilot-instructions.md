This project uses uv to manage virtual environments. Please use uv to preface any commands that involve Python package installations or virtual environment activations. For example, use `uv install <package>` to install a package within the uv-managed environment.

Use uv to run python scripts to ensure they execute within the correct virtual environment. For example, use `uv python script.py` to run a Python script.

When adding new dependencies to the project, make sure to update the uv configuration files accordingly to include these dependencies in the virtual environment.

This project uses ty as the type checker for Python code. Please ensure that all Python files pass ty checks before committing any changes. Run `uv run ty check` to verify type correctness in the codebase. All methods and functions should have appropriate type annotations to maintain type safety throughout the project.

This project uses Sphinx to generate documentation. Please ensure that any new modules, classes, or functions are documented with docstrings in accordance with Sphinx conventions. Docstrings should be in Google format.

When making significant changes to the codebase, please update the Sphinx documentation by running `uv run sphinx-build -b html docs/source docs/build` to regenerate the HTML documentation. Review the generated documentation to ensure that all changes are accurately reflected.
