# Documentation

This directory contains the Sphinx documentation for the NMETL project.

## Building the Documentation

### Prerequisites

Install the required packages:

```bash
pip install sphinx sphinx-rtd-theme myst-parser
```

### Build HTML Documentation

From this directory, run:

```bash
make html
```

The generated documentation will be in `_build/html/`. Open `_build/html/index.html` in your browser.

### Build PDF Documentation

```bash
make latexpdf
```

### Clean Build

```bash
make clean
```

## Documentation Structure

- `index.rst` - Main documentation entry point
- `conf.py` - Sphinx configuration
- `nmetl_tutorial.md` - Getting started tutorial
- `quickref.rst` - Quick reference guide
- `examples.rst` - Code examples
- `GRAMMAR_PARSER_GUIDE.md` - Comprehensive grammar parser guide
- `AST_VARIABLE_REPRESENTATION.md` - AST Variable usage and migration guide
- `SAT_SOLVER_INTEGRATION.md` - SAT solver integration guide
- `api/` - API reference documentation
  - `index.rst` - API overview
  - `nmetl.rst` - NMETL package API
  - `pycypher.rst` - PyCypher package API
  - `shared.rst` - Shared utilities API
  - `fastopendata.rst` - FastOpenData package API
- `TEST_COVERAGE_CYPHER_QUERY_SOLVER.md` - Testing documentation

## Adding New Documentation

### Add a New Tutorial

1. Create a new `.rst` or `.md` file in the `docs/` directory
2. Add it to the appropriate `toctree` in `index.rst`

### Add API Documentation for a New Module

1. Edit the appropriate file in `api/` (e.g., `api/nmetl.rst`)
2. Add an `automodule` directive:

```rst
.. automodule:: nmetl.new_module
   :members:
   :undoc-members:
   :show-inheritance:
```

### Add a New Package

1. Create a new `.rst` file in `api/` (e.g., `api/newpackage.rst`)
2. Add it to `api/index.rst` in the toctree
3. Document all modules in the package

## Documentation Guidelines

- Use clear, concise language
- Include code examples for all major features
- Document all public APIs with docstrings
- Keep examples up-to-date with the code
- Use semantic markup (code blocks, cross-references, etc.)

## Cross-References

Use Sphinx cross-references to link between pages:

```rst
:doc:`nmetl_tutorial`           # Link to document
:ref:`section-label`             # Link to section
:class:`nmetl.Session`           # Link to class
:func:`pycypher.parse`           # Link to function
:mod:`pycypher.fact_collection` # Link to module
```

## Docstring Format

Use Google-style or NumPy-style docstrings:

```python
def example_function(param1, param2):
    """
    Brief description of function.
    
    More detailed description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Example:
        >>> example_function(1, 2)
        3
    """
    return param1 + param2
```

## Auto-generated Documentation

The API reference documentation is automatically generated from docstrings in the source code using Sphinx's `autodoc` extension. To update:

1. Update docstrings in source code
2. Rebuild documentation with `make html`

## Viewing Documentation Locally

After building, you can view the documentation by opening:

```
_build/html/index.html
```

Or use Python's built-in server:

```bash
cd _build/html
python -m http.server 8000
```

Then open http://localhost:8000 in your browser.
