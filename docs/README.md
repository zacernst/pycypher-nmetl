# NMETL Documentation

This directory contains the Sphinx documentation for the NMETL project.

## Building Documentation

To build the HTML documentation:

```bash
cd /pycypher-nmetl/docs
LC_ALL=C.UTF-8 uv run sphinx-build -b html . _build/html
```

Or using the Makefile:

```bash
cd /pycypher-nmetl/docs
LC_ALL=C.UTF-8 make html
```

## Viewing Documentation

After building, open `_build/html/index.html` in your web browser:

```bash
$BROWSER _build/html/index.html
```

## Documentation Structure

- `index.rst` - Main entry point
- `getting_started.rst` - Installation and basic usage
- `api/` - API reference documentation for all packages
- `tutorials/` - Step-by-step tutorials
- `user_guide/` - In-depth user guides
- `developer_guide/` - Developer documentation
- `conf.py` - Sphinx configuration
- `_static/` - Static files (CSS, images, etc.)
- `_templates/` - Custom Sphinx templates

## Adding Content

### Creating a New Tutorial

1. Create a new `.rst` file in `tutorials/`
2. Add it to the `toctree` in `tutorials/index.rst`

### Documenting a New Module

1. Add an `automodule` directive in the appropriate API file
2. Ensure the module is in the `sys.path` (see `conf.py`)

### Updating API Documentation

The API documentation is automatically generated from docstrings. To update:

1. Update docstrings in source code
2. Rebuild the documentation

## Dependencies

Sphinx documentation requires:

- sphinx
- sphinx_rtd_theme
- myst-parser (for Markdown support)
- autodoc/napoleon extensions (for Google-style docstrings)

All dependencies are installed with the project via `uv sync`.

## Known Issues

- Some placeholder modules (e.g., `pycypher.validation`, `pycypher.solver`) generate warnings
- These warnings are expected until those modules are implemented
- The build still succeeds and generates complete documentation
