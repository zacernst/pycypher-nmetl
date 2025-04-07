# pycypher-nmetl Documentation

This directory contains the Sphinx documentation for the pycypher-nmetl project.

## Building the Documentation

### Prerequisites

Make sure you have the required dependencies installed:

```bash
uv pip install sphinx sphinx-rtd-theme sphinx-autoapi sphinx-autodocgen
```

Note: This project uses `uv` as the package manager. If you don't have it installed, you can install it following the instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv).

### Building HTML Documentation

To build the HTML documentation:

```bash
cd docs
make html
```

The built documentation will be available in the `build/html` directory.

### Building PDF Documentation

To build the PDF documentation (requires LaTeX):

```bash
cd docs
make latexpdf
```

The built PDF will be available in the `build/latex` directory.

## Documentation Structure

- `source/`: Contains the source files for the documentation
  - `conf.py`: Sphinx configuration file
  - `index.rst`: Main index file
  - `*.rst`: Various documentation pages
  - `api/`: API documentation
  - `_templates/`: Custom templates
  - `_static/`: Static files (CSS, JavaScript, images)

## Updating the Documentation

1. Edit the RST files in the `source/` directory
2. Build the documentation to preview changes
3. Commit your changes

## Publishing the Documentation

The documentation is automatically built and published to GitHub Pages when changes are pushed to the main branch.
