# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Add package paths to sys.path
sys.path.insert(0, os.path.abspath("../packages/pycypher/src"))
sys.path.insert(0, os.path.abspath("../packages/shared/src"))
sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "PyCypher"
copyright = "2026, PyCypher Contributors"
author = "PyCypher Contributors"
release = "0.0.19"
version = "0.0.19"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "myst_parser",
]

# Mock imports for missing modules or C-extensions
autodoc_mock_imports = [
    # Legacy pycypher modules that no longer exist in the codebase
    "pycypher.node_classes",
    "pycypher.cypher_parser",
    "pycypher.query",
    "pycypher.tree_mixin",
    "pycypher.lineage",
    # Optional telemetry dependency
    "pyroscope",
]

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"

# Autosummary settings — disabled to prevent duplicate-object warnings
# (the API pages in docs/api/ use explicit automodule directives instead)
autosummary_generate = False
autosummary_imported_members = False

# Napoleon settings (for Google and NumPy style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True

# MyST parser settings
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# Templates path
templates_path = ["_templates"]

# List of patterns to exclude
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**.ipynb_checkpoints",
    ".venv",
    "venv",
    "README.md",  # docs-internal build instructions, not user-facing
    "SPHINX_SETUP_COMPLETE.md",  # setup log, not user-facing
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# Theme options
html_theme_options = {
    "navigation_depth": 4,
    "collapse_navigation": False,
    "sticky_navigation": True,
    "includehidden": True,
    "titles_only": False,
}

# -- Options for intersphinx extension ---------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html#configuration

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

# Timeout for fetching remote inventories (seconds).
# Keeps builds fast when external sites are unreachable.
intersphinx_timeout = 5

# Disable TLS verification for intersphinx fetches so builds succeed
# behind corporate proxies or environments with self-signed CA certs.
tls_verify = False

# -- Options for LaTeX output ------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-latex-output

latex_engine = "xelatex"

# LaTeX preamble for Unicode character support
latex_preamble = r'''
% Unicode support for XeLaTeX - handles most characters natively
\usepackage{fontspec}
\defaultfontfeatures{Ligatures=TeX}
'''

latex_elements = {
    "preamble": latex_preamble,
    "fncychap": "\\usepackage[Bjornstrup]{fncychap}",
    "fontpkg": "\\usepackage{times}",
    "geometry": "\\usepackage[margin=1in]{geometry}",
    "extraclassoptions": "openany,oneside",
}

# -- Options for todo extension ----------------------------------------------

todo_include_todos = True

# Suppress known harmless warnings
suppress_warnings = [
    "autosummary",  # autosummary duplicate-member warnings
    "ref.duplicate",  # duplicate object descriptions from autosummary + automodule
    "autodoc.mocked_object",  # mocked-import warnings for unavailable packages
]


def setup(app: object) -> None:
    """Filter noisy Pydantic duplicate-object-description warnings.

    Pydantic model fields get documented twice by autodoc (once as a class
    attribute and once as a descriptor), producing ~150 harmless warnings.
    """
    import logging

    class _DuplicateFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return "duplicate object description" not in record.getMessage()

    # Sphinx 7.x uses the root "sphinx" logger hierarchy
    for name in ("sphinx", "sphinx.domains", "sphinx.domains.python"):
        logging.getLogger(name).addFilter(_DuplicateFilter())
