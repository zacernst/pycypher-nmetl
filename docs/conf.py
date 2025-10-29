import os
import sys

sys.path.insert(0, os.path.abspath("../packages/fastopendata"))
sys.path.insert(0, os.path.abspath("../packages/pycypher"))
sys.path.insert(0, os.path.abspath("../packages/nmetl"))
sys.path.insert(0, os.path.abspath("../packages/shared"))
sys.path.insert(0, os.path.abspath("../packages"))
sys.path.insert(0, os.path.abspath(".."))

print(os.path.abspath(".."))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "NMETL"
copyright = "2025, Zachary Ernst"
author = "Zachary Ernst"
release = "0.0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["myst_parser"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
