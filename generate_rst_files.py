#!/usr/bin/env python3
"""
Script to generate RST files for Sphinx documentation.
"""

import os
import sys
from pathlib import Path

# Define the base directory
BASE_DIR = Path("/Users/zac/git/pycypher-nmetl/docs")

# Define the module structures
PYCYPHER_MODULES = {
    "core": ["cypher_lexer", "cypher_parser", "node_classes", "tree_mixin"],
    "etl": [
        "data_source",
        "fact",
        "goldberg",
        "message_types",
        "queue_processor",
        "solver",
        "trigger",
    ],
    "shims": ["networkx_cypher"],
    "util": [
        "cli",
        "config",
        "configuration",
        "exceptions",
        "helpers",
        "logger",
        "nmetl_cli",
    ],
}

NMETL_MODULES = [
    "session",
    "data_source",
    "data_asset",
    "data_types",
    "queue_processor",
    "configuration",
    "config",
    "logger",
    "nmetl_cli",
]

FASTOPENDATA_MODULES = ["ingest", "transform", "export", "utils"]


# Create tutorial index files
def create_tutorial_index(package_name):
    file_path = BASE_DIR / package_name / "tutorials" / "index.rst"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    content = f"""{package_name.capitalize()} Tutorials
{"=" * len(package_name)}==========

This section contains tutorials and examples for the {package_name} package.

.. toctree::
   :maxdepth: 2
   
   getting_started
   advanced_usage
"""

    with open(file_path, "w") as f:
        f.write(content)

    # Create placeholder tutorial files
    for tutorial in ["getting_started", "advanced_usage"]:
        tutorial_path = (
            BASE_DIR / package_name / "tutorials" / f"{tutorial}.rst"
        )
        tutorial_content = f"""{tutorial.replace("_", " ").title()}
{"=" * len(tutorial)}

This is a placeholder for the {tutorial.replace("_", " ")} tutorial.
"""
        with open(tutorial_path, "w") as f:
            f.write(tutorial_content)


# Create PyCypher module RST files
def create_pycypher_rst_files():
    for category, modules in PYCYPHER_MODULES.items():
        category_dir = BASE_DIR / "pycypher" / "api" / category
        os.makedirs(category_dir, exist_ok=True)

        for module in modules:
            file_path = category_dir / f"{module}.rst"

            # Skip if file already exists
            if os.path.exists(file_path):
                continue

            title = module.replace("_", " ").title()
            content = f"""{title}
{"=" * len(title)}

.. automodule:: pycypher.{category}.{module}
   :members:
   :undoc-members:
   :show-inheritance:"""

            with open(file_path, "w") as f:
                f.write(content)


# Create NMETL module RST files
def create_nmetl_rst_files():
    api_dir = BASE_DIR / "nmetl" / "api"
    os.makedirs(api_dir, exist_ok=True)

    for module in NMETL_MODULES:
        file_path = api_dir / f"{module}.rst"

        # Skip if file already exists
        if os.path.exists(file_path):
            continue

        title = module.replace("_", " ").title()
        content = f"""{title}
{"=" * len(title)}

.. automodule:: nmetl.{module}
   :members:
   :undoc-members:
   :show-inheritance:"""

        with open(file_path, "w") as f:
            f.write(content)


# Create FastOpenData module RST files
def create_fastopendata_rst_files():
    api_dir = BASE_DIR / "fastopendata" / "api"
    os.makedirs(api_dir, exist_ok=True)

    for module in FASTOPENDATA_MODULES:
        file_path = api_dir / f"{module}.rst"

        # Skip if file already exists
        if os.path.exists(file_path):
            continue

        title = module.replace("_", " ").title()
        content = f"""{title}
{"=" * len(title)}

.. automodule:: fastopendata.{module}
   :members:
   :undoc-members:
   :show-inheritance:"""

        with open(file_path, "w") as f:
            f.write(content)


def main():
    # Create PyCypher RST files
    create_pycypher_rst_files()

    # Create NMETL RST files
    create_nmetl_rst_files()

    # Create FastOpenData RST files
    create_fastopendata_rst_files()

    # Create tutorial index files
    create_tutorial_index("pycypher")
    create_tutorial_index("nmetl")
    create_tutorial_index("fastopendata")

    print("RST files generated successfully!")


if __name__ == "__main__":
    main()
