#!/usr/bin/env python3
"""
Script to fix imports in RST files by replacing 'pycypher.etl' with 'nmetl'.
"""

import os
import re
import sys
from pathlib import Path


def fix_rst_file(file_path):
    """Fix imports in a single RST file."""
    with open(file_path, "r") as f:
        content = f.read()

    # Replace imports in automodule directives
    modified_content = re.sub(
        r".. automodule:: pycypher\.etl\.([a-zA-Z_]+)",
        r".. automodule:: nmetl.\1",
        content,
    )

    # Replace imports in toctree entries
    modified_content = re.sub(
        r"pycypher\.etl\.([a-zA-Z_]+)", r"nmetl.\1", modified_content
    )

    # Only write if changes were made
    if content != modified_content:
        print(f"Fixing imports in {file_path}")
        with open(file_path, "w") as f:
            f.write(modified_content)
        return True
    return False


def rename_rst_files(directory):
    """Rename RST files from pycypher.etl.* to nmetl.*."""
    changes_made = []

    for file_path in Path(directory).glob("**/*.rst"):
        if "pycypher.etl." in file_path.name:
            new_name = file_path.name.replace("pycypher.etl.", "nmetl.")
            new_path = file_path.parent / new_name

            print(f"Renaming {file_path} to {new_path}")
            os.rename(file_path, new_path)
            changes_made.append((file_path, new_path))

    return changes_made


def update_references(directory, renamed_files):
    """Update references to renamed files in other RST files."""
    for file_path in Path(directory).glob("**/*.rst"):
        with open(file_path, "r") as f:
            content = f.read()

        modified_content = content
        for old_path, new_path in renamed_files:
            old_name = old_path.name.replace(".rst", "")
            new_name = new_path.name.replace(".rst", "")

            # Replace references in toctree entries
            modified_content = modified_content.replace(old_name, new_name)

        if content != modified_content:
            print(f"Updating references in {file_path}")
            with open(file_path, "w") as f:
                f.write(modified_content)


def main():
    """Main function to fix all RST files."""
    sphinx_docs_dir = Path("/Users/zac/git/pycypher-nmetl/sphinx_docs")

    # First fix imports in all RST files
    files_fixed = 0
    for file_path in sphinx_docs_dir.glob("**/*.rst"):
        if fix_rst_file(file_path):
            files_fixed += 1

    # Then rename the files
    renamed_files = rename_rst_files(sphinx_docs_dir)

    # Finally update references to renamed files
    update_references(sphinx_docs_dir, renamed_files)

    print(f"Fixed imports in {files_fixed} files")
    print(f"Renamed {len(renamed_files)} files")
    print("Done!")


if __name__ == "__main__":
    main()
