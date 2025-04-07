#!/usr/bin/env python3
"""
Script to fix imports in RST files by replacing 'pycypher.util' with the correct module paths.
"""

import os
import re
from pathlib import Path

# Define the mapping of old imports to new imports
IMPORT_MAPPING = {
    "pycypher.util.logger": "nmetl.logger",
    "pycypher.util.cli": "nmetl.cli",
    "pycypher.util.configuration": "nmetl.configuration",
    "pycypher.util.config": "nmetl.config",
    "pycypher.util.nmetl_cli": "nmetl.nmetl_cli",
    "pycypher.util.exceptions": "nmetl.exceptions",
    "pycypher.util.helpers": "nmetl.helpers",
}

def fix_rst_file(file_path):
    """Fix imports in a single RST file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace imports in automodule directives
    modified_content = content
    for old_import, new_import in IMPORT_MAPPING.items():
        modified_content = modified_content.replace(
            f".. automodule:: {old_import}",
            f".. automodule:: {new_import}"
        )
    
    # Only write if changes were made
    if content != modified_content:
        print(f"Fixing imports in {file_path}")
        with open(file_path, 'w') as f:
            f.write(modified_content)
        return True
    return False

def main():
    """Main function to fix all RST files."""
    docs_dir = Path('/Users/zac/git/pycypher-nmetl/docs')
    
    # Fix imports in all RST files
    files_fixed = 0
    for file_path in docs_dir.glob('**/*.rst'):
        if fix_rst_file(file_path):
            files_fixed += 1
    
    print(f"Fixed imports in {files_fixed} files")
    print("Done!")

if __name__ == "__main__":
    main()
