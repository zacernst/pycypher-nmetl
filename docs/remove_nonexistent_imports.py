#!/usr/bin/env python3
"""
Script to remove references to non-existent modules from RST files.
"""

import os
import re
from pathlib import Path

# Define the modules to remove
MODULES_TO_REMOVE = [
    "nmetl.solver",
    "nmetl.goldberg",
    "fastopendata.utils",
    "nmetl.fact",
    "fastopendata.export",
    "fastopendata.transform",
    "fastopendata.ingest"
]

def remove_module_references(file_path):
    """Remove references to specified modules from a single RST file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    modified_content = content
    
    # Remove automodule directives for the specified modules
    for module in MODULES_TO_REMOVE:
        # Pattern to match the entire automodule section for the module
        pattern = rf'.. automodule:: {module}\s+:members:\s+:undoc-members:\s+:show-inheritance:'
        modified_content = re.sub(pattern, '', modified_content)
        
        # Pattern to match section headers for the module
        module_name = module.split('.')[-1]
        title_pattern = rf'{module_name.capitalize()}\n[~=]+\n\n'
        modified_content = re.sub(title_pattern, '', modified_content, flags=re.IGNORECASE)
    
    # Clean up any double newlines created by the removals
    modified_content = re.sub(r'\n{3,}', '\n\n', modified_content)
    
    # Only write if changes were made
    if content != modified_content:
        print(f"Removing module references in {file_path}")
        with open(file_path, 'w') as f:
            f.write(modified_content)
        return True
    return False

def remove_from_toctree(file_path):
    """Remove references to specified modules from toctree entries."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    modified_content = content
    
    # Remove toctree entries for the specified modules
    for module in MODULES_TO_REMOVE:
        module_path = module.replace('.', '/')
        pattern = rf'   {module_path}\n'
        modified_content = modified_content.replace(pattern, '')
    
    # Only write if changes were made
    if content != modified_content:
        print(f"Removing toctree entries in {file_path}")
        with open(file_path, 'w') as f:
            f.write(modified_content)
        return True
    return False

def main():
    """Main function to process all RST files."""
    docs_dir = Path('/Users/zac/git/pycypher-nmetl/docs')
    
    # Process all RST files
    files_modified = 0
    for file_path in docs_dir.glob('**/*.rst'):
        if remove_module_references(file_path):
            files_modified += 1
        if remove_from_toctree(file_path):
            files_modified += 1
    
    print(f"Modified {files_modified} files")
    print("Done!")

if __name__ == "__main__":
    main()
