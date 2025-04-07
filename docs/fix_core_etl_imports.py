#!/usr/bin/env python3
"""
Script to fix imports in RST files by replacing 'pycypher.core' and 'pycypher.etl' 
with the correct module paths.
"""

import os
import re
from pathlib import Path

# Define the mapping of old imports to new imports
IMPORT_MAPPING = {
    # Core modules
    "pycypher.core.cypher_lexer": "pycypher.cypher_lexer",
    "pycypher.core.cypher_parser": "pycypher.cypher_parser",
    "pycypher.core.node_classes": "pycypher.node_classes",
    "pycypher.core.tree_mixin": "pycypher.tree_mixin",
    
    # ETL modules
    "pycypher.etl.data_source": "nmetl.data_source",
    "pycypher.etl.fact": "nmetl.fact",
    "pycypher.etl.goldberg": "nmetl.goldberg",
    "pycypher.etl.message_types": "nmetl.message_types",
    "pycypher.etl.queue_processor": "nmetl.queue_processor",
    "pycypher.etl.solver": "nmetl.solver",
    "pycypher.etl.trigger": "nmetl.trigger",
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
        
        # Also replace references in toctree entries and other places
        modified_content = modified_content.replace(
            old_import,
            new_import
        )
    
    # Only write if changes were made
    if content != modified_content:
        print(f"Fixing imports in {file_path}")
        with open(file_path, 'w') as f:
            f.write(modified_content)
        return True
    return False

def rename_rst_files(directory):
    """Rename RST files from old module paths to new module paths."""
    changes_made = []
    
    for file_path in Path(directory).glob('**/*.rst'):
        old_name = file_path.name
        new_name = old_name
        
        for old_import, new_import in IMPORT_MAPPING.items():
            if old_import.replace('.', '_') in old_name:
                new_name = old_name.replace(
                    old_import.replace('.', '_'),
                    new_import.replace('.', '_')
                )
                break
        
        if old_name != new_name:
            new_path = file_path.parent / new_name
            print(f"Renaming {file_path} to {new_path}")
            
            # Create parent directory if it doesn't exist
            new_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Rename the file
            os.rename(file_path, new_path)
            changes_made.append((file_path, new_path))
    
    return changes_made

def update_references(directory, renamed_files):
    """Update references to renamed files in other RST files."""
    for file_path in Path(directory).glob('**/*.rst'):
        with open(file_path, 'r') as f:
            content = f.read()
        
        modified_content = content
        for old_path, new_path in renamed_files:
            old_name = old_path.name.replace('.rst', '')
            new_name = new_path.name.replace('.rst', '')
            
            # Replace references in toctree entries
            modified_content = modified_content.replace(old_name, new_name)
        
        if content != modified_content:
            print(f"Updating references in {file_path}")
            with open(file_path, 'w') as f:
                f.write(modified_content)

def main():
    """Main function to fix all RST files."""
    docs_dir = Path('/Users/zac/git/pycypher-nmetl/docs')
    
    # First fix imports in all RST files
    files_fixed = 0
    for file_path in docs_dir.glob('**/*.rst'):
        if fix_rst_file(file_path):
            files_fixed += 1
    
    # Then rename the files
    renamed_files = rename_rst_files(docs_dir)
    
    # Finally update references to renamed files
    update_references(docs_dir, renamed_files)
    
    print(f"Fixed imports in {files_fixed} files")
    print(f"Renamed {len(renamed_files)} files")
    print("Done!")

if __name__ == "__main__":
    main()
