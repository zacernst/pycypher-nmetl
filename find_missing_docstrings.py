#!/usr/bin/env python
"""
Script to find Python classes, methods, and functions without docstrings.
"""

import ast
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class DocstringVisitor(ast.NodeVisitor):
    """AST visitor to find missing docstrings in classes, methods, and functions."""

    def __init__(self):
        self.missing_docstrings = {
            "classes": [],
            "methods": [],
            "functions": []
        }
        self.current_class = None

    def visit_ClassDef(self, node):
        """Visit a class definition."""
        prev_class = self.current_class
        self.current_class = node.name
        
        # Check if class has a docstring
        if not ast.get_docstring(node):
            self.missing_docstrings["classes"].append(
                (self.current_class, node.lineno)
            )
        
        # Visit all child nodes
        self.generic_visit(node)
        self.current_class = prev_class

    def visit_FunctionDef(self, node):
        """Visit a function definition."""
        # Check if function has a docstring
        if not ast.get_docstring(node):
            if self.current_class:
                self.missing_docstrings["methods"].append(
                    (f"{self.current_class}.{node.name}", node.lineno)
                )
            else:
                self.missing_docstrings["functions"].append(
                    (node.name, node.lineno)
                )
        
        # Visit all child nodes
        self.generic_visit(node)


def analyze_file(file_path: str) -> Dict[str, List[Tuple[str, int]]]:
    """
    Analyze a Python file to find missing docstrings.
    
    Args:
        file_path: Path to the Python file to analyze
        
    Returns:
        Dictionary with lists of missing docstrings for classes, methods, and functions
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
            visitor = DocstringVisitor()
            visitor.visit(tree)
            return visitor.missing_docstrings
        except SyntaxError as e:
            print(f"Syntax error in {file_path}: {e}")
            return {"classes": [], "methods": [], "functions": []}


def find_python_files(directory: str) -> List[str]:
    """
    Find all Python files in a directory and its subdirectories.
    
    Args:
        directory: Directory to search for Python files
        
    Returns:
        List of paths to Python files
    """
    python_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                # Skip __pycache__ directories and virtual environments
                if '__pycache__' not in file_path and '.venv' not in file_path:
                    python_files.append(file_path)
    return python_files


def main():
    """Main function to analyze Python files in the repository."""
    # Get the repository root directory
    repo_dir = os.getcwd()
    
    # Find all Python files
    python_files = find_python_files(repo_dir)
    
    # Analyze each file
    total_missing = {"classes": 0, "methods": 0, "functions": 0}
    all_missing = {"classes": [], "methods": [], "functions": []}
    
    for file_path in python_files:
        rel_path = os.path.relpath(file_path, repo_dir)
        missing = analyze_file(file_path)
        
        # Add to totals
        for category in ["classes", "methods", "functions"]:
            total_missing[category] += len(missing[category])
            all_missing[category].extend([(rel_path, name, line) for name, line in missing[category]])
    
    # Print summary
    print(f"\nMissing Docstrings Summary:")
    print(f"  Classes:   {total_missing['classes']}")
    print(f"  Methods:   {total_missing['methods']}")
    print(f"  Functions: {total_missing['functions']}")
    print(f"  Total:     {sum(total_missing.values())}")
    
    # Print details if requested
    if len(sys.argv) > 1 and sys.argv[1] == "--details":
        print("\nDetails:")
        for category in ["classes", "methods", "functions"]:
            if all_missing[category]:
                print(f"\n{category.capitalize()}:")
                for file_path, name, line in all_missing[category]:
                    print(f"  {file_path}:{line} - {name}")


if __name__ == "__main__":
    main()
