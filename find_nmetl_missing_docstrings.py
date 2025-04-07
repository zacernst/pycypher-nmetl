#!/usr/bin/env python
"""
Script to find methods in the nmetl package without docstrings.
"""

import ast
import os
from pathlib import Path


class DocstringVisitor(ast.NodeVisitor):
    """AST visitor to find methods without docstrings."""

    def __init__(self):
        self.missing_docstrings = []
        self.current_class = None
        self.current_file = None

    def visit_ClassDef(self, node):
        """Visit a class definition."""
        prev_class = self.current_class
        self.current_class = node.name
        
        # Visit all child nodes
        self.generic_visit(node)
        self.current_class = prev_class

    def visit_FunctionDef(self, node):
        """Visit a function definition."""
        # Check if function has a docstring and is a method
        if self.current_class and not ast.get_docstring(node):
            self.missing_docstrings.append({
                "file": self.current_file,
                "class": self.current_class,
                "method": node.name,
                "line": node.lineno,
                "args": [arg.arg for arg in node.args.args],
                "returns": self._get_return_annotation(node)
            })
        
        # Visit all child nodes
        self.generic_visit(node)
    
    def _get_return_annotation(self, node):
        """Extract return annotation if present."""
        if node.returns:
            return ast.unparse(node.returns)
        return None


def analyze_file(file_path):
    """Analyze a Python file to find methods without docstrings."""
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
            visitor = DocstringVisitor()
            visitor.current_file = file_path
            visitor.visit(tree)
            return visitor.missing_docstrings
        except SyntaxError as e:
            print(f"Syntax error in {file_path}: {e}")
            return []


def main():
    """Main function to analyze Python files in the nmetl package."""
    # Get the repository root directory
    repo_dir = os.getcwd()
    
    # Define the nmetl package path
    package_path = os.path.join(repo_dir, "packages/nmetl/src/nmetl")
    
    if not os.path.exists(package_path):
        print(f"Package path does not exist: {package_path}")
        return
    
    # Find all Python files in the package
    all_missing = []
    
    for root, _, files in os.walk(package_path):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                # Skip __pycache__ directories
                if '__pycache__' not in file_path:
                    missing = analyze_file(file_path)
                    all_missing.extend(missing)
    
    # Sort by file and class
    all_missing.sort(key=lambda x: (x["file"], x["class"], x["line"]))
    
    # Print results
    print(f"\nFound {len(all_missing)} methods without docstrings in the nmetl package:")
    
    current_file = None
    current_class = None
    
    for item in all_missing:
        # Print file header if it's a new file
        if item["file"] != current_file:
            current_file = item["file"]
            rel_path = os.path.relpath(current_file, package_path)
            print(f"\n\n## File: {rel_path}")
            current_class = None
        
        # Print class header if it's a new class
        if item["class"] != current_class:
            current_class = item["class"]
            print(f"\n### Class: {current_class}")
        
        # Print method info
        args_str = ", ".join(item["args"])
        returns_str = f" -> {item['returns']}" if item["returns"] else ""
        print(f"\n#### Method: {item['method']}({args_str}){returns_str} (Line {item['line']})")


if __name__ == "__main__":
    main()
