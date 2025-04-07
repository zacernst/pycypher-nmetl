#!/usr/bin/env python
"""
Script to count all classes, methods, and functions in the repository.
"""

import ast
import os
from collections import defaultdict
from pathlib import Path


class ElementCounter(ast.NodeVisitor):
    """AST visitor to count all classes, methods, and functions."""

    def __init__(self):
        self.counts = {
            "classes": 0,
            "methods": 0,
            "functions": 0
        }
        self.current_class = None

    def visit_ClassDef(self, node):
        """Visit a class definition."""
        prev_class = self.current_class
        self.current_class = node.name
        
        # Count the class
        self.counts["classes"] += 1
        
        # Visit all child nodes
        self.generic_visit(node)
        self.current_class = prev_class

    def visit_FunctionDef(self, node):
        """Visit a function definition."""
        # Count the function or method
        if self.current_class:
            self.counts["methods"] += 1
        else:
            self.counts["functions"] += 1
        
        # Visit all child nodes
        self.generic_visit(node)


def analyze_file(file_path):
    """Analyze a Python file to count all elements."""
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
            counter = ElementCounter()
            counter.visit(tree)
            return counter.counts
        except SyntaxError as e:
            print(f"Syntax error in {file_path}: {e}")
            return {"classes": 0, "methods": 0, "functions": 0}


def main():
    """Main function to analyze Python files in the repository."""
    # Get the repository root directory
    repo_dir = os.getcwd()
    
    # Define the packages to analyze
    packages = [
        "packages/pycypher/src/pycypher",
        "packages/nmetl/src/nmetl",
        "packages/fastopendata/src/fastopendata",
        "tests"
    ]
    
    # Initialize counters
    package_stats = defaultdict(lambda: {"classes": 0, "methods": 0, "functions": 0, "total": 0})
    
    # Analyze each package
    for package_path in packages:
        full_path = os.path.join(repo_dir, package_path)
        
        if not os.path.exists(full_path):
            print(f"Package path does not exist: {package_path}")
            continue
        
        # Find all Python files in the package
        for root, _, files in os.walk(full_path):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    # Skip __pycache__ directories
                    if '__pycache__' not in file_path:
                        counts = analyze_file(file_path)
                        
                        # Add to package stats
                        for category in ["classes", "methods", "functions"]:
                            package_stats[package_path][category] += counts[category]
                            package_stats[package_path]["total"] += counts[category]
    
    # Print results
    print("\nTotal Elements by Package:")
    print("-" * 80)
    print(f"{'Package':<40} {'Classes':<10} {'Methods':<10} {'Functions':<10} {'Total':<10}")
    print("-" * 80)
    
    grand_total = {"classes": 0, "methods": 0, "functions": 0, "total": 0}
    
    for package, stats in sorted(package_stats.items()):
        print(f"{package:<40} {stats['classes']:<10} {stats['methods']:<10} {stats['functions']:<10} {stats['total']:<10}")
        
        # Add to grand total
        for category in ["classes", "methods", "functions", "total"]:
            grand_total[category] += stats[category]
    
    print("-" * 80)
    print(f"{'TOTAL':<40} {grand_total['classes']:<10} {grand_total['methods']:<10} {grand_total['functions']:<10} {grand_total['total']:<10}")


if __name__ == "__main__":
    main()
