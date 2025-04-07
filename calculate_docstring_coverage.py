#!/usr/bin/env python
"""
Script to calculate docstring coverage in the repository.
"""

import ast
import os
from collections import defaultdict
from pathlib import Path


class DocstringVisitor(ast.NodeVisitor):
    """AST visitor to count elements with and without docstrings."""

    def __init__(self):
        self.counts = {
            "classes": {"with_docstring": 0, "without_docstring": 0},
            "methods": {"with_docstring": 0, "without_docstring": 0},
            "functions": {"with_docstring": 0, "without_docstring": 0}
        }
        self.current_class = None

    def visit_ClassDef(self, node):
        """Visit a class definition."""
        prev_class = self.current_class
        self.current_class = node.name
        
        # Check if class has a docstring
        if ast.get_docstring(node):
            self.counts["classes"]["with_docstring"] += 1
        else:
            self.counts["classes"]["without_docstring"] += 1
        
        # Visit all child nodes
        self.generic_visit(node)
        self.current_class = prev_class

    def visit_FunctionDef(self, node):
        """Visit a function definition."""
        # Check if function has a docstring
        if ast.get_docstring(node):
            if self.current_class:
                self.counts["methods"]["with_docstring"] += 1
            else:
                self.counts["functions"]["with_docstring"] += 1
        else:
            if self.current_class:
                self.counts["methods"]["without_docstring"] += 1
            else:
                self.counts["functions"]["without_docstring"] += 1
        
        # Visit all child nodes
        self.generic_visit(node)


def analyze_file(file_path):
    """Analyze a Python file to count elements with and without docstrings."""
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
            visitor = DocstringVisitor()
            visitor.visit(tree)
            return visitor.counts
        except SyntaxError as e:
            print(f"Syntax error in {file_path}: {e}")
            return {
                "classes": {"with_docstring": 0, "without_docstring": 0},
                "methods": {"with_docstring": 0, "without_docstring": 0},
                "functions": {"with_docstring": 0, "without_docstring": 0}
            }


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
    package_stats = defaultdict(lambda: {
        "classes": {"with_docstring": 0, "without_docstring": 0},
        "methods": {"with_docstring": 0, "without_docstring": 0},
        "functions": {"with_docstring": 0, "without_docstring": 0}
    })
    
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
                            package_stats[package_path][category]["with_docstring"] += counts[category]["with_docstring"]
                            package_stats[package_path][category]["without_docstring"] += counts[category]["without_docstring"]
    
    # Calculate totals and percentages
    grand_total = {
        "classes": {"with_docstring": 0, "without_docstring": 0, "total": 0, "coverage": 0},
        "methods": {"with_docstring": 0, "without_docstring": 0, "total": 0, "coverage": 0},
        "functions": {"with_docstring": 0, "without_docstring": 0, "total": 0, "coverage": 0},
        "all": {"with_docstring": 0, "without_docstring": 0, "total": 0, "coverage": 0}
    }
    
    for package, stats in package_stats.items():
        for category in ["classes", "methods", "functions"]:
            with_ds = stats[category]["with_docstring"]
            without_ds = stats[category]["without_docstring"]
            total = with_ds + without_ds
            
            # Calculate coverage percentage
            coverage = (with_ds / total * 100) if total > 0 else 100
            
            # Add calculated values to stats
            stats[category]["total"] = total
            stats[category]["coverage"] = coverage
            
            # Add to grand total
            grand_total[category]["with_docstring"] += with_ds
            grand_total[category]["without_docstring"] += without_ds
            grand_total[category]["total"] += total
            
            # Add to overall total
            grand_total["all"]["with_docstring"] += with_ds
            grand_total["all"]["without_docstring"] += without_ds
            grand_total["all"]["total"] += total
    
    # Calculate coverage percentages for grand total
    for category in ["classes", "methods", "functions", "all"]:
        total = grand_total[category]["total"]
        with_ds = grand_total[category]["with_docstring"]
        grand_total[category]["coverage"] = (with_ds / total * 100) if total > 0 else 100
    
    # Print results
    print("\nDocstring Coverage by Package:")
    print("-" * 100)
    print(f"{'Package':<30} {'Classes':<20} {'Methods':<20} {'Functions':<20} {'Overall':<20}")
    print(f"{'':<30} {'Coverage':<10}{'Total':<10} {'Coverage':<10}{'Total':<10} {'Coverage':<10}{'Total':<10} {'Coverage':<10}{'Total':<10}")
    print("-" * 100)
    
    for package, stats in sorted(package_stats.items()):
        # Calculate overall coverage for the package
        total_with_ds = sum(stats[cat]["with_docstring"] for cat in ["classes", "methods", "functions"])
        total_all = sum(stats[cat]["total"] for cat in ["classes", "methods", "functions"])
        overall_coverage = (total_with_ds / total_all * 100) if total_all > 0 else 100
        
        print(f"{package.split('/')[-1]:<30} "
              f"{stats['classes']['coverage']:.1f}%{stats['classes']['total']:<9} "
              f"{stats['methods']['coverage']:.1f}%{stats['methods']['total']:<9} "
              f"{stats['functions']['coverage']:.1f}%{stats['functions']['total']:<9} "
              f"{overall_coverage:.1f}%{total_all:<9}")
    
    print("-" * 100)
    print(f"{'TOTAL':<30} "
          f"{grand_total['classes']['coverage']:.1f}%{grand_total['classes']['total']:<9} "
          f"{grand_total['methods']['coverage']:.1f}%{grand_total['methods']['total']:<9} "
          f"{grand_total['functions']['coverage']:.1f}%{grand_total['functions']['total']:<9} "
          f"{grand_total['all']['coverage']:.1f}%{grand_total['all']['total']:<9}")


if __name__ == "__main__":
    main()
