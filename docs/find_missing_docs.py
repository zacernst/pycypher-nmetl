#!/usr/bin/env python3
"""
Script to identify classes, methods, and functions that are missing inline documentation.
"""

import ast
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Define the packages to analyze
PACKAGES = [
    "../packages/pycypher/src/pycypher",
    "../packages/nmetl/src/nmetl",
    "../packages/fastopendata/src/fastopendata"
]

class DocstringVisitor(ast.NodeVisitor):
    """AST visitor to find missing docstrings."""
    
    def __init__(self):
        self.missing_docstrings = {
            "module": [],
            "class": [],
            "method": [],
            "function": []
        }
        self.current_module = ""
        self.current_class = ""
        
    def visit_Module(self, node):
        """Visit a module node."""
        if not ast.get_docstring(node):
            self.missing_docstrings["module"].append(self.current_module)
        self.generic_visit(node)
    
    def visit_ClassDef(self, node):
        """Visit a class definition node."""
        prev_class = self.current_class
        self.current_class = f"{self.current_module}.{node.name}"
        
        if not ast.get_docstring(node):
            self.missing_docstrings["class"].append(self.current_class)
        
        self.generic_visit(node)
        self.current_class = prev_class
    
    def visit_FunctionDef(self, node):
        """Visit a function definition node."""
        # Skip special methods like __init__, __str__, etc.
        if node.name.startswith('__') and node.name.endswith('__'):
            self.generic_visit(node)
            return
            
        if self.current_class:
            # This is a method
            full_name = f"{self.current_class}.{node.name}"
            if not ast.get_docstring(node):
                self.missing_docstrings["method"].append(full_name)
        else:
            # This is a function
            full_name = f"{self.current_module}.{node.name}"
            if not ast.get_docstring(node):
                self.missing_docstrings["function"].append(full_name)
        
        self.generic_visit(node)

def analyze_file(file_path: Path) -> Dict[str, List[str]]:
    """Analyze a Python file for missing docstrings."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
        visitor = DocstringVisitor()
        visitor.current_module = str(file_path).replace('/', '.').replace('.py', '')
        visitor.visit(tree)
        return visitor.missing_docstrings
    except SyntaxError:
        print(f"Syntax error in {file_path}")
        return {"module": [], "class": [], "method": [], "function": []}

def analyze_package(package_dir: str) -> Dict[str, List[str]]:
    """Analyze all Python files in a package for missing docstrings."""
    results = {"module": [], "class": [], "method": [], "function": []}
    
    for root, _, files in os.walk(package_dir):
        for file in files:
            if file.endswith('.py') and '__pycache__' not in root:
                file_path = Path(os.path.join(root, file))
                file_results = analyze_file(file_path)
                
                for key, values in file_results.items():
                    results[key].extend(values)
    
    return results

def calculate_importance(missing_docs: Dict[str, List[str]]) -> List[Tuple[str, str, float]]:
    """Calculate the importance of documenting each item."""
    importance_scores = []
    
    # Define weights for different types
    weights = {
        "module": 1.0,
        "class": 0.9,
        "method": 0.7,
        "function": 0.8
    }
    
    # Calculate importance based on type and name
    for doc_type, items in missing_docs.items():
        for item in items:
            score = weights[doc_type]
            
            # Increase score for items that are likely to be public API
            if not any(part.startswith('_') for part in item.split('.')):
                score += 0.2
            
            # Increase score for items with important-sounding names
            important_terms = ['main', 'api', 'core', 'parse', 'query', 'session', 'data']
            if any(term in item.lower() for term in important_terms):
                score += 0.1
            
            importance_scores.append((doc_type, item, score))
    
    # Sort by importance score (descending)
    return sorted(importance_scores, key=lambda x: x[2], reverse=True)

def main():
    """Main function to analyze packages and print results."""
    all_missing_docs = {"module": [], "class": [], "method": [], "function": []}
    
    for package_dir in PACKAGES:
        print(f"Analyzing {package_dir}...")
        package_results = analyze_package(package_dir)
        
        for key, values in package_results.items():
            all_missing_docs[key].extend(values)
    
    # Print summary
    print("\nSummary of missing documentation:")
    print(f"Modules: {len(all_missing_docs['module'])}")
    print(f"Classes: {len(all_missing_docs['class'])}")
    print(f"Methods: {len(all_missing_docs['method'])}")
    print(f"Functions: {len(all_missing_docs['function'])}")
    
    # Calculate and print importance
    importance_scores = calculate_importance(all_missing_docs)
    
    print("\nMost important items to document (top 20):")
    for i, (doc_type, item, score) in enumerate(importance_scores[:20], 1):
        print(f"{i}. {doc_type.capitalize()}: {item} (Score: {score:.2f})")
    
    # Print all missing docstrings by type
    print("\nAll missing module docstrings:")
    for item in sorted(all_missing_docs["module"]):
        print(f"  {item}")
    
    print("\nAll missing class docstrings:")
    for item in sorted(all_missing_docs["class"]):
        print(f"  {item}")
    
    print("\nAll missing function docstrings:")
    for item in sorted(all_missing_docs["function"]):
        print(f"  {item}")
    
    print("\nAll missing method docstrings:")
    for item in sorted(all_missing_docs["method"]):
        print(f"  {item}")

if __name__ == "__main__":
    main()
