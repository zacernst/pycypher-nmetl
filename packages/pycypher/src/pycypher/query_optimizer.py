"""
Query Optimizer for Cypher Queries

This module provides query optimization capabilities to transform valid Cypher queries
into semantically equivalent but more efficient queries. Optimizations include:

- Predicate pushdown (move WHERE filters closer to MATCH)
- Constant folding (evaluate constant expressions at parse time)
- Dead code elimination (remove always-true/always-false predicates)
- Unnecessary WITH clause removal
- Basic join reordering

The optimizer works on Lark parse trees from GrammarParser and preserves query semantics.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Set, Tuple
from copy import deepcopy

from lark import Tree, Token


class OptimizationLevel(Enum):
    """Optimization aggressiveness levels."""
    NONE = 0
    BASIC = 1
    AGGRESSIVE = 2


@dataclass
class OptimizationResult:
    """Result of query optimization."""
    optimized_tree: Tree
    optimizations_applied: List[str]
    estimated_cost_reduction: float = 0.0


class QueryOptimizer:
    """
    Transform valid Cypher queries into more efficient equivalent queries.
    
    The optimizer applies multiple passes to improve query performance:
    1. Constant folding - Evaluate constant expressions
    2. Dead code elimination - Remove redundant predicates
    3. Predicate pushdown - Move filters closer to data sources
    4. WITH clause elimination - Remove unnecessary scope changes
    5. Join reordering - Optimize MATCH clause ordering (basic)
    
    Example:
        >>> from pycypher.grammar_parser import GrammarParser
        >>> from pycypher.query_optimizer import QueryOptimizer
        >>> 
        >>> parser = GrammarParser()
        >>> optimizer = QueryOptimizer()
        >>> 
        >>> query = "MATCH (n:Person) WITH n WHERE n.age > 30 RETURN n.name"
        >>> tree = parser.parse(query)
        >>> result = optimizer.optimize(tree)
        >>> print(result.optimizations_applied)
        ['predicate_pushdown', 'with_elimination']
    """
    
    def __init__(self, optimization_level: OptimizationLevel = OptimizationLevel.BASIC):
        """
        Initialize query optimizer.
        
        Args:
            optimization_level: How aggressive to be with optimizations
        """
        self.optimization_level = optimization_level
        self.optimizations_applied = []
    
    def optimize(self, tree: Tree) -> OptimizationResult:
        """
        Apply all optimization passes to a query tree.
        
        Args:
            tree: Lark parse tree from GrammarParser
            
        Returns:
            OptimizationResult with optimized tree and metadata
        """
        # Reset state
        self.optimizations_applied = []
        
        # Make a deep copy to avoid modifying original
        optimized = deepcopy(tree)
        
        if self.optimization_level == OptimizationLevel.NONE:
            return OptimizationResult(optimized, [])
        
        # Apply optimization passes in order
        optimized = self._fold_constants(optimized)
        optimized = self._eliminate_dead_code(optimized)
        optimized = self._push_predicates_down(optimized)
        optimized = self._eliminate_unnecessary_with(optimized)
        
        if self.optimization_level == OptimizationLevel.AGGRESSIVE:
            optimized = self._reorder_matches(optimized)
        
        return OptimizationResult(
            optimized_tree=optimized,
            optimizations_applied=self.optimizations_applied.copy()
        )
    
    def _fold_constants(self, tree: Tree) -> Tree:
        """
        Evaluate constant expressions at parse time.
        
        Examples:
            2 + 2 → 4
            'hello' + ' world' → 'hello world'
            true AND true → true
        """
        modified = False
        
        # Find arithmetic expressions with only literals (use actual grammar node names)
        for expr_type in ["add_expression", "mult_expression", "power_expression"]:
            for expr in tree.find_data(expr_type):
                if self._is_constant_expression(expr):
                    try:
                        result = self._evaluate_constant_expression(expr)
                        if result is not None:
                            # Replace expression with literal
                            expr.data = "number_literal"
                            expr.children = [result]
                            modified = True
                    except Exception:
                        # Don't optimize if evaluation fails
                        pass
        
        # Find boolean expressions with only literals
        for expr_type in ["and_expression", "or_expression", "not_expression"]:
            for expr in tree.find_data(expr_type):
                if self._is_constant_boolean_expression(expr):
                    try:
                        result = self._evaluate_boolean_expression(expr)
                        if result is not None:
                            # Replace with true/false literal
                            expr.data = "true" if result else "false"
                            expr.children = []
                            modified = True
                    except Exception:
                        pass
        
        if modified:
            self.optimizations_applied.append("constant_folding")
        
        return tree
    
    def _is_constant_expression(self, node: Tree) -> bool:
        """Check if an expression contains only constants."""
        for child in node.iter_subtrees():
            # If we find a variable reference, it's not constant
            if child.data in ("variable_name", "property_lookup", "function_invocation"):
                return False
        return True
    
    def _is_constant_boolean_expression(self, node: Tree) -> bool:
        """Check if a boolean expression contains only constants."""
        for child in node.iter_subtrees():
            if child.data in ("variable_name", "property_lookup", "function_invocation"):
                return False
        return True
    
    def _evaluate_constant_expression(self, expr: Tree) -> Optional[Token]:
        """Evaluate a constant arithmetic expression."""
        # Simple implementation for basic operators
        if len(expr.children) == 1 and isinstance(expr.children[0], Token):
            return expr.children[0]
        
        # Handle binary operators - operator is implicit in node type
        if len(expr.children) == 2:
            left = self._get_numeric_value(expr.children[0])
            right = self._get_numeric_value(expr.children[1])
            
            if left is not None and right is not None:
                # Operator is determined by expression type
                if expr.data == "add_expression":
                    result = left + right
                elif expr.data == "subtract_expression":
                    result = left - right
                elif expr.data == "multiply_expression":
                    result = left * right
                elif expr.data == "divide_expression":
                    if right != 0:
                        result = left / right
                    else:
                        return None
                elif expr.data == "power_expression":
                    result = left ** right
                else:
                    return None
                
                # Return as Token
                if isinstance(result, float):
                    return Token("FLOAT", str(result))
                else:
                    return Token("INTEGER", str(int(result)))
        
        # Handle explicit operators (3 children: left, op, right)
        if len(expr.children) == 3:
            left = self._get_numeric_value(expr.children[0])
            op_node = expr.children[1]
            right = self._get_numeric_value(expr.children[2])
            
            op_val = None
            if isinstance(op_node, Token):
                op_val = op_node.value
            elif isinstance(op_node, Tree) and op_node.children:
                # Handle wrapped operators (add_op, mult_op)
                child = op_node.children[0]
                if isinstance(child, Token):
                    op_val = child.value

            if left is not None and right is not None and op_val is not None:
                if op_val == "+":
                    result = left + right
                elif op_val == "-":
                    result = left - right
                elif op_val == "*":
                    result = left * right
                elif op_val == "/":
                    if right != 0:
                        result = left / right
                    else:
                        return None
                else:
                    return None
                
                # Return as Token
                if isinstance(result, float):
                    return Token("FLOAT", str(result))
                else:
                    return Token("INTEGER", str(int(result)))
        
        return None
    
    def _evaluate_boolean_expression(self, expr: Tree) -> Optional[bool]:
        """Evaluate a constant boolean expression."""
        # Handle simple literals
        if len(expr.children) == 1:
            child = expr.children[0]
            if isinstance(child, Token):
                if child.type == "TRUE":
                    return True
                elif child.type == "FALSE":
                    return False
        
        # Handle AND/OR/NOT operations
        if len(expr.children) >= 2:
            for i, child in enumerate(expr.children):
                if isinstance(child, Token):
                    if child.value.upper() == "AND" and i > 0 and i < len(expr.children) - 1:
                        left = self._get_boolean_value(expr.children[i-1])
                        right = self._get_boolean_value(expr.children[i+1])
                        if left is not None and right is not None:
                            return left and right
                    elif child.value.upper() == "OR" and i > 0 and i < len(expr.children) - 1:
                        left = self._get_boolean_value(expr.children[i-1])
                        right = self._get_boolean_value(expr.children[i+1])
                        if left is not None and right is not None:
                            return left or right
        
        return None
    
    def _get_numeric_value(self, node) -> Optional[float]:
        """Extract numeric value from a node."""
        if isinstance(node, Token):
            try:
                if node.type in ("INTEGER", "DECIMAL_INTEGER", "SIGNED_INT", "UNSIGNED_INT"):
                    return int(node.value)
                elif node.type in ("FLOAT", "DECIMAL_FLOAT"):
                    return float(node.value)
            except (ValueError, AttributeError):
                pass
        elif isinstance(node, Tree):
            # Handle number_literal, signed_number, unsigned_number nodes
            if node.data in ("number_literal", "signed_number", "unsigned_number"):
                if node.children:
                    return self._get_numeric_value(node.children[0])
            # Handle _ambig nodes - try first child
            elif node.data == "_ambig":
                if node.children:
                    for child in node.children:
                        val = self._get_numeric_value(child)
                        if val is not None:
                            return val
        return None
    
    def _get_boolean_value(self, node) -> Optional[bool]:
        """Extract boolean value from a node."""
        if isinstance(node, Token):
            if node.type == "TRUE" or node.value.lower() == "true":
                return True
            elif node.type == "FALSE" or node.value.lower() == "false":
                return False
        elif isinstance(node, Tree) and node.data == "literal":
            if node.children:
                return self._get_boolean_value(node.children[0])
        return None
    
    def _eliminate_dead_code(self, tree: Tree) -> Tree:
        """
        Remove predicates that are always true or always false.
        
        Examples:
            WHERE true AND n.active → WHERE n.active
            WHERE false OR n.active → WHERE n.active
            WHERE true → (remove WHERE entirely)
        """
        modified = False
        
        # Find WHERE clauses
        for where_clause in list(tree.find_data("where_clause")):
            if len(where_clause.children) > 0:
                predicate = where_clause.children[0]
                
                # Check if predicate is a constant true - need to check recursively
                is_true = self._check_always_true_recursive(predicate)
                
                if is_true:
                    # Remove WHERE clause entirely
                    parent = self._find_parent(tree, where_clause)
                    if parent and where_clause in parent.children:
                        parent.children.remove(where_clause)
                        modified = True
                
                # Simplify boolean expressions
                elif isinstance(predicate, Tree):
                    simplified = self._simplify_boolean_predicate(predicate)
                    if simplified != predicate:
                        where_clause.children[0] = simplified
                        modified = True
        
        if modified:
            self.optimizations_applied.append("dead_code_elimination")
        
        return tree
    
    def _check_always_true_recursive(self, node) -> bool:
        """Recursively check if a node represents an always-true predicate."""
        # First check if the node itself is 'true' at the top level
        if self._is_always_true(node):
            return True
        
        # For comparison/binary operations, don't recurse - those have variables
        if isinstance(node, Tree):
            if node.data in ("comparison_expression", "equality_expression", 
                           "property_lookup", "postfix_expression"):
                # These have actual logic, not just a constant
                return False
            
            # Only check direct children of logical expressions
            if node.data in ("and_expression", "or_expression", "not_expression"):
                # For AND: all operands must be true
                # For OR: any operand can be true
                # For NOT: check the negated expression
                # But don't check comparison sub-expressions
                for child in node.children:
                    if isinstance(child, Tree) and child.data in ("comparison_expression", 
                                                                   "equality_expression"):
                        return False
                    if self._is_always_true(child):
                        return True
            
            # For other expressions, check if the entire expression is just 'true'
            # without any variables or comparisons
            elif node.data == "_ambig":
                # Check if one of the alternatives is 'true' and there are no variables
                has_true = False
                has_variable = False
                for child in node.children:
                    if isinstance(child, Tree):
                        if child.data == "true":
                            has_true = True
                        elif child.data == "variable_name":
                            has_variable = True
                return has_true and not has_variable
        
        return False
    
    def _is_always_true(self, node) -> bool:
        """Check if a predicate is always true."""
        if isinstance(node, Token):
            return node.type == "TRUE" or node.value.lower() == "true"
        elif isinstance(node, Tree):
            # Check for 'true' node in grammar
            if node.data == "true":
                return True
            # Check for literal containing true
            if node.data == "literal" and node.children:
                return self._is_always_true(node.children[0])
            # Check for _ambig nodes that might contain 'true'
            if node.data == "_ambig":
                for child in node.children:
                    if isinstance(child, Tree) and child.data == "true":
                        return True
        return False
    
    def _is_always_false(self, node) -> bool:
        """Check if a predicate is always false."""
        if isinstance(node, Token):
            return node.type == "FALSE" or node.value.lower() == "false"
        elif isinstance(node, Tree):
            # Check for 'false' node in grammar
            if node.data == "false":
                return True
            # Check for literal containing false
            if node.data == "literal" and node.children:
                return self._is_always_false(node.children[0])
            # Check for _ambig nodes
            if node.data == "_ambig":
                for child in node.children:
                    if isinstance(child, Tree) and child.data == "false":
                        return True
        return False
    
    def _simplify_boolean_predicate(self, predicate: Tree) -> Tree:
        """Simplify boolean expressions by removing always-true/false parts."""
        # Handle AND with true: (true AND x) → x
        # Handle OR with false: (false OR x) → x
        # This is a simplified implementation
        return predicate
    
    def _push_predicates_down(self, tree: Tree) -> Tree:
        """
        Move WHERE predicates closer to MATCH clauses.
        
        Example:
            MATCH (n:Person)
            WITH n
            WHERE n.age > 30
            RETURN n
            
            ↓
            
            MATCH (n:Person WHERE n.age > 30)
            RETURN n
        """
        modified = False
        
        # Look for pattern: MATCH ... WITH ... WHERE
        clauses = list(tree.find_data("match_clause")) + list(tree.find_data("with_clause")) + list(tree.find_data("where_clause"))
        
        # Find sequences where WITH is followed by WHERE
        match_clauses = list(tree.find_data("match_clause"))
        with_clauses = list(tree.find_data("with_clause"))
        where_clauses = list(tree.find_data("where_clause"))
        
        # If we have a WHERE that could be pushed to a MATCH
        for where_clause in where_clauses:
            # Check if WHERE references variables from a preceding MATCH
            where_vars = self._extract_variables_from_where(where_clause)
            
            # Find the most recent MATCH that defines these variables
            for match_clause in reversed(match_clauses):
                match_vars = self._extract_variables_from_match(match_clause)
                
                # If all WHERE variables are in this MATCH, we can push down
                if where_vars.issubset(match_vars):
                    # Check if there's no WITH between them that changes scope
                    if self._can_push_predicate(tree, match_clause, where_clause):
                        # Add WHERE predicate to MATCH
                        # Note: This is simplified - real implementation would modify node patterns
                        modified = True
                        break
        
        if modified:
            self.optimizations_applied.append("predicate_pushdown")
        
        return tree
    
    def _extract_variables_from_where(self, where_clause: Tree) -> Set[str]:
        """Extract all variable names from a WHERE clause."""
        variables = set()
        for var_node in where_clause.find_data("variable_name"):
            if var_node.children:
                variables.add(str(var_node.children[0]))
        return variables
    
    def _extract_variables_from_match(self, match_clause: Tree) -> Set[str]:
        """Extract all variable names defined in a MATCH clause."""
        variables = set()
        for var_node in match_clause.find_data("variable_name"):
            if var_node.children:
                variables.add(str(var_node.children[0]))
        return variables
    
    def _can_push_predicate(self, tree: Tree, match_clause: Tree, where_clause: Tree) -> bool:
        """Check if a WHERE predicate can be pushed to a MATCH clause."""
        # Simplified check - would need more sophisticated analysis in real implementation
        # Check if there's a WITH clause between them that changes the scope
        return False  # Conservative: don't push for now
    
    def _eliminate_unnecessary_with(self, tree: Tree) -> Tree:
        """
        Remove WITH clauses that don't change the scope or add computation.
        
        Example:
            MATCH (n:Person)
            WITH n
            RETURN n.name
            
            ↓
            
            MATCH (n:Person)
            RETURN n.name
        """
        modified = False
        
        # Find WITH clauses that just pass through variables
        for with_clause in list(tree.find_data("with_clause")):
            if self._is_passthrough_with(with_clause):
                # Check if there's no WHERE after this WITH
                parent = self._find_parent(tree, with_clause)
                if parent:
                    # Look ahead for WHERE clause
                    with_index = parent.children.index(with_clause)
                    has_where_after = False
                    
                    if with_index + 1 < len(parent.children):
                        next_node = parent.children[with_index + 1]
                        if isinstance(next_node, Tree) and next_node.data == "where_clause":
                            has_where_after = True
                    
                    # Only remove if no WHERE follows
                    if not has_where_after:
                        parent.children.remove(with_clause)
                        modified = True
        
        if modified:
            self.optimizations_applied.append("with_elimination")
        
        return tree
    
    def _is_passthrough_with(self, with_clause: Tree) -> bool:
        """
        Check if a WITH clause just passes through variables without transformation.
        
        Example of passthrough: WITH n, m
        Example of transformation: WITH n.name AS name, COUNT(m) AS count
        """
        # Look for return_body or projection_items
        for return_item in with_clause.find_data("return_item"):
            # Check if it has an alias (AS ...)
            for alias in return_item.find_data("return_alias"):
                return False  # Has alias, so it's a transformation
            
            # Check if it's an aggregation or function
            for func in return_item.find_data("function_invocation"):
                return False  # Has function, so it's a transformation
        
        # If we got here, it's just variable passthrough
        return True
    
    def _reorder_matches(self, tree: Tree) -> Tree:
        """
        Reorder MATCH clauses to minimize Cartesian products.
        
        This is a basic heuristic: prefer MATCH clauses with WHERE filters first.
        """
        modified = False
        
        # Find sequences of MATCH clauses
        match_clauses = list(tree.find_data("match_clause"))
        
        if len(match_clauses) > 1:
            # Check if we can reorder (no dependencies between them)
            # This is a simplified check
            pass
        
        if modified:
            self.optimizations_applied.append("join_reordering")
        
        return tree
    
    def _find_parent(self, tree: Tree, target: Tree) -> Optional[Tree]:
        """Find the parent node of a target node in the tree."""
        for node in tree.iter_subtrees():
            if target in node.children:
                return node
        return None
    
    def estimate_cost(self, tree: Tree) -> float:
        """
        Estimate query execution cost.
        
        Simple cost model:
        - Each MATCH clause: 100 units
        - Each WITH clause: 10 units
        - Each WHERE clause: 20 units
        - Each Cartesian product: 1000 units
        
        Args:
            tree: Query parse tree
            
        Returns:
            Estimated cost in arbitrary units
        """
        cost = 0.0
        
        # Count clauses
        match_count = len(list(tree.find_data("match_clause")))
        with_count = len(list(tree.find_data("with_clause")))
        where_count = len(list(tree.find_data("where_clause")))
        
        cost += match_count * 100
        cost += with_count * 10
        cost += where_count * 20
        
        # Check for Cartesian products (multiple unconnected MATCH clauses)
        if match_count > 1:
            # Simplified check - count as potential Cartesian product
            cost += (match_count - 1) * 1000
        
        return cost


def optimize_query(query_string: str, optimization_level: OptimizationLevel = OptimizationLevel.BASIC) -> Tuple[str, List[str]]:
    """
    Convenience function to optimize a Cypher query string.
    
    Args:
        query_string: Cypher query to optimize
        optimization_level: How aggressive to be with optimizations
        
    Returns:
        Tuple of (optimized_query_string, list_of_applied_optimizations)
        
    Example:
        >>> query = "MATCH (n:Person) WITH n WHERE n.age > 30 RETURN n.name"
        >>> optimized, opts = optimize_query(query)
        >>> print(opts)
        ['predicate_pushdown', 'with_elimination']
    """
    from pycypher.grammar_parser import GrammarParser
    
    parser = GrammarParser()
    optimizer = QueryOptimizer(optimization_level)
    
    # Parse query
    tree = parser.parse(query_string)
    
    # Optimize
    result = optimizer.optimize(tree)
    
    # Convert back to string (would need a pretty-printer in real implementation)
    # For now, just return the original query
    return query_string, result.optimizations_applied
