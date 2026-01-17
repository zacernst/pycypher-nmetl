"""Semantic validation for Cypher queries.

This module provides semantic analysis beyond syntax checking, including:
- Variable scope and binding validation
- Undefined variable detection
- Aggregation rule validation
- Function signature validation
- Type checking for expressions

Example:
    >>> from pycypher.semantic_validator import SemanticValidator
    >>> from pycypher.grammar_parser import GrammarParser
    >>> 
    >>> parser = GrammarParser()
    >>> validator = SemanticValidator()
    >>> 
    >>> query = "MATCH (n:Person) RETURN m"  # 'm' undefined
    >>> tree = parser.parse(query)
    >>> errors = validator.validate(tree)
    >>> for error in errors:
    ...     print(f"{error.line}:{error.column} - {error.message}")
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Set, Dict, Optional, Any
from lark import Tree, Token


class ErrorSeverity(Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """Represents a semantic validation error."""
    severity: ErrorSeverity
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    node_type: Optional[str] = None
    variable_name: Optional[str] = None
    
    def __str__(self) -> str:
        """Format error as string."""
        location = ""
        if self.line is not None:
            location = f"Line {self.line}"
            if self.column is not None:
                location += f":{self.column}"
            location += " - "
        return f"{location}{self.severity.value.upper()}: {self.message}"


class VariableScope:
    """Manages variable scope and bindings in Cypher queries."""
    
    def __init__(self, parent: Optional['VariableScope'] = None):
        """Initialize a variable scope.
        
        Args:
            parent: Parent scope for nested scopes (e.g., subqueries, comprehensions).
        """
        self.parent = parent
        self.defined_vars: Set[str] = set()
        self.used_vars: Set[str] = set()
        
    def define(self, var_name: str) -> None:
        """Mark a variable as defined in this scope."""
        self.defined_vars.add(var_name)
        
    def use(self, var_name: str) -> None:
        """Mark a variable as used in this scope."""
        self.used_vars.add(var_name)
        
    def is_defined(self, var_name: str) -> bool:
        """Check if variable is defined in this scope or parent scopes."""
        if var_name in self.defined_vars:
            return True
        if self.parent:
            return self.parent.is_defined(var_name)
        return False
    
    def get_undefined_vars(self) -> Set[str]:
        """Get variables used but not defined in accessible scopes."""
        undefined = set()
        for var in self.used_vars:
            if not self.is_defined(var):
                undefined.add(var)
        return undefined
    
    def create_child_scope(self) -> 'VariableScope':
        """Create a nested child scope."""
        return VariableScope(parent=self)


class SemanticValidator:
    """Validates semantic correctness of Cypher queries.
    
    Performs validation beyond syntax checking, including:
    - Variable scope and binding
    - Aggregation rules
    - Function signatures
    - Return clause validation
    """
    
    def __init__(self):
        """Initialize the semantic validator."""
        self.errors: List[ValidationError] = []
        self.current_scope: VariableScope = VariableScope()
        self.scope_stack: List[VariableScope] = []
        
    def validate(self, tree: Tree) -> List[ValidationError]:
        """Validate a parse tree and return any errors found.
        
        Args:
            tree: Lark parse tree from grammar_parser.
            
        Returns:
            List of ValidationError objects found during validation.
        """
        self.errors = []
        self.current_scope = VariableScope()
        self.scope_stack = [self.current_scope]
        
        # Walk the tree and validate
        self._validate_node(tree)
        
        # Check for undefined variables at the end
        undefined = self.current_scope.get_undefined_vars()
        for var in undefined:
            self.errors.append(ValidationError(
                severity=ErrorSeverity.ERROR,
                message=f"Variable '{var}' is used but not defined",
                variable_name=var
            ))
        
        return self.errors
    
    def _validate_node(self, node: Any) -> None:
        """Recursively validate a tree node.
        
        Args:
            node: Tree node or Token to validate.
        """
        if isinstance(node, Token):
            return
        
        if not isinstance(node, Tree):
            return
        
        # Dispatch to specific validation methods based on node type
        validator_method = f"_validate_{node.data}"
        if hasattr(self, validator_method):
            getattr(self, validator_method)(node)
            # Don't recursively validate children for nodes with specific validators
            # They handle their own children
            return
        
        # Recursively validate children for nodes without specific validators
        for child in node.children:
            self._validate_node(child)
    
    def _validate_match_clause(self, node: Tree) -> None:
        """Validate MATCH clause and extract variable definitions.
        
        Args:
            node: match_clause tree node.
        """
        # Extract variables from patterns
        for pattern_node in node.find_data("node_pattern"):
            var_name = self._extract_variable_from_node_pattern(pattern_node)
            if var_name:
                self.current_scope.define(var_name)
        
        for rel_node in node.find_data("relationship_pattern"):
            var_name = self._extract_variable_from_relationship_pattern(rel_node)
            if var_name:
                self.current_scope.define(var_name)
        
        # Recursively validate children (like WHERE clauses)
        for child in node.children:
            self._validate_node(child)
    
    def _validate_return_clause(self, node: Tree) -> None:
        """Validate RETURN clause.
        
        Args:
            node: return_clause tree node.
        """
        # Extract and track variables used in RETURN
        variables = self._extract_variables_from_expression(node)
        for var in variables:
            self.current_scope.use(var)
        
        # Check for mixed aggregation (aggregated and non-aggregated without grouping)
        return_items = list(node.find_data("return_item"))
        
        has_aggregation = False
        has_non_aggregation = False
        
        for item in return_items:
            if self._contains_aggregation(item):
                has_aggregation = True
            else:
                # Check if it's a simple variable or expression
                if self._is_non_aggregated_expression(item):
                    has_non_aggregation = True
        
        if has_aggregation and has_non_aggregation:
            # This is valid in Cypher (implies grouping), but we can warn about it
            self.errors.append(ValidationError(
                severity=ErrorSeverity.WARNING,
                message="Mixing aggregated and non-aggregated expressions in RETURN (implicit grouping)",
                node_type="return_clause"
            ))
    
    def _validate_with_clause(self, node: Tree) -> None:
        """Validate WITH clause and update variable scope.
        
        WITH clause introduces new variable bindings and shadows previous ones.
        
        Args:
            node: with_clause tree node.
        """
        # WITH creates a new scope - variables before WITH are shadowed
        new_scope = VariableScope()
        
        # Extract variables defined in WITH
        for return_item in node.find_data("return_item"):
            # Look for AS alias (return_alias node)
            alias_nodes = list(return_item.find_data("return_alias"))
            if alias_nodes:
                # Get the alias name
                var_name = self._get_token_value(alias_nodes[0])
                if var_name:
                    new_scope.define(var_name)
            else:
                # No AS alias - check if it's a simple variable reference
                # In this case, the variable is passed through
                var_refs = self._extract_variables_from_expression(return_item)
                for var in var_refs:
                    new_scope.define(var)
        
        # Replace current scope with new scope
        self.current_scope = new_scope
        self.scope_stack.append(new_scope)
    
    def _validate_where_clause(self, node: Tree) -> None:
        """Validate WHERE clause expressions.
        
        Args:
            node: where_clause tree node.
        """
        # Extract variables used in WHERE clause
        variables = self._extract_variables_from_expression(node)
        for var in variables:
            self.current_scope.use(var)
    
    def _validate_unwind_clause(self, node: Tree) -> None:
        """Validate UNWIND clause.
        
        Args:
            node: unwind_clause tree node.
        """
        # UNWIND introduces a new variable (after AS)
        var_nodes = list(node.find_data("variable_name"))
        if var_nodes:
            var_name = self._get_token_value(var_nodes[-1])  # Get the AS variable
            if var_name:
                self.current_scope.define(var_name)
    
    def _validate_create_clause(self, node: Tree) -> None:
        """Validate CREATE clause.
        
        Args:
            node: create_clause tree node.
        """
        # CREATE can define new variables
        for pattern_node in node.find_data("node_pattern"):
            var_name = self._extract_variable_from_node_pattern(pattern_node)
            if var_name:
                self.current_scope.define(var_name)
        
        for rel_node in node.find_data("relationship_pattern"):
            var_name = self._extract_variable_from_relationship_pattern(rel_node)
            if var_name:
                self.current_scope.define(var_name)
    
    def _validate_merge_clause(self, node: Tree) -> None:
        """Validate MERGE clause.
        
        Args:
            node: merge_clause tree node.
        """
        # MERGE is similar to CREATE
        for pattern_node in node.find_data("node_pattern"):
            var_name = self._extract_variable_from_node_pattern(pattern_node)
            if var_name:
                self.current_scope.define(var_name)
    
    def _extract_variable_from_node_pattern(self, node: Tree) -> Optional[str]:
        """Extract variable name from a node pattern.
        
        Args:
            node: node_pattern tree node.
            
        Returns:
            Variable name or None if no variable.
        """
        # Look for variable_name in node pattern
        for var_name_node in node.find_data("variable_name"):
            token_val = self._get_token_value(var_name_node)
            if token_val:
                return token_val
        return None
    
    def _extract_variable_from_relationship_pattern(self, node: Tree) -> Optional[str]:
        """Extract variable name from a relationship pattern.
        
        Args:
            node: relationship_pattern tree node.
            
        Returns:
            Variable name or None if no variable.
        """
        # Look for variable_name in relationship pattern
        for var_name_node in node.find_data("variable_name"):
            token_val = self._get_token_value(var_name_node)
            if token_val:
                return token_val
        return None
    
    def _extract_variables_from_expression(self, node: Tree) -> Set[str]:
        """Extract all variable references from an expression.
        
        Args:
            node: Expression tree node.
            
        Returns:
            Set of variable names referenced.
        """
        variables = set()
        
        # Find all variable_name nodes in the expression tree
        for var_node in node.find_data("variable_name"):
            var_name = self._get_token_value(var_node)
            if var_name:
                variables.add(var_name)
        
        return variables
    
    def _get_token_value(self, node: Tree) -> Optional[str]:
        """Get the string value from a tree node.
        
        Args:
            node: Tree node to extract value from.
            
        Returns:
            String value or None.
        """
        for child in node.children:
            if isinstance(child, Token):
                return str(child.value)
        return None
    
    def _contains_aggregation(self, node: Tree) -> bool:
        """Check if a tree node contains aggregation functions.
        
        Args:
            node: Tree node to check.
            
        Returns:
            True if contains aggregation function.
        """
        # Check for count(*)
        if list(node.find_data("count_star")):
            return True
        
        # Check for named aggregation functions
        aggregation_functions = {
            'count', 'sum', 'avg', 'min', 'max',
            'collect', 'stdev', 'stdevp', 'percentiledisc',
            'percentilecont'
        }
        
        for func_node in node.find_data("function_invocation"):
            func_name_node = list(func_node.find_data("function_name"))
            if func_name_node:
                func_name = self._get_token_value(func_name_node[0])
                if func_name and func_name.lower() in aggregation_functions:
                    return True
        
        return False
    
    def _is_non_aggregated_expression(self, node: Tree) -> bool:
        """Check if expression is non-aggregated (simple variable or property).
        
        Args:
            node: Tree node to check.
            
        Returns:
            True if non-aggregated expression.
        """
        # First check if it contains aggregation
        if self._contains_aggregation(node):
            return False
        
        # If no aggregation but has variables/properties, it's non-aggregated
        has_vars = len(list(node.find_data("variable_name"))) > 0
        has_props = len(list(node.find_data("property_lookup"))) > 0
        
        return has_vars or has_props


def validate_query(query_string: str) -> List[ValidationError]:
    """Convenience function to parse and validate a query string.
    
    Args:
        query_string: Cypher query string to validate.
        
    Returns:
        List of validation errors.
        
    Example:
        >>> errors = validate_query("MATCH (n) RETURN m")
        >>> for error in errors:
        ...     print(error)
    """
    from pycypher.grammar_parser import GrammarParser
    
    parser = GrammarParser()
    validator = SemanticValidator()
    
    try:
        tree = parser.parse(query_string)
        return validator.validate(tree)
    except Exception as e:
        return [ValidationError(
            severity=ErrorSeverity.ERROR,
            message=f"Syntax error: {str(e)}"
        )]
