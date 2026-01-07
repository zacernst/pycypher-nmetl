"""Tests for AST validation functionality."""

import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter, ValidationSeverity


@pytest.fixture
def parser():
    """Create a grammar parser."""
    return GrammarParser()


@pytest.fixture
def converter():
    """Create an AST converter."""
    return ASTConverter()


def parse_and_validate(parser, converter, query):
    """Helper to parse a query and return validation results."""
    tree = parser.parse(query)
    ast_dict = parser.transformer.transform(tree)
    typed_ast = converter.convert(ast_dict)
    return typed_ast.validate()


class TestUndefinedVariables:
    """Test detection of undefined variable references."""
    
    def test_undefined_variable_in_return(self, parser, converter):
        """Test error when returning undefined variable."""
        query = "MATCH (n) RETURN m"
        result = parse_and_validate(parser, converter, query)
        
        assert not result.is_valid
        assert result.has_errors
        assert len(result.issues) == 1
        assert result.issues[0].severity == ValidationSeverity.ERROR
        assert "m" in result.issues[0].message
        assert "never defined" in result.issues[0].message
    
    def test_undefined_variable_in_where(self, parser, converter):
        """Test error when using undefined variable in WHERE."""
        query = "MATCH (n) WHERE m.age > 30 RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        assert not result.is_valid
        assert "m" in str(result)
    
    def test_valid_variable_usage(self, parser, converter):
        """Test that valid variable usage passes validation."""
        query = "MATCH (n:Person) RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        # May have warnings but shouldn't have errors
        assert result.is_valid


class TestUnusedVariables:
    """Test detection of unused variables."""
    
    def test_unused_node_variable(self, parser, converter):
        """Test warning for unused node variable."""
        query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        assert result.is_valid  # No errors
        assert result.has_warnings
        
        # Both 'r' and 'm' are unused
        messages = [issue.message for issue in result.issues]
        assert any("m" in msg and "never used" in msg for msg in messages)
        assert any("r" in msg and "never used" in msg for msg in messages)
    
    def test_all_variables_used(self, parser, converter):
        """Test that using all variables doesn't trigger warning."""
        query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, r, m"
        result = parse_and_validate(parser, converter, query)
        
        # Should not have unused variable warnings
        unused_warnings = [
            issue for issue in result.issues 
            if "never used" in issue.message
        ]
        assert len(unused_warnings) == 0


class TestMissingLabels:
    """Test detection of patterns without labels."""
    
    def test_missing_label_warning(self, parser, converter):
        """Test warning for node pattern without label."""
        query = "MATCH (n) RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        assert result.is_valid  # Not an error, just a warning
        assert result.has_warnings
        
        missing_label_warnings = [
            issue for issue in result.issues
            if "no labels" in issue.message
        ]
        assert len(missing_label_warnings) == 1
        assert "index" in missing_label_warnings[0].suggestion.lower()
    
    def test_with_label_no_warning(self, parser, converter):
        """Test that patterns with labels don't trigger warning."""
        query = "MATCH (n:Person) RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        missing_label_warnings = [
            issue for issue in result.issues
            if "no labels" in issue.message
        ]
        assert len(missing_label_warnings) == 0
    
    def test_with_properties_no_warning(self, parser, converter):
        """Test that patterns with properties don't trigger warning."""
        query = "MATCH (n {name: 'Alice'}) RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        missing_label_warnings = [
            issue for issue in result.issues
            if "no labels" in issue.message
        ]
        assert len(missing_label_warnings) == 0


class TestUnreachableConditions:
    """Test detection of unreachable WHERE conditions."""
    
    def test_where_false(self, parser, converter):
        """Test warning for WHERE false."""
        query = "MATCH (n:Person) WHERE false RETURN n"
        result = parse_and_validate(parser, converter, query)
        
        assert result.is_valid  # Not an error, just a warning
        assert result.has_warnings
        
        unreachable_warnings = [
            issue for issue in result.issues
            if "always false" in issue.message
        ]
        assert len(unreachable_warnings) == 1


class TestDeleteWithoutDetach:
    """Test detection of potentially failing DELETE operations."""
    
    def test_delete_node_warning(self, parser, converter):
        """Test warning for DELETE without DETACH."""
        query = "MATCH (n:Person) DELETE n"
        result = parse_and_validate(parser, converter, query)
        
        assert result.is_valid
        assert result.has_warnings
        
        delete_warnings = [
            issue for issue in result.issues
            if "without DETACH" in issue.message
        ]
        assert len(delete_warnings) == 1
        assert "DETACH DELETE" in delete_warnings[0].suggestion


class TestExpensivePatterns:
    """Test detection of potentially expensive query patterns."""
    
    def test_cartesian_product_warning(self, parser, converter):
        """Test warning for unconnected MATCH clauses."""
        query = "MATCH (n:Person) MATCH (m:Company) RETURN n, m"
        result = parse_and_validate(parser, converter, query)
        
        cartesian_warnings = [
            issue for issue in result.issues
            if "Cartesian product" in issue.message
        ]
        assert len(cartesian_warnings) == 1
        assert "shared variables" in cartesian_warnings[0].suggestion
    
    def test_connected_matches_no_warning(self, parser, converter):
        """Test that connected MATCH clauses don't trigger warning."""
        query = "MATCH (n:Person) MATCH (n)-[r:KNOWS]->(m) RETURN n, m"
        result = parse_and_validate(parser, converter, query)
        
        cartesian_warnings = [
            issue for issue in result.issues
            if "Cartesian product" in issue.message
        ]
        assert len(cartesian_warnings) == 0


class TestComplexValidation:
    """Test validation of complex queries with multiple issues."""
    
    def test_multiple_issues(self, parser, converter):
        """Test query with multiple validation issues."""
        # Missing label + unused variable + undefined in RETURN
        query = "MATCH (n) MATCH (m:Person) RETURN x"
        result = parse_and_validate(parser, converter, query)
        
        assert not result.is_valid  # Has errors (undefined x)
        assert result.has_errors
        assert result.has_warnings
        assert len(result.issues) >= 3  # At least 3 issues
        
        # Check we have different types of issues
        severities = {issue.severity for issue in result.issues}
        assert ValidationSeverity.ERROR in severities
        assert ValidationSeverity.WARNING in severities
    
    def test_validation_result_string(self, parser, converter):
        """Test ValidationResult string representation."""
        query = "MATCH (n) RETURN m"
        result = parse_and_validate(parser, converter, query)
        
        result_str = str(result)
        assert "issue" in result_str.lower()
        assert "m" in result_str
    
    def test_clean_query(self, parser, converter):
        """Test that a well-written query has no issues."""
        query = "MATCH (n:Person)-[r:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n, m"
        result = parse_and_validate(parser, converter, query)
        
        assert result.is_valid
        # Might have an info about unused 'r', but that's ok
        error_and_warning_count = sum(
            1 for issue in result.issues 
            if issue.severity in (ValidationSeverity.ERROR, ValidationSeverity.WARNING)
        )
        # Should have minimal warnings (maybe unused r)
        assert error_and_warning_count <= 1


class TestValidationAPI:
    """Test the validation API and result handling."""
    
    def test_has_errors_property(self, parser, converter):
        """Test has_errors property."""
        query_with_error = "MATCH (n) RETURN undefined_var"
        result = parse_and_validate(parser, converter, query_with_error)
        assert result.has_errors
        
        query_without_error = "MATCH (n) RETURN n"
        result = parse_and_validate(parser, converter, query_without_error)
        assert not result.has_errors
    
    def test_is_valid_property(self, parser, converter):
        """Test is_valid property."""
        query = "MATCH (n:Person) RETURN n"
        result = parse_and_validate(parser, converter, query)
        assert result.is_valid
    
    def test_issue_string_format(self, parser, converter):
        """Test that issues format nicely."""
        query = "MATCH (n) RETURN undefined_var"
        result = parse_and_validate(parser, converter, query)
        
        for issue in result.issues:
            issue_str = str(issue)
            # Should contain severity
            assert "[ERROR]" in issue_str or "[WARNING]" in issue_str or "[INFO]" in issue_str
            # Should have a suggestion
            if issue.suggestion:
                assert "💡" in issue_str


class TestEdgeCases:
    """Test edge cases in validation."""
    
    def test_empty_query_components(self, parser, converter):
        """Test validation doesn't crash on edge cases."""
        query = "MATCH (n:Person) RETURN n"
        result = parse_and_validate(parser, converter, query)
        # Should complete without crashing
        assert isinstance(result.issues, list)
    
    def test_unwind_defines_variable(self, parser, converter):
        """Test that UNWIND defines a variable."""
        query = "UNWIND [1, 2, 3] AS x RETURN x"
        result = parse_and_validate(parser, converter, query)
        
        # x should not be undefined
        undefined_errors = [
            issue for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR and "x" in issue.message
        ]
        assert len(undefined_errors) == 0
    
    def test_with_clause_defines_variable(self, parser, converter):
        """Test that WITH defines variables."""
        query = "MATCH (n:Person) WITH n.name AS name RETURN name"
        result = parse_and_validate(parser, converter, query)
        
        # name should not be undefined
        undefined_errors = [
            issue for issue in result.issues
            if issue.severity == ValidationSeverity.ERROR and "name" in issue.message
        ]
        assert len(undefined_errors) == 0
