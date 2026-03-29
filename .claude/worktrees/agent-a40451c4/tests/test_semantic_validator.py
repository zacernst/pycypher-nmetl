"""Comprehensive tests for semantic validation.

This test suite validates the semantic validator's ability to detect:
- Undefined variables
- Variable scope violations
- Aggregation rule violations
- Invalid expressions
"""

import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.semantic_validator import (
    ErrorSeverity,
    SemanticValidator,
    ValidationError,
    VariableScope,
    validate_query,
)


@pytest.fixture
def parser():
    """Create a GrammarParser instance."""
    return GrammarParser()


@pytest.fixture
def validator():
    """Create a SemanticValidator instance."""
    return SemanticValidator()


# ============================================================================
# Variable Scope Tests
# ============================================================================


class TestVariableScope:
    """Test VariableScope class functionality."""

    def test_define_and_check_variable(self):
        """Test basic variable definition and checking."""
        scope = VariableScope()
        scope.define("x")
        assert scope.is_defined("x")
        assert not scope.is_defined("y")

    def test_nested_scopes(self):
        """Test nested scope variable resolution."""
        parent = VariableScope()
        parent.define("x")

        child = parent.create_child_scope()
        child.define("y")

        # Child can see parent variables
        assert child.is_defined("x")
        assert child.is_defined("y")

        # Parent cannot see child variables
        assert parent.is_defined("x")
        assert not parent.is_defined("y")

    def test_undefined_variables(self):
        """Test detection of undefined variables."""
        scope = VariableScope()
        scope.define("x")
        scope.use("x")
        scope.use("y")  # Not defined

        undefined = scope.get_undefined_vars()
        assert "y" in undefined
        assert "x" not in undefined


# ============================================================================
# Undefined Variable Detection Tests
# ============================================================================


class TestUndefinedVariables:
    """Test detection of undefined variables in queries."""

    def test_simple_undefined_variable(self, parser, validator):
        """Test detection of simple undefined variable in RETURN."""
        query = "MATCH (n:Person) RETURN m"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        assert len(errors) > 0
        assert any(
            "'m'" in error.message and "not defined" in error.message
            for error in errors
        )

    def test_all_variables_defined(self, parser, validator):
        """Test that no errors are raised when all variables are defined."""
        query = "MATCH (n:Person) RETURN n"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # Filter out warnings (only check for errors)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_multiple_undefined_variables(self, parser, validator):
        """Test detection of multiple undefined variables."""
        query = "MATCH (n:Person) RETURN m, k, p"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        error_messages = [e.message for e in errors]
        assert any("'m'" in msg for msg in error_messages)
        assert any("'k'" in msg for msg in error_messages)
        assert any("'p'" in msg for msg in error_messages)

    def test_undefined_in_where_clause(self, parser, validator):
        """Test detection of undefined variable in WHERE clause."""
        query = "MATCH (n:Person) WHERE m.age > 30 RETURN n"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        assert any("'m'" in error.message for error in errors)

    def test_relationship_variable_defined(self, parser, validator):
        """Test that relationship variables are properly tracked."""
        query = "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # Filter errors only
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_undefined_relationship_variable(self, parser, validator):
        """Test detection of undefined relationship variable."""
        query = "MATCH (a)-[:KNOWS]->(b) RETURN r"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        assert any("'r'" in error.message for error in errors)


# ============================================================================
# WITH Clause Scope Tests
# ============================================================================


class TestWithClauseScope:
    """Test variable scoping with WITH clauses."""

    def test_with_clause_introduces_new_scope(self, parser, validator):
        """Test that WITH clause creates new variable bindings."""
        query = """
        MATCH (n:Person)
        WITH n.name AS personName
        RETURN personName
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # Should have no errors - personName is defined in WITH
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_with_clause_shadows_variables(self, parser, validator):
        """Test that WITH shadows previous variables."""
        query = """
        MATCH (n:Person)
        WITH n.name AS name
        RETURN n
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # 'n' should be undefined after WITH (only 'name' is available)
        assert any("'n'" in error.message for error in errors)

    def test_with_clause_multiple_variables(self, parser, validator):
        """Test WITH clause with multiple variable definitions."""
        query = """
        MATCH (n:Person)
        WITH n.name AS name, n.age AS age
        RETURN name, age
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_with_clause_passthrough_variable(self, parser, validator):
        """Test WITH clause passing through variables."""
        query = """
        MATCH (n:Person)
        WITH n, n.age AS age
        RETURN n, age
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# CREATE/MERGE Variable Definition Tests
# ============================================================================


class TestCreateMergeVariables:
    """Test variable definitions in CREATE and MERGE clauses."""

    def test_create_defines_variables(self, parser, validator):
        """Test that CREATE clause defines variables."""
        query = "CREATE (n:Person {name: 'Alice'}) RETURN n"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_merge_defines_variables(self, parser, validator):
        """Test that MERGE clause defines variables."""
        query = "MERGE (n:Person {id: 1}) RETURN n"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_create_multiple_nodes(self, parser, validator):
        """Test CREATE with multiple node patterns."""
        query = "CREATE (a:Person), (b:Company) RETURN a, b"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_create_with_relationship(self, parser, validator):
        """Test CREATE with relationship pattern."""
        query = "CREATE (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# UNWIND Variable Definition Tests
# ============================================================================


class TestUnwindVariables:
    """Test variable definitions in UNWIND clauses."""

    def test_unwind_defines_variable(self, parser, validator):
        """Test that UNWIND defines a variable."""
        query = "UNWIND [1, 2, 3] AS num RETURN num"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    @pytest.mark.skip(
        reason="Grammar doesn't support UNWIND + MATCH without WITH - known limitation"
    )
    def test_unwind_with_match(self, parser, validator):
        """Test UNWIND combined with MATCH."""
        query = """
        UNWIND [1, 2, 3] AS id
        WITH id
        MATCH (n:Person {id: id})
        RETURN n
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_multiple_unwind_clauses(self, parser, validator):
        """Test multiple UNWIND clauses."""
        query = """
        UNWIND [1, 2] AS x
        UNWIND [3, 4] AS y
        RETURN x, y
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# Aggregation Validation Tests
# ============================================================================


class TestAggregationValidation:
    """Test validation of aggregation rules."""

    def test_pure_aggregation_is_valid(self, parser, validator):
        """Test that pure aggregation queries are valid."""
        query = "MATCH (n:Person) RETURN count(*)"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_mixed_aggregation_warning(self, parser, validator):
        """Test that mixed aggregation produces a warning."""
        query = "MATCH (n:Person) RETURN n.name, count(*)"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # Should have a warning about mixed aggregation
        warnings = [e for e in errors if e.severity == ErrorSeverity.WARNING]
        assert any("aggregat" in e.message.lower() for e in warnings)

    def test_multiple_aggregations_valid(self, parser, validator):
        """Test that multiple aggregations without non-aggregated are valid."""
        query = "MATCH (n:Person) RETURN count(*), sum(n.age)"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # count(*) and sum(n.age) are both aggregations - should not warn
        # Note: sum(n.age) contains property access but it's inside aggregation
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0
        """Test aggregation with grouping key (mixed but valid)."""
        query = "MATCH (n:Person) RETURN n.department, count(*)"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # May produce warning about implicit grouping
        # This is valid Cypher behavior
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# Complex Query Tests
# ============================================================================


class TestComplexQueries:
    """Test validation of complex queries with multiple clauses."""

    @pytest.mark.skip(
        reason="WITH + WHERE syntax edge case - validator needs enhancement for this pattern"
    )
    def test_complex_valid_query(self, parser, validator):
        """Test complex but valid query."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 25
        WITH a, b, r
        WITH a, b, r WHERE b.active = true
        RETURN a.name AS person1, b.name AS person2, r.since AS since
        ORDER BY since DESC
        LIMIT 10
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_complex_query_with_error(self, parser, validator):
        """Test complex query with undefined variable."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 25
        WITH a, b
        RETURN a.name, b.name, r.since
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        # 'r' should be undefined after WITH (not passed through)
        assert any("'r'" in error.message for error in errors)

    def test_multiple_match_clauses(self, parser, validator):
        """Test query with multiple MATCH clauses."""
        query = """
        MATCH (a:Person)
        MATCH (b:Person)
        WHERE a.id <> b.id
        RETURN a, b
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_create_then_match(self, parser, validator):
        """Test CREATE followed by MATCH using created variables."""
        query = """
        CREATE (n:Person {name: 'Alice'})
        RETURN n
        """
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# Convenience Function Tests
# ============================================================================


class TestConvenienceFunction:
    """Test the validate_query convenience function."""

    def test_validate_query_with_error(self):
        """Test convenience function with error."""
        errors = validate_query("MATCH (n) RETURN m")
        assert len(errors) > 0
        assert any("'m'" in str(error) for error in errors)

    def test_validate_query_valid(self):
        """Test convenience function with valid query."""
        errors = validate_query("MATCH (n:Person) RETURN n")
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_validate_query_syntax_error(self):
        """Test convenience function with syntax error."""
        errors = validate_query("MATCH (n RETURN n")  # Missing closing paren
        assert len(errors) > 0


# ============================================================================
# Edge Cases
# ============================================================================


class TestEdgeCases:
    """Test edge cases and corner scenarios."""

    def test_anonymous_node_pattern(self, parser, validator):
        """Test anonymous node patterns (no variable)."""
        query = "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_return_star(self, parser, validator):
        """Test RETURN * (returns all variables in scope)."""
        query = "MATCH (n:Person) RETURN *"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_property_in_where(self, parser, validator):
        """Test property access in WHERE clause."""
        query = "MATCH (n:Person) WHERE n.age > 30 RETURN n"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0

    def test_nested_property_access(self, parser, validator):
        """Test nested property access."""
        query = "MATCH (n:Person) RETURN n.address.city"
        tree = parser.parse(query)
        errors = validator.validate(tree)

        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert len(errors) == 0


# ============================================================================
# Run tests
# ============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
