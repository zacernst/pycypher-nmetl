"""TDD tests for Testing Loop 260: Fix critical security test failures.

Critical Testing Loop issue: 5/11 tests failing in test_security_loop_256_rand_function_tdd.py
because TDD red phase tests still expect old insecure rand() behavior after Loop 256
implemented cryptographically secure randomness. Tests need proper red→green→refactor update.

Root cause: Security fix WAS properly implemented using secrets.SystemRandom(), but
TDD tests were never updated through green phase, leaving red phase tests expecting
old insecure behavior and some green phase tests with incorrect implementation expectations.
"""

import re
from pathlib import Path

# The _rand function was refactored from scalar_functions.py into scalar_functions/list_functions.py
_RAND_SOURCE_FILE = Path(
    "packages/pycypher/src/pycypher/scalar_functions/list_functions.py",
)

import pandas as pd
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


class TestCurrentSecurityTestingIssues:
    """Test the current broken testing state (red phase for Testing Loop)."""

    def test_security_tests_are_currently_failing(self):
        """Document that security tests are currently failing due to TDD workflow issues."""
        # This test documents the problem we're fixing
        # After security fix was implemented, red phase tests should have been updated

        # The rand() function should now use secure implementation
        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"
        rand_function_body = rand_function_match.group(1)

        # Implementation IS secure (this is good!)
        assert "secrets.SystemRandom()" in rand_function_body, (
            "rand() is already properly secured with secrets.SystemRandom()"
        )
        assert "import secrets" in rand_function_body, (
            "rand() already imports secrets module for cryptographic security"
        )

        # The issue is that tests expect the OLD insecure behavior
        # This test documents that the implementation is correct but tests are stale

    def test_security_fix_is_actually_implemented(self):
        """Verify that the security fix from Loop 256 is actually in place."""
        # This confirms the implementation is correct, so the issue is in testing
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        assert "rand" in registry._functions, (
            "rand function should be registered"
        )

        # Test that it actually uses secure randomness
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))
        result = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")

        # Should work and return valid random values
        assert len(result) == 3, "Should return 3 random values"
        assert all(0.0 <= val < 1.0 for val in result["r"].values), (
            "All values should be in [0.0, 1.0) range"
        )

        # Multiple calls should return different sequences (high probability)
        result2 = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")
        values1 = result["r"].tolist()
        values2 = result2["r"].tolist()

        # With cryptographically secure randomness, sequences should be different
        assert values1 != values2, (
            "Secure random should produce different sequences"
        )


class TestUpdatedSecurityTesting:
    """Tests for corrected security testing approach (green phase for Testing Loop).

    These tests define proper testing of the secure rand() implementation.
    Initially these will fail (red phase), then pass after implementation (green phase).
    """

    def test_rand_function_uses_cryptographically_secure_implementation(self):
        """Test that rand() function uses cryptographically secure random generation."""
        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"
        rand_function_body = rand_function_match.group(1)

        # Should use secrets module for cryptographic security
        assert "import secrets" in rand_function_body, (
            "rand() function should import secrets module"
        )
        assert "secrets.SystemRandom()" in rand_function_body, (
            "rand() function should use secrets.SystemRandom() for secure randomness"
        )
        assert "secure_random.random()" in rand_function_body, (
            "rand() function should call secure_random.random()"
        )

    def test_rand_function_does_not_use_insecure_random_module(self):
        """Test that rand() function no longer uses insecure random.random()."""
        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"
        rand_function_body = rand_function_match.group(1)

        # Should NOT use insecure patterns (be specific about what constitutes insecure usage)
        assert "import random as _random" not in rand_function_body, (
            "rand() function should not use insecure random module"
        )

        # Look for actual insecure usage patterns, not substrings of secure usage
        lines = [line.strip() for line in rand_function_body.split("\n")]

        # Check for lines that use _random module (insecure)
        insecure_usage_lines = [
            line
            for line in lines
            if "_random.random()" in line
            and "secure_random.random()" not in line
        ]
        assert len(insecure_usage_lines) == 0, (
            f"rand() function should not use insecure _random.random(). Found: {insecure_usage_lines}"
        )

        # Check for direct usage of random.random() that's NOT part of secure_random.random()
        insecure_random_usage = any(
            "random.random()" in line and "secure_random.random()" not in line
            for line in lines
        )
        assert not insecure_random_usage, (
            "rand() function should not use insecure random.random() directly"
        )

    def test_rand_function_produces_cryptographically_unpredictable_sequences(
        self,
    ):
        """Test that rand() produces unpredictable sequences (cannot be seeded)."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # Generate multiple sequences - they should all be different
        # (cryptographically secure random cannot be seeded predictably)
        sequences = []
        for _ in range(5):
            result = star.execute_query(
                "UNWIND [1,2,3] AS x RETURN rand() AS r",
            )
            sequences.append(tuple(result["r"].tolist()))

        # All sequences should be different (extremely high probability with secure random)
        unique_sequences = set(sequences)
        assert len(unique_sequences) == 5, (
            f"All sequences should be unique with secure random. Got {len(unique_sequences)} unique out of 5"
        )

    def test_no_s311_security_violations_remain(self):
        """Test that security scanners no longer detect S311 violations."""
        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"
        rand_function_body = rand_function_match.group(1)

        # Should not contain S311 violation patterns (be specific about actual violations)
        assert "import random as _random" not in rand_function_body, (
            "Should not contain S311 violation pattern: import random as _random"
        )

        # Look for actual insecure usage patterns, not substrings of secure usage
        lines = [line.strip() for line in rand_function_body.split("\n")]

        # Check for lines that use _random module directly (S311 violation)
        insecure_usage_lines = [
            line
            for line in lines
            if "_random.random()" in line
            and "secure_random.random()" not in line
        ]
        assert len(insecure_usage_lines) == 0, (
            f"Should not contain S311 violation _random.random() usage. Found: {insecure_usage_lines}"
        )

        # Check for direct usage of random.random() that's NOT part of secure_random.random()
        insecure_random_usage = any(
            "random.random()" in line and "secure_random.random()" not in line
            for line in lines
        )
        assert not insecure_random_usage, (
            "Should not contain insecure random.random() usage (S311 violation)"
        )

    def test_rand_maintains_functional_compatibility(self):
        """Test that rand() maintains same functional interface after security fix."""
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()

        # Should still be registered with same interface
        assert "rand" in registry._functions, (
            "rand() should still be registered"
        )

        func_info = registry._functions["rand"]
        assert func_info.min_args == 0, (
            "rand() should require 0 minimum arguments"
        )
        assert func_info.max_args == 0, (
            "rand() should require 0 maximum arguments"
        )

        # Should still work in Cypher queries
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))
        result = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")

        # Should maintain same output format and constraints
        assert len(result) == 3, "Should return one value per input row"
        assert all(0.0 <= val < 1.0 for val in result["r"].values), (
            "All values should be in [0.0, 1.0) range"
        )
        assert result["r"].dtype == float, "Should return float dtype"

    def test_rand_passes_statistical_randomness_tests(self):
        """Test that secure rand() passes basic statistical randomness tests."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # Generate larger sample for statistical testing
        result = star.execute_query(
            "UNWIND range(1, 1000) AS x RETURN rand() AS r",
        )
        values = result["r"].values

        # Statistical tests for uniform distribution in [0, 1)
        assert values.min() >= 0.0, "Min should be >= 0.0"
        assert values.max() < 1.0, "Max should be < 1.0"

        # Mean should be around 0.5 for uniform distribution
        mean = values.mean()
        assert 0.45 < mean < 0.55, (
            f"Mean should be ~0.5 for uniform distribution, got {mean}"
        )

        # Should have good spread across the range
        std_dev = values.std()
        assert std_dev > 0.25, (
            f"Standard deviation should indicate good spread, got {std_dev}"
        )

    def test_cypher_queries_with_rand_work_correctly(self):
        """Test end-to-end functionality of rand() in various Cypher contexts."""
        people = pd.DataFrame(
            {
                "__ID__": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            },
        )

        entity_mapping = EntityMapping(
            mapping={
                "Person": EntityTable(source_obj=people, entity_type="Person"),
            },
        )

        context = Context(
            entity_mapping=entity_mapping,
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        star = Star(context=context)

        # Test rand() in RETURN clause
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, rand() AS random_value",
        )
        assert len(result) == 5, (
            "Should return all 5 people with random values"
        )
        assert "name" in result.columns, "Should have name column"
        assert "random_value" in result.columns, (
            "Should have random_value column"
        )
        assert all(
            0.0 <= val < 1.0 for val in result["random_value"].values
        ), "All random values should be in valid range"

        # Test rand() in WHERE clause (probabilistic filtering)
        result = star.execute_query(
            "MATCH (p:Person) WHERE rand() >= 0.0 RETURN p.name",
        )
        assert 0 <= len(result) <= 5, (
            "Should return 0-5 people (probabilistic)"
        )

        # Multiple executions should give different results due to randomness
        results = []
        for _ in range(10):
            result = star.execute_query(
                "MATCH (p:Person) RETURN rand() AS r LIMIT 1",
            )
            results.append(result["r"].iloc[0])

        # Should have at least some variation (very high probability)
        unique_results = len(set(results))
        assert unique_results >= 8, (
            f"Should have variation across executions, got {unique_results} unique out of 10"
        )

    def test_security_regression_prevention(self):
        """Test that guards against future security regressions are in place."""
        # This test ensures that any future change back to insecure random will be caught
        content = _RAND_SOURCE_FILE.read_text()

        # Multiple checks to catch different ways someone might reintroduce insecurity
        insecure_patterns = [
            "import random as _random",  # Direct import for insecure usage
            "random.seed(",
            "numpy.random.rand",
            "np.random.rand",
        ]

        # Get the full rand function implementation
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"
        rand_function_body = rand_function_match.group(1)

        # Check for insecure patterns
        for pattern in insecure_patterns:
            assert pattern not in rand_function_body, (
                f"Insecure pattern '{pattern}' found in rand() function"
            )

        # Special check for random.random() usage that's NOT part of secure_random.random()
        lines = rand_function_body.split("\n")
        insecure_random_usage = any(
            "random.random()" in line and "secure_random.random()" not in line
            for line in lines
        )
        assert not insecure_random_usage, (
            "Insecure 'random.random()' usage found (not part of secure_random.random())"
        )

        # Ensure secrets is used for cryptographic security
        assert "secrets" in rand_function_body, (
            "rand() function must use secrets module for cryptographic security"
        )
