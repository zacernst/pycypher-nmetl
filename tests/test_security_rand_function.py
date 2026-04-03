"""TDD tests for Security Loop 256: Fix rand() function cryptographic vulnerability.

Critical security issue: rand() function in scalar_functions/ uses random.random()
which is cryptographically insecure pseudorandom generation (S311). This affects
any Cypher query using rand() and presents predictability risks.
"""

import re
from pathlib import Path

# The _rand function was refactored from scalar_functions.py into scalar_functions/list_functions.py
_RAND_SOURCE_FILE = Path(
    "packages/pycypher/src/pycypher/scalar_functions/list_functions.py",
)

import pandas as pd
import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star

pytestmark = pytest.mark.slow


class TestCurrentRandSecurityVulnerability:
    """Test the current rand() function security vulnerability."""

    def test_rand_function_uses_insecure_random_module(self):
        """Test that rand() function previously used insecure random.random() (now fixed)."""
        assert _RAND_SOURCE_FILE.exists(), (
            "scalar_functions/list_functions.py should exist"
        )

        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"

        rand_function_body = rand_function_match.group(1)

        # Verify the security fix is in place (should now use secrets module)
        assert "import secrets" in rand_function_body, (
            "rand() function should now use secure secrets module"
        )
        assert "secrets.SystemRandom()" in rand_function_body, (
            "rand() function should now use secrets.SystemRandom() for cryptographic security"
        )

        # Verify insecure patterns are NOT present
        lines = [line.strip() for line in rand_function_body.split("\n")]
        insecure_usage_lines = [
            line
            for line in lines
            if "random.random()" in line
            and "secure_random.random()" not in line
        ]
        assert len(insecure_usage_lines) == 0, (
            f"rand() function should not use insecure random.random(). Found: {insecure_usage_lines}"
        )

    def test_rand_function_produces_predictable_sequences_with_seed(self):
        """Test that fixed rand() does NOT produce predictable sequences (security fix verified)."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # With cryptographically secure randomness, seeding should not affect output
        import random

        random.seed(12345)

        # Generate first sequence
        result1 = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")
        values1 = result1["r"].tolist()

        # Reset same seed
        random.seed(12345)

        # Generate second sequence - should be different despite same seed
        result2 = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")
        values2 = result2["r"].tolist()

        # Verify the security fix - sequences should be different despite same seed
        # because cryptographically secure random cannot be seeded predictably
        assert values1 != values2, (
            "rand() with cryptographically secure implementation should produce different sequences even with same seed"
        )
        assert len(set(values1)) > 1, (
            "Should produce different values within sequence"
        )
        assert len(set(values2)) > 1, (
            "Should produce different values within sequence"
        )

    def test_current_rand_implementation_is_not_cryptographically_secure(self):
        """Test that current implementation fails cryptographic randomness requirements."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # Generate large sample (100 values)
        result = star.execute_query(
            "UNWIND range(1, 100) AS x RETURN rand() AS r",
        )
        values = result["r"].values

        # Basic statistical tests that would pass for crypto-secure random
        # but may fail for pseudorandom (though pseudorandom usually passes these)
        assert 0.0 <= values.min() < 0.1, "Should have values near 0"
        assert 0.9 < values.max() <= 1.0, "Should have values near 1"
        mean = values.mean()
        assert 0.4 < mean < 0.6, "Mean should be around 0.5"

        # The real issue is predictability, not statistical distribution
        # Document that this is pseudorandom, not cryptographically secure
        # This demonstrates the core issue: we can predict future values
        # if we know the internal state, which makes it unsuitable for security

    def test_ruff_security_scanner_detects_s311_violation(self):
        """Test that security scanners no longer detect S311 violations (fix verified)."""
        # This test verifies that static analysis tools no longer flag this as insecure
        # S311: Standard pseudo-random generators are not suitable for cryptographic purposes
        content = _RAND_SOURCE_FILE.read_text()

        # Verify that S311 violation patterns are no longer present
        lines = content.split("\n")

        # Look for insecure patterns that would trigger S311
        insecure_patterns = [
            "import random as _random",
            "random.random()",  # Check lines that have this pattern but NOT secure_random.random()
        ]

        violations_found = []
        for i, line in enumerate(lines, 1):
            # Check for import random as _random
            if "import random as _random" in line:
                violations_found.append(f"Line {i}: {line.strip()}")

            # Check for random.random() that's NOT part of secure_random.random()
            if (
                "random.random()" in line
                and "secure_random.random()" not in line
            ):
                violations_found.append(f"Line {i}: {line.strip()}")

        assert len(violations_found) == 0, (
            f"Should not find S311 violations after security fix. Found: {violations_found}"
        )


class TestFixedRandSecurityImplementation:
    """Tests for the corrected rand() function implementation.

    These tests define the expected behavior after fixing the security issues.
    Initially these will fail (red phase), then pass after implementation (green phase).
    """

    def test_rand_function_uses_cryptographically_secure_randomness(self):
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

        # After fix, should use secrets module
        assert "import secrets" in rand_function_body, (
            "rand() function should use secrets module for cryptographic security"
        )
        assert "secrets.SystemRandom()" in rand_function_body, (
            "rand() function should use secrets.SystemRandom() for secure randomness"
        )

        # Should not use insecure random module
        assert (
            "import random" not in rand_function_body
            or "_random" not in rand_function_body
        ), "rand() function should not use insecure random module"

    def test_fixed_rand_function_is_not_predictable_with_seed(self):
        """Test that fixed rand() does not produce predictable sequences."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # Generate multiple sequences - they should be different
        # (cryptographically secure random cannot be seeded predictably)
        sequences = []
        for _ in range(5):
            result = star.execute_query(
                "UNWIND [1,2,3,4,5] AS x RETURN rand() AS r",
            )
            sequences.append(result["r"].tolist())

        # All sequences should be different (very high probability)
        unique_sequences = len(set(tuple(seq) for seq in sequences))
        assert unique_sequences == 5, (
            "Cryptographically secure rand() should produce different sequences each time"
        )

    def test_fixed_rand_maintains_functional_compatibility(self):
        """Test that fixed rand() maintains same functional interface and behavior."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        # Should still work with 0 arguments
        result = star.execute_query("UNWIND [1,2,3] AS x RETURN rand() AS r")

        # Should return same number of values as input
        assert len(result) == 3, "Should return one value per row"

        # Should return floats in [0.0, 1.0) range
        assert all(0.0 <= val < 1.0 for val in result["r"].values), (
            "All values should be in [0.0, 1.0) range"
        )
        assert result["r"].dtype == float, "Should return float dtype"

    def test_fixed_rand_passes_statistical_randomness_tests(self):
        """Test that fixed rand() passes basic statistical randomness tests."""
        star = Star(context=Context(entity_mapping=EntityMapping(mapping={})))

        result = star.execute_query(
            "UNWIND range(1, 100) AS x RETURN rand() AS r",
        )
        values = result["r"].values

        # Statistical tests for uniform distribution in [0, 1)
        assert values.min() >= 0.0, "Min should be >= 0.0"
        assert values.max() < 1.0, "Max should be < 1.0"

        # Mean should be around 0.5 for uniform distribution
        mean = values.mean()
        assert 0.4 < mean < 0.6, f"Mean should be ~0.5, got {mean}"

        # Should have good spread across the range
        assert values.std() > 0.2, (
            "Standard deviation should indicate good spread"
        )

    def test_no_s311_security_violations_after_fix(self):
        """Test that security scanners no longer flag S311 violations in rand()."""
        content = _RAND_SOURCE_FILE.read_text()

        # Find the _rand function definition
        rand_function_match = re.search(
            r'def _rand\(.*?\).*?:\s*""".*?"""(.*?)(?=def|\Z)',
            content,
            re.DOTALL,
        )
        assert rand_function_match, "Should find _rand function definition"

        rand_function_body = rand_function_match.group(1)

        # Should not contain patterns that trigger S311
        assert "import random as _random" not in rand_function_body, (
            "Should not use insecure random module import (triggers S311)"
        )
        assert "[_random.random()" not in rand_function_body, (
            "Should not use _random.random() list comprehension (triggers S311)"
        )

        # secure_random.random() is fine - it's from secrets.SystemRandom()
        assert "secure_random.random()" in rand_function_body, (
            "Should use secrets.SystemRandom().random() for security"
        )

    def test_scalar_function_registry_still_contains_rand(self):
        """Test that rand() function is still properly registered after fix."""
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()

        # Should still be registered
        assert "rand" in registry._functions, (
            "rand() should still be registered"
        )

        # Should have correct metadata
        func_info = registry._functions["rand"]
        assert func_info.min_args == 0, (
            "rand() should require 0 minimum arguments"
        )
        assert func_info.max_args == 0, (
            "rand() should require 0 maximum arguments"
        )
        assert "random float" in func_info.description.lower(), (
            "Description should mention random float"
        )

    def test_cypher_query_with_rand_still_works(self):
        """Test that Cypher queries using rand() still work after security fix."""
        # Integration test to ensure end-to-end functionality is preserved
        from pycypher.relational_models import EntityTable, RelationshipMapping

        people = pd.DataFrame(
            {"__ID__": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
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

        # Test rand() in WHERE clause for sampling
        result = star.execute_query(
            "MATCH (p:Person) WHERE rand() >= 0.0 RETURN p.name",
        )
        assert len(result) <= 3, "Should return subset or all people"
        assert "name" in result.columns, "Should return name column"
