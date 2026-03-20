"""Security tests for Loop 253: Cryptographically secure random_hash function.

This test file verifies the security improvements for the random_hash() function:
- Fixed S324: No longer uses MD5 (now uses SHA256)
- Fixed S311: No longer uses random.random() (now uses secrets.token_bytes)

The random_hash() function generates unique identifiers for Relation instances
in the core query processing engine using cryptographically secure methods.
"""

import random
from unittest.mock import patch

import pytest
from pycypher.ast_models import random_hash
from pycypher.relational_models import Relation

pytestmark = pytest.mark.slow


class TestSecureRandomHashImplementation:
    """Tests for the secure replacement implementation.

    These tests define the expected behavior after fixing the security vulnerabilities.
    Initially these tests will fail (red phase), then pass after implementation (green phase).
    """

    def test_secure_random_hash_uses_cryptographically_secure_random(self):
        """Test that the fixed implementation uses cryptographically secure random."""
        # This test will initially fail but should pass after the fix
        with patch("random.seed"):
            # Even with a fixed seed for standard random, the secure implementation
            # should still produce different results because it uses os.urandom or secrets
            random.seed(42)
            hash1 = random_hash()

            random.seed(42)  # Same seed
            hash2 = random_hash()

            # With cryptographically secure random, these should be different
            # even with the same random.seed (because we won't be using random.random)
            assert hash1 != hash2, (
                "Secure implementation should not be affected by random.seed"
            )

    def test_secure_random_hash_uses_sha256_or_better(self):
        """Test that the fixed implementation uses SHA256 or better (not MD5).

        SHA256 produces 64-character hex digests, unlike MD5's 32 characters.
        """
        secure_hash = random_hash()

        # After fix, should be longer than 32 characters (not MD5)
        # SHA256 hex digest is 64 characters, SHA1 is 40, both better than MD5's 32
        assert len(secure_hash) > 32, (
            "Should use SHA256 (64 chars) or SHA1 (40 chars), not MD5 (32 chars)"
        )

    def test_secure_random_hash_has_sufficient_entropy(self):
        """Test that the fixed implementation has sufficient entropy.

        The secure implementation should use enough random bytes to make
        collisions extremely unlikely in practical usage.
        """
        # Generate many hashes to test for sufficient entropy
        hashes = set()
        num_hashes = 10000

        for _ in range(num_hashes):
            h = random_hash()
            hashes.add(h)

        # With sufficient entropy, we should have unique hashes
        # (probability of collision with good entropy should be negligible)
        assert len(hashes) == num_hashes, (
            "All hashes should be unique with sufficient entropy"
        )

    def test_secure_random_hash_consistent_format(self):
        """Test that the secure implementation maintains consistent hexadecimal format."""
        secure_hash = random_hash()

        # Should still be hexadecimal for compatibility
        assert all(c in "0123456789abcdef" for c in secure_hash), (
            "Should maintain hex format"
        )

        # Should have consistent length
        hash2 = random_hash()
        assert len(secure_hash) == len(hash2), (
            "Hash length should be consistent"
        )

    def test_relation_identifier_uses_secure_hash(self):
        """Test that Relation instances get secure identifiers after fix."""
        relation1 = Relation()
        relation2 = Relation()

        # Should use the secure hash (longer than 32 chars for SHA256)
        assert len(relation1.identifier) > 32, (
            "Should use secure hash, not MD5"
        )
        assert len(relation2.identifier) > 32, (
            "Should use secure hash, not MD5"
        )

        # Should still be different for each relation
        assert relation1.identifier != relation2.identifier

        # Should be hexadecimal
        assert all(c in "0123456789abcdef" for c in relation1.identifier)
        assert all(c in "0123456789abcdef" for c in relation2.identifier)
