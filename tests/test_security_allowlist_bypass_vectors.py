"""Tests for SQL injection bypass vectors and Neo4j identifier hardening.

These tests verify that the allowlist-based SQL validation and hardened
Neo4j identifier validation block attack vectors that the previous
blocklist approach could not catch.

Run with:
    uv run pytest tests/test_security_allowlist_bypass_vectors.py -v
"""

from __future__ import annotations

import pytest
from pycypher.ingestion.security import SecurityError, validate_sql_query
from pycypher.sinks.neo4j import (
    NodeMapping,
    RelationshipMapping,
    _validate_cypher_identifier,
)
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Task #2: SQL injection allowlist — bypass vectors that defeat blocklists
# ---------------------------------------------------------------------------


class TestSQLAllowlistBlocksBypassVectors:
    """Attacks that the old blocklist approach could NOT catch."""

    def test_comment_injection_single_line_defused(self) -> None:
        """Single-line comment stripped — payload after -- is removed, SELECT passes."""
        # After comment stripping this becomes "SELECT * FROM source" which is safe.
        # The key point: the payload is DEFUSED, not executed.
        validate_sql_query("SELECT * FROM source -- ; DROP TABLE users")

    def test_comment_injection_hash_defused(self) -> None:
        """MySQL-style # comment stripped — payload is removed."""
        validate_sql_query("SELECT * FROM source # DROP TABLE users")

    def test_comment_injection_block_defused(self) -> None:
        """Block comment stripped — payload inside comment is removed."""
        validate_sql_query("SELECT * FROM source /* DROP TABLE users */")

    def test_comment_hiding_non_select_rejected(self) -> None:
        """Comment cannot hide a dangerous statement prefix."""
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("/* SELECT */ DROP TABLE users")

    def test_unterminated_block_comment(self) -> None:
        """Unterminated block comment should be rejected."""
        with pytest.raises(SecurityError, match="Unterminated block comment"):
            validate_sql_query("SELECT * FROM source /* never closed")

    def test_drop_table_rejected_by_allowlist(self) -> None:
        """DROP TABLE is not SELECT — rejected by allowlist, not pattern matching."""
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("DROP TABLE users")

    def test_delete_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("DELETE FROM users WHERE 1=1")

    def test_insert_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("INSERT INTO users VALUES ('hacked')")

    def test_update_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("UPDATE users SET admin=1")

    def test_create_table_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("CREATE TABLE hacked (data TEXT)")

    def test_alter_table_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("ALTER TABLE users ADD COLUMN hacked TEXT")

    def test_grant_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("GRANT ALL ON users TO public")

    def test_exec_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("EXEC sp_password 'sa', 'hacked'")

    def test_truncate_rejected_by_allowlist(self) -> None:
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("TRUNCATE TABLE users")

    def test_multi_statement_with_trailing_payload(self) -> None:
        """Semicolon-separated multi-statement attack."""
        with pytest.raises(SecurityError, match="Multiple SQL statements"):
            validate_sql_query("SELECT 1; DROP TABLE users;")

    def test_semicolon_in_middle_of_query(self) -> None:
        """Semicolon not at the end should be rejected."""
        with pytest.raises(SecurityError):
            validate_sql_query("SELECT 1; SELECT 2")

    def test_whitespace_obfuscation_still_blocked(self) -> None:
        """Tabs and newlines between keywords shouldn't bypass."""
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("DROP\t\nTABLE\t\nusers")

    def test_case_variation_blocked(self) -> None:
        """Mixed case doesn't bypass allowlist."""
        with pytest.raises(SecurityError, match="Only SELECT"):
            validate_sql_query("DrOp TaBlE users")


class TestSQLAllowlistAcceptsLegitimateQueries:
    """Ensure the allowlist doesn't reject valid data queries."""

    def test_simple_select(self) -> None:
        validate_sql_query("SELECT * FROM source")

    def test_select_with_where(self) -> None:
        validate_sql_query("SELECT name, age FROM users WHERE age > 21")

    def test_select_with_join(self) -> None:
        validate_sql_query(
            "SELECT a.name, b.value FROM source a JOIN other b ON a.id = b.id",
        )

    def test_select_with_subquery(self) -> None:
        validate_sql_query(
            "SELECT * FROM source WHERE id IN (SELECT id FROM other)",
        )

    def test_select_with_aggregation(self) -> None:
        validate_sql_query(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        )

    def test_select_with_trailing_semicolon(self) -> None:
        """A single trailing semicolon is fine — many SQL tools add it."""
        validate_sql_query("SELECT * FROM source;")

    def test_with_cte_select(self) -> None:
        """WITH (CTE) followed by SELECT is a legitimate read-only query."""
        validate_sql_query(
            "WITH cte AS (SELECT id FROM source) SELECT * FROM cte",
        )

    def test_select_with_order_limit(self) -> None:
        validate_sql_query(
            "SELECT * FROM source ORDER BY name LIMIT 100 OFFSET 10",
        )

    def test_select_with_quoted_strings(self) -> None:
        """String literals containing 'dangerous' words are fine."""
        validate_sql_query("SELECT * FROM source WHERE name = 'DROP TABLE'")

    def test_select_with_doubled_quotes(self) -> None:
        """SQL-standard escaped quotes should not break parsing."""
        validate_sql_query("SELECT * FROM source WHERE name = 'it''s fine'")

    def test_empty_query_rejected(self) -> None:
        with pytest.raises(SecurityError, match="Empty query"):
            validate_sql_query("")

    def test_whitespace_only_rejected(self) -> None:
        with pytest.raises(SecurityError, match="Empty query"):
            validate_sql_query("   \n\t  ")


# ---------------------------------------------------------------------------
# Task #3: Neo4j Cypher identifier hardening — new attack vectors
# ---------------------------------------------------------------------------


class TestCypherIdentifierUnicodeConfusables:
    """Unicode characters that normalise to dangerous chars via NFKC."""

    def test_fullwidth_grave_accent_rejected(self) -> None:
        """U+FF40 (fullwidth grave accent) normalises to backtick."""
        with pytest.raises(ValueError, match="backtick"):
            _validate_cypher_identifier("\uff40injection", "label")

    def test_fullwidth_left_curly_bracket_rejected(self) -> None:
        """U+FF5B (fullwidth left curly bracket) normalises to {."""
        with pytest.raises(ValueError, match="curly brace"):
            _validate_cypher_identifier("test\uff5b", "label")

    def test_fullwidth_right_curly_bracket_rejected(self) -> None:
        """U+FF5D (fullwidth right curly bracket) normalises to }."""
        with pytest.raises(ValueError, match="curly brace"):
            _validate_cypher_identifier("test\uff5d", "label")

    def test_fullwidth_left_square_bracket_rejected(self) -> None:
        """U+FF3B (fullwidth left square bracket) normalises to [."""
        with pytest.raises(ValueError, match="square bracket"):
            _validate_cypher_identifier("test\uff3b", "label")

    def test_fullwidth_right_square_bracket_rejected(self) -> None:
        """U+FF3D (fullwidth right square bracket) normalises to ]."""
        with pytest.raises(ValueError, match="square bracket"):
            _validate_cypher_identifier("test\uff3d", "label")

    def test_fullwidth_reverse_solidus_rejected(self) -> None:
        """U+FF3C (fullwidth reverse solidus) normalises to backslash."""
        with pytest.raises(ValueError, match="backslash"):
            _validate_cypher_identifier("test\uff3c", "label")


class TestCypherIdentifierNewDangerousChars:
    """Characters newly rejected beyond backtick and NUL."""

    def test_curly_brace_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="curly brace"):
            _validate_cypher_identifier("Person{}", "label")

    def test_curly_brace_close_rejected(self) -> None:
        with pytest.raises(ValueError, match="curly brace"):
            _validate_cypher_identifier("Person}", "label")

    def test_square_bracket_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="square bracket"):
            _validate_cypher_identifier("Person[0]", "label")

    def test_square_bracket_close_rejected(self) -> None:
        with pytest.raises(ValueError, match="square bracket"):
            _validate_cypher_identifier("Person]", "label")

    def test_backslash_rejected(self) -> None:
        with pytest.raises(ValueError, match="backslash"):
            _validate_cypher_identifier("Person\\n", "label")

    def test_realistic_cypher_injection_with_curlies(self) -> None:
        """Curly brace injection to alter MERGE property map."""
        # This payload has both backtick and curly braces — either triggers rejection.
        with pytest.raises(ValueError):
            _validate_cypher_identifier("Person` {admin: true}) // ", "label")

    def test_curly_brace_injection_without_backtick(self) -> None:
        """Pure curly brace injection without backtick."""
        with pytest.raises(ValueError, match="curly brace"):
            _validate_cypher_identifier("Person {admin: true}", "label")

    def test_realistic_injection_with_brackets(self) -> None:
        """Square bracket injection for alternate quoting."""
        with pytest.raises(ValueError, match="square bracket"):
            _validate_cypher_identifier(
                "[Person]; MATCH (n) DETACH DELETE n //",
                "label",
            )


class TestCypherIdentifierViaNodeMapping:
    """Verify hardened validation propagates through Pydantic models."""

    def test_node_mapping_rejects_curly_braces_in_label(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person{admin:true}", id_column="id")

    def test_node_mapping_rejects_unicode_backtick_in_label(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person\uff40", id_column="id")

    def test_node_mapping_rejects_backslash_in_id_property(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            NodeMapping(label="Person", id_column="id", id_property="id\\n")

    def test_relationship_mapping_rejects_curly_brace_in_rel_type(
        self,
    ) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS{since:2024}",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_relationship_mapping_rejects_brackets_in_label(self) -> None:
        with pytest.raises((ValueError, ValidationError)):
            RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person[0]",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )


class TestCypherIdentifierStillAcceptsValid:
    """Ensure hardened validation doesn't reject legitimate identifiers."""

    def test_simple_ascii_label(self) -> None:
        _validate_cypher_identifier("Person", "label")

    def test_label_with_underscores(self) -> None:
        _validate_cypher_identifier("My_Person_Label", "label")

    def test_label_with_hyphens(self) -> None:
        _validate_cypher_identifier("person-node", "label")

    def test_label_with_spaces(self) -> None:
        _validate_cypher_identifier("My Person", "label")

    def test_label_with_unicode_letters(self) -> None:
        _validate_cypher_identifier("Personne_FR", "label")

    def test_label_with_numbers(self) -> None:
        _validate_cypher_identifier("Person2024", "label")

    def test_label_with_dots(self) -> None:
        _validate_cypher_identifier("com.example.Person", "label")

    def test_label_with_emoji(self) -> None:
        """Emoji don't normalise to dangerous chars and should be accepted."""
        _validate_cypher_identifier("Person_\U0001f600", "label")
