"""Tests for Cypher query formatter and linter."""

from __future__ import annotations

from pycypher.query_formatter import (
    LintIssue,
    format_query,
    lint_query,
)


class TestFormatQuery:
    """Verify query formatting."""

    def test_uppercase_keywords(self) -> None:
        result = format_query("match (n:Person) return n.name")
        assert "MATCH" in result
        assert "RETURN" in result

    def test_clause_per_line(self) -> None:
        result = format_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        )
        lines = result.strip().split("\n")
        assert len(lines) >= 2
        assert lines[0].strip().startswith("MATCH")

    def test_preserves_string_literals(self) -> None:
        result = format_query(
            "MATCH (n:Person) WHERE n.name = 'match' RETURN n",
        )
        assert "'match'" in result

    def test_preserves_double_quoted_strings(self) -> None:
        result = format_query(
            'MATCH (n) WHERE n.name = "return" RETURN n',
        )
        assert '"return"' in result

    def test_empty_query(self) -> None:
        assert format_query("") == ""
        assert format_query("  ") == "  "

    def test_single_clause(self) -> None:
        result = format_query("RETURN 42")
        assert result.strip() == "RETURN 42"

    def test_optional_match(self) -> None:
        result = format_query(
            "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b",
        )
        assert "OPTIONAL MATCH" in result

    def test_where_indented(self) -> None:
        result = format_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n",
            indent=2,
        )
        lines = result.strip().split("\n")
        where_lines = [l for l in lines if "WHERE" in l]
        assert len(where_lines) >= 1

    def test_no_uppercase_option(self) -> None:
        result = format_query(
            "match (n) return n",
            uppercase=False,
        )
        assert "match" in result

    def test_relationship_pattern(self) -> None:
        result = format_query(
            "match (a:Person)-[r:KNOWS]->(b:Person) "
            "where a.age > 25 return a.name, b.name",
        )
        assert "MATCH" in result
        assert "WHERE" in result
        assert "RETURN" in result
        assert ":Person" in result
        assert ":KNOWS" in result

    def test_aggregation_query(self) -> None:
        result = format_query(
            "match (n:Person) return n.dept, count(n) as cnt",
        )
        assert "RETURN" in result
        assert "AS" in result

    def test_multi_clause_complex(self) -> None:
        result = format_query(
            "match (a:Person)-[:KNOWS]->(b:Person) "
            "with a, count(b) as friends "
            "where friends > 5 "
            "return a.name, friends "
            "order by friends desc "
            "limit 10",
        )
        assert "MATCH" in result
        assert "WITH" in result
        assert "RETURN" in result
        assert "ORDER BY" in result
        assert "LIMIT" in result

    def test_create_query(self) -> None:
        result = format_query(
            "create (n:Person {name: 'Alice', age: 30})",
        )
        assert "CREATE" in result

    def test_merge_with_on_create(self) -> None:
        result = format_query(
            "merge (n:Person {id: 1}) on create set n.created = timestamp()",
        )
        assert "MERGE" in result
        assert "ON CREATE SET" in result

    def test_unwind_query(self) -> None:
        result = format_query(
            "unwind [1, 2, 3] as x return x",
        )
        assert "UNWIND" in result

    def test_string_content_preserved(self) -> None:
        result = format_query(
            "MATCH (n) WHERE n.name = 'Alice' RETURN n",
        )
        assert "'Alice'" in result


class TestLintQuery:
    """Verify query linting."""

    def test_lowercase_keyword_detected(self) -> None:
        issues = lint_query("match (n) return n")
        keyword_issues = [i for i in issues if "should be uppercase" in i.message]
        assert len(keyword_issues) > 0

    def test_trailing_whitespace_detected(self) -> None:
        issues = lint_query("MATCH (n) RETURN n   ")
        trailing = [i for i in issues if "Trailing whitespace" in i.message]
        assert len(trailing) > 0

    def test_clean_query_no_issues(self) -> None:
        issues = lint_query("MATCH (n:Person) RETURN n.name")
        # Filter out parse errors (some queries may not fully parse)
        style_issues = [i for i in issues if i.severity == "warning"]
        # Simple MATCH...RETURN should have few or no style issues
        keyword_issues = [i for i in style_issues if "should be uppercase" in i.message]
        assert len(keyword_issues) == 0

    def test_lint_issue_dataclass(self) -> None:
        issue = LintIssue(
            line=1,
            column=5,
            message="test",
            severity="warning",
        )
        assert issue.line == 1
        assert issue.column == 5
        assert issue.message == "test"
        assert issue.severity == "warning"

    def test_lint_issue_frozen(self) -> None:
        import pytest

        issue = LintIssue(line=1, column=0, message="test")
        with pytest.raises(AttributeError):
            issue.line = 2  # type: ignore[misc]

    def test_mixed_case_query(self) -> None:
        issues = lint_query("Match (n) Return n")
        keyword_issues = [i for i in issues if "should be uppercase" in i.message]
        assert len(keyword_issues) >= 2


class TestTokenizer:
    """Verify string-preserving tokenizer."""

    def test_no_strings(self) -> None:
        from pycypher.query_formatter import _tokenize_preserving_strings

        tokens = _tokenize_preserving_strings("MATCH (n) RETURN n")
        assert len(tokens) == 1
        assert tokens[0] == (False, "MATCH (n) RETURN n")

    def test_single_quoted(self) -> None:
        from pycypher.query_formatter import _tokenize_preserving_strings

        tokens = _tokenize_preserving_strings("WHERE n.name = 'Alice'")
        assert any(t == (True, "'Alice'") for t in tokens)

    def test_double_quoted(self) -> None:
        from pycypher.query_formatter import _tokenize_preserving_strings

        tokens = _tokenize_preserving_strings('WHERE n.name = "Bob"')
        assert any(t == (True, '"Bob"') for t in tokens)

    def test_escaped_quote(self) -> None:
        from pycypher.query_formatter import _tokenize_preserving_strings

        tokens = _tokenize_preserving_strings(r"WHERE n.name = 'it\'s'")
        string_tokens = [t for is_str, t in tokens if is_str]
        assert len(string_tokens) == 1
        assert "\\'" in string_tokens[0]

    def test_multiple_strings(self) -> None:
        from pycypher.query_formatter import _tokenize_preserving_strings

        tokens = _tokenize_preserving_strings(
            "WHERE n.a = 'x' AND n.b = 'y'",
        )
        string_tokens = [t for is_str, t in tokens if is_str]
        assert len(string_tokens) == 2
