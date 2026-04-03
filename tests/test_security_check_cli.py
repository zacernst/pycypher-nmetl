"""Tests for the nmetl security-check CLI command."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner
from pycypher.cli.main import cli
from pycypher.cli.security import (
    SecurityReport,
    Severity,
    scan_config,
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def tmp_config(tmp_path):
    """Helper to write a YAML config and return its path."""

    def _write(yaml_text: str) -> Path:
        p = tmp_path / "pipeline.yaml"
        p.write_text(textwrap.dedent(yaml_text), encoding="utf-8")
        return p

    return _write


# ---------------------------------------------------------------------------
# Unit tests for scan_config
# ---------------------------------------------------------------------------


class TestScanConfigHTTP:
    """HTTP URL detection."""

    def test_http_remote_url_is_high(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: people
                  uri: "http://example.com/data.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 1
        assert http_findings[0].severity == Severity.HIGH

    def test_http_localhost_is_ok(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: people
                  uri: "http://localhost:8080/data.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 0


class TestScanConfigCredentials:
    """Embedded credentials detection."""

    def test_password_in_uri(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: db
                  uri: "postgresql://user:s3cret@db.example.com/mydb"
                  entity_type: Person
                  query: "SELECT * FROM people"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        cred_findings = [
            f for f in report.findings if f.category == "embedded-credentials"
        ]
        assert len(cred_findings) >= 1
        assert all(f.severity == Severity.HIGH for f in cred_findings)

    def test_credential_param_in_uri(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: api
                  uri: "https://api.example.com/data.csv?api_key=abc123"
                  entity_type: Record
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        cred_findings = [
            f for f in report.findings if f.category == "embedded-credentials"
        ]
        assert len(cred_findings) >= 1


class TestScanConfigEnvVars:
    """Environment variable security patterns."""

    def test_secret_env_var_warning(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: db
                  uri: "postgresql://user:${DB_PASSWORD}@db.example.com/mydb"
                  entity_type: Person
                  query: "SELECT * FROM people"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        env_findings = [
            f for f in report.findings if f.category == "env-var-secret"
        ]
        assert len(env_findings) >= 1
        assert env_findings[0].severity == Severity.MEDIUM


class TestScanConfigQueryInjection:
    """Query injection risk detection."""

    def test_env_var_in_inline_query(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            queries:
              - id: q1
                inline: "MATCH (n:${NODE_TYPE}) RETURN n"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        injection_findings = [
            f for f in report.findings if f.category == "query-injection"
        ]
        assert len(injection_findings) >= 1
        assert injection_findings[0].severity == Severity.HIGH

    def test_env_var_in_source_query(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: db
                  uri: "postgresql://localhost/mydb"
                  entity_type: Person
                  query: "SELECT * FROM ${TABLE_NAME}"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        injection_findings = [
            f for f in report.findings if f.category == "query-injection"
        ]
        assert len(injection_findings) >= 1


class TestScanConfigWildcardFunctions:
    """Wildcard function registration detection."""

    def test_wildcard_function_registration(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            functions:
              - module: "mypackage.utils"
                names: "*"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        wildcard_findings = [
            f for f in report.findings if f.category == "wildcard-import"
        ]
        assert len(wildcard_findings) == 1
        assert wildcard_findings[0].severity == Severity.MEDIUM


class TestScanConfigCleanConfig:
    """A clean config should produce minimal findings."""

    def test_clean_config_no_high_findings(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            project:
              name: "test-pipeline"
              description: "A clean test pipeline"
            sources:
              entities:
                - id: people
                  uri: "/data/people.csv"
                  entity_type: Person
            queries:
              - id: q1
                inline: "MATCH (n:Person) RETURN n"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        high_findings = [
            f for f in report.findings if f.severity == Severity.HIGH
        ]
        assert len(high_findings) == 0


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


class TestSecurityCheckCLI:
    """Integration tests for the security-check CLI command."""

    def test_clean_config_exit_zero(self, runner, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            project:
              name: "safe"
            sources:
              entities:
                - id: data
                  uri: "/safe/path/data.csv"
                  entity_type: Item
            queries:
              - id: q1
                inline: "MATCH (n:Item) RETURN n"
        """)
        result = runner.invoke(cli, ["security-check", str(cfg_path)])
        assert result.exit_code == 0
        assert (
            "No security findings" in result.output
            or "finding" in result.output
        )

    def test_http_url_exits_nonzero(self, runner, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: remote
                  uri: "http://evil.example.com/data.csv"
                  entity_type: Record
        """)
        result = runner.invoke(cli, ["security-check", str(cfg_path)])
        assert result.exit_code == 1
        assert "HIGH" in result.output

    def test_json_output(self, runner, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: remote
                  uri: "http://evil.example.com/data.csv"
                  entity_type: Record
        """)
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--json"],
        )
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert any(f["category"] == "insecure-transport" for f in data)

    def test_severity_filter(self, runner, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: data
                  uri: "/safe/path/data.csv"
                  entity_type: Item
        """)
        # With severity=high, info-level findings should be excluded
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--severity", "high"],
        )
        assert result.exit_code == 0
        assert "No security findings" in result.output

    def test_fail_on_medium(self, runner, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            functions:
              - module: "mypackage.utils"
                names: "*"
        """)
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--fail-on", "medium"],
        )
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# SecurityReport unit tests
# ---------------------------------------------------------------------------


class TestSecurityReport:
    def test_empty_report(self):
        report = SecurityReport()
        assert report.high_count == 0
        assert report.medium_count == 0
        assert len(report.findings) == 0

    def test_add_finding(self):
        report = SecurityReport()
        report.add(Severity.HIGH, "test", "msg", "loc")
        assert report.high_count == 1
        assert len(report.findings) == 1

    def test_mixed_severities(self):
        report = SecurityReport()
        report.add(Severity.HIGH, "a", "m1", "l1")
        report.add(Severity.MEDIUM, "b", "m2", "l2")
        report.add(Severity.LOW, "c", "m3", "l3")
        assert report.high_count == 1
        assert report.medium_count == 1
        assert len(report.findings) == 3

    def test_only_low_and_info_findings(self):
        report = SecurityReport()
        report.add(Severity.LOW, "a", "m1", "l1")
        report.add(Severity.INFO, "b", "m2", "l2")
        assert report.high_count == 0
        assert report.medium_count == 0
        assert len(report.findings) == 2

    def test_multiple_high_findings(self):
        report = SecurityReport()
        report.add(Severity.HIGH, "a", "m1", "l1")
        report.add(Severity.HIGH, "b", "m2", "l2")
        report.add(Severity.HIGH, "c", "m3", "l3")
        assert report.high_count == 3
        assert report.medium_count == 0


# ---------------------------------------------------------------------------
# Severity enum tests
# ---------------------------------------------------------------------------


class TestSeverityEnum:
    def test_all_values_exist(self):
        assert Severity.HIGH == "high"
        assert Severity.MEDIUM == "medium"
        assert Severity.LOW == "low"
        assert Severity.INFO == "info"

    def test_enum_members_complete(self):
        assert set(Severity) == {
            Severity.HIGH,
            Severity.MEDIUM,
            Severity.LOW,
            Severity.INFO,
        }


# ---------------------------------------------------------------------------
# Finding dataclass tests
# ---------------------------------------------------------------------------


class TestFinding:
    def test_finding_fields(self):
        from pycypher.cli.security import Finding

        f = Finding(
            severity=Severity.HIGH,
            category="test-cat",
            message="test message",
            location="test.location",
        )
        assert f.severity == Severity.HIGH
        assert f.category == "test-cat"
        assert f.message == "test message"
        assert f.location == "test.location"

    def test_finding_empty_strings(self):
        from pycypher.cli.security import Finding

        f = Finding(
            severity=Severity.INFO,
            category="",
            message="",
            location="",
        )
        assert f.category == ""
        assert f.message == ""
        assert f.location == ""


# ---------------------------------------------------------------------------
# Unit tests for _check_uri_security
# ---------------------------------------------------------------------------


class TestCheckUriSecurity:
    """Direct unit tests for URI security checking."""

    def test_http_127_0_0_1_is_allowed(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: local
                  uri: "http://127.0.0.1:9090/data.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 0

    def test_https_url_no_insecure_transport(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: secure
                  uri: "https://example.com/data.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 0

    def test_username_without_password_is_medium(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: db
                  uri: "postgresql://admin@db.example.com/mydb"
                  entity_type: Person
                  query: "SELECT * FROM t"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        username_findings = [
            f for f in report.findings if f.category == "username-in-uri"
        ]
        assert len(username_findings) == 1
        assert username_findings[0].severity == Severity.MEDIUM

    def test_file_uri_no_transport_finding(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: local
                  uri: "file:///data/people.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 0


# ---------------------------------------------------------------------------
# Unit tests for _check_env_var_patterns — all secret names
# ---------------------------------------------------------------------------


class TestCheckEnvVarPatterns:
    """Test detection of all inline secret name patterns."""

    @pytest.mark.parametrize(
        "var_name",
        ["MY_TOKEN", "API_KEY", "MY_APIKEY", "PRIVATE_KEY", "SECRET_KEY"],
    )
    def test_secret_env_var_names(self, var_name):
        """Test each inline secret name is detected via direct function call."""
        from pycypher.cli.security import _check_env_var_patterns

        report = SecurityReport()
        _check_env_var_patterns(
            f"postgresql://${{DB_USER}}:${{{var_name}}}@host/db",
            "test.uri",
            report,
        )
        env_findings = [
            f for f in report.findings if f.category == "env-var-secret"
        ]
        assert len(env_findings) >= 1
        assert all(f.severity == Severity.MEDIUM for f in env_findings)

    def test_non_secret_env_var_no_finding(self):
        """Non-secret env var names should not produce findings."""
        from pycypher.cli.security import _check_env_var_patterns

        report = SecurityReport()
        _check_env_var_patterns(
            "postgresql://${DATABASE_HOST}:5432/mydb",
            "test.uri",
            report,
        )
        env_findings = [
            f for f in report.findings if f.category == "env-var-secret"
        ]
        assert len(env_findings) == 0


# ---------------------------------------------------------------------------
# Unit tests for _check_glob_pattern
# ---------------------------------------------------------------------------


class TestCheckGlobPattern:
    """Test detection of all broad glob patterns."""

    @pytest.mark.parametrize(
        "glob",
        ["*", "**", "*.*", "**/*", "**/*.*"],
    )
    def test_broad_globs_flagged(self, tmp_config, glob):
        cfg_path = tmp_config(f"""\
            version: "1.0"
            sources:
              entities: []
            queries:
              - id: q1
                source: "{glob}"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        glob_findings = [
            f for f in report.findings if f.category == "broad-glob"
        ]
        assert len(glob_findings) == 1
        assert glob_findings[0].severity == Severity.MEDIUM

    def test_specific_glob_not_flagged(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            queries:
              - id: q1
                source: "data/*.csv"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        glob_findings = [
            f for f in report.findings if f.category == "broad-glob"
        ]
        assert len(glob_findings) == 0


# ---------------------------------------------------------------------------
# Unit tests for _check_query_injection — false positive check
# ---------------------------------------------------------------------------


class TestCheckQueryInjectionFalsePositive:
    """Queries without env vars should not produce injection findings."""

    def test_clean_query_no_injection_finding(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            queries:
              - id: q1
                inline: "MATCH (n:Person) WHERE n.age > 25 RETURN n.name"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        injection_findings = [
            f for f in report.findings if f.category == "query-injection"
        ]
        assert len(injection_findings) == 0


# ---------------------------------------------------------------------------
# scan_config coverage: relationship sources, output sinks, project metadata
# ---------------------------------------------------------------------------


class TestScanConfigRelationshipSources:
    """Relationship sources should be scanned like entity sources."""

    def test_http_in_relationship_source(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
              relationships:
                - id: knows
                  uri: "http://evil.example.com/rels.csv"
                  relationship_type: KNOWS
                  from_entity: Person
                  to_entity: Person
                  source_col: source_id
                  target_col: target_id
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 1
        assert "relationships[0]" in http_findings[0].location

    def test_injection_in_relationship_query(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
              relationships:
                - id: knows
                  uri: "postgresql://localhost/db"
                  relationship_type: KNOWS
                  from_entity: Person
                  to_entity: Person
                  source_col: source_id
                  target_col: target_id
                  query: "SELECT * FROM ${REL_TABLE}"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        injection_findings = [
            f for f in report.findings if f.category == "query-injection"
        ]
        assert len(injection_findings) >= 1


class TestScanConfigOutputSinks:
    """Output sink URIs should be scanned."""

    def test_http_in_output_uri(self):
        """Output sink URIs are checked via direct function call."""
        from pycypher.cli.security import _check_uri_security

        report = SecurityReport()
        _check_uri_security(
            "http://evil.example.com/upload.csv",
            "output[0].uri",
            report,
        )
        http_findings = [
            f for f in report.findings if f.category == "insecure-transport"
        ]
        assert len(http_findings) == 1
        assert "output[0]" in http_findings[0].location


class TestScanConfigProjectMetadata:
    """Missing project metadata should produce INFO finding."""

    def test_no_project_section_info_finding(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: people
                  uri: "/data/people.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        meta_findings = [
            f for f in report.findings if f.category == "missing-metadata"
        ]
        assert len(meta_findings) == 1
        assert meta_findings[0].severity == Severity.INFO

    def test_with_project_section_no_metadata_finding(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            project:
              name: "my-pipeline"
            sources:
              entities:
                - id: people
                  uri: "/data/people.csv"
                  entity_type: Person
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        meta_findings = [
            f for f in report.findings if f.category == "missing-metadata"
        ]
        assert len(meta_findings) == 0


# ---------------------------------------------------------------------------
# Wildcard function — no false positive
# ---------------------------------------------------------------------------


class TestScanConfigFunctionNoFalsePositive:
    """Named function registration should not be flagged."""

    def test_named_function_not_flagged(self, tmp_config):
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities: []
            functions:
              - module: "mypackage.utils"
                names: "my_func,other_func"
        """)
        from pycypher.ingestion.config import load_pipeline_config

        cfg = load_pipeline_config(cfg_path)
        report = scan_config(cfg)
        wildcard_findings = [
            f for f in report.findings if f.category == "wildcard-import"
        ]
        assert len(wildcard_findings) == 0


# ---------------------------------------------------------------------------
# Additional CLI integration tests
# ---------------------------------------------------------------------------


class TestSecurityCheckCLIExtended:
    """Extended CLI integration tests."""

    def test_fail_on_info(self, runner, tmp_config):
        """--fail-on info should exit 1 when info findings exist."""
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: data
                  uri: "/safe/data.csv"
                  entity_type: Item
        """)
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--fail-on", "info"],
        )
        # No project section = INFO finding, should fail
        assert result.exit_code == 1

    def test_severity_low_filters_info(self, runner, tmp_config):
        """--severity low should exclude info-level findings."""
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: data
                  uri: "/safe/data.csv"
                  entity_type: Item
        """)
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--severity", "low"],
        )
        # Only INFO finding (missing metadata) should be excluded
        assert result.exit_code == 0
        assert "No security findings" in result.output

    def test_json_output_fields_complete(self, runner, tmp_config):
        """JSON output should have all required fields."""
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: remote
                  uri: "http://evil.example.com/data.csv"
                  entity_type: Record
        """)
        result = runner.invoke(
            cli,
            ["security-check", str(cfg_path), "--json"],
        )
        data = json.loads(result.output)
        assert len(data) >= 1
        for finding in data:
            assert "severity" in finding
            assert "category" in finding
            assert "message" in finding
            assert "location" in finding

    def test_summary_output_with_mixed_severities(self, runner, tmp_config):
        """Summary should show high/medium counts."""
        cfg_path = tmp_config("""\
            version: "1.0"
            sources:
              entities:
                - id: remote
                  uri: "http://evil.example.com/data.csv"
                  entity_type: Record
            functions:
              - module: "pkg.mod"
                names: "*"
        """)
        result = runner.invoke(cli, ["security-check", str(cfg_path)])
        assert "high" in result.output.lower()
        assert "medium" in result.output.lower()

    @pytest.mark.parametrize(
        "param",
        ["password", "secret", "token", "apikey"],
    )
    def test_credential_patterns_detected(self, param):
        """Various credential parameter names should all be caught."""
        from pycypher.cli.security import _check_uri_security

        report = SecurityReport()
        _check_uri_security(
            f"https://api.example.com/data.csv?{param}=value123",
            "test.uri",
            report,
        )
        cred_findings = [
            f for f in report.findings if f.category == "embedded-credentials"
        ]
        assert len(cred_findings) >= 1, (
            f"Credential pattern '{param}' not detected"
        )
