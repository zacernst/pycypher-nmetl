"""Security check command for nmetl CLI.

Static analysis of pipeline YAML configs to detect security misconfigurations
before deployment: insecure URLs, overly broad file globs, missing credential
references, and unsafe environment variable patterns.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlparse

import click

from .common import load_config


class Severity(StrEnum):
    """Finding severity levels."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class Finding:
    """A single security finding."""

    severity: Severity
    category: str
    message: str
    location: str  # e.g. "sources.entities[0].uri"


@dataclass
class SecurityReport:
    """Aggregated security scan results."""

    findings: list[Finding] = field(default_factory=list)

    def add(
        self,
        severity: Severity,
        category: str,
        message: str,
        location: str,
    ) -> None:
        self.findings.append(
            Finding(
                severity=severity,
                category=category,
                message=message,
                location=location,
            ),
        )

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.HIGH)

    @property
    def medium_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.MEDIUM)


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

# Patterns that suggest credentials embedded in URIs
_CREDENTIAL_PATTERNS = re.compile(
    r"(password|passwd|secret|token|api_key|apikey)=",
    re.IGNORECASE,
)

# Overly broad glob patterns
_BROAD_GLOBS = {"*", "**", "*.*", "**/*", "**/*.*"}

# Env var reference pattern
_ENV_VAR_REF = re.compile(r"\$\{([^}]+)\}")

# Known sensitive env var names (should use vault/secret manager instead)
_INLINE_SECRET_NAMES = frozenset(
    {
        "PASSWORD",
        "SECRET",
        "TOKEN",
        "API_KEY",
        "APIKEY",
        "PRIVATE_KEY",
        "SECRET_KEY",
    },
)


def _check_uri_security(
    uri: str, location: str, report: SecurityReport
) -> None:
    """Check a single URI for security issues."""
    parsed = urlparse(uri)

    # HTTP where HTTPS should be used
    if parsed.scheme == "http":
        # Allow localhost/127.0.0.1 as HTTP
        host = parsed.hostname or ""
        if host not in ("localhost", "127.0.0.1", "::1", ""):
            report.add(
                Severity.HIGH,
                "insecure-transport",
                f"HTTP URL detected — data transmitted in cleartext: {uri}",
                location,
            )

    # Credentials embedded in URI
    if parsed.password:
        report.add(
            Severity.HIGH,
            "embedded-credentials",
            "Password embedded in URI — use environment variables instead",
            location,
        )
    elif _CREDENTIAL_PATTERNS.search(uri):
        report.add(
            Severity.HIGH,
            "embedded-credentials",
            "Credential-like parameter detected in URI — use environment "
            "variables or a secret manager instead",
            location,
        )

    # Userinfo in URI without password (username leak)
    if parsed.username and not parsed.password:
        report.add(
            Severity.MEDIUM,
            "username-in-uri",
            "Username embedded in URI — consider using environment variables",
            location,
        )


def _check_env_var_patterns(
    value: str, location: str, report: SecurityReport
) -> None:
    """Check environment variable references for unsafe patterns."""
    for match in _ENV_VAR_REF.finditer(value):
        var_name = match.group(1)
        upper = var_name.upper()
        # Warn if referencing vars that look like they hold secrets
        # but aren't being pulled from a secret manager
        for secret_name in _INLINE_SECRET_NAMES:
            if secret_name in upper:
                report.add(
                    Severity.MEDIUM,
                    "env-var-secret",
                    f"Environment variable ${{{var_name}}} appears to contain "
                    f"a secret — consider using a dedicated secret manager",
                    location,
                )
                break


def _check_glob_pattern(
    pattern: str, location: str, report: SecurityReport
) -> None:
    """Check file glob patterns for overly broad matches."""
    stripped = pattern.strip()
    if stripped in _BROAD_GLOBS:
        report.add(
            Severity.MEDIUM,
            "broad-glob",
            f"Overly broad file glob '{stripped}' — narrow the pattern to "
            f"specific file types or directories",
            location,
        )


def _check_query_injection(
    query: str, location: str, report: SecurityReport
) -> None:
    """Check inline queries for potential injection patterns."""
    # Env var substitution inside SQL/Cypher queries is risky
    if _ENV_VAR_REF.search(query):
        report.add(
            Severity.HIGH,
            "query-injection",
            "Environment variable substitution in query text — this may "
            "enable injection attacks. Use parameterized queries instead.",
            location,
        )


def scan_config(config: object) -> SecurityReport:
    """Run all security checks against a loaded PipelineConfig.

    Args:
        config: A validated PipelineConfig instance.

    Returns:
        A SecurityReport with all findings.

    """
    report = SecurityReport()

    # Check entity sources
    for i, source in enumerate(config.sources.entities):
        loc = f"sources.entities[{i}] (id={source.id!r})"
        _check_uri_security(source.uri, f"{loc}.uri", report)
        _check_env_var_patterns(source.uri, f"{loc}.uri", report)
        if source.query:
            _check_query_injection(source.query, f"{loc}.query", report)

    # Check relationship sources
    for i, source in enumerate(config.sources.relationships):
        loc = f"sources.relationships[{i}] (id={source.id!r})"
        _check_uri_security(source.uri, f"{loc}.uri", report)
        _check_env_var_patterns(source.uri, f"{loc}.uri", report)
        if source.query:
            _check_query_injection(source.query, f"{loc}.query", report)

    # Check output sinks
    for i, output in enumerate(config.output):
        loc = f"output[{i}] (query_id={output.query_id!r})"
        _check_uri_security(output.uri, f"{loc}.uri", report)
        _check_env_var_patterns(output.uri, f"{loc}.uri", report)

    # Check function registrations
    for i, func in enumerate(config.functions):
        loc = f"functions[{i}]"
        if func.module:
            # Warn about wildcard function registration
            if func.names == "*":
                report.add(
                    Severity.MEDIUM,
                    "wildcard-import",
                    f"Wildcard function registration from module "
                    f"'{func.module}' — explicitly list required functions",
                    loc,
                )

    # Check inline queries for injection risks
    for i, query in enumerate(config.queries):
        loc = f"queries[{i}] (id={query.id!r})"
        if query.inline:
            _check_query_injection(query.inline, f"{loc}.inline", report)
        if query.source:
            # Check glob-like source paths
            if "*" in query.source:
                _check_glob_pattern(query.source, f"{loc}.source", report)

    # Info: no project metadata
    if config.project is None:
        report.add(
            Severity.INFO,
            "missing-metadata",
            "No project metadata defined — consider adding a 'project' "
            "section for audit traceability",
            "project",
        )

    return report


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------

_SEVERITY_COLORS = {
    Severity.HIGH: "red",
    Severity.MEDIUM: "yellow",
    Severity.LOW: "cyan",
    Severity.INFO: "blue",
}

_SEVERITY_SYMBOLS = {
    Severity.HIGH: "!!",
    Severity.MEDIUM: "! ",
    Severity.LOW: "- ",
    Severity.INFO: "i ",
}


@click.command("security-check")
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--severity",
    type=click.Choice(["high", "medium", "low", "info"], case_sensitive=False),
    default="info",
    help="Minimum severity to report (default: info).",
)
@click.option(
    "--fail-on",
    type=click.Choice(["high", "medium", "low", "info"], case_sensitive=False),
    default="high",
    help="Exit with non-zero status if findings at this severity or above (default: high).",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output findings as JSON.",
)
def security_check(
    config: Path,
    severity: str,
    fail_on: str,
    output_json: bool,
) -> None:
    """Scan a pipeline config for security misconfigurations.

    Performs static analysis on CONFIG to detect:

    \b
      - HTTP URLs where HTTPS should be used
      - Embedded credentials in URIs
      - Overly broad file glob patterns
      - Unsafe environment variable patterns
      - Query injection risks from env var substitution
      - Wildcard function registrations
    """
    cfg = load_config(config)
    report = scan_config(cfg)

    severity_order = [
        Severity.INFO,
        Severity.LOW,
        Severity.MEDIUM,
        Severity.HIGH,
    ]
    min_idx = severity_order.index(Severity(severity))
    fail_idx = severity_order.index(Severity(fail_on))

    filtered = [
        f
        for f in report.findings
        if severity_order.index(f.severity) >= min_idx
    ]

    if output_json:
        import json

        data = [
            {
                "severity": f.severity.value,
                "category": f.category,
                "message": f.message,
                "location": f.location,
            }
            for f in filtered
        ]
        click.echo(json.dumps(data, indent=2))
    else:
        if not filtered:
            click.echo(
                click.style("No security findings.", fg="green", bold=True),
            )
        else:
            click.echo(
                click.style(
                    f"Security scan: {len(filtered)} finding(s)",
                    bold=True,
                ),
            )
            click.echo()
            for f in sorted(
                filtered,
                key=lambda x: severity_order.index(x.severity),
                reverse=True,
            ):
                symbol = _SEVERITY_SYMBOLS[f.severity]
                color = _SEVERITY_COLORS[f.severity]
                sev = click.style(
                    f"[{f.severity.value.upper()}]",
                    fg=color,
                    bold=True,
                )
                click.echo(f"  {symbol} {sev} {f.message}")
                click.echo(
                    f"     {click.style(f.location, dim=True)}",
                )
            click.echo()

            high = report.high_count
            med = report.medium_count
            summary_parts = []
            if high:
                summary_parts.append(click.style(f"{high} high", fg="red"))
            if med:
                summary_parts.append(click.style(f"{med} medium", fg="yellow"))
            rest = len(filtered) - high - med
            if rest:
                summary_parts.append(f"{rest} other")
            click.echo(f"  Summary: {', '.join(summary_parts)}")

    # Determine exit code
    should_fail = any(
        severity_order.index(f.severity) >= fail_idx for f in report.findings
    )
    if should_fail:
        raise SystemExit(1)
